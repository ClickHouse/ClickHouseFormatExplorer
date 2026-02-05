import { StartedClickHouseContainer, ClickHouseContainer } from '@testcontainers/clickhouse';
import { RowBinaryDecoder } from './rowbinary-decoder';
import { NativeDecoder } from './native-decoder';
import { ParsedData, AstNode } from '../types/ast';

const IMAGE = 'clickhouse/clickhouse-server:latest';

/**
 * Shared test container and helper functions for integration tests
 */
export class TestContext {
  container: StartedClickHouseContainer | null = null;
  private baseUrl: string = '';
  private username: string = '';
  private password: string = '';

  async start(): Promise<void> {
    this.container = await new ClickHouseContainer(IMAGE).start();
    this.baseUrl = this.container.getHttpUrl();
    this.username = this.container.getUsername();
    this.password = this.container.getPassword();
  }

  async stop(): Promise<void> {
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  async queryRowBinary(
    sql: string,
    settings?: Record<string, string | number>
  ): Promise<Uint8Array> {
    const params = new URLSearchParams({
      user: this.username,
      password: this.password,
    });
    if (settings) {
      for (const [key, value] of Object.entries(settings)) {
        params.set(key, String(value));
      }
    }
    const response = await fetch(`${this.baseUrl}/?${params}`, {
      method: 'POST',
      body: `${sql} FORMAT RowBinaryWithNamesAndTypes`,
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`ClickHouse error: ${text}`);
    }
    const buffer = await response.arrayBuffer();
    return new Uint8Array(buffer);
  }

  async queryNative(
    sql: string,
    settings?: Record<string, string | number>
  ): Promise<Uint8Array> {
    const params = new URLSearchParams({
      user: this.username,
      password: this.password,
    });
    if (settings) {
      for (const [key, value] of Object.entries(settings)) {
        params.set(key, String(value));
      }
    }
    const response = await fetch(`${this.baseUrl}/?${params}`, {
      method: 'POST',
      body: `${sql} FORMAT Native`,
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`ClickHouse error: ${text}`);
    }
    const buffer = await response.arrayBuffer();
    return new Uint8Array(buffer);
  }

  /**
   * Execute a command (CREATE TABLE, INSERT, etc.) without format
   */
  async execute(
    sql: string,
    settings?: Record<string, string | number>
  ): Promise<void> {
    const params = new URLSearchParams({
      user: this.username,
      password: this.password,
    });
    if (settings) {
      for (const [key, value] of Object.entries(settings)) {
        params.set(key, String(value));
      }
    }
    const response = await fetch(`${this.baseUrl}/?${params}`, {
      method: 'POST',
      body: sql,
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`ClickHouse error: ${text}`);
    }
  }
}

/**
 * Decode RowBinaryWithNamesAndTypes data
 */
export function decodeRowBinary(data: Uint8Array): ParsedData {
  const decoder = new RowBinaryDecoder(data);
  return decoder.decode();
}

/**
 * Decode Native format data
 */
export function decodeNative(data: Uint8Array): ParsedData {
  const decoder = new NativeDecoder(data);
  return decoder.decode();
}

/**
 * Result wrapper with helper methods for accessing values uniformly
 */
export interface DecodedResult {
  data: ParsedData;
  dataLength: number;

  /** Get all values from a column (flattened across rows/blocks) */
  getColumnValues(colIndex: number): unknown[];

  /** Get all display values from a column */
  getColumnDisplayValues(colIndex: number): string[];

  /** Get all AST nodes from a column */
  getColumnNodes(colIndex: number): AstNode[];

  /** Get a single value at row,col position */
  getValue(rowIndex: number, colIndex: number): unknown;

  /** Get a single AST node at row,col position */
  getNode(rowIndex: number, colIndex: number): AstNode;

  /** Get total row count */
  rowCount: number;

  /** Get column count */
  columnCount: number;
}

/**
 * Wrap RowBinary ParsedData with helper methods
 */
export function wrapRowBinaryResult(data: ParsedData, dataLength: number): DecodedResult {
  return {
    data,
    dataLength,
    rowCount: data.rows?.length ?? 0,
    columnCount: data.header.columns.length,

    getColumnValues(colIndex: number): unknown[] {
      return data.rows?.map(r => r.values[colIndex].value) ?? [];
    },

    getColumnDisplayValues(colIndex: number): string[] {
      return data.rows?.map(r => r.values[colIndex].displayValue) ?? [];
    },

    getColumnNodes(colIndex: number): AstNode[] {
      return data.rows?.map(r => r.values[colIndex]) ?? [];
    },

    getValue(rowIndex: number, colIndex: number): unknown {
      return data.rows![rowIndex].values[colIndex].value;
    },

    getNode(rowIndex: number, colIndex: number): AstNode {
      return data.rows![rowIndex].values[colIndex];
    },
  };
}

/**
 * Wrap Native ParsedData with helper methods
 */
export function wrapNativeResult(data: ParsedData, dataLength: number): DecodedResult {
  // Flatten block values into row-like access
  const flattenedColumns: AstNode[][] = [];
  const numCols = data.header.columns.length;

  for (let col = 0; col < numCols; col++) {
    flattenedColumns[col] = data.blocks?.flatMap(b => b.columns[col].values) ?? [];
  }

  const totalRows = data.blocks?.reduce((sum, b) => sum + b.rowCount, 0) ?? 0;

  return {
    data,
    dataLength,
    rowCount: totalRows,
    columnCount: numCols,

    getColumnValues(colIndex: number): unknown[] {
      return flattenedColumns[colIndex].map(n => n.value);
    },

    getColumnDisplayValues(colIndex: number): string[] {
      return flattenedColumns[colIndex].map(n => n.displayValue);
    },

    getColumnNodes(colIndex: number): AstNode[] {
      return flattenedColumns[colIndex];
    },

    getValue(rowIndex: number, colIndex: number): unknown {
      return flattenedColumns[colIndex][rowIndex].value;
    },

    getNode(rowIndex: number, colIndex: number): AstNode {
      return flattenedColumns[colIndex][rowIndex];
    },
  };
}

/**
 * Recursively validate byte ranges in an AST node
 */
export function assertValidByteRanges(node: AstNode, dataLength: number): void {
  if (node.byteRange.start >= node.byteRange.end) {
    throw new Error(
      `Invalid byte range for ${node.type}: start (${node.byteRange.start}) >= end (${node.byteRange.end})`
    );
  }
  if (node.byteRange.end > dataLength) {
    throw new Error(
      `Byte range end (${node.byteRange.end}) exceeds data length (${dataLength}) for ${node.type}`
    );
  }
  if (node.children) {
    for (const child of node.children) {
      assertValidByteRanges(child, dataLength);
    }
  }
}

/**
 * Get array elements from an Array node (skip length child if present)
 */
export function getArrayElements(node: AstNode): AstNode[] {
  if (!node.children) return [];
  // RowBinary: first child is length node
  if (node.children[0]?.label === 'length') {
    return node.children.slice(1);
  }
  // Native: may have different structure
  return node.children.filter(c => c.label !== 'length' && c.type !== 'ArraySizes');
}

/**
 * Get the unwrapped value from a Nullable node
 */
export function unwrapNullable(node: AstNode): unknown {
  if (node.value === null) return null;
  if (node.children && node.children.length > 0) {
    return node.children[0].value;
  }
  return node.value;
}

/**
 * Collect all leaf (childless) nodes from an AST tree
 */
export function collectLeafNodes(node: AstNode): AstNode[] {
  if (!node.children || node.children.length === 0) {
    return [node];
  }
  return node.children.flatMap(child => collectLeafNodes(child));
}

/**
 * Result of byte coverage analysis
 */
export interface ByteCoverageResult {
  /** Total bytes in the data */
  totalBytes: number;
  /** Number of bytes covered by leaf nodes */
  coveredBytes: number;
  /** Ranges of bytes not covered by any leaf node */
  uncoveredRanges: Array<{ start: number; end: number }>;
  /** Coverage percentage (0-100) */
  coveragePercent: number;
  /** Whether coverage is complete */
  isComplete: boolean;
}

/**
 * Analyze byte coverage of leaf nodes in the AST
 * Goes through all childless nodes and checks if the entire data is covered
 */
export function analyzeByteRange(data: ParsedData, dataLength: number): ByteCoverageResult {
  // Collect all leaf nodes from the AST
  const leafNodes: AstNode[] = [];

  // From header
  for (const col of data.header.columns) {
    // Column names and types are leaf nodes (no children)
    leafNodes.push({
      id: 'header-col-name',
      type: 'String',
      byteRange: col.nameByteRange,
      value: col.name,
      displayValue: col.name,
    });
    leafNodes.push({
      id: 'header-col-type',
      type: 'String',
      byteRange: col.typeByteRange,
      value: col.typeString,
      displayValue: col.typeString,
    });
  }

  // From rows (RowBinary format)
  if (data.rows) {
    for (const row of data.rows) {
      for (const node of row.values) {
        leafNodes.push(...collectLeafNodes(node));
      }
    }
  }

  // From blocks (Native format)
  if (data.blocks) {
    for (const block of data.blocks) {
      for (const col of block.columns) {
        for (const node of col.values) {
          leafNodes.push(...collectLeafNodes(node));
        }
      }
    }
  }

  // Sort by start position and merge overlapping ranges
  const sortedRanges = leafNodes
    .map(n => ({ start: n.byteRange.start, end: n.byteRange.end }))
    .filter(r => r.start < r.end) // Filter out zero-length ranges
    .sort((a, b) => a.start - b.start);

  // Merge overlapping ranges
  const mergedRanges: Array<{ start: number; end: number }> = [];
  for (const range of sortedRanges) {
    if (mergedRanges.length === 0) {
      mergedRanges.push({ ...range });
    } else {
      const last = mergedRanges[mergedRanges.length - 1];
      if (range.start <= last.end) {
        // Overlapping or adjacent - extend
        last.end = Math.max(last.end, range.end);
      } else {
        // Gap - add new range
        mergedRanges.push({ ...range });
      }
    }
  }

  // Find uncovered ranges
  const uncoveredRanges: Array<{ start: number; end: number }> = [];
  let expectedStart = 0;

  for (const range of mergedRanges) {
    if (range.start > expectedStart) {
      uncoveredRanges.push({ start: expectedStart, end: range.start });
    }
    expectedStart = range.end;
  }

  // Check for gap at the end
  if (expectedStart < dataLength) {
    uncoveredRanges.push({ start: expectedStart, end: dataLength });
  }

  // Calculate coverage
  const coveredBytes = mergedRanges.reduce((sum, r) => sum + (r.end - r.start), 0);
  const coveragePercent = dataLength > 0 ? (coveredBytes / dataLength) * 100 : 100;

  return {
    totalBytes: dataLength,
    coveredBytes,
    uncoveredRanges,
    coveragePercent,
    isComplete: uncoveredRanges.length === 0,
  };
}

/**
 * Format uncovered byte ranges for display
 */
export function formatUncoveredRanges(
  result: ByteCoverageResult,
  data: Uint8Array
): string {
  if (result.isComplete) {
    return 'All bytes covered';
  }

  const lines: string[] = [];
  lines.push(`Coverage: ${result.coveragePercent.toFixed(1)}% (${result.coveredBytes}/${result.totalBytes} bytes)`);
  lines.push(`Uncovered ranges (${result.uncoveredRanges.length}):`);

  for (const range of result.uncoveredRanges) {
    const bytes = data.slice(range.start, Math.min(range.end, range.start + 16));
    const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join(' ');
    const suffix = range.end - range.start > 16 ? '...' : '';
    lines.push(`  [${range.start}-${range.end}): ${hex}${suffix}`);
  }

  return lines.join('\n');
}
