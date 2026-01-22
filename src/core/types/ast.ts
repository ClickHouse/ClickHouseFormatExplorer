/**
 * Byte range for tracking positions in binary data
 */
export interface ByteRange {
  start: number; // Inclusive
  end: number;   // Exclusive
}

/**
 * AST node representing a decoded value with byte tracking
 */
export interface AstNode {
  id: string;
  type: string;
  byteRange: ByteRange;
  value: unknown;
  displayValue: string;
  children?: AstNode[];
  label?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Column definition from RBWNAT header
 */
export interface ColumnDefinition {
  name: string;
  nameByteRange: ByteRange;
  type: import('./clickhouse-types').ClickHouseType;
  typeString: string;
  typeByteRange: ByteRange;
}

/**
 * Parsed header from RowBinaryWithNamesAndTypes
 */
export interface HeaderNode {
  byteRange: ByteRange;
  columnCount: number;
  columns: ColumnDefinition[];
}

/**
 * Single row of data
 */
export interface RowNode {
  index: number;
  byteRange: ByteRange;
  values: AstNode[];
}

/**
 * Complete parsed data structure
 */
export interface ParsedData {
  header: HeaderNode;
  rows: RowNode[];
  totalBytes: number;
}
