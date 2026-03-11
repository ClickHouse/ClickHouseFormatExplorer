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
  columnCountRange: ByteRange;
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
 * Block header metadata (Native format)
 */
export interface BlockHeaderNode {
  byteRange: ByteRange;
  numColumns: number;
  numColumnsRange: ByteRange;
  numRows: number;
  numRowsRange: ByteRange;
  blockInfo?: NativeBlockInfo;
}

/**
 * Block node for block-based formats (Native)
 */
export interface BlockNode {
  index: number;
  byteRange: ByteRange;
  header: BlockHeaderNode;
  rowCount: number;
  columns: BlockColumnNode[];
}

/**
 * Column data within a block (Native format)
 */
export interface BlockColumnNode {
  id: string;
  name: string;
  nameByteRange: ByteRange;
  type: import('./clickhouse-types').ClickHouseType;
  typeString: string;
  typeByteRange: ByteRange;
  metadataByteRange: ByteRange;
  dataByteRange: ByteRange;
  serializationInfo?: NativeSerializationInfo;
  values: AstNode[];
}

export interface NativeBlockInfoField {
  fieldNumber: number;
  fieldName: string;
  value: boolean | number | number[];
  displayValue: string;
  fieldNumberRange: ByteRange;
  valueRange: ByteRange;
  byteRange: ByteRange;
}

export interface NativeBlockInfo {
  byteRange: ByteRange;
  terminatorRange: ByteRange;
  fields: NativeBlockInfoField[];
}

export interface NativeSerializationInfo {
  byteRange: ByteRange;
  hasCustomSerialization: boolean;
  hasCustomRange: ByteRange;
  kindStack: string[];
  kindStackRange?: ByteRange;
}

/**
 * Complete parsed data structure
 */
export interface ParsedData {
  format: import('./formats').ClickHouseFormat;
  header: HeaderNode;
  totalBytes: number;
  /** Row-based formats (RowBinaryWithNamesAndTypes) */
  rows?: RowNode[];
  /** Block-based formats (Native) */
  blocks?: BlockNode[];
}
