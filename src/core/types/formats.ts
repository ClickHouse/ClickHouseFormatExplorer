/**
 * Supported ClickHouse wire formats
 */
export enum ClickHouseFormat {
  RowBinaryWithNamesAndTypes = 'RowBinaryWithNamesAndTypes',
  Native = 'Native',
}

/**
 * Metadata about each format for UI display and feature detection
 */
export interface FormatMetadata {
  id: ClickHouseFormat;
  displayName: string;
  description: string;
  supportsBlocks: boolean;
}

/**
 * Format metadata registry
 */
export const FORMAT_METADATA: Record<ClickHouseFormat, FormatMetadata> = {
  [ClickHouseFormat.RowBinaryWithNamesAndTypes]: {
    id: ClickHouseFormat.RowBinaryWithNamesAndTypes,
    displayName: 'RowBinary (Names+Types)',
    description: 'Row-oriented binary format with header',
    supportsBlocks: false,
  },
  [ClickHouseFormat.Native]: {
    id: ClickHouseFormat.Native,
    displayName: 'Native',
    description: 'Column-oriented binary format with blocks',
    supportsBlocks: true,
  },
};
