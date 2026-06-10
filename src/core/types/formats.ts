/**
 * Supported ClickHouse wire formats
 */
export enum ClickHouseFormat {
  RowBinaryWithNamesAndTypes = 'RowBinaryWithNamesAndTypes',
  Native = 'Native',
  /** Native TCP protocol capture (packet stream), not an HTTP FORMAT. */
  NativeProtocol = 'NativeProtocol',
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
    displayName: 'Native format (HTTP)',
    description: 'Column-oriented Native format body fetched over HTTP',
    supportsBlocks: true,
  },
  [ClickHouseFormat.NativeProtocol]: {
    id: ClickHouseFormat.NativeProtocol,
    displayName: 'Native protocol + format (TCP)',
    description: 'Full native TCP protocol packet stream (handshake, packets, and Native blocks) captured via proxy',
    supportsBlocks: true,
  },
};
