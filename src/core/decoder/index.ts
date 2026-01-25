import { ClickHouseFormat } from '../types/formats';
import { FormatDecoder } from './format-decoder';
import { RowBinaryDecoder } from './rowbinary-decoder';
import { NativeDecoder } from './native-decoder';

// Re-export types and classes
export { FormatDecoder } from './format-decoder';
export { RowBinaryDecoder } from './rowbinary-decoder';
export { NativeDecoder } from './native-decoder';
export { BinaryReader } from './reader';
export { decodeLEB128, decodeLEB128BigInt } from './leb128';

/**
 * Factory function to create the appropriate decoder for a format
 */
export function createDecoder(data: Uint8Array, format: ClickHouseFormat): FormatDecoder {
  switch (format) {
    case ClickHouseFormat.RowBinaryWithNamesAndTypes:
      return new RowBinaryDecoder(data);
    case ClickHouseFormat.Native:
      return new NativeDecoder(data);
    default:
      throw new Error(`Unsupported format: ${format}`);
  }
}
