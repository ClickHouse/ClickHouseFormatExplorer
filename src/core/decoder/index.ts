import { ClickHouseFormat } from '../types/formats';
import { ParsedData } from '../types/ast';
import { RowBinaryDecoder } from './rowbinary-decoder';
import { NativeDecoder } from './native-decoder';
import { ProtocolDecoder } from './protocol-decoder';
import { DEFAULT_NATIVE_PROTOCOL_VERSION } from '../types/native-protocol';

// Re-export types and classes
export { FormatDecoder } from './format-decoder';
export { RowBinaryDecoder } from './rowbinary-decoder';
export { NativeDecoder } from './native-decoder';
export { ProtocolDecoder } from './protocol-decoder';
export type { ProtocolCapture } from './protocol-decoder';
export { BinaryReader } from './reader';
export { decodeLEB128, decodeLEB128BigInt } from './leb128';

/** Minimal shape shared by every format decoder. */
export interface Decoder {
  decode(): ParsedData;
}

/**
 * Factory function to create the appropriate decoder for a format.
 *
 * For NativeProtocol the `data` is the concatenated [c2s][s2c] capture buffer
 * and `options.protocolC2SLength` is the split point (length of the
 * client→server portion).
 */
export function createDecoder(
  data: Uint8Array,
  format: ClickHouseFormat,
  options?: { nativeProtocolVersion?: number; protocolC2SLength?: number },
): Decoder {
  switch (format) {
    case ClickHouseFormat.RowBinaryWithNamesAndTypes:
      return new RowBinaryDecoder(data);
    case ClickHouseFormat.Native:
      return new NativeDecoder(data, options?.nativeProtocolVersion ?? DEFAULT_NATIVE_PROTOCOL_VERSION);
    case ClickHouseFormat.NativeProtocol:
      return new ProtocolDecoder(data, options?.protocolC2SLength ?? 0);
    default:
      throw new Error(`Unsupported format: ${format as string}`);
  }
}
