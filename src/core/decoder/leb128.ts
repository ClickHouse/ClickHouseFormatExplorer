import { BinaryReader } from './reader';
import { ByteRange } from '../types/ast';

/**
 * Decode an unsigned LEB128 variable-length integer
 * Used for String/Array/Map lengths
 */
export function decodeLEB128(reader: BinaryReader): { value: number; range: ByteRange } {
  const start = reader.offset;
  let result = 0;
  let shift = 0;

  while (true) {
    const { value: byte } = reader.readUInt8();
    result |= (byte & 0x7f) << shift;

    if ((byte & 0x80) === 0) {
      break;
    }
    shift += 7;

    // Safety check - prevent infinite loop
    if (shift > 35) {
      throw new Error('LEB128 overflow - value too large for 32-bit number');
    }
  }

  return { value: result, range: { start, end: reader.offset } };
}

/**
 * Decode an unsigned LEB128 into BigInt for very large values
 */
export function decodeLEB128BigInt(reader: BinaryReader): { value: bigint; range: ByteRange } {
  const start = reader.offset;
  let result = 0n;
  let shift = 0n;

  while (true) {
    const { value: byte } = reader.readUInt8();
    result |= BigInt(byte & 0x7f) << shift;

    if ((byte & 0x80) === 0) {
      break;
    }
    shift += 7n;

    // Safety check
    if (shift > 70n) {
      throw new Error('LEB128 overflow - value too large');
    }
  }

  return { value: result, range: { start, end: reader.offset } };
}
