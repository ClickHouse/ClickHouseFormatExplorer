import { ByteRange } from '../types/ast';

/**
 * Binary reader with byte-range tracking for highlighting
 */
export class BinaryReader {
  private data: DataView;
  private bytes: Uint8Array;
  private pos: number = 0;

  constructor(data: Uint8Array) {
    this.bytes = data;
    this.data = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }

  get offset(): number {
    return this.pos;
  }

  get remaining(): number {
    return this.bytes.length - this.pos;
  }

  get length(): number {
    return this.bytes.length;
  }

  private makeRange(start: number): ByteRange {
    return { start, end: this.pos };
  }

  // Unsigned integers
  readUInt8(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getUint8(this.pos++);
    return { value, range: this.makeRange(start) };
  }

  readUInt16LE(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getUint16(this.pos, true);
    this.pos += 2;
    return { value, range: this.makeRange(start) };
  }

  readUInt32LE(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getUint32(this.pos, true);
    this.pos += 4;
    return { value, range: this.makeRange(start) };
  }

  readUInt64LE(): { value: bigint; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getBigUint64(this.pos, true);
    this.pos += 8;
    return { value, range: this.makeRange(start) };
  }

  // For 128-bit unsigned integers
  readUInt128LE(): { value: bigint; range: ByteRange } {
    const start = this.pos;
    let value = 0n;
    for (let i = 0; i < 16; i++) {
      value |= BigInt(this.bytes[this.pos + i]) << BigInt(i * 8);
    }
    this.pos += 16;
    return { value, range: this.makeRange(start) };
  }

  // For 256-bit unsigned integers
  readUInt256LE(): { value: bigint; range: ByteRange } {
    const start = this.pos;
    let value = 0n;
    for (let i = 0; i < 32; i++) {
      value |= BigInt(this.bytes[this.pos + i]) << BigInt(i * 8);
    }
    this.pos += 32;
    return { value, range: this.makeRange(start) };
  }

  // Signed integers
  readInt8(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getInt8(this.pos++);
    return { value, range: this.makeRange(start) };
  }

  readInt16LE(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getInt16(this.pos, true);
    this.pos += 2;
    return { value, range: this.makeRange(start) };
  }

  readInt32LE(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getInt32(this.pos, true);
    this.pos += 4;
    return { value, range: this.makeRange(start) };
  }

  readInt64LE(): { value: bigint; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getBigInt64(this.pos, true);
    this.pos += 8;
    return { value, range: this.makeRange(start) };
  }

  // For 128-bit signed integers
  readInt128LE(): { value: bigint; range: ByteRange } {
    const { value: unsigned, range } = this.readUInt128LE();
    const signBit = 1n << 127n;
    const value = unsigned >= signBit ? unsigned - (1n << 128n) : unsigned;
    return { value, range };
  }

  // For 256-bit signed integers
  readInt256LE(): { value: bigint; range: ByteRange } {
    const { value: unsigned, range } = this.readUInt256LE();
    const signBit = 1n << 255n;
    const value = unsigned >= signBit ? unsigned - (1n << 256n) : unsigned;
    return { value, range };
  }

  // Floating point
  readFloat32LE(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getFloat32(this.pos, true);
    this.pos += 4;
    return { value, range: this.makeRange(start) };
  }

  readFloat64LE(): { value: number; range: ByteRange } {
    const start = this.pos;
    const value = this.data.getFloat64(this.pos, true);
    this.pos += 8;
    return { value, range: this.makeRange(start) };
  }

  // BFloat16 - read as UInt16 and convert
  readBFloat16LE(): { value: number; range: ByteRange } {
    const start = this.pos;
    const bfloat16Bits = this.data.getUint16(this.pos, true);
    this.pos += 2;

    // Convert BFloat16 to Float32 by left-shifting 16 bits
    const float32Bits = bfloat16Bits << 16;
    const buffer = new ArrayBuffer(4);
    const view = new DataView(buffer);
    view.setUint32(0, float32Bits, false); // big-endian for proper bit interpretation
    const value = view.getFloat32(0, false);

    return { value, range: this.makeRange(start) };
  }

  // Raw bytes
  readBytes(length: number): { value: Uint8Array; range: ByteRange } {
    const start = this.pos;
    const value = this.bytes.slice(this.pos, this.pos + length);
    this.pos += length;
    return { value, range: this.makeRange(start) };
  }

  // Peek at bytes without advancing
  peekBytes(length: number): Uint8Array {
    return this.bytes.slice(this.pos, this.pos + length);
  }

  // Skip bytes
  skip(length: number): void {
    this.pos += length;
  }
}
