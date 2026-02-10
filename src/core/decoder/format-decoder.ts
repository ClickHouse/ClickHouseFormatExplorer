import { BinaryReader } from './reader';
import { decodeLEB128 } from './leb128';
import { AstNode, ParsedData } from '../types/ast';
import { ClickHouseFormat } from '../types/formats';

/**
 * Abstract base class for format-specific decoders
 */
export abstract class FormatDecoder {
  protected reader: BinaryReader;
  protected nodeIdCounter = 0;

  constructor(data: Uint8Array) {
    this.reader = new BinaryReader(data);
  }

  /** Format identifier */
  abstract readonly format: ClickHouseFormat;

  /** Main decode entry point */
  abstract decode(): ParsedData;

  /** Generate unique node ID */
  protected generateId(): string {
    return `node-${this.nodeIdCounter++}`;
  }

  /** Decode a LEB128 varint and return both the value and a leaf AstNode for it */
  protected decodeLEB128Node(label: string = 'length'): { count: number; node: AstNode } {
    const start = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    return {
      count,
      node: {
        id: this.generateId(),
        type: 'VarUInt',
        byteRange: { start, end: this.reader.offset },
        value: count,
        displayValue: String(count),
        label,
      },
    };
  }

  /** Create a UInt8 discriminant leaf node (for Nullable, Variant, Geometry, etc.) */
  protected createDiscriminantNode(startOffset: number, value: number, label: string): AstNode {
    return {
      id: this.generateId(),
      type: 'UInt8',
      byteRange: { start: startOffset, end: startOffset + 1 },
      value,
      displayValue: String(value),
      label,
    };
  }
}
