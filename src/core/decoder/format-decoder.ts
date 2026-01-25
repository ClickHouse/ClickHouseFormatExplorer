import { BinaryReader } from './reader';
import { ParsedData } from '../types/ast';
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
}
