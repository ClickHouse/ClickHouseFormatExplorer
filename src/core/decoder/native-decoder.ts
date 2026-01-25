import { FormatDecoder } from './format-decoder';
import { decodeLEB128 } from './leb128';
import { parseType } from '../parser/type-parser';
import { ClickHouseType, typeToString } from '../types/clickhouse-types';
import { AstNode, BlockColumnNode, BlockHeaderNode, BlockNode, ByteRange, ColumnDefinition, HeaderNode, ParsedData } from '../types/ast';
import { ClickHouseFormat } from '../types/formats';

/**
 * Native format decoder (column-oriented with blocks)
 *
 * Native format structure:
 * - Multiple blocks, each with:
 *   - numColumns (LEB128)
 *   - numRows (LEB128)
 *   - For each column:
 *     - name (LEB128 length + bytes)
 *     - type (LEB128 length + bytes)
 *     - column data (all values for this column)
 * - Empty block (0 columns, 0 rows) signals end
 */
export class NativeDecoder extends FormatDecoder {
  readonly format = ClickHouseFormat.Native;

  decode(): ParsedData {
    const blocks = this.decodeBlocks();
    const header = this.buildHeaderFromBlocks(blocks);

    return {
      format: this.format,
      header,
      blocks,
      totalBytes: this.reader.length,
    };
  }

  private decodeBlocks(): BlockNode[] {
    const blocks: BlockNode[] = [];
    let blockIndex = 0;

    while (this.reader.remaining > 0) {
      const block = this.decodeBlock(blockIndex);

      // Empty block (0 columns or 0 rows) signals end
      if (block.columns.length === 0 || block.rowCount === 0) {
        break;
      }

      blocks.push(block);
      blockIndex++;
    }

    return blocks;
  }

  private decodeBlock(index: number): BlockNode {
    const startOffset = this.reader.offset;

    // Read numColumns with byte range tracking
    const numColumnsStart = this.reader.offset;
    const { value: numColumns } = decodeLEB128(this.reader);
    const numColumnsRange: ByteRange = { start: numColumnsStart, end: this.reader.offset };

    // Read numRows with byte range tracking
    const numRowsStart = this.reader.offset;
    const { value: numRows } = decodeLEB128(this.reader);
    const numRowsRange: ByteRange = { start: numRowsStart, end: this.reader.offset };

    const header: BlockHeaderNode = {
      numColumns,
      numColumnsRange,
      numRows,
      numRowsRange,
    };

    // Empty block check
    if (numColumns === 0 || numRows === 0) {
      return {
        index,
        byteRange: { start: startOffset, end: this.reader.offset },
        header,
        rowCount: 0,
        columns: [],
      };
    }

    // Decode each column
    const columns: BlockColumnNode[] = [];
    for (let i = 0; i < numColumns; i++) {
      const column = this.decodeBlockColumn(index, i, numRows);
      columns.push(column);
    }

    return {
      index,
      byteRange: { start: startOffset, end: this.reader.offset },
      header,
      rowCount: numRows,
      columns,
    };
  }

  private decodeBlockColumn(blockIndex: number, columnIndex: number, rowCount: number): BlockColumnNode {
    // Read column name
    const nameStart = this.reader.offset;
    const { value: nameLen } = decodeLEB128(this.reader);
    const { value: nameBytes } = this.reader.readBytes(nameLen);
    const name = new TextDecoder().decode(nameBytes);
    const nameByteRange: ByteRange = { start: nameStart, end: this.reader.offset };

    // Read column type
    const typeStart = this.reader.offset;
    const { value: typeLen } = decodeLEB128(this.reader);
    const { value: typeBytes } = this.reader.readBytes(typeLen);
    const typeString = new TextDecoder().decode(typeBytes);
    const type = parseType(typeString);
    const typeByteRange: ByteRange = { start: typeStart, end: this.reader.offset };

    // Read column data
    const dataStart = this.reader.offset;
    const values = this.decodeColumnData(type, rowCount);
    const dataByteRange: ByteRange = { start: dataStart, end: this.reader.offset };

    return {
      id: `block-${blockIndex}-col-${columnIndex}`,
      name,
      nameByteRange,
      type,
      typeString,
      typeByteRange,
      dataByteRange,
      values,
    };
  }

  private decodeColumnData(type: ClickHouseType, rowCount: number): AstNode[] {
    // Handle complex types that have different columnar encoding
    switch (type.kind) {
      case 'Nullable':
      case 'Array':
      case 'Map':
      case 'LowCardinality':
      case 'Variant':
      case 'Dynamic':
      case 'JSON':
      case 'Nested':
        throw new Error(`Native format: ${typeToString(type)} not yet implemented`);
    }

    // Simple types: decode rowCount values sequentially
    const values: AstNode[] = [];
    for (let i = 0; i < rowCount; i++) {
      const node = this.decodeValue(type);
      node.label = `[${i}]`;
      values.push(node);
    }
    return values;
  }

  private decodeValue(type: ClickHouseType): AstNode {
    switch (type.kind) {
      // Unsigned integers
      case 'UInt8':
        return this.decodeUInt8();
      case 'UInt16':
        return this.decodeUInt16();
      case 'UInt32':
        return this.decodeUInt32();
      case 'UInt64':
        return this.decodeUInt64();
      case 'UInt128':
        return this.decodeUInt128();
      case 'UInt256':
        return this.decodeUInt256();

      // Signed integers
      case 'Int8':
        return this.decodeInt8();
      case 'Int16':
        return this.decodeInt16();
      case 'Int32':
        return this.decodeInt32();
      case 'Int64':
        return this.decodeInt64();
      case 'Int128':
        return this.decodeInt128();
      case 'Int256':
        return this.decodeInt256();

      // Floats
      case 'Float32':
        return this.decodeFloat32();
      case 'Float64':
        return this.decodeFloat64();
      case 'BFloat16':
        return this.decodeBFloat16();

      // Strings
      case 'String':
        return this.decodeString();
      case 'FixedString':
        return this.decodeFixedString(type.length);

      // Bool
      case 'Bool':
        return this.decodeBool();

      // Date/Time
      case 'Date':
        return this.decodeDate();
      case 'Date32':
        return this.decodeDate32();
      case 'DateTime':
        return this.decodeDateTime(type.timezone);
      case 'DateTime64':
        return this.decodeDateTime64(type.precision, type.timezone);

      // Special
      case 'UUID':
        return this.decodeUUID();
      case 'IPv4':
        return this.decodeIPv4();
      case 'IPv6':
        return this.decodeIPv6();

      // Decimal
      case 'Decimal32':
        return this.decodeDecimal32(type.scale);
      case 'Decimal64':
        return this.decodeDecimal64(type.scale);
      case 'Decimal128':
        return this.decodeDecimal128(type.scale);
      case 'Decimal256':
        return this.decodeDecimal256(type.scale);

      // Enum
      case 'Enum8':
        return this.decodeEnum8(type.values);
      case 'Enum16':
        return this.decodeEnum16(type.values);

      // Tuple (fixed-size, same encoding)
      case 'Tuple':
        return this.decodeTuple(type.elements, type.names);

      // Geo types (same encoding as RowBinary)
      case 'Point':
        return this.decodePoint();

      default:
        throw new Error(`Native format: ${typeToString(type)} not yet implemented`);
    }
  }

  private buildHeaderFromBlocks(blocks: BlockNode[]): HeaderNode {
    if (blocks.length === 0) {
      return {
        byteRange: { start: 0, end: 0 },
        columnCount: 0,
        columnCountRange: { start: 0, end: 0 },
        columns: [],
      };
    }

    const firstBlock = blocks[0];
    const columns: ColumnDefinition[] = firstBlock.columns.map((col) => ({
      name: col.name,
      nameByteRange: col.nameByteRange,
      type: col.type,
      typeString: col.typeString,
      typeByteRange: col.typeByteRange,
    }));

    return {
      byteRange: { start: 0, end: firstBlock.columns[0]?.dataByteRange.start ?? 0 },
      columnCount: columns.length,
      // For Native format, column count is per-block, use first block's range
      columnCountRange: firstBlock.header.numColumnsRange,
      columns,
    };
  }

  // Integer decoders
  private decodeUInt8(): AstNode {
    const { value, range } = this.reader.readUInt8();
    return {
      id: this.generateId(),
      type: 'UInt8',
      byteRange: range,
      value,
      displayValue: String(value),
    };
  }

  private decodeUInt16(): AstNode {
    const { value, range } = this.reader.readUInt16LE();
    return {
      id: this.generateId(),
      type: 'UInt16',
      byteRange: range,
      value,
      displayValue: String(value),
    };
  }

  private decodeUInt32(): AstNode {
    const { value, range } = this.reader.readUInt32LE();
    return {
      id: this.generateId(),
      type: 'UInt32',
      byteRange: range,
      value,
      displayValue: String(value),
    };
  }

  private decodeUInt64(): AstNode {
    const { value, range } = this.reader.readUInt64LE();
    return {
      id: this.generateId(),
      type: 'UInt64',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  private decodeUInt128(): AstNode {
    const { value, range } = this.reader.readUInt128LE();
    return {
      id: this.generateId(),
      type: 'UInt128',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  private decodeUInt256(): AstNode {
    const { value, range } = this.reader.readUInt256LE();
    return {
      id: this.generateId(),
      type: 'UInt256',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  private decodeInt8(): AstNode {
    const { value, range } = this.reader.readInt8();
    return {
      id: this.generateId(),
      type: 'Int8',
      byteRange: range,
      value,
      displayValue: String(value),
    };
  }

  private decodeInt16(): AstNode {
    const { value, range } = this.reader.readInt16LE();
    return {
      id: this.generateId(),
      type: 'Int16',
      byteRange: range,
      value,
      displayValue: String(value),
    };
  }

  private decodeInt32(): AstNode {
    const { value, range } = this.reader.readInt32LE();
    return {
      id: this.generateId(),
      type: 'Int32',
      byteRange: range,
      value,
      displayValue: String(value),
    };
  }

  private decodeInt64(): AstNode {
    const { value, range } = this.reader.readInt64LE();
    return {
      id: this.generateId(),
      type: 'Int64',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  private decodeInt128(): AstNode {
    const { value, range } = this.reader.readInt128LE();
    return {
      id: this.generateId(),
      type: 'Int128',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  private decodeInt256(): AstNode {
    const { value, range } = this.reader.readInt256LE();
    return {
      id: this.generateId(),
      type: 'Int256',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  // Float decoders
  private decodeFloat32(): AstNode {
    const { value, range } = this.reader.readFloat32LE();
    return {
      id: this.generateId(),
      type: 'Float32',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  private decodeFloat64(): AstNode {
    const { value, range } = this.reader.readFloat64LE();
    return {
      id: this.generateId(),
      type: 'Float64',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  private decodeBFloat16(): AstNode {
    const { value, range } = this.reader.readBFloat16LE();
    return {
      id: this.generateId(),
      type: 'BFloat16',
      byteRange: range,
      value,
      displayValue: value.toString(),
    };
  }

  // String decoders
  private decodeString(): AstNode {
    const startOffset = this.reader.offset;
    const { value: length } = decodeLEB128(this.reader);
    const { value: bytes } = this.reader.readBytes(length);
    const str = new TextDecoder().decode(bytes);

    return {
      id: this.generateId(),
      type: 'String',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: str,
      displayValue: `"${str}"`,
    };
  }

  private decodeFixedString(length: number): AstNode {
    const { value: bytes, range } = this.reader.readBytes(length);
    // Find first null byte to get actual string length
    let actualLength = length;
    for (let i = 0; i < bytes.length; i++) {
      if (bytes[i] === 0) {
        actualLength = i;
        break;
      }
    }
    const str = new TextDecoder().decode(bytes.slice(0, actualLength));

    return {
      id: this.generateId(),
      type: `FixedString(${length})`,
      byteRange: range,
      value: str,
      displayValue: `"${str}"`,
      metadata: { fixedLength: length, actualLength },
    };
  }

  // Bool decoder
  private decodeBool(): AstNode {
    const { value, range } = this.reader.readUInt8();
    return {
      id: this.generateId(),
      type: 'Bool',
      byteRange: range,
      value: value !== 0,
      displayValue: value !== 0 ? 'true' : 'false',
    };
  }

  // Date/Time decoders
  private decodeDate(): AstNode {
    const { value, range } = this.reader.readUInt16LE();
    const date = new Date(value * 24 * 60 * 60 * 1000);
    return {
      id: this.generateId(),
      type: 'Date',
      byteRange: range,
      value: date,
      displayValue: date.toISOString().split('T')[0],
      metadata: { daysSinceEpoch: value },
    };
  }

  private decodeDate32(): AstNode {
    const { value, range } = this.reader.readInt32LE();
    const date = new Date(value * 24 * 60 * 60 * 1000);
    return {
      id: this.generateId(),
      type: 'Date32',
      byteRange: range,
      value: date,
      displayValue: date.toISOString().split('T')[0],
      metadata: { daysSinceEpoch: value },
    };
  }

  private decodeDateTime(timezone?: string): AstNode {
    const { value, range } = this.reader.readUInt32LE();
    const date = new Date(value * 1000);
    return {
      id: this.generateId(),
      type: timezone ? `DateTime('${timezone}')` : 'DateTime',
      byteRange: range,
      value: date,
      displayValue: date.toISOString().replace('T', ' ').replace('Z', ''),
      metadata: { secondsSinceEpoch: value, timezone },
    };
  }

  private decodeDateTime64(precision: number, timezone?: string): AstNode {
    const { value, range } = this.reader.readInt64LE();
    const divisor = BigInt(Math.pow(10, precision));
    const seconds = Number(value / divisor);
    const subseconds = Number(value % divisor);
    const date = new Date(seconds * 1000 + subseconds / Math.pow(10, precision - 3));

    return {
      id: this.generateId(),
      type: timezone ? `DateTime64(${precision}, '${timezone}')` : `DateTime64(${precision})`,
      byteRange: range,
      value: date,
      displayValue: date.toISOString().replace('T', ' ').replace('Z', ''),
      metadata: { ticksSinceEpoch: value.toString(), precision, timezone },
    };
  }

  // Special type decoders
  private decodeUUID(): AstNode {
    const { value: bytes, range } = this.reader.readBytes(16);

    // ClickHouse UUID has special byte ordering
    const hex = (b: number) => b.toString(16).padStart(2, '0');

    const uuid = [
      hex(bytes[7]),
      hex(bytes[6]),
      hex(bytes[5]),
      hex(bytes[4]),
      '-',
      hex(bytes[3]),
      hex(bytes[2]),
      '-',
      hex(bytes[1]),
      hex(bytes[0]),
      '-',
      hex(bytes[15]),
      hex(bytes[14]),
      '-',
      hex(bytes[13]),
      hex(bytes[12]),
      hex(bytes[11]),
      hex(bytes[10]),
      hex(bytes[9]),
      hex(bytes[8]),
    ].join('');

    return {
      id: this.generateId(),
      type: 'UUID',
      byteRange: range,
      value: uuid,
      displayValue: uuid,
    };
  }

  private decodeIPv4(): AstNode {
    const { value: bytes, range } = this.reader.readBytes(4);
    // IPv4 stored as little-endian UInt32
    const ip = `${bytes[3]}.${bytes[2]}.${bytes[1]}.${bytes[0]}`;

    return {
      id: this.generateId(),
      type: 'IPv4',
      byteRange: range,
      value: ip,
      displayValue: ip,
    };
  }

  private decodeIPv6(): AstNode {
    const { value: bytes, range } = this.reader.readBytes(16);
    // Format as standard IPv6
    const groups: string[] = [];
    for (let i = 0; i < 16; i += 2) {
      groups.push(((bytes[i] << 8) | bytes[i + 1]).toString(16));
    }
    const ip = groups.join(':');

    return {
      id: this.generateId(),
      type: 'IPv6',
      byteRange: range,
      value: ip,
      displayValue: ip,
    };
  }

  // Decimal decoders
  private decodeDecimal32(scale: number): AstNode {
    const { value, range } = this.reader.readInt32LE();
    const scaleFactor = Math.pow(10, scale);
    const decoded = value / scaleFactor;
    return {
      id: this.generateId(),
      type: `Decimal32(${scale})`,
      byteRange: range,
      value: decoded,
      displayValue: decoded.toFixed(scale),
      metadata: { scale, rawValue: value },
    };
  }

  private decodeDecimal64(scale: number): AstNode {
    const { value, range } = this.reader.readInt64LE();
    const scaleFactor = BigInt(Math.pow(10, scale));
    const wholePart = value / scaleFactor;
    const fracPart = value % scaleFactor;
    const decoded = Number(wholePart) + Number(fracPart) / Number(scaleFactor);
    return {
      id: this.generateId(),
      type: `Decimal64(${scale})`,
      byteRange: range,
      value: decoded,
      displayValue: decoded.toFixed(scale),
      metadata: { scale, rawValue: value.toString() },
    };
  }

  private decodeDecimal128(scale: number): AstNode {
    const { value, range } = this.reader.readInt128LE();
    const scaleFactor = 10n ** BigInt(scale);
    const wholePart = value / scaleFactor;
    const fracPart = value >= 0n ? value % scaleFactor : -((-value) % scaleFactor);
    return {
      id: this.generateId(),
      type: `Decimal128(${scale})`,
      byteRange: range,
      value: value.toString(),
      displayValue: `${wholePart}.${fracPart.toString().padStart(scale, '0')}`,
      metadata: { scale, rawValue: value.toString() },
    };
  }

  private decodeDecimal256(scale: number): AstNode {
    const { value, range } = this.reader.readInt256LE();
    const scaleFactor = 10n ** BigInt(scale);
    const wholePart = value / scaleFactor;
    const fracPart = value >= 0n ? value % scaleFactor : -((-value) % scaleFactor);
    return {
      id: this.generateId(),
      type: `Decimal256(${scale})`,
      byteRange: range,
      value: value.toString(),
      displayValue: `${wholePart}.${fracPart.toString().padStart(scale, '0')}`,
      metadata: { scale, rawValue: value.toString() },
    };
  }

  // Enum decoders
  private decodeEnum8(values: Map<number, string>): AstNode {
    const { value, range } = this.reader.readUInt8();
    const name = values.get(value) ?? `<unknown:${value}>`;

    return {
      id: this.generateId(),
      type: 'Enum8',
      byteRange: range,
      value,
      displayValue: `'${name}'`,
      metadata: { enumValue: value, enumName: name },
    };
  }

  private decodeEnum16(values: Map<number, string>): AstNode {
    const { value, range } = this.reader.readUInt16LE();
    const name = values.get(value) ?? `<unknown:${value}>`;

    return {
      id: this.generateId(),
      type: 'Enum16',
      byteRange: range,
      value,
      displayValue: `'${name}'`,
      metadata: { enumValue: value, enumName: name },
    };
  }

  // Tuple decoder (same encoding as RowBinary for Native)
  private decodeTuple(elements: ClickHouseType[], names?: string[]): AstNode {
    const startOffset = this.reader.offset;
    const children: AstNode[] = [];

    for (let i = 0; i < elements.length; i++) {
      const label = names?.[i] ?? `[${i}]`;
      const child = this.decodeValue(elements[i]);
      child.label = label;
      children.push(child);
    }

    const typeStr = names
      ? `Tuple(${elements.map((e, i) => `${names[i]} ${typeToString(e)}`).join(', ')})`
      : `Tuple(${elements.map(typeToString).join(', ')})`;

    return {
      id: this.generateId(),
      type: typeStr,
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.map((c) => c.value),
      displayValue: `(${children.map((c) => c.displayValue).join(', ')})`,
      children,
    };
  }

  // Point decoder (same as RowBinary)
  private decodePoint(): AstNode {
    const startOffset = this.reader.offset;
    const x = this.decodeFloat64();
    const y = this.decodeFloat64();

    x.label = 'x';
    y.label = 'y';

    return {
      id: this.generateId(),
      type: 'Point',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: [x.value, y.value],
      displayValue: `(${x.displayValue}, ${y.displayValue})`,
      children: [x, y],
    };
  }
}
