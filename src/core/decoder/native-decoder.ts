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
        return this.decodeNullableColumn(type.inner, rowCount);
      case 'Array':
        return this.decodeArrayColumn(type.element, rowCount);
      case 'Map':
        return this.decodeMapColumn(type.key, type.value, rowCount);
      case 'LowCardinality':
        return this.decodeLowCardinalityColumn(type.inner, rowCount);
      case 'Variant':
        return this.decodeVariantColumn(type.variants, rowCount);
      case 'Dynamic':
        return this.decodeDynamicColumn(rowCount);
      case 'JSON':
        return this.decodeJSONColumn(type, rowCount);
      case 'Tuple':
        return this.decodeTupleColumn(type.elements, type.names, rowCount);
      case 'Nested':
        throw new Error(`Native format: ${typeToString(type)} not yet implemented`);
      // Geometry - Variant of geo types
      case 'Geometry':
        return this.decodeGeometryColumn(rowCount);
      // Geo types - Array-based
      case 'Ring':
        return this.decodeRingColumn(rowCount);
      case 'Polygon':
        return this.decodePolygonColumn(rowCount);
      case 'MultiPolygon':
        return this.decodeMultiPolygonColumn(rowCount);
      case 'LineString':
        return this.decodeLineStringColumn(rowCount);
      case 'MultiLineString':
        return this.decodeMultiLineStringColumn(rowCount);
      case 'QBit':
        return this.decodeQBitColumn(type.element, type.dimension, rowCount);
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
      case 'Time':
        return this.decodeTime();
      case 'Time64':
        return this.decodeTime64(type.precision);

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

      // QBit vector type
      case 'QBit':
        return this.decodeQBit(type.element, type.dimension);

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

  private decodeTime(): AstNode {
    const { value, range } = this.reader.readInt32LE();
    const sign = value < 0 ? '-' : '';
    const absValue = Math.abs(value);
    const hours = Math.floor(absValue / 3600);
    const minutes = Math.floor((absValue % 3600) / 60);
    const seconds = absValue % 60;

    return {
      id: this.generateId(),
      type: 'Time',
      byteRange: range,
      value,
      displayValue: `${sign}${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`,
      metadata: { totalSeconds: value },
    };
  }

  private decodeTime64(precision: number): AstNode {
    const { value, range } = this.reader.readInt64LE();
    const divisor = BigInt(Math.pow(10, precision));
    const totalSeconds = Number(value / divisor);
    const subseconds = Number(value % divisor);

    const sign = totalSeconds < 0 ? '-' : '';
    const absSeconds = Math.abs(totalSeconds);
    const hours = Math.floor(absSeconds / 3600);
    const minutes = Math.floor((absSeconds % 3600) / 60);
    const seconds = absSeconds % 60;

    return {
      id: this.generateId(),
      type: `Time64(${precision})`,
      byteRange: range,
      value: value.toString(),
      displayValue: `${sign}${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}.${Math.abs(subseconds).toString().padStart(precision, '0')}`,
      metadata: { precision, rawValue: value.toString() },
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

  // Tuple decoder for single value (used in nested contexts)
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

  /**
   * Tuple in Native format (columnar):
   * Each element stream is written sequentially for all rows
   * - For Tuple(A, B) with N rows: A0, A1, ..., A(N-1), B0, B1, ..., B(N-1)
   */
  private decodeTupleColumn(elements: ClickHouseType[], names: string[] | undefined, rowCount: number): AstNode[] {
    const typeStr = names
      ? `Tuple(${elements.map((e, i) => `${names[i]} ${typeToString(e)}`).join(', ')})`
      : `Tuple(${elements.map(typeToString).join(', ')})`;

    // Read all values for each element type
    const elementColumns: AstNode[][] = [];
    for (let i = 0; i < elements.length; i++) {
      elementColumns.push(this.decodeColumnData(elements[i], rowCount));
    }

    // Assemble tuples
    const values: AstNode[] = [];
    for (let row = 0; row < rowCount; row++) {
      const children: AstNode[] = [];

      for (let el = 0; el < elements.length; el++) {
        const label = names?.[el] ?? `[${el}]`;
        const child = elementColumns[el][row];
        child.label = label;
        children.push(child);
      }

      const startOffset = children[0]?.byteRange.start ?? this.reader.offset;
      const endOffset = children[children.length - 1]?.byteRange.end ?? this.reader.offset;

      values.push({
        id: this.generateId(),
        type: typeStr,
        byteRange: { start: startOffset, end: endOffset },
        value: children.map((c) => c.value),
        displayValue: `(${children.map((c) => c.displayValue).join(', ')})`,
        label: `[${row}]`,
        children,
      });
    }

    return values;
  }

  // =========================================
  // Complex type column decoders (Native-specific)
  // =========================================

  /**
   * Nullable in Native format:
   * 1. NullMap stream: N bytes (0x00 = not null, 0x01 = null)
   * 2. Values stream: N values of inner type (including placeholders for NULLs)
   */
  private decodeNullableColumn(innerType: ClickHouseType, rowCount: number): AstNode[] {
    const typeStr = `Nullable(${typeToString(innerType)})`;

    // Read null map first
    const nullMapStart = this.reader.offset;
    const nullMap: boolean[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt8();
      nullMap.push(value !== 0);
    }

    // Read all values (even for NULLs)
    const innerValues = this.decodeColumnData(innerType, rowCount);

    // Combine null map with values
    const values: AstNode[] = [];
    for (let i = 0; i < rowCount; i++) {
      const isNull = nullMap[i];
      const innerNode = innerValues[i];

      if (isNull) {
        values.push({
          id: this.generateId(),
          type: typeStr,
          byteRange: {
            start: nullMapStart + i,
            end: innerNode.byteRange.end,
          },
          value: null,
          displayValue: 'NULL',
          label: `[${i}]`,
          metadata: { isNull: true },
        });
      } else {
        values.push({
          id: this.generateId(),
          type: typeStr,
          byteRange: {
            start: nullMapStart + i,
            end: innerNode.byteRange.end,
          },
          value: innerNode.value,
          displayValue: innerNode.displayValue,
          label: `[${i}]`,
          children: innerNode.children,
          metadata: { isNull: false },
        });
      }
    }

    return values;
  }

  /**
   * Array in Native format:
   * 1. ArraySizes stream: N cumulative offsets as UInt64
   * 2. ArrayElements stream: flattened elements
   */
  private decodeArrayColumn(elementType: ClickHouseType, rowCount: number): AstNode[] {
    const typeStr = `Array(${typeToString(elementType)})`;

    // Read cumulative offsets
    const offsetsStart = this.reader.offset;
    const offsets: bigint[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt64LE();
      offsets.push(value);
    }

    // Calculate total elements and individual array sizes
    const totalElements = rowCount > 0 ? Number(offsets[rowCount - 1]) : 0;
    const sizes: number[] = [];
    let prevOffset = 0n;
    for (let i = 0; i < rowCount; i++) {
      sizes.push(Number(offsets[i] - prevOffset));
      prevOffset = offsets[i];
    }

    // Read all elements
    const allElements = this.decodeColumnData(elementType, totalElements);
    const elementsEnd = this.reader.offset;

    // Distribute elements to arrays
    const values: AstNode[] = [];
    let elementIndex = 0;
    for (let i = 0; i < rowCount; i++) {
      const size = sizes[i];
      const arrayElements = allElements.slice(elementIndex, elementIndex + size);
      elementIndex += size;

      // Update labels for array elements
      arrayElements.forEach((el, j) => {
        el.label = `[${j}]`;
      });

      values.push({
        id: this.generateId(),
        type: typeStr,
        byteRange: { start: offsetsStart + i * 8, end: elementsEnd },
        value: arrayElements.map(e => e.value),
        displayValue: `[${arrayElements.map(e => e.displayValue).join(', ')}]`,
        label: `[${i}]`,
        children: arrayElements,
        metadata: { size },
      });
    }

    return values;
  }

  /**
   * Map in Native format (as Array(Tuple(K, V))):
   * 1. ArraySizes stream: cumulative offsets
   * 2. Keys stream: all keys
   * 3. Values stream: all values
   */
  private decodeMapColumn(keyType: ClickHouseType, valueType: ClickHouseType, rowCount: number): AstNode[] {
    const typeStr = `Map(${typeToString(keyType)}, ${typeToString(valueType)})`;

    // Read cumulative offsets
    const offsets: bigint[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt64LE();
      offsets.push(value);
    }

    // Calculate sizes
    const totalEntries = rowCount > 0 ? Number(offsets[rowCount - 1]) : 0;
    const sizes: number[] = [];
    let prevOffset = 0n;
    for (let i = 0; i < rowCount; i++) {
      sizes.push(Number(offsets[i] - prevOffset));
      prevOffset = offsets[i];
    }

    // Read all keys
    const allKeys = this.decodeColumnData(keyType, totalEntries);

    // Read all values
    const allValues = this.decodeColumnData(valueType, totalEntries);

    // Distribute to maps
    const values: AstNode[] = [];
    let entryIndex = 0;
    for (let i = 0; i < rowCount; i++) {
      const size = sizes[i];
      const entries: AstNode[] = [];

      for (let j = 0; j < size; j++) {
        const key = allKeys[entryIndex + j];
        const value = allValues[entryIndex + j];

        key.label = 'key';
        value.label = 'value';

        entries.push({
          id: this.generateId(),
          type: `Tuple(${typeToString(keyType)}, ${typeToString(valueType)})`,
          byteRange: { start: key.byteRange.start, end: value.byteRange.end },
          value: [key.value, value.value],
          displayValue: `${key.displayValue}: ${value.displayValue}`,
          label: `[${j}]`,
          children: [key, value],
        });
      }

      entryIndex += size;

      values.push({
        id: this.generateId(),
        type: typeStr,
        byteRange: { start: entries[0]?.byteRange.start ?? this.reader.offset, end: this.reader.offset },
        value: Object.fromEntries(entries.map(e => [e.children![0].value, e.children![1].value])),
        displayValue: `{${entries.map(e => e.displayValue).join(', ')}}`,
        label: `[${i}]`,
        children: entries,
        metadata: { size },
      });
    }

    return values;
  }

  /**
   * LowCardinality in Native format:
   * 1. DictionaryKeys stream: KeysVersion (UInt64)
   * 2. DictionaryIndexes stream: type + dictionary + indexes
   */
  private decodeLowCardinalityColumn(innerType: ClickHouseType, rowCount: number): AstNode[] {
    const typeStr = `LowCardinality(${typeToString(innerType)})`;
    const startOffset = this.reader.offset;

    // Read KeysVersion (should be 1)
    this.reader.readUInt64LE(); // keysVersion - not used

    // Read IndexesSerializationType
    const { value: serializationType } = this.reader.readUInt64LE();

    // Extract flags from serialization type
    const indexType = Number(serializationType & 0xFFn);
    const hasAdditionalKeys = ((serializationType >> 9n) & 1n) === 1n;
    // const needGlobalDictionary = ((serializationType >> 8n) & 1n) === 1n;
    // const needUpdateDictionary = ((serializationType >> 10n) & 1n) === 1n;

    // Read additional keys (dictionary)
    let dictionary: AstNode[] = [];
    if (hasAdditionalKeys) {
      const { value: numKeys } = this.reader.readUInt64LE();

      // Determine the actual inner type for decoding (unwrap Nullable if present)
      const dictType = innerType.kind === 'Nullable' ? innerType.inner : innerType;
      dictionary = this.decodeColumnData(dictType, Number(numKeys));
    }

    // Read row count
    const { value: numRows } = this.reader.readUInt64LE();

    // Read indexes
    const indexes: number[] = [];
    for (let i = 0; i < Number(numRows); i++) {
      let idx: number;
      switch (indexType) {
        case 0: // UInt8
          idx = this.reader.readUInt8().value;
          break;
        case 1: // UInt16
          idx = this.reader.readUInt16LE().value;
          break;
        case 2: // UInt32
          idx = this.reader.readUInt32LE().value;
          break;
        case 3: // UInt64
          idx = Number(this.reader.readUInt64LE().value);
          break;
        default:
          throw new Error(`Unknown LowCardinality index type: ${indexType}`);
      }
      indexes.push(idx);
    }

    // Handle Nullable inner type - index 0 is the null placeholder
    const isNullable = innerType.kind === 'Nullable';

    // Build result values from indexes
    // Note: The dictionary includes a placeholder at index 0 (empty/default value for nullable)
    // Actual data values start at index 1, so we use direct dictionary lookup
    const values: AstNode[] = [];
    for (let i = 0; i < rowCount; i++) {
      const idx = indexes[i];

      if (isNullable && idx === 0) {
        // NULL value (index 0 is the null placeholder in dictionary)
        values.push({
          id: this.generateId(),
          type: typeStr,
          byteRange: { start: startOffset, end: this.reader.offset },
          value: null,
          displayValue: 'NULL',
          label: `[${i}]`,
        });
      } else {
        // Non-null value - direct dictionary lookup (index maps directly to dictionary position)
        const dictEntry = dictionary[idx];

        if (dictEntry) {
          values.push({
            id: this.generateId(),
            type: typeStr,
            byteRange: { start: startOffset, end: this.reader.offset },
            value: dictEntry.value,
            displayValue: dictEntry.displayValue,
            label: `[${i}]`,
            metadata: { dictionaryIndex: idx },
          });
        } else {
          values.push({
            id: this.generateId(),
            type: typeStr,
            byteRange: { start: startOffset, end: this.reader.offset },
            value: `<unknown:${idx}>`,
            displayValue: `<unknown:${idx}>`,
            label: `[${i}]`,
          });
        }
      }
    }

    return values;
  }

  /**
   * Variant in Native format:
   * 1. Discriminators prefix: mode (UInt64)
   * 2. Discriminators: N bytes (0xFF = NULL)
   * 3. Variant elements: sparse columns for each variant type
   */
  private decodeVariantColumn(variants: ClickHouseType[], rowCount: number): AstNode[] {
    const typeStr = `Variant(${variants.map(typeToString).join(', ')})`;
    const startOffset = this.reader.offset;

    // Read mode (0 = BASIC)
    const { value: _mode } = this.reader.readUInt64LE();

    // Read discriminators
    const discriminators: number[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt8();
      discriminators.push(value);
    }

    // Count values per variant type
    const countPerVariant: number[] = new Array(variants.length).fill(0);
    for (const disc of discriminators) {
      if (disc !== 0xFF && disc < variants.length) {
        countPerVariant[disc]++;
      }
    }

    // Read sparse data for each variant
    const variantData: AstNode[][] = [];
    for (let v = 0; v < variants.length; v++) {
      const count = countPerVariant[v];
      if (count > 0) {
        variantData[v] = this.decodeColumnData(variants[v], count);
      } else {
        variantData[v] = [];
      }
    }

    // Track current position in each variant's data
    const variantPositions: number[] = new Array(variants.length).fill(0);

    // Build result values
    const values: AstNode[] = [];
    for (let i = 0; i < rowCount; i++) {
      const disc = discriminators[i];

      if (disc === 0xFF) {
        // NULL
        values.push({
          id: this.generateId(),
          type: typeStr,
          byteRange: { start: startOffset, end: this.reader.offset },
          value: null,
          displayValue: 'NULL',
          label: `[${i}]`,
          metadata: { discriminator: disc },
        });
      } else if (disc < variants.length) {
        const variantNode = variantData[disc][variantPositions[disc]++];
        values.push({
          id: this.generateId(),
          type: typeToString(variants[disc]),
          byteRange: variantNode.byteRange,
          value: variantNode.value,
          displayValue: variantNode.displayValue,
          label: `[${i}]`,
          children: variantNode.children,
          metadata: { discriminator: disc, variantType: typeToString(variants[disc]) },
        });
      } else {
        values.push({
          id: this.generateId(),
          type: typeStr,
          byteRange: { start: startOffset, end: this.reader.offset },
          value: `<unknown discriminator: ${disc}>`,
          displayValue: `<unknown discriminator: ${disc}>`,
          label: `[${i}]`,
        });
      }
    }

    return values;
  }

  /**
   * Dynamic in Native format:
   * 1. DynamicStructure stream: version + type list
   * 2. DynamicData stream: internal Variant data (with extra SharedVariant)
   *
   * The internal Variant has numTypes + 1 variants:
   * - Index 0 to numTypes-1: declared types
   * - Index numTypes: SharedVariant (values of other types)
   * - Index 0xFF: NULL
   *
   * V1 format (version=1): SharedVariant stores values as String representation
   * V2 format (version=2): SharedVariant stores values as binary type index + binary value
   */
  private decodeDynamicColumn(rowCount: number): AstNode[] {
    const startOffset = this.reader.offset;

    // Read version (1 = V1, 2 = V2)
    const { value: version } = this.reader.readUInt64LE();

    // Read max_dynamic_types (V1 only - in V2 this field doesn't exist or has different meaning)
    if (version === 1n) {
      decodeLEB128(this.reader); // max_dynamic_types, historical field
    }

    // Read num_dynamic_types
    const { value: numTypes } = decodeLEB128(this.reader);

    // Read type names
    const typeNames: string[] = [];
    for (let i = 0; i < numTypes; i++) {
      const { value: len } = decodeLEB128(this.reader);
      const { value: bytes } = this.reader.readBytes(len);
      typeNames.push(new TextDecoder().decode(bytes));
    }

    // Parse types
    const variants = typeNames.map(name => parseType(name));

    // Read Variant discriminators prefix (mode)
    const { value: _mode } = this.reader.readUInt64LE();

    // Read discriminators
    const discriminators: number[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt8();
      discriminators.push(value);
    }

    // Count values per variant type (including SharedVariant at index numTypes)
    const countPerVariant: number[] = new Array(numTypes + 1).fill(0);
    for (const disc of discriminators) {
      if (disc !== 0xFF && disc <= numTypes) {
        countPerVariant[disc]++;
      }
    }

    // Read sparse data for declared types
    const variantData: AstNode[][] = [];
    for (let v = 0; v < numTypes; v++) {
      const count = countPerVariant[v];
      if (count > 0) {
        variantData[v] = this.decodeColumnData(variants[v], count);
      } else {
        variantData[v] = [];
      }
    }

    // Read SharedVariant data (index = numTypes)
    const sharedVariantCount = countPerVariant[numTypes];
    const sharedVariantData: AstNode[] = [];
    if (sharedVariantCount > 0) {
      if (version === 1n) {
        // V1: SharedVariant stores values as length-prefixed String representation
        for (let i = 0; i < sharedVariantCount; i++) {
          const valueStart = this.reader.offset;
          const { value: len } = decodeLEB128(this.reader);
          const { value: bytes } = this.reader.readBytes(len);
          const strValue = new TextDecoder().decode(bytes);

          sharedVariantData.push({
            id: this.generateId(),
            type: 'String',
            byteRange: { start: valueStart, end: this.reader.offset },
            value: strValue,
            displayValue: `"${strValue}"`,
            metadata: { isSharedVariant: true, serializationVersion: 1 },
          });
        }
      } else {
        // V2: SharedVariant stores values as binary type index + binary value
        for (let i = 0; i < sharedVariantCount; i++) {
          const valueStart = this.reader.offset;
          const { value: typeIdx } = decodeLEB128(this.reader);
          const innerType = this.decodeDynamicBinaryType(typeIdx);
          const innerValue = this.decodeValue(innerType);

          sharedVariantData.push({
            id: this.generateId(),
            type: typeToString(innerType),
            byteRange: { start: valueStart, end: this.reader.offset },
            value: innerValue.value,
            displayValue: innerValue.displayValue,
            children: innerValue.children,
            metadata: { isSharedVariant: true, binaryTypeIndex: typeIdx, serializationVersion: 2 },
          });
        }
      }
    }
    variantData[numTypes] = sharedVariantData;

    // Track current position in each variant's data
    const variantPositions: number[] = new Array(numTypes + 1).fill(0);

    // Build result values
    const values: AstNode[] = [];
    for (let i = 0; i < rowCount; i++) {
      const disc = discriminators[i];

      if (disc === 0xFF) {
        // NULL
        values.push({
          id: this.generateId(),
          type: 'Dynamic',
          byteRange: { start: startOffset, end: this.reader.offset },
          value: null,
          displayValue: 'NULL',
          label: `[${i}]`,
          metadata: { discriminator: disc },
        });
      } else if (disc <= numTypes) {
        const variantNode = variantData[disc][variantPositions[disc]++];
        values.push({
          id: this.generateId(),
          type: 'Dynamic',
          byteRange: variantNode.byteRange,
          value: variantNode.value,
          displayValue: variantNode.displayValue,
          label: `[${i}]`,
          children: variantNode.children,
          metadata: { discriminator: disc, actualType: variantNode.type },
        });
      } else {
        values.push({
          id: this.generateId(),
          type: 'Dynamic',
          byteRange: { start: startOffset, end: this.reader.offset },
          value: `<unknown discriminator: ${disc}>`,
          displayValue: `<unknown discriminator: ${disc}>`,
          label: `[${i}]`,
        });
      }
    }

    return values;
  }

  /**
   * Decode binary type index used in Dynamic's SharedVariant (V2 format)
   */
  private decodeDynamicBinaryType(typeIdx: number): ClickHouseType {
    // Common binary type indexes from ClickHouse BinaryTypeIndex enum
    const typeMap: Record<number, ClickHouseType> = {
      0: { kind: 'String' } as ClickHouseType, // Nothing maps to String as fallback
      1: { kind: 'UInt8' },
      2: { kind: 'UInt16' },
      3: { kind: 'UInt32' },
      4: { kind: 'UInt64' },
      5: { kind: 'UInt128' },
      6: { kind: 'UInt256' },
      7: { kind: 'Int8' },
      8: { kind: 'Int16' },
      9: { kind: 'Int32' },
      10: { kind: 'Int64' },
      11: { kind: 'Int128' },
      12: { kind: 'Int256' },
      13: { kind: 'Float32' },
      14: { kind: 'Float64' },
      15: { kind: 'Date' },
      16: { kind: 'Date32' },
      17: { kind: 'DateTime' },
      // 18: DateTime64 with precision - handled specially
      19: { kind: 'String' },
      20: { kind: 'UUID' },
      21: { kind: 'IPv4' },
      22: { kind: 'IPv6' },
      23: { kind: 'Bool' },
      // 24+: Complex types that need additional parsing
    };

    const type = typeMap[typeIdx];
    if (type) {
      return type;
    }

    // For unknown types, return String as fallback
    return { kind: 'String' };
  }

  /**
   * JSON in Native format
   *
   * Actual format discovered through debugging:
   * 1. max_dynamic_paths (UInt64) - typically 0
   * 2. typed_paths_count (LEB128) - number of dynamic paths in this column
   * 3. columns_count (LEB128) - same as typed_paths_count
   * 4. Path names (String for each)
   * 5. Column info for each column:
   *    - offset (UInt64)
   *    - serialization_kind (UInt16)
   *    - type_name (String)
   *    - metadata (UInt64)
   * 6. TYPED SUB-COLUMN VALUES (if JSON type has typed paths like JSON(a Int32))
   *    - These are raw values without flag bytes
   * 7. Dynamic column values (flag byte + value for each column/row)
   * 8. Shared data offsets (UInt64 per row)
   *
   * The resulting AST includes all structural elements as children.
   */
  private decodeJSONColumn(type: ClickHouseType, rowCount: number): AstNode[] {
    const startOffset = this.reader.offset;
    const values: AstNode[] = [];

    // Read max_dynamic_paths (UInt64) with AST node
    const maxDynPathsNode = this.decodeUInt64();
    maxDynPathsNode.label = 'max_dynamic_paths';

    // Read typed_paths_count with AST node
    const typedPathsCountStart = this.reader.offset;
    const { value: typedPathsCount } = decodeLEB128(this.reader);
    const typedPathsCountNode: AstNode = {
      id: this.generateId(),
      type: 'VarUInt',
      byteRange: { start: typedPathsCountStart, end: this.reader.offset },
      value: typedPathsCount,
      displayValue: String(typedPathsCount),
      label: 'typed_paths_count',
    };

    // Get typed sub-columns from JSON type
    const jsonType = type as { kind: 'JSON'; typedPaths?: Map<string, ClickHouseType> };
    const typedSubColumns = jsonType.typedPaths;

    // Handle JSON with no dynamic paths
    if (typedPathsCount === 0) {
      // If there are typed sub-columns, read them
      if (typedSubColumns && typedSubColumns.size > 0) {
        // For fully-typed JSON: flag byte + typed values + shared offset
        // Collect nodes per row: [flagNode, ...pathValueNodes]
        const rowData: { flagNode: AstNode; pathNodes: Map<string, AstNode> }[] = [];

        for (let row = 0; row < rowCount; row++) {
          // Read flag byte (0 = object present)
          const flagNode = this.decodeUInt8();
          flagNode.label = 'object_present';

          const pathNodes = new Map<string, AstNode>();
          for (const [pathName, pathType] of typedSubColumns) {
            const node = this.decodeValue(pathType);
            pathNodes.set(pathName, node);
          }

          rowData.push({ flagNode, pathNodes });
        }

        // Read shared_data_offsets
        const sharedOffsetNodes: AstNode[] = [];
        for (let i = 0; i < rowCount; i++) {
          const node = this.decodeUInt64();
          node.label = `shared_data_offset[${i}]`;
          sharedOffsetNodes.push(node);
        }

        // Build result nodes with all structural AST children
        for (let row = 0; row < rowCount; row++) {
          const children: AstNode[] = [];
          const jsonValue: Record<string, unknown> = {};

          // Add structural nodes
          children.push(maxDynPathsNode);
          children.push(typedPathsCountNode);
          children.push(rowData[row].flagNode);

          // Add path nodes
          for (const [pathName] of typedSubColumns) {
            const valueNode = rowData[row].pathNodes.get(pathName)!;
            jsonValue[pathName] = valueNode.value;

            children.push({
              id: this.generateId(),
              type: 'JSON path',
              byteRange: valueNode.byteRange,
              value: { [pathName]: valueNode.value },
              displayValue: `${pathName}: ${valueNode.displayValue}`,
              label: pathName,
              children: [valueNode],
            });
          }

          // Add shared offset node
          children.push(sharedOffsetNodes[row]);

          values.push({
            id: this.generateId(),
            type: 'JSON',
            byteRange: { start: startOffset, end: this.reader.offset },
            value: jsonValue,
            displayValue: `{${typedSubColumns.size} paths}`,
            label: `[${row}]`,
            children,
          });
        }

        return values;
      }

      // No typed sub-columns either - return empty JSON objects
      // Read shared_data_offsets
      const sharedOffsetNodes: AstNode[] = [];
      for (let i = 0; i < rowCount; i++) {
        const node = this.decodeUInt64();
        node.label = `shared_data_offset[${i}]`;
        sharedOffsetNodes.push(node);
      }

      for (let i = 0; i < rowCount; i++) {
        values.push({
          id: this.generateId(),
          type: 'JSON',
          byteRange: { start: startOffset, end: this.reader.offset },
          value: {},
          displayValue: '{0 paths}',
          label: `[${i}]`,
          children: [maxDynPathsNode, typedPathsCountNode, sharedOffsetNodes[i]],
        });
      }
      return values;
    }

    // Read columns_count with AST node
    const columnsCountStart = this.reader.offset;
    const { value: columnsCount } = decodeLEB128(this.reader);
    const columnsCountNode: AstNode = {
      id: this.generateId(),
      type: 'VarUInt',
      byteRange: { start: columnsCountStart, end: this.reader.offset },
      value: columnsCount,
      displayValue: String(columnsCount),
      label: 'columns_count',
    };

    // Read path names with AST nodes
    const pathNameNodes: AstNode[] = [];
    const pathNames: string[] = [];
    for (let i = 0; i < typedPathsCount; i++) {
      const pathStart = this.reader.offset;
      const { value: nameLen } = decodeLEB128(this.reader);
      const { value: nameBytes } = this.reader.readBytes(nameLen);
      const pathName = new TextDecoder().decode(nameBytes);
      pathNames.push(pathName);

      pathNameNodes.push({
        id: this.generateId(),
        type: 'String',
        byteRange: { start: pathStart, end: this.reader.offset },
        value: pathName,
        displayValue: `"${pathName}"`,
        label: `path_name[${i}]`,
      });
    }

    // Read column info with AST nodes
    const columnInfoNodes: AstNode[] = [];
    const columnInfos: { typeName: string }[] = [];
    for (let i = 0; i < columnsCount; i++) {
      const infoStart = this.reader.offset;

      const offsetNode = this.decodeUInt64();
      offsetNode.label = 'offset';

      const kindNode = this.decodeUInt16();
      kindNode.label = 'serialization_kind';

      const typeStart = this.reader.offset;
      const { value: typeLen } = decodeLEB128(this.reader);
      const { value: typeBytes } = this.reader.readBytes(typeLen);
      const typeName = new TextDecoder().decode(typeBytes);
      const typeNameNode: AstNode = {
        id: this.generateId(),
        type: 'String',
        byteRange: { start: typeStart, end: this.reader.offset },
        value: typeName,
        displayValue: `"${typeName}"`,
        label: 'type',
      };

      const metadataNode = this.decodeUInt64();
      metadataNode.label = 'metadata';

      columnInfos.push({ typeName });
      columnInfoNodes.push({
        id: this.generateId(),
        type: 'column_info',
        byteRange: { start: infoStart, end: this.reader.offset },
        value: { offset: offsetNode.value, kind: kindNode.value, type: typeName, metadata: metadataNode.value },
        displayValue: `${pathNames[i]}: ${typeName}`,
        label: `column_info[${i}]`,
        children: [offsetNode, kindNode, typeNameNode, metadataNode],
      });
    }

    // Read typed sub-column values first (if any) - collect AstNodes per path
    const typedPathNodes: Map<string, AstNode[]> = new Map();
    if (typedSubColumns && typedSubColumns.size > 0) {
      for (const [pathName, pathType] of typedSubColumns) {
        const nodes: AstNode[] = [];
        for (let row = 0; row < rowCount; row++) {
          nodes.push(this.decodeValue(pathType));
        }
        typedPathNodes.set(pathName, nodes);
      }
    }

    // Read dynamic column values (flag byte + value for each column/row) - collect AstNodes
    const dynamicPathData: { flagNodes: AstNode[]; valueNodes: AstNode[] }[] = [];
    for (let colIdx = 0; colIdx < columnsCount; colIdx++) {
      const { typeName } = columnInfos[colIdx];
      const colType = parseType(typeName);
      const flagNodes: AstNode[] = [];
      const valueNodes: AstNode[] = [];

      for (let row = 0; row < rowCount; row++) {
        const flagNode = this.decodeUInt8();
        flagNode.label = 'flag';
        flagNodes.push(flagNode);
        valueNodes.push(this.decodeValue(colType));
      }

      dynamicPathData.push({ flagNodes, valueNodes });
    }

    // Read shared_data_offsets
    const sharedOffsetNodes: AstNode[] = [];
    for (let i = 0; i < rowCount; i++) {
      const node = this.decodeUInt64();
      node.label = `shared_data_offset[${i}]`;
      sharedOffsetNodes.push(node);
    }

    // Build result nodes with all structural AST children
    for (let row = 0; row < rowCount; row++) {
      const children: AstNode[] = [];
      const jsonValue: Record<string, unknown> = {};

      // Add header structural nodes (shared across rows)
      children.push(maxDynPathsNode);
      children.push(typedPathsCountNode);
      children.push(columnsCountNode);

      // Add path name nodes
      for (const node of pathNameNodes) {
        children.push(node);
      }

      // Add column info nodes
      for (const node of columnInfoNodes) {
        children.push(node);
      }

      // Add typed sub-column path nodes
      for (const [pathName, nodes] of typedPathNodes) {
        const valueNode = nodes[row];
        this.setNestedValue(jsonValue, pathName, valueNode.value);

        children.push({
          id: this.generateId(),
          type: 'JSON path',
          byteRange: valueNode.byteRange,
          value: { [pathName]: valueNode.value },
          displayValue: `${pathName}: ${valueNode.displayValue}`,
          label: pathName,
          children: [valueNode],
        });
      }

      // Add dynamic path nodes (with flag + value)
      for (let colIdx = 0; colIdx < columnsCount; colIdx++) {
        const pathName = pathNames[colIdx];
        const { flagNodes, valueNodes } = dynamicPathData[colIdx];
        const flagNode = flagNodes[row];
        const valueNode = valueNodes[row];

        this.setNestedValue(jsonValue, pathName, valueNode.value);

        children.push({
          id: this.generateId(),
          type: 'JSON path',
          byteRange: { start: flagNode.byteRange.start, end: valueNode.byteRange.end },
          value: { [pathName]: valueNode.value },
          displayValue: `${pathName}: ${valueNode.displayValue}`,
          label: pathName,
          children: [flagNode, valueNode],
        });
      }

      // Add shared offset node
      children.push(sharedOffsetNodes[row]);

      const totalPaths = typedPathNodes.size + dynamicPathData.length;
      values.push({
        id: this.generateId(),
        type: 'JSON',
        byteRange: { start: startOffset, end: this.reader.offset },
        value: jsonValue,
        displayValue: `{${totalPaths} paths}`,
        label: `[${row}]`,
        children,
      });
    }

    return values;
  }

  /**
   * Helper to set a nested path value in an object
   * e.g., setNestedValue(obj, "nested.x", 10) -> obj.nested.x = 10
   */
  private setNestedValue(obj: Record<string, unknown>, path: string, value: unknown): void {
    const parts = path.split('.');
    let current = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!(parts[i] in current)) {
        current[parts[i]] = {};
      }
      current = current[parts[i]] as Record<string, unknown>;
    }
    current[parts[parts.length - 1]] = value;
  }

  /**
   * Helper to convert BigInt values to displayable format
   */
  private convertBigIntForDisplay(obj: unknown): unknown {
    if (obj === null || obj === undefined) return obj;
    if (typeof obj === 'bigint') return obj.toString();
    if (Array.isArray(obj)) return obj.map(v => this.convertBigIntForDisplay(v));
    if (typeof obj === 'object') {
      const result: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(obj)) {
        result[k] = this.convertBigIntForDisplay(v);
      }
      return result;
    }
    return obj;
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

  // =========================================
  // Geo type column decoders
  // =========================================

  /**
   * Ring = Array(Point) in columnar format
   */
  private decodeRingColumn(rowCount: number): AstNode[] {
    // Read cumulative offsets
    const offsets: bigint[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt64LE();
      offsets.push(value);
    }

    // Calculate sizes
    const totalPoints = rowCount > 0 ? Number(offsets[rowCount - 1]) : 0;
    const sizes: number[] = [];
    let prevOffset = 0n;
    for (let i = 0; i < rowCount; i++) {
      sizes.push(Number(offsets[i] - prevOffset));
      prevOffset = offsets[i];
    }

    // Read all points (columnar - all X's first, then all Y's)
    const allX: AstNode[] = [];
    for (let i = 0; i < totalPoints; i++) {
      allX.push(this.decodeFloat64());
    }
    const allY: AstNode[] = [];
    for (let i = 0; i < totalPoints; i++) {
      allY.push(this.decodeFloat64());
    }

    // Assemble points
    const allPoints: AstNode[] = [];
    for (let i = 0; i < totalPoints; i++) {
      allX[i].label = 'x';
      allY[i].label = 'y';
      allPoints.push({
        id: this.generateId(),
        type: 'Point',
        byteRange: { start: allX[i].byteRange.start, end: allY[i].byteRange.end },
        value: [allX[i].value, allY[i].value],
        displayValue: `(${allX[i].displayValue}, ${allY[i].displayValue})`,
        children: [allX[i], allY[i]],
      });
    }

    // Distribute to rings
    const values: AstNode[] = [];
    let pointIndex = 0;
    for (let i = 0; i < rowCount; i++) {
      const size = sizes[i];
      const ringPoints = allPoints.slice(pointIndex, pointIndex + size);
      pointIndex += size;

      ringPoints.forEach((p, j) => {
        p.label = `[${j}]`;
      });

      values.push({
        id: this.generateId(),
        type: 'Ring',
        byteRange: { start: ringPoints[0]?.byteRange.start ?? this.reader.offset, end: this.reader.offset },
        value: ringPoints.map(p => p.value),
        displayValue: `[${ringPoints.map(p => p.displayValue).join(', ')}]`,
        label: `[${i}]`,
        children: ringPoints,
        metadata: { size },
      });
    }

    return values;
  }

  /**
   * Polygon = Array(Ring) in columnar format
   */
  private decodePolygonColumn(rowCount: number): AstNode[] {
    // First: Array of Ring offsets
    const ringOffsets: bigint[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt64LE();
      ringOffsets.push(value);
    }

    const totalRings = rowCount > 0 ? Number(ringOffsets[rowCount - 1]) : 0;
    const ringSizes: number[] = [];
    let prevOffset = 0n;
    for (let i = 0; i < rowCount; i++) {
      ringSizes.push(Number(ringOffsets[i] - prevOffset));
      prevOffset = ringOffsets[i];
    }

    // Decode all rings
    const allRings = this.decodeRingColumn(totalRings);

    // Distribute to polygons
    const values: AstNode[] = [];
    let ringIndex = 0;
    for (let i = 0; i < rowCount; i++) {
      const size = ringSizes[i];
      const polygonRings = allRings.slice(ringIndex, ringIndex + size);
      ringIndex += size;

      polygonRings.forEach((r, j) => {
        r.label = `[${j}]`;
      });

      values.push({
        id: this.generateId(),
        type: 'Polygon',
        byteRange: { start: polygonRings[0]?.byteRange.start ?? this.reader.offset, end: this.reader.offset },
        value: polygonRings.map(r => r.value),
        displayValue: `[${polygonRings.map(r => r.displayValue).join(', ')}]`,
        label: `[${i}]`,
        children: polygonRings,
        metadata: { size },
      });
    }

    return values;
  }

  /**
   * MultiPolygon = Array(Polygon) in columnar format
   */
  private decodeMultiPolygonColumn(rowCount: number): AstNode[] {
    // First: Array of Polygon offsets
    const polyOffsets: bigint[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt64LE();
      polyOffsets.push(value);
    }

    const totalPolygons = rowCount > 0 ? Number(polyOffsets[rowCount - 1]) : 0;
    const polySizes: number[] = [];
    let prevOffset = 0n;
    for (let i = 0; i < rowCount; i++) {
      polySizes.push(Number(polyOffsets[i] - prevOffset));
      prevOffset = polyOffsets[i];
    }

    // Decode all polygons
    const allPolygons = this.decodePolygonColumn(totalPolygons);

    // Distribute to multipolygons
    const values: AstNode[] = [];
    let polyIndex = 0;
    for (let i = 0; i < rowCount; i++) {
      const size = polySizes[i];
      const multiPolyPolygons = allPolygons.slice(polyIndex, polyIndex + size);
      polyIndex += size;

      multiPolyPolygons.forEach((p, j) => {
        p.label = `[${j}]`;
      });

      values.push({
        id: this.generateId(),
        type: 'MultiPolygon',
        byteRange: { start: multiPolyPolygons[0]?.byteRange.start ?? this.reader.offset, end: this.reader.offset },
        value: multiPolyPolygons.map(p => p.value),
        displayValue: `[${multiPolyPolygons.map(p => p.displayValue).join(', ')}]`,
        label: `[${i}]`,
        children: multiPolyPolygons,
        metadata: { size },
      });
    }

    return values;
  }

  /**
   * LineString = Array(Point) in columnar format (same as Ring)
   */
  private decodeLineStringColumn(rowCount: number): AstNode[] {
    const rings = this.decodeRingColumn(rowCount);
    // Change type from Ring to LineString
    for (const r of rings) {
      r.type = 'LineString';
    }
    return rings;
  }

  /**
   * MultiLineString = Array(LineString) in columnar format
   */
  private decodeMultiLineStringColumn(rowCount: number): AstNode[] {
    // First: Array of LineString offsets
    const lineOffsets: bigint[] = [];
    for (let i = 0; i < rowCount; i++) {
      const { value } = this.reader.readUInt64LE();
      lineOffsets.push(value);
    }

    const totalLines = rowCount > 0 ? Number(lineOffsets[rowCount - 1]) : 0;
    const lineSizes: number[] = [];
    let prevOffset = 0n;
    for (let i = 0; i < rowCount; i++) {
      lineSizes.push(Number(lineOffsets[i] - prevOffset));
      prevOffset = lineOffsets[i];
    }

    // Decode all linestrings
    const allLines = this.decodeLineStringColumn(totalLines);

    // Distribute to multilinestrings
    const values: AstNode[] = [];
    let lineIndex = 0;
    for (let i = 0; i < rowCount; i++) {
      const size = lineSizes[i];
      const multiLineLines = allLines.slice(lineIndex, lineIndex + size);
      lineIndex += size;

      multiLineLines.forEach((l, j) => {
        l.label = `[${j}]`;
      });

      values.push({
        id: this.generateId(),
        type: 'MultiLineString',
        byteRange: { start: multiLineLines[0]?.byteRange.start ?? this.reader.offset, end: this.reader.offset },
        value: multiLineLines.map(l => l.value),
        displayValue: `[${multiLineLines.map(l => l.displayValue).join(', ')}]`,
        label: `[${i}]`,
        children: multiLineLines,
        metadata: { size },
      });
    }

    return values;
  }

  /**
   * Geometry is a Variant of geo types:
   * ClickHouse sorts variant types alphabetically:
   * 0=LineString, 1=MultiLineString, 2=MultiPolygon, 3=Point, 4=Polygon, 5=Ring
   */
  private decodeGeometryColumn(rowCount: number): AstNode[] {
    // Geometry is internally a Variant type with alphabetically sorted geo types
    const geoVariants: ClickHouseType[] = [
      { kind: 'LineString' },      // 0
      { kind: 'MultiLineString' }, // 1
      { kind: 'MultiPolygon' },    // 2
      { kind: 'Point' },           // 3
      { kind: 'Polygon' },         // 4
      { kind: 'Ring' },            // 5
    ];

    // Decode as Variant
    const variantValues = this.decodeVariantColumn(geoVariants, rowCount);

    // Update type to Geometry
    for (const v of variantValues) {
      const originalType = v.type;
      v.metadata = { ...v.metadata, geometryType: originalType };
    }

    return variantValues;
  }

  /**
   * QBit columnar decoder - bit-transposed vector format
   *
   * In Native format, QBit uses bit-transposed encoding:
   * - Data is organized as: for each bit plane (MSB to LSB), for each row, 1 byte
   * - Each byte contains packed bits from all vector elements
   * - Element 0 → bit 0, element 1 → bit 1, etc.
   *
   * For Float32: 32 bit planes, so 32 bytes per row
   * For Float64: 64 bit planes, so 64 bytes per row
   * For BFloat16: 16 bit planes, so 16 bytes per row
   */
  private decodeQBitColumn(elementType: ClickHouseType, dimension: number, rowCount: number): AstNode[] {
    const startOffset = this.reader.offset;

    // Determine bits per element based on type
    const bitsPerElement = elementType.kind === 'Float64' ? 64 : elementType.kind === 'BFloat16' ? 16 : 32;
    const totalBytes = bitsPerElement * rowCount;

    // Read all bit-transposed data
    const { value: data } = this.reader.readBytes(totalBytes);

    // Decode each row
    const values: AstNode[] = [];
    for (let row = 0; row < rowCount; row++) {
      const children: AstNode[] = [];

      for (let elem = 0; elem < dimension; elem++) {
        // Reconstruct the float value for this element
        let bits = 0n;

        for (let bitPlane = 0; bitPlane < bitsPerElement; bitPlane++) {
          // Data is organized as: [bp31_row0, bp31_row1, ..., bp30_row0, bp30_row1, ...]
          const byteIndex = bitPlane * rowCount + row;
          const byte = data[byteIndex];

          // Extract this element's bit from the byte
          const bit = (byte >> elem) & 1;

          // Place it in the correct position (MSB first)
          const bitPosition = bitsPerElement - 1 - bitPlane;
          if (bit) {
            bits |= 1n << BigInt(bitPosition);
          }
        }

        // Convert bits to float
        const floatValue = this.bitsToFloat(bits, elementType.kind as 'Float32' | 'Float64' | 'BFloat16');

        children.push({
          id: this.generateId(),
          type: typeToString(elementType),
          byteRange: { start: startOffset, end: this.reader.offset },
          value: floatValue,
          displayValue: floatValue.toString(),
          label: `[${elem}]`,
        });
      }

      const typeStr = `QBit(${typeToString(elementType)}, ${dimension})`;
      values.push({
        id: this.generateId(),
        type: typeStr,
        byteRange: { start: startOffset, end: this.reader.offset },
        value: children.map(c => c.value),
        displayValue: `[${children.map(c => c.displayValue).join(', ')}]`,
        label: `[${row}]`,
        children,
        metadata: { dimension, elementType: typeToString(elementType) },
      });
    }

    return values;
  }

  /**
   * Convert bit pattern to float value
   */
  private bitsToFloat(bits: bigint, type: 'Float32' | 'Float64' | 'BFloat16'): number {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);

    if (type === 'Float32') {
      view.setUint32(0, Number(bits), true);
      return view.getFloat32(0, true);
    } else if (type === 'Float64') {
      view.setBigUint64(0, bits, true);
      return view.getFloat64(0, true);
    } else {
      // BFloat16: 16-bit format, convert to Float32 by left-shifting
      const float32Bits = Number(bits) << 16;
      view.setUint32(0, float32Bits, false);
      return view.getFloat32(0, false);
    }
  }

  // Single QBit value decoder (for nested contexts - delegates to column decoder)
  private decodeQBit(elementType: ClickHouseType, dimension: number): AstNode {
    const values = this.decodeQBitColumn(elementType, dimension, 1);
    return values[0];
  }
}
