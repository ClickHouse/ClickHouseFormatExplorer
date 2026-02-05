import { FormatDecoder } from './format-decoder';
import { decodeLEB128 } from './leb128';
import { parseType } from '../parser/type-parser';
import { ClickHouseType, typeToString } from '../types/clickhouse-types';
import { AstNode, ByteRange, ColumnDefinition, HeaderNode, ParsedData, RowNode } from '../types/ast';
import { ClickHouseFormat } from '../types/formats';

/**
 * RowBinaryWithNamesAndTypes format decoder
 */
export class RowBinaryDecoder extends FormatDecoder {
  readonly format = ClickHouseFormat.RowBinaryWithNamesAndTypes;

  /**
   * Decode complete RBWNAT data
   */
  decode(): ParsedData {
    const header = this.decodeHeader();
    const rows = this.decodeRows(header.columns);
    return {
      format: this.format,
      header,
      rows,
      totalBytes: this.reader.length,
    };
  }

  /**
   * Decode the RBWNAT header (column count, names, types)
   */
  private decodeHeader(): HeaderNode {
    const startOffset = this.reader.offset;

    // Read column count with byte range tracking
    const columnCountStart = this.reader.offset;
    const { value: columnCount } = decodeLEB128(this.reader);
    const columnCountRange: ByteRange = { start: columnCountStart, end: this.reader.offset };

    // Read column names
    const names: Array<{ name: string; range: ByteRange }> = [];
    for (let i = 0; i < columnCount; i++) {
      const nameStart = this.reader.offset;
      const { value: len } = decodeLEB128(this.reader);
      const { value: bytes } = this.reader.readBytes(len);
      const name = new TextDecoder().decode(bytes);
      names.push({
        name,
        range: { start: nameStart, end: this.reader.offset },
      });
    }

    // Read column types
    const types: Array<{ type: ClickHouseType; typeString: string; range: ByteRange }> = [];
    for (let i = 0; i < columnCount; i++) {
      const typeStart = this.reader.offset;
      const { value: len } = decodeLEB128(this.reader);
      const { value: bytes } = this.reader.readBytes(len);
      const typeString = new TextDecoder().decode(bytes);
      const type = parseType(typeString);
      types.push({
        type,
        typeString,
        range: { start: typeStart, end: this.reader.offset },
      });
    }

    // Build column definitions
    const columns: ColumnDefinition[] = names.map((n, i) => ({
      name: n.name,
      nameByteRange: n.range,
      type: types[i].type,
      typeString: types[i].typeString,
      typeByteRange: types[i].range,
    }));

    return {
      byteRange: { start: startOffset, end: this.reader.offset },
      columnCount,
      columnCountRange,
      columns,
    };
  }

  /**
   * Decode all data rows
   */
  private decodeRows(columns: ColumnDefinition[]): RowNode[] {
    const rows: RowNode[] = [];
    let rowIndex = 0;

    while (this.reader.remaining > 0) {
      const rowStart = this.reader.offset;
      const values: AstNode[] = [];

      for (const col of columns) {
        const node = this.decodeValue(col.type, col.name);
        values.push(node);
      }

      rows.push({
        index: rowIndex++,
        byteRange: { start: rowStart, end: this.reader.offset },
        values,
      });
    }

    return rows;
  }

  /**
   * Decode a single value based on its type
   */
  private decodeValue(type: ClickHouseType, label?: string): AstNode {
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

      // Decimals
      case 'Decimal32':
        return this.decodeDecimal32(type.scale);
      case 'Decimal64':
        return this.decodeDecimal64(type.scale);
      case 'Decimal128':
        return this.decodeDecimal128(type.scale);
      case 'Decimal256':
        return this.decodeDecimal256(type.scale);

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
      case 'Enum8':
        return this.decodeEnum8(type.values);
      case 'Enum16':
        return this.decodeEnum16(type.values);

      // Collections
      case 'Array':
        return this.decodeArray(type.element);
      case 'Tuple':
        return this.decodeTuple(type.elements, type.names);
      case 'Map':
        return this.decodeMap(type.key, type.value);

      // Wrappers
      case 'Nullable':
        return this.decodeNullable(type.inner);
      case 'LowCardinality':
        // LowCardinality doesn't affect wire format
        return this.decodeValue(type.inner, label);

      // Advanced types
      case 'Variant':
        return this.decodeVariant(type.variants);
      case 'Dynamic':
        return this.decodeDynamic();
      case 'JSON':
        return this.decodeJSON(type.typedPaths);

      // Geo types
      case 'Point':
        return this.decodePoint();
      case 'Ring':
        return this.decodeRing();
      case 'Polygon':
        return this.decodePolygon();
      case 'MultiPolygon':
        return this.decodeMultiPolygon();
      case 'LineString':
        return this.decodeLineString();
      case 'MultiLineString':
        return this.decodeMultiLineString();
      case 'Geometry':
        return this.decodeGeometry();

      // Nested
      case 'Nested':
        return this.decodeNested(type.fields);

      // QBit
      case 'QBit':
        return this.decodeQBit(type.element, type.dimension);

      // AggregateFunction
      case 'AggregateFunction':
        return this.decodeAggregateFunction(type.functionName, type.argTypes);

      // Interval types (all stored as Int64)
      case 'IntervalNanosecond':
        return this.decodeInterval('IntervalNanosecond', 'nanoseconds');
      case 'IntervalMicrosecond':
        return this.decodeInterval('IntervalMicrosecond', 'microseconds');
      case 'IntervalMillisecond':
        return this.decodeInterval('IntervalMillisecond', 'milliseconds');
      case 'IntervalSecond':
        return this.decodeInterval('IntervalSecond', 'seconds');
      case 'IntervalMinute':
        return this.decodeInterval('IntervalMinute', 'minutes');
      case 'IntervalHour':
        return this.decodeInterval('IntervalHour', 'hours');
      case 'IntervalDay':
        return this.decodeInterval('IntervalDay', 'days');
      case 'IntervalWeek':
        return this.decodeInterval('IntervalWeek', 'weeks');
      case 'IntervalMonth':
        return this.decodeInterval('IntervalMonth', 'months');
      case 'IntervalQuarter':
        return this.decodeInterval('IntervalQuarter', 'quarters');
      case 'IntervalYear':
        return this.decodeInterval('IntervalYear', 'years');

      default:
        throw new Error(`Unknown type: ${(type as ClickHouseType).kind}`);
    }
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
      displayValue: `${sign}${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}.${subseconds.toString().padStart(precision, '0')}`,
      metadata: { precision, rawValue: value.toString() },
    };
  }

  // Special type decoders
  private decodeUUID(): AstNode {
    const { value: bytes, range } = this.reader.readBytes(16);

    // ClickHouse UUID has special byte ordering
    // Wire: E7 11 B3 5C 04 C4 F0 61 A0 DB D3 6A 00 A6 7B 90
    // UUID: 61f0c404-5cb3-11e7-907b-a6006ad3dba0
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

  private decodeEnum8(values: Map<number, string>): AstNode {
    const { value, range } = this.reader.readInt8();
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

  // Collection decoders
  private decodeArray(elementType: ClickHouseType): AstNode {
    const startOffset = this.reader.offset;

    // Decode array length with AST node
    const lengthStart = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    const lengthNode: AstNode = {
      id: this.generateId(),
      type: 'VarUInt',
      byteRange: { start: lengthStart, end: this.reader.offset },
      value: count,
      displayValue: String(count),
      label: 'length',
    };

    const children: AstNode[] = [lengthNode];

    for (let i = 0; i < count; i++) {
      const child = this.decodeValue(elementType, `[${i}]`);
      child.label = `[${i}]`;
      children.push(child);
    }

    return {
      id: this.generateId(),
      type: `Array(${typeToString(elementType)})`,
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.slice(1).map((c) => c.value), // Skip length node for value
      displayValue: `[${count} elements]`,
      children,
    };
  }

  private decodeTuple(elements: ClickHouseType[], names?: string[]): AstNode {
    const startOffset = this.reader.offset;
    const children: AstNode[] = [];

    for (let i = 0; i < elements.length; i++) {
      const label = names?.[i] ?? `[${i}]`;
      const child = this.decodeValue(elements[i], label);
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

  private decodeMap(keyType: ClickHouseType, valueType: ClickHouseType): AstNode {
    const startOffset = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    const children: AstNode[] = [];

    for (let i = 0; i < count; i++) {
      const entryStart = this.reader.offset;
      const keyNode = this.decodeValue(keyType);
      const valueNode = this.decodeValue(valueType);

      // Create a tuple node for each key-value pair
      const entryNode: AstNode = {
        id: this.generateId(),
        type: `(${typeToString(keyType)}, ${typeToString(valueType)})`,
        byteRange: { start: entryStart, end: this.reader.offset },
        value: [keyNode.value, valueNode.value],
        displayValue: `${keyNode.displayValue}: ${valueNode.displayValue}`,
        children: [
          { ...keyNode, label: 'key' },
          { ...valueNode, label: 'value' },
        ],
        label: `[${i}]`,
      };
      children.push(entryNode);
    }

    return {
      id: this.generateId(),
      type: `Map(${typeToString(keyType)}, ${typeToString(valueType)})`,
      byteRange: { start: startOffset, end: this.reader.offset },
      value: Object.fromEntries(children.map((c) => [c.children![0].value, c.children![1].value])),
      displayValue: `{${count} entries}`,
      children,
    };
  }

  // Nullable decoder
  private decodeNullable(innerType: ClickHouseType): AstNode {
    const startOffset = this.reader.offset;
    const { value: isNull } = this.reader.readUInt8();

    if (isNull === 1) {
      return {
        id: this.generateId(),
        type: `Nullable(${typeToString(innerType)})`,
        byteRange: { start: startOffset, end: this.reader.offset },
        value: null,
        displayValue: 'NULL',
      };
    }

    const child = this.decodeValue(innerType);
    return {
      id: this.generateId(),
      type: `Nullable(${typeToString(innerType)})`,
      byteRange: { start: startOffset, end: this.reader.offset },
      value: child.value,
      displayValue: child.displayValue,
      children: [child],
    };
  }

  // Variant decoder
  private decodeVariant(variants: ClickHouseType[]): AstNode {
    const startOffset = this.reader.offset;
    const { value: discriminant } = this.reader.readUInt8();

    // Discriminant 0xFF means NULL
    if (discriminant === 0xff) {
      return {
        id: this.generateId(),
        type: `Variant(${variants.map(typeToString).join(', ')})`,
        byteRange: { start: startOffset, end: this.reader.offset },
        value: null,
        displayValue: 'NULL',
      };
    }

    if (discriminant >= variants.length) {
      throw new Error(`Invalid Variant discriminant ${discriminant}, only ${variants.length} variants defined`);
    }

    const selectedType = variants[discriminant];
    const child = this.decodeValue(selectedType);

    return {
      id: this.generateId(),
      type: `Variant(${variants.map(typeToString).join(', ')})`,
      byteRange: { start: startOffset, end: this.reader.offset },
      value: child.value,
      displayValue: child.displayValue,
      children: [child],
      metadata: { discriminant, selectedType: typeToString(selectedType) },
    };
  }

  // Dynamic type decoder
  private decodeDynamic(): AstNode {
    const startOffset = this.reader.offset;

    // Read BinaryTypeIndex and any type parameters
    const { value: typeIndex } = this.reader.readUInt8();

    // Decode the type from binary encoding (this may consume additional bytes for type params)
    const dynamicType = this.decodeDynamicType(typeIndex);

    // Capture where the type definition ends (before the value)
    const typeDefEndOffset = this.reader.offset;

    if (dynamicType === null) {
      // Type index 0x00 = Nothing/NULL
      const typeDefNode: AstNode = {
        id: this.generateId(),
        type: 'BinaryTypeIndex',
        byteRange: { start: startOffset, end: typeDefEndOffset },
        value: typeIndex,
        displayValue: 'Nothing (NULL)',
        label: 'type',
      };

      return {
        id: this.generateId(),
        type: 'Dynamic',
        byteRange: { start: startOffset, end: this.reader.offset },
        value: null,
        displayValue: 'NULL',
        children: [typeDefNode],
        metadata: { typeIndex },
      };
    }

    const typeStr = typeToString(dynamicType);

    // Create a node for the type definition bytes
    const typeDefNode: AstNode = {
      id: this.generateId(),
      type: 'BinaryTypeIndex',
      byteRange: { start: startOffset, end: typeDefEndOffset },
      value: typeIndex,
      displayValue: typeStr,
      label: 'type',
    };

    const child = this.decodeValue(dynamicType);
    child.label = 'value';

    return {
      id: this.generateId(),
      type: 'Dynamic',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: child.value,
      displayValue: child.displayValue,
      children: [typeDefNode, child],
      metadata: { typeIndex, decodedType: typeStr },
    };
  }

  // Decode dynamic type from BinaryTypeIndex
  private decodeDynamicType(typeIndex: number): ClickHouseType | null {
    // BinaryTypeIndex values from ClickHouse docs
    switch (typeIndex) {
      case 0x00: return null; // Nothing
      case 0x01: return { kind: 'UInt8' };
      case 0x02: return { kind: 'UInt16' };
      case 0x03: return { kind: 'UInt32' };
      case 0x04: return { kind: 'UInt64' };
      case 0x05: return { kind: 'UInt128' };
      case 0x06: return { kind: 'UInt256' };
      case 0x07: return { kind: 'Int8' };
      case 0x08: return { kind: 'Int16' };
      case 0x09: return { kind: 'Int32' };
      case 0x0a: return { kind: 'Int64' };
      case 0x0b: return { kind: 'Int128' };
      case 0x0c: return { kind: 'Int256' };
      case 0x0d: return { kind: 'Float32' };
      case 0x0e: return { kind: 'Float64' };
      case 0x0f: return { kind: 'Date' };
      case 0x10: return { kind: 'Date32' };
      case 0x11: return { kind: 'DateTime' };
      case 0x12: {
        // DateTime with timezone
        const { value: tzLen } = decodeLEB128(this.reader);
        const { value: tzBytes } = this.reader.readBytes(tzLen);
        const timezone = new TextDecoder().decode(tzBytes);
        return { kind: 'DateTime', timezone };
      }
      case 0x13: {
        // DateTime64
        const { value: precision } = this.reader.readUInt8();
        return { kind: 'DateTime64', precision };
      }
      case 0x14: {
        // DateTime64 with timezone
        const { value: precision } = this.reader.readUInt8();
        const { value: tzLen } = decodeLEB128(this.reader);
        const { value: tzBytes } = this.reader.readBytes(tzLen);
        const timezone = new TextDecoder().decode(tzBytes);
        return { kind: 'DateTime64', precision, timezone };
      }
      case 0x15: return { kind: 'String' };
      case 0x16: {
        // FixedString
        const { value: length } = decodeLEB128(this.reader);
        return { kind: 'FixedString', length };
      }
      case 0x17: {
        // Enum8 - values are Int8
        const values = this.decodeEnumDefinition(1);
        return { kind: 'Enum8', values };
      }
      case 0x18: {
        // Enum16 - values are Int16
        const values = this.decodeEnumDefinition(2);
        return { kind: 'Enum16', values };
      }
      case 0x19: {
        // Decimal32
        const { value: precision } = decodeLEB128(this.reader);
        const { value: scale } = decodeLEB128(this.reader);
        return { kind: 'Decimal32', precision, scale };
      }
      case 0x1a: {
        // Decimal64
        const { value: precision } = decodeLEB128(this.reader);
        const { value: scale } = decodeLEB128(this.reader);
        return { kind: 'Decimal64', precision, scale };
      }
      case 0x1b: {
        // Decimal128
        const { value: precision } = decodeLEB128(this.reader);
        const { value: scale } = decodeLEB128(this.reader);
        return { kind: 'Decimal128', precision, scale };
      }
      case 0x1c: {
        // Decimal256
        const { value: precision } = decodeLEB128(this.reader);
        const { value: scale } = decodeLEB128(this.reader);
        return { kind: 'Decimal256', precision, scale };
      }
      case 0x1d: return { kind: 'UUID' };
      case 0x1e: {
        // Array
        const nextTypeIndex = this.reader.readUInt8().value;
        const element = this.decodeDynamicType(nextTypeIndex);
        if (!element) throw new Error('Array element type cannot be Nothing');
        return { kind: 'Array', element };
      }
      case 0x1f: {
        // Tuple (unnamed)
        const { value: count } = decodeLEB128(this.reader);
        const elements: ClickHouseType[] = [];
        for (let i = 0; i < count; i++) {
          const elemTypeIndex = this.reader.readUInt8().value;
          const elem = this.decodeDynamicType(elemTypeIndex);
          if (!elem) throw new Error('Tuple element type cannot be Nothing');
          elements.push(elem);
        }
        return { kind: 'Tuple', elements };
      }
      case 0x20: {
        // Named Tuple
        const { value: count } = decodeLEB128(this.reader);
        const elements: ClickHouseType[] = [];
        const names: string[] = [];
        for (let i = 0; i < count; i++) {
          const { value: nameLen } = decodeLEB128(this.reader);
          const { value: nameBytes } = this.reader.readBytes(nameLen);
          names.push(new TextDecoder().decode(nameBytes));
          const elemTypeIndex = this.reader.readUInt8().value;
          const elem = this.decodeDynamicType(elemTypeIndex);
          if (!elem) throw new Error('Tuple element type cannot be Nothing');
          elements.push(elem);
        }
        return { kind: 'Tuple', elements, names };
      }
      case 0x23: {
        // Nullable
        const innerTypeIndex = this.reader.readUInt8().value;
        const inner = this.decodeDynamicType(innerTypeIndex);
        if (!inner) throw new Error('Nullable inner type cannot be Nothing');
        return { kind: 'Nullable', inner };
      }
      case 0x26: {
        // LowCardinality
        const innerTypeIndex = this.reader.readUInt8().value;
        const inner = this.decodeDynamicType(innerTypeIndex);
        if (!inner) throw new Error('LowCardinality inner type cannot be Nothing');
        return { kind: 'LowCardinality', inner };
      }
      case 0x27: {
        // Map
        const keyTypeIndex = this.reader.readUInt8().value;
        const key = this.decodeDynamicType(keyTypeIndex);
        const valueTypeIndex = this.reader.readUInt8().value;
        const value = this.decodeDynamicType(valueTypeIndex);
        if (!key || !value) throw new Error('Map key/value type cannot be Nothing');
        return { kind: 'Map', key, value };
      }
      case 0x28: return { kind: 'IPv4' };
      case 0x29: return { kind: 'IPv6' };
      case 0x2a: {
        // Variant
        const { value: count } = decodeLEB128(this.reader);
        const variants: ClickHouseType[] = [];
        for (let i = 0; i < count; i++) {
          const varTypeIndex = this.reader.readUInt8().value;
          const variant = this.decodeDynamicType(varTypeIndex);
          if (!variant) throw new Error('Variant type cannot be Nothing');
          variants.push(variant);
        }
        return { kind: 'Variant', variants };
      }
      case 0x2b: {
        // Dynamic
        const { value: maxTypes } = decodeLEB128(this.reader);
        return { kind: 'Dynamic', maxTypes: maxTypes > 0 ? maxTypes : undefined };
      }
      case 0x2d: return { kind: 'Bool' };
      case 0x30: {
        // JSON with full parameters:
        // - 1 byte: serialization version
        // - LEB128: max_dynamic_paths
        // - 1 byte: max_dynamic_types
        // - LEB128: typed_paths_count + definitions
        // - LEB128: skip_paths_count + names
        // - LEB128: skip_regexp_count + patterns
        this.reader.readUInt8(); // serialization version
        const { value: maxDynamicPaths } = decodeLEB128(this.reader);
        this.reader.readUInt8(); // max_dynamic_types

        // Read typed paths
        const { value: typedPathsCount } = decodeLEB128(this.reader);
        const typedPaths = new Map<string, ClickHouseType>();
        for (let i = 0; i < typedPathsCount; i++) {
          const { value: nameLen } = decodeLEB128(this.reader);
          const { value: nameBytes } = this.reader.readBytes(nameLen);
          const name = new TextDecoder().decode(nameBytes);
          const typeIndex = this.reader.readUInt8().value;
          const pathType = this.decodeDynamicType(typeIndex);
          if (pathType) typedPaths.set(name, pathType);
        }

        // Skip paths (not stored in type, just consume the bytes)
        const { value: skipPathsCount } = decodeLEB128(this.reader);
        for (let i = 0; i < skipPathsCount; i++) {
          const { value: nameLen } = decodeLEB128(this.reader);
          this.reader.readBytes(nameLen);
        }

        // Skip regexp patterns
        const { value: skipRegexpCount } = decodeLEB128(this.reader);
        for (let i = 0; i < skipRegexpCount; i++) {
          const { value: patternLen } = decodeLEB128(this.reader);
          this.reader.readBytes(patternLen);
        }

        return {
          kind: 'JSON',
          maxDynamicPaths: maxDynamicPaths > 0 ? maxDynamicPaths : undefined,
          typedPaths: typedPaths.size > 0 ? typedPaths : undefined,
        };
      }
      case 0x31: return { kind: 'BFloat16' };
      case 0x32: return { kind: 'Time' };
      case 0x34: {
        // Time64
        const { value: precision } = this.reader.readUInt8();
        return { kind: 'Time64', precision };
      }
      default:
        throw new Error(`Unknown BinaryTypeIndex: 0x${typeIndex.toString(16)}`);
    }
  }

  // Helper to decode enum definition in Dynamic context
  // byteSize: 1 for Enum8, 2 for Enum16
  private decodeEnumDefinition(byteSize: 1 | 2): Map<number, string> {
    const { value: count } = decodeLEB128(this.reader);
    const values = new Map<number, string>();
    for (let i = 0; i < count; i++) {
      const { value: nameLen } = decodeLEB128(this.reader);
      const { value: nameBytes } = this.reader.readBytes(nameLen);
      const name = new TextDecoder().decode(nameBytes);
      const enumValue = byteSize === 1
        ? this.reader.readInt8().value
        : this.reader.readInt16LE().value;
      values.set(enumValue, name);
    }
    return values;
  }

  // JSON type decoder
  private decodeJSON(typedPaths?: Map<string, ClickHouseType>): AstNode {
    const startOffset = this.reader.offset;

    // Read number of paths
    const { value: pathCount } = decodeLEB128(this.reader);
    const children: AstNode[] = [];

    for (let i = 0; i < pathCount; i++) {
      const pathStart = this.reader.offset;

      // Read path string
      const { value: pathLen } = decodeLEB128(this.reader);
      const { value: pathBytes } = this.reader.readBytes(pathLen);
      const path = new TextDecoder().decode(pathBytes);

      // Check if this is a typed path
      const typedType = typedPaths?.get(path);

      let valueNode: AstNode;
      if (typedType) {
        // Typed path - decode according to declared type
        valueNode = this.decodeValue(typedType);
      } else {
        // Dynamic path - value is encoded as Dynamic
        valueNode = this.decodeDynamic();
      }

      const pathNode: AstNode = {
        id: this.generateId(),
        type: 'JSON path',
        byteRange: { start: pathStart, end: this.reader.offset },
        value: { [path]: valueNode.value },
        displayValue: `${path}: ${valueNode.displayValue}`,
        label: path,
        children: [valueNode],
      };
      children.push(pathNode);
    }

    // Build the combined JSON value
    const jsonValue: Record<string, unknown> = {};
    for (const child of children) {
      const path = child.label!;
      jsonValue[path] = child.children![0].value;
    }

    return {
      id: this.generateId(),
      type: 'JSON',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: jsonValue,
      displayValue: `{${pathCount} paths}`,
      children,
    };
  }

  // Geo type decoders

  // Point = Tuple(Float64, Float64)
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

  // Ring = Array(Point)
  private decodeRing(): AstNode {
    const startOffset = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    const children: AstNode[] = [];

    for (let i = 0; i < count; i++) {
      const point = this.decodePoint();
      point.label = `[${i}]`;
      children.push(point);
    }

    return {
      id: this.generateId(),
      type: 'Ring',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.map((c) => c.value),
      displayValue: `[${count} points]`,
      children,
    };
  }

  // Polygon = Array(Ring)
  private decodePolygon(): AstNode {
    const startOffset = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    const children: AstNode[] = [];

    for (let i = 0; i < count; i++) {
      const ring = this.decodeRing();
      ring.label = `[${i}]`;
      children.push(ring);
    }

    return {
      id: this.generateId(),
      type: 'Polygon',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.map((c) => c.value),
      displayValue: `[${count} rings]`,
      children,
    };
  }

  // MultiPolygon = Array(Polygon)
  private decodeMultiPolygon(): AstNode {
    const startOffset = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    const children: AstNode[] = [];

    for (let i = 0; i < count; i++) {
      const polygon = this.decodePolygon();
      polygon.label = `[${i}]`;
      children.push(polygon);
    }

    return {
      id: this.generateId(),
      type: 'MultiPolygon',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.map((c) => c.value),
      displayValue: `[${count} polygons]`,
      children,
    };
  }

  // LineString = Array(Point)
  private decodeLineString(): AstNode {
    const startOffset = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    const children: AstNode[] = [];

    for (let i = 0; i < count; i++) {
      const point = this.decodePoint();
      point.label = `[${i}]`;
      children.push(point);
    }

    return {
      id: this.generateId(),
      type: 'LineString',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.map((c) => c.value),
      displayValue: `[${count} points]`,
      children,
    };
  }

  // MultiLineString = Array(LineString)
  private decodeMultiLineString(): AstNode {
    const startOffset = this.reader.offset;
    const { value: count } = decodeLEB128(this.reader);
    const children: AstNode[] = [];

    for (let i = 0; i < count; i++) {
      const lineString = this.decodeLineString();
      lineString.label = `[${i}]`;
      children.push(lineString);
    }

    return {
      id: this.generateId(),
      type: 'MultiLineString',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.map((c) => c.value),
      displayValue: `[${count} line strings]`,
      children,
    };
  }

  // Geometry = Variant of geo types
  private decodeGeometry(): AstNode {
    const startOffset = this.reader.offset;
    const { value: discriminant } = this.reader.readUInt8();

    // Discriminant indices for Geometry:
    // 0=LineString, 1=MultiLineString, 2=MultiPolygon, 3=Point, 4=Polygon, 5=Ring
    let child: AstNode;
    let typeName: string;

    switch (discriminant) {
      case 0:
        child = this.decodeLineString();
        typeName = 'LineString';
        break;
      case 1:
        child = this.decodeMultiLineString();
        typeName = 'MultiLineString';
        break;
      case 2:
        child = this.decodeMultiPolygon();
        typeName = 'MultiPolygon';
        break;
      case 3:
        child = this.decodePoint();
        typeName = 'Point';
        break;
      case 4:
        child = this.decodePolygon();
        typeName = 'Polygon';
        break;
      case 5:
        child = this.decodeRing();
        typeName = 'Ring';
        break;
      default:
        throw new Error(`Unknown Geometry discriminant: ${discriminant}`);
    }

    return {
      id: this.generateId(),
      type: 'Geometry',
      byteRange: { start: startOffset, end: this.reader.offset },
      value: child.value,
      displayValue: `${typeName}: ${child.displayValue}`,
      children: [child],
      metadata: { discriminant, geoType: typeName },
    };
  }

  // Nested type decoder
  private decodeNested(fields: { name: string; type: ClickHouseType }[]): AstNode {
    const startOffset = this.reader.offset;
    const children: AstNode[] = [];

    // Nested is encoded as a sequence of arrays, one per field
    for (const field of fields) {
      const arrayNode = this.decodeArray(field.type);
      arrayNode.label = field.name;
      arrayNode.type = `Array(${typeToString(field.type)})`;
      children.push(arrayNode);
    }

    return {
      id: this.generateId(),
      type: `Nested(${fields.map((f) => `${f.name} ${typeToString(f.type)}`).join(', ')})`,
      byteRange: { start: startOffset, end: this.reader.offset },
      value: Object.fromEntries(children.map((c) => [c.label, c.value])),
      displayValue: `{${fields.length} fields}`,
      children,
    };
  }

  // QBit type decoder - stores quantized bit vectors
  // In RowBinary format, QBit is serialized like Array: VarUInt size + sequential elements
  private decodeQBit(elementType: ClickHouseType, dimension: number): AstNode {
    const startOffset = this.reader.offset;

    // Read size (should match dimension)
    const { value: size } = decodeLEB128(this.reader);

    const children: AstNode[] = [];
    for (let i = 0; i < size; i++) {
      const child = this.decodeValue(elementType);
      child.label = `[${i}]`;
      children.push(child);
    }

    return {
      id: this.generateId(),
      type: `QBit(${typeToString(elementType)}, ${dimension})`,
      byteRange: { start: startOffset, end: this.reader.offset },
      value: children.map((c) => c.value),
      displayValue: `[${children.map(c => c.displayValue).join(', ')}]`,
      children,
      metadata: { dimension, elementType: typeToString(elementType), size },
    };
  }

  // AggregateFunction decoder - format is function-specific, NO length prefix
  private decodeAggregateFunction(functionName: string, argTypes: ClickHouseType[]): AstNode {
    const startOffset = this.reader.offset;
    const children: AstNode[] = [];
    const funcLower = functionName.toLowerCase();

    const argTypesStr = argTypes.map(typeToString).join(', ');
    const typeStr = argTypesStr
      ? `AggregateFunction(${functionName}, ${argTypesStr})`
      : `AggregateFunction(${functionName})`;

    let displayValue: string;
    let value: unknown;

    if (funcLower === 'avg') {
      // avg: numerator (type depends on arg) + VarUInt denominator
      const numNode = argTypes.length > 0
        ? this.decodeValue(argTypes[0])
        : this.decodeUInt64();
      numNode.label = 'numerator (sum)';
      children.push(numNode);

      const denomStart = this.reader.offset;
      const { value: denominator } = decodeLEB128(this.reader);
      const denomNode: AstNode = {
        id: this.generateId(),
        type: 'VarUInt',
        byteRange: { start: denomStart, end: this.reader.offset },
        value: denominator,
        displayValue: String(denominator),
        label: 'denominator (count)',
      };
      children.push(denomNode);

      const sum = numNode.value;
      const avg = denominator > 0 ? Number(sum) / denominator : 0;
      displayValue = `avg=${avg.toFixed(2)} (sum=${sum}, count=${denominator})`;
      value = { sum, count: denominator, avg };
    } else if (funcLower === 'sum') {
      // sum: fixed-size value based on argument type
      const sumNode = argTypes.length > 0
        ? this.decodeValue(argTypes[0])
        : this.decodeUInt64();
      sumNode.label = 'sum';
      children.push(sumNode);
      displayValue = `sum=${sumNode.displayValue}`;
      value = sumNode.value;
    } else if (funcLower === 'count') {
      // count: VarUInt
      const countStart = this.reader.offset;
      const { value: count } = decodeLEB128(this.reader);
      const countNode: AstNode = {
        id: this.generateId(),
        type: 'VarUInt',
        byteRange: { start: countStart, end: this.reader.offset },
        value: count,
        displayValue: String(count),
        label: 'count',
      };
      children.push(countNode);
      displayValue = `count=${count}`;
      value = count;
    } else {
      // Unknown aggregate function - we can't decode without knowing the format
      // This is a limitation: we need function-specific knowledge
      throw new Error(
        `AggregateFunction(${functionName}) has no length prefix and format is unknown. ` +
        `Supported: avg, sum, count`
      );
    }

    return {
      id: this.generateId(),
      type: typeStr,
      byteRange: { start: startOffset, end: this.reader.offset },
      value,
      displayValue,
      children,
      metadata: { functionName, argTypes: argTypesStr },
    };
  }

  /**
   * Decode an Interval type (stored as Int64)
   */
  private decodeInterval(typeName: string, unit: string): AstNode {
    const { value, range } = this.reader.readInt64LE();
    return {
      id: this.generateId(),
      type: typeName,
      byteRange: range,
      value,
      displayValue: `${value} ${unit}`,
    };
  }
}
