/**
 * ClickHouse type system representation
 */
export type ClickHouseType =
  // Primitive integers
  | { kind: 'UInt8' }
  | { kind: 'UInt16' }
  | { kind: 'UInt32' }
  | { kind: 'UInt64' }
  | { kind: 'UInt128' }
  | { kind: 'UInt256' }
  | { kind: 'Int8' }
  | { kind: 'Int16' }
  | { kind: 'Int32' }
  | { kind: 'Int64' }
  | { kind: 'Int128' }
  | { kind: 'Int256' }
  // Floats
  | { kind: 'Float32' }
  | { kind: 'Float64' }
  | { kind: 'BFloat16' }
  // Decimals
  | { kind: 'Decimal32'; precision: number; scale: number }
  | { kind: 'Decimal64'; precision: number; scale: number }
  | { kind: 'Decimal128'; precision: number; scale: number }
  | { kind: 'Decimal256'; precision: number; scale: number }
  // Strings
  | { kind: 'String' }
  | { kind: 'FixedString'; length: number }
  // Date/Time
  | { kind: 'Date' }
  | { kind: 'Date32' }
  | { kind: 'DateTime'; timezone?: string }
  | { kind: 'DateTime64'; precision: number; timezone?: string }
  | { kind: 'Time' }
  | { kind: 'Time64'; precision: number }
  // Special
  | { kind: 'UUID' }
  | { kind: 'IPv4' }
  | { kind: 'IPv6' }
  | { kind: 'Bool' }
  // Enums
  | { kind: 'Enum8'; values: Map<number, string> }
  | { kind: 'Enum16'; values: Map<number, string> }
  // Collections
  | { kind: 'Array'; element: ClickHouseType }
  | { kind: 'Tuple'; elements: ClickHouseType[]; names?: string[] }
  | { kind: 'Map'; key: ClickHouseType; value: ClickHouseType }
  // Wrappers
  | { kind: 'Nullable'; inner: ClickHouseType }
  | { kind: 'LowCardinality'; inner: ClickHouseType }
  // Advanced
  | { kind: 'Variant'; variants: ClickHouseType[] }
  | { kind: 'Dynamic'; maxTypes?: number }
  | { kind: 'JSON'; typedPaths?: Map<string, ClickHouseType>; maxDynamicPaths?: number }
  // Geo
  | { kind: 'Point' }
  | { kind: 'Ring' }
  | { kind: 'Polygon' }
  | { kind: 'MultiPolygon' }
  | { kind: 'LineString' }
  | { kind: 'MultiLineString' }
  | { kind: 'Geometry' }
  // Nested (represented as array of arrays)
  | { kind: 'Nested'; fields: { name: string; type: ClickHouseType }[] }
  // QBit vector type
  | { kind: 'QBit'; element: ClickHouseType; dimension: number }
  // Aggregate function state
  | { kind: 'AggregateFunction'; functionName: string; argTypes: ClickHouseType[] };

/**
 * Convert a ClickHouseType back to its string representation
 */
export function typeToString(type: ClickHouseType): string {
  switch (type.kind) {
    // Simple types
    case 'UInt8':
    case 'UInt16':
    case 'UInt32':
    case 'UInt64':
    case 'UInt128':
    case 'UInt256':
    case 'Int8':
    case 'Int16':
    case 'Int32':
    case 'Int64':
    case 'Int128':
    case 'Int256':
    case 'Float32':
    case 'Float64':
    case 'BFloat16':
    case 'String':
    case 'Date':
    case 'Date32':
    case 'Time':
    case 'UUID':
    case 'IPv4':
    case 'IPv6':
    case 'Bool':
    case 'Point':
    case 'Ring':
    case 'Polygon':
    case 'MultiPolygon':
    case 'LineString':
    case 'MultiLineString':
    case 'Geometry':
      return type.kind;

    // Parameterized types
    case 'FixedString':
      return `FixedString(${type.length})`;

    case 'Decimal32':
    case 'Decimal64':
    case 'Decimal128':
    case 'Decimal256':
      return `${type.kind}(${type.scale})`;

    case 'DateTime':
      return type.timezone ? `DateTime('${type.timezone}')` : 'DateTime';

    case 'DateTime64':
      return type.timezone
        ? `DateTime64(${type.precision}, '${type.timezone}')`
        : `DateTime64(${type.precision})`;

    case 'Time64':
      return `Time64(${type.precision})`;

    case 'Enum8':
    case 'Enum16': {
      const entries = Array.from(type.values.entries())
        .map(([val, name]) => `'${name}' = ${val}`)
        .join(', ');
      return `${type.kind}(${entries})`;
    }

    // Collections
    case 'Array':
      return `Array(${typeToString(type.element)})`;

    case 'Tuple': {
      if (type.names && type.names.length === type.elements.length) {
        const parts = type.elements.map((el, i) => `${type.names![i]} ${typeToString(el)}`);
        return `Tuple(${parts.join(', ')})`;
      }
      return `Tuple(${type.elements.map(typeToString).join(', ')})`;
    }

    case 'Map':
      return `Map(${typeToString(type.key)}, ${typeToString(type.value)})`;

    // Wrappers
    case 'Nullable':
      return `Nullable(${typeToString(type.inner)})`;

    case 'LowCardinality':
      return `LowCardinality(${typeToString(type.inner)})`;

    // Advanced
    case 'Variant':
      return `Variant(${type.variants.map(typeToString).join(', ')})`;

    case 'Dynamic':
      return type.maxTypes ? `Dynamic(${type.maxTypes})` : 'Dynamic';

    case 'JSON': {
      // Include params if present
      const params: string[] = [];
      if (type.maxDynamicPaths !== undefined) {
        params.push(`max_dynamic_paths=${type.maxDynamicPaths}`);
      }
      if (type.typedPaths) {
        for (const [path, pathType] of type.typedPaths) {
          params.push(`${path} ${typeToString(pathType)}`);
        }
      }
      return params.length > 0 ? `JSON(${params.join(', ')})` : 'JSON';
    }

    case 'Nested': {
      const fields = type.fields.map((f) => `${f.name} ${typeToString(f.type)}`);
      return `Nested(${fields.join(', ')})`;
    }

    case 'QBit':
      return `QBit(${typeToString(type.element)}, ${type.dimension})`;

    case 'AggregateFunction': {
      const args = type.argTypes.map(typeToString).join(', ');
      return args ? `AggregateFunction(${type.functionName}, ${args})` : `AggregateFunction(${type.functionName})`;
    }
  }
}

/**
 * Get the color CSS variable for a type
 */
export function getTypeColor(type: ClickHouseType): string {
  switch (type.kind) {
    case 'UInt8':
    case 'UInt16':
    case 'UInt32':
    case 'UInt64':
    case 'UInt128':
    case 'UInt256':
    case 'Int8':
    case 'Int16':
    case 'Int32':
    case 'Int64':
    case 'Int128':
    case 'Int256':
      return 'var(--type-int)';

    case 'Float32':
    case 'Float64':
    case 'BFloat16':
    case 'Decimal32':
    case 'Decimal64':
    case 'Decimal128':
    case 'Decimal256':
      return 'var(--type-float)';

    case 'String':
    case 'FixedString':
      return 'var(--type-string)';

    case 'Date':
    case 'Date32':
    case 'DateTime':
    case 'DateTime64':
    case 'Time':
    case 'Time64':
      return 'var(--type-date)';

    case 'Array':
      return 'var(--type-array)';

    case 'Tuple':
    case 'Nested':
      return 'var(--type-tuple)';

    case 'Map':
      return 'var(--type-map)';

    case 'Nullable':
      return 'var(--type-nullable)';

    case 'UUID':
    case 'IPv4':
    case 'IPv6':
      return 'var(--type-special)';

    case 'Enum8':
    case 'Enum16':
      return 'var(--type-enum)';

    case 'Bool':
      return 'var(--type-bool)';

    // Advanced types
    case 'Variant':
    case 'Dynamic':
    case 'JSON':
      return 'var(--type-special)';

    // Geo types
    case 'Point':
    case 'Ring':
    case 'Polygon':
    case 'MultiPolygon':
    case 'LineString':
    case 'MultiLineString':
    case 'Geometry':
      return 'var(--type-tuple)';

    // Vector types
    case 'LowCardinality':
    case 'QBit':
      return 'var(--type-array)';

    // Aggregate function state
    case 'AggregateFunction':
      return 'var(--type-special)';

    default:
      return 'var(--type-default)';
  }
}
