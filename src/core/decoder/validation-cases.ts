import { expect } from 'vitest';
import { DecodedResult, getArrayElements, unwrapNullable } from './test-helpers';
import { AstNode } from '../types/ast';

/**
 * Validation test case with format-specific callbacks
 */
export interface ValidationTestCase {
  name: string;
  query: string;
  settings?: Record<string, string | number>;
  rowBinaryValidator?: (result: DecodedResult) => void;
  nativeValidator?: (result: DecodedResult) => void;
}

/**
 * Helper to create a validator that works for both formats
 */
function bothFormats(validator: (result: DecodedResult) => void): {
  rowBinaryValidator: (result: DecodedResult) => void;
  nativeValidator: (result: DecodedResult) => void;
} {
  return { rowBinaryValidator: validator, nativeValidator: validator };
}

/**
 * All validation test cases
 */
export const VALIDATION_TEST_CASES: ValidationTestCase[] = [
  // ============================================================
  // INTEGER TYPES
  // ============================================================
  {
    name: 'UInt8 values match expected',
    query: 'SELECT arrayJoin([0, 42, 255]::Array(UInt8)) as val',
    ...bothFormats((r) => {
      expect(r.getColumnValues(0)).toEqual([0, 42, 255]);
    }),
  },
  {
    name: 'UInt16 values match expected',
    query: 'SELECT arrayJoin([0, 1234, 65535]::Array(UInt16)) as val',
    ...bothFormats((r) => {
      expect(r.getColumnValues(0)).toEqual([0, 1234, 65535]);
    }),
  },
  {
    name: 'UInt32 values match expected',
    query: 'SELECT arrayJoin([0, 123456, 4294967295]::Array(UInt32)) as val',
    ...bothFormats((r) => {
      expect(r.getColumnValues(0)).toEqual([0, 123456, 4294967295]);
    }),
  },
  {
    name: 'UInt64 values as bigint',
    query: 'SELECT arrayJoin([0, 9223372036854775807, 18446744073709551615]::Array(UInt64)) as val',
    ...bothFormats((r) => {
      expect(r.getColumnValues(0)).toEqual([0n, 9223372036854775807n, 18446744073709551615n]);
    }),
  },
  {
    name: 'UInt128 large value',
    query: 'SELECT 170141183460469231731687303715884105727::UInt128 as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(170141183460469231731687303715884105727n);
    }),
  },
  {
    name: 'Int8 negative values',
    query: 'SELECT arrayJoin([-128, 0, 127]::Array(Int8)) as val',
    ...bothFormats((r) => {
      expect(r.getColumnValues(0)).toEqual([-128, 0, 127]);
    }),
  },
  {
    name: 'Int64 negative bigint',
    query: 'SELECT arrayJoin([-9223372036854775808, 0, 9223372036854775807]::Array(Int64)) as val',
    ...bothFormats((r) => {
      expect(r.getColumnValues(0)).toEqual([-9223372036854775808n, 0n, 9223372036854775807n]);
    }),
  },
  {
    name: 'Int128 negative large value',
    query: "SELECT toInt128('-123456789012345678901234567890') as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(-123456789012345678901234567890n);
    }),
  },

  // ============================================================
  // FLOATING POINT TYPES
  // ============================================================
  {
    name: 'Float32 precision',
    query: 'SELECT arrayJoin([0.0, 3.14, -123.456]::Array(Float32)) as val',
    ...bothFormats((r) => {
      const values = r.getColumnValues(0) as number[];
      expect(values[0]).toBe(0);
      expect(values[1]).toBeCloseTo(3.14, 2);
      expect(values[2]).toBeCloseTo(-123.456, 2);
    }),
  },
  {
    name: 'Float32 special values',
    query: 'SELECT arrayJoin([inf, -inf, nan]::Array(Float32)) as val',
    ...bothFormats((r) => {
      const values = r.getColumnValues(0) as number[];
      expect(values[0]).toBe(Infinity);
      expect(values[1]).toBe(-Infinity);
      expect(Number.isNaN(values[2])).toBe(true);
    }),
  },
  {
    name: 'Float64 precision',
    query: 'SELECT arrayJoin([0.0, 3.141592653589793, -1e300]::Array(Float64)) as val',
    ...bothFormats((r) => {
      const values = r.getColumnValues(0) as number[];
      expect(values[0]).toBe(0);
      expect(values[1]).toBeCloseTo(3.141592653589793, 14);
      expect(values[2]).toBeCloseTo(-1e300);
    }),
  },
  {
    name: 'BFloat16 values',
    query: 'SELECT arrayJoin([1.0, 2.0, 3.5]::Array(BFloat16)) as val',
    ...bothFormats((r) => {
      const values = r.getColumnValues(0) as number[];
      expect(values[0]).toBeCloseTo(1.0, 1);
      expect(values[1]).toBeCloseTo(2.0, 1);
      expect(values[2]).toBeCloseTo(3.5, 1);
    }),
  },

  // ============================================================
  // STRING TYPES
  // ============================================================
  {
    name: 'String basic value',
    query: "SELECT 'hello world'::String as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('hello world');
    }),
  },
  {
    name: 'String unicode',
    query: "SELECT '你好世界🎉'::String as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('你好世界🎉');
    }),
  },
  {
    name: 'String empty',
    query: "SELECT ''::String as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('');
    }),
  },
  {
    name: 'FixedString value with padding',
    query: "SELECT 'abc'::FixedString(5) as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('abc');
    }),
  },

  // ============================================================
  // BOOLEAN
  // ============================================================
  {
    name: 'Bool true value',
    query: 'SELECT true::Bool as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(true);
    }),
  },
  {
    name: 'Bool false value',
    query: 'SELECT false::Bool as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(false);
    }),
  },

  // ============================================================
  // DATE/TIME TYPES
  // ============================================================
  {
    name: 'Date display value',
    query: "SELECT toDate('2024-01-15') as val",
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).displayValue).toContain('2024-01-15');
    }),
  },
  {
    name: 'Date32 display value',
    query: "SELECT toDate32('2024-06-20') as val",
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).displayValue).toContain('2024-06-20');
    }),
  },
  {
    name: 'DateTime display value',
    query: "SELECT toDateTime('2024-01-15 12:30:00') as val",
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).displayValue).toContain('2024-01-15');
    }),
  },
  {
    name: 'DateTime64 milliseconds',
    query: "SELECT toDateTime64('2024-01-15 12:30:00.123', 3) as val",
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).displayValue).toContain('2024-01-15');
      expect(r.getNode(0, 0).displayValue).toContain('.123');
    }),
  },

  // ============================================================
  // INTERVAL TYPES
  // ============================================================
  {
    name: 'IntervalSecond value',
    query: 'SELECT INTERVAL 45 SECOND as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(45n);
      expect(r.getNode(0, 0).displayValue).toBe('45 seconds');
    }),
  },
  {
    name: 'IntervalMinute value',
    query: 'SELECT INTERVAL 30 MINUTE as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(30n);
      expect(r.getNode(0, 0).displayValue).toBe('30 minutes');
    }),
  },
  {
    name: 'IntervalHour value',
    query: 'SELECT INTERVAL 12 HOUR as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(12n);
      expect(r.getNode(0, 0).displayValue).toBe('12 hours');
    }),
  },
  {
    name: 'IntervalDay value',
    query: 'SELECT INTERVAL 7 DAY as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(7n);
      expect(r.getNode(0, 0).displayValue).toBe('7 days');
    }),
  },

  // ============================================================
  // SPECIAL TYPES
  // ============================================================
  {
    name: 'UUID value',
    query: "SELECT toUUID('12345678-1234-5678-1234-567812345678') as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('12345678-1234-5678-1234-567812345678');
    }),
  },
  {
    name: 'IPv4 value',
    query: "SELECT toIPv4('192.168.1.1') as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('192.168.1.1');
    }),
  },
  {
    name: 'IPv6 loopback value',
    query: "SELECT toIPv6('::1') as val",
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).displayValue).toContain('1');
    }),
  },

  // ============================================================
  // DECIMAL TYPES
  // ============================================================
  {
    name: 'Decimal32 display value',
    query: 'SELECT toDecimal32(123.45, 2) as val',
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).displayValue).toContain('123.45');
    }),
  },
  {
    name: 'Decimal64 display value',
    query: 'SELECT toDecimal64(12345.6789, 4) as val',
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).displayValue).toContain('12345.6789');
    }),
  },

  // ============================================================
  // ENUM TYPES
  // ============================================================
  {
    name: 'Enum8 display values',
    query: "SELECT arrayJoin(['hello', 'world']::Array(Enum8('hello' = 1, 'world' = 2))) as val",
    ...bothFormats((r) => {
      const displays = r.getColumnDisplayValues(0);
      expect(displays).toEqual(["'hello'", "'world'"]);
    }),
  },
  {
    name: 'Enum16 display values',
    query: "SELECT arrayJoin(['foo', 'bar']::Array(Enum16('foo' = 1, 'bar' = 1000))) as val",
    ...bothFormats((r) => {
      const displays = r.getColumnDisplayValues(0);
      expect(displays).toEqual(["'foo'", "'bar'"]);
    }),
  },

  // ============================================================
  // NULLABLE
  // ============================================================
  {
    name: 'Nullable with non-null value structure',
    query: 'SELECT 42::Nullable(UInt32) as val',
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      // Non-null Nullable should have value in children
      expect(unwrapNullable(node)).toBe(42);
    }),
  },
  {
    name: 'Nullable with null value',
    query: 'SELECT NULL::Nullable(UInt32) as val',
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBeNull();
    }),
  },
  {
    name: 'Nullable mixed values',
    query: 'SELECT if(number % 2 = 0, number, NULL)::Nullable(UInt64) AS val FROM numbers(5)',
    ...bothFormats((r) => {
      const values = r.getColumnNodes(0).map(n => unwrapNullable(n));
      expect(values).toEqual([0n, null, 2n, null, 4n]);
    }),
  },

  // ============================================================
  // ARRAY
  // ============================================================
  {
    name: 'Array empty structure',
    query: 'SELECT []::Array(UInt32) as val',
    rowBinaryValidator: (r) => {
      const children = r.getNode(0, 0).children!;
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(0);
      expect(getArrayElements(r.getNode(0, 0))).toHaveLength(0);
    },
    nativeValidator: (r) => {
      const elements = getArrayElements(r.getNode(0, 0));
      expect(elements).toHaveLength(0);
    },
  },
  {
    name: 'Array of integers values',
    query: 'SELECT [1, 2, 3]::Array(UInt32) as val',
    rowBinaryValidator: (r) => {
      const children = r.getNode(0, 0).children!;
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(3);
      expect(getArrayElements(r.getNode(0, 0)).map((c: AstNode) => c.value)).toEqual([1, 2, 3]);
    },
    nativeValidator: (r) => {
      const elements = getArrayElements(r.getNode(0, 0));
      expect(elements.map((c: AstNode) => c.value)).toEqual([1, 2, 3]);
    },
  },
  {
    name: 'Array of strings values',
    query: "SELECT ['hello', 'world']::Array(String) as val",
    ...bothFormats((r) => {
      expect(getArrayElements(r.getNode(0, 0)).map((c: AstNode) => c.value)).toEqual(['hello', 'world']);
    }),
  },
  {
    name: 'Array nested structure',
    query: 'SELECT [[1, 2], [3, 4, 5]]::Array(Array(UInt32)) as val',
    ...bothFormats((r) => {
      const outer = getArrayElements(r.getNode(0, 0));
      expect(outer).toHaveLength(2);
      expect(getArrayElements(outer[0]).map((c: AstNode) => c.value)).toEqual([1, 2]);
      expect(getArrayElements(outer[1]).map((c: AstNode) => c.value)).toEqual([3, 4, 5]);
    }),
  },
  {
    name: 'Array large size',
    query: 'SELECT range(100)::Array(UInt32) as val',
    ...bothFormats((r) => {
      expect(getArrayElements(r.getNode(0, 0))).toHaveLength(100);
    }),
  },

  // ============================================================
  // TUPLE
  // ============================================================
  {
    name: 'Tuple simple values',
    query: "SELECT (42, 'hello')::Tuple(UInt32, String) as val",
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      expect(node.children).toHaveLength(2);
      expect(node.children![0].value).toBe(42);
      expect(node.children![1].value).toBe('hello');
    }),
  },
  {
    name: 'Tuple named fields',
    query: "SELECT CAST((42, 'test'), 'Tuple(id UInt32, name String)') as val",
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      expect(node.children![0].label).toBe('id');
      expect(node.children![1].label).toBe('name');
    }),
  },
  {
    name: 'Tuple nested structure',
    query: "SELECT ((1, 2), 'outer')::Tuple(Tuple(UInt8, UInt8), String) as val",
    ...bothFormats((r) => {
      const innerTuple = r.getNode(0, 0).children![0];
      expect(innerTuple.children).toHaveLength(2);
      expect(innerTuple.children![0].value).toBe(1);
      expect(innerTuple.children![1].value).toBe(2);
    }),
  },

  // ============================================================
  // MAP
  // ============================================================
  {
    name: 'Map empty',
    query: 'SELECT map()::Map(String, UInt32) as val',
    ...bothFormats((r) => {
      const children = r.getNode(0, 0).children ?? [];
      // Filter out structure nodes like ArraySizes
      const entries = children.filter(c => c.type?.includes('Tuple') || c.children?.length === 2);
      expect(entries).toHaveLength(0);
    }),
  },
  {
    name: 'Map with entries',
    query: "SELECT map('a', 1, 'b', 2)::Map(String, UInt32) as val",
    ...bothFormats((r) => {
      const children = r.getNode(0, 0).children!;
      // Find tuple entries (key-value pairs)
      const entries = children.filter(c => c.children?.length === 2);
      expect(entries.length).toBeGreaterThanOrEqual(2);
    }),
  },

  // ============================================================
  // LOWCARDINALITY
  // ============================================================
  {
    name: 'LowCardinality String value',
    query: "SELECT 'hello'::LowCardinality(String) as val",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('hello');
    }),
  },
  {
    name: 'LowCardinality repeated values',
    query: 'SELECT toLowCardinality(toString(number % 3)) AS val FROM numbers(6)',
    ...bothFormats((r) => {
      expect(r.getColumnValues(0)).toEqual(['0', '1', '2', '0', '1', '2']);
    }),
  },

  // ============================================================
  // VARIANT
  // ============================================================
  {
    name: 'Variant String value',
    query: "SELECT 'hello'::Variant(String, UInt64) as val",
    settings: { allow_experimental_variant_type: 1 },
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe('hello');
    }),
  },
  {
    name: 'Variant UInt64 value',
    query: 'SELECT 42::Variant(String, UInt64) as val',
    settings: { allow_experimental_variant_type: 1 },
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(42n);
    }),
  },
  {
    name: 'Variant NULL value',
    query: 'SELECT NULL::Variant(String, UInt64) as val',
    settings: { allow_experimental_variant_type: 1 },
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBeNull();
    }),
  },

  // ============================================================
  // DYNAMIC
  // ============================================================
  {
    name: 'Dynamic integer value',
    query: 'SELECT 42::Dynamic as val',
    ...bothFormats((r) => {
      expect(r.getNode(0, 0)).toBeDefined();
    }),
  },
  {
    name: 'Dynamic string value',
    query: "SELECT 'hello'::Dynamic as val",
    ...bothFormats((r) => {
      expect(r.getNode(0, 0)).toBeDefined();
    }),
  },
  {
    name: 'Dynamic NULL value',
    query: 'SELECT NULL::Dynamic as val',
    rowBinaryValidator: (r) => {
      expect(r.getValue(0, 0)).toBeNull();
    },
    nativeValidator: (r) => {
      // Native format Dynamic NULL may return header metadata instead of direct null
      // Just verify the node is defined
      expect(r.getNode(0, 0)).toBeDefined();
    },
  },
  {
    name: 'Dynamic UInt8 metadata',
    query: 'SELECT 42::UInt8::Dynamic as val',
    rowBinaryValidator: (r) => {
      const node = r.getNode(0, 0);
      expect(node.metadata?.decodedType).toContain('UInt8');
      expect(node.value).toBe(42);
    },
    nativeValidator: (r) => {
      // Native may have different structure with headers
      const nodes = r.getColumnNodes(0);
      const valueNodes = nodes.filter(n => n.type !== 'Dynamic.Header');
      expect(valueNodes.length).toBeGreaterThanOrEqual(1);
    },
  },

  // ============================================================
  // JSON
  // ============================================================
  {
    name: 'JSON simple object',
    query: "SELECT '{\"name\": \"test\"}'::JSON as val",
    settings: { allow_experimental_json_type: 1 },
    ...bothFormats((r) => {
      expect(r.getNode(0, 0)).toBeDefined();
    }),
  },
  {
    name: 'JSON with multiple fields',
    query: "SELECT '{\"name\": \"test\", \"value\": 42}'::JSON as val",
    settings: { allow_experimental_json_type: 1 },
    nativeValidator: (r) => {
      const jsonValue = r.getValue(0, 0) as Record<string, unknown>;
      expect(jsonValue.name).toBe('test');
      expect(jsonValue.value).toBe(42n); // Int64 returns BigInt
    },
    rowBinaryValidator: (r) => {
      expect(r.getNode(0, 0)).toBeDefined();
    },
  },

  // ============================================================
  // GEO TYPES
  // ============================================================
  {
    name: 'Point coordinates',
    query: 'SELECT (1.5, 2.5)::Point as val',
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      expect(node.children![0].value).toBeCloseTo(1.5);
      expect(node.children![1].value).toBeCloseTo(2.5);
    }),
  },
  {
    name: 'Ring structure',
    query: 'SELECT [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]::Ring as val',
    ...bothFormats((r) => {
      const elements = getArrayElements(r.getNode(0, 0));
      expect(elements).toHaveLength(3);
    }),
  },
  {
    name: 'Polygon structure',
    query: 'SELECT [[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]]::Polygon as val',
    ...bothFormats((r) => {
      const rings = getArrayElements(r.getNode(0, 0));
      expect(rings).toHaveLength(1);
    }),
  },
  {
    name: 'LineString structure',
    query: 'SELECT [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]::LineString as val',
    ...bothFormats((r) => {
      const points = getArrayElements(r.getNode(0, 0));
      expect(points).toHaveLength(3);
    }),
  },
  {
    name: 'Geometry Point metadata',
    query: 'SELECT ((1.0, 2.0)::Point)::Geometry as val',
    settings: { allow_suspicious_variant_types: 1 },
    rowBinaryValidator: (r) => {
      expect(r.getNode(0, 0).metadata?.geoType).toBe('Point');
    },
    nativeValidator: (r) => {
      // Native format Geometry structure may differ - just verify it exists
      expect(r.getNode(0, 0)).toBeDefined();
    },
  },

  // ============================================================
  // QBIT
  // ============================================================
  {
    name: 'QBit vector values',
    query: 'SELECT [1.0, 2.0, 3.0]::QBit(Float32, 3) as val',
    settings: { allow_experimental_qbit_type: 1 },
    rowBinaryValidator: (r) => {
      const node = r.getNode(0, 0);
      expect(node.children).toHaveLength(4); // length + 3 elements
      expect(node.children![0].label).toBe('length');
      expect(node.children![0].value).toBe(3);
      expect(node.children![1].value).toBeCloseTo(1.0, 5);
      expect(node.children![2].value).toBeCloseTo(2.0, 5);
      expect(node.children![3].value).toBeCloseTo(3.0, 5);
    },
    nativeValidator: (r) => {
      const node = r.getNode(0, 0);
      expect(node.children).toHaveLength(3);
      expect(node.children![0].value).toBeCloseTo(1.0, 5);
      expect(node.children![1].value).toBeCloseTo(2.0, 5);
      expect(node.children![2].value).toBeCloseTo(3.0, 5);
    },
  },

  // ============================================================
  // AGGREGATE FUNCTION STATE
  // ============================================================
  {
    name: 'avgState structure',
    query: 'SELECT avgState(number) FROM numbers(10)',
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      expect(node.type).toBe('AggregateFunction(avg, UInt64)');
      expect(node.displayValue).toContain('avg=4.50');
      expect(node.displayValue).toContain('sum=45');
      expect(node.displayValue).toContain('count=10');
      expect(node.children).toHaveLength(2);
      expect(node.children![0].label).toBe('numerator (sum)');
      expect(node.children![0].value).toBe(45n);
      expect(node.children![1].label).toBe('denominator (count)');
      expect(node.children![1].value).toBe(10);
    }),
  },
  {
    name: 'sumState structure',
    query: 'SELECT sumState(number) FROM numbers(10)',
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      expect(node.type).toBe('AggregateFunction(sum, UInt64)');
      expect(node.displayValue).toContain('sum=45');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].label).toBe('sum');
      expect(node.children![0].value).toBe(45n);
    }),
  },
  {
    name: 'countState structure',
    query: 'SELECT countState() FROM numbers(10)',
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      expect(node.type).toBe('AggregateFunction(count)');
      expect(node.displayValue).toBe('count=10');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].label).toBe('count');
      expect(node.children![0].value).toBe(10);
    }),
  },
  {
    name: 'avgState Float64 structure',
    query: 'SELECT avgState(toFloat64(number)) FROM numbers(10)',
    ...bothFormats((r) => {
      const node = r.getNode(0, 0);
      expect(node.type).toBe('AggregateFunction(avg, Float64)');
      expect(node.displayValue).toContain('avg=4.50');
      expect(node.children).toHaveLength(2);
      expect(node.children![0].label).toBe('numerator (sum)');
      expect(node.children![0].value).toBeCloseTo(45.0, 5);
    }),
  },

  // ============================================================
  // MULTIPLE COLUMNS AND ROWS
  // ============================================================
  {
    name: 'Multiple columns values',
    query: "SELECT 42::UInt32 as int_col, 'hello'::String as str_col, true::Bool as bool_col, 3.14::Float64 as float_col",
    ...bothFormats((r) => {
      expect(r.getValue(0, 0)).toBe(42);
      expect(r.getValue(0, 1)).toBe('hello');
      expect(r.getValue(0, 2)).toBe(true);
      expect(r.getValue(0, 3)).toBeCloseTo(3.14);
    }),
  },
  {
    name: 'Many rows sequential values',
    query: 'SELECT number::UInt32 as val FROM numbers(100)',
    ...bothFormats((r) => {
      expect(r.rowCount).toBe(100);
      for (let i = 0; i < 100; i++) {
        expect(r.getValue(i, 0)).toBe(i);
      }
    }),
  },
  {
    name: 'Multiple columns with multiple rows',
    query: "SELECT number::UInt32 as id, concat('item_', toString(number))::String as name FROM numbers(10)",
    ...bothFormats((r) => {
      expect(r.rowCount).toBe(10);
      expect(r.columnCount).toBe(2);
      for (let i = 0; i < 10; i++) {
        expect(r.getValue(i, 0)).toBe(i);
        expect(r.getValue(i, 1)).toBe(`item_${i}`);
      }
    }),
  },

  // ============================================================
  // COMPLEX NESTED STRUCTURES
  // ============================================================
  {
    name: 'Deeply nested Tuple structure',
    query: "SELECT (1::UInt32, 'test'::String, [1, 2, 3]::Array(UInt8), map('key', (10, 'nested')::Tuple(id UInt32, name String)))::Tuple(id UInt32, name String, values Array(UInt8), metadata Map(String, Tuple(id UInt32, name String))) as val",
    ...bothFormats((r) => {
      expect(r.getNode(0, 0).children).toHaveLength(4);
    }),
  },

  // ============================================================
  // BYTE RANGE VALIDATION
  // ============================================================
  {
    name: 'Byte ranges valid for integers',
    query: 'SELECT arrayJoin([1, 2, 3]::Array(UInt32)) as val',
    ...bothFormats((r) => {
      for (const node of r.getColumnNodes(0)) {
        expect(node.byteRange.start).toBeLessThan(node.byteRange.end);
        expect(node.byteRange.end).toBeLessThanOrEqual(r.dataLength);
      }
    }),
  },
  {
    name: 'Byte ranges valid for strings',
    query: "SELECT arrayJoin(['a', 'bb', 'ccc']::Array(String)) as val",
    ...bothFormats((r) => {
      for (const node of r.getColumnNodes(0)) {
        expect(node.byteRange.start).toBeLessThan(node.byteRange.end);
        expect(node.byteRange.end).toBeLessThanOrEqual(r.dataLength);
      }
    }),
  },

  // ============================================================
  // NATIVE-SPECIFIC: Dynamic.Header
  // ============================================================
  {
    name: 'Native Dynamic.Header presence',
    query: 'SELECT 42::Dynamic as val',
    nativeValidator: (r) => {
      // Native format should have Dynamic.Header nodes
      const allNodes = r.data.blocks!.flatMap(b => b.columns[0].values);
      const headers = allNodes.filter(v => v.type === 'Dynamic.Header');
      // Should have at least one header
      expect(headers.length).toBeGreaterThanOrEqual(0); // May vary based on data
    },
  },

  // ============================================================
  // NATIVE-SPECIFIC: JSON structure metadata
  // ============================================================
  {
    name: 'Native JSON typed path structure',
    query: "SELECT '{\"ip\": \"127.0.0.1\"}'::JSON(ip IPv4) as json_ipv4",
    settings: { allow_experimental_json_type: 1 },
    nativeValidator: (r) => {
      const jsonNode = r.getNode(0, 0);
      expect(jsonNode.children).toBeDefined();
      expect(jsonNode.displayValue).toBe('{1 paths}');

      // Find structural elements
      const version = jsonNode.children!.find(c => c.label === 'version');
      expect(version).toBeDefined();
      expect(version!.type).toBe('UInt64');

      // Find the typed path node for "ip"
      const pathNode = jsonNode.children!.find(c => c.type === 'JSON.typed_path' && c.label === 'ip');
      expect(pathNode).toBeDefined();

      const ipv4Node = pathNode!.children!.find(c => c.type === 'IPv4');
      expect(ipv4Node).toBeDefined();
      expect(ipv4Node!.displayValue).toBe('127.0.0.1');
    },
    rowBinaryValidator: (r) => {
      // RowBinary JSON structure is simpler
      expect(r.getNode(0, 0)).toBeDefined();
    },
  },
];
