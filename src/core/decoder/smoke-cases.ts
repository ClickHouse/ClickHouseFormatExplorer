/**
 * Smoke test cases - verify parsing succeeds without value validation
 */
export interface SmokeTestCase {
  name: string;
  query: string;
  settings?: Record<string, string | number>;
  skipRowBinary?: boolean;
  skipNative?: boolean;
}

/**
 * All smoke test cases organized by category
 */
export const SMOKE_TEST_CASES: SmokeTestCase[] = [
  // ============================================================
  // INTEGER TYPES
  // ============================================================
  { name: 'UInt8 values', query: 'SELECT arrayJoin([0, 42, 255]::Array(UInt8)) as val' },
  { name: 'UInt16 values', query: 'SELECT arrayJoin([0, 1234, 65535]::Array(UInt16)) as val' },
  { name: 'UInt32 values', query: 'SELECT arrayJoin([0, 123456, 4294967295]::Array(UInt32)) as val' },
  { name: 'UInt64 values', query: 'SELECT arrayJoin([0, 9223372036854775807, 18446744073709551615]::Array(UInt64)) as val' },
  { name: 'UInt128 value', query: 'SELECT 170141183460469231731687303715884105727::UInt128 as val' },
  { name: 'UInt256 values', query: 'SELECT arrayJoin([0, 1]::Array(UInt256)) as val' },
  { name: 'Int8 values', query: 'SELECT arrayJoin([-128, 0, 127]::Array(Int8)) as val' },
  { name: 'Int16 values', query: 'SELECT arrayJoin([-32768, 0, 32767]::Array(Int16)) as val' },
  { name: 'Int32 values', query: 'SELECT arrayJoin([-2147483648, 0, 2147483647]::Array(Int32)) as val' },
  { name: 'Int64 values', query: 'SELECT arrayJoin([-9223372036854775808, 0, 9223372036854775807]::Array(Int64)) as val' },
  { name: 'Int128 negative', query: "SELECT toInt128('-123456789012345678901234567890') as val" },
  { name: 'Int256 negative', query: "SELECT toInt256('-12345678901234567890123456789012345678901234567890') as val" },

  // ============================================================
  // FLOATING POINT TYPES
  // ============================================================
  { name: 'Float32 values', query: 'SELECT arrayJoin([0.0, 3.14, -123.456]::Array(Float32)) as val' },
  { name: 'Float32 special', query: 'SELECT arrayJoin([inf, -inf, nan]::Array(Float32)) as val' },
  { name: 'Float64 values', query: 'SELECT arrayJoin([0.0, 3.141592653589793, -1e300]::Array(Float64)) as val' },
  { name: 'Float64 special', query: 'SELECT arrayJoin([inf, -inf, nan]::Array(Float64)) as val' },
  { name: 'BFloat16 values', query: 'SELECT arrayJoin([1.0, 2.0, 3.5]::Array(BFloat16)) as val' },
  { name: 'BFloat16 special', query: 'SELECT arrayJoin([inf, -inf, nan]::Array(BFloat16)) as val' },

  // ============================================================
  // STRING TYPES
  // ============================================================
  { name: 'String basic', query: "SELECT 'hello world'::String as val" },
  { name: 'String empty', query: "SELECT ''::String as val" },
  { name: 'String unicode', query: "SELECT '你好世界🎉'::String as val" },
  { name: 'String special chars', query: "SELECT 'line1\\nline2\\ttab'::String as val" },
  { name: 'FixedString basic', query: "SELECT 'abc'::FixedString(5) as val" },
  { name: 'FixedString exact', query: "SELECT 'hello'::FixedString(5) as val" },
  { name: 'FixedString unicode', query: "SELECT '中文'::FixedString(10) as val" },

  // ============================================================
  // BOOLEAN
  // ============================================================
  { name: 'Bool true', query: 'SELECT true::Bool as val' },
  { name: 'Bool false', query: 'SELECT false::Bool as val' },

  // ============================================================
  // DATE/TIME TYPES
  // ============================================================
  { name: 'Date', query: "SELECT toDate('2024-01-15') as val" },
  { name: 'Date32', query: "SELECT toDate32('2024-06-20') as val" },
  { name: 'Date32 historic', query: "SELECT toDate32('1900-01-01') as val" },
  { name: 'DateTime', query: "SELECT toDateTime('2024-01-15 12:30:00') as val" },
  { name: 'DateTime with tz', query: "SELECT toDateTime('2024-01-15 12:30:00', 'UTC') as val" },
  { name: 'DateTime64(3)', query: "SELECT toDateTime64('2024-01-15 12:30:00.123', 3) as val" },
  { name: 'DateTime64(6)', query: "SELECT toDateTime64('2024-01-15 12:30:00.123456', 6) as val" },
  { name: 'DateTime64(9)', query: "SELECT toDateTime64('2024-01-15 12:30:00.123456789', 9) as val" },
  { name: 'DateTime64 with tz', query: "SELECT toDateTime64('2024-01-15 12:30:00.123', 3, 'America/New_York') as val" },

  // ============================================================
  // INTERVAL TYPES
  // ============================================================
  { name: 'IntervalSecond', query: 'SELECT INTERVAL 45 SECOND as val' },
  { name: 'IntervalMinute', query: 'SELECT INTERVAL 30 MINUTE as val' },
  { name: 'IntervalHour', query: 'SELECT INTERVAL 12 HOUR as val' },
  { name: 'IntervalDay', query: 'SELECT INTERVAL 7 DAY as val' },
  { name: 'IntervalWeek', query: 'SELECT INTERVAL 2 WEEK as val' },
  { name: 'IntervalMonth', query: 'SELECT INTERVAL 3 MONTH as val' },
  { name: 'IntervalQuarter', query: 'SELECT INTERVAL 1 QUARTER as val' },
  { name: 'IntervalYear', query: 'SELECT INTERVAL 5 YEAR as val' },

  // ============================================================
  // SPECIAL TYPES
  // ============================================================
  { name: 'UUID', query: "SELECT toUUID('12345678-1234-5678-1234-567812345678') as val" },
  { name: 'IPv4', query: "SELECT toIPv4('192.168.1.1') as val" },
  { name: 'IPv6 loopback', query: "SELECT toIPv6('::1') as val" },
  { name: 'IPv6 full', query: "SELECT toIPv6('2001:db8::8a2e:370:7334') as val" },

  // ============================================================
  // DECIMAL TYPES
  // ============================================================
  { name: 'Decimal32', query: 'SELECT toDecimal32(123.45, 2) as val' },
  { name: 'Decimal64', query: 'SELECT toDecimal64(12345.6789, 4) as val' },
  { name: 'Decimal128', query: 'SELECT toDecimal128(123456789.123456789, 9) as val' },
  { name: 'Decimal256', query: 'SELECT toDecimal256(0, 20) as val' },

  // ============================================================
  // ENUM TYPES
  // ============================================================
  { name: 'Enum8', query: "SELECT arrayJoin(['hello', 'world']::Array(Enum8('hello' = 1, 'world' = 2))) as val" },
  { name: 'Enum16', query: "SELECT arrayJoin(['foo', 'bar']::Array(Enum16('foo' = 1, 'bar' = 1000))) as val" },

  // ============================================================
  // NULLABLE
  // ============================================================
  { name: 'Nullable non-null', query: 'SELECT 42::Nullable(UInt32) as val' },
  { name: 'Nullable null', query: 'SELECT NULL::Nullable(UInt32) as val' },
  { name: 'Nullable mixed', query: 'SELECT if(number % 2 = 0, number, NULL)::Nullable(UInt64) AS val FROM numbers(5)' },
  { name: 'Nullable String', query: "SELECT arrayJoin(['hello', NULL, 'world']::Array(Nullable(String))) as val" },

  // ============================================================
  // ARRAY
  // ============================================================
  { name: 'Array empty', query: 'SELECT []::Array(UInt32) as val' },
  { name: 'Array integers', query: 'SELECT [1, 2, 3]::Array(UInt32) as val' },
  { name: 'Array strings', query: "SELECT ['hello', 'world']::Array(String) as val" },
  { name: 'Array nested', query: 'SELECT [[1, 2], [3, 4, 5]]::Array(Array(UInt32)) as val' },
  { name: 'Array of Nullable', query: 'SELECT [1, NULL, 3]::Array(Nullable(UInt32)) as val' },
  { name: 'Array large', query: 'SELECT range(100)::Array(UInt32) as val' },
  { name: 'Array deeply nested', query: 'SELECT [[[1, 2], [3]], [[4, 5, 6]]]::Array(Array(Array(UInt8))) as val' },

  // ============================================================
  // TUPLE
  // ============================================================
  { name: 'Tuple simple', query: "SELECT (42, 'hello')::Tuple(UInt32, String) as val" },
  { name: 'Tuple named', query: "SELECT CAST((42, 'test'), 'Tuple(id UInt32, name String)') as val" },
  { name: 'Tuple nested', query: "SELECT ((1, 2), 'outer')::Tuple(Tuple(UInt8, UInt8), String) as val" },

  // ============================================================
  // MAP
  // ============================================================
  { name: 'Map empty', query: 'SELECT map()::Map(String, UInt32) as val' },
  { name: 'Map with entries', query: "SELECT map('a', 1, 'b', 2)::Map(String, UInt32) as val" },
  { name: 'Map integer keys', query: "SELECT map(1, 'one', 2, 'two')::Map(UInt32, String) as val" },
  { name: 'Map large', query: "SELECT map('k1', 1, 'k2', 2, 'k3', 3, 'k4', 4, 'k5', 5)::Map(String, UInt32) as val" },

  // ============================================================
  // LOWCARDINALITY
  // ============================================================
  { name: 'LowCardinality String', query: "SELECT 'hello'::LowCardinality(String) as val" },
  { name: 'LowCardinality repeated', query: 'SELECT toLowCardinality(toString(number % 3)) AS val FROM numbers(6)' },
  { name: 'LowCardinality Nullable', query: "SELECT arrayJoin(['a', NULL, 'b']::Array(LowCardinality(Nullable(String)))) as val" },

  // ============================================================
  // VARIANT
  // ============================================================
  { name: 'Variant String', query: "SELECT 'hello'::Variant(String, UInt64) as val", settings: { allow_experimental_variant_type: 1 } },
  { name: 'Variant UInt64', query: 'SELECT 42::Variant(String, UInt64) as val', settings: { allow_experimental_variant_type: 1 } },
  { name: 'Variant NULL', query: 'SELECT NULL::Variant(String, UInt64) as val', settings: { allow_experimental_variant_type: 1 } },
  { name: 'Variant multi-type', query: "SELECT arrayJoin(['hello'::Variant(String, UInt64, Float64), 42::Variant(String, UInt64, Float64), 3.14::Variant(String, UInt64, Float64)]) as val", settings: { allow_experimental_variant_type: 1 } },

  // ============================================================
  // DYNAMIC
  // ============================================================
  { name: 'Dynamic integer', query: 'SELECT 42::Dynamic as val' },
  { name: 'Dynamic string', query: "SELECT 'hello'::Dynamic as val" },
  { name: 'Dynamic NULL', query: 'SELECT NULL::Dynamic as val' },
  { name: 'Dynamic UInt8', query: 'SELECT 42::UInt8::Dynamic as val' },
  { name: 'Dynamic UInt16', query: 'SELECT 1000::UInt16::Dynamic as val' },
  { name: 'Dynamic UInt32', query: 'SELECT 100000::UInt32::Dynamic as val' },
  { name: 'Dynamic UInt64', query: 'SELECT 10000000000::UInt64::Dynamic as val' },
  { name: 'Dynamic Int8', query: 'SELECT toInt8(-42)::Dynamic as val' },
  { name: 'Dynamic Int16', query: 'SELECT toInt16(-1000)::Dynamic as val' },
  { name: 'Dynamic Int32', query: 'SELECT toInt32(-100000)::Dynamic as val' },
  { name: 'Dynamic Int64', query: 'SELECT toInt64(-10000000000)::Dynamic as val' },
  { name: 'Dynamic Float32', query: 'SELECT 3.14::Float32::Dynamic as val' },
  { name: 'Dynamic Float64', query: 'SELECT 3.141592653589793::Float64::Dynamic as val' },
  { name: 'Dynamic Date', query: "SELECT toDate('2024-01-15')::Dynamic as val" },
  { name: 'Dynamic DateTime64', query: "SELECT toDateTime64('2024-01-15 12:30:45.123', 3)::Dynamic as val" },
  { name: 'Dynamic UUID', query: "SELECT toUUID('550e8400-e29b-41d4-a716-446655440000')::Dynamic as val" },
  { name: 'Dynamic IPv4', query: "SELECT toIPv4('192.168.1.1')::Dynamic as val" },
  { name: 'Dynamic IPv6', query: "SELECT toIPv6('2001:db8::1')::Dynamic as val" },
  { name: 'Dynamic Bool', query: 'SELECT true::Bool::Dynamic as val' },
  { name: 'Dynamic Array', query: 'SELECT [1, 2, 3]::Array(UInt8)::Dynamic as val' },
  { name: 'Dynamic Tuple', query: "SELECT (1, 'test')::Tuple(UInt32, String)::Dynamic as val" },
  { name: 'Dynamic Map', query: "SELECT map('key', 'value')::Map(String, String)::Dynamic as val" },
  { name: 'Dynamic Decimal32', query: 'SELECT toDecimal32(123.45, 2)::Dynamic as val' },
  { name: 'Dynamic Decimal64', query: 'SELECT toDecimal64(12345.6789, 4)::Dynamic as val' },
  { name: 'Dynamic BFloat16', query: 'SELECT toBFloat16(1.5)::Dynamic as val' },
  { name: 'Dynamic Enum8', query: "SELECT CAST('active', 'Enum8(\\'active\\' = 1, \\'inactive\\' = 2)')::Dynamic as val" },
  { name: 'Dynamic Enum16', query: "SELECT CAST('pending', 'Enum16(\\'pending\\' = 100, \\'done\\' = 200)')::Dynamic as val" },

  // ============================================================
  // JSON
  // ============================================================
  { name: 'JSON simple', query: "SELECT '{\"name\": \"test\"}'::JSON as val", settings: { allow_experimental_json_type: 1 } },
  { name: 'JSON multiple fields', query: "SELECT '{\"name\": \"test\", \"value\": 42}'::JSON as val", settings: { allow_experimental_json_type: 1 } },
  { name: 'JSON typed paths', query: "SELECT '{\"id\": 42, \"name\": \"test\"}'::JSON(id UInt32, name String) as val", settings: { allow_experimental_json_type: 1 } },
  { name: 'JSON typed IPv4', query: "SELECT '{\"ip\": \"127.0.0.1\"}'::JSON(ip IPv4) as json_ipv4", settings: { allow_experimental_json_type: 1 } },
  { name: 'JSON nested', query: "SELECT '{\"id\": 1, \"nested\": {\"x\": 10, \"y\": 20}}'::JSON as val", settings: { allow_experimental_json_type: 1 } },
  { name: 'JSON with other columns', query: "SELECT '{\"name\": \"test\", \"value\": 42}'::JSON as json_col, 42::UInt8 as uint8_col, 'hello'::String as string_col", settings: { allow_experimental_json_type: 1 } },
  { name: 'JSON max_dynamic_paths', query: "SELECT '{\"a\": 1, \"b\": 2, \"c\": 3}'::JSON(max_dynamic_paths=2) AS col", settings: { allow_experimental_json_type: 1 } },

  // ============================================================
  // GEO TYPES
  // ============================================================
  { name: 'Point', query: 'SELECT (1.5, 2.5)::Point as val' },
  { name: 'Ring', query: 'SELECT [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]::Ring as val' },
  { name: 'Polygon', query: 'SELECT [[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]]::Polygon as val' },
  { name: 'MultiPolygon', query: 'SELECT [[[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]]]::MultiPolygon as val' },
  { name: 'LineString', query: 'SELECT [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]::LineString as val' },
  { name: 'MultiLineString', query: 'SELECT [[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]::MultiLineString as val' },
  { name: 'Geometry as Point', query: 'SELECT ((1.0, 2.0)::Point)::Geometry as val', settings: { allow_suspicious_variant_types: 1 } },

  // ============================================================
  // QBIT
  // ============================================================
  { name: 'QBit(Float32, 3)', query: 'SELECT [1.0, 2.0, 3.0]::QBit(Float32, 3) as val', settings: { allow_experimental_qbit_type: 1 } },
  { name: 'QBit(Float32, 5)', query: 'SELECT [1.0, 2.0, 3.0, 4.0, 5.0]::QBit(Float32, 5) as val', settings: { allow_experimental_qbit_type: 1 } },

  // ============================================================
  // AGGREGATE FUNCTION STATE
  // ============================================================
  { name: 'avgState', query: 'SELECT avgState(number) FROM numbers(10)' },
  { name: 'sumState', query: 'SELECT sumState(number) FROM numbers(10)' },
  { name: 'countState', query: 'SELECT countState() FROM numbers(10)' },
  { name: 'sumState Float64', query: 'SELECT sumState(toFloat64(number)) FROM numbers(10)' },
  { name: 'avgState Float64', query: 'SELECT avgState(toFloat64(number)) FROM numbers(10)' },

  // ============================================================
  // MULTIPLE COLUMNS AND ROWS
  // ============================================================
  { name: 'Multiple columns', query: "SELECT 42::UInt32 as int_col, 'hello'::String as str_col, true::Bool as bool_col, 3.14::Float64 as float_col" },
  { name: 'Many rows', query: 'SELECT number::UInt32 as val FROM numbers(100)' },
  { name: 'Multiple columns and rows', query: "SELECT number::UInt32 as id, concat('item_', toString(number))::String as name FROM numbers(10)" },

  // ============================================================
  // COMPLEX NESTED STRUCTURES
  // ============================================================
  { name: 'Deeply nested Tuple', query: "SELECT (1::UInt32, 'test'::String, [1, 2, 3]::Array(UInt8), map('key', (10, 'nested')::Tuple(id UInt32, name String)))::Tuple(id UInt32, name String, values Array(UInt8), metadata Map(String, Tuple(id UInt32, name String))) as val" },
  { name: '5-level nested arrays', query: 'SELECT [[[[[1, 2], [3]], [[4]]]]]::Array(Array(Array(Array(Array(UInt8))))) as val' },
  { name: 'Nested Tuples deep', query: "SELECT ((((1, 2), 3), 4), 5)::Tuple(Tuple(Tuple(Tuple(UInt8, UInt8), UInt8), UInt8), UInt8) as val" },

  // ============================================================
  // EDGE CASES - EXTREME VALUES
  // ============================================================
  { name: 'UInt256 max', query: "SELECT toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') as val" },
  { name: 'Int256 min', query: "SELECT toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') as val" },
  { name: 'Decimal256 scale 76', query: 'SELECT toDecimal256(0, 76) as val' },
  { name: 'Float64 subnormal', query: 'SELECT toFloat64(5e-324) as val' },

  // ============================================================
  // EDGE CASES - STRINGS
  // ============================================================
  { name: 'String with null byte', query: "SELECT 'hello\\0world'::String as val" },
  { name: 'String 4-byte UTF-8', query: "SELECT '𝕳𝖊𝖑𝖑𝖔'::String as val" },
  { name: 'String RTL', query: "SELECT 'مرحبا'::String as val" },
  { name: 'String zero-width', query: "SELECT 'a\\u200Bb'::String as val" },
  { name: 'String large 10KB', query: "SELECT repeat('x', 10000)::String as val" },

  // ============================================================
  // EDGE CASES - DATES
  // ============================================================
  { name: 'Date32 year 2299', query: "SELECT toDate32('2299-12-31') as val" },
  { name: 'Date32 year 1900', query: "SELECT toDate32('1900-01-01') as val" },
  { name: 'DateTime unusual tz', query: "SELECT toDateTime('2024-01-15 12:00:00', 'Pacific/Fiji') as val" },

  // ============================================================
  // EDGE CASES - EMPTY/NULL
  // ============================================================
  { name: 'Map with empty value', query: "SELECT map('key', '')::Map(String, String) as val" },
  { name: 'All NULL column', query: 'SELECT NULL::Nullable(UInt32) as val FROM numbers(5)' },
  { name: 'Array of empty arrays', query: 'SELECT [[], [], []]::Array(Array(UInt32)) as val' },
  { name: 'Empty Ring', query: 'SELECT []::Ring as val' },

  // ============================================================
  // EDGE CASES - LARGE DATA
  // ============================================================
  { name: '1000-element array', query: 'SELECT range(1000)::Array(UInt32) as val' },
  { name: '100-entry map', query: "SELECT arrayMap(x -> (toString(x), x), range(100))::Map(String, UInt64) as val" },
  { name: '500 rows', query: 'SELECT number::UInt64 as val FROM numbers(500)' },
  { name: '20 columns', query: 'SELECT number as c1, number as c2, number as c3, number as c4, number as c5, number as c6, number as c7, number as c8, number as c9, number as c10, number as c11, number as c12, number as c13, number as c14, number as c15, number as c16, number as c17, number as c18, number as c19, number as c20 FROM numbers(1)' },

  // ============================================================
  // EDGE CASES - ENUM
  // ============================================================
  { name: 'Enum8 negative', query: "SELECT CAST('neg', 'Enum8(\\'neg\\' = -128, \\'pos\\' = 127)') as val" },
  { name: 'Enum special chars', query: "SELECT CAST('hello world', 'Enum8(\\'hello world\\' = 1)') as val" },

  // ============================================================
  // EDGE CASES - IP
  // ============================================================
  { name: 'IPv6 full notation', query: "SELECT toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334') as val" },
  { name: 'IPv6 all ones', query: "SELECT toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff') as val" },

  // ============================================================
  // LOWCARDINALITY EDGE CASES
  // ============================================================
  { name: 'LowCardinality all unique', query: 'SELECT toLowCardinality(toString(number)) AS val FROM numbers(100)' },
  { name: 'LowCardinality single repeated', query: "SELECT toLowCardinality('same') AS val FROM numbers(100)" },
];
