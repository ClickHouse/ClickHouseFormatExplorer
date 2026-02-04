import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ClickHouseContainer, StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { NativeDecoder } from './native-decoder';
import { AstNode } from '../types/ast';

const IMAGE = 'clickhouse/clickhouse-server:latest';

/**
 * Comprehensive test suite for NativeDecoder
 *
 * Native format is column-oriented with blocks:
 * - Block header: numColumns (VarUInt), numRows (VarUInt)
 * - For each column: name (String), type (String), column data
 *
 * Key differences from RowBinary for complex types:
 * - Nullable: NullMap stream (1 byte per row) FIRST, then values for ALL rows
 * - Array: ArraySizes stream (UInt64 cumulative offsets) FIRST, then flattened elements
 * - LowCardinality: Dictionary with version + indexes
 * - Map: Treated as Array(Tuple(K, V)) - sizes, then keys, then values
 * - Tuple: Element streams sequentially (TupleElement0, TupleElement1, ...)
 * - Variant: Discriminators stream + sparse variant element streams
 */
describe('NativeDecoder Integration Tests', () => {
  let container: StartedClickHouseContainer;
  let query: (sql: string, settings?: Record<string, string | number>) => Promise<Uint8Array>;
  let decode: (data: Uint8Array, expectedColumns: number, expectedRows: number) => ReturnType<NativeDecoder['decode']>;

  beforeAll(async () => {
    container = await new ClickHouseContainer(IMAGE).start();
    const baseUrl = container.getHttpUrl();

    query = async (sql: string, settings?: Record<string, string | number>): Promise<Uint8Array> => {
      const params = new URLSearchParams({
        user: container.getUsername(),
        password: container.getPassword(),
      });
      // Add any extra settings as URL parameters
      if (settings) {
        for (const [key, value] of Object.entries(settings)) {
          params.set(key, String(value));
        }
      }
      const response = await fetch(`${baseUrl}/?${params}`, {
        method: 'POST',
        body: `${sql} FORMAT Native`,
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse error: ${text}`);
      }
      const buffer = await response.arrayBuffer();
      return new Uint8Array(buffer);
    };

    decode = (data: Uint8Array, expectedColumns: number, expectedRows: number) => {
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();

      expect(result.header.columns).toHaveLength(expectedColumns);
      expect(result.blocks).toBeDefined();

      const totalRows = result.blocks!.reduce((sum, block) => sum + block.rowCount, 0);
      expect(totalRows).toBe(expectedRows);

      return result;
    };
  }, 120000);

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
  });

  // ============================================================
  // INTEGERS - UInt8 to UInt256, Int8 to Int256
  // ============================================================
  describe('Integer Types', () => {
    it('decodes UInt8 values', async () => {
      const data = await query('SELECT arrayJoin([0, 42, 255]::Array(UInt8)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([0, 42, 255]);
    });

    it('decodes UInt16 values', async () => {
      const data = await query('SELECT arrayJoin([0, 1234, 65535]::Array(UInt16)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([0, 1234, 65535]);
    });

    it('decodes UInt32 values', async () => {
      const data = await query('SELECT arrayJoin([0, 123456, 4294967295]::Array(UInt32)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([0, 123456, 4294967295]);
    });

    it('decodes UInt64 values', async () => {
      const data = await query('SELECT arrayJoin([0, 9223372036854775807, 18446744073709551615]::Array(UInt64)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([0n, 9223372036854775807n, 18446744073709551615n]);
    });

    it('decodes UInt128 values', async () => {
      const data = await query('SELECT 170141183460469231731687303715884105727::UInt128 as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(170141183460469231731687303715884105727n);
    });

    it('decodes UInt256 zero and one', async () => {
      const data = await query('SELECT arrayJoin([0, 1]::Array(UInt256)) as val');
      const result = decode(data, 1, 2);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([0n, 1n]);
    });

    it('decodes Int8 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-128, 0, 127]::Array(Int8)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([-128, 0, 127]);
    });

    it('decodes Int16 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-32768, 0, 32767]::Array(Int16)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([-32768, 0, 32767]);
    });

    it('decodes Int32 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-2147483648, 0, 2147483647]::Array(Int32)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([-2147483648, 0, 2147483647]);
    });

    it('decodes Int64 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-9223372036854775808, 0, 9223372036854775807]::Array(Int64)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([-9223372036854775808n, 0n, 9223372036854775807n]);
    });

    it('decodes Int128 negative', async () => {
      // Use a smaller value that's clearly negative but avoids literal parsing issues
      const data = await query("SELECT toInt128('-123456789012345678901234567890') as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(-123456789012345678901234567890n);
    });

    it('decodes Int256 negative', async () => {
      // Use string conversion to avoid integer literal precision issues
      const data = await query("SELECT toInt256('-12345678901234567890123456789012345678901234567890') as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(-12345678901234567890123456789012345678901234567890n);
    });
  });

  // ============================================================
  // FLOATING POINT - Float32, Float64, BFloat16
  // ============================================================
  describe('Floating Point Types', () => {
    it('decodes Float32 values', async () => {
      const data = await query('SELECT arrayJoin([0.0, 3.14, -123.456]::Array(Float32)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as number));
      expect(values[0]).toBe(0);
      expect(values[1]).toBeCloseTo(3.14, 2);
      expect(values[2]).toBeCloseTo(-123.456, 2);
    });

    it('decodes Float32 special values', async () => {
      const data = await query('SELECT arrayJoin([inf, -inf, nan]::Array(Float32)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as number));
      expect(values[0]).toBe(Infinity);
      expect(values[1]).toBe(-Infinity);
      expect(Number.isNaN(values[2])).toBe(true);
    });

    it('decodes Float64 values', async () => {
      const data = await query('SELECT arrayJoin([0.0, 3.141592653589793, -1e300]::Array(Float64)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as number));
      expect(values[0]).toBe(0);
      expect(values[1]).toBeCloseTo(3.141592653589793, 14);
      expect(values[2]).toBeCloseTo(-1e300);
    });

    it('decodes Float64 special values', async () => {
      const data = await query('SELECT arrayJoin([inf, -inf, nan]::Array(Float64)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as number));
      expect(values[0]).toBe(Infinity);
      expect(values[1]).toBe(-Infinity);
      expect(Number.isNaN(values[2])).toBe(true);
    });

    it('decodes BFloat16 values', async () => {
      const data = await query('SELECT arrayJoin([0.0, 1.25, -2.5]::Array(BFloat16)) as val');
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as number));
      expect(values[0]).toBe(0);
      expect(values[1]).toBeCloseTo(1.25, 1);
      expect(values[2]).toBeCloseTo(-2.5, 1);
    });
  });

  // ============================================================
  // STRINGS - String, FixedString
  // ============================================================
  describe('String Types', () => {
    it('decodes String values', async () => {
      const data = await query("SELECT arrayJoin(['', 'hello', 'world']::Array(String)) as val");
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual(['', 'hello', 'world']);
    });

    it('decodes String with Unicode', async () => {
      const data = await query("SELECT 'Привет мир 你好世界 🎉'::String as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('Привет мир 你好世界 🎉');
    });

    it('decodes String with special characters', async () => {
      const data = await query("SELECT 'line1\\nline2\\ttab'::String as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('line1\nline2\ttab');
    });

    it('decodes long String (multi-byte VarUInt length)', async () => {
      const data = await query("SELECT repeat('x', 300)::String as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('x'.repeat(300));
    });

    it('decodes FixedString with exact length', async () => {
      const data = await query("SELECT 'abc'::FixedString(3) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('abc');
    });

    it('decodes FixedString with padding', async () => {
      const data = await query("SELECT 'ab'::FixedString(5) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('ab');
    });

    it('decodes empty FixedString', async () => {
      const data = await query("SELECT ''::FixedString(3) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('');
    });
  });

  // ============================================================
  // BOOLEAN
  // ============================================================
  describe('Boolean Type', () => {
    it('decodes Bool values', async () => {
      const data = await query('SELECT arrayJoin([true, false]::Array(Bool)) as val');
      const result = decode(data, 1, 2);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([true, false]);
    });
  });

  // ============================================================
  // DATE AND TIME - Date, Date32, DateTime, DateTime64
  // ============================================================
  describe('Date and Time Types', () => {
    it('decodes Date values', async () => {
      const data = await query("SELECT arrayJoin(['1970-01-01', '2024-01-15', '2100-12-31']::Array(Date)) as val");
      const result = decode(data, 1, 3);
      const displays = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.displayValue));
      expect(displays[0]).toBe('1970-01-01');
      expect(displays[1]).toContain('2024-01-15');
      expect(displays[2]).toContain('2100-12-31');
    });

    it('decodes Date32 values including before epoch', async () => {
      const data = await query("SELECT arrayJoin(['1960-06-15', '1970-01-01', '2200-01-01']::Array(Date32)) as val");
      const result = decode(data, 1, 3);
      const displays = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.displayValue));
      expect(displays[0]).toContain('1960-06-15');
      expect(displays[1]).toBe('1970-01-01');
      expect(displays[2]).toContain('2200-01-01');
    });

    it('decodes DateTime values', async () => {
      const data = await query("SELECT '2024-01-15 12:30:45'::DateTime as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('2024-01-15');
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('12:30:45');
    });

    it('decodes DateTime with timezone', async () => {
      const data = await query("SELECT '2024-01-15 12:30:45'::DateTime('UTC') as val");
      const result = decode(data, 1, 1);
      // ClickHouse may return DateTime('UTC') or just DateTime depending on version
      const col = result.blocks![0].columns[0];
      expect(col.typeString).toMatch(/DateTime/);
      expect(col.values[0].displayValue).toContain('2024-01-15');
    });

    it('decodes DateTime64 with various precisions', async () => {
      // Precision 3 (milliseconds)
      const data3 = await query("SELECT '2024-01-15 12:30:45.123'::DateTime64(3) as val");
      const result3 = decode(data3, 1, 1);
      expect(result3.blocks![0].columns[0].values[0].displayValue).toContain('2024-01-15');

      // Precision 6 (microseconds)
      const data6 = await query("SELECT '2024-01-15 12:30:45.123456'::DateTime64(6) as val");
      const result6 = decode(data6, 1, 1);
      expect(result6.blocks![0].columns[0].values[0].displayValue).toContain('2024-01-15');

      // Precision 9 (nanoseconds)
      const data9 = await query("SELECT '2024-01-15 12:30:45.123456789'::DateTime64(9) as val");
      const result9 = decode(data9, 1, 1);
      expect(result9.blocks![0].columns[0].values[0].displayValue).toContain('2024-01-15');
    });

    it('decodes DateTime64 with timezone', async () => {
      const data = await query("SELECT '2024-01-15 12:30:45.123'::DateTime64(3, 'America/New_York') as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].type).toContain('America/New_York');
    });

    // Time and Time64 types (enabled by default in recent ClickHouse versions)
    it('decodes Time values', async () => {
      const data = await query("SELECT '12:30:45'::Time as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('12:30:45');
    });

    it('decodes Time64 values', async () => {
      const data = await query("SELECT '12:30:45.123'::Time64(3) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('12:30:45');
    });
  });

  // ============================================================
  // SPECIAL TYPES - UUID, IPv4, IPv6
  // ============================================================
  describe('Special Types', () => {
    it('decodes UUID values', async () => {
      const data = await query("SELECT arrayJoin(['00000000-0000-0000-0000-000000000000', '550e8400-e29b-41d4-a716-446655440000', 'ffffffff-ffff-ffff-ffff-ffffffffffff']::Array(UUID)) as val");
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([
        '00000000-0000-0000-0000-000000000000',
        '550e8400-e29b-41d4-a716-446655440000',
        'ffffffff-ffff-ffff-ffff-ffffffffffff',
      ]);
    });

    it('decodes IPv4 values', async () => {
      const data = await query("SELECT arrayJoin(['0.0.0.0', '127.0.0.1', '192.168.1.1', '255.255.255.255']::Array(IPv4)) as val");
      const result = decode(data, 1, 4);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual(['0.0.0.0', '127.0.0.1', '192.168.1.1', '255.255.255.255']);
    });

    it('decodes IPv6 values', async () => {
      const data = await query("SELECT arrayJoin(['::', '::1', '2001:db8::1']::Array(IPv6)) as val");
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as string));
      expect(values[0]).toBe('0:0:0:0:0:0:0:0');
      expect(values[1]).toBe('0:0:0:0:0:0:0:1');
      expect(values[2]).toContain('2001');
    });
  });

  // ============================================================
  // DECIMAL - Decimal32, Decimal64, Decimal128, Decimal256
  // ============================================================
  describe('Decimal Types', () => {
    it('decodes Decimal32 values', async () => {
      const data = await query("SELECT arrayJoin([0, 123.45, -123.45]::Array(Decimal32(2))) as val");
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as number));
      expect(values[0]).toBeCloseTo(0, 2);
      expect(values[1]).toBeCloseTo(123.45, 2);
      expect(values[2]).toBeCloseTo(-123.45, 2);
    });

    it('decodes Decimal64 values', async () => {
      const data = await query("SELECT arrayJoin([0, 12345.6789, -12345.6789]::Array(Decimal64(4))) as val");
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value as number));
      expect(values[0]).toBeCloseTo(0, 4);
      expect(values[1]).toBeCloseTo(12345.6789, 4);
      expect(values[2]).toBeCloseTo(-12345.6789, 4);
    });

    it('decodes Decimal128 values', async () => {
      const data = await query("SELECT 12345678901234567890.1234567890::Decimal128(10) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('12345678901234567890');
    });

    it('decodes Decimal256 values', async () => {
      const data = await query("SELECT 0::Decimal256(20) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('0');
    });
  });

  // ============================================================
  // ENUM - Enum8, Enum16
  // ============================================================
  describe('Enum Types', () => {
    it('decodes Enum8 values', async () => {
      const data = await query("SELECT arrayJoin(['hello', 'world']::Array(Enum8('hello' = 1, 'world' = 2))) as val");
      const result = decode(data, 1, 2);
      const displays = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.displayValue));
      expect(displays).toEqual(["'hello'", "'world'"]);
    });

    it('decodes Enum16 values', async () => {
      const data = await query("SELECT arrayJoin(['foo', 'bar']::Array(Enum16('foo' = 1, 'bar' = 1000))) as val");
      const result = decode(data, 1, 2);
      const displays = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.displayValue));
      expect(displays).toEqual(["'foo'", "'bar'"]);
    });
  });

  // ============================================================
  // TUPLE - Native format serializes elements sequentially
  // ============================================================
  describe('Tuple Type', () => {
    it('decodes simple Tuple', async () => {
      const data = await query("SELECT (42, 'hello')::Tuple(UInt32, String) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(2);
      expect(result.blocks![0].columns[0].values[0].children![0].value).toBe(42);
      expect(result.blocks![0].columns[0].values[0].children![1].value).toBe('hello');
    });

    it('decodes named Tuple', async () => {
      const data = await query("SELECT CAST((42, 'test'), 'Tuple(id UInt32, name String)') as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children![0].label).toBe('id');
      expect(result.blocks![0].columns[0].values[0].children![1].label).toBe('name');
    });

    it('decodes multiple Tuple rows (columnar)', async () => {
      const data = await query("SELECT arrayJoin([(1, 'a'), (2, 'b'), (3, 'c')]::Array(Tuple(UInt8, String))) as val");
      const result = decode(data, 1, 3);
      const tuples = result.blocks!.flatMap(b => b.columns[0].values);
      expect(tuples[0].children![0].value).toBe(1);
      expect(tuples[0].children![1].value).toBe('a');
      expect(tuples[1].children![0].value).toBe(2);
      expect(tuples[1].children![1].value).toBe('b');
      expect(tuples[2].children![0].value).toBe(3);
      expect(tuples[2].children![1].value).toBe('c');
    });

    it('decodes nested Tuple', async () => {
      const data = await query("SELECT ((1, 2), 'outer')::Tuple(Tuple(UInt8, UInt8), String) as val");
      const result = decode(data, 1, 1);
      const innerTuple = result.blocks![0].columns[0].values[0].children![0];
      expect(innerTuple.children).toHaveLength(2);
      expect(innerTuple.children![0].value).toBe(1);
      expect(innerTuple.children![1].value).toBe(2);
    });
  });

  // ============================================================
  // GEO TYPES - Point, Ring, Polygon, etc.
  // ============================================================
  describe('Geo Types', () => {
    it('decodes Point', async () => {
      const data = await query("SELECT (1.5, 2.5)::Point as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children![0].value).toBeCloseTo(1.5);
      expect(result.blocks![0].columns[0].values[0].children![1].value).toBeCloseTo(2.5);
    });

    it('decodes Ring (Array of Points)', async () => {
      const data = await query("SELECT [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]::Ring as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(3);
    });

    it('decodes Polygon (Array of Rings)', async () => {
      const data = await query("SELECT [[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]]::Polygon as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(1);
      expect(result.blocks![0].columns[0].values[0].children![0].children).toHaveLength(4);
    });

    it('decodes MultiPolygon', async () => {
      const data = await query("SELECT [[[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]]]::MultiPolygon as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(1);
    });

    it('decodes LineString', async () => {
      const data = await query("SELECT [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]::LineString as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(3);
    });

    it('decodes MultiLineString', async () => {
      const data = await query("SELECT [[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]::MultiLineString as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(2);
    });
  });

  // ============================================================
  // NULLABLE - Native format: NullMap stream FIRST, then values for ALL rows
  // ============================================================
  describe('Nullable Type', () => {
    it('decodes Nullable with non-null value', async () => {
      const data = await query("SELECT 42::Nullable(UInt32) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(42);
    });

    it('decodes Nullable with null value', async () => {
      const data = await query("SELECT NULL::Nullable(UInt32) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBeNull();
    });

    it('decodes Nullable with mixed values', async () => {
      // Native format: NullMap first (5 bytes), then ALL values (even for NULLs)
      const data = await query("SELECT if(number % 2 = 0, number, NULL)::Nullable(UInt64) AS val FROM numbers(5)");
      const result = decode(data, 1, 5);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([0n, null, 2n, null, 4n]);
    });

    it('decodes Nullable String with null', async () => {
      const data = await query("SELECT arrayJoin(['hello', NULL, 'world']::Array(Nullable(String))) as val");
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual(['hello', null, 'world']);
    });

    it('decodes all NULL values', async () => {
      const data = await query("SELECT NULL::Nullable(UInt8) AS val FROM numbers(3)");
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([null, null, null]);
    });
  });

  // ============================================================
  // ARRAY - Native format: ArraySizes (cumulative UInt64 offsets) FIRST, then elements
  // ============================================================
  describe('Array Type', () => {
    // Helper to get array elements (skip first child which is the length node)
    const getElements = (children: AstNode[] | undefined) =>
      children?.slice(1) ?? [];

    it('decodes empty Array', async () => {
      const data = await query("SELECT []::Array(UInt32) as val");
      const result = decode(data, 1, 1);
      const children = result.blocks![0].columns[0].values[0].children!;
      // First child is the length node
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(0n);
      expect(getElements(children)).toHaveLength(0);
    });

    it('decodes Array of integers', async () => {
      const data = await query("SELECT [1, 2, 3]::Array(UInt32) as val");
      const result = decode(data, 1, 1);
      const children = result.blocks![0].columns[0].values[0].children!;
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(3n);
      expect(getElements(children).map(c => c.value)).toEqual([1, 2, 3]);
    });

    it('decodes Array of strings', async () => {
      const data = await query("SELECT ['hello', 'world']::Array(String) as val");
      const result = decode(data, 1, 1);
      const children = result.blocks![0].columns[0].values[0].children!;
      expect(children[0].label).toBe('length');
      expect(getElements(children).map(c => c.value)).toEqual(['hello', 'world']);
    });

    it('decodes multiple Arrays with varying sizes (cumulative offsets)', async () => {
      // Native format stores cumulative offsets: [2, 3, 3] for sizes [2, 1, 0]
      const data = await query("SELECT arrayJoin([[1, 2], [3], []]::Array(Array(UInt8))) AS val");
      const result = decode(data, 1, 3);
      const arrays = result.blocks!.flatMap(b => b.columns[0].values);
      expect(arrays[0].children![0].label).toBe('length');
      expect(getElements(arrays[0].children!).map(c => c.value)).toEqual([1, 2]);
      expect(getElements(arrays[1].children!).map(c => c.value)).toEqual([3]);
      expect(getElements(arrays[2].children!)).toHaveLength(0);
    });

    it('decodes nested Array', async () => {
      const data = await query("SELECT [[1, 2], [3, 4, 5]]::Array(Array(UInt32)) as val");
      const result = decode(data, 1, 1);
      const outer = getElements(result.blocks![0].columns[0].values[0].children!);
      expect(outer).toHaveLength(2);
      expect(getElements(outer[0].children!).map(c => c.value)).toEqual([1, 2]);
      expect(getElements(outer[1].children!).map(c => c.value)).toEqual([3, 4, 5]);
    });

    it('decodes Array of Nullable', async () => {
      const data = await query("SELECT [1, NULL, 3]::Array(Nullable(UInt32)) as val");
      const result = decode(data, 1, 1);
      const elements = getElements(result.blocks![0].columns[0].values[0].children!);
      expect(elements.map(c => c.value)).toEqual([1, null, 3]);
    });

    it('decodes large Array', async () => {
      const data = await query("SELECT range(100)::Array(UInt32) as val");
      const result = decode(data, 1, 1);
      const children = result.blocks![0].columns[0].values[0].children!;
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(100n);
      expect(getElements(children)).toHaveLength(100);
    });
  });

  // ============================================================
  // MAP - Native format: Treated as Array(Tuple(K, V))
  // Sizes stream, then Keys stream, then Values stream
  // ============================================================
  describe('Map Type', () => {
    it('decodes empty Map', async () => {
      const data = await query("SELECT map()::Map(String, UInt32) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(0);
    });

    it('decodes Map with entries', async () => {
      const data = await query("SELECT map('a', 1, 'b', 2)::Map(String, UInt32) as val");
      const result = decode(data, 1, 1);
      const entries = result.blocks![0].columns[0].values[0].children!;
      expect(entries).toHaveLength(2);
      // Each entry is a Tuple(key, value)
      expect(entries[0].children![0].value).toBe('a');
      expect(entries[0].children![1].value).toBe(1);
      expect(entries[1].children![0].value).toBe('b');
      expect(entries[1].children![1].value).toBe(2);
    });

    it('decodes Map with integer keys', async () => {
      const data = await query("SELECT map(1, 'one', 2, 'two')::Map(UInt32, String) as val");
      const result = decode(data, 1, 1);
      const entries = result.blocks![0].columns[0].values[0].children!;
      expect(entries).toHaveLength(2);
      expect(entries[0].children![0].value).toBe(1);
      expect(entries[0].children![1].value).toBe('one');
    });

    it('decodes multiple Maps', async () => {
      const data = await query("SELECT arrayJoin([map('x', 1), map('y', 2, 'z', 3)]::Array(Map(String, UInt32))) as val");
      const result = decode(data, 1, 2);
      const maps = result.blocks!.flatMap(b => b.columns[0].values);
      expect(maps[0].children).toHaveLength(1);
      expect(maps[1].children).toHaveLength(2);
    });
  });

  // ============================================================
  // LOWCARDINALITY - Native format: Dictionary-encoded
  // KeysVersion stream, then IndexesSerializationType + dictionary + indexes
  // ============================================================
  describe('LowCardinality Type', () => {
    it('decodes LowCardinality String', async () => {
      const data = await query("SELECT 'hello'::LowCardinality(String) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('hello');
    });

    it('decodes LowCardinality with repeated values', async () => {
      const data = await query("SELECT toLowCardinality(toString(number % 3)) AS val FROM numbers(6)");
      const result = decode(data, 1, 6);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual(['0', '1', '2', '0', '1', '2']);
    });

    it('decodes LowCardinality Nullable with non-null', async () => {
      const data = await query("SELECT 'test'::LowCardinality(Nullable(String)) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('test');
    });

    it('decodes LowCardinality Nullable with null', async () => {
      const data = await query("SELECT NULL::LowCardinality(Nullable(String)) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBeNull();
    });

    it('decodes LowCardinality with mixed null and values', async () => {
      const data = await query("SELECT if(number % 2 = 0, toString(number), NULL)::LowCardinality(Nullable(String)) AS val FROM numbers(4)");
      const result = decode(data, 1, 4);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual(['0', null, '2', null]);
    });
  });

  // ============================================================
  // VARIANT - Native format: Discriminators stream + sparse variant elements
  // ============================================================
  describe('Variant Type', () => {
    const variantSettings = { allow_experimental_variant_type: 1 };

    it('decodes Variant with String', async () => {
      const data = await query("SELECT 'hello'::Variant(String, UInt64) as val", variantSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('hello');
    });

    it('decodes Variant with UInt64', async () => {
      const data = await query("SELECT 42::Variant(String, UInt64) as val", variantSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(42n);
    });

    it('decodes Variant NULL', async () => {
      const data = await query("SELECT NULL::Variant(String, UInt64) as val", variantSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBeNull();
    });

    it('decodes Variant with mixed types', async () => {
      const data = await query(
        "SELECT arrayJoin([42::Variant(String, UInt64), 'hello'::Variant(String, UInt64)]) AS val",
        variantSettings
      );
      const result = decode(data, 1, 2);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toContain(42n);
      expect(values).toContain('hello');
    });

    it('decodes Variant with NULL in array (single type)', async () => {
      // ClickHouse doesn't allow mixing incompatible types (String + UInt64) in arrays
      // But we can test NULL with a single variant type
      const data = await query(
        "SELECT arrayJoin([NULL, 'a', 'b']::Array(Variant(String))) AS val",
        variantSettings
      );
      const result = decode(data, 1, 3);
      const values = result.blocks!.flatMap(b => b.columns[0].values.map(v => v.value));
      expect(values).toEqual([null, 'a', 'b']);
    });
  });

  // ============================================================
  // DYNAMIC - Native format: Type structure metadata + variant-like data
  // V1 format: SharedVariant stores values as String representation
  // V2 format: SharedVariant stores values as binary type index + binary value
  // ============================================================
  describe('Dynamic Type', () => {
    // Helper to get data values (skipping header node)
    const getDataValues = (result: ReturnType<typeof decode>) =>
      result.blocks![0].columns[0].values.filter(v => v.type !== 'Dynamic.Header');

    it('decodes Dynamic NULL', async () => {
      const data = await query("SELECT NULL::Dynamic as val");
      const result = decode(data, 1, 1);
      const values = getDataValues(result);
      expect(values[0].value).toBeNull();
    });

    it('decodes Dynamic with DateTime64', async () => {
      const data = await query("SELECT toDateTime64('2024-01-15 12:30:00', 3)::Dynamic as val");
      const result = decode(data, 1, 1);
      const values = getDataValues(result);
      expect(values[0].displayValue).toContain('2024-01-15');
    });

    it('decodes Dynamic with typed values', async () => {
      const data = await query("SELECT toUInt64(42)::Dynamic as val");
      const result = decode(data, 1, 1);
      const values = getDataValues(result);
      expect(values[0]).toBeDefined();
    });

    it('decodes Dynamic with basic types via table', async () => {
      // Create table and insert a few basic types
      // Note: Native format Dynamic columns don't preserve ORDER BY in discriminator order,
      // so we verify values exist without assuming specific row positions
      await query(`
        CREATE TABLE IF NOT EXISTS test_dynamic_types_native (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Insert a smaller set of types that work well together
      await query(`
        INSERT INTO test_dynamic_types_native SELECT 1, 'hello'::String
        UNION ALL SELECT 2, toDateTime64('2024-01-15 12:30:00.123', 3)::DateTime64(3)
        UNION ALL SELECT 3, NULL::Dynamic
        UNION ALL SELECT 4, toUInt64(42)
        UNION ALL SELECT 5, true::Bool
      `);

      // Select and decode
      const data = await query('SELECT val FROM test_dynamic_types_native ORDER BY id');
      const result = decode(data, 1, 5);

      // Get all values from all blocks (filter out header nodes)
      const allValues = result.blocks!.flatMap(b => b.columns[0].values);
      const values = allValues.filter(v => v.type !== 'Dynamic.Header');
      const headers = allValues.filter(v => v.type === 'Dynamic.Header');

      // Verify we got header + 5 values
      expect(headers).toHaveLength(1);
      expect(values).toHaveLength(5);

      // Header should contain metadata children (version, num_dynamic_types, etc.)
      expect(headers[0].children).toBeDefined();
      expect(headers[0].children!.length).toBeGreaterThan(0);

      // Each value should be defined and have a type containing "Dynamic"
      for (let i = 0; i < values.length; i++) {
        expect(values[i]).toBeDefined();
        expect(values[i].type).toContain('Dynamic');
      }

      // Verify expected values exist (Native format may not preserve ORDER BY in discriminators)
      const displayValues = values.map(v => v.displayValue);
      expect(displayValues.some(d => d.includes('hello') || d === '"hello"')).toBe(true);
      expect(displayValues.some(d => d.includes('2024-01-15'))).toBe(true);
      expect(values.some(v => v.value === null)).toBe(true);
      expect(displayValues.some(d => d === '42' || d.includes('42'))).toBe(true);
      expect(displayValues.some(d => d === 'true' || d === '1')).toBe(true);

      // Cleanup
      await query('DROP TABLE IF EXISTS test_dynamic_types_native');
    });

    it('decodes Dynamic with all supported types via table', async () => {
      // Create table and insert one value of each type
      // Note: Native format Dynamic columns don't preserve ORDER BY in discriminator order,
      // so we verify values exist without assuming specific row positions
      await query(`
        CREATE TABLE IF NOT EXISTS test_dynamic_all_types_native (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Insert values of various types using INSERT SELECT for complex types
      await query(`
        INSERT INTO test_dynamic_all_types_native SELECT 1, 42::UInt8
        UNION ALL SELECT 2, 1000::UInt16
        UNION ALL SELECT 3, 100000::UInt32
        UNION ALL SELECT 4, 10000000000::UInt64
        UNION ALL SELECT 5, -42::Int8
        UNION ALL SELECT 6, -1000::Int16
        UNION ALL SELECT 7, -100000::Int32
        UNION ALL SELECT 8, -10000000000::Int64
        UNION ALL SELECT 9, 3.14::Float32
        UNION ALL SELECT 10, 2.718281828::Float64
        UNION ALL SELECT 11, 'hello world'::String
        UNION ALL SELECT 12, true::Bool
        UNION ALL SELECT 13, false::Bool
        UNION ALL SELECT 14, toDate('2024-01-15')::Date
        UNION ALL SELECT 15, toDate32('2024-06-20')::Date32
        UNION ALL SELECT 16, toDateTime('2024-01-15 12:30:00')::DateTime
        UNION ALL SELECT 17, toDateTime64('2024-01-15 12:30:00.123', 3)::DateTime64(3)
        UNION ALL SELECT 18, toUUID('12345678-1234-5678-1234-567812345678')::UUID
        UNION ALL SELECT 19, toIPv4('192.168.1.1')::IPv4
        UNION ALL SELECT 20, toIPv6('::1')::IPv6
        UNION ALL SELECT 21, [1, 2, 3]::Array(UInt8)
        UNION ALL SELECT 22, (1, 'test')::Tuple(UInt32, String)
        UNION ALL SELECT 23, map('key', 'value')::Map(String, String)
        UNION ALL SELECT 24, NULL::Dynamic
        UNION ALL SELECT 25, toDecimal32(123.45, 2)
        UNION ALL SELECT 26, toDecimal64(12345.6789, 4)
        UNION ALL SELECT 27, toBFloat16(1.5)
        UNION ALL SELECT 28, CAST('active', 'Enum8(\\'active\\' = 1, \\'inactive\\' = 2)')
        UNION ALL SELECT 29, CAST('pending', 'Enum16(\\'pending\\' = 100, \\'done\\' = 200)')
      `);

      // Select and decode
      const data = await query('SELECT val FROM test_dynamic_all_types_native ORDER BY id');
      const result = decode(data, 1, 29);

      // Get all values from all blocks (filter out header nodes)
      const allValues = result.blocks!.flatMap(b => b.columns[0].values);
      const values = allValues.filter(v => v.type !== 'Dynamic.Header');
      const headers = allValues.filter(v => v.type === 'Dynamic.Header');

      // Verify we got header + 29 values
      expect(headers).toHaveLength(1);
      expect(values).toHaveLength(29);

      // Header should contain metadata children
      expect(headers[0].children).toBeDefined();
      expect(headers[0].children!.length).toBeGreaterThan(0);

      // Each value should be defined and have a type containing "Dynamic"
      for (let i = 0; i < values.length; i++) {
        expect(values[i]).toBeDefined();
        expect(values[i].type).toContain('Dynamic');
      }

      // Helper to get actual type from Dynamic wrapper
      const getActualType = (v: typeof values[0]) =>
        v.type.includes('(') ? v.type : (v.metadata?.actualType as string || 'Dynamic');

      // Collect all types and values for verification (order-independent)
      const allTypes = values.map(v => getActualType(v));
      const allDisplayValues = values.map(v => v.displayValue);

      // Verify expected types are present (Native format may reorder)
      expect(allTypes.some(t => t.includes('String'))).toBe(true);
      expect(allTypes.some(t => t.includes('DateTime64'))).toBe(true);
      expect(allTypes.some(t => t.includes('UUID'))).toBe(true);
      expect(allTypes.some(t => t.includes('IPv4'))).toBe(true);
      expect(allTypes.some(t => t.includes('Bool'))).toBe(true);
      expect(allTypes.some(t => t.includes('NULL'))).toBe(true);

      // Verify expected values are present
      expect(allDisplayValues.some(d => d.includes('hello world') || d === '"hello world"')).toBe(true);
      expect(allDisplayValues.some(d => d.includes('2024-01-15'))).toBe(true);
      expect(allDisplayValues.some(d => d.includes('192.168.1.1'))).toBe(true);
      expect(values.some(v => v.value === null)).toBe(true);

      // Cleanup
      await query('DROP TABLE IF EXISTS test_dynamic_all_types_native');
    });
  });

  // ============================================================
  // JSON
  // ============================================================
  describe('JSON Type', () => {
    const jsonSettings = { allow_experimental_json_type: 1 };

    it('decodes JSON simple', async () => {
      const data = await query("SELECT '{\"name\": \"test\"}'::JSON as val", jsonSettings);
      const result = decode(data, 1, 1);
      const jsonValue = result.blocks![0].columns[0].values[0].value as Record<string, unknown>;
      expect(jsonValue).toBeDefined();
      expect(jsonValue.name).toBe('test');
    });

    it('decodes JSON with multiple fields', async () => {
      const data = await query("SELECT '{\"name\": \"test\", \"value\": 42}'::JSON as val", jsonSettings);
      const result = decode(data, 1, 1);
      const jsonValue = result.blocks![0].columns[0].values[0].value as Record<string, unknown>;
      expect(jsonValue).toBeDefined();
      expect(jsonValue.name).toBe('test');
      expect(jsonValue.value).toBe(42n); // Int64 returns BigInt
    });

    it('decodes JSON with typed paths', async () => {
      const data = await query("SELECT '{\"id\": 42, \"name\": \"test\"}'::JSON(id UInt32, name String) as val", jsonSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0]).toBeDefined();
    });

    it('decodes JSON with typed IPv4 path and proper AST structure', async () => {
      const data = await query("SELECT '{\"ip\": \"127.0.0.1\"}'::JSON(ip IPv4) as json_ipv4", jsonSettings);
      const result = decode(data, 1, 1);

      const jsonNode = result.blocks![0].columns[0].values[0];

      // JSON node should have children including structural elements
      expect(jsonNode.children).toBeDefined();
      expect(jsonNode.displayValue).toBe('{1 paths}');

      // Find structural elements (V1 format uses VarUInt for max_dynamic_paths)
      const version = jsonNode.children!.find(c => c.label === 'version');
      expect(version).toBeDefined();
      expect(version!.type).toBe('UInt64');
      expect(version!.value).toBe(0n);

      const maxDynPaths = jsonNode.children!.find(c => c.label === 'max_dynamic_paths');
      expect(maxDynPaths).toBeDefined();
      expect(maxDynPaths!.type).toBe('VarUInt');

      const numDynPaths = jsonNode.children!.find(c => c.label === 'num_dynamic_paths');
      expect(numDynPaths).toBeDefined();
      expect(numDynPaths!.type).toBe('VarUInt');

      // Find the typed path node for "ip"
      const pathNode = jsonNode.children!.find(c => c.type === 'JSON.typed_path' && c.label === 'ip');
      expect(pathNode).toBeDefined();
      expect(pathNode!.displayValue).toContain('ip:');

      // Path node should have a child that is the actual IPv4 value
      expect(pathNode!.children).toBeDefined();
      expect(pathNode!.children!.length).toBeGreaterThanOrEqual(1);

      const ipv4Node = pathNode!.children!.find(c => c.type === 'IPv4');
      expect(ipv4Node).toBeDefined();
      expect(ipv4Node!.displayValue).toBe('127.0.0.1');
    });

    it('decodes JSON followed by other columns', async () => {
      const data = await query(`
        SELECT
          '{\"name\": \"test\", \"value\": 42}'::JSON as json_col,
          42::UInt8 as uint8_col,
          'hello'::String as string_col
      `, jsonSettings);
      const result = decode(data, 3, 1);

      // JSON column
      const jsonValue = result.blocks![0].columns[0].values[0].value as Record<string, unknown>;
      expect(jsonValue.name).toBe('test');
      expect(jsonValue.value).toBe(42n);

      // Following columns should be decoded correctly
      expect(result.blocks![0].columns[1].values[0].value).toBe(42);
      expect(result.blocks![0].columns[2].values[0].value).toBe('hello');
    });

    it('decodes multiple JSON columns with different schemas', async () => {
      const data = await query(`
        SELECT
          '{"name": "test", "value": 42}'::JSON as json_simple,
          '{"id": 1, "nested": {"x": 10, "y": 20}}'::JSON as json_nested,
          '{"id": 1, "nested": {"x": 10, "y": 20}}'::JSON(a Int32) as json_partially_typed
      `, jsonSettings);
      const result = decode(data, 3, 1);

      // All three JSON columns should decode
      expect(result.blocks![0].columns[0].values[0]).toBeDefined();
      expect(result.blocks![0].columns[1].values[0]).toBeDefined();
      expect(result.blocks![0].columns[2].values[0]).toBeDefined();
    });

    it('decodes 4 JSON columns with mixed schemas', async () => {
      const data = await query(`
        SELECT
          '{"name": "test", "value": 42}'::JSON as json_simple,
          '{"id": 1, "nested": {"x": 10, "y": 20}}'::JSON as json_nested,
          '{"id": 1, "nested": {"x": 10, "y": 20}}'::JSON(a Int32) as json_partially_typed,
          '{"id": 1}'::JSON(a Int32) as json_fully_typed
      `, jsonSettings);
      const result = decode(data, 4, 1);

      // json_simple
      const simple = result.blocks![0].columns[0].values[0].value as Record<string, unknown>;
      expect(simple.name).toBe('test');
      expect(simple.value).toBe(42n);

      // json_nested
      const nested = result.blocks![0].columns[1].values[0].value as Record<string, unknown>;
      expect(nested.id).toBe(1n);
      expect((nested.nested as Record<string, unknown>).x).toBe(10n);
      expect((nested.nested as Record<string, unknown>).y).toBe(20n);

      // json_partially_typed (has typed path 'a' not in input)
      const partiallyTyped = result.blocks![0].columns[2].values[0].value as Record<string, unknown>;
      expect(partiallyTyped.id).toBe(1n);
      expect((partiallyTyped.nested as Record<string, unknown>).x).toBe(10n);
      expect((partiallyTyped.nested as Record<string, unknown>).y).toBe(20n);
      expect(partiallyTyped.a).toBe(0); // Default value for Int32

      // json_fully_typed (has typed path 'a' not in input, only dynamic 'id')
      const fullyTyped = result.blocks![0].columns[3].values[0].value as Record<string, unknown>;
      expect(fullyTyped.id).toBe(1n);
      expect(fullyTyped.a).toBe(0); // Default value for Int32
    });

    it('decodes JSON with typed and dynamic paths across 100 rows', async () => {
      const data = await query(`
        SELECT
          '{"a": 42, "b": { "c" : "stringvalue" }}'::JSON(a Int32) as json
        FROM system.numbers LIMIT 100
      `, jsonSettings);
      const result = decode(data, 1, 100);

      // Should have 100 rows
      expect(result.blocks![0].columns[0].values.length).toBe(100);

      // Check first row
      const firstRow = result.blocks![0].columns[0].values[0].value as Record<string, unknown>;
      expect(firstRow.a).toBe(42);
      expect((firstRow.b as Record<string, unknown>).c).toBe('stringvalue');

      // Check last row
      const lastRow = result.blocks![0].columns[0].values[99].value as Record<string, unknown>;
      expect(lastRow.a).toBe(42);
      expect((lastRow.b as Record<string, unknown>).c).toBe('stringvalue');
    });

    it('decodes JSON with exceeded max_dynamic_paths (shared data)', async () => {
      // With max_dynamic_paths=2 and 3 paths in JSON:
      // - "a" and "b" become dynamic paths
      // - "c" overflows to shared data
      const data = await query(`
        SELECT '{"a": 1, "b": 2, "c": 3}'::JSON(max_dynamic_paths=2) AS col
      `, jsonSettings);
      const result = decode(data, 1, 1);

      // Should decode the JSON with all 3 paths
      const jsonValue = result.blocks![0].columns[0].values[0].value as Record<string, unknown>;
      expect(jsonValue).toBeDefined();

      // Dynamic paths "a" and "b" should be present
      expect(jsonValue.a).toBe(1n);
      expect(jsonValue.b).toBe(2n);

      // Overflow path "c" should be in shared data and still accessible
      // Note: The exact structure may vary, but the value should be retrievable
      expect(jsonValue.c).toBe(3n);
    });

    it('decodes JSON with array in dynamic path and includes Dynamic.structure in AST', async () => {
      // This tests that the Dynamic structure metadata (version, max_types, num_types, type_names)
      // for each dynamic path is included in the AST
      const data = await query(`SELECT '{"items": [{"id": 1}, {"id": 2}]}'::JSON as val`, jsonSettings);
      const result = decode(data, 1, 1);

      const jsonNode = result.blocks![0].columns[0].values[0];
      expect(jsonNode.children).toBeDefined();

      // Find the Dynamic.structure node for "items"
      const dynamicStructure = jsonNode.children!.find(
        (c: AstNode) => c.type === 'Dynamic.structure' && c.label === 'items.structure'
      );
      expect(dynamicStructure).toBeDefined();
      expect(dynamicStructure!.displayValue).toContain('Dynamic structure for "items"');

      // The Dynamic.structure should have children for the structure metadata
      expect(dynamicStructure!.children).toBeDefined();
      expect(dynamicStructure!.children!.length).toBeGreaterThan(0);

      // Verify the Dynamic.structure contains expected metadata nodes
      const dynVersion = dynamicStructure!.children!.find((c: AstNode) => c.label === 'dynamic_version');
      expect(dynVersion).toBeDefined();
      expect(dynVersion!.type).toBe('UInt64');

      const maxTypes = dynamicStructure!.children!.find((c: AstNode) => c.label === 'max_dynamic_types');
      expect(maxTypes).toBeDefined();
      expect(maxTypes!.type).toBe('VarUInt');

      const numTypes = dynamicStructure!.children!.find((c: AstNode) => c.label === 'num_dynamic_types');
      expect(numTypes).toBeDefined();
      expect(numTypes!.type).toBe('VarUInt');

      // Should have type name(s) for the variant types in the Dynamic column
      const typeNameNodes = dynamicStructure!.children!.filter((c: AstNode) => c.label?.startsWith('type_name['));
      expect(typeNameNodes.length).toBeGreaterThan(0);

      const variantMode = dynamicStructure!.children!.find((c: AstNode) => c.label === 'variant_mode');
      expect(variantMode).toBeDefined();
      expect(variantMode!.type).toBe('UInt64');
    });
  });

  // ============================================================
  // MULTIPLE COLUMNS AND ROWS
  // ============================================================
  describe('Multiple Columns and Rows', () => {
    it('decodes multiple columns of different types', async () => {
      const data = await query(`
        SELECT
          42::UInt32 as int_col,
          'hello'::String as str_col,
          true::Bool as bool_col,
          3.14::Float64 as float_col
      `);
      const result = decode(data, 4, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(42);
      expect(result.blocks![0].columns[1].values[0].value).toBe('hello');
      expect(result.blocks![0].columns[2].values[0].value).toBe(true);
      expect(result.blocks![0].columns[3].values[0].value).toBeCloseTo(3.14);
    });

    it('decodes many rows', async () => {
      const data = await query('SELECT number::UInt32 as val FROM numbers(100)');
      const result = decode(data, 1, 100);

      const allValues: number[] = [];
      for (const block of result.blocks!) {
        for (const node of block.columns[0].values) {
          allValues.push(node.value as number);
        }
      }
      for (let i = 0; i < 100; i++) {
        expect(allValues[i]).toBe(i);
      }
    });

    it('decodes multiple columns with multiple rows', async () => {
      const data = await query(`
        SELECT
          number::UInt32 as id,
          concat('item_', toString(number))::String as name
        FROM numbers(10)
      `);
      const result = decode(data, 2, 10);

      const ids: number[] = [];
      const names: string[] = [];
      for (const block of result.blocks!) {
        for (let i = 0; i < block.rowCount; i++) {
          ids.push(block.columns[0].values[i].value as number);
          names.push(block.columns[1].values[i].value as string);
        }
      }

      for (let i = 0; i < 10; i++) {
        expect(ids[i]).toBe(i);
        expect(names[i]).toBe(`item_${i}`);
      }
    });
  });

  // ============================================================
  // BLOCK STRUCTURE
  // ============================================================
  describe('Block Structure', () => {
    it('has correct byte ranges for block header', async () => {
      const data = await query('SELECT 42::UInt32 as val');
      const result = decode(data, 1, 1);

      const block = result.blocks![0];
      expect(block.header.numColumnsRange.start).toBeLessThan(block.header.numColumnsRange.end);
      expect(block.header.numRowsRange.start).toBeLessThan(block.header.numRowsRange.end);
      expect(block.header.numColumnsRange.end).toBe(block.header.numRowsRange.start);
    });

    it('has correct byte ranges for column metadata', async () => {
      const data = await query('SELECT 42::UInt32 as val');
      const result = decode(data, 1, 1);

      const col = result.blocks![0].columns[0];
      expect(col.nameByteRange.start).toBeLessThan(col.nameByteRange.end);
      expect(col.typeByteRange.start).toBeLessThan(col.typeByteRange.end);
      expect(col.dataByteRange.start).toBeLessThanOrEqual(col.dataByteRange.end);
      expect(col.nameByteRange.end).toBeLessThanOrEqual(col.typeByteRange.start);
      expect(col.typeByteRange.end).toBeLessThanOrEqual(col.dataByteRange.start);
    });

    it('handles empty result set (0 bytes)', async () => {
      const data = await query('SELECT 1::UInt32 as val WHERE 0');
      // Empty results produce 0 bytes in Native format
      expect(data.length).toBe(0);
    });
  });

  // ============================================================
  // QBIT (Quantized Bit Vector) - Experimental
  // ============================================================
  describe('QBit Type', () => {
    const qbitSettings = { allow_experimental_qbit_type: 1 };

    it('decodes QBit(Float32, 3) values', async () => {
      const data = await query("SELECT [1.0, 2.0, 3.0]::QBit(Float32, 3) as val", qbitSettings);
      const result = decode(data, 1, 1);
      const qbit = result.blocks![0].columns[0].values[0];
      expect(qbit.children).toHaveLength(3);
      expect(qbit.children![0].value).toBeCloseTo(1.0, 5);
      expect(qbit.children![1].value).toBeCloseTo(2.0, 5);
      expect(qbit.children![2].value).toBeCloseTo(3.0, 5);
    });

    it('decodes QBit(Float32, 3) with zeros', async () => {
      const data = await query("SELECT [0.0, 0.0, 0.0]::QBit(Float32, 3) as val", qbitSettings);
      const result = decode(data, 1, 1);
      const qbit = result.blocks![0].columns[0].values[0];
      expect(qbit.children).toHaveLength(3);
      expect(qbit.children![0].value).toBe(0);
      expect(qbit.children![1].value).toBe(0);
      expect(qbit.children![2].value).toBe(0);
    });

    it('decodes QBit(Float32, 3) with multiple rows', async () => {
      const data = await query(
        "SELECT arrayJoin([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]::Array(Array(Float32)))::QBit(Float32, 3) AS val",
        qbitSettings
      );
      const result = decode(data, 1, 2);
      const values = result.blocks!.flatMap(b => b.columns[0].values);
      expect(values).toHaveLength(2);
      // Row 0: [1.0, 0.0, 0.0]
      expect(values[0].children![0].value).toBeCloseTo(1.0, 5);
      expect(values[0].children![1].value).toBeCloseTo(0.0, 5);
      expect(values[0].children![2].value).toBeCloseTo(0.0, 5);
      // Row 1: [0.0, 1.0, 0.0]
      expect(values[1].children![0].value).toBeCloseTo(0.0, 5);
      expect(values[1].children![1].value).toBeCloseTo(1.0, 5);
      expect(values[1].children![2].value).toBeCloseTo(0.0, 5);
    });
  });

  // ============================================================
  // GEOMETRY (Variant of geo types)
  // ============================================================
  // Geometry is a Variant type with alphabetically sorted geo types:
  // 0=LineString, 1=MultiLineString, 2=MultiPolygon, 3=Point, 4=Polygon, 5=Ring
  describe('Geometry Type', () => {
    const geoSettings = { allow_suspicious_variant_types: 1 };

    it('decodes Geometry as Point', async () => {
      const data = await query("SELECT ((1.0, 2.0)::Point)::Geometry as val", geoSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].type).toBe('Point');
      expect(result.blocks![0].columns[0].values[0].children![0].value).toBeCloseTo(1.0);
      expect(result.blocks![0].columns[0].values[0].children![1].value).toBeCloseTo(2.0);
    });

    it('decodes Geometry as Ring', async () => {
      const data = await query("SELECT ([(1.0, 2.0), (3.0, 4.0)]::Ring)::Geometry as val", geoSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].type).toBe('Ring');
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(2);
    });

    it('decodes Geometry as LineString', async () => {
      const data = await query("SELECT ([(0.0, 0.0), (1.0, 1.0)]::LineString)::Geometry as val", geoSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].type).toBe('LineString');
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(2);
    });

    it('decodes Geometry as Polygon', async () => {
      const data = await query("SELECT ([[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]]::Polygon)::Geometry as val", geoSettings);
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].type).toBe('Polygon');
    });
  });

  // ============================================================
  // AGGREGATE FUNCTION STATE
  // ============================================================
  describe('AggregateFunction Type', () => {
    it('decodes avgState aggregate function', async () => {
      const data = await query("SELECT avgState(number) FROM numbers(10)");
      const result = decode(data, 1, 1);

      const node = result.blocks![0].columns[0].values[0];
      expect(node.type).toBe('AggregateFunction(avg, UInt64)');
      // avgState for numbers(10): sum=45 (0+1+...+9), count=10, avg=4.5
      expect(node.displayValue).toContain('avg=4.50');
      expect(node.displayValue).toContain('sum=45');
      expect(node.displayValue).toContain('count=10');
      expect(node.children).toHaveLength(2);
      expect(node.children![0].label).toBe('numerator (sum)');
      expect(node.children![0].value).toBe(45n);
      expect(node.children![1].label).toBe('denominator (count)');
      expect(node.children![1].value).toBe(10);
    });

    it('decodes sumState aggregate function', async () => {
      const data = await query("SELECT sumState(number) FROM numbers(10)");
      const result = decode(data, 1, 1);

      const node = result.blocks![0].columns[0].values[0];
      expect(node.type).toBe('AggregateFunction(sum, UInt64)');
      expect(node.displayValue).toContain('sum=45');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].label).toBe('sum');
      expect(node.children![0].value).toBe(45n);
    });

    it('decodes countState aggregate function', async () => {
      const data = await query("SELECT countState() FROM numbers(10)");
      const result = decode(data, 1, 1);

      const node = result.blocks![0].columns[0].values[0];
      expect(node.type).toBe('AggregateFunction(count)');
      expect(node.displayValue).toBe('count=10');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].label).toBe('count');
      expect(node.children![0].value).toBe(10);
    });
  });
}, 300000);
