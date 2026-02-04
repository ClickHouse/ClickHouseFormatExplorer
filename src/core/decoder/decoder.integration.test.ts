import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ClickHouseContainer, StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { RowBinaryDecoder } from './rowbinary-decoder';
import { AstNode } from '../types/ast';

const IMAGE = 'clickhouse/clickhouse-server:latest';

/**
 * Comprehensive test suite for RowBinaryDecoder (RowBinaryWithNamesAndTypes format)
 *
 * RowBinary is row-oriented:
 * - Header: column count (LEB128), column names, column types
 * - Rows: for each row, values in column order
 *
 * Key differences from Native format:
 * - Row-oriented vs column-oriented
 * - Nullable: 1 byte null flag + value (vs separate null map stream)
 * - Array: LEB128 size + elements (vs cumulative offsets + flattened elements)
 * - LowCardinality: transparent wrapper (vs dictionary-encoded)
 */
describe('RowBinaryDecoder Integration Tests', () => {
  let container: StartedClickHouseContainer;
  let query: (sql: string, settings?: Record<string, string | number>) => Promise<Uint8Array>;
  let decode: (data: Uint8Array, expectedColumns: number, expectedRows: number) => ReturnType<RowBinaryDecoder['decode']>;

  beforeAll(async () => {
    container = await new ClickHouseContainer(IMAGE).start();
    const baseUrl = container.getHttpUrl();

    query = async (sql: string, settings?: Record<string, string | number>): Promise<Uint8Array> => {
      const params = new URLSearchParams({
        user: container.getUsername(),
        password: container.getPassword(),
      });
      if (settings) {
        for (const [key, value] of Object.entries(settings)) {
          params.set(key, String(value));
        }
      }
      const response = await fetch(`${baseUrl}/?${params}`, {
        method: 'POST',
        body: `${sql} FORMAT RowBinaryWithNamesAndTypes`,
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse error: ${text}`);
      }
      const buffer = await response.arrayBuffer();
      return new Uint8Array(buffer);
    };

    decode = (data: Uint8Array, expectedColumns: number, expectedRows: number) => {
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();

      expect(result.header.columns).toHaveLength(expectedColumns);
      expect(result.rows).toBeDefined();
      expect(result.rows!).toHaveLength(expectedRows);

      // Verify all nodes have valid byte ranges
      for (const row of result.rows!) {
        for (const node of row.values) {
          expect(node.byteRange.start).toBeLessThan(node.byteRange.end);
          expect(node.byteRange.end).toBeLessThanOrEqual(data.length);
        }
      }

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
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([0, 42, 255]);
    });

    it('decodes UInt16 values', async () => {
      const data = await query('SELECT arrayJoin([0, 1234, 65535]::Array(UInt16)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([0, 1234, 65535]);
    });

    it('decodes UInt32 values', async () => {
      const data = await query('SELECT arrayJoin([0, 123456, 4294967295]::Array(UInt32)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([0, 123456, 4294967295]);
    });

    it('decodes UInt64 values', async () => {
      const data = await query('SELECT arrayJoin([0, 9223372036854775807, 18446744073709551615]::Array(UInt64)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([0n, 9223372036854775807n, 18446744073709551615n]);
    });

    it('decodes UInt128 values', async () => {
      const data = await query('SELECT 170141183460469231731687303715884105727::UInt128 as val');
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe(170141183460469231731687303715884105727n);
    });

    it('decodes UInt256 zero and one', async () => {
      const data = await query('SELECT arrayJoin([0, 1]::Array(UInt256)) as val');
      const result = decode(data, 1, 2);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([0n, 1n]);
    });

    it('decodes Int8 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-128, 0, 127]::Array(Int8)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([-128, 0, 127]);
    });

    it('decodes Int16 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-32768, 0, 32767]::Array(Int16)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([-32768, 0, 32767]);
    });

    it('decodes Int32 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-2147483648, 0, 2147483647]::Array(Int32)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([-2147483648, 0, 2147483647]);
    });

    it('decodes Int64 values including negative', async () => {
      const data = await query('SELECT arrayJoin([-9223372036854775808, 0, 9223372036854775807]::Array(Int64)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([-9223372036854775808n, 0n, 9223372036854775807n]);
    });

    it('decodes Int128 negative', async () => {
      const data = await query("SELECT toInt128('-123456789012345678901234567890') as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe(-123456789012345678901234567890n);
    });

    it('decodes Int256 negative', async () => {
      const data = await query("SELECT toInt256('-12345678901234567890123456789012345678901234567890') as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe(-12345678901234567890123456789012345678901234567890n);
    });
  });

  // ============================================================
  // FLOATING POINT - Float32, Float64, BFloat16
  // ============================================================
  describe('Floating Point Types', () => {
    it('decodes Float32 values', async () => {
      const data = await query('SELECT arrayJoin([0.0, 3.14, -123.456]::Array(Float32)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value as number);
      expect(values[0]).toBe(0);
      expect(values[1]).toBeCloseTo(3.14, 2);
      expect(values[2]).toBeCloseTo(-123.456, 2);
    });

    it('decodes Float32 special values', async () => {
      const data = await query('SELECT arrayJoin([inf, -inf, nan]::Array(Float32)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value as number);
      expect(values[0]).toBe(Infinity);
      expect(values[1]).toBe(-Infinity);
      expect(Number.isNaN(values[2])).toBe(true);
    });

    it('decodes Float64 values', async () => {
      const data = await query('SELECT arrayJoin([0.0, 3.141592653589793, -1e300]::Array(Float64)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value as number);
      expect(values[0]).toBe(0);
      expect(values[1]).toBeCloseTo(3.141592653589793, 14);
      expect(values[2]).toBeCloseTo(-1e300);
    });

    it('decodes Float64 special values', async () => {
      const data = await query('SELECT arrayJoin([inf, -inf, nan]::Array(Float64)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value as number);
      expect(values[0]).toBe(Infinity);
      expect(values[1]).toBe(-Infinity);
      expect(Number.isNaN(values[2])).toBe(true);
    });

    it('decodes BFloat16 values', async () => {
      const data = await query('SELECT arrayJoin([0.0, 1.25, toFloat32(-2.5)]::Array(BFloat16)) as val');
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value as number);
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
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual(['', 'hello', 'world']);
    });

    it('decodes String with Unicode', async () => {
      const data = await query("SELECT 'Привет мир 你好世界 🎉'::String as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe('Привет мир 你好世界 🎉');
    });

    it('decodes String with special characters', async () => {
      const data = await query("SELECT 'line1\\nline2\\ttab'::String as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe('line1\nline2\ttab');
    });

    it('decodes long String', async () => {
      const data = await query("SELECT repeat('x', 300)::String as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe('x'.repeat(300));
    });

    it('decodes FixedString with exact length', async () => {
      const data = await query("SELECT 'abc'::FixedString(3) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe('abc');
    });

    it('decodes FixedString with padding', async () => {
      const data = await query("SELECT 'ab'::FixedString(5) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe('ab');
    });
  });

  // ============================================================
  // BOOLEAN
  // ============================================================
  describe('Boolean Type', () => {
    it('decodes Bool values', async () => {
      const data = await query('SELECT arrayJoin([true, false]::Array(Bool)) as val');
      const result = decode(data, 1, 2);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([true, false]);
    });
  });

  // ============================================================
  // DATE AND TIME - Date, Date32, DateTime, DateTime64, Time, Time64
  // ============================================================
  describe('Date and Time Types', () => {
    it('decodes Date values', async () => {
      const data = await query("SELECT arrayJoin(['1970-01-01', '2024-01-15', '2100-12-31']::Array(Date)) as val");
      const result = decode(data, 1, 3);
      const displays = result.rows!.map(r => r.values[0].displayValue);
      expect(displays[0]).toBe('1970-01-01');
      expect(displays[1]).toContain('2024-01-15');
      expect(displays[2]).toContain('2100-12-31');
    });

    it('decodes Date32 values including before epoch', async () => {
      const data = await query("SELECT arrayJoin(['1960-06-15', '1970-01-01', '2200-01-01']::Array(Date32)) as val");
      const result = decode(data, 1, 3);
      const displays = result.rows!.map(r => r.values[0].displayValue);
      expect(displays[0]).toContain('1960-06-15');
      expect(displays[1]).toBe('1970-01-01');
      expect(displays[2]).toContain('2200-01-01');
    });

    it('decodes DateTime values', async () => {
      const data = await query("SELECT '2024-01-15 12:30:45'::DateTime as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
      expect(result.rows![0].values[0].displayValue).toContain('12:30:45');
    });

    it('decodes DateTime with timezone', async () => {
      const data = await query("SELECT '2024-01-15 12:30:45'::DateTime('UTC') as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    it('decodes DateTime64 with various precisions', async () => {
      // Precision 3 (milliseconds)
      const data3 = await query("SELECT '2024-01-15 12:30:45.123'::DateTime64(3) as val");
      const result3 = decode(data3, 1, 1);
      expect(result3.rows![0].values[0].displayValue).toContain('2024-01-15');

      // Precision 6 (microseconds)
      const data6 = await query("SELECT '2024-01-15 12:30:45.123456'::DateTime64(6) as val");
      const result6 = decode(data6, 1, 1);
      expect(result6.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    it('decodes Time values', async () => {
      const data = await query("SELECT '12:30:45'::Time as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].displayValue).toContain('12:30:45');
    });

    it('decodes Time64 values', async () => {
      const data = await query("SELECT '12:30:45.123'::Time64(3) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].displayValue).toContain('12:30:45');
    });
  });

  // ============================================================
  // SPECIAL TYPES - UUID, IPv4, IPv6
  // ============================================================
  describe('Special Types', () => {
    it('decodes UUID values', async () => {
      const data = await query("SELECT arrayJoin(['00000000-0000-0000-0000-000000000000', '550e8400-e29b-41d4-a716-446655440000', 'ffffffff-ffff-ffff-ffff-ffffffffffff']::Array(UUID)) as val");
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual([
        '00000000-0000-0000-0000-000000000000',
        '550e8400-e29b-41d4-a716-446655440000',
        'ffffffff-ffff-ffff-ffff-ffffffffffff',
      ]);
    });

    it('decodes IPv4 values', async () => {
      const data = await query("SELECT arrayJoin(['0.0.0.0', '127.0.0.1', '192.168.1.1', '255.255.255.255']::Array(IPv4)) as val");
      const result = decode(data, 1, 4);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual(['0.0.0.0', '127.0.0.1', '192.168.1.1', '255.255.255.255']);
    });

    it('decodes IPv6 values', async () => {
      const data = await query("SELECT arrayJoin(['::', '::1', '2001:db8::1']::Array(IPv6)) as val");
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value as string);
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
      const values = result.rows!.map(r => r.values[0].value as number);
      expect(values[0]).toBeCloseTo(0, 2);
      expect(values[1]).toBeCloseTo(123.45, 2);
      expect(values[2]).toBeCloseTo(-123.45, 2);
    });

    it('decodes Decimal64 values', async () => {
      const data = await query("SELECT arrayJoin([0, 12345.6789, -12345.6789]::Array(Decimal64(4))) as val");
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => r.values[0].value as number);
      expect(values[0]).toBeCloseTo(0, 4);
      expect(values[1]).toBeCloseTo(12345.6789, 4);
      expect(values[2]).toBeCloseTo(-12345.6789, 4);
    });

    it('decodes Decimal128 values', async () => {
      const data = await query("SELECT 12345678901234567890.1234567890::Decimal128(10) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].displayValue).toContain('12345678901234567890');
    });

    it('decodes Decimal256 values', async () => {
      const data = await query("SELECT 0::Decimal256(20) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].displayValue).toContain('0');
    });
  });

  // ============================================================
  // ENUM - Enum8, Enum16
  // ============================================================
  describe('Enum Types', () => {
    it('decodes Enum8 values', async () => {
      const data = await query("SELECT arrayJoin(['hello', 'world']::Array(Enum8('hello' = 1, 'world' = 2))) as val");
      const result = decode(data, 1, 2);
      const displays = result.rows!.map(r => r.values[0].displayValue);
      expect(displays).toEqual(["'hello'", "'world'"]);
    });

    it('decodes Enum16 values', async () => {
      const data = await query("SELECT arrayJoin(['foo', 'bar']::Array(Enum16('foo' = 1, 'bar' = 1000))) as val");
      const result = decode(data, 1, 2);
      const displays = result.rows!.map(r => r.values[0].displayValue);
      expect(displays).toEqual(["'foo'", "'bar'"]);
    });
  });

  // ============================================================
  // NULLABLE
  // ============================================================
  describe('Nullable Type', () => {
    it('decodes Nullable with non-null value', async () => {
      const data = await query("SELECT 42::Nullable(UInt32) as val");
      const result = decode(data, 1, 1);
      // RowBinary Nullable wraps value in children
      expect(result.rows![0].values[0].children?.[0].value).toBe(42);
    });

    it('decodes Nullable with null value', async () => {
      const data = await query("SELECT NULL::Nullable(UInt32) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBeNull();
    });

    it('decodes Nullable with mixed values', async () => {
      const data = await query("SELECT if(number % 2 = 0, number, NULL)::Nullable(UInt64) AS val FROM numbers(5)");
      const result = decode(data, 1, 5);
      const values = result.rows!.map(r => {
        if (r.values[0].value === null) return null;
        return r.values[0].children?.[0].value as bigint ?? r.values[0].value;
      });
      expect(values).toEqual([0n, null, 2n, null, 4n]);
    });

    it('decodes Nullable String with null', async () => {
      const data = await query("SELECT arrayJoin(['hello', NULL, 'world']::Array(Nullable(String))) as val");
      const result = decode(data, 1, 3);
      const values = result.rows!.map(r => {
        if (r.values[0].value === null) return null;
        return r.values[0].children?.[0].value as string ?? r.values[0].value;
      });
      expect(values).toEqual(['hello', null, 'world']);
    });
  });

  // ============================================================
  // ARRAY
  // ============================================================
  describe('Array Type', () => {
    // Helper to get array elements (skip first child which is the length node)
    const getElements = (children: AstNode[] | undefined) =>
      children?.slice(1) ?? [];

    it('decodes empty Array', async () => {
      const data = await query("SELECT []::Array(UInt32) as val");
      const result = decode(data, 1, 1);
      const children = result.rows![0].values[0].children!;
      // First child is the length node
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(0);
      expect(getElements(children)).toHaveLength(0);
    });

    it('decodes Array of integers', async () => {
      const data = await query("SELECT [1, 2, 3]::Array(UInt32) as val");
      const result = decode(data, 1, 1);
      const children = result.rows![0].values[0].children!;
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(3);
      expect(getElements(children).map((c: AstNode) => c.value)).toEqual([1, 2, 3]);
    });

    it('decodes Array of strings', async () => {
      const data = await query("SELECT ['hello', 'world']::Array(String) as val");
      const result = decode(data, 1, 1);
      const children = result.rows![0].values[0].children!;
      expect(children[0].label).toBe('length');
      expect(getElements(children).map((c: AstNode) => c.value)).toEqual(['hello', 'world']);
    });

    it('decodes nested Array', async () => {
      const data = await query("SELECT [[1, 2], [3, 4, 5]]::Array(Array(UInt32)) as val");
      const result = decode(data, 1, 1);
      const outer = getElements(result.rows![0].values[0].children!);
      expect(outer).toHaveLength(2);
      expect(getElements(outer[0].children!).map((c: AstNode) => c.value)).toEqual([1, 2]);
      expect(getElements(outer[1].children!).map((c: AstNode) => c.value)).toEqual([3, 4, 5]);
    });

    it('decodes Array of Nullable', async () => {
      const data = await query("SELECT [1, NULL, 3]::Array(Nullable(UInt32)) as val");
      const result = decode(data, 1, 1);
      const elements = getElements(result.rows![0].values[0].children!);
      // Nullable in Array wraps non-null values
      expect(elements[0].children?.[0].value ?? elements[0].value).toBe(1);
      expect(elements[1].value).toBeNull();
      expect(elements[2].children?.[0].value ?? elements[2].value).toBe(3);
    });

    it('decodes large Array', async () => {
      const data = await query("SELECT range(100)::Array(UInt32) as val");
      const result = decode(data, 1, 1);
      const children = result.rows![0].values[0].children!;
      expect(children[0].label).toBe('length');
      expect(children[0].value).toBe(100);
      expect(getElements(children)).toHaveLength(100);
    });
  });

  // ============================================================
  // TUPLE
  // ============================================================
  describe('Tuple Type', () => {
    it('decodes simple Tuple', async () => {
      const data = await query("SELECT (42, 'hello')::Tuple(UInt32, String) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(2);
      expect(result.rows![0].values[0].children![0].value).toBe(42);
      expect(result.rows![0].values[0].children![1].value).toBe('hello');
    });

    it('decodes named Tuple', async () => {
      const data = await query("SELECT CAST((42, 'test'), 'Tuple(id UInt32, name String)') as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children![0].label).toBe('id');
      expect(result.rows![0].values[0].children![1].label).toBe('name');
    });

    it('decodes nested Tuple', async () => {
      const data = await query("SELECT ((1, 2), 'outer')::Tuple(Tuple(UInt8, UInt8), String) as val");
      const result = decode(data, 1, 1);
      const innerTuple = result.rows![0].values[0].children![0];
      expect(innerTuple.children).toHaveLength(2);
      expect(innerTuple.children![0].value).toBe(1);
      expect(innerTuple.children![1].value).toBe(2);
    });
  });

  // ============================================================
  // MAP
  // ============================================================
  describe('Map Type', () => {
    it('decodes empty Map', async () => {
      const data = await query("SELECT map()::Map(String, UInt32) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(0);
    });

    it('decodes Map with entries', async () => {
      const data = await query("SELECT map('a', 1, 'b', 2)::Map(String, UInt32) as val");
      const result = decode(data, 1, 1);
      const entries = result.rows![0].values[0].children!;
      expect(entries).toHaveLength(2);
      expect(entries[0].children![0].value).toBe('a');
      expect(entries[0].children![1].value).toBe(1);
      expect(entries[1].children![0].value).toBe('b');
      expect(entries[1].children![1].value).toBe(2);
    });

    it('decodes Map with integer keys', async () => {
      const data = await query("SELECT map(1, 'one', 2, 'two')::Map(UInt32, String) as val");
      const result = decode(data, 1, 1);
      const entries = result.rows![0].values[0].children!;
      expect(entries).toHaveLength(2);
      expect(entries[0].children![0].value).toBe(1);
      expect(entries[0].children![1].value).toBe('one');
    });
  });

  // ============================================================
  // LOWCARDINALITY - Transparent wrapper in RowBinary
  // ============================================================
  describe('LowCardinality Type', () => {
    it('decodes LowCardinality String', async () => {
      const data = await query("SELECT 'hello'::LowCardinality(String) as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe('hello');
    });

    it('decodes LowCardinality with repeated values', async () => {
      const data = await query("SELECT toLowCardinality(toString(number % 3)) AS val FROM numbers(6)");
      const result = decode(data, 1, 6);
      const values = result.rows!.map(r => r.values[0].value);
      expect(values).toEqual(['0', '1', '2', '0', '1', '2']);
    });
  });

  // ============================================================
  // VARIANT
  // ============================================================
  describe('Variant Type', () => {
    const variantSettings = { allow_experimental_variant_type: 1 };

    it('decodes Variant with String', async () => {
      const data = await query("SELECT 'hello'::Variant(String, UInt64) as val", variantSettings);
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe('hello');
    });

    it('decodes Variant with UInt64', async () => {
      const data = await query("SELECT 42::Variant(String, UInt64) as val", variantSettings);
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBe(42n);
    });

    it('decodes Variant NULL', async () => {
      const data = await query("SELECT NULL::Variant(String, UInt64) as val", variantSettings);
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBeNull();
    });
  });

  // ============================================================
  // DYNAMIC
  // ============================================================
  describe('Dynamic Type', () => {
    it('decodes Dynamic with integer', async () => {
      const data = await query("SELECT 42::Dynamic as val");
      const result = decode(data, 1, 1);
      // Dynamic in RowBinary uses binary type index
      expect(result.rows![0].values[0]).toBeDefined();
    });

    it('decodes Dynamic with string', async () => {
      const data = await query("SELECT 'hello'::Dynamic as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0]).toBeDefined();
    });

    it('decodes Dynamic NULL', async () => {
      const data = await query("SELECT NULL::Dynamic as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].value).toBeNull();
    });

    it('decodes Dynamic with all supported types via table', async () => {
      // Create table and insert one value of each type
      await query(`
        CREATE TABLE IF NOT EXISTS test_dynamic_types (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Insert values of various types using INSERT SELECT for complex types
      await query(`
        INSERT INTO test_dynamic_types SELECT 1, 42::UInt8
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
      const data = await query('SELECT val FROM test_dynamic_types ORDER BY id');
      const result = decode(data, 1, 29);

      // Verify each row has a value (or null for the NULL case)
      const values = result.rows!.map(r => r.values[0]);

      // Helper to get the decoded type from metadata
      const getDecodedType = (v: typeof values[0]) => v.metadata?.decodedType as string || '';

      // Check specific types were decoded via metadata.decodedType
      expect(getDecodedType(values[0])).toContain('UInt8');
      expect(values[0].value).toBe(42);

      expect(getDecodedType(values[1])).toContain('UInt16');
      expect(values[1].value).toBe(1000);

      expect(getDecodedType(values[2])).toContain('UInt32');
      expect(values[2].value).toBe(100000);

      expect(getDecodedType(values[3])).toContain('UInt64');
      expect(values[3].value).toBe(10000000000n);

      expect(getDecodedType(values[4])).toContain('Int8');
      expect(values[4].value).toBe(-42);

      expect(getDecodedType(values[5])).toContain('Int16');
      expect(values[5].value).toBe(-1000);

      expect(getDecodedType(values[6])).toContain('Int32');
      expect(values[6].value).toBe(-100000);

      expect(getDecodedType(values[7])).toContain('Int64');
      expect(values[7].value).toBe(-10000000000n);

      expect(getDecodedType(values[8])).toContain('Float32');
      expect(values[8].value).toBeCloseTo(3.14, 2);

      expect(getDecodedType(values[9])).toContain('Float64');
      expect(values[9].value).toBeCloseTo(2.718281828, 5);

      expect(getDecodedType(values[10])).toContain('String');
      expect(values[10].value).toBe('hello world');

      expect(getDecodedType(values[11])).toContain('Bool');
      expect(values[11].value).toBe(true);

      expect(getDecodedType(values[12])).toContain('Bool');
      expect(values[12].value).toBe(false);

      expect(getDecodedType(values[13])).toContain('Date');
      expect(values[13].displayValue).toContain('2024-01-15');

      expect(getDecodedType(values[14])).toContain('Date32');
      expect(values[14].displayValue).toContain('2024-06-20');

      expect(getDecodedType(values[15])).toContain('DateTime');
      expect(values[15].displayValue).toContain('2024-01-15');

      expect(getDecodedType(values[16])).toContain('DateTime64');
      expect(values[16].displayValue).toContain('2024-01-15');

      expect(getDecodedType(values[17])).toContain('UUID');
      expect(values[17].value).toBe('12345678-1234-5678-1234-567812345678');

      expect(getDecodedType(values[18])).toContain('IPv4');
      expect(values[18].value).toBe('192.168.1.1');

      expect(getDecodedType(values[19])).toContain('IPv6');
      expect(values[19].displayValue).toContain('1');

      expect(getDecodedType(values[20])).toContain('Array');
      expect(values[20].children).toBeDefined();

      expect(getDecodedType(values[21])).toContain('Tuple');
      expect(values[21].children).toBeDefined();

      expect(getDecodedType(values[22])).toContain('Map');
      expect(values[22].children).toBeDefined();

      // NULL
      expect(values[23].value).toBeNull();

      expect(getDecodedType(values[24])).toContain('Decimal32');
      expect(getDecodedType(values[25])).toContain('Decimal64');

      // BFloat16
      expect(getDecodedType(values[26])).toContain('BFloat16');
      expect(values[26].value).toBeCloseTo(1.5, 1);

      // Enum8
      expect(getDecodedType(values[27])).toContain('Enum8');
      expect(values[27].displayValue).toContain('active');

      // Enum16
      expect(getDecodedType(values[28])).toContain('Enum16');
      expect(values[28].displayValue).toContain('pending');

      // Cleanup
      await query('DROP TABLE IF EXISTS test_dynamic_types');
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
      expect(result.rows![0].values[0]).toBeDefined();
    });

    it('decodes JSON with typed paths', async () => {
      const data = await query("SELECT '{\"id\": 42, \"name\": \"test\"}'::JSON(id UInt32, name String) as val", jsonSettings);
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0]).toBeDefined();
    });
  });

  // ============================================================
  // GEO TYPES
  // ============================================================
  describe('Geo Types', () => {
    it('decodes Point', async () => {
      const data = await query("SELECT (1.5, 2.5)::Point as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children![0].value).toBeCloseTo(1.5);
      expect(result.rows![0].values[0].children![1].value).toBeCloseTo(2.5);
    });

    it('decodes Ring (Array of Points)', async () => {
      const data = await query("SELECT [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]::Ring as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(3);
    });

    it('decodes Polygon (Array of Rings)', async () => {
      const data = await query("SELECT [[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]]::Polygon as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(1);
    });

    it('decodes MultiPolygon', async () => {
      const data = await query("SELECT [[[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]]]::MultiPolygon as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(1);
    });

    it('decodes LineString', async () => {
      const data = await query("SELECT [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]::LineString as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(3);
    });

    it('decodes MultiLineString', async () => {
      const data = await query("SELECT [[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]::MultiLineString as val");
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(2);
    });

    it('decodes Geometry as Point', async () => {
      const data = await query("SELECT ((1.0, 2.0)::Point)::Geometry as val", { allow_suspicious_variant_types: 1 });
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].metadata?.geoType).toBe('Point');
    });
  });

  // ============================================================
  // QBIT
  // ============================================================
  describe('QBit Type', () => {
    const qbitSettings = { allow_experimental_qbit_type: 1 };

    it('decodes QBit(Float32, 3) values', async () => {
      const data = await query("SELECT [1.0, 2.0, 3.0]::QBit(Float32, 3) as val", qbitSettings);
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(3);
      expect(result.rows![0].values[0].children![0].value).toBeCloseTo(1.0, 5);
      expect(result.rows![0].values[0].children![1].value).toBeCloseTo(2.0, 5);
      expect(result.rows![0].values[0].children![2].value).toBeCloseTo(3.0, 5);
    });
  });

  // ============================================================
  // AGGREGATE FUNCTION STATE
  // ============================================================
  describe('AggregateFunction Type', () => {
    it('decodes avgState aggregate function', async () => {
      const data = await query("SELECT avgState(number) FROM numbers(10)");
      const result = decode(data, 1, 1);

      const node = result.rows![0].values[0];
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

      const node = result.rows![0].values[0];
      expect(node.type).toBe('AggregateFunction(sum, UInt64)');
      expect(node.displayValue).toContain('sum=45');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].label).toBe('sum');
      expect(node.children![0].value).toBe(45n);
    });

    it('decodes countState aggregate function', async () => {
      const data = await query("SELECT countState() FROM numbers(10)");
      const result = decode(data, 1, 1);

      const node = result.rows![0].values[0];
      expect(node.type).toBe('AggregateFunction(count)');
      expect(node.displayValue).toBe('count=10');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].label).toBe('count');
      expect(node.children![0].value).toBe(10);
    });

    it('decodes sumState with Float64', async () => {
      const data = await query("SELECT sumState(toFloat64(number)) FROM numbers(10)");
      const result = decode(data, 1, 1);

      const node = result.rows![0].values[0];
      expect(node.type).toBe('AggregateFunction(sum, Float64)');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].label).toBe('sum');
      // sum of 0+1+...+9 = 45.0
      expect(node.children![0].value).toBeCloseTo(45.0, 5);
    });

    it('decodes avgState with Float64', async () => {
      const data = await query("SELECT avgState(toFloat64(number)) FROM numbers(10)");
      const result = decode(data, 1, 1);

      const node = result.rows![0].values[0];
      expect(node.type).toBe('AggregateFunction(avg, Float64)');
      expect(node.displayValue).toContain('avg=4.50');
      expect(node.children).toHaveLength(2);
      expect(node.children![0].label).toBe('numerator (sum)');
      // For Float64, numerator is also Float64
      expect(node.children![0].value).toBeCloseTo(45.0, 5);
      expect(node.children![1].label).toBe('denominator (count)');
      expect(node.children![1].value).toBe(10);
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
      expect(result.rows![0].values[0].value).toBe(42);
      expect(result.rows![0].values[1].value).toBe('hello');
      expect(result.rows![0].values[2].value).toBe(true);
      expect(result.rows![0].values[3].value).toBeCloseTo(3.14);
    });

    it('decodes many rows', async () => {
      const data = await query('SELECT number::UInt32 as val FROM numbers(100)');
      const result = decode(data, 1, 100);
      for (let i = 0; i < 100; i++) {
        expect(result.rows![i].values[0].value).toBe(i);
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

      for (let i = 0; i < 10; i++) {
        expect(result.rows![i].values[0].value).toBe(i);
        expect(result.rows![i].values[1].value).toBe(`item_${i}`);
      }
    });
  });

  // ============================================================
  // COMPLEX NESTED STRUCTURE
  // ============================================================
  describe('Complex Nested Structures', () => {
    it('decodes deeply nested structure', async () => {
      const data = await query(`
        SELECT
          (
            1::UInt32,
            'test'::String,
            [1, 2, 3]::Array(UInt8),
            map('key', (10, 'nested')::Tuple(id UInt32, name String))
          )::Tuple(
            id UInt32,
            name String,
            values Array(UInt8),
            metadata Map(String, Tuple(id UInt32, name String))
          ) as val
      `);
      const result = decode(data, 1, 1);
      expect(result.rows![0].values[0].children).toHaveLength(4);
    });
  });
}, 300000);
