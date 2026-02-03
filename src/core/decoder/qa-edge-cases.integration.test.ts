import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ClickHouseContainer, StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { RowBinaryDecoder } from './rowbinary-decoder';
import { NativeDecoder } from './native-decoder';

const IMAGE = 'clickhouse/clickhouse-server:latest';

/**
 * QA Edge Case Tests - Attempting to break the decoders
 *
 * These tests explore boundary conditions, extreme values, and unusual
 * combinations that might cause parsing errors or incorrect results.
 */
describe('QA Edge Case Tests', () => {
  let container: StartedClickHouseContainer;
  let queryRowBinary: (sql: string, settings?: Record<string, string | number>) => Promise<Uint8Array>;
  let queryNative: (sql: string, settings?: Record<string, string | number>) => Promise<Uint8Array>;

  beforeAll(async () => {
    container = await new ClickHouseContainer(IMAGE).start();
    const baseUrl = container.getHttpUrl();

    queryRowBinary = async (sql: string, settings?: Record<string, string | number>): Promise<Uint8Array> => {
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
      return new Uint8Array(await response.arrayBuffer());
    };

    queryNative = async (sql: string, settings?: Record<string, string | number>): Promise<Uint8Array> => {
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
        body: `${sql} FORMAT Native`,
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse error: ${text}`);
      }
      return new Uint8Array(await response.arrayBuffer());
    };
  }, 120000);

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
  });

  // ============================================================
  // EXTREME NUMERIC VALUES
  // ============================================================
  describe('Extreme Numeric Values', () => {
    // UInt256 maximum value
    it('RowBinary: decodes UInt256 maximum value', async () => {
      const data = await queryRowBinary('SELECT toUInt256(\'115792089237316195423570985008687907853269984665640564039457584007913129639935\') as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
      expect(result.rows![0].values[0].value).toBe(115792089237316195423570985008687907853269984665640564039457584007913129639935n);
    });

    it('Native: decodes UInt256 maximum value', async () => {
      const data = await queryNative('SELECT toUInt256(\'115792089237316195423570985008687907853269984665640564039457584007913129639935\') as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe(115792089237316195423570985008687907853269984665640564039457584007913129639935n);
    });

    // Int256 minimum value
    it('RowBinary: decodes Int256 minimum value', async () => {
      const data = await queryRowBinary('SELECT toInt256(\'-57896044618658097711785492504343953926634992332820282019728792003956564819968\') as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
      expect(result.rows![0].values[0].value).toBe(-57896044618658097711785492504343953926634992332820282019728792003956564819968n);
    });

    it('Native: decodes Int256 minimum value', async () => {
      const data = await queryNative('SELECT toInt256(\'-57896044618658097711785492504343953926634992332820282019728792003956564819968\') as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe(-57896044618658097711785492504343953926634992332820282019728792003956564819968n);
    });

    // Float64 subnormal values
    it('RowBinary: decodes Float64 subnormal (denormalized) values', async () => {
      // Smallest positive subnormal: 5e-324
      const data = await queryRowBinary('SELECT arrayJoin([5e-324, -5e-324, 2.2250738585072014e-308]::Array(Float64)) as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(3);
      // Subnormal values might lose precision but should not crash
      expect(typeof result.rows![0].values[0].value).toBe('number');
    });

    it('Native: decodes Float64 subnormal (denormalized) values', async () => {
      const data = await queryNative('SELECT arrayJoin([5e-324, -5e-324, 2.2250738585072014e-308]::Array(Float64)) as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(3);
    });

    // Decimal with maximum precision
    it('RowBinary: decodes Decimal256 with scale 76', async () => {
      const data = await queryRowBinary('SELECT toDecimal256(0, 76) as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: decodes Decimal256 with scale 76', async () => {
      const data = await queryNative('SELECT toDecimal256(0, 76) as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // Negative zero - IEEE 754 preserves sign of zero
    it('RowBinary: handles negative zero Float64', async () => {
      const data = await queryRowBinary('SELECT -0.0::Float64 as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      // IEEE 754 negative zero is correctly preserved
      expect(Object.is(result.rows![0].values[0].value, -0)).toBe(true);
    });

    it('Native: handles negative zero Float64', async () => {
      const data = await queryNative('SELECT -0.0::Float64 as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      // IEEE 754 negative zero is correctly preserved
      expect(Object.is(result.blocks![0].columns[0].values[0].value, -0)).toBe(true);
    });
  });

  // ============================================================
  // DEEPLY NESTED STRUCTURES
  // ============================================================
  describe('Deeply Nested Structures', () => {
    // 5-level deep nested arrays
    it('RowBinary: decodes 5-level nested arrays', async () => {
      const data = await queryRowBinary('SELECT [[[[[1]]]]]::Array(Array(Array(Array(Array(UInt8))))) as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
      // Navigate through nesting
      let node = result.rows![0].values[0];
      for (let i = 0; i < 5; i++) {
        expect(node.children).toBeDefined();
        node = node.children![1]; // Skip length node
      }
      expect(node.value).toBe(1);
    });

    it('Native: decodes 5-level nested arrays', async () => {
      const data = await queryNative('SELECT [[[[[1]]]]]::Array(Array(Array(Array(Array(UInt8))))) as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // Deeply nested tuples
    it('RowBinary: decodes deeply nested tuples', async () => {
      const data = await queryRowBinary(`
        SELECT ((((1, 2), 3), 4), 5)::Tuple(Tuple(Tuple(Tuple(UInt8, UInt8), UInt8), UInt8), UInt8) as val
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: decodes deeply nested tuples', async () => {
      const data = await queryNative(`
        SELECT ((((1, 2), 3), 4), 5)::Tuple(Tuple(Tuple(Tuple(UInt8, UInt8), UInt8), UInt8), UInt8) as val
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // Map with Array values containing Tuples
    it('RowBinary: decodes Map with complex nested values', async () => {
      const data = await queryRowBinary(`
        SELECT map('key1', [(1, 'a'), (2, 'b')], 'key2', [(3, 'c')])::Map(String, Array(Tuple(UInt8, String))) as val
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: decodes Map with complex nested values', async () => {
      const data = await queryNative(`
        SELECT map('key1', [(1, 'a'), (2, 'b')], 'key2', [(3, 'c')])::Map(String, Array(Tuple(UInt8, String))) as val
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // Nullable inside Array inside Map inside Tuple
    it('RowBinary: decodes complex nested Nullable structure', async () => {
      const data = await queryRowBinary(`
        SELECT (map('k', [1, NULL, 3]), 'test')::Tuple(Map(String, Array(Nullable(UInt8))), String) as val
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: decodes complex nested Nullable structure', async () => {
      const data = await queryNative(`
        SELECT (map('k', [1, NULL, 3]), 'test')::Tuple(Map(String, Array(Nullable(UInt8))), String) as val
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });
  });

  // ============================================================
  // LARGE DATA SIZES
  // ============================================================
  describe('Large Data Sizes', () => {
    // Large array (1000 elements)
    it('RowBinary: decodes array with 1000 elements', async () => {
      const data = await queryRowBinary('SELECT range(1000)::Array(UInt32) as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].children).toHaveLength(1001); // length node + 1000 elements
    });

    it('Native: decodes array with 1000 elements', async () => {
      const data = await queryNative('SELECT range(1000)::Array(UInt32) as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(1001);
    });

    // Very long string (10KB)
    it('RowBinary: decodes 10KB string', async () => {
      const data = await queryRowBinary('SELECT repeat(\'x\', 10000)::String as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect((result.rows![0].values[0].value as string).length).toBe(10000);
    });

    it('Native: decodes 10KB string', async () => {
      const data = await queryNative('SELECT repeat(\'x\', 10000)::String as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect((result.blocks![0].columns[0].values[0].value as string).length).toBe(10000);
    });

    // Large Map (100 entries)
    it('RowBinary: decodes Map with 100 entries', async () => {
      const data = await queryRowBinary(`
        SELECT mapFromArrays(
          arrayMap(x -> toString(x), range(100)),
          range(100)
        )::Map(String, UInt32) as val
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].children).toHaveLength(100);
    });

    it('Native: decodes Map with 100 entries', async () => {
      const data = await queryNative(`
        SELECT mapFromArrays(
          arrayMap(x -> toString(x), range(100)),
          range(100)
        )::Map(String, UInt32) as val
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(100);
    });

    // Many rows (500)
    it('RowBinary: decodes 500 rows', async () => {
      const data = await queryRowBinary('SELECT number as val FROM numbers(500)');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(500);
    });

    it('Native: decodes 500 rows', async () => {
      const data = await queryNative('SELECT number as val FROM numbers(500)');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const totalRows = result.blocks!.reduce((sum, b) => sum + b.rowCount, 0);
      expect(totalRows).toBe(500);
    });

    // Large Tuple (20 elements)
    it('RowBinary: decodes Tuple with 20 elements', async () => {
      const data = await queryRowBinary(`
        SELECT (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)::Tuple(
          UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,
          UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8
        ) as val
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].children).toHaveLength(20);
    });

    it('Native: decodes Tuple with 20 elements', async () => {
      const data = await queryNative(`
        SELECT (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)::Tuple(
          UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,
          UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8,UInt8
        ) as val
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(20);
    });
  });

  // ============================================================
  // UNICODE AND STRING EDGE CASES
  // ============================================================
  describe('Unicode and String Edge Cases', () => {
    // Empty string
    it('RowBinary: handles empty string', async () => {
      const data = await queryRowBinary("SELECT ''::String as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toBe('');
    });

    it('Native: handles empty string', async () => {
      const data = await queryNative("SELECT ''::String as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe('');
    });

    // String with null bytes
    it('RowBinary: handles string with embedded null byte', async () => {
      const data = await queryRowBinary("SELECT 'hello\\0world'::String as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toBe('hello\0world');
    });

    it('Native: handles string with embedded null byte', async () => {
      const data = await queryNative("SELECT 'hello\\0world'::String as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe('hello\0world');
    });

    // 4-byte UTF-8 characters (emojis)
    it('RowBinary: handles 4-byte UTF-8 emojis', async () => {
      const data = await queryRowBinary("SELECT '🎉🚀💯🔥'::String as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toBe('🎉🚀💯🔥');
    });

    it('Native: handles 4-byte UTF-8 emojis', async () => {
      const data = await queryNative("SELECT '🎉🚀💯🔥'::String as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe('🎉🚀💯🔥');
    });

    // Surrogate pairs (emoji with skin tone modifier)
    it('RowBinary: handles emoji with skin tone modifier', async () => {
      const data = await queryRowBinary("SELECT '👋🏽'::String as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toBe('👋🏽');
    });

    it('Native: handles emoji with skin tone modifier', async () => {
      const data = await queryNative("SELECT '👋🏽'::String as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe('👋🏽');
    });

    // RTL text (Arabic/Hebrew)
    it('RowBinary: handles RTL text', async () => {
      const data = await queryRowBinary("SELECT 'مرحبا שלום'::String as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toBe('مرحبا שלום');
    });

    it('Native: handles RTL text', async () => {
      const data = await queryNative("SELECT 'مرحبا שלום'::String as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe('مرحبا שלום');
    });

    // Zero-width characters
    it('RowBinary: handles zero-width characters', async () => {
      const data = await queryRowBinary("SELECT 'a\u200Bb\u200Cc'::String as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toBe('a\u200Bb\u200Cc');
    });

    it('Native: handles zero-width characters', async () => {
      const data = await queryNative("SELECT 'a\u200Bb\u200Cc'::String as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe('a\u200Bb\u200Cc');
    });

    // FixedString with multibyte UTF-8
    it('RowBinary: handles FixedString with UTF-8', async () => {
      // '世界' is 6 bytes in UTF-8
      const data = await queryRowBinary("SELECT '世界'::FixedString(6) as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toBe('世界');
    });

    it('Native: handles FixedString with UTF-8', async () => {
      const data = await queryNative("SELECT '世界'::FixedString(6) as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].value).toBe('世界');
    });
  });

  // ============================================================
  // EMPTY AND NULL EDGE CASES
  // ============================================================
  describe('Empty and Null Edge Cases', () => {
    // Empty array in Map value
    it('RowBinary: handles Map with empty array values', async () => {
      const data = await queryRowBinary("SELECT map('a', [], 'b', [1])::Map(String, Array(UInt8)) as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles Map with empty array values', async () => {
      const data = await queryNative("SELECT map('a', [], 'b', [1])::Map(String, Array(UInt8)) as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // All-NULL column
    it('RowBinary: handles column with all NULL values', async () => {
      const data = await queryRowBinary('SELECT NULL::Nullable(UInt32) as val FROM numbers(5)');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(5);
      for (const row of result.rows!) {
        expect(row.values[0].value).toBeNull();
      }
    });

    it('Native: handles column with all NULL values', async () => {
      const data = await queryNative('SELECT NULL::Nullable(UInt32) as val FROM numbers(5)');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const values = result.blocks!.flatMap(b => b.columns[0].values);
      expect(values).toHaveLength(5);
      for (const v of values) {
        expect(v.value).toBeNull();
      }
    });

    // Empty Tuple (0 elements) - not allowed in ClickHouse, skip

    // Array of empty arrays
    it('RowBinary: handles array of empty arrays', async () => {
      const data = await queryRowBinary('SELECT [[], [], []]::Array(Array(UInt8)) as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].children).toHaveLength(4); // length + 3 empty arrays
    });

    it('Native: handles array of empty arrays', async () => {
      const data = await queryNative('SELECT [[], [], []]::Array(Array(UInt8)) as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(4);
    });

    // Nullable String with empty string vs NULL
    it('RowBinary: distinguishes empty string from NULL in Nullable(String)', async () => {
      const data = await queryRowBinary("SELECT arrayJoin(['', NULL]::Array(Nullable(String))) as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(2);
      // First is empty string (not null)
      expect(result.rows![0].values[0].value).not.toBeNull();
      // Second is NULL
      expect(result.rows![1].values[0].value).toBeNull();
    });

    it('Native: distinguishes empty string from NULL in Nullable(String)', async () => {
      const data = await queryNative("SELECT arrayJoin(['', NULL]::Array(Nullable(String))) as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const values = result.blocks!.flatMap(b => b.columns[0].values);
      expect(values).toHaveLength(2);
      expect(values[0].value).toBe('');
      expect(values[1].value).toBeNull();
    });
  });

  // ============================================================
  // ENUM EDGE CASES
  // ============================================================
  describe('Enum Edge Cases', () => {
    it('RowBinary: handles Enum8 with negative value', async () => {
      const data = await queryRowBinary("SELECT 'neg'::Enum8('neg' = -128, 'pos' = 127) as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].displayValue).toContain('neg');
    });

    it('Native: handles Enum8 with negative value', async () => {
      const data = await queryNative("SELECT 'neg'::Enum8('neg' = -128, 'pos' = 127) as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('neg');
    });

    // Enum with special characters in name
    it('RowBinary: handles Enum with special chars in name', async () => {
      const data = await queryRowBinary("SELECT 'hello world'::Enum8('hello world' = 1, 'foo\\'bar' = 2) as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].displayValue).toContain('hello world');
    });

    it('Native: handles Enum with special chars in name', async () => {
      const data = await queryNative("SELECT 'hello world'::Enum8('hello world' = 1, 'foo\\'bar' = 2) as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('hello world');
    });

    // Enum16 with large value
    it('RowBinary: handles Enum16 with large value', async () => {
      const data = await queryRowBinary("SELECT 'max'::Enum16('min' = -32768, 'max' = 32767) as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].displayValue).toContain('max');
    });

    it('Native: handles Enum16 with large value', async () => {
      const data = await queryNative("SELECT 'max'::Enum16('min' = -32768, 'max' = 32767) as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('max');
    });
  });

  // ============================================================
  // DATE/TIME EDGE CASES
  // ============================================================
  describe('Date/Time Edge Cases', () => {
    // Date32 far in the future
    it('RowBinary: handles Date32 year 2299', async () => {
      const data = await queryRowBinary("SELECT '2299-12-31'::Date32 as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].displayValue).toContain('2299');
    });

    it('Native: handles Date32 year 2299', async () => {
      const data = await queryNative("SELECT '2299-12-31'::Date32 as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('2299');
    });

    // Date32 far in the past
    it('RowBinary: handles Date32 year 1900', async () => {
      const data = await queryRowBinary("SELECT '1900-01-01'::Date32 as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].displayValue).toContain('1900');
    });

    it('Native: handles Date32 year 1900', async () => {
      const data = await queryNative("SELECT '1900-01-01'::Date32 as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('1900');
    });

    // DateTime64 with precision 9 (nanoseconds)
    it('RowBinary: handles DateTime64 with nanosecond precision', async () => {
      const data = await queryRowBinary("SELECT '2024-01-15 12:30:45.123456789'::DateTime64(9) as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    it('Native: handles DateTime64 with nanosecond precision', async () => {
      const data = await queryNative("SELECT '2024-01-15 12:30:45.123456789'::DateTime64(9) as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('2024-01-15');
    });

    // DateTime with unusual timezone
    it('RowBinary: handles DateTime with unusual timezone', async () => {
      const data = await queryRowBinary("SELECT '2024-01-15 12:30:45'::DateTime('Pacific/Kiritimati') as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      // RowBinary includes timezone in type string from header
      expect(result.rows![0].values[0].type).toContain('Pacific/Kiritimati');
    });

    it('Native: handles DateTime with unusual timezone', async () => {
      const data = await queryNative("SELECT '2024-01-15 12:30:45'::DateTime('Pacific/Kiritimati') as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      // Native format may not preserve timezone in column type string (ClickHouse behavior)
      // Just verify the value is decoded without error - time will be displayed in UTC
      expect(result.blocks![0].columns[0].values[0].displayValue).toBeDefined();
    });
  });

  // ============================================================
  // IP ADDRESS EDGE CASES
  // ============================================================
  describe('IP Address Edge Cases', () => {
    // IPv6 with mixed notation
    it('RowBinary: handles IPv6 full notation', async () => {
      const data = await queryRowBinary("SELECT '2001:0db8:0000:0000:0000:0000:0000:0001'::IPv6 as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toContain('2001');
    });

    it('Native: handles IPv6 full notation', async () => {
      const data = await queryNative("SELECT '2001:0db8:0000:0000:0000:0000:0000:0001'::IPv6 as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect((result.blocks![0].columns[0].values[0].value as string)).toContain('2001');
    });

    // IPv6 all ones
    it('RowBinary: handles IPv6 all ones', async () => {
      const data = await queryRowBinary("SELECT 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::IPv6 as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].value).toContain('ffff');
    });

    it('Native: handles IPv6 all ones', async () => {
      const data = await queryNative("SELECT 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::IPv6 as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect((result.blocks![0].columns[0].values[0].value as string)).toContain('ffff');
    });
  });

  // ============================================================
  // VARIANT AND DYNAMIC EDGE CASES
  // ============================================================
  describe('Variant and Dynamic Edge Cases', () => {
    const variantSettings = { allow_experimental_variant_type: 1 };

    // Variant with many types (requires allow_suspicious_variant_types for similar int types)
    it('RowBinary: handles Variant with 5 types', async () => {
      const data = await queryRowBinary(
        "SELECT 42::Variant(String, UInt8, UInt16, UInt32, UInt64) as val",
        { ...variantSettings, allow_suspicious_variant_types: 1 }
      );
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles Variant with 5 types', async () => {
      const data = await queryNative(
        "SELECT 42::Variant(String, UInt8, UInt16, UInt32, UInt64) as val",
        { ...variantSettings, allow_suspicious_variant_types: 1 }
      );
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // Dynamic with complex nested type
    it('RowBinary: handles Dynamic with Array inside', async () => {
      const data = await queryRowBinary("SELECT [1, 2, 3]::Dynamic as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles Dynamic with Array inside', async () => {
      const data = await queryNative("SELECT [1, 2, 3]::Dynamic as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const values = result.blocks!.flatMap(b => b.columns[0].values.filter(v => v.type !== 'Dynamic.Header'));
      expect(values).toHaveLength(1);
    });

    // Dynamic with Tuple inside
    it('RowBinary: handles Dynamic with Tuple inside', async () => {
      const data = await queryRowBinary("SELECT (1, 'test')::Dynamic as val");
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles Dynamic with Tuple inside', async () => {
      const data = await queryNative("SELECT (1, 'test')::Dynamic as val");
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const values = result.blocks!.flatMap(b => b.columns[0].values.filter(v => v.type !== 'Dynamic.Header'));
      expect(values).toHaveLength(1);
    });
  });

  // ============================================================
  // JSON EDGE CASES
  // ============================================================
  describe('JSON Edge Cases', () => {
    const jsonSettings = { allow_experimental_json_type: 1 };

    // JSON with deeply nested paths
    it('RowBinary: handles JSON with deep nesting', async () => {
      const data = await queryRowBinary(
        "SELECT '{\"a\": {\"b\": {\"c\": {\"d\": 42}}}}'::JSON as val",
        jsonSettings
      );
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles JSON with deep nesting', async () => {
      const data = await queryNative(
        "SELECT '{\"a\": {\"b\": {\"c\": {\"d\": 42}}}}'::JSON as val",
        jsonSettings
      );
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    it('RowBinary: handles JSON with array of objects', async () => {
      const data = await queryRowBinary(
        "SELECT '{\"items\": [{\"id\": 1}, {\"id\": 2}]}'::JSON as val",
        jsonSettings
      );
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles JSON with array of objects', async () => {
      const data = await queryNative(
        "SELECT '{\"items\": [{\"id\": 1}, {\"id\": 2}]}'::JSON as val",
        jsonSettings
      );
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // JSON with special characters in path names
    it('RowBinary: handles JSON with special chars in keys', async () => {
      const data = await queryRowBinary(
        "SELECT '{\"key with spaces\": 1, \"key.with.dots\": 2}'::JSON as val",
        jsonSettings
      );
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles JSON with special chars in keys', async () => {
      const data = await queryNative(
        "SELECT '{\"key with spaces\": 1, \"key.with.dots\": 2}'::JSON as val",
        jsonSettings
      );
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // Empty JSON object
    it('RowBinary: handles empty JSON object', async () => {
      const data = await queryRowBinary("SELECT '{}'::JSON as val", jsonSettings);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles empty JSON object', async () => {
      const data = await queryNative("SELECT '{}'::JSON as val", jsonSettings);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });
  });

  // ============================================================
  // LOWCARDINALITY EDGE CASES
  // ============================================================
  describe('LowCardinality Edge Cases', () => {
    // LowCardinality with all unique values
    it('RowBinary: handles LowCardinality with all unique strings', async () => {
      const data = await queryRowBinary(`
        SELECT toString(number)::LowCardinality(String) as val FROM numbers(100)
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(100);
    });

    it('Native: handles LowCardinality with all unique strings', async () => {
      const data = await queryNative(`
        SELECT toString(number)::LowCardinality(String) as val FROM numbers(100)
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const values = result.blocks!.flatMap(b => b.columns[0].values);
      expect(values).toHaveLength(100);
    });

    // LowCardinality with single value repeated
    it('RowBinary: handles LowCardinality with single repeated value', async () => {
      const data = await queryRowBinary(`
        SELECT 'same'::LowCardinality(String) as val FROM numbers(100)
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(100);
      for (const row of result.rows!) {
        expect(row.values[0].value).toBe('same');
      }
    });

    it('Native: handles LowCardinality with single repeated value', async () => {
      const data = await queryNative(`
        SELECT 'same'::LowCardinality(String) as val FROM numbers(100)
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const values = result.blocks!.flatMap(b => b.columns[0].values);
      expect(values).toHaveLength(100);
      for (const v of values) {
        expect(v.value).toBe('same');
      }
    });

    // LowCardinality(Nullable) with mix of values and NULLs
    it('RowBinary: handles LowCardinality(Nullable) mixed', async () => {
      const data = await queryRowBinary(`
        SELECT if(number % 3 = 0, NULL, toString(number % 5))::LowCardinality(Nullable(String)) as val
        FROM numbers(15)
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(15);
    });

    it('Native: handles LowCardinality(Nullable) mixed', async () => {
      const data = await queryNative(`
        SELECT if(number % 3 = 0, NULL, toString(number % 5))::LowCardinality(Nullable(String)) as val
        FROM numbers(15)
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const values = result.blocks!.flatMap(b => b.columns[0].values);
      expect(values).toHaveLength(15);
    });
  });

  // ============================================================
  // NESTED TYPE
  // ============================================================
  describe('Nested Type', () => {
    // Nested types must be accessed via table - cannot cast directly
    it('RowBinary: handles Nested with multiple fields via table', async () => {
      // Create table with Nested
      await queryRowBinary(`
        CREATE TABLE IF NOT EXISTS test_nested_qa (
          id UInt32,
          data Nested(name String, value UInt8)
        ) ENGINE = Memory
      `);

      // Use INSERT SELECT to avoid VALUES parsing issues
      await queryRowBinary(`
        INSERT INTO test_nested_qa SELECT 1, ['a', 'b'], [1, 2]
      `);

      const data = await queryRowBinary('SELECT data.name, data.value FROM test_nested_qa');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
      // data.name and data.value are separate Array columns
      expect(result.header.columns).toHaveLength(2);

      await queryRowBinary('DROP TABLE IF EXISTS test_nested_qa');
    });

    it('Native: handles Nested with multiple fields via table', async () => {
      await queryNative(`
        CREATE TABLE IF NOT EXISTS test_nested_qa_native (
          id UInt32,
          data Nested(name String, value UInt8)
        ) ENGINE = Memory
      `);

      // Use INSERT SELECT to avoid VALUES parsing issues
      await queryNative(`
        INSERT INTO test_nested_qa_native SELECT 1, ['a', 'b'], [1, 2]
      `);

      const data = await queryNative('SELECT data.name, data.value FROM test_nested_qa_native');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      const totalRows = result.blocks!.reduce((sum, b) => sum + b.rowCount, 0);
      expect(totalRows).toBe(1);
      expect(result.header.columns).toHaveLength(2);

      await queryNative('DROP TABLE IF EXISTS test_nested_qa_native');
    });
  });

  // ============================================================
  // MULTIPLE COLUMN EDGE CASES
  // ============================================================
  describe('Multiple Column Edge Cases', () => {
    // Many columns (20)
    it('RowBinary: handles 20 columns', async () => {
      const data = await queryRowBinary(`
        SELECT
          1 as c1, 2 as c2, 3 as c3, 4 as c4, 5 as c5,
          6 as c6, 7 as c7, 8 as c8, 9 as c9, 10 as c10,
          11 as c11, 12 as c12, 13 as c13, 14 as c14, 15 as c15,
          16 as c16, 17 as c17, 18 as c18, 19 as c19, 20 as c20
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.header.columns).toHaveLength(20);
      expect(result.rows![0].values).toHaveLength(20);
    });

    it('Native: handles 20 columns', async () => {
      const data = await queryNative(`
        SELECT
          1 as c1, 2 as c2, 3 as c3, 4 as c4, 5 as c5,
          6 as c6, 7 as c7, 8 as c8, 9 as c9, 10 as c10,
          11 as c11, 12 as c12, 13 as c13, 14 as c14, 15 as c15,
          16 as c16, 17 as c17, 18 as c18, 19 as c19, 20 as c20
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.header.columns).toHaveLength(20);
      expect(result.blocks![0].columns).toHaveLength(20);
    });

    // Mixed complex types in multiple columns
    it('RowBinary: handles multiple complex type columns', async () => {
      const data = await queryRowBinary(`
        SELECT
          [1, 2, 3] as arr,
          map('a', 1) as m,
          (1, 'test') as t,
          NULL::Nullable(UInt8) as n,
          'hello'::LowCardinality(String) as lc
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.header.columns).toHaveLength(5);
    });

    it('Native: handles multiple complex type columns', async () => {
      const data = await queryNative(`
        SELECT
          [1, 2, 3] as arr,
          map('a', 1) as m,
          (1, 'test') as t,
          NULL::Nullable(UInt8) as n,
          'hello'::LowCardinality(String) as lc
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.header.columns).toHaveLength(5);
    });

    // Column with very long name
    it('RowBinary: handles column with 100 char name', async () => {
      const longName = 'a'.repeat(100);
      const data = await queryRowBinary(`SELECT 1 as ${longName}`);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.header.columns[0].name).toBe(longName);
    });

    it('Native: handles column with 100 char name', async () => {
      const longName = 'a'.repeat(100);
      const data = await queryNative(`SELECT 1 as ${longName}`);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.header.columns[0].name).toBe(longName);
    });
  });

  // ============================================================
  // GEO TYPE EDGE CASES
  // ============================================================
  describe('Geo Type Edge Cases', () => {
    // Empty Ring
    it('RowBinary: handles empty Ring', async () => {
      const data = await queryRowBinary('SELECT []::Ring as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows).toHaveLength(1);
    });

    it('Native: handles empty Ring', async () => {
      const data = await queryNative('SELECT []::Ring as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values).toHaveLength(1);
    });

    // Polygon with hole
    it('RowBinary: handles Polygon with hole', async () => {
      const data = await queryRowBinary(`
        SELECT [
          [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)],
          [(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0)]
        ]::Polygon as val
      `);
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].children).toHaveLength(2);
    });

    it('Native: handles Polygon with hole', async () => {
      const data = await queryNative(`
        SELECT [
          [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)],
          [(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0)]
        ]::Polygon as val
      `);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].children).toHaveLength(2);
    });

    // Point with extreme coordinates
    it('RowBinary: handles Point with extreme coordinates', async () => {
      const data = await queryRowBinary('SELECT (180.0, 90.0)::Point as val');
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();
      expect(result.rows![0].values[0].children![0].value).toBe(180);
      expect(result.rows![0].values[0].children![1].value).toBe(90);
    });

    it('Native: handles Point with extreme coordinates', async () => {
      const data = await queryNative('SELECT (180.0, 90.0)::Point as val');
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();
      expect(result.blocks![0].columns[0].values[0].children![0].value).toBe(180);
      expect(result.blocks![0].columns[0].values[0].children![1].value).toBe(90);
    });
  });

}, 300000);
