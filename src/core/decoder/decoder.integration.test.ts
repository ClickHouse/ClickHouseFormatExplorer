import { describe, it, expect } from 'vitest';
import { ClickHouseContainer } from '@testcontainers/clickhouse';
import { RowBinaryDecoder } from './decoder';

const IMAGE = 'clickhouse/clickhouse-server:latest';

describe('RowBinaryDecoder Integration Tests', () => {
  it('decodes all ClickHouse types correctly', async () => {
    await using container = await new ClickHouseContainer(IMAGE).start();
    const url = container.getHttpUrl();

    async function query(sql: string): Promise<Uint8Array> {
      const response = await fetch(`${url}/?user=${container.getUsername()}&password=${container.getPassword()}`, {
        method: 'POST',
        body: `${sql} FORMAT RowBinaryWithNamesAndTypes`,
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse error: ${text}`);
      }
      const buffer = await response.arrayBuffer();
      return new Uint8Array(buffer);
    }

    function decode(data: Uint8Array, expectedColumns: number, expectedRows: number) {
      const decoder = new RowBinaryDecoder(data);
      const result = decoder.decode();

      expect(result.header.columns).toHaveLength(expectedColumns);
      expect(result.rows).toHaveLength(expectedRows);

      // Verify all nodes have valid byte ranges
      for (const row of result.rows) {
        for (const node of row.values) {
          expect(node.byteRange.start).toBeLessThan(node.byteRange.end);
          expect(node.byteRange.end).toBeLessThanOrEqual(data.length);
        }
      }

      return result;
    }

    // Integer types
    {
      const data = await query('SELECT 42::UInt8 as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(42);
    }

    {
      const data = await query('SELECT 1234::UInt16 as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(1234);
    }

    {
      const data = await query('SELECT 123456::UInt32 as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(123456);
    }

    {
      const data = await query('SELECT 9223372036854775807::UInt64 as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(9223372036854775807n);
    }

    {
      const data = await query('SELECT 123456789012345678901234567890::UInt128 as val');
      decode(data, 1, 1);
    }

    {
      const data = await query('SELECT 123456789012345678901234567890::UInt256 as val');
      decode(data, 1, 1);
    }

    {
      const data = await query('SELECT toInt8(-42) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(-42);
    }

    {
      const data = await query('SELECT toInt16(-1234) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(-1234);
    }

    {
      const data = await query('SELECT toInt32(-123456) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(-123456);
    }

    {
      const data = await query('SELECT toInt64(-9223372036854775807) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(-9223372036854775807n);
    }

    {
      const data = await query("SELECT toInt128('-123456789012345678901234567890') as val");
      decode(data, 1, 1);
    }

    {
      const data = await query("SELECT toInt256('-123456789012345678901234567890') as val");
      decode(data, 1, 1);
    }

    // Float types
    {
      const data = await query('SELECT 3.14::Float32 as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBeCloseTo(3.14, 2);
    }

    {
      const data = await query('SELECT 3.141592653589793::Float64 as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBeCloseTo(3.141592653589793, 10);
    }

    // String types
    {
      const data = await query("SELECT 'hello world'::String as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe('hello world');
    }

    {
      const data = await query("SELECT ''::String as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe('');
    }

    {
      const data = await query("SELECT 'abc'::FixedString(5) as val");
      const result = decode(data, 1, 1);
      // Decoder may trim trailing nulls or keep them
      expect(result.rows[0].values[0].value).toMatch(/^abc/);
    }

    // Boolean
    {
      const data = await query('SELECT true::Bool as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(true);
    }

    {
      const data = await query('SELECT false::Bool as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(false);
    }

    // Date/Time types
    {
      const data = await query("SELECT '2024-01-15'::Date as val");
      decode(data, 1, 1);
    }

    {
      const data = await query("SELECT '2024-01-15'::Date32 as val");
      decode(data, 1, 1);
    }

    {
      const data = await query("SELECT '2024-01-15 12:30:45'::DateTime as val");
      decode(data, 1, 1);
    }

    {
      const data = await query("SELECT '2024-01-15 12:30:45'::DateTime('UTC') as val");
      decode(data, 1, 1);
    }

    {
      const data = await query("SELECT '2024-01-15 12:30:45.123456'::DateTime64(6) as val");
      decode(data, 1, 1);
    }

    // UUID
    {
      const data = await query("SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe('550e8400-e29b-41d4-a716-446655440000');
    }

    // IP addresses
    {
      const data = await query("SELECT '192.168.1.1'::IPv4 as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe('192.168.1.1');
    }

    {
      const data = await query("SELECT '2001:db8::1'::IPv6 as val");
      decode(data, 1, 1);
    }

    // Decimal types
    {
      const data = await query('SELECT 123.45::Decimal32(2) as val');
      decode(data, 1, 1);
    }

    {
      const data = await query('SELECT 123456.789::Decimal64(3) as val');
      decode(data, 1, 1);
    }

    {
      const data = await query('SELECT 123456789.123456789::Decimal128(9) as val');
      decode(data, 1, 1);
    }

    // Nullable
    {
      const data = await query('SELECT 42::Nullable(UInt32) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children?.[0].value).toBe(42);
    }

    {
      const data = await query('SELECT NULL::Nullable(UInt32) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].value).toBe(null);
    }

    // Array
    {
      const data = await query('SELECT [1, 2, 3]::Array(UInt8) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(3);
    }

    {
      const data = await query('SELECT []::Array(UInt8) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(0);
    }

    {
      const data = await query('SELECT [[1, 2], [3, 4]]::Array(Array(UInt8)) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(2);
    }

    // Tuple
    {
      const data = await query('SELECT (1, 2, 3)::Tuple(UInt8, UInt16, UInt32) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(3);
    }

    {
      const data = await query("SELECT (1, 'hello')::Tuple(id UInt32, name String) as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(2);
    }

    // Map
    {
      const data = await query("SELECT map('a', 1, 'b', 2)::Map(String, UInt8) as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(2);
    }

    {
      const data = await query('SELECT map()::Map(String, UInt8) as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(0);
    }

    // Enum
    {
      const data = await query("SELECT 'hello'::Enum8('hello' = 1, 'world' = 2) as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].displayValue).toContain('hello');
    }

    {
      const data = await query("SELECT 'world'::Enum16('hello' = 1, 'world' = 2) as val");
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].displayValue).toContain('world');
    }

    // LowCardinality
    {
      const data = await query("SELECT 'hello'::LowCardinality(String) as val");
      decode(data, 1, 1);
    }

    {
      const data = await query(`
        SELECT val::LowCardinality(String) as val
        FROM (SELECT * FROM (SELECT 'a' as val UNION ALL SELECT 'b' UNION ALL SELECT 'a' UNION ALL SELECT 'c'))
      `);
      decode(data, 1, 4);
    }

    // Variant
    {
      const data = await query("SELECT 'hello'::Variant(String, UInt32) as val");
      decode(data, 1, 1);
    }

    {
      const data = await query('SELECT 42::Variant(String, UInt32) as val');
      decode(data, 1, 1);
    }

    // Dynamic
    {
      const data = await query('SELECT 42::Dynamic as val');
      decode(data, 1, 1);
    }

    {
      const data = await query("SELECT 'hello'::Dynamic as val");
      decode(data, 1, 1);
    }

    // JSON
    {
      const data = await query(`SELECT '{"a": 1, "b": "hello"}'::JSON as val`);
      decode(data, 1, 1);
    }

    {
      const data = await query(`SELECT '{"a": {"b": 42}}'::JSON(\`a.b\` Int32) as val`);
      decode(data, 1, 1);
    }

    {
      const data = await query(`SELECT '{"a": {"b": {"c": [1, 2, 3]}}}'::JSON as val`);
      decode(data, 1, 1);
    }

    // Geo types
    {
      const data = await query('SELECT (1.5, 2.5)::Point as val');
      const result = decode(data, 1, 1);
      expect(result.rows[0].values[0].children).toHaveLength(2);
    }

    {
      const data = await query('SELECT [(0, 0), (1, 0), (1, 1), (0, 0)]::Ring as val');
      decode(data, 1, 1);
    }

    {
      const data = await query('SELECT [(0, 0), (1, 1), (2, 0)]::LineString as val');
      decode(data, 1, 1);
    }

    // Multiple columns
    {
      const data = await query("SELECT 1::UInt8 as a, 'hello'::String as b, [1,2,3]::Array(UInt8) as c");
      const result = decode(data, 3, 1);
      expect(result.header.columns[0].name).toBe('a');
      expect(result.header.columns[1].name).toBe('b');
      expect(result.header.columns[2].name).toBe('c');
    }

    // Multiple rows
    {
      const data = await query('SELECT number::UInt32 as val FROM numbers(10)');
      const result = decode(data, 1, 10);
      for (let i = 0; i < 10; i++) {
        expect(result.rows[i].values[0].value).toBe(i);
      }
    }

    // Complex nested structure
    {
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
      decode(data, 1, 1);
    }
  }, 120000);
});
