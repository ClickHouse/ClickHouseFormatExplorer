import { describe, it, expect } from 'vitest';
import { ClickHouseContainer } from '@testcontainers/clickhouse';
import { NativeDecoder } from './native-decoder';

const IMAGE = 'clickhouse/clickhouse-server:latest';

describe('NativeDecoder Integration Tests', () => {
  it('decodes simple types correctly', async () => {
    await using container = await new ClickHouseContainer(IMAGE).start();
    const url = container.getHttpUrl();

    async function query(sql: string): Promise<Uint8Array> {
      const response = await fetch(`${url}/?user=${container.getUsername()}&password=${container.getPassword()}`, {
        method: 'POST',
        body: `${sql} FORMAT Native`,
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse error: ${text}`);
      }
      const buffer = await response.arrayBuffer();
      return new Uint8Array(buffer);
    }

    function decode(data: Uint8Array, expectedColumns: number, expectedRows: number) {
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();

      expect(result.header.columns).toHaveLength(expectedColumns);
      expect(result.blocks).toBeDefined();

      // Count total rows across all blocks
      const totalRows = result.blocks!.reduce((sum, block) => sum + block.rowCount, 0);
      expect(totalRows).toBe(expectedRows);

      return result;
    }

    // UInt8
    {
      const data = await query('SELECT 42::UInt8 as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(42);
    }

    // UInt16
    {
      const data = await query('SELECT 1234::UInt16 as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(1234);
    }

    // UInt32
    {
      const data = await query('SELECT 123456::UInt32 as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(123456);
    }

    // UInt64
    {
      const data = await query('SELECT 9223372036854775807::UInt64 as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(9223372036854775807n);
    }

    // Int8
    {
      const data = await query('SELECT toInt8(-42) as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(-42);
    }

    // Int16
    {
      const data = await query('SELECT toInt16(-1234) as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(-1234);
    }

    // Int32
    {
      const data = await query('SELECT toInt32(-123456) as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(-123456);
    }

    // Int64
    {
      const data = await query('SELECT toInt64(-9223372036854775807) as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(-9223372036854775807n);
    }

    // Float32
    {
      const data = await query('SELECT 3.14::Float32 as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBeCloseTo(3.14, 2);
    }

    // Float64
    {
      const data = await query('SELECT 3.141592653589793::Float64 as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBeCloseTo(3.141592653589793, 10);
    }

    // String
    {
      const data = await query("SELECT 'hello world'::String as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('hello world');
    }

    // Empty String
    {
      const data = await query("SELECT ''::String as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('');
    }

    // FixedString
    {
      const data = await query("SELECT 'abc'::FixedString(5) as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toMatch(/^abc/);
    }

    // Boolean
    {
      const data = await query('SELECT true::Bool as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(true);
    }

    {
      const data = await query('SELECT false::Bool as val');
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe(false);
    }

    // Date
    {
      const data = await query("SELECT '2024-01-15'::Date as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('2024-01-15');
    }

    // DateTime
    {
      const data = await query("SELECT '2024-01-15 12:30:45'::DateTime as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].displayValue).toContain('2024-01-15');
    }

    // Multiple columns
    {
      const data = await query("SELECT 1::UInt8 as a, 'hello'::String as b, 42::UInt32 as c");
      const result = decode(data, 3, 1);
      expect(result.header.columns[0].name).toBe('a');
      expect(result.header.columns[1].name).toBe('b');
      expect(result.header.columns[2].name).toBe('c');
      expect(result.blocks![0].columns[0].values[0].value).toBe(1);
      expect(result.blocks![0].columns[1].values[0].value).toBe('hello');
      expect(result.blocks![0].columns[2].values[0].value).toBe(42);
    }

    // Multiple rows
    {
      const data = await query('SELECT number::UInt32 as val FROM numbers(10)');
      const result = decode(data, 1, 10);

      // Values might be spread across multiple blocks
      const allValues: number[] = [];
      for (const block of result.blocks!) {
        for (const node of block.columns[0].values) {
          allValues.push(node.value as number);
        }
      }
      for (let i = 0; i < 10; i++) {
        expect(allValues[i]).toBe(i);
      }
    }

    // Block structure verification
    {
      const data = await query('SELECT 42::UInt32 as val');
      const result = decode(data, 1, 1);

      // Should have at least one data block
      expect(result.blocks!.length).toBeGreaterThanOrEqual(1);

      // First block should have proper structure
      const firstBlock = result.blocks![0];
      expect(firstBlock.rowCount).toBe(1);
      expect(firstBlock.columns).toHaveLength(1);
      expect(firstBlock.columns[0].name).toBe('val');
      expect(firstBlock.columns[0].typeString).toBe('UInt32');
      expect(firstBlock.columns[0].values).toHaveLength(1);
    }

    // UUID
    {
      const data = await query("SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('550e8400-e29b-41d4-a716-446655440000');
    }

    // IPv4
    {
      const data = await query("SELECT '192.168.1.1'::IPv4 as val");
      const result = decode(data, 1, 1);
      expect(result.blocks![0].columns[0].values[0].value).toBe('192.168.1.1');
    }

  }, 120000);
});
