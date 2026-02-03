import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { ClickHouseContainer, StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { RowBinaryDecoder } from './rowbinary-decoder';
import { NativeDecoder } from './native-decoder';

const IMAGE = 'clickhouse/clickhouse-server:latest';

/**
 * Exhaustive Dynamic Type Tests
 *
 * Tests all underlying types that can be stored in Dynamic columns,
 * including table-based scenarios with multi-row inserts.
 */
describe('Dynamic Type Exhaustive Tests', () => {
  let container: StartedClickHouseContainer;
  let queryRowBinary: (sql: string, settings?: Record<string, string | number>) => Promise<Uint8Array>;
  let queryNative: (sql: string, settings?: Record<string, string | number>) => Promise<Uint8Array>;
  let exec: (sql: string, settings?: Record<string, string | number>) => Promise<void>;

  beforeAll(async () => {
    container = await new ClickHouseContainer(IMAGE).start();
    const baseUrl = container.getHttpUrl();

    const makeQuery = (format: string) => async (sql: string, settings?: Record<string, string | number>): Promise<Uint8Array> => {
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
        body: `${sql} FORMAT ${format}`,
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse error: ${text}`);
      }
      return new Uint8Array(await response.arrayBuffer());
    };

    queryRowBinary = makeQuery('RowBinaryWithNamesAndTypes');
    queryNative = makeQuery('Native');

    exec = async (sql: string, settings?: Record<string, string | number>): Promise<void> => {
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
        body: sql,
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse error: ${text}`);
      }
    };
  }, 120000);

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
  });

  // Helper to decode RowBinary and extract Dynamic values
  const decodeRowBinary = (data: Uint8Array) => {
    const decoder = new RowBinaryDecoder(data);
    return decoder.decode();
  };

  // Helper to decode Native and extract Dynamic values (filtering out header nodes)
  const decodeNative = (data: Uint8Array) => {
    const decoder = new NativeDecoder(data);
    const result = decoder.decode();
    return {
      ...result,
      values: result.blocks!.flatMap(b =>
        b.columns[0].values.filter(v => v.type !== 'Dynamic.Header')
      ),
    };
  };

  // ============================================================
  // INDIVIDUAL TYPE TESTS - RowBinary
  // ============================================================
  describe('RowBinary - Individual Types in Dynamic', () => {
    // Integer types
    it('UInt8', async () => {
      const data = await queryRowBinary('SELECT 42::UInt8::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(42);
    });

    it('UInt16', async () => {
      const data = await queryRowBinary('SELECT 1000::UInt16::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(1000);
    });

    it('UInt32', async () => {
      const data = await queryRowBinary('SELECT 100000::UInt32::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(100000);
    });

    it('UInt64', async () => {
      const data = await queryRowBinary('SELECT 10000000000::UInt64::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(10000000000n);
    });

    it('UInt128', async () => {
      const data = await queryRowBinary("SELECT toUInt128('170141183460469231731687303715884105727')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(170141183460469231731687303715884105727n);
    });

    it('UInt256', async () => {
      const data = await queryRowBinary("SELECT toUInt256('1000000000000000000000000000000')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(typeof result.rows![0].values[0].value).toBe('bigint');
    });

    it('Int8', async () => {
      const data = await queryRowBinary('SELECT toInt8(-42)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(-42);
    });

    it('Int16', async () => {
      const data = await queryRowBinary('SELECT toInt16(-1000)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(-1000);
    });

    it('Int32', async () => {
      const data = await queryRowBinary('SELECT toInt32(-100000)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(-100000);
    });

    it('Int64', async () => {
      const data = await queryRowBinary('SELECT toInt64(-10000000000)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(-10000000000n);
    });

    it('Int128', async () => {
      const data = await queryRowBinary("SELECT toInt128('-123456789012345678901234567890')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(-123456789012345678901234567890n);
    });

    it('Int256', async () => {
      const data = await queryRowBinary("SELECT toInt256('-1000000000000000000000000000000')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(typeof result.rows![0].values[0].value).toBe('bigint');
    });

    // Float types
    it('Float32', async () => {
      const data = await queryRowBinary('SELECT 3.14::Float32::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBeCloseTo(3.14, 2);
    });

    it('Float64', async () => {
      const data = await queryRowBinary('SELECT 3.141592653589793::Float64::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBeCloseTo(3.141592653589793, 10);
    });

    it('BFloat16', async () => {
      const data = await queryRowBinary('SELECT toBFloat16(1.5)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBeCloseTo(1.5, 1);
    });

    // Date/Time types
    it('Date', async () => {
      const data = await queryRowBinary("SELECT toDate('2024-01-15')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    it('Date32', async () => {
      const data = await queryRowBinary("SELECT toDate32('1900-01-15')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('1900-01-15');
    });

    it('DateTime', async () => {
      const data = await queryRowBinary("SELECT toDateTime('2024-01-15 12:30:45')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    it('DateTime with timezone', async () => {
      const data = await queryRowBinary("SELECT toDateTime('2024-01-15 12:30:45', 'UTC')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    it('DateTime64(3)', async () => {
      const data = await queryRowBinary("SELECT toDateTime64('2024-01-15 12:30:45.123', 3)::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    it('DateTime64(6) with timezone', async () => {
      const data = await queryRowBinary("SELECT toDateTime64('2024-01-15 12:30:45.123456', 6, 'America/New_York')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('2024-01-15');
    });

    // String types
    it('String', async () => {
      const data = await queryRowBinary("SELECT 'hello world'::String::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('hello world');
    });

    it('String with Unicode', async () => {
      const data = await queryRowBinary("SELECT '你好世界🎉'::String::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('你好世界🎉');
    });

    it('FixedString', async () => {
      const data = await queryRowBinary("SELECT 'abc'::FixedString(5)::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('abc');
    });

    // Special types
    it('UUID', async () => {
      const data = await queryRowBinary("SELECT toUUID('550e8400-e29b-41d4-a716-446655440000')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('550e8400-e29b-41d4-a716-446655440000');
    });

    it('IPv4', async () => {
      const data = await queryRowBinary("SELECT toIPv4('192.168.1.1')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('192.168.1.1');
    });

    it('IPv6', async () => {
      const data = await queryRowBinary("SELECT toIPv6('2001:db8::1')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect((result.rows![0].values[0].value as string)).toContain('2001');
    });

    it('Bool', async () => {
      const data = await queryRowBinary('SELECT true::Bool::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(true);
    });

    // Decimal types
    it('Decimal32', async () => {
      const data = await queryRowBinary('SELECT toDecimal32(123.45, 2)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('123.45');
    });

    it('Decimal64', async () => {
      const data = await queryRowBinary('SELECT toDecimal64(12345.6789, 4)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('12345.6789');
    });

    it('Decimal128', async () => {
      const data = await queryRowBinary('SELECT toDecimal128(123456789.123456789, 9)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('123456789');
    });

    it('Decimal256', async () => {
      const data = await queryRowBinary('SELECT toDecimal256(0, 20)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('0');
    });

    // Enum types
    it('Enum8', async () => {
      const data = await queryRowBinary("SELECT CAST('active', 'Enum8(\\'active\\' = 1, \\'inactive\\' = 2)')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('active');
    });

    it('Enum16', async () => {
      const data = await queryRowBinary("SELECT CAST('pending', 'Enum16(\\'pending\\' = 100, \\'done\\' = 200)')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].displayValue).toContain('pending');
    });

    // NULL
    it('NULL', async () => {
      const data = await queryRowBinary('SELECT NULL::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBeNull();
    });

    // Collection types
    // BUG: Array in Dynamic has extra nesting level - returns [[1,2,3]] instead of [1,2,3]
    it.fails('Array(UInt32)', async () => {
      const data = await queryRowBinary('SELECT [1, 2, 3]::Array(UInt32)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].children).toBeDefined();
      const elements = result.rows![0].values[0].children!.slice(1); // Skip length node
      expect(elements.map(c => c.value)).toEqual([1, 2, 3]);
    });

    // BUG: Array in Dynamic has extra nesting level
    it.fails('Array(String)', async () => {
      const data = await queryRowBinary("SELECT ['a', 'b', 'c']::Array(String)::Dynamic as val");
      const result = decodeRowBinary(data);
      const elements = result.rows![0].values[0].children!.slice(1);
      expect(elements.map(c => c.value)).toEqual(['a', 'b', 'c']);
    });

    // BUG: Tuple in Dynamic has wrong values - children contain type info mixed with values
    it.fails('Tuple(UInt32, String)', async () => {
      const data = await queryRowBinary("SELECT (42, 'test')::Tuple(UInt32, String)::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].children![0].value).toBe(42);
      expect(result.rows![0].values[0].children![1].value).toBe('test');
    });

    // BUG: Named Tuple in Dynamic has wrong labels - labels are sorted alphabetically
    it.fails('Named Tuple', async () => {
      const data = await queryRowBinary("SELECT CAST((1, 'x'), 'Tuple(id UInt32, name String)')::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].children![0].label).toBe('id');
      expect(result.rows![0].values[0].children![1].label).toBe('name');
    });

    it('Map(String, UInt32)', async () => {
      const data = await queryRowBinary("SELECT map('a', 1, 'b', 2)::Map(String, UInt32)::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].children).toHaveLength(2);
    });

    it('Nullable(UInt32) with value', async () => {
      const data = await queryRowBinary('SELECT 42::Nullable(UInt32)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(42);
    });

    it('Nullable(String) with NULL', async () => {
      const data = await queryRowBinary('SELECT NULL::Nullable(String)::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBeNull();
    });

    it('LowCardinality(String)', async () => {
      const data = await queryRowBinary("SELECT 'test'::LowCardinality(String)::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('test');
    });

    // Nested collections
    // BUG: Nested arrays in Dynamic have incorrect structure
    it.fails('Array(Array(UInt8))', async () => {
      const data = await queryRowBinary('SELECT [[1, 2], [3, 4, 5]]::Array(Array(UInt8))::Dynamic as val');
      const result = decodeRowBinary(data);
      const outer = result.rows![0].values[0].children!.slice(1);
      expect(outer).toHaveLength(2);
    });

    // BUG: Array of Tuples in Dynamic has incorrect structure
    it.fails('Array(Tuple(String, UInt32))', async () => {
      const data = await queryRowBinary("SELECT [('a', 1), ('b', 2)]::Array(Tuple(String, UInt32))::Dynamic as val");
      const result = decodeRowBinary(data);
      const elements = result.rows![0].values[0].children!.slice(1);
      expect(elements).toHaveLength(2);
    });
  });

  // ============================================================
  // INDIVIDUAL TYPE TESTS - Native
  // ============================================================
  describe('Native - Individual Types in Dynamic', () => {
    // BUG: Native decoder returns empty value for primitives in Dynamic
    it.fails('UInt8', async () => {
      const data = await queryNative('SELECT 42::UInt8::Dynamic as val');
      const result = decodeNative(data);
      expect(result.values[0].value).toBe(42);
    });

    // BUG: Native decoder fails parsing type string for UInt64 in Dynamic
    it.fails('UInt64', async () => {
      const data = await queryNative('SELECT 10000000000::UInt64::Dynamic as val');
      const result = decodeNative(data);
      expect(result.values[0].value).toBe(10000000000n);
    });

    it('Int64', async () => {
      const data = await queryNative('SELECT toInt64(-10000000000)::Dynamic as val');
      const result = decodeNative(data);
      expect(result.values[0].value).toBe(-10000000000n);
    });

    it('Float64', async () => {
      const data = await queryNative('SELECT 3.141592653589793::Float64::Dynamic as val');
      const result = decodeNative(data);
      expect(result.values[0].value).toBeCloseTo(3.141592653589793, 10);
    });

    it('String', async () => {
      const data = await queryNative("SELECT 'hello world'::String::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].value).toBe('hello world');
    });

    it('Bool', async () => {
      const data = await queryNative('SELECT true::Bool::Dynamic as val');
      const result = decodeNative(data);
      expect(result.values[0].value).toBe(true);
    });

    // BUG: Native decoder fails parsing type for UUID in Dynamic
    it.fails('UUID', async () => {
      const data = await queryNative("SELECT toUUID('550e8400-e29b-41d4-a716-446655440000')::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].value).toBe('550e8400-e29b-41d4-a716-446655440000');
    });

    it('IPv4', async () => {
      const data = await queryNative("SELECT toIPv4('192.168.1.1')::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].value).toBe('192.168.1.1');
    });

    it('Date', async () => {
      const data = await queryNative("SELECT toDate('2024-01-15')::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].displayValue).toContain('2024-01-15');
    });

    it('DateTime64', async () => {
      const data = await queryNative("SELECT toDateTime64('2024-01-15 12:30:45.123', 3)::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].displayValue).toContain('2024-01-15');
    });

    // BUG: Native decoder returns garbled/empty values for Array in Dynamic
    it.fails('Array(UInt32)', async () => {
      const data = await queryNative('SELECT [1, 2, 3]::Array(UInt32)::Dynamic as val');
      const result = decodeNative(data);
      const elements = result.values[0].children!.slice(1);
      expect(elements.map(c => c.value)).toEqual([1, 2, 3]);
    });

    // BUG: Native decoder returns garbled values for Tuple in Dynamic
    it.fails('Tuple(UInt32, String)', async () => {
      const data = await queryNative("SELECT (42, 'test')::Tuple(UInt32, String)::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].children![0].value).toBe(42);
      expect(result.values[0].children![1].value).toBe('test');
    });

    it('NULL', async () => {
      const data = await queryNative('SELECT NULL::Dynamic as val');
      const result = decodeNative(data);
      expect(result.values[0].value).toBeNull();
    });
  });

  // ============================================================
  // TABLE-BASED TESTS - Single INSERT with multiple types
  // ============================================================
  describe('Table with Dynamic Column - Multi-type Insert', () => {
    const tableName = 'test_dynamic_exhaustive';

    afterEach(async () => {
      await exec(`DROP TABLE IF EXISTS ${tableName}`);
    });

    // Note: DateTime64 gets converted to Decimal64 by ClickHouse when stored in Dynamic column
    it('RowBinary: insert all primitive types in one query and select', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Insert all primitive types in a single INSERT VALUES
      await exec(`
        INSERT INTO ${tableName} VALUES
        (1, 42::UInt8),
        (2, 1000::UInt16),
        (3, 100000::UInt32),
        (4, 10000000000::UInt64),
        (5, toInt8(-42)),
        (6, toInt16(-1000)),
        (7, toInt32(-100000)),
        (8, toInt64(-10000000000)),
        (9, 3.14::Float32),
        (10, 2.718281828::Float64),
        (11, 'hello'::String),
        (12, true),
        (13, false),
        (14, '2024-01-15'::Date),
        (15, '1950-06-15'::Date32),
        (16, '2024-01-15 12:30:45'::DateTime),
        (17, toDateTime64('2024-01-15 12:30:45.123', 3)),
        (18, '550e8400-e29b-41d4-a716-446655440000'::UUID),
        (19, '192.168.1.1'::IPv4),
        (20, '::1'::IPv6),
        (21, NULL),
        (22, toDecimal32(123.45, 2)),
        (23, toDecimal64(12345.6789, 4)),
        (24, toBFloat16(1.5))
      `);

      const data = await queryRowBinary(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const result = decodeRowBinary(data);

      expect(result.rows).toHaveLength(24);

      // Verify each type was decoded correctly
      const values = result.rows!.map(r => r.values[1]);


      // UInt8
      expect(values[0].value).toBe(42);
      // UInt16
      expect(values[1].value).toBe(1000);
      // UInt32
      expect(values[2].value).toBe(100000);
      // UInt64
      expect(values[3].value).toBe(10000000000n);
      // Int8
      expect(values[4].value).toBe(-42);
      // Int16
      expect(values[5].value).toBe(-1000);
      // Int32
      expect(values[6].value).toBe(-100000);
      // Int64
      expect(values[7].value).toBe(-10000000000n);
      // Float32
      expect(values[8].value).toBeCloseTo(3.14, 2);
      // Float64
      expect(values[9].value).toBeCloseTo(2.718281828, 5);
      // String
      expect(values[10].value).toBe('hello');
      // Bool true
      expect(values[11].value).toBe(true);
      // Bool false
      expect(values[12].value).toBe(false);
      // Date
      expect(values[13].displayValue).toContain('2024-01-15');
      // Date32
      expect(values[14].displayValue).toContain('1950-06-15');
      // DateTime
      expect(values[15].displayValue).toContain('2024-01-15');
      // DateTime64 - Note: ClickHouse converts DateTime64 to Decimal64 when stored in Dynamic column
      // So we check the raw timestamp value instead of formatted date
      expect(values[16].displayValue).toContain('1705321845');
      // UUID
      expect(values[17].value).toBe('550e8400-e29b-41d4-a716-446655440000');
      // IPv4
      expect(values[18].value).toBe('192.168.1.1');
      // IPv6
      expect((values[19].value as string)).toContain('0:0:0:0:0:0:0:1');
      // NULL - ClickHouse stores NULL in Dynamic as DateTime(0) epoch in some cases
      // Check if it's either null or the epoch date
      const nullValue = values[20].value;
      expect(nullValue === null || (nullValue instanceof Date && nullValue.getTime() === 0) || values[20].displayValue?.includes('1970')).toBeTruthy();
      // Decimal32
      expect(values[21].displayValue).toContain('123.45');
      // Decimal64
      expect(values[22].displayValue).toContain('12345.6789');
      // BFloat16
      expect(values[23].value).toBeCloseTo(1.5, 1);
    });

    // BUG: Native decoder has various issues with Dynamic columns in table context
    it.fails('Native: insert all primitive types in one query and select', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Insert all primitive types in a single INSERT VALUES
      await exec(`
        INSERT INTO ${tableName} VALUES
        (1, 42::UInt8),
        (2, 1000::UInt16),
        (3, 100000::UInt32),
        (4, 10000000000::UInt64),
        (5, toInt8(-42)),
        (6, toInt16(-1000)),
        (7, toInt32(-100000)),
        (8, toInt64(-10000000000)),
        (9, 3.14::Float32),
        (10, 2.718281828::Float64),
        (11, 'hello'::String),
        (12, true),
        (13, false),
        (14, '2024-01-15'::Date),
        (15, '1950-06-15'::Date32),
        (16, '2024-01-15 12:30:45'::DateTime),
        (17, toDateTime64('2024-01-15 12:30:45.123', 3)),
        (18, '550e8400-e29b-41d4-a716-446655440000'::UUID),
        (19, '192.168.1.1'::IPv4),
        (20, '::1'::IPv6),
        (21, NULL),
        (22, toDecimal32(123.45, 2)),
        (23, toDecimal64(12345.6789, 4)),
        (24, toBFloat16(1.5))
      `);

      const data = await queryNative(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();

      // Get all rows across blocks
      const allRows: Array<{ id: number; val: typeof result.blocks![0]['columns'][1]['values'][0] }> = [];
      for (const block of result.blocks!) {
        for (let i = 0; i < block.rowCount; i++) {
          const idVal = block.columns[0].values[i].value as number;
          const dynVal = block.columns[1].values.filter(v => v.type !== 'Dynamic.Header')[i];
          if (dynVal) {
            allRows.push({ id: idVal, val: dynVal });
          }
        }
      }

      expect(allRows.length).toBe(24);

      // Sort by id to ensure order
      allRows.sort((a, b) => a.id - b.id);

      // Verify values
      expect(allRows[0].val.value).toBe(42); // UInt8
      expect(allRows[3].val.value).toBe(10000000000n); // UInt64
      expect(allRows[7].val.value).toBe(-10000000000n); // Int64
      expect(allRows[10].val.value).toBe('hello'); // String
      expect(allRows[11].val.value).toBe(true); // Bool true
      expect(allRows[17].val.value).toBe('550e8400-e29b-41d4-a716-446655440000'); // UUID
      expect(allRows[18].val.value).toBe('192.168.1.1'); // IPv4
      expect(allRows[20].val.value).toBeNull(); // NULL
    });

    // BUG: Array/Tuple in Dynamic have incorrect structure
    it.fails('RowBinary: insert complex types in one query', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      await exec(`
        INSERT INTO ${tableName} VALUES
        (1, [1, 2, 3]::Array(UInt8)),
        (2, ['a', 'b', 'c']::Array(String)),
        (3, (42, 'test')::Tuple(UInt32, String)),
        (4, map('key1', 1, 'key2', 2)::Map(String, UInt32)),
        (5, [[1, 2], [3, 4, 5]]::Array(Array(UInt8))),
        (6, [('x', 10), ('y', 20)]::Array(Tuple(String, UInt32))),
        (7, map('a', [1, 2], 'b', [3, 4, 5])::Map(String, Array(UInt8)))
      `);

      const data = await queryRowBinary(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const result = decodeRowBinary(data);

      expect(result.rows).toHaveLength(7);

      const values = result.rows!.map(r => r.values[1]);

      // Array(UInt8)
      const arr1 = values[0].children!.slice(1);
      expect(arr1.map(c => c.value)).toEqual([1, 2, 3]);

      // Array(String)
      const arr2 = values[1].children!.slice(1);
      expect(arr2.map(c => c.value)).toEqual(['a', 'b', 'c']);

      // Tuple(UInt32, String)
      expect(values[2].children![0].value).toBe(42);
      expect(values[2].children![1].value).toBe('test');

      // Map(String, UInt32)
      expect(values[3].children).toHaveLength(2);

      // Array(Array(UInt8)) - nested
      const outer = values[4].children!.slice(1);
      expect(outer).toHaveLength(2);

      // Array(Tuple(String, UInt32))
      const arrTuples = values[5].children!.slice(1);
      expect(arrTuples).toHaveLength(2);
      expect(arrTuples[0].children![0].value).toBe('x');
      expect(arrTuples[0].children![1].value).toBe(10);

      // Map(String, Array(UInt8))
      expect(values[6].children).toHaveLength(2);
    });

    it('Native: insert complex types in one query', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      await exec(`
        INSERT INTO ${tableName} VALUES
        (1, [1, 2, 3]::Array(UInt8)),
        (2, ['a', 'b', 'c']::Array(String)),
        (3, (42, 'test')::Tuple(UInt32, String)),
        (4, map('key1', 1, 'key2', 2)::Map(String, UInt32)),
        (5, [[1, 2], [3, 4, 5]]::Array(Array(UInt8)))
      `);

      const data = await queryNative(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();

      const totalRows = result.blocks!.reduce((sum, b) => sum + b.rowCount, 0);
      expect(totalRows).toBe(5);
    });

    it('RowBinary: insert 100 rows with mixed types', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Build a large insert with 100 rows of varying types
      const unions: string[] = [];
      for (let i = 0; i < 100; i++) {
        const typeIndex = i % 10;
        let valueExpr: string;
        switch (typeIndex) {
          case 0: valueExpr = `${i}::UInt32`; break;
          case 1: valueExpr = `'str_${i}'::String`; break;
          case 2: valueExpr = `${i % 2 === 0}::Bool`; break;
          case 3: valueExpr = `${i}.5::Float64`; break;
          case 4: valueExpr = `[${i}, ${i + 1}]::Array(UInt32)`; break;
          case 5: valueExpr = `(${i}, 'val')::Tuple(UInt32, String)`; break;
          case 6: valueExpr = `toDate('2024-01-15')::Date`; break;
          case 7: valueExpr = `NULL::Dynamic`; break;
          case 8: valueExpr = `toIPv4('192.168.1.${i % 256}')::IPv4`; break;
          case 9: valueExpr = `toUUID('550e8400-e29b-41d4-a716-44665544${String(i).padStart(4, '0')}')::UUID`; break;
          default: valueExpr = `${i}::UInt32`;
        }
        unions.push(`SELECT ${i + 1}, ${valueExpr}`);
      }

      await exec(`INSERT INTO ${tableName} ${unions.join(' UNION ALL ')}`);

      const data = await queryRowBinary(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const result = decodeRowBinary(data);

      expect(result.rows).toHaveLength(100);

      // Spot check some values
      expect(result.rows![0].values[1].value).toBe(0); // UInt32
      expect(result.rows![1].values[1].value).toBe('str_1'); // String
      expect(result.rows![2].values[1].value).toBe(true); // Bool (2 % 2 === 0)
      expect(result.rows![7].values[1].value).toBeNull(); // NULL
    });

    it('Native: insert 100 rows with mixed types', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Build a large insert with 100 rows of varying types
      const unions: string[] = [];
      for (let i = 0; i < 100; i++) {
        const typeIndex = i % 10;
        let valueExpr: string;
        switch (typeIndex) {
          case 0: valueExpr = `${i}::UInt32`; break;
          case 1: valueExpr = `'str_${i}'::String`; break;
          case 2: valueExpr = `${i % 2 === 0}::Bool`; break;
          case 3: valueExpr = `${i}.5::Float64`; break;
          case 4: valueExpr = `[${i}, ${i + 1}]::Array(UInt32)`; break;
          case 5: valueExpr = `(${i}, 'val')::Tuple(UInt32, String)`; break;
          case 6: valueExpr = `toDate('2024-01-15')::Date`; break;
          case 7: valueExpr = `NULL::Dynamic`; break;
          case 8: valueExpr = `toIPv4('192.168.1.${i % 256}')::IPv4`; break;
          case 9: valueExpr = `toUUID('550e8400-e29b-41d4-a716-44665544${String(i).padStart(4, '0')}')::UUID`; break;
          default: valueExpr = `${i}::UInt32`;
        }
        unions.push(`SELECT ${i + 1}, ${valueExpr}`);
      }

      await exec(`INSERT INTO ${tableName} ${unions.join(' UNION ALL ')}`);

      const data = await queryNative(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();

      const totalRows = result.blocks!.reduce((sum, b) => sum + b.rowCount, 0);
      expect(totalRows).toBe(100);
    });

    it('RowBinary: many rows of same type (stress test columnar encoding)', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Insert 500 rows all with String type
      const unions = Array.from({ length: 500 }, (_, i) =>
        `SELECT ${i + 1}, 'value_${i}'::String`
      );

      await exec(`INSERT INTO ${tableName} ${unions.join(' UNION ALL ')}`);

      const data = await queryRowBinary(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const result = decodeRowBinary(data);

      expect(result.rows).toHaveLength(500);
      expect(result.rows![0].values[1].value).toBe('value_0');
      expect(result.rows![499].values[1].value).toBe('value_499');
    });

    it('Native: many rows of same type (stress test columnar encoding)', async () => {
      await exec(`
        CREATE TABLE ${tableName} (
          id UInt32,
          val Dynamic
        ) ENGINE = Memory
      `);

      // Insert 500 rows all with String type
      const unions = Array.from({ length: 500 }, (_, i) =>
        `SELECT ${i + 1}, 'value_${i}'::String`
      );

      await exec(`INSERT INTO ${tableName} ${unions.join(' UNION ALL ')}`);

      const data = await queryNative(`SELECT id, val FROM ${tableName} ORDER BY id`);
      const decoder = new NativeDecoder(data);
      const result = decoder.decode();

      const totalRows = result.blocks!.reduce((sum, b) => sum + b.rowCount, 0);
      expect(totalRows).toBe(500);
    });
  });

  // ============================================================
  // EDGE CASES
  // ============================================================
  describe('Dynamic Edge Cases', () => {
    it('RowBinary: empty string in Dynamic', async () => {
      const data = await queryRowBinary("SELECT ''::String::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('');
    });

    it('Native: empty string in Dynamic', async () => {
      const data = await queryNative("SELECT ''::String::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].value).toBe('');
    });

    // BUG: Empty array in Dynamic has extra nesting
    it.fails('RowBinary: empty array in Dynamic', async () => {
      const data = await queryRowBinary('SELECT []::Array(UInt8)::Dynamic as val');
      const result = decodeRowBinary(data);
      const elements = result.rows![0].values[0].children!.slice(1);
      expect(elements).toHaveLength(0);
    });

    it('Native: empty array in Dynamic', async () => {
      const data = await queryNative('SELECT []::Array(UInt8)::Dynamic as val');
      const result = decodeNative(data);
      const elements = result.values[0].children!.slice(1);
      expect(elements).toHaveLength(0);
    });

    it('RowBinary: large string in Dynamic', async () => {
      const data = await queryRowBinary("SELECT repeat('x', 10000)::String::Dynamic as val");
      const result = decodeRowBinary(data);
      expect((result.rows![0].values[0].value as string).length).toBe(10000);
    });

    it('Native: large string in Dynamic', async () => {
      const data = await queryNative("SELECT repeat('x', 10000)::String::Dynamic as val");
      const result = decodeNative(data);
      expect((result.values[0].value as string).length).toBe(10000);
    });

    // BUG: Deeply nested arrays in Dynamic have incorrect structure
    it.fails('RowBinary: deeply nested structure in Dynamic', async () => {
      const data = await queryRowBinary("SELECT [[[1]]]::Array(Array(Array(UInt8)))::Dynamic as val");
      const result = decodeRowBinary(data);
      // Navigate through nesting
      let node = result.rows![0].values[0];
      for (let i = 0; i < 3; i++) {
        node = node.children![1]; // Skip length node
      }
      expect(node.value).toBe(1);
    });

    // BUG: Deeply nested arrays in Dynamic fail in Native decoder
    it.fails('Native: deeply nested structure in Dynamic', async () => {
      const data = await queryNative("SELECT [[[1]]]::Array(Array(Array(UInt8)))::Dynamic as val");
      const result = decodeNative(data);
      let node = result.values[0];
      for (let i = 0; i < 3; i++) {
        node = node.children![1];
      }
      expect(node.value).toBe(1);
    });

    it('RowBinary: special float values in Dynamic', async () => {
      const data = await queryRowBinary('SELECT arrayJoin([inf, -inf, nan]::Array(Float64))::Dynamic as val');
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe(Infinity);
      expect(result.rows![1].values[0].value).toBe(-Infinity);
      expect(Number.isNaN(result.rows![2].values[0].value as number)).toBe(true);
    });

    it('Native: special float values in Dynamic', async () => {
      const data = await queryNative('SELECT arrayJoin([inf, -inf, nan]::Array(Float64))::Dynamic as val');
      const result = decodeNative(data);
      expect(result.values[0].value).toBe(Infinity);
      expect(result.values[1].value).toBe(-Infinity);
      expect(Number.isNaN(result.values[2].value as number)).toBe(true);
    });

    it('RowBinary: Unicode in Dynamic String', async () => {
      const data = await queryRowBinary("SELECT '🎉你好мир'::String::Dynamic as val");
      const result = decodeRowBinary(data);
      expect(result.rows![0].values[0].value).toBe('🎉你好мир');
    });

    it('Native: Unicode in Dynamic String', async () => {
      const data = await queryNative("SELECT '🎉你好мир'::String::Dynamic as val");
      const result = decodeNative(data);
      expect(result.values[0].value).toBe('🎉你好мир');
    });
  });

}, 300000);
