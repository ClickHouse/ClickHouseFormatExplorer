/**
 * Regression tests that verify the Native decoder against the byte-level
 * examples in docs/full_native_spec.md. Each test encodes a single-column,
 * protocol-0 block (no BlockInfo, no has_custom_serialization byte) so the
 * column `data` bytes are exactly the spec's example bytes.
 *
 * These cover the fixed-width / variable-length / composite families, where a
 * faithful byte example fully exercises the encoding. The versioned/stateful
 * types (LowCardinality, Variant, Dynamic, JSON) get dedicated structural
 * tests in native-spec-versioned.test.ts after a semantic spec comparison.
 */
import { describe, expect, it } from 'vitest';
import { NativeDecoder } from './native-decoder';
import { AstNode } from '../types/ast';
import { analyzeByteRange } from './test-helpers';

function encodeLeb128(value: number | bigint): number[] {
  let current = BigInt(value);
  const bytes: number[] = [];
  do {
    let byte = Number(current & 0x7fn);
    current >>= 7n;
    if (current !== 0n) byte |= 0x80;
    bytes.push(byte);
  } while (current !== 0n);
  return bytes;
}

function encodeString(value: string): number[] {
  const bytes = Array.from(new TextEncoder().encode(value));
  return [...encodeLeb128(bytes.length), ...bytes];
}

function u32(v: number): number[] {
  return [v & 0xff, (v >> 8) & 0xff, (v >> 16) & 0xff, (v >> 24) & 0xff];
}
function u64(v: number | bigint): number[] {
  const out: number[] = [];
  let cur = BigInt(v);
  for (let i = 0; i < 8; i++) {
    out.push(Number(cur & 0xffn));
    cur >>= 8n;
  }
  return out;
}

/** Build a protocol-0 single-column block and return the decoded column values. */
function decodeColumn(typeString: string, data: number[], numRows: number): AstNode[] {
  const bytes = new Uint8Array([
    ...encodeLeb128(1), // numColumns
    ...encodeLeb128(numRows), // numRows
    ...encodeString('c'),
    ...encodeString(typeString),
    ...data,
  ]);
  const parsed = new NativeDecoder(bytes, 0).decode();
  const column = parsed.blocks?.[0]?.columns[0];
  if (!column) throw new Error('no column decoded');
  // Coverage sanity: every byte should be claimed by some leaf node.
  const coverage = analyzeByteRange(parsed, bytes.length);
  if (!coverage.isComplete) {
    throw new Error(`incomplete byte coverage for ${typeString}: ${JSON.stringify(coverage.uncoveredRanges)}`);
  }
  return column.values;
}

function values(typeString: string, data: number[], numRows: number): unknown[] {
  return decodeColumn(typeString, data, numRows).map((n) => n.value);
}

describe('Native spec — fixed-width types', () => {
  it('UInt32 [1, 256, 65536]', () => {
    expect(values('UInt32', [0x01, 0, 0, 0, 0x00, 0x01, 0, 0, 0x00, 0x00, 0x01, 0], 3)).toEqual([1, 256, 65536]);
  });

  it('Int32 [-1, 42]', () => {
    expect(values('Int32', [0xff, 0xff, 0xff, 0xff, 0x2a, 0, 0, 0], 2)).toEqual([-1, 42]);
  });

  it('UInt64 / Int64 round-trip', () => {
    expect(values('UInt64', [0x01, 0, 0, 0, 0, 0, 0, 0], 1)).toEqual([1n]);
    expect(values('Int64', [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 1)).toEqual([-1n]);
  });

  it('Float32 1.5', () => {
    expect(values('Float32', [0x00, 0x00, 0xc0, 0x3f], 1)).toEqual([1.5]);
  });

  it('Float64 1.5', () => {
    expect(values('Float64', [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f], 1)).toEqual([1.5]);
  });

  it('Bool [true, false, true]', () => {
    expect(values('Bool', [0x01, 0x00, 0x01], 3)).toEqual([true, false, true]);
  });

  it('Date 1970-01-02 (1 day)', () => {
    const node = decodeColumn('Date', [0x01, 0x00], 1)[0];
    expect(node.metadata?.daysSinceEpoch).toBe(1);
    expect(node.displayValue).toBe('1970-01-02');
  });

  it('Date32 1900-01-01 (-25567 days)', () => {
    const node = decodeColumn('Date32', [0x21, 0x9c, 0xff, 0xff], 1)[0];
    expect(node.metadata?.daysSinceEpoch).toBe(-25567);
    expect(node.displayValue).toBe('1900-01-01');
  });

  it('DateTime — UInt32 LE seconds since epoch', () => {
    // NOTE: docs/full_native_spec.md's DateTime example is internally
    // inconsistent — the bytes A8 84 F4 65 decode to 1710523560, not the
    // 1710513000 the prose claims (which would be 68 5B F4 65). The decoder
    // faithfully decodes the bytes; we assert the byte-accurate value.
    const node = decodeColumn("DateTime('UTC')", [0xa8, 0x84, 0xf4, 0x65], 1)[0];
    expect(node.metadata?.secondsSinceEpoch).toBe(1710523560);
  });

  it('DateTime64(3, UTC) 1705321845123 ms', () => {
    const node = decodeColumn("DateTime64(3, 'UTC')", [0x83, 0x51, 0x1a, 0x0d, 0x8d, 0x01, 0x00, 0x00], 1)[0];
    expect(node.metadata?.ticksSinceEpoch).toBe('1705321845123');
  });

  it('DateTime64(0) 1705321845 s', () => {
    const node = decodeColumn('DateTime64(0)', [0x75, 0x25, 0xa5, 0x65, 0x00, 0x00, 0x00, 0x00], 1)[0];
    expect(node.metadata?.ticksSinceEpoch).toBe('1705321845');
  });

  it('UUID 550e8400-e29b-41d4-a716-446655440000', () => {
    const wire = [0xd4, 0x41, 0x9b, 0xe2, 0x00, 0x84, 0x0e, 0x55, 0x00, 0x00, 0x44, 0x55, 0x66, 0x44, 0x16, 0xa7];
    expect(values('UUID', wire, 1)).toEqual(['550e8400-e29b-41d4-a716-446655440000']);
  });

  it('IPv4 192.168.1.10', () => {
    expect(values('IPv4', [0x0a, 0x01, 0xa8, 0xc0], 1)).toEqual(['192.168.1.10']);
  });

  it('IPv6 2001:db8::1', () => {
    const wire = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01];
    const node = decodeColumn('IPv6', wire, 1)[0];
    // Spec canonical form is 2001:db8::1 (with :: zero compression).
    expect(node.displayValue).toBe('2001:db8::1');
  });

  it('Enum8 [active, inactive, active]', () => {
    const nodes = decodeColumn("Enum8('active' = 1, 'inactive' = 2)", [0x01, 0x02, 0x01], 3);
    expect(nodes.map((n) => n.value)).toEqual([1, 2, 1]);
    expect(nodes.map((n) => n.metadata?.enumName)).toEqual(['active', 'inactive', 'active']);
  });

  it('Enum16 30000', () => {
    const node = decodeColumn("Enum16('a' = 1, 'b' = 30000)", [0x30, 0x75], 1)[0];
    expect(node.value).toBe(30000);
    expect(node.metadata?.enumName).toBe('b');
  });

  it('Decimal(9, 4) 123.4567 -> 1234567', () => {
    const node = decodeColumn('Decimal(9, 4)', [0x87, 0xd6, 0x12, 0x00], 1)[0];
    expect(node.metadata?.rawValue).toBe(1234567);
    expect(node.displayValue).toBe('123.4567');
  });

  it('Decimal(18, 1) -1.5 -> -15', () => {
    const node = decodeColumn('Decimal(18, 1)', [0xf1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 1)[0];
    expect(node.metadata?.rawValue).toBe('-15');
    expect(node.displayValue).toBe('-1.5');
  });

  it('Decimal(38, 4) 123.4567 (16 bytes)', () => {
    const wire = [0x87, 0xd6, 0x12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const node = decodeColumn('Decimal(38, 4)', wire, 1)[0];
    expect(node.metadata?.rawValue).toBe('1234567');
    expect(node.displayValue).toBe('123.4567');
  });
});

describe('Native spec — variable-length types', () => {
  it('String ["ab", "", "c"]', () => {
    expect(values('String', [0x02, 0x61, 0x62, 0x00, 0x01, 0x63], 3)).toEqual(['ab', '', 'c']);
  });

  it('FixedString(3) ["abc", "de\\0"]', () => {
    const nodes = decodeColumn('FixedString(3)', [0x61, 0x62, 0x63, 0x64, 0x65, 0x00], 2);
    expect(nodes.map((n) => n.value)).toEqual(['abc', 'de']);
  });
});

describe('Native spec — composite types', () => {
  it('Nullable(UInt8) [5, NULL, 9]', () => {
    expect(values('Nullable(UInt8)', [0x00, 0x01, 0x00, 0x05, 0x00, 0x09], 3)).toEqual([5, null, 9]);
  });

  it('Nullable(String) ["hello", NULL, "world"]', () => {
    const data = [
      0x00, 0x01, 0x00,
      0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
      0x00,
      0x05, 0x77, 0x6f, 0x72, 0x6c, 0x64,
    ];
    expect(values('Nullable(String)', data, 3)).toEqual(['hello', null, 'world']);
  });

  it('Array(UInt32) [[10,20,30], [], [40,50]]', () => {
    const data = [
      ...u64(3), ...u64(3), ...u64(5),
      ...u32(10), ...u32(20), ...u32(30), ...u32(40), ...u32(50),
    ];
    const nodes = decodeColumn('Array(UInt32)', data, 3);
    expect(nodes.map((n) => n.value)).toEqual([[10, 20, 30], [], [40, 50]]);
  });

  it('Array(String) [["a","bb"], []]', () => {
    const data = [...u64(2), ...u64(2), 0x01, 0x61, 0x02, 0x62, 0x62];
    const nodes = decodeColumn('Array(String)', data, 2);
    expect(nodes.map((n) => n.value)).toEqual([['a', 'bb'], []]);
  });

  it('Tuple(UInt8, UInt8) columnar layout', () => {
    const nodes = decodeColumn('Tuple(UInt8, UInt8)', [0x01, 0x02, 0x03, 0x04, 0x05, 0x06], 3);
    expect(nodes.map((n) => n.value)).toEqual([[1, 4], [2, 5], [3, 6]]);
  });

  it('Tuple(UInt32, String)', () => {
    const data = [...u32(10), ...u32(20), 0x01, 0x61, 0x02, 0x62, 0x62];
    const nodes = decodeColumn('Tuple(UInt32, String)', data, 2);
    expect(nodes.map((n) => n.value)).toEqual([[10, 'a'], [20, 'bb']]);
  });

  it('Map(UInt8, UInt8) {1:10,2:20}, {3:30}', () => {
    const data = [...u64(2), ...u64(3), 0x01, 0x02, 0x03, 0x0a, 0x14, 0x1e];
    const nodes = decodeColumn('Map(UInt8, UInt8)', data, 2);
    expect(nodes.map((n) => n.value)).toEqual([{ '1': 10, '2': 20 }, { '3': 30 }]);
  });

  it('Map(String, UInt32) {a:1, b:2}', () => {
    const data = [...u64(2), 0x01, 0x61, 0x01, 0x62, ...u32(1), ...u32(2)];
    const nodes = decodeColumn('Map(String, UInt32)', data, 1);
    expect(nodes.map((n) => n.value)).toEqual([{ a: 1, b: 2 }]);
  });

  it('Nested(a UInt8, b String) [[(10,x),(20,y)], [(30,z)]]', () => {
    const data = [
      ...u64(2), ...u64(3),
      0x0a, 0x14, 0x1e,
      0x01, 0x78, 0x01, 0x79, 0x01, 0x7a,
    ];
    const nodes = decodeColumn('Nested(a UInt8, b String)', data, 2);
    // Nested is byte-identical to Array(Tuple(a UInt8, b String)).
    expect(nodes.map((n) => n.value)).toEqual([
      [[10, 'x'], [20, 'y']],
      [[30, 'z']],
    ]);
  });

  it('Nullable(Nothing) — SELECT NULL, 3 rows all NULL', () => {
    const data = [0x01, 0x01, 0x01, 0x30, 0x30, 0x30];
    expect(values('Nullable(Nothing)', data, 3)).toEqual([null, null, null]);
  });
});

/**
 * Versioned/stateful types. The byte fixtures below were captured from
 * `clickhouse-local ... FORMAT Native` (protocol-0 output, no has_custom byte)
 * so they are ground truth, not hand-derived.
 */
describe('Native spec — versioned types', () => {
  it('LowCardinality(String) [a, b, a, c, b]', () => {
    const data = [
      ...u64(1), // state prefix
      ...u64(0x600), // metadata (HasAdditionalKeys + NeedUpdateDictionary)
      ...u64(4), // dict_size
      0x00, // dict[0] = "" placeholder
      0x01, 0x61, // dict[1] = "a"
      0x01, 0x62, // dict[2] = "b"
      0x01, 0x63, // dict[3] = "c"
      ...u64(5), // keys_count
      0x01, 0x02, 0x01, 0x03, 0x02,
    ];
    expect(values('LowCardinality(String)', data, 5)).toEqual(['a', 'b', 'a', 'c', 'b']);
  });

  it('LowCardinality(Nullable(String)) [a, NULL, b] — NULL is dict index 0', () => {
    // Real clickhouse-local bytes. The spec claims dict[1] is the null marker,
    // but ClickHouse actually reserves index 0 for NULL and index 1 for the
    // default/empty placeholder (real values start at index 2). dict = ["","","a","b"],
    // keys = [2, 0, 3].
    const data = [
      ...u64(1), // state prefix
      ...u64(0x600), // metadata
      ...u64(4), // dict_size
      0x00, // dict[0] = "" (NULL slot)
      0x00, // dict[1] = "" (default placeholder)
      0x01, 0x61, // dict[2] = "a"
      0x01, 0x62, // dict[3] = "b"
      ...u64(3), // keys_count
      0x02, 0x00, 0x03, // keys -> a, NULL, b
    ];
    expect(values('LowCardinality(Nullable(String))', data, 3)).toEqual(['a', null, 'b']);
  });

  it('LowCardinality(Nullable(String)) [a, NULL, "", b] — NULL (idx 0) vs empty string (idx 1)', () => {
    // Real clickhouse-local bytes. NULL and a genuine empty string both serialize
    // as "" in the dictionary, but reference different reserved slots: NULL -> key 0,
    // empty-string default -> key 1. dict = ["","","a","b"], keys = [2, 0, 1, 3].
    const data = [
      ...u64(1), // state prefix
      ...u64(0x600), // metadata
      ...u64(4), // dict_size
      0x00, // dict[0] = "" (NULL slot)
      0x00, // dict[1] = "" (default/empty placeholder)
      0x01, 0x61, // dict[2] = "a"
      0x01, 0x62, // dict[3] = "b"
      ...u64(4), // keys_count
      0x02, 0x00, 0x01, 0x03, // keys -> a, NULL, "", b
    ];
    const nodes = decodeColumn('LowCardinality(Nullable(String))', data, 4);
    expect(nodes.map((n) => n.value)).toEqual(['a', null, '', 'b']);
    // The empty-string row must be a real value, not NULL.
    expect(nodes[1].value).toBeNull();
    expect(nodes[2].value).toBe('');
  });

  it('Variant(String, UInt64) [42, "hi", NULL] (BASIC mode)', () => {
    const data = [
      ...u64(0), // BASIC mode
      0x01, 0x00, 0xff, // discriminators: UInt64, String, NULL
      0x02, 0x68, 0x69, // String run "hi"
      ...u64(42), // UInt64 run
    ];
    expect(values('Variant(String, UInt64)', data, 3)).toEqual([42n, 'hi', null]);
  });

  it('Dynamic default (internal-variant V1) [42::UInt64, "hi", NULL]', () => {
    const data = [
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // version = 1
      0x02, // max_dynamic_types
      0x02, // num_dynamic_types
      0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, // "String"
      0x06, 0x55, 0x49, 0x6e, 0x74, 0x36, 0x34, // "UInt64"
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // variant mode = 0
      0x02, 0x01, 0xff, // discriminators (sorted: SharedVariant,String,UInt64): UInt64, String, NULL
      0x02, 0x68, 0x69, // String run "hi"
      0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // UInt64 run 42
    ];
    // The Dynamic header node is values[0]; row values follow.
    const nodes = decodeColumn('Dynamic', data, 3);
    expect(nodes.slice(1).map((n) => n.value)).toEqual([42n, 'hi', null]);
  });

  it('Dynamic FLATTENED (v3) [42::UInt64, "hi", NULL]', () => {
    const data = [
      0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // version = 3
      0x02, // num_types
      0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, // "String"
      0x06, 0x55, 0x49, 0x6e, 0x74, 0x36, 0x34, // "UInt64"
      0x01, 0x00, 0x02, // discriminators (wire order): UInt64(1), String(0), NULL(2)
      0x02, 0x68, 0x69, // String run "hi"
      0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // UInt64 run 42
    ];
    const nodes = decodeColumn('Dynamic', data, 3);
    expect(nodes.slice(1).map((n) => n.value)).toEqual([42n, 'hi', null]);
  });
});

describe('Native spec — JSON', () => {
  it('JSON default (Object v0) {"a":1,"b":"hi"}', () => {
    const data = [
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // version 0
      0x02, // max_dynamic_paths
      0x02, // num_dynamic_paths
      0x01, 0x61, 0x01, 0x62, // "a", "b"
      // "a" Dynamic structure (v1)
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x05, 0x49, 0x6e, 0x74, 0x36, 0x34,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      // "b" Dynamic structure (v1)
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      // "a" data: disc 0, Int64 = 1
      0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      // "b" data: disc 0, String "hi"
      0x01, 0x02, 0x68, 0x69,
      // shared data offset
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    const nodes = decodeColumn('JSON', data, 1);
    expect(nodes[0].value).toEqual({ a: 1n, b: 'hi' });
  });

  it('JSON Tier-1 String fallback (version 1)', () => {
    const data = [
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // version 1
      0x07, 0x7b, 0x22, 0x61, 0x22, 0x3a, 0x31, 0x7d, // {"a":1}
      0x0a, 0x7b, 0x22, 0x78, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x7d, // {"x":true}
    ];
    const nodes = decodeColumn('JSON', data, 2);
    expect(nodes.map((n) => n.value)).toEqual([{ a: 1 }, { x: true }]);
    expect(nodes.map((n) => n.metadata?.jsonText)).toEqual(['{"a":1}', '{"x":true}']);
  });

  it('JSON FLATTENED (v3) {"a":1,"b":"hi"} — both dynamic', () => {
    const data = [
      0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // version 3
      0x02, // num_dynamic_paths
      0x01, 0x61, 0x01, 0x62, // "a", "b"
      // "a" Dynamic prefix (v3, [Int64])
      0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x05, 0x49, 0x6e, 0x74, 0x36, 0x34,
      // "b" Dynamic prefix (v3, [String])
      0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67,
      // "a" data: disc 0, Int64 = 1
      0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      // "b" data: disc 0, String "hi"
      0x00, 0x02, 0x68, 0x69,
    ];
    const nodes = decodeColumn('JSON', data, 1);
    expect(nodes[0].value).toEqual({ a: 1n, b: 'hi' });
  });

  it('JSON FLATTENED (v3) with typed path JSON(a UInt32)', () => {
    const data = [
      0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // version 3
      0x01, // num_dynamic_paths
      0x01, 0x62, // dynamic path "b"
      // "b" Dynamic prefix (v3, [String])
      0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67,
      // typed path "a" data: UInt32 = 7
      0x07, 0x00, 0x00, 0x00,
      // "b" data: disc 0, String "hi"
      0x00, 0x02, 0x68, 0x69,
    ];
    const nodes = decodeColumn('JSON(a UInt32)', data, 1);
    expect(nodes[0].value).toEqual({ a: 7, b: 'hi' });
  });
});
