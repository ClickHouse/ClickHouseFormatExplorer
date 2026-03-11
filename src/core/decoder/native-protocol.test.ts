import { describe, expect, it } from 'vitest';
import { NativeDecoder } from './native-decoder';

function encodeLeb128(value: number | bigint): number[] {
  let current = BigInt(value);
  const bytes: number[] = [];

  do {
    let byte = Number(current & 0x7fn);
    current >>= 7n;
    if (current !== 0n) {
      byte |= 0x80;
    }
    bytes.push(byte);
  } while (current !== 0n);

  return bytes;
}

function encodeString(value: string): number[] {
  const bytes = Array.from(new TextEncoder().encode(value));
  return [...encodeLeb128(bytes.length), ...bytes];
}

function encodeUInt64LE(value: number | bigint): number[] {
  const bytes: number[] = [];
  let current = BigInt(value);
  for (let i = 0; i < 8; i++) {
    bytes.push(Number(current & 0xffn));
    current >>= 8n;
  }
  return bytes;
}

function encodeSparseOffsets(nonDefaultRows: number[], rowCount: number): number[] {
  const END_OF_GRANULE_FLAG = 1n << 62n;
  const bytes: number[] = [];
  let start = 0;

  for (const row of nonDefaultRows) {
    const groupSize = row - start;
    bytes.push(...encodeLeb128(groupSize));
    start += groupSize + 1;
  }

  const trailingDefaults = start < rowCount ? rowCount - start : 0;
  bytes.push(...encodeLeb128(BigInt(trailingDefaults) | END_OF_GRANULE_FLAG));

  return bytes;
}

function collectNodes(node: unknown): Array<{ type?: string; label?: string; value?: unknown; children?: unknown[] }> {
  if (!node || typeof node !== 'object') {
    return [];
  }

  const typedNode = node as { children?: unknown[] };
  const nodes = [typedNode as { type?: string; label?: string; value?: unknown; children?: unknown[] }];
  for (const child of typedNode.children ?? []) {
    nodes.push(...collectNodes(child));
  }
  return nodes;
}

describe('NativeDecoder protocol-aware parsing', () => {
  it('parses legacy HTTP Native blocks without protocol metadata', () => {
    const bytes = new Uint8Array([
      0x01, // numColumns
      0x02, // numRows
      ...encodeString('n'),
      ...encodeString('UInt8'),
      0x01,
      0x02,
    ]);

    const parsed = new NativeDecoder(bytes, 0).decode();

    expect(parsed.blocks).toHaveLength(1);
    expect(parsed.blocks?.[0].header.blockInfo).toBeUndefined();
    expect(parsed.blocks?.[0].columns[0].values.map((node) => node.value)).toEqual([1, 2]);
  });

  it('parses BlockInfo and sparse serialization metadata for modern protocol versions', () => {
    const bytes = new Uint8Array([
      0x01, // field 1: is_overflows
      0x00, // false
      0x02, // field 2: bucket_num
      0xff, 0xff, 0xff, 0xff, // -1
      0x03, // field 3: out_of_order_buckets
      0x00, // empty vector
      0x00, // BlockInfo terminator
      0x01, // numColumns
      0x03, // numRows
      ...encodeString('n'),
      ...encodeString('UInt8'),
      0x01, // has_custom
      0x01, // SPARSE kind stack
      ...encodeSparseOffsets([1], 3),
      0x07, // non-default value
    ]);

    const parsed = new NativeDecoder(bytes, 54483).decode();
    const block = parsed.blocks?.[0];
    const column = block?.columns[0];

    expect(block?.header.blockInfo?.fields.map((field) => field.fieldName)).toEqual([
      'is_overflows',
      'bucket_num',
      'out_of_order_buckets',
    ]);
    expect(column?.serializationInfo?.hasCustomSerialization).toBe(true);
    expect(column?.serializationInfo?.kindStack).toEqual(['DEFAULT', 'SPARSE']);
    expect(column?.values.map((node) => node.value)).toEqual([0, 7, 0]);
    expect(column?.values[0].metadata?.isDefaultValue).toBe(true);
    expect(column?.values[1].metadata?.isDefaultValue).toBeUndefined();
  });

  it('rejects BlockInfo field 3 before protocol version 54480', () => {
    const bytes = new Uint8Array([
      0x03,
      0x00,
      0x00,
      0x01,
      0x01,
      ...encodeString('n'),
      ...encodeString('UInt8'),
      0x07,
    ]);

    expect(() => new NativeDecoder(bytes, 54473).decode()).toThrow(
      'BlockInfo field 3 requires protocol version 54480+',
    );
  });

  it('parses replicated serialization kind stacks', () => {
    const bytes = new Uint8Array([
      0x01,
      0x00,
      0x02,
      0xff, 0xff, 0xff, 0xff,
      0x03,
      0x01,
      0x05, 0x00, 0x00, 0x00,
      0x00,
      0x01,
      0x02,
      ...encodeString('n'),
      ...encodeString('UInt8'),
      0x01,
      0x04,
      0x02,
      0x01,
      0x00,
      0x01,
      0x02,
      0x07,
      0x09,
    ]);

    const parsed = new NativeDecoder(bytes, 54482).decode();
    expect(parsed.blocks?.[0].columns[0].serializationInfo?.kindStack).toEqual(['DEFAULT', 'REPLICATED']);
    expect(parsed.blocks?.[0].columns[0].values.map((node) => node.value)).toEqual([7, 9]);
    expect(parsed.blocks?.[0].columns[0].values[0].metadata?.replicatedIndex).toBe(0);
    expect(parsed.blocks?.[0].columns[0].values[1].metadata?.replicatedIndex).toBe(1);
  });

  it('parses nullable sparse serialization', () => {
    const bytes = new Uint8Array([
      0x01,
      0x00,
      0x02,
      0xff, 0xff, 0xff, 0xff,
      0x03,
      0x00,
      0x00,
      0x01,
      0x03,
      ...encodeString('n'),
      ...encodeString('Nullable(UInt8)'),
      0x01,
      0x01,
      ...encodeSparseOffsets([1], 3),
      0x07,
    ]);

    const parsed = new NativeDecoder(bytes, 54483).decode();
    expect(parsed.blocks?.[0].columns[0].serializationInfo?.kindStack).toEqual(['DEFAULT', 'SPARSE']);
    expect(parsed.blocks?.[0].columns[0].values.map((node) => node.value)).toEqual([null, 7, null]);
    expect(parsed.blocks?.[0].columns[0].values[0].metadata?.isNull).toBe(true);
    expect(parsed.blocks?.[0].columns[0].values[1].metadata?.isNull).toBe(false);
  });

  it('parses JSON object v2 with Dynamic v2 prefixes', () => {
    const bytes = new Uint8Array([
      0x00,
      0x01,
      0x01,
      ...encodeString('j'),
      ...encodeString('JSON(a UInt8)'),
      0x00,
      ...encodeUInt64LE(2),
      0x01,
      ...encodeString('b'),
      ...encodeUInt64LE(2),
      0x01,
      ...encodeString('String'),
      ...encodeUInt64LE(0),
      0x2a,
      0x01,
      ...encodeString('hi'),
      ...encodeUInt64LE(0),
    ]);

    const parsed = new NativeDecoder(bytes, 54473).decode();
    const value = parsed.blocks?.[0].columns[0].values[0];

    expect(value?.type).toBe('JSON');
    expect(value?.value).toEqual({ a: 42, b: 'hi' });

    const nodes = collectNodes(value);
    const objectVersion = nodes.find((node) => node.label === 'version');
    const dynamicVersion = nodes.find(
      (node) => node.type === 'UInt64' && node.label === 'dynamic_version',
    );

    expect(objectVersion?.value).toBe(2n);
    expect(dynamicVersion?.value).toBe(2n);
    expect(nodes.some((node) => node.label === 'max_dynamic_paths')).toBe(false);
    expect(nodes.some((node) => node.label === 'max_dynamic_types')).toBe(false);
  });
});
