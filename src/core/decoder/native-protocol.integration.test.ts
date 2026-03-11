import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NATIVE_PROTOCOL_PRESETS } from '../types/native-protocol';
import { TestContext, decodeNative } from './test-helpers';
import { AstNode } from '../types/ast';

interface NativeProtocolMatrixCase {
  name: string;
  query: string;
  settings?: Record<string, string | number>;
  assertParsed: (parsed: ReturnType<typeof decodeNative>, revision: number) => void;
}

function collectNodes(nodes: AstNode[]): AstNode[] {
  const collected: AstNode[] = [];

  const visit = (node: AstNode) => {
    collected.push(node);
    node.children?.forEach(visit);
  };

  nodes.forEach(visit);
  return collected;
}

const NATIVE_PROTOCOL_MATRIX_CASES: NativeProtocolMatrixCase[] = [
  {
    name: 'simple UInt8 column',
    query: 'SELECT number::UInt8 AS val FROM numbers(3)',
    assertParsed: (parsed, revision) => {
      expect(parsed.blocks).toHaveLength(1);
      expect(parsed.blocks?.[0].columns[0].values.map((node) => node.value)).toEqual([0, 1, 2]);
      expect(parsed.blocks?.[0].header.blockInfo === undefined).toBe(revision === 0);
    },
  },
  {
    name: 'LowCardinality compatibility',
    query: 'SELECT toLowCardinality(toString(number % 2)) AS val FROM numbers(4)',
    settings: { allow_suspicious_low_cardinality_types: 1 },
    assertParsed: (parsed, revision) => {
      const column = parsed.blocks?.[0].columns[0];
      expect(column).toBeDefined();
      if (revision !== 0 && revision < 54405) {
        expect(column?.typeString).toBe('String');
      } else {
        expect(column?.typeString).toBe('LowCardinality(String)');
      }
    },
  },
  {
    name: 'AggregateFunction compatibility',
    query: 'SELECT avgState(number) AS val FROM numbers(10)',
    assertParsed: (parsed) => {
      const node = parsed.blocks?.[0].columns[0].values[0];
      expect(node?.type).toBe('AggregateFunction(avg, UInt64)');
      expect(node?.displayValue).toContain('avg=4.50');
    },
  },
  {
    name: 'sparse serialization gate',
    query: 'SELECT if(number = 5, 1, 0)::UInt8 AS sparse_val FROM numbers(10)',
    assertParsed: (parsed, revision) => {
      const column = parsed.blocks?.[0].columns[0];
      expect(column).toBeDefined();
      expect(column?.values.map((node) => node.value)).toEqual([0, 0, 0, 0, 0, 1, 0, 0, 0, 0]);

      if (revision === 0 || revision < 54454) {
        expect(column?.serializationInfo).toBeUndefined();
      } else if (revision < 54465) {
        expect(column?.serializationInfo?.hasCustomSerialization).toBe(false);
      } else {
        expect(column?.serializationInfo?.hasCustomSerialization).toBe(true);
        expect(column?.serializationInfo?.kindStack).toEqual(['DEFAULT', 'SPARSE']);
      }
    },
  },
  {
    name: 'Dynamic serialization version gate',
    query: 'SELECT 42::Dynamic AS val',
    settings: { allow_experimental_dynamic_type: 1 },
    assertParsed: (parsed, revision) => {
      const column = parsed.blocks?.[0].columns[0];
      const headerNode = column?.values[0];
      expect(headerNode?.type).toBe('Dynamic.Header');
      expect((headerNode?.value as { version: number }).version).toBe(revision >= 54473 ? 2 : 1);
    },
  },
  {
    name: 'Nullable sparse serialization gate',
    query: 'SELECT if(number = 5, 42, NULL)::Nullable(UInt8) AS sparse_nullable FROM numbers(10)',
    assertParsed: (parsed, revision) => {
      const column = parsed.blocks?.[0].columns[0];
      expect(column).toBeDefined();
      expect(column?.values.map((node) => node.value)).toEqual([null, null, null, null, null, 42, null, null, null, null]);

      if (revision === 0 || revision < 54454) {
        expect(column?.serializationInfo).toBeUndefined();
      } else if (revision < 54483) {
        expect(column?.serializationInfo?.hasCustomSerialization).toBe(false);
      } else {
        expect(column?.serializationInfo?.hasCustomSerialization).toBe(true);
        expect(column?.serializationInfo?.kindStack).toEqual(['DEFAULT', 'SPARSE']);
      }
    },
  },
  {
    name: 'JSON dynamic-path serialization version gate',
    query: `SELECT '{"ip":"127.0.0.1","name":"test"}'::JSON(ip IPv4) AS val`,
    settings: { allow_experimental_json_type: 1 },
    assertParsed: (parsed, revision) => {
      const column = parsed.blocks?.[0].columns[0];
      const jsonNode = column?.values[0];
      expect(jsonNode?.type).toBe('JSON');

      const structureNodes = collectNodes(jsonNode ? [jsonNode] : []).filter(
        (node) => node.type === 'Dynamic.structure',
      );
      expect(structureNodes.length).toBeGreaterThan(0);

      const versionNode = structureNodes[0].children?.find((child) => child.label === 'dynamic_version');
      expect(versionNode?.value).toBe(revision >= 54473 ? 2n : 1n);
    },
  },
];

describe('Native protocol revision matrix', () => {
  const ctx = new TestContext();

  beforeAll(async () => {
    await ctx.start();
  }, 120000);

  afterAll(async () => {
    await ctx.stop();
  });

  for (const testCase of NATIVE_PROTOCOL_MATRIX_CASES) {
    describe(testCase.name, () => {
      it.each(NATIVE_PROTOCOL_PRESETS.map((preset) => preset.value))(
        'revision %s',
        async (revision) => {
          const data = await ctx.queryNative(testCase.query, {
            ...(testCase.settings ?? {}),
            client_protocol_version: revision,
          });
          const parsed = decodeNative(data, revision);

          testCase.assertParsed(parsed, revision);
        },
      );
    });
  }
}, 300000);
