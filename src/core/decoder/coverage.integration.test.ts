import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  TestContext,
  decodeRowBinary,
  decodeNative,
  analyzeByteRange,
  formatUncoveredRanges,
} from './test-helpers';
import { SMOKE_TEST_CASES } from './smoke-cases';
import { NATIVE_PROTOCOL_PRESETS } from '../types/native-protocol';

interface NativeCoverageMatrixCase {
  name: string;
  query: string;
  settings?: Record<string, string | number>;
}

const NATIVE_COVERAGE_MATRIX_CASES: NativeCoverageMatrixCase[] = [
  {
    name: 'simple UInt8 column',
    query: 'SELECT number::UInt8 AS val FROM numbers(3)',
  },
  {
    name: 'multiple columns baseline',
    query: "SELECT 42::UInt32 as int_col, 'hello'::String as str_col, true::Bool as bool_col, 3.14::Float64 as float_col",
  },
  {
    name: 'Array integers',
    query: 'SELECT [1, 2, 3]::Array(UInt32) as val',
  },
  {
    name: 'Tuple simple',
    query: "SELECT (42, 'hello')::Tuple(UInt32, String) as val",
  },
  {
    name: 'Map with entries',
    query: "SELECT map('a', 1, 'b', 2)::Map(String, UInt32) as val",
  },
  {
    name: 'LowCardinality compatibility',
    query: 'SELECT toLowCardinality(toString(number % 2)) AS val FROM numbers(4)',
    settings: { allow_suspicious_low_cardinality_types: 1 },
  },
  {
    name: 'AggregateFunction compatibility',
    query: 'SELECT avgState(number) AS val FROM numbers(10)',
  },
  {
    name: 'serialization metadata gate',
    query: 'SELECT if(number = 5, 1, 0)::UInt8 AS sparse_val FROM numbers(10)',
  },
  {
    name: 'Nullable serialization metadata gate',
    query: 'SELECT if(number = 5, 42, NULL)::Nullable(UInt8) AS sparse_nullable FROM numbers(10)',
  },
  {
    name: 'Dynamic serialization version gate',
    query: 'SELECT 42::Dynamic AS val',
    settings: { allow_experimental_dynamic_type: 1 },
  },
  {
    name: 'JSON dynamic-path serialization version gate',
    query: `SELECT '{"ip":"127.0.0.1","name":"test"}'::JSON(ip IPv4) AS val`,
    settings: { allow_experimental_json_type: 1 },
  },
];

/**
 * Byte coverage tests - verify that the AST leaf nodes cover all bytes in the data
 *
 * These tests ensure that every byte in the decoded data is accounted for by
 * at least one leaf node in the AST tree.
 */
describe('Byte Coverage Tests', () => {
  const ctx = new TestContext();

  beforeAll(async () => {
    await ctx.start();
  }, 120000);

  afterAll(async () => {
    await ctx.stop();
  });

  describe('RowBinary Format', () => {
    it.each(SMOKE_TEST_CASES)(
      '$name - byte coverage',
      async ({ query, settings, skipRowBinary }) => {
        if (skipRowBinary) return;

        const data = await ctx.queryRowBinary(query, settings);
        const parsed = decodeRowBinary(data);
        const coverage = analyzeByteRange(parsed, data.length);

        if (!coverage.isComplete) {
          const details = formatUncoveredRanges(coverage, data);
          console.log(`[RowBinary] ${query}\n${details}`);
        }

        expect(coverage.isComplete).toBe(true);
      },
    );
  });

  describe('Native Format', () => {
    for (const testCase of NATIVE_COVERAGE_MATRIX_CASES) {
      describe(testCase.name, () => {
        it.each(NATIVE_PROTOCOL_PRESETS.map((preset) => preset.value))(
          'revision %s - byte coverage',
          async (revision) => {
            const data = await ctx.queryNative(testCase.query, {
              ...(testCase.settings ?? {}),
              client_protocol_version: revision,
            });
            const parsed = decodeNative(data, revision);
            const coverage = analyzeByteRange(parsed, data.length);

            if (!coverage.isComplete) {
              const details = formatUncoveredRanges(coverage, data);
              console.log(`[Native r${revision}] ${testCase.query}\n${details}`);
            }

            expect(coverage.isComplete).toBe(true);
          },
        );
      });
    }
  });
}, 300000);
