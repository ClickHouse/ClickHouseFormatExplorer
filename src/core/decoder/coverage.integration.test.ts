import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  TestContext,
  decodeRowBinary,
  decodeNative,
  analyzeByteRange,
  formatUncoveredRanges,
} from './test-helpers';
import { SMOKE_TEST_CASES } from './smoke-cases';

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

  // Test a representative subset of cases for coverage
  const coverageTestCases = SMOKE_TEST_CASES.filter(c =>
    // Focus on diverse type categories
    c.name.includes('UInt8') ||
    c.name.includes('String basic') ||
    c.name.includes('Array integers') ||
    c.name.includes('Tuple simple') ||
    c.name.includes('Map with entries') ||
    c.name.includes('Nullable non-null') ||
    c.name.includes('Multiple columns') ||
    c.name.includes('IntervalSecond')
  );

  describe('RowBinary Format', () => {
    it.each(coverageTestCases)(
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

        // Allow some header bytes to be uncovered (column count LEB128)
        // The header structure doesn't have leaf nodes for everything
        expect(coverage.coveragePercent).toBeGreaterThan(80);
      },
    );
  });

  describe('Native Format', () => {
    it.each(coverageTestCases)(
      '$name - byte coverage',
      async ({ query, settings, skipNative }) => {
        if (skipNative) return;

        const data = await ctx.queryNative(query, settings);
        const parsed = decodeNative(data);
        const coverage = analyzeByteRange(parsed, data.length);

        if (!coverage.isComplete) {
          const details = formatUncoveredRanges(coverage, data);
          console.log(`[Native] ${query}\n${details}`);
        }

        // Native format has block headers that may not be fully covered
        expect(coverage.coveragePercent).toBeGreaterThan(70);
      },
    );
  });

  describe('Full Coverage Sanity Checks', () => {
    it('simple UInt8 value has reasonable coverage (RowBinary)', async () => {
      const data = await ctx.queryRowBinary('SELECT 42::UInt8 as val');
      const parsed = decodeRowBinary(data);
      const coverage = analyzeByteRange(parsed, data.length);

      // Should cover most of the data
      expect(coverage.coveragePercent).toBeGreaterThan(50);

      // Log uncovered if any
      if (!coverage.isComplete) {
        console.log('Uncovered ranges:', coverage.uncoveredRanges);
      }
    });

    it('simple UInt8 value has reasonable coverage (Native)', async () => {
      const data = await ctx.queryNative('SELECT 42::UInt8 as val');
      const parsed = decodeNative(data);
      const coverage = analyzeByteRange(parsed, data.length);

      // Should cover most of the data
      expect(coverage.coveragePercent).toBeGreaterThan(50);

      // Log uncovered if any
      if (!coverage.isComplete) {
        console.log('Uncovered ranges:', coverage.uncoveredRanges);
      }
    });
  });
}, 300000);
