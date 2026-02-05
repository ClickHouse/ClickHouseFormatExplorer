import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { TestContext, decodeRowBinary, decodeNative } from './test-helpers';
import { SMOKE_TEST_CASES } from './smoke-cases';

/**
 * Smoke tests - verify parsing succeeds without value validation
 *
 * These tests ensure that the decoders can successfully parse all supported
 * ClickHouse types without throwing errors. Detailed value validation is
 * handled in validation.integration.test.ts.
 */
describe('Smoke Tests', () => {
  const ctx = new TestContext();

  beforeAll(async () => {
    await ctx.start();
  }, 120000);

  afterAll(async () => {
    await ctx.stop();
  });

  describe('RowBinary Format', () => {
    const rowBinaryCases = SMOKE_TEST_CASES.filter(c => !c.skipRowBinary);

    it.each(rowBinaryCases)(
      '$name',
      async ({ query, settings }) => {
        const data = await ctx.queryRowBinary(query, settings);
        expect(() => decodeRowBinary(data)).not.toThrow();

        // Basic structure validation
        const result = decodeRowBinary(data);
        expect(result.header.columns.length).toBeGreaterThan(0);
        expect(result.rows).toBeDefined();
      },
    );
  });

  describe('Native Format', () => {
    const nativeCases = SMOKE_TEST_CASES.filter(c => !c.skipNative);

    it.each(nativeCases)(
      '$name',
      async ({ query, settings }) => {
        const data = await ctx.queryNative(query, settings);
        expect(() => decodeNative(data)).not.toThrow();

        // Basic structure validation
        const result = decodeNative(data);
        expect(result.header.columns.length).toBeGreaterThan(0);
        expect(result.blocks).toBeDefined();
      },
    );
  });
}, 300000);
