import { describe, it, beforeAll, afterAll } from 'vitest';
import {
  TestContext,
  decodeRowBinary,
  decodeNative,
  wrapRowBinaryResult,
  wrapNativeResult,
} from './test-helpers';
import { VALIDATION_TEST_CASES } from './validation-cases';

/**
 * Validation tests - verify decoded values and structure
 *
 * These tests check that decoded values match expected results and that
 * AST structure (children, byte ranges, metadata) is correct.
 */
describe('Validation Tests', () => {
  const ctx = new TestContext();

  beforeAll(async () => {
    await ctx.start();
  }, 120000);

  afterAll(async () => {
    await ctx.stop();
  });

  describe('RowBinary Format', () => {
    const rowBinaryCases = VALIDATION_TEST_CASES.filter(c => c.rowBinaryValidator);

    it.each(rowBinaryCases)(
      '$name',
      async ({ query, settings, rowBinaryValidator }) => {
        const data = await ctx.queryRowBinary(query, settings);
        const parsed = decodeRowBinary(data);
        const result = wrapRowBinaryResult(parsed, data.length);
        rowBinaryValidator!(result);
      },
    );
  });

  describe('Native Format', () => {
    const nativeCases = VALIDATION_TEST_CASES.filter(c => c.nativeValidator);

    it.each(nativeCases)(
      '$name',
      async ({ query, settings, nativeValidator }) => {
        const data = await ctx.queryNative(query, settings);
        const parsed = decodeNative(data);
        const result = wrapNativeResult(parsed, data.length);
        nativeValidator!(result);
      },
    );
  });
}, 300000);
