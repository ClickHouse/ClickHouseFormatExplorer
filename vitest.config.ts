import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    testTimeout: 60000,
    hookTimeout: 120000,
    pool: 'forks',
    exclude: ['e2e/**', 'node_modules/**'],
  },
});
