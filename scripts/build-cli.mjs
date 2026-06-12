#!/usr/bin/env node
// Bundle the chfx CLI into a single self-contained ESM file for publishing.
// The decoders in src/core are dependency- and DOM-free, so esbuild can bundle
// them for Node. The package version is injected via `define` so the binary
// reports it without reading package.json at runtime.

import esbuild from 'esbuild';
import { readFileSync } from 'node:fs';
import { chmodSync } from 'node:fs';

const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const outfile = 'dist/cli/index.js';

await esbuild.build({
  entryPoints: ['src/cli/index.ts'],
  bundle: true,
  platform: 'node',
  target: 'node20',
  format: 'esm',
  outfile,
  banner: { js: '#!/usr/bin/env node' },
  define: { __CHFX_VERSION__: JSON.stringify(pkg.version) },
  logLevel: 'info',
});

chmodSync(outfile, 0o755);
