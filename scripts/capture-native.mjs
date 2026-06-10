#!/usr/bin/env node
// @ts-check
/**
 * CLI: capture a single native-protocol query exchange to a .chproto dump.
 *
 *   node scripts/capture-native.mjs --query "SELECT 1" --out capture.chproto
 *
 * Options:
 *   --query <sql>        (required) SQL to run
 *   --out <file>         output dump path (default: capture.chproto)
 *   --host <h>           server host (default 127.0.0.1)
 *   --port <p>           server native port (default 9000)
 *   --user / --password / --database
 *   --client <path>      path to clickhouse-client
 *   --setting k=v        per-query setting (repeatable)
 */

import fs from 'node:fs';
import { captureQuery, encodeDump } from './native-proxy.mjs';

function parseArgs(argv) {
  const out = { settings: {}, clientArgs: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    switch (a) {
      case '--query': out.query = next(); break;
      case '--out': out.out = next(); break;
      case '--host': out.host = next(); break;
      case '--port': out.port = Number(next()); break;
      case '--user': out.user = next(); break;
      case '--password': out.password = next(); break;
      case '--database': out.database = next(); break;
      case '--client': out.clientPath = next(); break;
      case '--setting': {
        const [k, ...rest] = next().split('=');
        out.settings[k] = rest.join('=');
        break;
      }
      default: out.clientArgs.push(a);
    }
  }
  return out;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (!opts.query) {
    console.error('error: --query is required');
    process.exit(2);
  }
  const outPath = opts.out ?? 'capture.chproto';
  const capture = await captureQuery(opts);
  fs.writeFileSync(outPath, encodeDump(capture));
  const total = capture.c2s.length + capture.s2c.length;
  console.error(
    `captured ${capture.segments.length} segments → ${outPath} ` +
    `(C2S ${capture.c2s.length}B, S2C ${capture.s2c.length}B, total ${total}B)`,
  );
  if (capture.meta.stderr) console.error(`client stderr: ${capture.meta.stderr}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
