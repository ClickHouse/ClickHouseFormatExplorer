import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { execFileSync, execSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';

import { decodeBuffer, decodeCommand } from './commands/decode';
import { queryCommand } from './commands/query';
import { captureCommand } from './commands/capture';
import { proxyCommand, type ProxyDeps } from './commands/proxy';
import { resolveCaptureOptions, resolveHttpConnection, parseHostPort } from './connection';
import { parseArgs, stringOption, boolOption, arrayOption } from './args';
import { stringify, CliError } from './output';
import { ClickHouseFormat } from '../core/types/formats';
import { parseChprotoDump } from '../core/decoder/protocol-dump';
import { captureQuery, startCaptureProxy, encodeDump, type Capture } from '../../scripts/native-proxy.mjs';

/** Run `fn` with env vars temporarily set, restoring prior values after. */
function withEnv(vars: Record<string, string | undefined>, fn: () => void): void {
  const prior: Record<string, string | undefined> = {};
  for (const key of Object.keys(vars)) {
    prior[key] = process.env[key];
    if (vars[key] === undefined) delete process.env[key];
    else process.env[key] = vars[key];
  }
  try {
    fn();
  } finally {
    for (const key of Object.keys(prior)) {
      if (prior[key] === undefined) delete process.env[key];
      else process.env[key] = prior[key];
    }
  }
}

const FIXTURE_DIR = fileURLToPath(new URL('../core/decoder/fixtures/protocol/', import.meta.url));
const CLI_ENTRY = fileURLToPath(new URL('./index.ts', import.meta.url));

const fixtures = readdirSync(FIXTURE_DIR).filter((f) => f.endsWith('.chproto'));
const fixturePath = (name: string) => `${FIXTURE_DIR}${name}`;
const readFixture = (name: string) => new Uint8Array(readFileSync(fixturePath(name)));

const enc = (s: string) => [...s].map((c) => c.charCodeAt(0));
// Minimal valid bodies for a single column `x UInt8` with one row valued 1.
const ROWBINARY_BODY = new Uint8Array([0x01, 0x01, ...enc('x'), 0x05, ...enc('UInt8'), 0x01]);
const NATIVE_BODY = new Uint8Array([0x01, 0x01, 0x01, ...enc('x'), 0x05, ...enc('UInt8'), 0x01]);

/** Build a fake native capture from a real fixture (for an injected captureQuery). */
function fakeCaptureOf(name: string) {
  const { c2s, s2c, meta } = parseChprotoDump(readFixture(name));
  const cb = Buffer.from(c2s);
  const sb = Buffer.from(s2c);
  return {
    c2s: cb,
    s2c: sb,
    segments: [
      { dir: 0 as const, data: cb },
      { dir: 1 as const, data: sb },
    ],
    meta: meta ?? {},
  };
}

describe('parseArgs', () => {
  it('parses positionals, long/short flags, =values, and value flags', () => {
    const args = parseArgs(['file.bin', '--format', 'native', '-f', 'rowbinary', '--compact', '--protocol-version=54483'], {
      valueFlags: ['format', 'protocol-version'],
      aliases: { f: 'format' },
    });
    expect(args.positionals).toEqual(['file.bin']);
    expect(args.options.format).toBe('rowbinary'); // later -f overrides
    expect(args.options.compact).toBe(true);
    expect(stringOption(args, 'protocol-version')).toBe('54483');
    expect(boolOption(args, 'compact')).toBe(true);
  });

  it('treats lone - as a positional and -- as end-of-options', () => {
    const args = parseArgs(['-', '--', '--not-a-flag']);
    expect(args.positionals).toEqual(['-', '--not-a-flag']);
  });

  it('throws a usage error when a value flag is missing its value', () => {
    expect(() => parseArgs(['--format'], { valueFlags: ['format'] })).toThrow(CliError);
  });
});

describe('decodeBuffer — protocol fixtures', () => {
  it('has fixtures to test', () => {
    expect(fixtures.length).toBeGreaterThan(0);
  });

  it.each(fixtures)('decodes %s as NativeProtocol with a negotiated version', (name) => {
    const bytes = readFixture(name);
    const result = decodeBuffer(bytes);
    expect(result.format).toBe(ClickHouseFormat.NativeProtocol);
    expect(result.formatDetected).toBe(true);
    expect(typeof result.protocolVersion).toBe('number');
    expect(result.protocol?.c2sLength).toBeGreaterThan(0);
    // outputBytes is the combined c2s+s2c stream the byteRanges index into,
    // so it strictly exceeds the client→server portion alone.
    expect(result.protocol!.c2sLength).toBeLessThan(result.outputBytes.length);
  });

  it.each(fixtures)('serializes %s to JSON without throwing (bigint/byte-safe)', (name) => {
    const result = decodeBuffer(readFixture(name));
    // The whole ParsedData must survive serialization (decoded values include bigints).
    const json = stringify({ data: result.parsed, bytesHex: Buffer.from(result.outputBytes).toString('hex') }, true);
    const round = JSON.parse(json);
    expect(typeof round.bytesHex).toBe('string');
    expect(round.bytesHex.length % 2).toBe(0);
    expect(round.bytesHex.length / 2).toBe(result.outputBytes.length);
  });
});

describe('decodeBuffer — raw bodies', () => {
  it('decodes a RowBinary body with explicit --format and via autodetect', () => {
    const forced = decodeBuffer(ROWBINARY_BODY, { format: 'rowbinary' });
    expect(forced.format).toBe(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    expect(forced.formatDetected).toBe(false);
    expect(forced.parsed.rows?.[0]?.values?.[0]?.value).toBe(1);

    const auto = decodeBuffer(ROWBINARY_BODY);
    expect(auto.format).toBe(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    expect(auto.formatDetected).toBe(true);
  });

  it('decodes a Native body with explicit --format and via autodetect', () => {
    const forced = decodeBuffer(NATIVE_BODY, { format: 'native' });
    expect(forced.format).toBe(ClickHouseFormat.Native);
    expect(forced.protocolVersion).toBe(0);

    const auto = decodeBuffer(NATIVE_BODY);
    expect(auto.format).toBe(ClickHouseFormat.Native);
    expect(auto.formatDetected).toBe(true);
  });

  it('throws a usage error when nothing decodes', () => {
    const garbage = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    expect(() => decodeBuffer(garbage)).toThrow(CliError);
  });

  it('terminates (no infinite loop / OOM) on tiny degenerate inputs', () => {
    // A 0-column RowBinary header (0x00) used to loop forever and exhaust memory.
    // These must each return or throw quickly — the vitest timeout guards regressions.
    for (const bytes of [[0x00, 0x02], [0x00], [0x01], [0x00, 0x00, 0x00]]) {
      const input = Uint8Array.from(bytes);
      try {
        decodeBuffer(input);
      } catch (err) {
        expect(err).toBeInstanceOf(CliError);
      }
      // Forcing rowbinary on a 0-column header must terminate too.
      expect(() => decodeBuffer(Uint8Array.from([0x00, 0x00]), { format: 'rowbinary' })).not.toThrow();
    }
  });

  it('propagates a decode failure when forced to the wrong format', () => {
    const chproto = readFixture(fixtures[0]);
    expect(() => decodeBuffer(chproto, { format: 'rowbinary' })).toThrow();
  });
});

describe('decodeCommand (file + envelope)', () => {
  it('builds the full envelope from a file path', async () => {
    interface DecodeEnvelope {
      chfx: Record<string, unknown>;
      source: Record<string, unknown>;
      format: string;
      conventions: { byteRange: string };
      bytesHex: string;
      protocol: { c2sLength: number };
    }
    const { data: rawData, compact } = await decodeCommand([fixturePath(fixtures[0]), '--compact']);
    const data = rawData as DecodeEnvelope;
    expect(compact).toBe(true);
    expect(data.chfx).toMatchObject({ tool: 'chfx', command: 'decode' });
    expect(data.source).toMatchObject({ kind: 'file' });
    expect(data.format).toBe('NativeProtocol');
    expect(data.conventions.byteRange).toContain('byteRange');
    expect(data.bytesHex.length / 2).toBeGreaterThan(0);
    // bytesHex covers the combined stream the ranges index into (c2s + s2c).
    expect(data.protocol.c2sLength).toBeLessThan(data.bytesHex.length / 2);
  });

  it('rejects an unknown --format value', async () => {
    await expect(decodeCommand([fixturePath(fixtures[0]), '--format', 'bogus'])).rejects.toThrow(CliError);
  });
});

describe('per-node inline bytes', () => {
  interface Envelope {
    nodeBytes: boolean;
    bytesHex: string;
    data: unknown;
  }

  function findNodeWithBytes(v: unknown): { start: number; end: number; bytes: string } | null {
    if (Array.isArray(v)) {
      for (const item of v) {
        const found = findNodeWithBytes(item);
        if (found) return found;
      }
      return null;
    }
    if (v && typeof v === 'object') {
      const o = v as Record<string, unknown>;
      const br = o.byteRange as { start?: unknown; end?: unknown } | undefined;
      if (br && typeof br.start === 'number' && typeof br.end === 'number' && typeof o.bytes === 'string') {
        return { start: br.start, end: br.end, bytes: o.bytes };
      }
      for (const key of Object.keys(o)) {
        const found = findNodeWithBytes(o[key]);
        if (found) return found;
      }
    }
    return null;
  }

  function anyBytesField(v: unknown): boolean {
    if (Array.isArray(v)) return v.some(anyBytesField);
    if (v && typeof v === 'object') {
      const o = v as Record<string, unknown>;
      if ('bytes' in o) return true;
      return Object.values(o).some(anyBytesField);
    }
    return false;
  }

  it("attaches each node's bytes inline, matching its byteRange slice of bytesHex", async () => {
    const { data } = await decodeCommand([fixturePath(fixtures[0])]);
    const env = data as Envelope;
    expect(env.nodeBytes).toBe(true);
    const node = findNodeWithBytes(env.data);
    expect(node).not.toBeNull();
    expect(node!.bytes).toBe(env.bytesHex.slice(node!.start * 2, node!.end * 2));
  });

  it('omits inline bytes with --no-node-bytes', async () => {
    const { data } = await decodeCommand([fixturePath(fixtures[0]), '--no-node-bytes']);
    const env = data as Envelope;
    expect(env.nodeBytes).toBe(false);
    expect(anyBytesField(env.data)).toBe(false);
  });
});

describe('parseArgs — repeatable flags', () => {
  it('accumulates multiFlags into an array', () => {
    const args = parseArgs(['--setting', 'a=1', '--setting', 'b=2'], { multiFlags: ['setting'] });
    expect(arrayOption(args, 'setting')).toEqual(['a=1', 'b=2']);
  });
});

describe('resolveCaptureOptions', () => {
  it('requires --query', () => {
    expect(() => resolveCaptureOptions(parseArgs([]))).toThrow(CliError);
  });

  it('takes flags over env and parses settings, dropping experimental when asked', () => {
    const args = parseArgs(
      ['--query', 'SELECT 1', '--host', 'h1', '--port', '9001', '--setting', 'max_threads=2', '--no-experimental-settings'],
      { valueFlags: ['query', 'host', 'port'], multiFlags: ['setting'] },
    );
    const opts = resolveCaptureOptions(args);
    expect(opts).toMatchObject({ query: 'SELECT 1', host: 'h1', port: 9001 });
    expect(opts.settings).toEqual({ max_threads: '2' });
  });

  it('sends experimental settings by default', () => {
    const opts = resolveCaptureOptions(parseArgs(['--query', 'SELECT 1'], { valueFlags: ['query'] }));
    expect(opts.settings).toHaveProperty('allow_experimental_json_type', '1');
  });

  it('rejects a non-numeric port', () => {
    expect(() => resolveCaptureOptions(parseArgs(['--query', 'x', '--port', 'abc'], { valueFlags: ['query', 'port'] }))).toThrow(
      CliError,
    );
  });

  it('rejects an out-of-range port', () => {
    expect(() =>
      resolveCaptureOptions(parseArgs(['--query', 'x', '--port', '99999'], { valueFlags: ['query', 'port'] })),
    ).toThrow(/between 1 and 65535/);
  });
});

describe('query / capture (with injected capture)', () => {
  // Use a fake capture from a real fixture so no server/clickhouse-client is needed.
  const fakeCapture = () => fakeCaptureOf(fixtures[0]);
  const deps = { captureQuery: async () => fakeCapture() };

  it('query captures + decodes in one step', async () => {
    const out = await queryCommand(['--query', 'SELECT 1'], deps);
    expect(out.stdout).toBe('json');
    const env = out.data as { chfx: { command: string }; source: { kind: string; query: string }; format: string; bytesHex: string };
    expect(env.chfx.command).toBe('query');
    expect(env.source).toMatchObject({ kind: 'query', query: 'SELECT 1' });
    expect(env.format).toBe('NativeProtocol');
    expect(env.bytesHex.length).toBeGreaterThan(0);
  });

  it('query --save writes a round-trippable .chproto dump', async () => {
    const path = join(tmpdir(), 'chfx-query-save.chproto');
    try {
      await queryCommand(['--query', 'SELECT 1', '--save', path], deps);
      const reparsed = parseChprotoDump(new Uint8Array(readFileSync(path)));
      expect(reparsed.c2s.length).toBe(fakeCapture().c2s.length);
    } finally {
      rmSync(path, { force: true });
    }
  });

  it('capture without --out streams a raw .chproto dump to stdout', async () => {
    const out = await captureCommand(['--query', 'SELECT 1'], deps);
    expect(out.stdout).toBe('raw');
    if (out.stdout !== 'raw') throw new Error('expected raw');
    const reparsed = parseChprotoDump(out.bytes);
    expect(reparsed.c2s.length).toBe(fakeCapture().c2s.length);
  });

  it('capture --out writes a file and returns a JSON summary', async () => {
    const path = join(tmpdir(), 'chfx-capture-out.chproto');
    try {
      const out = await captureCommand(['--query', 'SELECT 1', '--out', path], deps);
      expect(out.stdout).toBe('json');
      const data = (out as { data: { saved: string; bytes: number; segments: number } }).data;
      expect(data.saved).toBe(path);
      expect(data.bytes).toBeGreaterThan(0);
      expect(data.segments).toBe(2);
      expect(readFileSync(path).length).toBe(data.bytes);
    } finally {
      rmSync(path, { force: true });
    }
  });
});

describe('query --protocol http (injected fetch)', () => {
  const fetchReturning = (body: Uint8Array | string, status = 200): typeof fetch =>
    (async () => new Response(body, { status })) as typeof fetch;

  it('decodes a Native body and records the http transport', async () => {
    const out = await queryCommand(['--query', 'SELECT 1', '--protocol', 'http'], { fetch: fetchReturning(NATIVE_BODY) });
    const env = out.data as { format: string; source: { protocol: string; httpFormat: string } };
    expect(env.format).toBe('Native');
    expect(env.source.protocol).toBe('http');
    expect(env.source.httpFormat).toBe('Native');
  });

  it('decodes a RowBinaryWithNamesAndTypes body', async () => {
    const out = await queryCommand(
      ['--query', 'SELECT 1', '--protocol', 'http', '--format', 'RowBinaryWithNamesAndTypes'],
      { fetch: fetchReturning(ROWBINARY_BODY) },
    );
    expect((out.data as { format: string }).format).toBe('RowBinaryWithNamesAndTypes');
  });

  it('surfaces a non-2xx HTTP response as an error', async () => {
    await expect(
      queryCommand(['--query', 'x', '--protocol', 'http'], { fetch: fetchReturning('Code: 60', 404) }),
    ).rejects.toThrow(CliError);
  });

  it('rejects --format with tcp and --save with http', async () => {
    await expect(queryCommand(['--query', 'x', '--format', 'native'], { captureQuery: async () => { throw new Error('unused'); } }))
      .rejects.toThrow(/--format only applies/);
    await expect(queryCommand(['--query', 'x', '--protocol', 'http', '--save', '/tmp/x'], { fetch: fetchReturning(NATIVE_BODY) }))
      .rejects.toThrow(/--save only applies/);
  });
});

describe('decode — edge & error paths', () => {
  it('rejects empty input as a usage error', async () => {
    const path = join(tmpdir(), 'chfx-empty.bin');
    writeFileSync(path, Buffer.alloc(0));
    try {
      await expect(decodeCommand([path])).rejects.toThrow(/empty/);
    } finally {
      rmSync(path, { force: true });
    }
  });

  it('reports a missing file as an io error', async () => {
    await expect(decodeCommand([join(tmpdir(), 'chfx-does-not-exist.bin')])).rejects.toThrow(CliError);
  });

  it('rejects an invalid --protocol-version before reading input', async () => {
    await expect(decodeCommand(['whatever', '--protocol-version', '-1'])).rejects.toThrow(/non-negative integer/);
    await expect(decodeCommand(['whatever', '--protocol-version', 'abc'])).rejects.toThrow(/non-negative integer/);
  });

  it('reports an ambiguous raw body (both decoders accept) as a usage error', () => {
    // A 0x00 lead byte parses as a 0-column RowBinary header AND as a Native block.
    expect(() => decodeBuffer(Uint8Array.from([0x00, 0x02]))).toThrow(/ambiguous/);
  });

  it('rejects an unknown flag instead of silently ignoring it', async () => {
    await expect(decodeCommand(['file.bin', '--frmat', 'native'])).rejects.toThrow(/unknown option: --frmat/);
  });

  it('rejects an extra positional argument', async () => {
    await expect(decodeCommand(['a.bin', 'b.bin'])).rejects.toThrow(/unexpected argument: b\.bin/);
  });
});

describe('query — http request construction (spied fetch)', () => {
  it('builds URL params + auth headers + body correctly', async () => {
    let url = '';
    let init: RequestInit | undefined;
    const fetchSpy = (async (u: string | URL | Request, i?: RequestInit) => {
      url = String(u);
      init = i;
      return new Response(NATIVE_BODY, { status: 200 });
    }) as typeof fetch;

    await queryCommand(
      ['--query', 'SELECT 1', '--protocol', 'http', '--user', 'bob', '--password', 'pw', '--database', 'db1', '--setting', 'max_threads=2', '--no-experimental-settings'],
      { fetch: fetchSpy },
    );

    const parsed = new URL(url);
    expect(parsed.port).toBe('8123');
    expect(parsed.searchParams.get('default_format')).toBe('Native');
    expect(parsed.searchParams.get('max_threads')).toBe('2');
    expect(parsed.searchParams.get('database')).toBe('db1');
    const headers = init!.headers as Record<string, string>;
    expect(headers['X-ClickHouse-User']).toBe('bob');
    expect(headers['X-ClickHouse-Key']).toBe('pw');
    expect(init!.body).toBe('SELECT 1');
  });

  it('requests RowBinaryWithNamesAndTypes when --format selects it', async () => {
    let url = '';
    const fetchSpy = (async (u: string | URL | Request) => {
      url = String(u);
      return new Response(ROWBINARY_BODY, { status: 200 });
    }) as typeof fetch;
    await queryCommand(['--query', 'SELECT 1', '--protocol', 'http', '--format', 'RowBinaryWithNamesAndTypes'], { fetch: fetchSpy });
    expect(new URL(url).searchParams.get('default_format')).toBe('RowBinaryWithNamesAndTypes');
  });

  it('forwards --protocol-version as client_protocol_version', async () => {
    let url = '';
    const fetchSpy = (async (u: string | URL | Request) => {
      url = String(u);
      return new Response(NATIVE_BODY, { status: 200 });
    }) as typeof fetch;
    // decode of the v0 body at this revision may fail; we only assert the request param.
    await queryCommand(['--query', 'x', '--protocol', 'http', '--protocol-version', '54465'], { fetch: fetchSpy }).catch(() => {});
    expect(new URL(url).searchParams.get('client_protocol_version')).toBe('54465');
  });
});

describe('query / capture — failure modes', () => {
  const throwingCapture = { captureQuery: async () => { throw new Error('boom'); } };
  const fetchThrows = (async () => { throw new Error('ECONNREFUSED'); }) as typeof fetch;
  const fetchEmpty = (async () => new Response(new Uint8Array(0), { status: 200 })) as typeof fetch;

  it('wraps a tcp capture failure as an io error', async () => {
    await expect(queryCommand(['--query', 'x'], throwingCapture)).rejects.toThrow(/capture failed/);
  });

  it('wraps a --save write failure as an io error', async () => {
    const badPath = join(tmpdir(), 'chfx-no-such-dir', 'x.chproto');
    await expect(queryCommand(['--query', 'x', '--save', badPath], { captureQuery: async () => fakeCaptureOf(fixtures[0]) }))
      .rejects.toThrow(/cannot write --save/);
  });

  it('reports an empty http body as a decode error', async () => {
    await expect(queryCommand(['--query', 'x', '--protocol', 'http'], { fetch: fetchEmpty })).rejects.toThrow(/empty body/);
  });

  it('wraps an http transport failure as an io error', async () => {
    await expect(queryCommand(['--query', 'x', '--protocol', 'http'], { fetch: fetchThrows })).rejects.toThrow(/HTTP request failed/);
  });

  it('rejects an unknown --protocol', async () => {
    await expect(queryCommand(['--query', 'x', '--protocol', 'ftp'])).rejects.toThrow(/unknown --protocol/);
  });

  it('rejects an unknown http --format', async () => {
    await expect(queryCommand(['--query', 'x', '--protocol', 'http', '--format', 'json'], { fetch: fetchEmpty }))
      .rejects.toThrow(/unknown --format/);
  });

  it('rejects a typo\'d/unknown flag (e.g. --protcol) instead of ignoring it', async () => {
    await expect(queryCommand(['--query', 'x', '--protcol', 'http'])).rejects.toThrow(/unknown option: --protcol/);
  });

  it('rejects an unexpected positional argument', async () => {
    await expect(queryCommand(['--query', 'x', 'extra'])).rejects.toThrow(/unexpected argument: extra/);
  });

  it("rejects --save '-' (stdout already carries the JSON)", async () => {
    await expect(queryCommand(['--query', 'x', '--save', '-'], { captureQuery: async () => fakeCaptureOf(fixtures[0]) }))
      .rejects.toThrow(/--save '-' is not supported/);
  });

  it('wraps a capture-command failure as an io error', async () => {
    await expect(captureCommand(['--query', 'x'], throwingCapture)).rejects.toThrow(/capture failed/);
  });

  it('capture rejects an unknown flag', async () => {
    await expect(captureCommand(['--query', 'x', '--nope'])).rejects.toThrow(/unknown option: --nope/);
  });
});

describe('capture — aliases & raw stdout variants', () => {
  it('-o is an alias for --out', async () => {
    const path = join(tmpdir(), 'chfx-capture-alias.chproto');
    try {
      const out = await captureCommand(['--query', 'x', '-o', path], { captureQuery: async () => fakeCaptureOf(fixtures[0]) });
      expect(out.stdout).toBe('json');
      expect(readFileSync(path).length).toBeGreaterThan(0);
    } finally {
      rmSync(path, { force: true });
    }
  });

  it('--out - streams raw bytes to stdout', async () => {
    const out = await captureCommand(['--query', 'x', '--out', '-'], { captureQuery: async () => fakeCaptureOf(fixtures[0]) });
    expect(out.stdout).toBe('raw');
  });
});

describe('connection — env fallbacks & precedence', () => {
  it('uses CH_NATIVE_HOST/PORT when flags are absent, flags win when present', () => {
    withEnv({ CH_NATIVE_HOST: 'envhost', CH_NATIVE_PORT: '9999' }, () => {
      const fromEnv = resolveCaptureOptions(parseArgs(['--query', 'x'], { valueFlags: ['query'] }));
      expect(fromEnv).toMatchObject({ host: 'envhost', port: 9999 });

      const flagWins = resolveCaptureOptions(
        parseArgs(['--query', 'x', '--host', 'flaghost', '--port', '8888'], { valueFlags: ['query', 'host', 'port'] }),
      );
      expect(flagWins).toMatchObject({ host: 'flaghost', port: 8888 });
    });
  });

  it('resolveHttpConnection defaults to 127.0.0.1:8123 and honors CH_HTTP_PORT', () => {
    const def = resolveHttpConnection(parseArgs(['--query', 'x'], { valueFlags: ['query'] }));
    expect(def).toMatchObject({ host: '127.0.0.1', port: 8123 });
    withEnv({ CH_HTTP_PORT: '18123' }, () => {
      expect(resolveHttpConnection(parseArgs(['--query', 'x'], { valueFlags: ['query'] })).port).toBe(18123);
    });
  });

  it('an explicit --setting overrides a default experimental setting', () => {
    const opts = resolveCaptureOptions(
      parseArgs(['--query', 'x', '--setting', 'allow_experimental_json_type=0'], { valueFlags: ['query'], multiFlags: ['setting'] }),
    );
    expect(opts.settings!.allow_experimental_json_type).toBe('0');
  });
});

describe('native proxy — no hang when the client exits before connecting', () => {
  it('rejects instead of hanging when clickhouse-client exits pre-connect', async () => {
    // `false` exits non-zero immediately without ever connecting to the proxy.
    // Before the fix this hung forever (the proxy `done` never resolved); the
    // 10s timeout fails the test if that regresses.
    await expect(captureQuery({ query: 'SELECT 1', clientPath: 'false' })).rejects.toThrow(/exited/);
  }, 10000);
});

describe('end-to-end via tsx (entry, stdin, exit codes)', () => {
  const run = (args: string[], input?: Buffer) =>
    execFileSync('npx', ['tsx', CLI_ENTRY, ...args], { input, stdio: ['pipe', 'pipe', 'pipe'] }).toString();

  it('decodes a fixture file and prints parseable JSON (exit 0)', () => {
    const out = run(['decode', fixturePath(fixtures[0]), '--compact']);
    expect(JSON.parse(out).format).toBe('NativeProtocol');
  }, 30000);

  it('decodes from stdin', () => {
    const out = run(['decode', '--compact'], Buffer.from(readFixture(fixtures[0])));
    expect(JSON.parse(out).source.kind).toBe('stdin');
  }, 30000);

  it('exits non-zero with a JSON error envelope on bad input', () => {
    let code = 0;
    let stderr = '';
    try {
      execFileSync('npx', ['tsx', CLI_ENTRY, 'decode'], { input: Buffer.from('garbage'), stdio: ['pipe', 'pipe', 'pipe'] });
    } catch (err) {
      const e = err as { status: number; stderr: Buffer };
      code = e.status;
      stderr = e.stderr.toString();
    }
    expect(code).toBe(2);
    expect(JSON.parse(stderr).error.kind).toBe('usage');
  }, 30000);

  it('prints the version and exits 0', () => {
    expect(run(['--version']).trim()).toMatch(/^\d+\.\d+\.\d+/);
  }, 30000);

  it('rejects an unknown command with exit 2', () => {
    let code = 0;
    let stderr = '';
    try {
      execFileSync('npx', ['tsx', CLI_ENTRY, 'frobnicate'], { stdio: ['ignore', 'pipe', 'pipe'] });
    } catch (err) {
      const e = err as { status: number; stderr: Buffer };
      code = e.status;
      stderr = e.stderr.toString();
    }
    expect(code).toBe(2);
    expect(JSON.parse(stderr).error.message).toMatch(/unknown command/);
  }, 30000);

  it('exits cleanly when stdout is closed early (no EPIPE stack trace)', () => {
    const errFile = join(tmpdir(), 'chfx-epipe.err');
    try {
      // Large pretty output piped into a reader that closes after a few bytes.
      execSync(`npx tsx "${CLI_ENTRY}" decode "${fixturePath(fixtures[0])}" 2>"${errFile}" | head -c 5 >/dev/null`, {
        shell: '/bin/bash',
        stdio: ['ignore', 'ignore', 'ignore'],
      });
      const err = readFileSync(errFile, 'utf8');
      expect(err).not.toMatch(/EPIPE|Error:|\bat /);
    } finally {
      rmSync(errFile, { force: true });
    }
  }, 30000);
});

describe('parseHostPort', () => {
  it('parses bare port, host:port, and bare host (with default port)', () => {
    expect(parseHostPort('9000', { flag: 'listen' })).toEqual({ host: '127.0.0.1', port: 9000 });
    expect(parseHostPort('0.0.0.0:9100', { flag: 'listen' })).toEqual({ host: '0.0.0.0', port: 9100 });
    expect(parseHostPort('db.internal', { flag: 'target', defaultPort: 9000 })).toEqual({ host: 'db.internal', port: 9000 });
    expect(parseHostPort(':9000', { flag: 'listen' })).toEqual({ host: '127.0.0.1', port: 9000 });
  });

  it('requires a port when no default and rejects out-of-range ports', () => {
    expect(() => parseHostPort('myhost', { flag: 'listen' })).toThrow(/needs a port/);
    expect(() => parseHostPort('host:0', { flag: 'listen' })).toThrow(/between 1 and 65535/);
    expect(() => parseHostPort('99999', { flag: 'listen' })).toThrow(/between 1 and 65535/);
    expect(() => parseHostPort('host:nope', { flag: 'target', defaultPort: 9000 })).toThrow(/between 1 and 65535/);
  });
});

/** A proxy capture from a real fixture, stamped with proxy meta (connection id). */
function fakeProxyCapture(name: string, connection: number): Capture {
  const c = fakeCaptureOf(name);
  return { ...c, meta: { ...c.meta, source: 'proxy', target: '127.0.0.1:9000', connection } };
}

/** Injectable ProxyDeps that fire the given captures then resolve done. */
function proxyHarness(captures: Capture[]) {
  const text: string[] = [];
  const diag: string[] = [];
  const files: Record<string, Uint8Array> = {};
  const dirs: string[] = [];
  const state: { listenOpts?: Record<string, unknown>; shutdown?: () => void } = {};
  const deps: Partial<ProxyDeps> = {
    startCaptureProxy: async (opts) => {
      state.listenOpts = opts as unknown as Record<string, unknown>;
      let resolveDone!: () => void;
      const done = new Promise<void>((r) => {
        resolveDone = r;
      });
      // Fire captures on a microtask, then resolve done (simulates the server
      // closing — in `once` mode after the first, in persistent mode on Ctrl-C).
      queueMicrotask(() => {
        for (const c of captures) opts.onCapture?.(c);
        resolveDone();
      });
      return { host: opts.listenHost ?? '127.0.0.1', port: opts.listenPort || 0, done, close: () => resolveDone() };
    },
    writeText: (t) => text.push(t),
    writeDiag: (t) => diag.push(t),
    writeFile: async (f, b) => {
      files[f] = b;
    },
    ensureDir: async (dir) => {
      dirs.push(dir);
    },
    registerShutdown: (h) => {
      state.shutdown = h;
    },
  };
  return { deps, text, diag, files, dirs, state };
}

describe('proxy — single-shot (injected server)', () => {
  it('streams the raw .chproto dump to stdout when neither --out nor --decode is given', async () => {
    const cap = fakeProxyCapture(fixtures[0], 1);
    const h = proxyHarness([cap]);
    const out = await proxyCommand(['--listen', '9000', '--target', '127.0.0.1:9000'], h.deps);
    expect(out.stdout).toBe('raw');
    // The raw dump is returned as the command output (index.ts writes it).
    const bytes = (out as { bytes: Uint8Array }).bytes;
    const parsed = parseChprotoDump(bytes);
    expect(parsed.meta?.connection).toBe(1);
    expect(h.state.listenOpts).toMatchObject({ listenPort: 9000, targetHost: '127.0.0.1', targetPort: 9000, once: true });
  });

  it('--decode emits the shared decode envelope with proxy source', async () => {
    const cap = fakeProxyCapture(fixtures[0], 1);
    const h = proxyHarness([cap]);
    const out = await proxyCommand(['--listen', '9000', '--target', '9000', '--decode'], h.deps);
    expect(out.stdout).toBe('json');
    const data = (out as { data: Record<string, unknown> }).data;
    expect(data.format).toBe(ClickHouseFormat.NativeProtocol);
    expect((data.chfx as Record<string, unknown>).command).toBe('proxy');
    expect(data.source).toMatchObject({ kind: 'proxy', connection: 1 });
  });

  it('--out writes the dump file and returns a JSON summary', async () => {
    const cap = fakeProxyCapture(fixtures[0], 1);
    const h = proxyHarness([cap]);
    const out = await proxyCommand(['--listen', '9000', '--target', '9000', '-o', '/tmp/cap.chproto'], h.deps);
    expect(out.stdout).toBe('json');
    expect(h.files['/tmp/cap.chproto']).toBeInstanceOf(Uint8Array);
    const data = (out as { data: Record<string, unknown> }).data;
    expect(data.saved).toBe('/tmp/cap.chproto');
    expect(data.segments).toBe(cap.segments.length);
  });

  it('errors when no connection was captured', async () => {
    const h = proxyHarness([]); // fires nothing
    await expect(proxyCommand(['--listen', '9000', '--target', '9000'], h.deps)).rejects.toThrow(/no connection captured/);
  });
});

describe('proxy — persistent (injected server)', () => {
  it('--persistent --save-dir writes one dump per connection and a stop summary', async () => {
    const caps = [fakeProxyCapture(fixtures[0], 1), fakeProxyCapture(fixtures[0], 2)];
    const h = proxyHarness(caps);
    const out = await proxyCommand(['--listen', '9000', '--target', '9000', '--persistent', '--save-dir', '/tmp/caps'], h.deps);
    expect(out.stdout).toBe('none');
    expect(h.dirs).toContain('/tmp/caps');
    expect(Object.keys(h.files).sort()).toEqual(['/tmp/caps/conn-0001.chproto', '/tmp/caps/conn-0002.chproto']);
    expect(h.diag.some((d) => /stopped after 2 connection/.test(d))).toBe(true);
    expect(typeof h.state.shutdown).toBe('function');
  });

  it('--persistent --decode streams a JSON doc per connection to stdout', async () => {
    const caps = [fakeProxyCapture(fixtures[0], 1), fakeProxyCapture(fixtures[0], 2)];
    const h = proxyHarness(caps);
    const out = await proxyCommand(['--listen', '9000', '--target', '9000', '--persistent', '--decode', '--compact'], h.deps);
    expect(out.stdout).toBe('none');
    expect(h.text).toHaveLength(2);
    for (const doc of h.text) {
      const parsed = JSON.parse(doc) as Record<string, unknown>;
      expect((parsed.chfx as Record<string, unknown>).command).toBe('proxy');
    }
  });
});

describe('proxy — usage errors', () => {
  const cases: Array<[string, string[], RegExp]> = [
    ['missing --listen', ['--target', '9000'], /--listen is required/],
    ['missing --target', ['--listen', '9000'], /--target is required/],
    ['--persistent + --once', ['--listen', '9000', '--target', '9000', '--persistent', '--once'], /mutually exclusive/],
    ['--persistent without a sink', ['--listen', '9000', '--target', '9000', '--persistent'], /needs an output sink/],
    ['--out in persistent mode', ['--listen', '9000', '--target', '9000', '--persistent', '--save-dir', 'd', '-o', 'f'], /single-shot only/],
    ['--save-dir in once mode', ['--listen', '9000', '--target', '9000', '--save-dir', 'd'], /for --persistent mode/],
    ['unknown flag', ['--listen', '9000', '--target', '9000', '--frob'], /unknown option/],
  ];
  for (const [name, argv, re] of cases) {
    it(`rejects ${name}`, async () => {
      await expect(proxyCommand(argv, proxyHarness([]).deps)).rejects.toThrow(re);
    });
  }
});

describe('startCaptureProxy — real sockets', () => {
  it('forwards both directions and captures the stream', async () => {
    // Upstream sends a greeting on connect, then echoes whatever it receives.
    const upstream = net.createServer((sock) => {
      sock.write(Buffer.from([0xaa, 0xbb]));
      sock.on('data', (d) => sock.write(d));
      sock.on('end', () => sock.end());
    });
    await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
    const upPort = (upstream.address() as net.AddressInfo).port;

    let captured: Capture | undefined;
    const proxy = await startCaptureProxy({
      targetHost: '127.0.0.1',
      targetPort: upPort,
      once: true,
      onCapture: (c) => {
        captured = c;
      },
    });

    await new Promise<void>((resolve, reject) => {
      const client = net.connect(proxy.port, proxy.host, () => client.write(Buffer.from([0x01, 0x02, 0x03])));
      let received = 0;
      client.on('data', (d) => {
        received += d.length;
        if (received >= 5) client.end(); // greeting(2) + echo(3)
      });
      client.on('error', reject);
      client.on('close', () => resolve());
    });

    await proxy.done;
    upstream.close();

    expect(captured).toBeDefined();
    expect([...captured!.c2s]).toEqual([0x01, 0x02, 0x03]);
    expect([...captured!.s2c]).toEqual([0xaa, 0xbb, 0x01, 0x02, 0x03]);
    expect(captured!.meta).toMatchObject({ source: 'proxy', connection: 1, target: `127.0.0.1:${upPort}` });
    // A round-tripped dump preserves the captured streams.
    const round = parseChprotoDump(new Uint8Array(encodeDump(captured!)));
    expect([...round.c2s]).toEqual([0x01, 0x02, 0x03]);
  }, 15000);

  it('does not hang in `once` mode when the upstream refuses the connection', async () => {
    // Bind+close a port to get one that is (almost certainly) closed.
    const tmp = net.createServer();
    await new Promise<void>((resolve) => tmp.listen(0, '127.0.0.1', resolve));
    const deadPort = (tmp.address() as net.AddressInfo).port;
    await new Promise<void>((resolve) => tmp.close(() => resolve()));

    const errors: Error[] = [];
    const proxy = await startCaptureProxy({
      targetHost: '127.0.0.1',
      targetPort: deadPort,
      once: true,
      onError: (e) => errors.push(e),
    });

    const client = net.connect(proxy.port, proxy.host, () => client.write(Buffer.from([0x01])));
    client.on('error', () => {});

    // `done` must resolve even though the upstream connect failed.
    await proxy.done;
    proxy.close();
    expect(errors.length).toBeGreaterThan(0);
  }, 15000);
});
