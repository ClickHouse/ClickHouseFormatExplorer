import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

import { decodeBuffer, decodeCommand } from './commands/decode';
import { parseArgs, stringOption, boolOption } from './args';
import { stringify, CliError } from './output';
import { ClickHouseFormat } from '../core/types/formats';

const FIXTURE_DIR = fileURLToPath(new URL('../core/decoder/fixtures/protocol/', import.meta.url));
const CLI_ENTRY = fileURLToPath(new URL('./index.ts', import.meta.url));

const fixtures = readdirSync(FIXTURE_DIR).filter((f) => f.endsWith('.chproto'));
const fixturePath = (name: string) => `${FIXTURE_DIR}${name}`;
const readFixture = (name: string) => new Uint8Array(readFileSync(fixturePath(name)));

const enc = (s: string) => [...s].map((c) => c.charCodeAt(0));
// Minimal valid bodies for a single column `x UInt8` with one row valued 1.
const ROWBINARY_BODY = new Uint8Array([0x01, 0x01, ...enc('x'), 0x05, ...enc('UInt8'), 0x01]);
const NATIVE_BODY = new Uint8Array([0x01, 0x01, 0x01, ...enc('x'), 0x05, ...enc('UInt8'), 0x01]);

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
});
