import process from 'node:process';
import { readFile } from 'node:fs/promises';
import { Buffer } from 'node:buffer';

import { ClickHouseFormat } from '../../core/types/formats';
import type { ParsedData } from '../../core/types/ast';
import { createDecoder, ProtocolDecoder } from '../../core/decoder';
import { parseChprotoDump } from '../../core/decoder/protocol-dump';
import { DEFAULT_NATIVE_PROTOCOL_VERSION } from '../../core/types/native-protocol';

import { CliError } from '../output';
import { CHFX_VERSION, CLI_SCHEMA_VERSION } from '../version';
import { parseArgs, stringOption, boolOption } from '../args';

export const FORMAT_NAMES = ['chproto', 'native', 'rowbinary'] as const;
export type FormatName = (typeof FORMAT_NAMES)[number];

const CHPROTO_MAGIC = 'CHPROTO1';

interface DecodeCore {
  format: ClickHouseFormat;
  /** Version negotiated (chproto) or requested (native); null for RowBinary. */
  protocolVersion: number | null;
  /** Buffer that every AstNode.byteRange indexes into (combined stream for chproto). */
  outputBytes: Uint8Array;
  parsed: ParsedData;
  /** Present only for NativeProtocol captures. */
  protocol?: { negotiatedVersion: number | null; c2sLength: number; dumpMeta: Record<string, unknown> };
}

export interface DecodeResult extends DecodeCore {
  formatDetected: boolean;
}

function isChproto(bytes: Uint8Array): boolean {
  if (bytes.length < CHPROTO_MAGIC.length) return false;
  for (let i = 0; i < CHPROTO_MAGIC.length; i++) {
    if (bytes[i] !== CHPROTO_MAGIC.charCodeAt(i)) return false;
  }
  return true;
}

function decodeChproto(bytes: Uint8Array): DecodeCore {
  const { c2s, s2c, meta } = parseChprotoDump(bytes);
  const combined = new Uint8Array(c2s.length + s2c.length);
  combined.set(c2s, 0);
  combined.set(s2c, c2s.length);
  const parsed = new ProtocolDecoder(combined, c2s.length, meta).decode();
  const negotiated = parsed.metadata?.negotiatedVersion;
  return {
    format: ClickHouseFormat.NativeProtocol,
    protocolVersion: typeof negotiated === 'number' ? negotiated : null,
    outputBytes: combined,
    parsed,
    protocol: {
      negotiatedVersion: typeof negotiated === 'number' ? negotiated : null,
      c2sLength: c2s.length,
      dumpMeta: meta ?? {},
    },
  };
}

function decodeNative(bytes: Uint8Array, protocolVersion: number): DecodeCore {
  const parsed = createDecoder(bytes, ClickHouseFormat.Native, { nativeProtocolVersion: protocolVersion }).decode();
  return { format: ClickHouseFormat.Native, protocolVersion, outputBytes: bytes, parsed };
}

function decodeRowBinary(bytes: Uint8Array): DecodeCore {
  const parsed = createDecoder(bytes, ClickHouseFormat.RowBinaryWithNamesAndTypes).decode();
  return { format: ClickHouseFormat.RowBinaryWithNamesAndTypes, protocolVersion: null, outputBytes: bytes, parsed };
}

/**
 * Decode a raw buffer. `format` forces a decoder; when omitted, `.chproto` is
 * detected by its magic header and raw bodies are autodetected best-effort by
 * trial decode (RowBinary vs Native). Ambiguous or unrecognized input is a
 * usage error directing the caller to pass `--format`.
 */
export function decodeBuffer(
  bytes: Uint8Array,
  opts: { format?: FormatName; protocolVersion?: number } = {},
): DecodeResult {
  const version = opts.protocolVersion ?? DEFAULT_NATIVE_PROTOCOL_VERSION;

  if (opts.format) {
    const core =
      opts.format === 'chproto'
        ? decodeChproto(bytes)
        : opts.format === 'native'
          ? decodeNative(bytes, version)
          : decodeRowBinary(bytes);
    return { ...core, formatDetected: false };
  }

  if (isChproto(bytes)) {
    return { ...decodeChproto(bytes), formatDetected: true };
  }

  // Raw body: trial-decode each candidate; a wrong format almost always throws.
  const matched: DecodeCore[] = [];
  for (const run of [() => decodeRowBinary(bytes), () => decodeNative(bytes, version)]) {
    try {
      matched.push(run());
    } catch {
      // not this format
    }
  }
  if (matched.length === 1) {
    return { ...matched[0], formatDetected: true };
  }
  throw new CliError(
    'usage',
    matched.length === 0
      ? 'could not autodetect format; pass --format chproto|native|rowbinary'
      : 'format is ambiguous (raw Native and RowBinary bodies look alike); pass --format native|rowbinary',
    { matched: matched.map((m) => m.format) },
  );
}

function asByteRange(v: unknown): { start: number; end: number } | null {
  if (v && typeof v === 'object') {
    const r = v as { start?: unknown; end?: unknown };
    if (typeof r.start === 'number' && typeof r.end === 'number') return { start: r.start, end: r.end };
  }
  return null;
}

/**
 * Deep-clone the decoded tree, attaching each node's own raw bytes (hex) inline
 * as `bytes` for every object carrying a {start, end} byteRange. Lets a consumer
 * read a node's bytes directly without slicing the top-level bytesHex. Parent
 * nodes contain their children's bytes, so this trades output size for
 * convenience; disable with `--no-node-bytes`.
 */
function attachNodeBytes(value: unknown, buffer: Uint8Array): unknown {
  if (Array.isArray(value)) return value.map((v) => attachNodeBytes(v, buffer));
  if (value instanceof Uint8Array || value instanceof Map || value instanceof Set) return value;
  if (value && typeof value === 'object') {
    const src = value as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(src)) out[key] = attachNodeBytes(src[key], buffer);
    const range = asByteRange(src.byteRange);
    if (range) out.bytes = Buffer.from(buffer.subarray(range.start, range.end)).toString('hex');
    return out;
  }
  return value;
}

async function readInput(path: string | undefined): Promise<{ bytes: Uint8Array; source: Record<string, unknown> }> {
  if (path && path !== '-') {
    try {
      const buf = await readFile(path);
      return { bytes: new Uint8Array(buf), source: { kind: 'file', path, byteLength: buf.length } };
    } catch (err) {
      throw new CliError('io', `cannot read file: ${path}`, { cause: (err as Error).message });
    }
  }
  if (process.stdin.isTTY) {
    throw new CliError('usage', 'no input: pass a file path or pipe binary data to stdin');
  }
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) chunks.push(chunk as Buffer);
  const buf = Buffer.concat(chunks);
  return { bytes: new Uint8Array(buf), source: { kind: 'stdin', byteLength: buf.length } };
}

function parseProtocolVersion(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) {
    throw new CliError('usage', `--protocol-version must be a non-negative integer, got: ${raw}`);
  }
  return n;
}

export async function decodeCommand(rest: string[]): Promise<{ data: unknown; compact: boolean }> {
  const args = parseArgs(rest, {
    valueFlags: ['format', 'protocol-version'],
    aliases: { f: 'format' },
  });

  const format = stringOption(args, 'format') as FormatName | undefined;
  if (format && !FORMAT_NAMES.includes(format)) {
    throw new CliError('usage', `unknown --format '${format}'; expected one of ${FORMAT_NAMES.join(', ')}`);
  }
  const protocolVersion = parseProtocolVersion(stringOption(args, 'protocol-version'));
  const compact = boolOption(args, 'compact');
  const includeNodeBytes = !boolOption(args, 'no-node-bytes');

  const { bytes, source } = await readInput(args.positionals[0]);
  if (bytes.length === 0) {
    throw new CliError('usage', 'input is empty');
  }

  const result = decodeBuffer(bytes, { format, protocolVersion });

  const data = {
    chfx: { tool: 'chfx', version: CHFX_VERSION, schemaVersion: CLI_SCHEMA_VERSION, command: 'decode' },
    source,
    format: result.format,
    formatDetected: result.formatDetected,
    protocolVersion: result.protocolVersion,
    nodeBytes: includeNodeBytes,
    ...(result.protocol ? { protocol: result.protocol } : {}),
    conventions: {
      byteRange:
        'Each node has byteRange {start, end} into bytesHex (2 hex chars per byte; start inclusive, end exclusive).',
      bytes:
        'When nodeBytes is true, each node also carries its own raw bytes inline as "bytes" (hex); pass --no-node-bytes to omit and slice bytesHex by range instead.',
    },
    bytesHex: Buffer.from(result.outputBytes).toString('hex'),
    data: includeNodeBytes ? attachNodeBytes(result.parsed, result.outputBytes) : result.parsed,
  };

  return { data, compact };
}
