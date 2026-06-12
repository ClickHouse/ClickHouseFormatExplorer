import process from 'node:process';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

import {
  startCaptureProxy as defaultStartCaptureProxy,
  encodeDump,
  type Capture,
  type StartCaptureProxyOptions,
} from '../../../scripts/native-proxy.mjs';

import { parseArgs, stringOption, boolOption, rejectUnknownArgs } from '../args';
import { CliError, stringify, writeStderr, type CommandOutput } from '../output';
import { CHFX_VERSION, CLI_SCHEMA_VERSION } from '../version';
import { parseHostPort } from '../connection';
import { decodeCaptureStreams, buildDecodeEnvelope, type DecodeResult } from './decode';

export interface ProxyServerHandle {
  host: string;
  port: number;
  done: Promise<void>;
  close: () => void;
}

export interface ProxyDeps {
  startCaptureProxy: (opts: StartCaptureProxyOptions) => Promise<ProxyServerHandle>;
  /** Write one decoded JSON document to stdout (persistent --decode mode). */
  writeText: (text: string) => void;
  /** Write a diagnostic line to stderr (keeps stdout clean for dumps/JSON). */
  writeDiag: (text: string) => void;
  writeFile: (file: string, bytes: Uint8Array) => Promise<void>;
  ensureDir: (dir: string) => Promise<void>;
  /** Register a shutdown handler (SIGINT/SIGTERM) for persistent mode. */
  registerShutdown: (handler: () => void) => void;
}

const DEFAULT_DEPS: ProxyDeps = {
  startCaptureProxy: defaultStartCaptureProxy,
  writeText: (text) => process.stdout.write(text.endsWith('\n') ? text : `${text}\n`),
  writeDiag: writeStderr,
  writeFile: async (file, bytes) => {
    await writeFile(file, bytes);
  },
  ensureDir: async (dir) => {
    await mkdir(dir, { recursive: true });
  },
  registerShutdown: (handler) => {
    process.once('SIGINT', handler);
    process.once('SIGTERM', handler);
  },
};

interface ProxyConfig {
  listen: { host: string; port: number };
  target: { host: string; port: number };
  decode: boolean;
  includeNodeBytes: boolean;
  compact: boolean;
  out?: string;
  saveDir?: string;
}

const ALLOWED = ['listen', 'target', 'out', 'save-dir', 'decode', 'persistent', 'once', 'compact', 'no-node-bytes'];

/**
 * Run a standalone capturing proxy. It listens on `--listen`, forwards every
 * connection to `--target`, and records the native packet stream — any native
 * client (clickhouse-client, Go/JDBC/Python drivers, …) connects through it; the
 * proxy never spawns a client itself.
 *
 * - Default **single-shot**: capture the first connection, then write a
 *   `.chproto` (`--out`), or decode it to JSON (`--decode`), or stream the raw
 *   dump to stdout, and exit.
 * - **`--persistent`**: keep serving; write one `.chproto` per connection to
 *   `--save-dir` and/or stream decoded JSON per connection (`--decode`). Stops
 *   on Ctrl-C.
 *
 * Plaintext/uncompressed streams only (same constraint as `capture`/`query`);
 * TLS and compressed connections are unsupported.
 */
export async function proxyCommand(rest: string[], deps: Partial<ProxyDeps> = {}): Promise<CommandOutput> {
  const d: ProxyDeps = { ...DEFAULT_DEPS, ...deps };
  const args = parseArgs(rest, {
    valueFlags: ['listen', 'target', 'out', 'save-dir'],
    aliases: { o: 'out' },
  });
  rejectUnknownArgs(args, ALLOWED);

  const listenRaw = stringOption(args, 'listen');
  if (!listenRaw) throw new CliError('usage', '--listen is required, e.g. --listen 9000 or --listen 0.0.0.0:9000');
  const targetRaw = stringOption(args, 'target');
  if (!targetRaw) throw new CliError('usage', '--target is required, e.g. --target 127.0.0.1:9000');

  const persistent = boolOption(args, 'persistent');
  const once = boolOption(args, 'once');
  if (persistent && once) throw new CliError('usage', '--persistent and --once are mutually exclusive');

  const out = stringOption(args, 'out');
  const saveDir = stringOption(args, 'save-dir');
  const config: ProxyConfig = {
    listen: parseHostPort(listenRaw, { flag: 'listen' }),
    target: parseHostPort(targetRaw, { flag: 'target', defaultPort: 9000 }),
    decode: boolOption(args, 'decode'),
    includeNodeBytes: !boolOption(args, 'no-node-bytes'),
    compact: boolOption(args, 'compact'),
    out,
    saveDir,
  };

  if (persistent) {
    if (out) throw new CliError('usage', '--out is single-shot only; use --save-dir in --persistent mode');
    if (!saveDir && !config.decode) {
      throw new CliError('usage', '--persistent needs an output sink: pass --save-dir <dir> and/or --decode');
    }
    return runPersistent(d, config);
  }

  if (saveDir) throw new CliError('usage', '--save-dir is for --persistent mode; use --out for single-shot');
  return runOnce(d, config);
}

/** Decode a captured connection into the shared decode envelope. */
function captureToEnvelope(capture: Capture, config: ProxyConfig): Record<string, unknown> {
  const result: DecodeResult = {
    ...decodeCaptureStreams(capture.c2s, capture.s2c, capture.meta),
    formatDetected: true,
  };
  const source: Record<string, unknown> = {
    kind: 'proxy',
    target: `${config.target.host}:${config.target.port}`,
    connection: capture.meta.connection,
  };
  return buildDecodeEnvelope(result, source, { command: 'proxy', includeNodeBytes: config.includeNodeBytes });
}

async function startServer(
  d: ProxyDeps,
  config: ProxyConfig,
  handlers: Pick<StartCaptureProxyOptions, 'once' | 'onCapture' | 'onError'>,
): Promise<ProxyServerHandle> {
  try {
    return await d.startCaptureProxy({
      targetHost: config.target.host,
      targetPort: config.target.port,
      listenHost: config.listen.host,
      listenPort: config.listen.port,
      ...handlers,
    });
  } catch (err) {
    throw new CliError('io', `cannot listen on ${config.listen.host}:${config.listen.port}: ${(err as Error).message}`);
  }
}

async function runOnce(d: ProxyDeps, config: ProxyConfig): Promise<CommandOutput> {
  let captured: Capture | undefined;
  const errors: Error[] = [];

  const server = await startServer(d, config, {
    once: true,
    onCapture: (c) => {
      captured = c;
    },
    onError: (e) => errors.push(e),
  });

  d.writeDiag(
    `chfx proxy: listening on ${server.host}:${server.port} → ${config.target.host}:${config.target.port} ` +
      `(single-shot). Point your ClickHouse client at it.`,
  );
  await server.done;

  if (!captured) {
    throw new CliError('io', `no connection captured${errors.length ? `: ${errors[0].message}` : ''}`);
  }

  const dump = encodeDump(captured);
  if (config.out) {
    try {
      await d.writeFile(config.out, new Uint8Array(dump));
    } catch (err) {
      throw new CliError('io', `cannot write --out file: ${config.out}`, { cause: (err as Error).message });
    }
    d.writeDiag(`chfx proxy: captured connection → ${config.out} (${dump.length} bytes)`);
  }

  if (config.decode) {
    return { stdout: 'json', data: captureToEnvelope(captured, config), compact: config.compact };
  }
  if (config.out) {
    const data = {
      chfx: { tool: 'chfx', version: CHFX_VERSION, schemaVersion: CLI_SCHEMA_VERSION, command: 'proxy' },
      target: `${config.target.host}:${config.target.port}`,
      saved: config.out,
      bytes: dump.length,
      c2sBytes: captured.c2s.length,
      s2cBytes: captured.s2c.length,
      segments: captured.segments.length,
    };
    return { stdout: 'json', data, compact: config.compact };
  }
  // No --out and no --decode: stream the raw dump so `chfx proxy … | chfx decode` works.
  return { stdout: 'raw', bytes: new Uint8Array(dump) };
}

async function runPersistent(d: ProxyDeps, config: ProxyConfig): Promise<CommandOutput> {
  if (config.saveDir) await d.ensureDir(config.saveDir);

  let count = 0;
  const pending: Promise<void>[] = [];

  const handle = await startServer(d, config, {
    once: false,
    onCapture: (capture) => {
      count += 1;
      pending.push(handleConnection(d, capture, config));
    },
    onError: (e) => d.writeDiag(`chfx proxy: connection error: ${e.message}`),
  });

  d.writeDiag(
    `chfx proxy: listening on ${handle.host}:${handle.port} → ${config.target.host}:${config.target.port} ` +
      `(persistent). Ctrl-C to stop.`,
  );
  d.registerShutdown(() => handle.close());

  await handle.done;
  await Promise.allSettled(pending);
  d.writeDiag(`chfx proxy: stopped after ${count} connection(s).`);
  return { stdout: 'none' };
}

/** Persist and/or decode one captured connection (used in persistent mode). */
async function handleConnection(d: ProxyDeps, capture: Capture, config: ProxyConfig): Promise<void> {
  const id = typeof capture.meta.connection === 'number' ? capture.meta.connection : 0;
  const dump = encodeDump(capture);

  if (config.saveDir) {
    const file = path.join(config.saveDir, `conn-${String(id).padStart(4, '0')}.chproto`);
    try {
      await d.writeFile(file, new Uint8Array(dump));
      d.writeDiag(`chfx proxy: connection ${id} → ${file} (${dump.length} bytes)`);
    } catch (err) {
      d.writeDiag(`chfx proxy: connection ${id}: cannot write ${file}: ${(err as Error).message}`);
    }
  } else {
    d.writeDiag(`chfx proxy: connection ${id} captured (${dump.length} bytes)`);
  }

  if (config.decode) {
    try {
      d.writeText(stringify(captureToEnvelope(capture, config), config.compact));
    } catch (err) {
      d.writeDiag(`chfx proxy: connection ${id}: decode failed: ${(err as Error).message}`);
    }
  }
}
