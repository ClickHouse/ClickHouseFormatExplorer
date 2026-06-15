import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Buffer } from 'node:buffer';
import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ClickHouseContainer, StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { TestContainers } from 'testcontainers';

import { proxyCommand, type ProxyDeps } from './commands/proxy';
import { decodeBuffer } from './commands/decode';
import { ClickHouseFormat } from '../core/types/formats';

/**
 * End-to-end integration tests for `chfx proxy`.
 *
 * Topology: a real ClickHouse container runs the server. The proxy runs on the
 * host (the actual proxyCommand, with the real startCaptureProxy + filesystem),
 * forwarding to the container's mapped native port. A real `clickhouse-client`
 * inside the container connects *back* to the host proxy through
 * `host.testcontainers.internal` (set up by TestContainers.exposeHostPorts).
 *
 * The in-container client is "remote", so it would compress by default — we pass
 * `--compression 0` to keep the captured stream plaintext (the proxy only
 * supports plaintext/uncompressed, the same constraint as the rest of chfx).
 */

const IMAGE = 'clickhouse/clickhouse-server:latest';

/**
 * Fixed candidate listen ports, chosen *below* the OS ephemeral range
 * (typically 32768+). Docker publishes container ports from the ephemeral
 * range, so picking from there (as OS-assigned free ports do) races with
 * testcontainers' own port mappings / forwarder — which is exactly what made
 * this suite flaky in CI. Low, explicit ports the OS won't auto-assign avoid
 * that; the retry in `startProxy` covers the rare case one is occupied anyway.
 */
const CANDIDATE_PORTS = Array.from({ length: 24 }, (_, i) => 14100 + i);

function isAddrInUse(err: unknown): boolean {
  return /EADDRINUSE/.test(err instanceof Error ? err.message : String(err));
}

async function waitFor(pred: () => boolean, label: string, timeoutMs = 20000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timeout waiting for: ${label}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

const hexOf = (s: string) => Buffer.from(s, 'utf-8').toString('hex');

describe('chfx proxy — integration (real ClickHouse + real native client)', () => {
  let container: StartedClickHouseContainer;
  let targetEndpoint: string;
  let tmp: string;
  let portCursor = 0;

  beforeAll(async () => {
    // Expose every candidate port to containers BEFORE starting ClickHouse, so
    // each can resolve host.testcontainers.internal:<port> and so a retry can
    // fall back to any of them.
    await TestContainers.exposeHostPorts(...CANDIDATE_PORTS);
    container = await new ClickHouseContainer(IMAGE).start();
    targetEndpoint = `${container.getHost()}:${container.getPort()}`;
    tmp = mkdtempSync(join(tmpdir(), 'chfx-proxy-'));
  }, 120000);

  afterAll(async () => {
    if (tmp) rmSync(tmp, { recursive: true, force: true });
    if (container) await container.stop();
  });

  /**
   * Run `clickhouse-client` inside the container, connecting back through the
   * host proxy. Returns when the client exits (so the proxied connection closes).
   */
  async function clientQuery(sql: string, port: number): Promise<void> {
    const result = await container.exec([
      'clickhouse-client',
      '--host',
      'host.testcontainers.internal',
      '--port',
      String(port),
      '--user',
      'test',
      '--password',
      'test',
      '--database',
      'test',
      '--compression',
      '0',
      '--query',
      sql,
    ]);
    if (result.exitCode !== 0) {
      throw new Error(`clickhouse-client exited ${result.exitCode}: ${result.output}`);
    }
  }

  /** Real proxy deps: real server + real fs, but capture stdout/stderr text. */
  function makeDeps() {
    const text: string[] = [];
    const diag: string[] = [];
    const shutdown: { fn?: () => void } = {};
    const deps: Partial<ProxyDeps> = {
      writeText: (t) => text.push(t),
      writeDiag: (t) => diag.push(t),
      registerShutdown: (fn) => {
        shutdown.fn = fn;
      },
    };
    return { deps, text, diag, shutdown };
  }

  /**
   * Start `proxyCommand` and wait until it's actually listening. If the chosen
   * port is taken (EADDRINUSE), fall back to the next candidate and retry — so a
   * transient port clash can't fail the test (and never hangs the full waitFor,
   * since a bind failure is detected immediately). `listenForm` controls whether
   * --listen is passed as a bare port or `host:port`.
   */
  async function startProxy(args: string[], listenForm: 'bare' | 'hostport' = 'bare') {
    let lastErr: unknown;
    for (let i = 0; i < CANDIDATE_PORTS.length; i++) {
      const port = CANDIDATE_PORTS[portCursor++ % CANDIDATE_PORTS.length];
      const harness = makeDeps();
      const listenArg = listenForm === 'hostport' ? `127.0.0.1:${port}` : String(port);
      const pending = proxyCommand(['--listen', listenArg, '--target', targetEndpoint, ...args], harness.deps);
      let earlyErr: unknown;
      // Capture an early bind rejection so it neither becomes an unhandled
      // rejection nor blocks the listening wait below.
      pending.catch((err) => {
        earlyErr = err;
      });
      await waitFor(
        () => earlyErr !== undefined || harness.diag.some((d) => /listening on/.test(d)),
        'proxy listening',
      );
      if (earlyErr === undefined) return { port, pending, ...harness };
      if (!isAddrInUse(earlyErr)) throw earlyErr;
      lastErr = earlyErr;
    }
    throw new Error(`no free candidate port for proxy (last: ${String(lastErr)})`);
  }

  /**
   * Drive a single-shot proxy run with `args`, firing the client once the proxy
   * is listening. Resolves with the command's output + captured diagnostics.
   */
  async function runOnce(args: string[], sql: string) {
    const { port, pending, text, diag } = await startProxy(args);
    await clientQuery(sql, port);
    const out = await pending;
    return { out, text, diag };
  }

  it('single-shot: streams a raw .chproto dump (default) that decodes to NativeProtocol', async () => {
    const marker = 'mark_raw_42';
    const { out } = await runOnce([], `SELECT '${marker}' AS m`);
    expect(out.stdout).toBe('raw');
    const bytes = (out as { bytes: Uint8Array }).bytes;
    expect(Buffer.from(bytes.subarray(0, 8)).toString()).toBe('CHPROTO1');
    const decoded = decodeBuffer(bytes, { format: 'chproto' });
    expect(decoded.format).toBe(ClickHouseFormat.NativeProtocol);
    // The captured stream carries both the query (c2s) and the result (s2c).
    expect(Buffer.from(decoded.outputBytes).toString('hex')).toContain(hexOf(marker));
  }, 90000);

  it('single-shot --decode: returns the proxy decode envelope', async () => {
    const marker = 'mark_decode_7';
    const { out } = await runOnce(['--decode'], `SELECT '${marker}' AS m, 1 AS n`);
    expect(out.stdout).toBe('json');
    const data = (out as { data: Record<string, unknown> }).data;
    expect(data.format).toBe(ClickHouseFormat.NativeProtocol);
    expect((data.chfx as Record<string, unknown>).command).toBe('proxy');
    expect(data.source).toMatchObject({ kind: 'proxy', target: targetEndpoint });
    expect(typeof data.protocolVersion).toBe('number');
    expect(data.nodeBytes).toBe(true);
    expect(data.bytesHex).toContain(hexOf(marker));
  }, 90000);

  it('single-shot --out: writes the dump file and returns a JSON summary', async () => {
    const file = join(tmp, 'once.chproto');
    const { out } = await runOnce(['--out', file], 'SELECT 1 AS one');
    expect(out.stdout).toBe('json');
    const data = (out as { data: Record<string, unknown> }).data;
    expect(data.saved).toBe(file);
    expect(data.bytes).toBeGreaterThan(0);
    // The written file is a real, decodable dump.
    const onDisk = new Uint8Array(readFileSync(file));
    expect(decodeBuffer(onDisk, { format: 'chproto' }).format).toBe(ClickHouseFormat.NativeProtocol);
  }, 90000);

  it('single-shot --out + --decode: writes the file *and* returns the decode envelope', async () => {
    const file = join(tmp, 'once-both.chproto');
    const { out } = await runOnce(['--out', file, '--decode'], 'SELECT 123 AS v');
    expect(out.stdout).toBe('json');
    const data = (out as { data: Record<string, unknown> }).data;
    expect(data.format).toBe(ClickHouseFormat.NativeProtocol);
    const onDisk = new Uint8Array(readFileSync(file));
    expect(decodeBuffer(onDisk, { format: 'chproto' }).format).toBe(ClickHouseFormat.NativeProtocol);
  }, 90000);

  it('single-shot --decode --no-node-bytes --compact: honours output controls', async () => {
    const { out } = await runOnce(['--decode', '--no-node-bytes', '--compact'], 'SELECT 5 AS five');
    expect(out.stdout).toBe('json');
    const json = out as { data: Record<string, unknown>; compact: boolean };
    expect(json.compact).toBe(true);
    expect(json.data.nodeBytes).toBe(false);
  }, 90000);

  it('accepts a host:port form for --listen', async () => {
    const { port, pending, diag } = await startProxy(['--decode'], 'hostport');
    await clientQuery('SELECT 1', port);
    const out = await pending;
    expect(out.stdout).toBe('json');
    expect(diag.some((d) => d.includes(`127.0.0.1:${port}`))).toBe(true);
  }, 90000);

  it('persistent --save-dir: writes one dump per connection until stopped', async () => {
    const dir = join(tmp, 'persist-dumps');
    const { port, pending, diag, shutdown } = await startProxy(['--persistent', '--save-dir', dir]);

    await clientQuery('SELECT 1 AS a', port);
    await clientQuery('SELECT 2 AS b', port);
    await waitFor(() => readdirSync(dir).filter((f) => f.endsWith('.chproto')).length >= 2, 'two dumps written');

    shutdown.fn!(); // simulate Ctrl-C
    const out = await pending;

    expect(out.stdout).toBe('none');
    const files = readdirSync(dir).filter((f) => f.endsWith('.chproto')).sort();
    expect(files).toEqual(['conn-0001.chproto', 'conn-0002.chproto']);
    for (const f of files) {
      const bytes = new Uint8Array(readFileSync(join(dir, f)));
      expect(decodeBuffer(bytes, { format: 'chproto' }).format).toBe(ClickHouseFormat.NativeProtocol);
    }
    expect(diag.some((d) => /stopped after 2 connection/.test(d))).toBe(true);
  }, 120000);

  it('persistent --decode: streams one JSON envelope per connection', async () => {
    const { port, pending, text, shutdown } = await startProxy(['--persistent', '--decode', '--compact']);

    await clientQuery('SELECT 10 AS x', port);
    await clientQuery('SELECT 20 AS y', port);
    await waitFor(() => text.length >= 2, 'two decoded docs');

    shutdown.fn!();
    const out = await pending;

    expect(out.stdout).toBe('none');
    expect(text).toHaveLength(2);
    for (const doc of text) {
      const parsed = JSON.parse(doc) as Record<string, unknown>;
      expect((parsed.chfx as Record<string, unknown>).command).toBe('proxy');
      expect(parsed.format).toBe(ClickHouseFormat.NativeProtocol);
    }
  }, 120000);
});
