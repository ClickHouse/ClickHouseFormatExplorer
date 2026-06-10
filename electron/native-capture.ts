import net from 'node:net';
import { spawn } from 'node:child_process';
import { Buffer } from 'node:buffer';

/**
 * TCP proxy + capture for the ClickHouse native protocol, used by the desktop
 * app. It opens a one-shot proxy on localhost, drives `clickhouse-client`
 * through it for a single query, and returns the two per-direction byte
 * streams. Because both ends are localhost, clickhouse-client disables
 * compression, so the capture is plaintext, uncompressed native-protocol
 * packets — what ProtocolDecoder consumes. TLS/compression are out of scope.
 *
 * This mirrors scripts/native-proxy.mjs (the CLI / test-fixture harness); the
 * two are kept intentionally small so they stay in sync.
 */

interface Segment {
  dir: 0 | 1; // 0 = client->server, 1 = server->client
  data: Buffer;
}

const DIR_C2S = 0;
const DIR_S2C = 1;

export interface CaptureOptions {
  query: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  clientPath?: string;
  settings?: Record<string, string>;
}

export interface CaptureResult {
  c2s: Buffer;
  s2c: Buffer;
  meta: Record<string, unknown>;
}

function startProxy(targetHost: string, targetPort: number): Promise<{
  port: number;
  done: Promise<Segment[]>;
  close: () => void;
}> {
  const segments: Segment[] = [];
  return new Promise((resolve, reject) => {
    let resolveDone!: (value: Segment[]) => void;
    let rejectDone!: (reason: Error) => void;
    const done = new Promise<Segment[]>((res, rej) => {
      resolveDone = res;
      rejectDone = rej;
    });
    let handled = false;

    const server = net.createServer((client) => {
      if (handled) {
        client.destroy();
        return;
      }
      handled = true;
      const upstream = net.connect(targetPort, targetHost);
      let openEnds = 2;
      const closeOne = () => {
        openEnds -= 1;
        if (openEnds === 0) {
          server.close();
          resolveDone(segments);
        }
      };
      client.on('data', (chunk) => {
        segments.push({ dir: DIR_C2S, data: Buffer.from(chunk) });
        upstream.write(chunk);
      });
      upstream.on('data', (chunk) => {
        segments.push({ dir: DIR_S2C, data: Buffer.from(chunk) });
        client.write(chunk);
      });
      client.on('end', () => upstream.end());
      upstream.on('end', () => client.end());
      client.on('close', closeOne);
      upstream.on('close', closeOne);
      const fail = (err: Error) => {
        client.destroy();
        upstream.destroy();
        try { server.close(); } catch { /* ignore */ }
        rejectDone(err);
      };
      client.on('error', fail);
      upstream.on('error', fail);
    });

    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      if (addr === null || typeof addr === 'string') {
        reject(new Error('proxy failed to bind an ephemeral port'));
        return;
      }
      resolve({ port: addr.port, done, close: () => server.close() });
    });
  });
}

function concat(segments: Segment[], dir: 0 | 1): Buffer {
  return Buffer.concat(segments.filter((s) => s.dir === dir).map((s) => s.data));
}

export async function captureNativeQuery(opts: CaptureOptions): Promise<CaptureResult> {
  const host = opts.host ?? '127.0.0.1';
  const port = opts.port ?? 9000;
  const { port: proxyPort, done, close } = await startProxy(host, port);

  const args = ['--host', '127.0.0.1', '--port', String(proxyPort), '--query', opts.query];
  if (opts.user) args.push('--user', opts.user);
  if (opts.password) args.push('--password', opts.password);
  if (opts.database) args.push('--database', opts.database);
  for (const [k, v] of Object.entries(opts.settings ?? {})) args.push(`--${k}=${v}`);

  const child = spawn(opts.clientPath ?? 'clickhouse-client', args, {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (d: Buffer) => { stderr += d.toString(); });
  child.stdout.on('data', () => { /* drain */ });

  const exit = new Promise<number | null>((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code) => resolve(code));
  });

  let segments: Segment[];
  try {
    const code = await exit;
    segments = await done;
    if (code !== 0 && segments.every((s) => s.dir === DIR_C2S)) {
      close();
      throw new Error(`clickhouse-client exited ${code}: ${stderr.trim()}`);
    }
  } catch (err) {
    close();
    throw err;
  }

  return {
    c2s: concat(segments, DIR_C2S),
    s2c: concat(segments, DIR_S2C),
    meta: { query: opts.query, host, port, stderr: stderr.trim() },
  };
}
