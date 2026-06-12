// @ts-check
/**
 * TCP proxy + dump harness for the ClickHouse native protocol.
 *
 * Sits between a native client (clickhouse-client) and a ClickHouse server,
 * forwarding bytes in both directions and teeing every byte into a capture.
 * Because the proxy listens on localhost and clickhouse-client disables
 * compression for localhost connections, the captured stream is plaintext,
 * uncompressed native-protocol packets — exactly what protocol-decoder.ts
 * consumes. TLS and compression are intentionally out of scope.
 *
 * The capture is stored as two concatenated per-direction byte streams
 * (client->server and server->client). Each direction is an independent,
 * ordered native-protocol stream: a packet may be split across TCP segments,
 * so the decoder must treat each direction as one contiguous buffer. The raw
 * segment log (with direction + order) is kept too, for a faithful timeline.
 *
 * Dump file layout (.chproto):
 *   magic     "CHPROTO1"                (8 bytes)
 *   metaLen   u32 LE                     length of the metadata JSON
 *   meta      metaLen bytes              UTF-8 JSON {query, host, port, ...}
 *   segments  repeated until EOF:
 *               dir   u8                 0 = client->server, 1 = server->client
 *               len   u32 LE             segment byte length
 *               data  len bytes          raw segment bytes
 */

import net from 'node:net';
import { spawn } from 'node:child_process';
import { Buffer } from 'node:buffer';

export const MAGIC = 'CHPROTO1';
export const DIR_C2S = 0;
export const DIR_S2C = 1;

/**
 * @typedef {{ dir: 0 | 1, data: Buffer }} Segment
 * @typedef {{ c2s: Buffer, s2c: Buffer, segments: Segment[], meta: Record<string, unknown> }} Capture
 */

/**
 * Start a one-shot capturing TCP proxy. It accepts a single client connection,
 * forwards it to (targetHost, targetPort), records every byte, and resolves
 * once both ends have closed.
 *
 * @param {object} opts
 * @param {string} opts.targetHost
 * @param {number} opts.targetPort
 * @param {string} [opts.listenHost]
 * @returns {Promise<{ port: number, done: Promise<Segment[]>, close: () => void }>}
 */
export function startProxy({ targetHost, targetPort, listenHost = '127.0.0.1' }) {
  /** @type {Segment[]} */
  const segments = [];

  return new Promise((resolve, reject) => {
    /** @type {(value: Segment[]) => void} */
    let resolveDone;
    /** @type {(reason: Error) => void} */
    let rejectDone;
    const done = new Promise((res, rej) => {
      resolveDone = res;
      rejectDone = rej;
    });

    let handled = false;
    // Settle `done` exactly once. Crucially, `close()` resolves it too, so a
    // caller can always unblock `await done` even if no client ever connected
    // (e.g. the client rejected a bad flag and exited before connecting).
    let settled = false;
    const finishOk = () => {
      if (settled) return;
      settled = true;
      try { server.close(); } catch { /* ignore */ }
      resolveDone(segments);
    };
    const finishErr = (/** @type {Error} */ err) => {
      if (settled) return;
      settled = true;
      try { server.close(); } catch { /* ignore */ }
      rejectDone(err);
    };

    const server = net.createServer((client) => {
      if (handled) {
        // Only the first connection is captured; ignore stragglers.
        client.destroy();
        return;
      }
      handled = true;

      const upstream = net.connect(targetPort, targetHost);
      let openEnds = 2;
      const closeOne = () => {
        openEnds -= 1;
        if (openEnds === 0) finishOk();
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

      const fail = (/** @type {Error} */ err) => {
        client.destroy();
        upstream.destroy();
        finishErr(err);
      };
      client.on('error', fail);
      upstream.on('error', fail);
    });

    server.on('error', reject);
    server.listen(0, listenHost, () => {
      const addr = server.address();
      if (addr === null || typeof addr === 'string') {
        reject(new Error('proxy failed to bind an ephemeral port'));
        return;
      }
      // `close()` resolves `done` with whatever was captured so far.
      resolve({ port: addr.port, done, close: finishOk });
    });
  });
}

/**
 * Split an ordered segment log into the two concatenated per-direction streams.
 * @param {Segment[]} segments
 */
export function splitStreams(segments) {
  const c2s = Buffer.concat(segments.filter((s) => s.dir === DIR_C2S).map((s) => s.data));
  const s2c = Buffer.concat(segments.filter((s) => s.dir === DIR_S2C).map((s) => s.data));
  return { c2s, s2c };
}

/**
 * Capture a single query by driving clickhouse-client through the proxy.
 *
 * @param {object} opts
 * @param {string} opts.query
 * @param {string} [opts.host]            ClickHouse server host (default 127.0.0.1)
 * @param {number} [opts.port]            ClickHouse native port (default 9000)
 * @param {string} [opts.user]
 * @param {string} [opts.password]
 * @param {string} [opts.database]
 * @param {string} [opts.clientPath]      path to clickhouse-client (default "clickhouse-client")
 * @param {string[]} [opts.clientArgs]    extra args appended to clickhouse-client
 * @param {Record<string,string>} [opts.settings]  per-query settings (--name=value)
 * @returns {Promise<Capture>}
 */
export async function captureQuery({
  query,
  host = '127.0.0.1',
  port = 9000,
  user,
  password,
  database,
  clientPath = 'clickhouse-client',
  clientArgs = [],
  settings = {},
}) {
  const { port: proxyPort, done, close } = await startProxy({ targetHost: host, targetPort: port });

  const args = ['--host', '127.0.0.1', '--port', String(proxyPort), '--query', query];
  if (user) args.push('--user', user);
  if (password) args.push('--password', password);
  if (database) args.push('--database', database);
  for (const [k, v] of Object.entries(settings)) args.push(`--${k}=${v}`);
  args.push(...clientArgs);

  const child = spawn(clientPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', (d) => { stderr += d.toString(); });
  // Drain stdout so the client isn't blocked on a full pipe.
  child.stdout.on('data', () => {});

  const exit = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code) => resolve(code));
  });

  let segments;
  try {
    const code = await exit;
    if (code !== 0) {
      // The client exited non-zero. If it failed before completing a proxied
      // connection (e.g. it rejected a bad --setting flag at startup), `done`
      // never resolves on its own. Give any in-flight close events a brief
      // window, then force the proxy closed so we don't hang forever.
      await Promise.race([done, new Promise((r) => setTimeout(r, 100))]);
      close();
    }
    segments = await done;
    if (code !== 0 && segments.every((s) => s.dir === DIR_C2S)) {
      // Client failed before the server answered anything useful.
      throw new Error(`clickhouse-client exited ${code}: ${stderr.trim()}`);
    }
  } catch (err) {
    close();
    throw err;
  }

  const { c2s, s2c } = splitStreams(segments);
  return {
    c2s,
    s2c,
    segments,
    meta: { query, host, port, user, database, settings, stderr: stderr.trim() },
  };
}

/**
 * Serialize a capture to the .chproto dump format.
 * @param {Capture} capture
 * @returns {Buffer}
 */
export function encodeDump(capture) {
  const metaJson = Buffer.from(JSON.stringify(capture.meta), 'utf-8');
  const head = Buffer.alloc(MAGIC.length + 4);
  head.write(MAGIC, 0, 'ascii');
  head.writeUInt32LE(metaJson.length, MAGIC.length);

  const parts = [head, metaJson];
  for (const seg of capture.segments) {
    const segHead = Buffer.alloc(5);
    segHead.writeUInt8(seg.dir, 0);
    segHead.writeUInt32LE(seg.data.length, 1);
    parts.push(segHead, seg.data);
  }
  return Buffer.concat(parts);
}

/**
 * Parse a .chproto dump back into a capture.
 * @param {Buffer} buf
 * @returns {Capture}
 */
export function decodeDump(buf) {
  if (buf.subarray(0, MAGIC.length).toString('ascii') !== MAGIC) {
    throw new Error('not a CHPROTO dump (bad magic)');
  }
  let pos = MAGIC.length;
  const metaLen = buf.readUInt32LE(pos);
  pos += 4;
  const meta = JSON.parse(buf.subarray(pos, pos + metaLen).toString('utf-8'));
  pos += metaLen;

  /** @type {Segment[]} */
  const segments = [];
  while (pos < buf.length) {
    const dir = /** @type {0 | 1} */ (buf.readUInt8(pos));
    pos += 1;
    const len = buf.readUInt32LE(pos);
    pos += 4;
    segments.push({ dir, data: Buffer.from(buf.subarray(pos, pos + len)) });
    pos += len;
  }
  const { c2s, s2c } = splitStreams(segments);
  return { c2s, s2c, segments, meta };
}
