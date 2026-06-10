// @ts-check
/**
 * HTTP middleware that runs a native-protocol capture and returns the `.chproto`
 * dump. Used by the Vite dev/preview server so the **web** UI (which can't open
 * a raw TCP socket or spawn a process) can still capture packet streams: the
 * browser POSTs the SQL to `/capture`, this handler drives clickhouse-client
 * through the proxy, and responds with the dump bytes for the client to decode.
 *
 * Connection defaults come from env vars so it works without config:
 *   CH_NATIVE_HOST (default localhost), CH_NATIVE_PORT (9000),
 *   CH_USER (default), CH_PASSWORD (empty), CLICKHOUSE_CLIENT (clickhouse-client)
 */

import { captureQuery, encodeDump } from './native-proxy.mjs';

/**
 * Experimental type settings so Variant/Dynamic/JSON/QBit queries work. Sent as
 * per-query client settings. Disabled when CAPTURE_EXPERIMENTAL_SETTINGS=0 — a
 * readonly ClickHouse user rejects per-query setting changes, so in that case
 * the settings must come from the user's profile instead (see docker/users.xml).
 */
const EXPERIMENTAL_SETTINGS = {
  allow_experimental_variant_type: '1',
  allow_experimental_dynamic_type: '1',
  allow_experimental_json_type: '1',
  allow_suspicious_variant_types: '1',
  allow_experimental_qbit_type: '1',
  allow_suspicious_low_cardinality_types: '1',
};

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

/**
 * Build a connect-style request handler for native-protocol captures.
 * @param {{ host?: string, port?: number, user?: string, password?: string, clientPath?: string }} [opts]
 */
export function createCaptureHandler(opts = {}) {
  const host = opts.host ?? process.env.CH_NATIVE_HOST ?? 'localhost';
  const port = Number(opts.port ?? process.env.CH_NATIVE_PORT ?? 9000);
  const user = opts.user ?? process.env.CH_USER ?? 'default';
  const password = opts.password ?? process.env.CH_PASSWORD ?? '';
  const clientPath = opts.clientPath ?? process.env.CLICKHOUSE_CLIENT ?? 'clickhouse-client';
  const injectExperimental = (process.env.CAPTURE_EXPERIMENTAL_SETTINGS ?? '1') !== '0';
  const settings = opts.settings ?? (injectExperimental ? EXPERIMENTAL_SETTINGS : {});

  return async (req, res) => {
    if (req.method !== 'POST') {
      res.statusCode = 405;
      res.end('Use POST with the SQL query as the request body');
      return;
    }
    try {
      const query = (await readBody(req)).trim();
      if (!query) {
        res.statusCode = 400;
        res.end('Empty query');
        return;
      }
      const capture = await captureQuery({
        query,
        host,
        port,
        user,
        password,
        clientPath,
        settings,
      });
      const dump = encodeDump(capture);
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('Content-Length', String(dump.length));
      res.end(dump);
    } catch (err) {
      res.statusCode = 500;
      res.setHeader('Content-Type', 'text/plain');
      res.end(String(err && err.message ? err.message : err));
    }
  };
}

/** Vite plugin: serve the capture handler at `/capture` in dev and preview. */
export function captureServerPlugin() {
  const handler = createCaptureHandler();
  return {
    name: 'native-protocol-capture',
    configureServer(server) {
      server.middlewares.use('/capture', handler);
    },
    configurePreviewServer(server) {
      server.middlewares.use('/capture', handler);
    },
  };
}
