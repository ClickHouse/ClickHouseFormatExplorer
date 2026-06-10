#!/usr/bin/env node
// @ts-check
/**
 * Standalone HTTP server exposing the native-protocol capture endpoint. Used in
 * production (Docker) where there is no Vite dev server: nginx proxies
 * `/capture` to this process, which drives clickhouse-client through the proxy
 * and returns the `.chproto` dump for the browser to decode.
 *
 * Configuration (env):
 *   CAPTURE_PORT (default 8124), CAPTURE_BIND (default 127.0.0.1)
 *   CH_NATIVE_HOST, CH_NATIVE_PORT, CH_USER, CH_PASSWORD, CLICKHOUSE_CLIENT
 *   CAPTURE_EXPERIMENTAL_SETTINGS (default 1; set 0 for readonly users)
 */

import http from 'node:http';
import { createCaptureHandler } from './capture-middleware.mjs';

const port = Number(process.env.CAPTURE_PORT ?? 8124);
const bind = process.env.CAPTURE_BIND ?? '127.0.0.1';
const handler = createCaptureHandler();

const server = http.createServer((req, res) => {
  const url = (req.url ?? '').split('?')[0];
  if (url === '/health') {
    res.statusCode = 200;
    res.end('ok');
    return;
  }
  if (url !== '/capture' && url !== '/') {
    res.statusCode = 404;
    res.end('not found');
    return;
  }
  handler(req, res);
});

server.listen(port, bind, () => {
  console.error(`native-protocol capture server listening on http://${bind}:${port}`);
});
