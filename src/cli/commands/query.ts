import { writeFile } from 'node:fs/promises';

import {
  captureQuery as defaultCaptureQuery,
  encodeDump,
  type Capture,
  type CaptureQueryOptions,
} from '../../../scripts/native-proxy.mjs';

import { ClickHouseFormat } from '../../core/types/formats';
import { appendClickHouseRequestParams } from '../../core/clickhouse/request-params';
import { parseArgs, stringOption, boolOption } from '../args';
import { CliError, type JsonOutput } from '../output';
import { resolveCaptureOptions, resolveHttpConnection, CONNECTION_VALUE_FLAGS, CONNECTION_MULTI_FLAGS } from '../connection';
import { decodeBuffer, decodeCaptureStreams, buildDecodeEnvelope, type DecodeResult, type FormatName } from './decode';

export interface QueryDeps {
  captureQuery: (opts: CaptureQueryOptions) => Promise<Capture>;
  fetch: typeof fetch;
}

const DEFAULT_DEPS: QueryDeps = { captureQuery: defaultCaptureQuery, fetch: globalThis.fetch };

/** HTTP `--format` accepts the short or full name; maps to the wire format + decoder. */
function resolveHttpFormat(raw: string | undefined): { wire: ClickHouseFormat; cli: FormatName } {
  switch ((raw ?? 'native').toLowerCase()) {
    case 'native':
      return { wire: ClickHouseFormat.Native, cli: 'native' };
    case 'rowbinary':
    case 'rowbinarywithnamesandtypes':
      return { wire: ClickHouseFormat.RowBinaryWithNamesAndTypes, cli: 'rowbinary' };
    default:
      throw new CliError('usage', `unknown --format '${raw}'; expected native or RowBinaryWithNamesAndTypes`);
  }
}

function parseProtocolVersion(raw: string | undefined): number {
  if (raw === undefined) return 0;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) {
    throw new CliError('usage', `--protocol-version must be a non-negative integer, got: ${raw}`);
  }
  return n;
}

/**
 * Run a query and decode the result in one step.
 * - `--protocol tcp` (default): drive clickhouse-client through the capturing
 *   proxy and decode the full native packet stream. `--save <file>` keeps the dump.
 * - `--protocol http`: POST to ClickHouse HTTP, requesting `--format`
 *   (native | RowBinaryWithNamesAndTypes), and decode the response body.
 */
export async function queryCommand(rest: string[], deps: Partial<QueryDeps> = {}): Promise<JsonOutput> {
  const merged: QueryDeps = { ...DEFAULT_DEPS, ...deps };
  const args = parseArgs(rest, {
    valueFlags: [...CONNECTION_VALUE_FLAGS, 'save', 'protocol', 'format', 'protocol-version'],
    multiFlags: CONNECTION_MULTI_FLAGS,
  });
  const compact = boolOption(args, 'compact');
  const includeNodeBytes = !boolOption(args, 'no-node-bytes');

  const protocol = stringOption(args, 'protocol') ?? 'tcp';
  if (protocol !== 'tcp' && protocol !== 'http') {
    throw new CliError('usage', `unknown --protocol '${protocol}'; expected tcp or http`);
  }

  const data =
    protocol === 'http'
      ? await runHttp(args, merged, includeNodeBytes)
      : await runTcp(args, merged, includeNodeBytes);

  return { stdout: 'json', data, compact };
}

async function runTcp(
  args: ReturnType<typeof parseArgs>,
  deps: QueryDeps,
  includeNodeBytes: boolean,
): Promise<Record<string, unknown>> {
  if (stringOption(args, 'format') !== undefined) {
    throw new CliError('usage', '--format only applies to --protocol http');
  }
  const save = stringOption(args, 'save');
  const captureOpts = resolveCaptureOptions(args);

  let capture: Capture;
  try {
    capture = await deps.captureQuery(captureOpts);
  } catch (err) {
    throw new CliError('io', `capture failed: ${(err as Error).message}`);
  }
  if (save) {
    await writeFile(save, encodeDump(capture));
  }

  const result: DecodeResult = { ...decodeCaptureStreams(capture.c2s, capture.s2c, capture.meta), formatDetected: true };
  const source: Record<string, unknown> = {
    kind: 'query',
    protocol: 'tcp',
    query: captureOpts.query,
    host: captureOpts.host ?? '127.0.0.1',
    port: captureOpts.port ?? 9000,
    ...(save ? { saved: save } : {}),
  };
  return buildDecodeEnvelope(result, source, { command: 'query', includeNodeBytes });
}

async function runHttp(
  args: ReturnType<typeof parseArgs>,
  deps: QueryDeps,
  includeNodeBytes: boolean,
): Promise<Record<string, unknown>> {
  if (stringOption(args, 'save') !== undefined) {
    throw new CliError('usage', '--save only applies to --protocol tcp (HTTP returns a body, not a capture)');
  }
  const format = resolveHttpFormat(stringOption(args, 'format'));
  const protocolVersion = parseProtocolVersion(stringOption(args, 'protocol-version'));
  const conn = resolveHttpConnection(args);

  const params = new URLSearchParams();
  appendClickHouseRequestParams(params, format.wire, protocolVersion);
  for (const [key, value] of Object.entries(conn.settings)) params.set(key, value);
  if (conn.database) params.set('database', conn.database);

  const headers: Record<string, string> = { 'Content-Type': 'text/plain' };
  if (conn.user) headers['X-ClickHouse-User'] = conn.user;
  if (conn.password) headers['X-ClickHouse-Key'] = conn.password;

  let res: Response;
  try {
    res = await deps.fetch(`http://${conn.host}:${conn.port}/?${params.toString()}`, {
      method: 'POST',
      body: conn.query,
      headers,
    });
  } catch (err) {
    throw new CliError('io', `HTTP request failed: ${(err as Error).message}`);
  }
  if (!res.ok) {
    throw new CliError('io', `ClickHouse HTTP ${res.status}: ${(await res.text()).trim()}`);
  }
  const body = new Uint8Array(await res.arrayBuffer());
  if (body.length === 0) {
    throw new CliError('decode', 'server returned an empty body');
  }

  const result = decodeBuffer(body, { format: format.cli, protocolVersion });
  const source: Record<string, unknown> = {
    kind: 'query',
    protocol: 'http',
    query: conn.query,
    host: conn.host,
    port: conn.port,
    httpFormat: format.wire,
  };
  return buildDecodeEnvelope(result, source, { command: 'query', includeNodeBytes });
}
