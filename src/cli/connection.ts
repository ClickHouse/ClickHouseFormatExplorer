import process from 'node:process';

import { stringOption, arrayOption, boolOption, type ParsedArgs } from './args';
import { CliError } from './output';
import type { CaptureQueryOptions } from '../../scripts/native-proxy.mjs';

/**
 * Experimental type settings sent per-query so Variant/Dynamic/JSON/QBit decode.
 * Mirrors the web capture server; disable with --no-experimental-settings for
 * read-only/strict servers that reject them.
 */
const EXPERIMENTAL_SETTINGS: Record<string, string> = {
  allow_experimental_variant_type: '1',
  allow_experimental_dynamic_type: '1',
  allow_experimental_json_type: '1',
  allow_suspicious_variant_types: '1',
  allow_experimental_qbit_type: '1',
  allow_suspicious_low_cardinality_types: '1',
};

/** Flags shared by `query` and `capture` for arg-parser specs. */
export const CONNECTION_VALUE_FLAGS = ['query', 'host', 'port', 'user', 'password', 'database', 'client'];
export const CONNECTION_MULTI_FLAGS = ['setting'];
/** Every connection-related option name (for unknown-arg rejection). */
export const CONNECTION_ALLOWED = [...CONNECTION_VALUE_FLAGS, ...CONNECTION_MULTI_FLAGS, 'no-experimental-settings'];

function parseSettings(pairs: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const pair of pairs) {
    const eq = pair.indexOf('=');
    if (eq === -1) throw new CliError('usage', `--setting must be key=value, got: ${pair}`);
    out[pair.slice(0, eq)] = pair.slice(eq + 1);
  }
  return out;
}

export interface QueryBase {
  query: string;
  host?: string;
  user?: string;
  password?: string;
  database?: string;
  settings: Record<string, string>;
}

/**
 * Transport-independent query + connection bits, with env fallbacks
 * (CH_NATIVE_HOST, CH_USER, CH_PASSWORD, CH_DATABASE). Experimental type
 * settings are merged first so explicit --setting can override them.
 */
export function resolveQueryBase(args: ParsedArgs): QueryBase {
  const query = stringOption(args, 'query');
  if (!query) throw new CliError('usage', '--query is required');

  const experimental = !boolOption(args, 'no-experimental-settings');
  const settings = { ...(experimental ? EXPERIMENTAL_SETTINGS : {}), ...parseSettings(arrayOption(args, 'setting')) };

  return {
    query,
    host: stringOption(args, 'host') ?? process.env.CH_NATIVE_HOST,
    user: stringOption(args, 'user') ?? process.env.CH_USER,
    password: stringOption(args, 'password') ?? process.env.CH_PASSWORD,
    database: stringOption(args, 'database') ?? process.env.CH_DATABASE,
    settings,
  };
}

function parsePort(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const port = Number(raw);
  if (!Number.isInteger(port) || port <= 0) {
    throw new CliError('usage', `--port must be a positive integer, got: ${raw}`);
  }
  return port;
}

/**
 * Native-protocol (TCP) capture options. Undefined host/port let the proxy apply
 * its own defaults (127.0.0.1:9000). Env: CH_NATIVE_PORT, CLICKHOUSE_CLIENT.
 */
export function resolveCaptureOptions(args: ParsedArgs): CaptureQueryOptions {
  return {
    ...resolveQueryBase(args),
    port: parsePort(stringOption(args, 'port') ?? process.env.CH_NATIVE_PORT),
    clientPath: stringOption(args, 'client') ?? process.env.CLICKHOUSE_CLIENT,
  };
}

/** HTTP connection: host/port default 127.0.0.1:8123 (env CH_NATIVE_HOST, CH_HTTP_PORT). */
export function resolveHttpConnection(args: ParsedArgs): QueryBase & { host: string; port: number } {
  const base = resolveQueryBase(args);
  return {
    ...base,
    host: base.host ?? '127.0.0.1',
    port: parsePort(stringOption(args, 'port') ?? process.env.CH_HTTP_PORT) ?? 8123,
  };
}
