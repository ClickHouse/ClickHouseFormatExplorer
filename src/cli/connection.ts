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

/**
 * Parse a `[host:]port` (or bare `port`) endpoint, used by `chfx proxy` for
 * --listen / --target. A leading host is optional (defaults to `defaultHost`);
 * the port may default via `defaultPort`, otherwise it is required. `flag` only
 * shapes error messages.
 */
export function parseHostPort(
  raw: string,
  opts: { flag: string; defaultHost?: string; defaultPort?: number },
): { host: string; port: number } {
  const { flag, defaultHost = '127.0.0.1', defaultPort } = opts;
  let host = defaultHost;
  let portRaw: string | undefined;

  if (raw.startsWith('[')) {
    // Bracketed IPv6: [::1] or [::1]:9000
    const end = raw.indexOf(']');
    if (end === -1) throw new CliError('usage', `--${flag}: unterminated IPv6 address: ${raw}`);
    host = raw.slice(1, end);
    const rest = raw.slice(end + 1);
    if (rest.startsWith(':')) portRaw = rest.slice(1);
    else if (rest !== '') throw new CliError('usage', `--${flag}: unexpected text after IPv6 address: ${raw}`);
  } else {
    const firstColon = raw.indexOf(':');
    const lastColon = raw.lastIndexOf(':');
    if (firstColon !== -1 && firstColon === lastColon) {
      // Exactly one colon → host:port (host may be empty → default).
      const left = raw.slice(0, lastColon);
      if (left) host = left;
      portRaw = raw.slice(lastColon + 1);
    } else if (firstColon !== -1) {
      // Multiple colons, unbracketed → a bare IPv6 literal with no port.
      host = raw;
    } else if (/^\d+$/.test(raw)) {
      portRaw = raw;
    } else {
      host = raw;
    }
  }

  if (portRaw === undefined || portRaw === '') {
    if (defaultPort !== undefined) return { host, port: defaultPort };
    throw new CliError('usage', `--${flag} needs a port, e.g. --${flag} 9000 or --${flag} 0.0.0.0:9000`);
  }
  const port = Number(portRaw);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new CliError('usage', `--${flag} port must be an integer between 1 and 65535, got: ${portRaw}`);
  }
  return { host, port };
}

function parsePort(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new CliError('usage', `--port must be an integer between 1 and 65535, got: ${raw}`);
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
