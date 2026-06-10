import { ClickHouseFormat } from '../types/formats';
import { DEFAULT_NATIVE_PROTOCOL_VERSION } from '../types/native-protocol';
import { appendClickHouseRequestParams } from './request-params';
import { parseChprotoDump } from '../decoder/protocol-dump';

/**
 * Electron IPC API exposed via preload script
 */
interface ElectronAPI {
  executeQuery(options: { query: string; format: string; nativeProtocolVersion?: number }): Promise<ArrayBuffer>;
  captureNativeProtocol(options: { query: string }): Promise<{ c2s: Uint8Array; s2c: Uint8Array; meta?: Record<string, unknown> }>;
  getConfig(): Promise<{ host: string }>;
  saveConfig(config: { host: string }): Promise<void>;
}

declare global {
  interface Window {
    electronAPI?: ElectronAPI;
  }
}

/**
 * ClickHouse HTTP client for fetching binary format data
 */

export interface QueryOptions {
  query: string;
  format?: ClickHouseFormat;
  nativeProtocolVersion?: number;
  timeout?: number;
}

export interface QueryResult {
  data: Uint8Array;
  timing: number;
}

export interface ProtocolCaptureResult {
  /** Concatenated [c2s][s2c] buffer (rawData for the hex viewer). */
  combined: Uint8Array;
  /** Split point: byte length of the client→server portion. */
  c2sLength: number;
  timing: number;
  meta?: Record<string, unknown>;
}

export class ClickHouseClient {
  private baseUrl: string;
  private captureUrl: string;

  constructor(baseUrl = '/clickhouse', captureUrl = '/capture') {
    this.baseUrl = baseUrl;
    this.captureUrl = captureUrl;
  }

  /**
   * Execute a query and return raw binary data
   */
  async query({
    query,
    format = ClickHouseFormat.RowBinaryWithNamesAndTypes,
    nativeProtocolVersion = DEFAULT_NATIVE_PROTOCOL_VERSION,
    timeout = 30000,
  }: QueryOptions): Promise<QueryResult> {
    if (window.electronAPI) {
      const startTime = performance.now();
      const buffer = await Promise.race([
        window.electronAPI.executeQuery({ query, format, nativeProtocolVersion }),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Query timeout after ${timeout}ms`)), timeout)
        ),
      ]);
      return { data: new Uint8Array(buffer), timing: performance.now() - startTime };
    }

    const startTime = performance.now();

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      const params = new URLSearchParams();
      appendClickHouseRequestParams(params, format, nativeProtocolVersion);

      const response = await fetch(`${this.baseUrl}/?${params.toString()}`, {
        method: 'POST',
        body: query,
        headers: {
          'Content-Type': 'text/plain',
        },
        signal: controller.signal,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`ClickHouse error (${response.status}): ${errorText}`);
      }

      const buffer = await response.arrayBuffer();
      const data = new Uint8Array(buffer);
      const timing = performance.now() - startTime;

      return { data, timing };
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error(`Query timeout after ${timeout}ms`);
      }
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Capture a query over the native TCP protocol. Drives clickhouse-client
   * through a capturing proxy and returns both per-direction streams
   * concatenated for the protocol decoder.
   *
   * - Desktop (Electron): the proxy runs in the main process via IPC.
   * - Web: POSTs the SQL to the `/capture` endpoint (Vite dev/preview server),
   *   which runs the proxy server-side and returns a `.chproto` dump. The
   *   browser cannot open raw TCP itself, so this requires the dev/preview
   *   server (or another host serving `/capture`).
   */
  async captureProtocol(query: string): Promise<ProtocolCaptureResult> {
    const startTime = performance.now();

    if (window.electronAPI?.captureNativeProtocol) {
      const { c2s, s2c, meta } = await window.electronAPI.captureNativeProtocol({ query });
      return assembleCapture(new Uint8Array(c2s), new Uint8Array(s2c), meta, startTime);
    }

    const response = await fetch(this.captureUrl, {
      method: 'POST',
      body: query,
      headers: { 'Content-Type': 'text/plain' },
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Capture failed (${response.status}): ${text}`);
    }
    const dump = new Uint8Array(await response.arrayBuffer());
    const { c2s, s2c, meta } = parseChprotoDump(dump);
    return assembleCapture(c2s, s2c, meta, startTime);
  }
}

/** Concatenate the two per-direction streams and record the split point. */
function assembleCapture(
  c2s: Uint8Array,
  s2c: Uint8Array,
  meta: Record<string, unknown> | undefined,
  startTime: number,
): ProtocolCaptureResult {
  const combined = new Uint8Array(c2s.length + s2c.length);
  combined.set(c2s, 0);
  combined.set(s2c, c2s.length);
  return { combined, c2sLength: c2s.length, timing: performance.now() - startTime, meta };
}

// Default client instance
export const clickhouse = new ClickHouseClient();

/**
 * Default sample query that exercises various types
 */
export const DEFAULT_QUERY = `SELECT
    42 :: UInt8 AS u8,
    1000 :: UInt32 AS u32,
    -123456789 :: Int64 AS i64,
    3.14159 :: Float32 AS f32,
    2.718281828 :: Float64 AS f64,
    'Hello, World!' AS str,
    NULL :: Nullable(UInt32) AS null_val,
    42 :: Nullable(UInt32) AS notnull_val,
    [1, 2, 3] :: Array(UInt32) AS arr,
    (100, 'tuple_str', 3.14) :: Tuple(UInt32, String, Float32) AS tup,
    [[1, 2], [3, 4, 5]] :: Array(Array(UInt16)) AS nested_arr,
    [(1, 'first'), (2, 'second')] :: Array(Tuple(UInt32, String)) AS complex`;
