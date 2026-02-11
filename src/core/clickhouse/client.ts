import { ClickHouseFormat } from '../types/formats';

/**
 * Electron IPC API exposed via preload script
 */
interface ElectronAPI {
  executeQuery(options: { query: string; format: string }): Promise<ArrayBuffer>;
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
  timeout?: number;
}

export interface QueryResult {
  data: Uint8Array;
  timing: number;
}

export class ClickHouseClient {
  private baseUrl: string;

  constructor(baseUrl = '/clickhouse') {
    this.baseUrl = baseUrl;
  }

  /**
   * Execute a query and return raw binary data
   */
  async query({ query, format = ClickHouseFormat.RowBinaryWithNamesAndTypes, timeout = 30000 }: QueryOptions): Promise<QueryResult> {
    if (window.electronAPI) {
      const startTime = performance.now();
      const buffer = await Promise.race([
        window.electronAPI.executeQuery({ query, format }),
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
      const response = await fetch(`${this.baseUrl}/?default_format=${format}`, {
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
