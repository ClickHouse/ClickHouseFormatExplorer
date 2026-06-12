export interface OptionDoc {
  flag: string;
  value?: string;
  description: string;
}

export interface CommandDoc {
  name: string;
  summary: string;
  usage: string;
  details?: string;
  options: OptionDoc[];
}

/** Single source of truth for the human `--help` text. */
export const COMMANDS: CommandDoc[] = [
  {
    name: 'decode',
    summary: 'Decode a binary dump (.chproto / Native / RowBinary) to structured JSON.',
    usage: 'chfx decode [file] [--format chproto|native|rowbinary] [--protocol-version N] [--compact]',
    details: 'Reads from <file>, or from stdin when no path is given (or path is "-").',
    options: [
      {
        flag: '--format, -f',
        value: 'chproto|native|rowbinary',
        description:
          'Force the decoder. Omit to autodetect: .chproto by magic header, raw bodies by trial decode (ambiguous → error asking for --format).',
      },
      {
        flag: '--protocol-version',
        value: 'N',
        description: 'Native client_protocol_version used to interpret a raw Native body (default 0).',
      },
      {
        flag: '--no-node-bytes',
        description:
          "Omit each node's inline raw bytes; consumers slice the top-level bytesHex by byteRange instead. Smaller output.",
      },
      { flag: '--compact', description: 'Emit single-line JSON instead of pretty-printed (2-space) JSON.' },
      { flag: '--help, -h', description: 'Show help for this command.' },
    ],
  },
  {
    name: 'query',
    summary: 'Run a query and decode the result in one step (native TCP capture or HTTP).',
    usage: 'chfx query --query "<sql>" [--protocol tcp|http] [--format ...] [connection options]',
    details:
      'tcp (default): drive clickhouse-client through a capturing proxy and decode the full packet ' +
      'stream (--save keeps the .chproto). http: POST to ClickHouse HTTP, request --format, decode the body.',
    options: [
      { flag: '--query', value: 'sql', description: 'SQL to run (required).' },
      { flag: '--protocol', value: 'tcp|http', description: 'Transport. tcp = native capture (default); http = HTTP request.' },
      { flag: '--format', value: 'native|RowBinaryWithNamesAndTypes', description: 'HTTP body format to request + decode (http only; default native).' },
      { flag: '--protocol-version', value: 'N', description: 'client_protocol_version for an http Native query (default 0).' },
      { flag: '--save', value: 'file', description: 'Write the raw .chproto capture here (tcp only).' },
      { flag: '--host / --port', value: 'h / p', description: 'Server host / port (env CH_NATIVE_HOST, CH_NATIVE_PORT/CH_HTTP_PORT; default 9000 tcp, 8123 http).' },
      { flag: '--user / --password', description: 'Credentials (env CH_USER / CH_PASSWORD).' },
      { flag: '--database', value: 'db', description: 'Default database (env CH_DATABASE).' },
      { flag: '--setting', value: 'k=v', description: 'Per-query setting; repeatable.' },
      { flag: '--no-experimental-settings', description: 'Do not send Variant/Dynamic/JSON/QBit enabling settings.' },
      { flag: '--client', value: 'path', description: 'Path to clickhouse-client, tcp only (env CLICKHOUSE_CLIENT).' },
      { flag: '--no-node-bytes / --compact', description: 'Same output controls as decode.' },
      { flag: '--help, -h', description: 'Show help for this command.' },
    ],
  },
  {
    name: 'capture',
    summary: 'Capture a query over the native protocol to a .chproto dump (no decode).',
    usage: 'chfx capture --query "<sql>" [--out <file>] [connection options]',
    details: 'Writes the dump to --out, or streams raw dump bytes to stdout when --out is omitted.',
    options: [
      { flag: '--query', value: 'sql', description: 'SQL to run (required).' },
      { flag: '--out, -o', value: 'file', description: 'Write the .chproto dump here; omit to stream raw bytes to stdout.' },
      { flag: '(connection)', description: 'Same --host/--port/--user/--password/--database/--setting/--client as query.' },
      { flag: '--help, -h', description: 'Show help for this command.' },
    ],
  },
  {
    name: 'proxy',
    summary: 'Listen as a capturing proxy any native client connects through (no client spawned).',
    usage: 'chfx proxy --listen [host:]port --target host:port [--out file | --save-dir dir] [--decode] [--persistent]',
    details:
      'Forwards every connection to --target and records the native stream. Single-shot by default (capture ' +
      'the first connection, then exit); --persistent keeps serving until Ctrl-C. Plaintext/uncompressed only.',
    options: [
      { flag: '--listen', value: '[host:]port', description: 'Address to listen on (host defaults to 127.0.0.1).' },
      { flag: '--target', value: 'host:port', description: 'Upstream ClickHouse native endpoint (default port 9000).' },
      { flag: '--out, -o', value: 'file', description: 'Single-shot: write the .chproto dump here (else raw dump streams to stdout).' },
      { flag: '--decode', description: 'Decode each capture to JSON on stdout instead of writing/streaming the dump.' },
      { flag: '--save-dir', value: 'dir', description: 'Persistent: write one conn-NNNN.chproto per connection into this dir.' },
      { flag: '--persistent / --once', description: 'Serve many connections until Ctrl-C, or stop after the first (default --once).' },
      { flag: '--no-node-bytes / --compact', description: 'Same output controls as decode (apply when --decode is set).' },
      { flag: '--help, -h', description: 'Show help for this command.' },
    ],
  },
];

export function findCommand(name: string): CommandDoc | undefined {
  return COMMANDS.find((c) => c.name === name);
}
