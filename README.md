# ClickHouse Format Explorer

A tool for visualizing ClickHouse RowBinary and Native format data. Features an interactive hex viewer with AST-based type visualization. Available as a web app or Electron desktop app.

![Screenshot](.static/screenshot.png)

## Features

- **Format support**: RowBinary and Native, modular system allows adding more
- **Native protocol version**: Select the Native `client_protocol_version` to inspect revision-specific wire layouts
- **Hex Viewer**: Virtual-scrolling hex display with ASCII column
- **AST Tree**: Collapsible tree view showing decoded structure
- **Interactive Highlighting**: Selecting a node in the tree highlights corresponding bytes in the hex view (and vice versa)
- **Full Type Support**: All ClickHouse types including Variant, Dynamic, JSON, Geo types, Nested, etc.
- **Desktop App**: Electron app that connects to your existing ClickHouse server (no bundled DB)
- **CLI (`chfx`)**: Decode `.chproto` / Native / RowBinary dumps to structured JSON from the terminal — agent-friendly

## Quick Start (Docker)

Run with bundled ClickHouse server:

```bash
docker build -t rowbinary-explorer .
docker run -d -p 8080:80 rowbinary-explorer
```

Open http://localhost:8080

### Bundled ClickHouse version

The image bundles a ClickHouse server, pinned to `latest` by default. Choose a
specific version at **build time** with the `CH_VERSION` build argument (it maps
to the `clickhouse/clickhouse-server:<tag>` image tag):

```bash
# docker build
docker build --build-arg CH_VERSION=24.3 -t rowbinary-explorer .

# docker compose
CH_VERSION=24.3 docker compose build
```

The version is baked into the image — rebuild to change it.

## CLI (`chfx`)

A command-line tool that runs or decodes ClickHouse wire-format data and prints
structured JSON — the same AST the web UI renders, plus the raw bytes — so it
can be scripted or driven by an agent.

### Quick start

```bash
npm install
npm run cli:build   # build the binary → dist/cli/index.js
npm link            # makes `chfx` available on your PATH (points at the built binary)

# Run a query and see it decoded — one step, no intermediate file:
chfx query --query "SELECT number AS n, [number] AS arr FROM numbers(3)"

# Decode a dump you already have (or pipe one in):
chfx decode capture.chproto
clickhouse-client -q "SELECT 1 FORMAT Native" | chfx decode -f native -
```

> Prefer not to `npm link`? Use `npm run cli -- <args>` (runs from source via
> tsx, no build) or `node dist/cli/index.js <args>` after `cli:build`.

Output is a single JSON document on **stdout**; diagnostics and a JSON error
envelope go to **stderr**. Exit codes: `0` success, `2` usage error, `1` I/O or
decode error.

### Commands

| Command | Description |
|---------|-------------|
| `chfx query --query "<sql>"` | Run a query **and decode it** in one step (no file). `--protocol tcp` (default) captures the native packet stream via `clickhouse-client`; `--protocol http` POSTs to ClickHouse HTTP and decodes the `--format` body. `--save <f>` keeps the `.chproto` dump (tcp). |
| `chfx capture --query "<sql>"` | Capture a query to a `.chproto` dump only (native protocol). Writes `--out <f>`, or streams raw bytes to stdout (so `chfx capture … \| chfx decode` works). `npm run capture` is an alias. |
| `chfx proxy --listen <port> --target <host:port>` | Listen as a capturing TCP proxy that **any** native client connects through (clickhouse-client, Go/JDBC/Python drivers, …). Single-shot by default; `--persistent` serves many connections. See below. |
| `chfx decode [file]` | Decode a `.chproto`, Native, or RowBinary dump to JSON. Reads stdin when no file (or `-`) is given. |
| `chfx --help` / `chfx <cmd> --help` | Human-readable help. |
| `chfx --version` | Print the version. |

### `query` transport options

| Option | Description |
|--------|-------------|
| `--protocol tcp\|http` | Transport. `tcp` (default) = native capture via `clickhouse-client`. `http` = HTTP request. |
| `--format native\|RowBinaryWithNamesAndTypes` | **http only** — the body format to request and decode (default `native`). |
| `--protocol-version <N>` | `client_protocol_version` for an http Native query (default `0`). |
| `--save <file>` | **tcp only** — also write the raw `.chproto` capture. |

### Connection options (`query` / `capture`)

| Option | Description |
|--------|-------------|
| `--query <sql>` | SQL to run (required). |
| `--host` / `--port` | Server host / port. Env: `CH_NATIVE_HOST`, `CH_NATIVE_PORT` (tcp) / `CH_HTTP_PORT` (http). Default `127.0.0.1`, port `9000` (tcp) / `8123` (http). |
| `--user` / `--password` | Credentials. Env: `CH_USER` / `CH_PASSWORD`. |
| `--database <db>` | Default database. Env: `CH_DATABASE`. |
| `--setting k=v` | Per-query setting; repeatable. |
| `--no-experimental-settings` | Don't send the Variant/Dynamic/JSON/QBit enabling settings (sent by default). |
| `--client <path>` | Path to `clickhouse-client` (tcp only). Env: `CLICKHOUSE_CLIENT`. |
| `--out <file>` (`capture`) | Where to write the `.chproto` dump. |

### `proxy` — capture any native client

Unlike `query`/`capture` (which drive `clickhouse-client` for you), `proxy`
just **listens**: it forwards every connection to `--target` and tees the native
packet stream into a capture. Point any native client at the listen address —
the proxy never spawns one itself. Plaintext/uncompressed connections only (TLS
and compressed streams are unsupported, the same constraint as the other native
paths).

```bash
# Single-shot: capture the next connection, write a dump, exit.
chfx proxy --listen 9100 --target 127.0.0.1:9000 --out cap.chproto
clickhouse-client --port 9100 --query "SELECT 1"   # in another shell

# Single-shot, decoded straight to JSON (no file):
chfx proxy --listen 9100 --target 127.0.0.1:9000 --decode

# Persistent: serve many connections, one dump per connection, until Ctrl-C.
chfx proxy --listen 9100 --target 127.0.0.1:9000 --persistent --save-dir ./caps
```

| Option | Description |
|--------|-------------|
| `--listen <[host:]port>` | Address to listen on (host defaults to `127.0.0.1`). Required. |
| `--target <host:port>` | Upstream ClickHouse native endpoint (default port `9000`). Required. |
| `--out <file>` (`-o`) | **Single-shot** — write the `.chproto` dump here; omit (and no `--decode`) to stream the raw dump to stdout. |
| `--decode` | Decode each capture to a JSON envelope on stdout (instead of writing/streaming the raw dump). |
| `--save-dir <dir>` | **Persistent** — write one `conn-NNNN.chproto` per connection into this directory. |
| `--persistent` / `--once` | Serve until Ctrl-C, or stop after the first connection (default `--once`). |
| `--no-node-bytes` / `--compact` | Same output controls as `decode` (apply when `--decode` is set). |

Diagnostics (the listen address, per-connection notices) go to **stderr**, so
stdout stays a clean dump or JSON stream.

### `decode` options

| Option | Description |
|--------|-------------|
| `--format`, `-f` `<chproto\|native\|rowbinary>` | Force the decoder. Omitted → autodetect: `.chproto` by magic header, raw bodies by trial decode (ambiguous input errors and asks for `--format`). |
| `--protocol-version <N>` | Native `client_protocol_version` used to interpret a raw Native body (default `0`). |
| `--no-node-bytes` | Omit each node's inline raw bytes (consumers slice `bytesHex` by range instead). Smaller output. |
| `--compact` | Emit single-line JSON instead of pretty-printed. |

### Output shape

```jsonc
{
  "chfx":    { "tool": "chfx", "version": "...", "schemaVersion": 1, "command": "decode" },  // or "query"
  "source":  { "kind": "file", "path": "...", "byteLength": 2417 },  // kind "stdin" | "query" too
  "format":  "NativeProtocol",          // | Native | RowBinaryWithNamesAndTypes
  "formatDetected": true,                // false when forced via --format
  "protocolVersion": 54482,              // negotiated (chproto) / requested (native) / null (rowbinary)
  "nodeBytes": true,                     // false when --no-node-bytes was passed
  "protocol": { "negotiatedVersion": 54482, "c2sLength": 191, "dumpMeta": { ... } },
  "bytesHex": "0011436c...",            // the whole decoded buffer, encoded once
  "data":    { /* ParsedData: header, rows|blocks, trailingNodes, metadata */ }
}
```

Every node has a `byteRange` of `{start, end}` byte offsets into `bytesHex` (two
hex chars per byte; `start` inclusive, `end` exclusive). By default each node
**also carries its own raw bytes inline** as a `bytes` hex string, so a consumer
can read the bytes behind any value without slicing `bytesHex` itself — pass
`--no-node-bytes` to drop them for smaller output.

> Decoded values are JSON-safe: 64-bit and larger integers become decimal
> strings, and raw byte blobs become hex.

## Desktop App

For developers who already run ClickHouse locally. Download the latest release for your platform from the [Releases](../../releases) page:

| Platform | Format |
|----------|--------|
| Windows  | `.exe` (NSIS installer) |
| macOS    | `.dmg` |
| Linux    | `.AppImage` / `.deb` |

### Configuration

The app looks for a `config.json` file next to the executable:

```json
{
  "host": "http://localhost:8123"
}
```

You can also change the host from the **Host** field in the toolbar. Changes are saved back to `config.json`.

### Building from source

```bash
npm install
npm run electron:dev    # Dev mode with hot reload
npm run electron:build  # Package installer for current platform
```

## Web Development Setup

For local web development (requires ClickHouse at `localhost:8123`):

```bash
npm install
npm run dev
```

Open http://localhost:5173

## Usage

1. Enter a SQL query in the input box
2. Click "Execute" to fetch data from ClickHouse
3. Explore the parsed data:
   - Click nodes in the AST tree to highlight bytes
   - Click bytes in the hex viewer to select the corresponding node
   - Use "Expand All" / "Collapse All" to navigate complex structures
4. When using `Native`, choose a protocol preset to compare legacy HTTP output against newer revisions such as custom serialization, Dynamic/JSON v2, replicated, and nullable sparse encodings

## Example Queries

```sql
-- Basic types
SELECT 42::UInt32, 'hello'::String, [1,2,3]::Array(UInt8)

-- Complex nested structures
SELECT (1, 'foo', [1,2,3])::Tuple(id UInt32, name String, values Array(UInt8))

-- Dynamic/JSON types
SELECT '{"a": 1, "b": "hello"}'::JSON
SELECT 42::Dynamic

-- With typed JSON paths
SELECT '{"user": {"id": 123}}'::JSON(`user.id` UInt32)
```

## Native Protocol Versions

The `Native` format toolbar exposes upstream protocol milestones from `0` through `54483`. This controls the `client_protocol_version` request parameter and the local decoder behavior, so the explorer can parse:

- legacy HTTP Native blocks without `BlockInfo` (`0`)
- per-column serialization metadata (`54454+`)
- sparse and replicated serialization kinds (`54465+`, `54482+`)
- Dynamic/JSON v2 Native layouts (`54473+`)
- nullable sparse serialization (`54483`)

See [docs/native-protocol-versions.md](docs/native-protocol-versions.md) for the revision-by-revision reference, and [docs/nativespec.md](docs/nativespec.md) for the Native layout details.

## Tech Stack

- React + TypeScript + Vite
- Zustand (state management)
- react-window (virtualized hex viewer)
- react-resizable-panels (split pane layout)
- Electron (desktop app, optional)
- Vitest + testcontainers (integration testing)
- Playwright (e2e testing)
