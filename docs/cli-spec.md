# CLI & Tooling Spec

Captures the decisions from the requirements session for the agent-usable CLI
and related work (todo items 1, 2, 4, 5). **Item 3 (configurable TCP protocol
version) is dropped** — clickhouse-client negotiates `min(client, server)` on
its own and exposes no way to force a version, and clamping it in the proxy is
out of scope.

## 1. CLI — `chfx`

A single npm `bin` named **`chfx`**, run via Node/tsx. Decoders are imported
directly from `src/core` (one source of truth — see §Output). The package is
prepared **publish-ready**: a build step compiles `src/cli` + the needed core to
`dist`, and `package.json` (`bin`, `files`, `exports`) is wired so `npm publish`
works. Local use via `npx chfx` / `npm link`.

### Agent-usability requirements (item 1)
- **Deterministic JSON on stdout.** Primary output is one well-formed JSON
  document with stable key ordering. Diagnostics/logs go to **stderr** only.
- **Non-interactive by default.** No prompts, spinners, color, or progress on
  stdout. Everything is flag-controllable so an agent can script it.
- **Self-describing output.** The JSON carries a tool/schema version marker, the
  format, the (for captures) negotiated protocol version, and documents the
  `byteRange` convention inline.
- (Errors are emitted as JSON on stderr with a sane exit code — not a raw stack
  trace — but elaborate exit-code taxonomy was not prioritized.)

### Commands

#### `chfx decode`
Import a binary dump from a file **or stdin** and emit structured JSON.
- Inputs: `.chproto` captures, raw **Native** bodies, raw **RowBinary**
  (`RowBinaryWithNamesAndTypes`) bodies.
- Format detection: **autodetect with override.** `.chproto` is detected by its
  magic header; raw bodies are autodetected best-effort with
  `--format native|rowbinary|chproto` to force it (the reliable path when the
  Native/RowBinary heads are ambiguous).
- Accepts binary on **stdin** (e.g. piped from clickhouse-client) as well as a
  file path argument.

#### `chfx query` (implemented)
Run a query **and decode it in one step** — no intermediate file — over either
transport, emitting the same envelope as `decode`:
- **`--protocol tcp`** (default): drives `clickhouse-client` through the
  capturing proxy (`scripts/native-proxy.mjs`) and decodes the native packet
  stream. `--save <file>` also writes the raw `.chproto` dump.
- **`--protocol http`**: POSTs to ClickHouse HTTP requesting `--format`
  (`native` | `RowBinaryWithNamesAndTypes`, default native) and decodes the
  body. `--protocol-version <N>` sets the Native `client_protocol_version`.
  Port defaults to 8123 (env `CH_HTTP_PORT`); user/password go via
  `X-ClickHouse-User`/`-Key` headers.
- SQL via the **`--query` flag**. **Experimental type settings** sent by default
  (`--no-experimental-settings` to disable); `--setting k=v` repeatable.
- Connection flags `--host/--port/--user/--password/--database/--client` with
  env fallbacks (`CH_NATIVE_HOST`, `CH_NATIVE_PORT`/`CH_HTTP_PORT`, `CH_USER`,
  `CH_PASSWORD`, `CH_DATABASE`, `CLICKHOUSE_CLIENT`).
- **Deferred:** TLS. (The shelved own-TCP-client would remove the
  `clickhouse-client` dependency for tcp and could revive item 3.)

#### `chfx capture` (implemented)
Capture a query to a `.chproto` dump **without decoding**. `--out <file>` (`-o`)
writes the dump; omitted, it streams the raw dump bytes to stdout so
`chfx capture … | chfx decode` works. Shares all `query` connection flags.
**`npm run capture` is a thin alias** to `chfx capture` (the standalone
`scripts/capture-native.mjs` was folded in and removed).

#### `chfx proxy` (item 5 — standalone capture proxy)
A listener that forwards to a target server and captures the native TCP stream.
**Any** native client (clickhouse-client, Go/JDBC/Python drivers, …) connects
through it — the proxy does not spawn the client itself.
- Configurable lifecycle:
  - **Default: single-shot** — accept one connection, capture, write a
    `.chproto` (and decode if asked), then exit.
  - Flags opt into **persistent** mode (long-running, one `.chproto` per
    connection to an output dir) and **live decode** (`--decode` streams decoded
    JSON per connection to stdout).
- Flags: `--listen`, `--target host:port`, `--out`/`--save-dir`, `--decode`,
  `--persistent`/`--once`, plus remote `--user/--password`/TLS where applicable.
- **Plaintext/uncompressed only** (same constraint as today). TLS/compressed
  streams are unsupported — error clearly and document it.

#### `--help`
Human-readable help (`chfx --help`, `chfx <command> --help`). A standalone
machine-readable `schema` command was considered but **dropped** while the CLI
has a single real command: `--help` covers human discovery and the `decode`
output is already self-describing (carries `schemaVersion` and the byteRange/bytes
conventions inline). Revisit a structured-discovery surface (a `schema` command
or `--help --json`) once `query`/`proxy` add a multi-command contract.

### Output (item 4)
- **Reuse the web `ParsedData`/`AstNode` shape verbatim**, serialized to JSON,
  wrapped with top-level metadata: tool/schema version, format, negotiated
  protocol version (for captures), `nodeBytes`, and the raw bytes.
- **Top-level `bytesHex`**: the whole decoded buffer encoded once as hex
  (for NativeProtocol this is the combined c2s+s2c stream the ranges index into).
- **Per-node inline bytes (default on)**: every node with a `byteRange {start,
  end}` (exclusive end) also carries its own raw bytes as a `bytes` hex string,
  so a consumer reads a value's bytes directly without slicing `bytesHex`. This
  trades output size (parents duplicate children's bytes) for convenience;
  `--no-node-bytes` omits them and falls back to range lookups against `bytesHex`.
- JSON-safe values: bigints → decimal strings, byte blobs → hex.

### Tests & docs (cross-cutting)
- **Thorough CLI tests/fixtures**: decode against the existing
  `src/core/decoder/fixtures/protocol/*.chproto`, golden-JSON output assertions,
  plus query/proxy coverage.
- **README**: a short, practical **quick-start** near the top, and further down a
  full reference of all commands, options, settings, and defaults.

## 2. Docker — configurable server version (item 2)
- `ARG CH_VERSION=latest` → `FROM clickhouse/clickhouse-server:${CH_VERSION}`.
- Surfaced through docker-compose, e.g. `CH_VERSION=24.3 docker compose build`.
- Version is baked at **build time** (no runtime/CI-matrix publishing for now).

## 3. Configurable TCP/native protocol version — **dropped**
No reliable way to force the negotiated version via clickhouse-client; the
existing Native HTTP `client_protocol_version` selector is sufficient.
