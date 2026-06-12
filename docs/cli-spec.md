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

#### `chfx query`
Run SQL against a server and decode the result in one step.
- Transport: **both, `--transport http|native`.**
  - `http`: POST to ClickHouse HTTP, request the chosen format, decode the body.
  - `native`: drive clickhouse-client through the capture proxy and decode the
    full `.chproto` packet stream.
- Default `--format native` (richest). **Experimental type settings**
  (Variant/Dynamic/JSON enablement) are **sent by default**, with
  `--no-experimental-settings` to disable for read-only/strict servers that
  reject them.
- Remote connection flags: `--host/--port/--user/--password`, env-var fallbacks,
  and HTTPS/TLS where applicable.

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

#### `chfx schema` / `--help`
Machine-readable description of commands, flags, and the output JSON shape for
agent discovery.

### Output (item 4)
- **Reuse the web `ParsedData`/`AstNode` shape verbatim**, serialized to JSON,
  wrapped with top-level metadata: tool/schema version, format, negotiated
  protocol version (for captures), and the raw bytes.
- **Raw bytes inline, once**, as a top-level **hex** string. Agents read a node's
  `byteRange {start, end}` (exclusive end) and slice the hex to inspect bytes —
  no second command or sidecar file.

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
