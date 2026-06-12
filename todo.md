# TODO

Spec for items 1, 2, 4, 5: [docs/cli-spec.md](docs/cli-spec.md)

1. **CLI (`chfx`) usable by agent** — _implemented (branch `cli-foundation-decode`,
   with item 4)._ npm bin via Node/tsx, publish-ready (esbuild bundle); `npm link`
   for a PATH `chfx`. Deterministic JSON on stdout, JSON error envelope on stderr,
   non-interactive, self-describing. Commands: `decode`, `query`, `capture` +
   `--help`/`--version`. (A standalone `schema` command was dropped — `--help` +
   self-describing output suffice for now.)
2. **Configurable server version in the image** — _done (#42)._ `ARG CH_VERSION` →
   `FROM clickhouse/clickhouse-server:${CH_VERSION}`, surfaced via docker-compose.
3. ~~Configurable protocol version in TCP + native web interface~~ — **dropped**
   (clickhouse-client can't force the negotiated version; HTTP selector suffices).
4. **Import binary dump in CLI → structured output** — _implemented (with item 1)._
   `chfx decode`: autodetect `.chproto`/Native/RowBinary with `--format` override,
   stdin supported. Emits the web `ParsedData`/`AstNode` JSON; top-level `bytesHex`
   (whole buffer once) plus per-node inline `bytes` by default (`--no-node-bytes`
   to omit).
   - **UX one-shot (implemented):** `chfx query --query "<sql>"` runs **and** decodes
     in one step — no intermediate file. `--protocol tcp` (default) captures via
     clickhouse-client; `--protocol http` POSTs and decodes the `--format`
     (native | RowBinaryWithNamesAndTypes) body. `--save` keeps the dump (tcp).
     `chfx capture` writes a dump (file or raw stdout); `npm run capture` aliases it.
5. **Use with external clients, in the CLI** — `chfx proxy`: standalone capture
   proxy any native client connects through. Single-shot by default; persistent
   and live-decode via flags. Plaintext/uncompressed only. _(Not yet built — the
   `query`/`capture` above are us being the client; the proxy captures other
   clients.)_

Cross-cutting: thorough CLI tests/fixtures; README quick-start + full options
reference; remote auth/TLS flags. _(Native own-TCP-client to drop the
clickhouse-client dependency: considered, shelved for now.)_

6. Deploy similar to play.clickhouse.com?
