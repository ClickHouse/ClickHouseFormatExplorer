# TODO

Spec for items 1, 2, 4, 5: [docs/cli-spec.md](docs/cli-spec.md)

1. **CLI (`chfx`) usable by agent** — npm bin via Node/tsx, publish-ready.
   Deterministic JSON on stdout, non-interactive, self-describing. Commands:
   `decode`, `query`, `proxy`, `schema`/`--help`.
2. **Configurable server version in the image** — `ARG CH_VERSION` →
   `FROM clickhouse/clickhouse-server:${CH_VERSION}`, surfaced via docker-compose.
3. ~~Configurable protocol version in TCP + native web interface~~ — **dropped**
   (clickhouse-client can't force the negotiated version; HTTP selector suffices).
4. **Import binary dump in CLI → structured output** — `chfx decode` (and
   `query`): autodetect `.chproto`/Native/RowBinary with `--format` override,
   stdin supported. Emits the web `ParsedData`/`AstNode` JSON plus the full raw
   buffer inline as one hex string; agents slice it via each node's `byteRange`.
5. **Use with external clients, in the CLI** — `chfx proxy`: standalone capture
   proxy any native client connects through. Single-shot by default; persistent
   and live-decode via flags. Plaintext/uncompressed only.

Cross-cutting: thorough CLI tests/fixtures; README quick-start + full options
reference; remote auth/TLS flags for `query`/`proxy`.

6. Deploy similar to play.clickhouse.com?
