// Injected at build time by scripts/build-cli.mjs via esbuild `define`. Under
// `tsx` (dev) the identifier is undeclared, so `typeof` safely yields the
// dev fallback rather than throwing.
declare const __CHFX_VERSION__: string;

export const CHFX_VERSION: string =
  typeof __CHFX_VERSION__ === 'string' ? __CHFX_VERSION__ : '0.0.0-dev';

/** Version of the CLI's JSON output envelope. Bump on breaking shape changes. */
export const CLI_SCHEMA_VERSION = 1;
