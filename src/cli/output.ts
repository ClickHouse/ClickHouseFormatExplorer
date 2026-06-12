import process from 'node:process';
import { Buffer } from 'node:buffer';

/**
 * Error type carrying a machine-readable kind and process exit code. Thrown
 * anywhere in the CLI and rendered to stderr as a JSON envelope by the entry
 * point. `usage` errors exit 2 (bad invocation); everything else exits 1.
 */
export class CliError extends Error {
  constructor(
    public readonly kind: 'usage' | 'io' | 'decode',
    message: string,
    public readonly details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = 'CliError';
  }

  get exitCode(): number {
    return this.kind === 'usage' ? 2 : 1;
  }

  get code(): string {
    return `E_${this.kind.toUpperCase()}`;
  }
}

/** A command either emits a JSON document or raw bytes on stdout. */
export interface JsonOutput {
  stdout: 'json';
  data: unknown;
  compact: boolean;
}
export interface RawOutput {
  stdout: 'raw';
  bytes: Uint8Array;
}
export type CommandOutput = JsonOutput | RawOutput;

/**
 * JSON.stringify replacer that makes decoded values safe to serialize:
 * bigint → decimal string, byte arrays → hex, Map/Set → plain structures.
 */
export function jsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Uint8Array) return Buffer.from(value).toString('hex');
  if (value instanceof Map) return Object.fromEntries(value as Map<unknown, unknown>);
  if (value instanceof Set) return Array.from(value as Set<unknown>);
  return value;
}

/** Serialize a result object. Pretty (2-space) by default; compact on request. */
export function stringify(obj: unknown, compact: boolean): string {
  return JSON.stringify(obj, jsonReplacer, compact ? undefined : 2);
}

export function writeStdout(text: string): void {
  process.stdout.write(text.endsWith('\n') ? text : `${text}\n`);
}

export function writeStderr(text: string): void {
  process.stderr.write(text.endsWith('\n') ? text : `${text}\n`);
}

/** Render any thrown value as a JSON error envelope on stderr; return exit code. */
export function emitError(err: unknown): number {
  const cli =
    err instanceof CliError
      ? err
      : new CliError('decode', err instanceof Error ? err.message : String(err));
  const payload = {
    error: {
      code: cli.code,
      kind: cli.kind,
      message: cli.message,
      ...(cli.details ? { details: cli.details } : {}),
    },
  };
  writeStderr(JSON.stringify(payload, jsonReplacer, 2));
  return cli.exitCode;
}
