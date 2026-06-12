import { writeFile } from 'node:fs/promises';

import {
  captureQuery as defaultCaptureQuery,
  encodeDump,
  type Capture,
  type CaptureQueryOptions,
} from '../../../scripts/native-proxy.mjs';

import { parseArgs, stringOption, boolOption, rejectUnknownArgs } from '../args';
import { CliError, type CommandOutput } from '../output';
import { CHFX_VERSION, CLI_SCHEMA_VERSION } from '../version';
import { resolveCaptureOptions, CONNECTION_VALUE_FLAGS, CONNECTION_MULTI_FLAGS, CONNECTION_ALLOWED } from '../connection';

export interface CaptureDeps {
  captureQuery: (opts: CaptureQueryOptions) => Promise<Capture>;
}

/**
 * Capture a query over the native protocol to a .chproto dump, without decoding.
 * With `--out <file>` it writes the dump and prints a JSON summary; otherwise it
 * streams the raw dump bytes to stdout (so `chfx capture … | chfx decode` works).
 */
export async function captureCommand(
  rest: string[],
  deps: CaptureDeps = { captureQuery: defaultCaptureQuery },
): Promise<CommandOutput> {
  const args = parseArgs(rest, {
    valueFlags: [...CONNECTION_VALUE_FLAGS, 'out'],
    multiFlags: CONNECTION_MULTI_FLAGS,
    aliases: { o: 'out' },
  });
  rejectUnknownArgs(args, [...CONNECTION_ALLOWED, 'out', 'compact']);
  const compact = boolOption(args, 'compact');
  const out = stringOption(args, 'out');
  const captureOpts = resolveCaptureOptions(args);

  let capture: Capture;
  try {
    capture = await deps.captureQuery(captureOpts);
  } catch (err) {
    throw new CliError('io', `capture failed: ${(err as Error).message}`);
  }

  const dump = encodeDump(capture);

  if (!out || out === '-') {
    return { stdout: 'raw', bytes: new Uint8Array(dump) };
  }

  try {
    await writeFile(out, dump);
  } catch (err) {
    throw new CliError('io', `cannot write --out file: ${out}`, { cause: (err as Error).message });
  }
  const data = {
    chfx: { tool: 'chfx', version: CHFX_VERSION, schemaVersion: CLI_SCHEMA_VERSION, command: 'capture' },
    query: captureOpts.query,
    saved: out,
    bytes: dump.length,
    c2sBytes: capture.c2s.length,
    s2cBytes: capture.s2c.length,
    segments: capture.segments.length,
  };
  return { stdout: 'json', data, compact };
}
