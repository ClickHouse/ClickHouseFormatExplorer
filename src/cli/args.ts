import { CliError } from './output';

export interface ParsedArgs {
  positionals: string[];
  options: Record<string, string | boolean | string[]>;
}

export interface ArgSpec {
  /** Option names (canonical) that consume the following token as a single value. */
  valueFlags?: string[];
  /** Option names that consume a value and accumulate repeats into an array. */
  multiFlags?: string[];
  /** Short/alternate name → canonical name. */
  aliases?: Record<string, string>;
}

/**
 * Minimal, dependency-free argument parser. Supports `--flag`, `--flag value`,
 * `--flag=value`, single-dash aliases (`-f`), `--` to end option parsing, and a
 * lone `-` as a positional (stdin marker). Boolean flags are any option not in
 * `valueFlags`. Unknown flags are accepted (validated per-command) so the
 * parser stays generic.
 */
export function parseArgs(argv: string[], spec: ArgSpec = {}): ParsedArgs {
  const valueFlags = new Set(spec.valueFlags ?? []);
  const multiFlags = new Set(spec.multiFlags ?? []);
  const aliases = spec.aliases ?? {};
  const positionals: string[] = [];
  const options: Record<string, string | boolean | string[]> = {};

  const addValue = (name: string, value: string) => {
    if (multiFlags.has(name)) {
      const existing = options[name];
      options[name] = Array.isArray(existing) ? [...existing, value] : [value];
    } else {
      options[name] = value;
    }
  };

  let i = 0;
  let positionalOnly = false;
  while (i < argv.length) {
    const tok = argv[i++];

    if (positionalOnly || tok === '-' || !tok.startsWith('-')) {
      positionals.push(tok);
      continue;
    }
    if (tok === '--') {
      positionalOnly = true;
      continue;
    }

    const isLong = tok.startsWith('--');
    let name = tok.slice(isLong ? 2 : 1);
    let inlineValue: string | undefined;
    if (isLong) {
      const eq = name.indexOf('=');
      if (eq !== -1) {
        inlineValue = name.slice(eq + 1);
        name = name.slice(0, eq);
      }
    }
    name = aliases[name] ?? name;

    if (inlineValue !== undefined) {
      addValue(name, inlineValue);
    } else if (valueFlags.has(name) || multiFlags.has(name)) {
      if (i >= argv.length) {
        throw new CliError('usage', `option --${name} requires a value`);
      }
      addValue(name, argv[i++]);
    } else {
      options[name] = true;
    }
  }

  return { positionals, options };
}

/**
 * Read an option as a single string, or undefined if absent. Errors if it was
 * given without a value (a bare boolean flag) or accumulated as an array
 * (declared in `multiFlags` and given more than once).
 */
export function stringOption(args: ParsedArgs, name: string): string | undefined {
  const v = args.options[name];
  if (v === undefined) return undefined;
  if (typeof v === 'boolean') {
    throw new CliError('usage', `option --${name} requires a value`);
  }
  if (Array.isArray(v)) {
    throw new CliError('usage', `option --${name} may only be given once`);
  }
  return v;
}

export function boolOption(args: ParsedArgs, name: string): boolean {
  return args.options[name] === true || args.options[name] === 'true';
}

/**
 * Reject anything the command doesn't recognize: an option whose (canonical)
 * name isn't in `allowed`, or positionals beyond `maxPositionals`. Keeps the
 * permissive parser generic while letting each command fail fast on typos like
 * `--protcol` instead of silently ignoring them. `allowed` lists canonical
 * names (aliases are already resolved by parseArgs).
 */
export function rejectUnknownArgs(args: ParsedArgs, allowed: string[], maxPositionals = 0): void {
  const known = new Set(allowed);
  for (const name of Object.keys(args.options)) {
    if (!known.has(name)) {
      throw new CliError('usage', `unknown option: --${name}`);
    }
  }
  if (args.positionals.length > maxPositionals) {
    throw new CliError('usage', `unexpected argument: ${args.positionals[maxPositionals]}`);
  }
}

/** Read a repeatable option as an array (empty if absent). */
export function arrayOption(args: ParsedArgs, name: string): string[] {
  const v = args.options[name];
  if (v === undefined) return [];
  if (Array.isArray(v)) return v;
  if (typeof v === 'boolean') {
    throw new CliError('usage', `option --${name} requires a value`);
  }
  return [v];
}
