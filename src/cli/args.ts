import { CliError } from './output';

export interface ParsedArgs {
  positionals: string[];
  options: Record<string, string | boolean>;
}

export interface ArgSpec {
  /** Option names (canonical) that consume the following token as a value. */
  valueFlags?: string[];
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
  const aliases = spec.aliases ?? {};
  const positionals: string[] = [];
  const options: Record<string, string | boolean> = {};

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
      options[name] = inlineValue;
    } else if (valueFlags.has(name)) {
      if (i >= argv.length) {
        throw new CliError('usage', `option --${name} requires a value`);
      }
      options[name] = argv[i++];
    } else {
      options[name] = true;
    }
  }

  return { positionals, options };
}

/** Read an option as a string, or undefined if absent. Errors if it's a bare boolean flag. */
export function stringOption(args: ParsedArgs, name: string): string | undefined {
  const v = args.options[name];
  if (v === undefined) return undefined;
  if (typeof v === 'boolean') {
    throw new CliError('usage', `option --${name} requires a value`);
  }
  return v;
}

export function boolOption(args: ParsedArgs, name: string): boolean {
  return args.options[name] === true || args.options[name] === 'true';
}
