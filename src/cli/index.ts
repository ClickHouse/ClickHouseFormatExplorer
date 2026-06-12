import process from 'node:process';

import { CliError, emitError, writeStdout, stringify, type CommandOutput } from './output';
import { CHFX_VERSION } from './version';
import { COMMANDS, findCommand, type CommandDoc } from './registry';
import { decodeCommand } from './commands/decode';
import { queryCommand } from './commands/query';
import { captureCommand } from './commands/capture';

function generalHelp(): string {
  const lines = [
    'chfx — ClickHouse Format Explorer CLI',
    '',
    'Decode ClickHouse wire-format dumps into structured JSON for humans and agents.',
    '',
    'Usage: chfx <command> [options]',
    '',
    'Commands:',
    ...COMMANDS.map((c) => `  ${c.name.padEnd(8)} ${c.summary}`),
    '',
    'Global:',
    '  --help, -h      Show help (per command: chfx <command> --help)',
    '  --version, -V   Print version',
    '',
    'Output: a single JSON document on stdout; diagnostics and a JSON error',
    'envelope on stderr. Run "chfx schema" for machine-readable documentation.',
  ];
  return lines.join('\n');
}

function commandHelp(doc: CommandDoc): string {
  const lines = [doc.summary, '', `Usage: ${doc.usage}`];
  if (doc.details) lines.push('', doc.details);
  lines.push('', 'Options:');
  const width = Math.max(...doc.options.map((o) => `${o.flag}${o.value ? ` <${o.value}>` : ''}`.length));
  for (const o of doc.options) {
    const left = `${o.flag}${o.value ? ` <${o.value}>` : ''}`.padEnd(width);
    lines.push(`  ${left}   ${o.description}`);
  }
  return lines.join('\n');
}

async function run(argv: string[]): Promise<number> {
  const [command, ...rest] = argv;

  if (!command || command === 'help' || command === '--help' || command === '-h') {
    writeStdout(generalHelp());
    return 0;
  }
  if (command === '--version' || command === '-V') {
    writeStdout(CHFX_VERSION);
    return 0;
  }

  if (rest.includes('--help') || rest.includes('-h')) {
    const doc = findCommand(command);
    if (!doc) throw new CliError('usage', `unknown command: ${command} (try: chfx --help)`);
    writeStdout(commandHelp(doc));
    return 0;
  }

  let out: CommandOutput;
  switch (command) {
    case 'decode':
      out = await decodeCommand(rest);
      break;
    case 'query':
      out = await queryCommand(rest);
      break;
    case 'capture':
      out = await captureCommand(rest);
      break;
    default:
      throw new CliError('usage', `unknown command: ${command} (try: chfx --help)`);
  }

  if (out.stdout === 'json') {
    writeStdout(stringify(out.data, out.compact));
  } else {
    process.stdout.write(out.bytes);
  }
  return 0;
}

run(process.argv.slice(2))
  .then((code) => {
    process.exitCode = code;
  })
  .catch((err) => {
    process.exitCode = emitError(err);
  });
