export interface OptionDoc {
  flag: string;
  value?: string;
  description: string;
}

export interface CommandDoc {
  name: string;
  summary: string;
  usage: string;
  details?: string;
  options: OptionDoc[];
}

/** Single source of truth for `chfx schema` and the human `--help` text. */
export const COMMANDS: CommandDoc[] = [
  {
    name: 'decode',
    summary: 'Decode a binary dump (.chproto / Native / RowBinary) to structured JSON.',
    usage: 'chfx decode [file] [--format chproto|native|rowbinary] [--protocol-version N] [--compact]',
    details: 'Reads from <file>, or from stdin when no path is given (or path is "-").',
    options: [
      {
        flag: '--format, -f',
        value: 'chproto|native|rowbinary',
        description:
          'Force the decoder. Omit to autodetect: .chproto by magic header, raw bodies by trial decode (ambiguous → error asking for --format).',
      },
      {
        flag: '--protocol-version',
        value: 'N',
        description: 'Native client_protocol_version used to interpret a raw Native body (default 0).',
      },
      {
        flag: '--no-node-bytes',
        description:
          "Omit each node's inline raw bytes; consumers slice the top-level bytesHex by byteRange instead. Smaller output.",
      },
      { flag: '--compact', description: 'Emit single-line JSON instead of pretty-printed (2-space) JSON.' },
      { flag: '--help, -h', description: 'Show help for this command.' },
    ],
  },
];

export function findCommand(name: string): CommandDoc | undefined {
  return COMMANDS.find((c) => c.name === name);
}
