/**
 * Token types for ClickHouse type string lexer
 */
export type Token =
  | { type: 'IDENTIFIER'; value: string }
  | { type: 'LPAREN' }
  | { type: 'RPAREN' }
  | { type: 'COMMA' }
  | { type: 'NUMBER'; value: number }
  | { type: 'STRING'; value: string }
  | { type: 'EQUALS' };

/**
 * Tokenize a ClickHouse type string
 * Handles complex cases like Enum8('f\'' = 1, 'x =' = 2)
 */
export function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;

  while (i < input.length) {
    // Skip whitespace
    if (/\s/.test(input[i])) {
      i++;
      continue;
    }

    // Single char tokens
    if (input[i] === '(') {
      tokens.push({ type: 'LPAREN' });
      i++;
      continue;
    }
    if (input[i] === ')') {
      tokens.push({ type: 'RPAREN' });
      i++;
      continue;
    }
    if (input[i] === ',') {
      tokens.push({ type: 'COMMA' });
      i++;
      continue;
    }
    if (input[i] === '=') {
      tokens.push({ type: 'EQUALS' });
      i++;
      continue;
    }

    // String literal (for enums, timezones)
    if (input[i] === "'") {
      let str = '';
      i++; // skip opening quote
      while (i < input.length) {
        if (input[i] === '\\' && i + 1 < input.length) {
          // Handle escape sequences
          const nextChar = input[i + 1];
          if (nextChar === "'") {
            str += "'";
          } else if (nextChar === '\\') {
            str += '\\';
          } else if (nextChar === 'n') {
            str += '\n';
          } else if (nextChar === 't') {
            str += '\t';
          } else {
            str += nextChar;
          }
          i += 2;
        } else if (input[i] === "'") {
          i++; // skip closing quote
          break;
        } else {
          str += input[i];
          i++;
        }
      }
      tokens.push({ type: 'STRING', value: str });
      continue;
    }

    // Number (including negative)
    if (/[0-9]/.test(input[i]) || (input[i] === '-' && i + 1 < input.length && /[0-9]/.test(input[i + 1]))) {
      let num = '';
      if (input[i] === '-') {
        num += input[i];
        i++;
      }
      while (i < input.length && /[0-9]/.test(input[i])) {
        num += input[i];
        i++;
      }
      tokens.push({ type: 'NUMBER', value: parseInt(num, 10) });
      continue;
    }

    // Backtick-quoted identifier (for JSON paths like `a.b`)
    if (input[i] === '`') {
      let ident = '';
      i++; // skip opening backtick
      while (i < input.length && input[i] !== '`') {
        if (input[i] === '\\' && i + 1 < input.length) {
          // Handle escaped backtick
          if (input[i + 1] === '`') {
            ident += '`';
            i += 2;
          } else {
            ident += input[i];
            i++;
          }
        } else {
          ident += input[i];
          i++;
        }
      }
      i++; // skip closing backtick
      tokens.push({ type: 'IDENTIFIER', value: ident });
      continue;
    }

    // Identifier (type names, field names)
    if (/[a-zA-Z_]/.test(input[i])) {
      let ident = '';
      while (i < input.length && /[a-zA-Z0-9_]/.test(input[i])) {
        ident += input[i];
        i++;
      }
      tokens.push({ type: 'IDENTIFIER', value: ident });
      continue;
    }

    throw new Error(`Unexpected character '${input[i]}' at position ${i} in type string: ${input}`);
  }

  return tokens;
}
