import { tokenize, Token } from './type-lexer';
import { ClickHouseType, typeToString } from '../types/clickhouse-types';

/**
 * Parse a ClickHouse type string into a structured type object
 */
export function parseType(typeString: string): ClickHouseType {
  const tokens = tokenize(typeString);
  let pos = 0;

  function peek(): Token | undefined {
    return tokens[pos];
  }

  function consume(): Token {
    if (pos >= tokens.length) {
      throw new Error(`Unexpected end of type string: ${typeString}`);
    }
    return tokens[pos++];
  }

  function expect(type: Token['type']): Token {
    const t = consume();
    if (t.type !== type) {
      throw new Error(`Expected ${type}, got ${t.type} in type string: ${typeString}`);
    }
    return t;
  }

  function parseTypeExpr(): ClickHouseType {
    const token = consume();
    if (token.type !== 'IDENTIFIER') {
      throw new Error(`Expected type name, got ${token.type} in type string: ${typeString}`);
    }

    const typeName = token.value;

    // Simple types without parameters
    const simpleTypes: Record<string, ClickHouseType> = {
      UInt8: { kind: 'UInt8' },
      UInt16: { kind: 'UInt16' },
      UInt32: { kind: 'UInt32' },
      UInt64: { kind: 'UInt64' },
      UInt128: { kind: 'UInt128' },
      UInt256: { kind: 'UInt256' },
      Int8: { kind: 'Int8' },
      Int16: { kind: 'Int16' },
      Int32: { kind: 'Int32' },
      Int64: { kind: 'Int64' },
      Int128: { kind: 'Int128' },
      Int256: { kind: 'Int256' },
      Float32: { kind: 'Float32' },
      Float64: { kind: 'Float64' },
      BFloat16: { kind: 'BFloat16' },
      String: { kind: 'String' },
      Bool: { kind: 'Bool' },
      Date: { kind: 'Date' },
      Date32: { kind: 'Date32' },
      Time: { kind: 'Time' },
      UUID: { kind: 'UUID' },
      IPv4: { kind: 'IPv4' },
      IPv6: { kind: 'IPv6' },
      Point: { kind: 'Point' },
      Ring: { kind: 'Ring' },
      Polygon: { kind: 'Polygon' },
      MultiPolygon: { kind: 'MultiPolygon' },
      LineString: { kind: 'LineString' },
      MultiLineString: { kind: 'MultiLineString' },
      Geometry: { kind: 'Geometry' },
      Dynamic: { kind: 'Dynamic' },
      JSON: { kind: 'JSON' },
      DateTime: { kind: 'DateTime' },
      // Interval types (all stored as Int64)
      IntervalNanosecond: { kind: 'IntervalNanosecond' },
      IntervalMicrosecond: { kind: 'IntervalMicrosecond' },
      IntervalMillisecond: { kind: 'IntervalMillisecond' },
      IntervalSecond: { kind: 'IntervalSecond' },
      IntervalMinute: { kind: 'IntervalMinute' },
      IntervalHour: { kind: 'IntervalHour' },
      IntervalDay: { kind: 'IntervalDay' },
      IntervalWeek: { kind: 'IntervalWeek' },
      IntervalMonth: { kind: 'IntervalMonth' },
      IntervalQuarter: { kind: 'IntervalQuarter' },
      IntervalYear: { kind: 'IntervalYear' },
    };

    // If next token is not LPAREN, it's a simple type
    if (peek()?.type !== 'LPAREN') {
      if (simpleTypes[typeName]) {
        return simpleTypes[typeName];
      }
      throw new Error(`Unknown simple type: ${typeName}`);
    }

    // Parameterized types
    consume(); // LPAREN

    switch (typeName) {
      case 'Array': {
        const element = parseTypeExpr();
        expect('RPAREN');
        return { kind: 'Array', element };
      }

      case 'Nullable': {
        const inner = parseTypeExpr();
        expect('RPAREN');
        return { kind: 'Nullable', inner };
      }

      case 'LowCardinality': {
        const inner = parseTypeExpr();
        expect('RPAREN');
        return { kind: 'LowCardinality', inner };
      }

      case 'Map': {
        const key = parseTypeExpr();
        expect('COMMA');
        const value = parseTypeExpr();
        expect('RPAREN');
        return { kind: 'Map', key, value };
      }

      case 'Tuple': {
        return parseTuple();
      }

      case 'FixedString': {
        const lenToken = expect('NUMBER') as { type: 'NUMBER'; value: number };
        expect('RPAREN');
        return { kind: 'FixedString', length: lenToken.value };
      }

      case 'Decimal':
      case 'Decimal32':
      case 'Decimal64':
      case 'Decimal128':
      case 'Decimal256': {
        return parseDecimal(typeName);
      }

      case 'DateTime': {
        // Optional timezone
        if (peek()?.type === 'STRING') {
          const tz = (consume() as { type: 'STRING'; value: string }).value;
          expect('RPAREN');
          return { kind: 'DateTime', timezone: tz };
        }
        expect('RPAREN');
        return { kind: 'DateTime' };
      }

      case 'DateTime64': {
        const precision = (expect('NUMBER') as { type: 'NUMBER'; value: number }).value;
        let timezone: string | undefined;
        if (peek()?.type === 'COMMA') {
          consume();
          timezone = (expect('STRING') as { type: 'STRING'; value: string }).value;
        }
        expect('RPAREN');
        return { kind: 'DateTime64', precision, timezone };
      }

      case 'Time64': {
        const precision = (expect('NUMBER') as { type: 'NUMBER'; value: number }).value;
        expect('RPAREN');
        return { kind: 'Time64', precision };
      }

      case 'Enum8':
      case 'Enum16': {
        return parseEnum(typeName as 'Enum8' | 'Enum16');
      }

      case 'Variant': {
        return parseVariant();
      }

      case 'Dynamic': {
        // Optional max_types parameter: Dynamic(max_types=N) or Dynamic(N) or Dynamic
        let maxTypes: number | undefined;

        if (peek()?.type === 'NUMBER') {
          // Simple form: Dynamic(N)
          maxTypes = (consume() as { type: 'NUMBER'; value: number }).value;
        } else if (peek()?.type === 'IDENTIFIER') {
          // Named parameter form: Dynamic(max_types=N)
          const identToken = consume() as { type: 'IDENTIFIER'; value: string };
          if (identToken.value === 'max_types') {
            expect('EQUALS');
            const valueToken = expect('NUMBER') as { type: 'NUMBER'; value: number };
            maxTypes = valueToken.value;
          }
          // Skip unknown parameters silently
        }

        expect('RPAREN');
        return maxTypes !== undefined ? { kind: 'Dynamic', maxTypes } : { kind: 'Dynamic' };
      }

      case 'JSON': {
        // Parse JSON type with optional parameters and typed paths:
        // JSON(max_dynamic_paths=N, path1 Type1, path2 Type2, ...)
        const typedPaths = new Map<string, ClickHouseType>();
        let maxDynamicPaths: number | undefined;

        while (peek()?.type !== 'RPAREN') {
          // Identifier (could be a parameter name or path name)
          const identToken = expect('IDENTIFIER') as { type: 'IDENTIFIER'; value: string };
          const ident = identToken.value;

          // Check if this is a parameter assignment (identifier=value)
          if (peek()?.type === 'EQUALS') {
            consume(); // consume '='
            const valueToken = expect('NUMBER') as { type: 'NUMBER'; value: number };
            if (ident === 'max_dynamic_paths') {
              maxDynamicPaths = valueToken.value;
            }
            // Skip unknown parameters silently
          } else {
            // This is a typed path: path Type
            const pathType = parseTypeExpr();
            typedPaths.set(ident, pathType);
          }

          if (peek()?.type === 'COMMA') {
            consume();
          }
        }
        consume(); // RPAREN

        return {
          kind: 'JSON',
          typedPaths: typedPaths.size > 0 ? typedPaths : undefined,
          maxDynamicPaths,
        };
      }

      case 'Nested': {
        return parseNested();
      }

      case 'QBit': {
        // QBit(element_type, dimension)
        const element = parseTypeExpr();
        expect('COMMA');
        const dimension = (expect('NUMBER') as { type: 'NUMBER'; value: number }).value;
        expect('RPAREN');
        return { kind: 'QBit', element, dimension };
      }

      case 'AggregateFunction': {
        // AggregateFunction(functionName, argType1, argType2, ...)
        const funcNameToken = expect('IDENTIFIER') as { type: 'IDENTIFIER'; value: string };
        const functionName = funcNameToken.value;
        const argTypes: ClickHouseType[] = [];

        while (peek()?.type === 'COMMA') {
          consume(); // COMMA
          argTypes.push(parseTypeExpr());
        }
        expect('RPAREN');
        return { kind: 'AggregateFunction', functionName, argTypes };
      }

      default:
        throw new Error(`Unknown parameterized type: ${typeName}`);
    }
  }

  function parseTuple(): ClickHouseType {
    const elements: ClickHouseType[] = [];
    const names: string[] = [];
    let hasNames = false;

    while (peek()?.type !== 'RPAREN') {
      // Check if this is a named element: `name Type`
      // Look ahead to see if pattern is IDENTIFIER IDENTIFIER or IDENTIFIER LPAREN
      const current = peek();

      if (current?.type === 'IDENTIFIER') {
        const savedPos = pos;
        const firstIdent = (consume() as { type: 'IDENTIFIER'; value: string }).value;
        const next = peek();

        // If next is another identifier or LPAREN (start of type), first was a name
        if (next?.type === 'IDENTIFIER' || next?.type === 'LPAREN') {
          // Check if the first identifier could be a type name
          const mightBeType =
            next?.type === 'LPAREN' ||
            [
              'UInt8',
              'UInt16',
              'UInt32',
              'UInt64',
              'Int8',
              'Int16',
              'Int32',
              'Int64',
              'Float32',
              'Float64',
              'String',
              'Bool',
              'Date',
              'DateTime',
              'UUID',
              'Array',
              'Tuple',
              'Map',
              'Nullable',
            ].includes(firstIdent);

          if (!mightBeType || (next?.type === 'IDENTIFIER' && mightBeType)) {
            // First identifier was a name
            hasNames = true;
            names.push(firstIdent);
            elements.push(parseTypeExpr());
          } else {
            // First identifier was the type itself
            pos = savedPos;
            elements.push(parseTypeExpr());
            if (hasNames) {
              names.push(''); // Anonymous element in named tuple
            }
          }
        } else {
          // It's a simple type (no params, no name before it)
          pos = savedPos;
          elements.push(parseTypeExpr());
          if (hasNames) {
            names.push('');
          }
        }
      } else {
        elements.push(parseTypeExpr());
        if (hasNames) {
          names.push('');
        }
      }

      if (peek()?.type === 'COMMA') {
        consume();
      }
    }
    consume(); // RPAREN

    return { kind: 'Tuple', elements, names: hasNames ? names : undefined };
  }

  function parseEnum(enumKind: 'Enum8' | 'Enum16'): ClickHouseType {
    const values = new Map<number, string>();

    while (peek()?.type !== 'RPAREN') {
      const name = (expect('STRING') as { type: 'STRING'; value: string }).value;
      expect('EQUALS');
      const value = (expect('NUMBER') as { type: 'NUMBER'; value: number }).value;
      values.set(value, name);

      if (peek()?.type === 'COMMA') {
        consume();
      }
    }
    consume(); // RPAREN

    return { kind: enumKind, values };
  }

  function parseVariant(): ClickHouseType {
    const variants: ClickHouseType[] = [];

    while (peek()?.type !== 'RPAREN') {
      variants.push(parseTypeExpr());
      if (peek()?.type === 'COMMA') {
        consume();
      }
    }
    consume(); // RPAREN

    // Important: Variant types are always sorted alphabetically by their string representation
    variants.sort((a, b) => typeToString(a).localeCompare(typeToString(b)));

    return { kind: 'Variant', variants };
  }

  function parseNested(): ClickHouseType {
    const fields: { name: string; type: ClickHouseType }[] = [];

    while (peek()?.type !== 'RPAREN') {
      // Each field is: name Type
      const nameToken = expect('IDENTIFIER') as { type: 'IDENTIFIER'; value: string };
      const fieldType = parseTypeExpr();
      fields.push({ name: nameToken.value, type: fieldType });

      if (peek()?.type === 'COMMA') {
        consume();
      }
    }
    consume(); // RPAREN

    return { kind: 'Nested', fields };
  }

  function parseDecimal(
    typeName: string
  ): ClickHouseType & { kind: 'Decimal32' | 'Decimal64' | 'Decimal128' | 'Decimal256' } {
    const first = (expect('NUMBER') as { type: 'NUMBER'; value: number }).value;
    let precision: number;
    let scale: number;

    if (peek()?.type === 'COMMA') {
      consume();
      scale = (expect('NUMBER') as { type: 'NUMBER'; value: number }).value;
      precision = first;
    } else {
      // Single parameter is scale
      scale = first;
      // Precision is derived from type
      switch (typeName) {
        case 'Decimal':
        case 'Decimal32':
          precision = 9;
          break;
        case 'Decimal64':
          precision = 18;
          break;
        case 'Decimal128':
          precision = 38;
          break;
        case 'Decimal256':
          precision = 76;
          break;
        default:
          precision = 9;
      }
    }

    expect('RPAREN');

    // Map 'Decimal' to appropriate width based on precision
    let kind: 'Decimal32' | 'Decimal64' | 'Decimal128' | 'Decimal256';
    if (typeName === 'Decimal') {
      if (precision <= 9) kind = 'Decimal32';
      else if (precision <= 18) kind = 'Decimal64';
      else if (precision <= 38) kind = 'Decimal128';
      else kind = 'Decimal256';
    } else {
      kind = typeName as 'Decimal32' | 'Decimal64' | 'Decimal128' | 'Decimal256';
    }

    return { kind, precision, scale };
  }

  const result = parseTypeExpr();

  // Ensure all tokens consumed
  if (pos < tokens.length) {
    throw new Error(`Unexpected tokens after type: ${typeString}`);
  }

  return result;
}
