# CLAUDE.md - RowBinary Visualizer

## Project Overview

A web-based tool for visualizing ClickHouse RowBinary wire format data. Features an interactive hex viewer with AST-based type visualization, similar to ImHex. The tool queries a local ClickHouse database and presents the raw binary data alongside a decoded AST tree with bidirectional highlighting.

**Current scope**: RowBinaryWithNamesAndTypes format only, with plans for expansion to other ClickHouse wire formats.

## Tech Stack

- **Frontend**: React 18 + TypeScript + Vite
- **State**: Zustand
- **UI**: react-window (virtualized hex viewer), react-resizable-panels (split panes)
- **Testing**: Vitest + testcontainers (ClickHouse integration tests)
- **Deployment**: Docker (bundles ClickHouse + nginx + built frontend)

## Commands

```bash
npm run dev       # Start dev server (requires ClickHouse at localhost:8123)
npm run build     # Build for production
npm run test      # Run integration tests (uses testcontainers)
npm run lint      # ESLint check

# Docker (self-contained with bundled ClickHouse)
docker build -t rowbinary-explorer .
docker run -d -p 8080:80 rowbinary-explorer
```

## Directory Structure

```
src/
├── components/           # React components
│   ├── App.tsx           # Main layout with resizable panels
│   ├── QueryInput.tsx    # SQL query input + run button
│   ├── HexViewer/        # Virtualized hex viewer with highlighting
│   └── AstTree/          # Collapsible AST tree view
├── core/
│   ├── types/
│   │   ├── ast.ts                # AstNode, ByteRange, ParsedData interfaces
│   │   └── clickhouse-types.ts   # ClickHouseType discriminated union
│   ├── decoder/
│   │   ├── decoder.ts    # Main RowBinaryDecoder - decodes all types
│   │   ├── reader.ts     # BinaryReader with byte-range tracking
│   │   └── leb128.ts     # LEB128 varint decoder
│   ├── parser/
│   │   ├── type-lexer.ts   # Tokenizer for type strings
│   │   └── type-parser.ts  # Parser: string -> ClickHouseType
│   └── clickhouse/
│       └── client.ts     # HTTP client for ClickHouse queries
├── store/
│   └── store.ts          # Zustand store (query, parsed data, UI state)
└── styles/               # CSS files
docker/
├── nginx.conf            # Proxies /clickhouse to ClickHouse server
├── users.xml             # Read-only ClickHouse user
└── supervisord.conf      # Runs nginx + ClickHouse together
```

## Wire Format Docs

 * RowBinary: rowbinaryspec.md
 * Native: nativespec.md

## Key Concepts

### AstNode
Every decoded value is represented as an `AstNode` (`src/core/types/ast.ts:12`):
- `id` - Unique identifier for selection/highlighting
- `type` - ClickHouse type name string
- `byteRange` - `{start, end}` byte offsets (exclusive end)
- `value` - Decoded JavaScript value
- `displayValue` - Human-readable string
- `children` - Child nodes for composite types (Array, Tuple, etc.)

### ClickHouseType
A discriminated union representing all ClickHouse types (`src/core/types/clickhouse-types.ts:4`):
- Primitives: `UInt8`-`UInt256`, `Int8`-`Int256`, `Float32/64`, `String`, etc.
- Composites: `Array`, `Tuple`, `Map`, `Nullable`, `LowCardinality`
- Advanced: `Variant`, `Dynamic`, `JSON`
- Geo: `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString`, `MultiLineString`, `Geometry`
- Other: `Enum8/16`, `Nested`, `QBit`

### Decoding Flow
1. User enters SQL query, clicks "Run Query"
2. `ClickHouseClient` (`src/core/clickhouse/client.ts`) POSTs query with `default_format=RowBinaryWithNamesAndTypes`
3. `RowBinaryDecoder` (`src/core/decoder/decoder.ts:10`) decodes:
   - Header: column count (LEB128), column names, column types
   - Type strings parsed via `parseType()` into `ClickHouseType`
   - Rows: for each row, decode each column value based on its type
4. Each decoded value returns an `AstNode` with byte tracking
5. UI renders hex view (left) and AST tree (right)

### Interactive Highlighting
- Click a node in AST tree → highlights corresponding bytes in hex view
- Click a byte in hex view → selects the deepest AST node containing that byte
- State managed in Zustand store: `activeNodeId`, `hoveredNodeId`

## Adding a New ClickHouse Type

1. Add type variant to `ClickHouseType` in `src/core/types/clickhouse-types.ts`
2. Add `typeToString()` case for serialization back to string
3. Add `getTypeColor()` case for UI coloring
4. Add parser case in `src/core/parser/type-parser.ts`
5. Add decoder method in `RowBinaryDecoder` (`src/core/decoder/decoder.ts`):
   - Add case in `decodeValue()` switch
   - Implement `decode{TypeName}()` method returning `AstNode`
6. If type has binary type index (for Dynamic), add to `decodeDynamicType()`

## Important Implementation Details

- **LEB128**: Variable-length integers used for string lengths, array sizes, column counts
- **UUID byte order**: ClickHouse uses a special byte ordering (see `decodeUUID()` at `decoder.ts:629`)
- **IPv4**: Stored as little-endian UInt32, displayed in reverse order
- **Dynamic type**: Uses BinaryTypeIndex encoding; type is encoded in the data itself
- **LowCardinality**: Does not affect wire format in RowBinary (transparent wrapper)
- **Nested**: Encoded as parallel arrays, one per field

## Testing

Integration tests use testcontainers to spin up a real ClickHouse instance:
```bash
npm run test  # Runs src/core/decoder/decoder.integration.test.ts
```

Tests verify decoding of various type combinations against actual ClickHouse output.
