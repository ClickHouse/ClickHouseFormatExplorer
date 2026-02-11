# CLAUDE.md - ClickHouse Format Explorer

## Project Overview

A tool for visualizing ClickHouse RowBinary and Native wire format data. Features an interactive hex viewer with AST-based type visualization, similar to ImHex. Available as a web app (Docker) or an Electron desktop app that connects to your existing ClickHouse server.

**Current scope**: RowBinaryWithNamesAndTypes and Native formats.

## Tech Stack

- **Frontend**: React 18 + TypeScript + Vite
- **State**: Zustand
- **UI**: react-window (virtualized hex viewer), react-resizable-panels (split panes)
- **Desktop**: Electron (optional, connects to user's ClickHouse)
- **Testing**: Vitest + testcontainers (integration), Playwright (Electron e2e)
- **Deployment**: Docker (bundles ClickHouse + nginx) or Electron desktop app

## Commands

```bash
npm run dev             # Start web dev server (requires ClickHouse at localhost:8123)
npm run build           # Build web app for production
npm run test            # Run integration tests (uses testcontainers)
npm run lint            # ESLint check
npm run test:e2e        # Build Electron + run Playwright e2e tests

# Electron desktop app
npm run electron:dev    # Dev mode with hot reload
npm run electron:build  # Package desktop installer for current platform

# Docker (self-contained with bundled ClickHouse)
docker build -t rowbinary-explorer .
docker run -d -p 8080:80 rowbinary-explorer
```

## Directory Structure

```
src/
├── components/           # React components
│   ├── App.tsx           # Main layout with resizable panels
│   ├── QueryInput.tsx    # SQL query input + run button + connection settings
│   ├── HexViewer/        # Virtualized hex viewer with highlighting
│   └── AstTree/          # Collapsible AST tree view
├── core/
│   ├── types/
│   │   ├── ast.ts                # AstNode, ByteRange, ParsedData interfaces
│   │   └── clickhouse-types.ts   # ClickHouseType discriminated union
│   ├── decoder/
│   │   ├── rowbinary-decoder.ts  # RowBinaryWithNamesAndTypes decoder
│   │   ├── native-decoder.ts     # Native format decoder
│   │   ├── reader.ts             # BinaryReader with byte-range tracking
│   │   ├── leb128.ts             # LEB128 varint decoder
│   │   ├── test-helpers.ts       # Shared test utilities
│   │   ├── smoke-cases.ts        # Smoke test case definitions
│   │   └── validation-cases.ts   # Validation test case definitions
│   ├── parser/
│   │   ├── type-lexer.ts   # Tokenizer for type strings
│   │   └── type-parser.ts  # Parser: string -> ClickHouseType
│   └── clickhouse/
│       └── client.ts     # HTTP client (fetch for web, IPC for Electron)
├── store/
│   └── store.ts          # Zustand store (query, parsed data, UI state)
└── styles/               # CSS files
electron/
├── main.ts               # Electron main process (window, IPC handlers)
└── preload.ts            # Preload script (contextBridge → electronAPI)
e2e/
└── electron.spec.ts      # Playwright Electron e2e tests
docs/
├── rowbinaryspec.md      # RowBinary wire format specification
├── nativespec.md         # Native wire format specification
└── jsonspec.md           # JSON type specification
docker/
├── nginx.conf            # Proxies /clickhouse to ClickHouse server
├── users.xml             # Read-only ClickHouse user
└── supervisord.conf      # Runs nginx + ClickHouse together
```

## Wire Format Docs

 * RowBinary: docs/rowbinaryspec.md
 * Native: docs/nativespec.md
 * JSON: docs/jsonspec.md

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
- Intervals: `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear` (stored as Int64)
- Other: `Enum8/16`, `Nested`, `QBit`, `AggregateFunction`

### Decoding Flow
1. User enters SQL query, clicks "Run Query"
2. `ClickHouseClient` (`src/core/clickhouse/client.ts`) sends query:
   - **Web mode**: `fetch()` via Vite proxy or nginx
   - **Electron mode**: IPC to main process → `fetch()` to ClickHouse (no CORS)
3. Decoder parses the binary response:
   - **RowBinary** (`rowbinary-decoder.ts`): Row-oriented, header + rows
   - **Native** (`native-decoder.ts`): Column-oriented with blocks
4. Type strings parsed via `parseType()` into `ClickHouseType`
5. Each decoded value returns an `AstNode` with byte tracking
6. UI renders hex view (left) and AST tree (right)

### Electron Architecture
```
Renderer (React)               Main Process (Node.js)
  │                                │
  ├─ window.electronAPI            │
  │   .executeQuery(opts) ────────►├─ fetch(clickhouseUrl + query)
  │                                │   → ArrayBuffer
  │◄── IPC response ──────────────┤
  │                                │
  ├─ Uint8Array → decoders         │
  └─ render hex view + AST tree    │
```

- Runtime detection: `window.electronAPI` exists → IPC path, otherwise → `fetch()`
- `vite-plugin-electron` activates only when `ELECTRON=true` env var is set
- Connection config in `config.json` (project root in dev, next to executable in prod)
- Experimental ClickHouse settings (Variant, Dynamic, JSON, etc.) sent as query params

### Interactive Highlighting
- Click a node in AST tree → highlights corresponding bytes in hex view
- Click a byte in hex view → selects the deepest AST node containing that byte
- State managed in Zustand store: `activeNodeId`, `hoveredNodeId`

## Adding a New ClickHouse Type

1. Add type variant to `ClickHouseType` in `src/core/types/clickhouse-types.ts`
2. Add `typeToString()` case for serialization back to string
3. Add `getTypeColor()` case for UI coloring
4. Add parser case in `src/core/parser/type-parser.ts`
5. Add decoder method in `RowBinaryDecoder` (`src/core/decoder/rowbinary-decoder.ts`):
   - Add case in `decodeValue()` switch
   - Implement `decode{TypeName}()` method returning `AstNode`
6. Add decoder method in `NativeDecoder` (`src/core/decoder/native-decoder.ts`):
   - Add case in `decodeValue()` switch
   - For columnar types, may need `decode{TypeName}Column()` method
7. If type has binary type index (for Dynamic), add to `decodeDynamicType()`
8. Add test cases to `smoke-cases.ts` and `validation-cases.ts`

## Important Implementation Details

- **LEB128**: Variable-length integers used for string lengths, array sizes, column counts
- **UUID byte order**: ClickHouse uses a special byte ordering (see `decodeUUID()` at `decoder.ts:629`)
- **IPv4**: Stored as little-endian UInt32, displayed in reverse order
- **Dynamic type**: Uses BinaryTypeIndex encoding; type is encoded in the data itself
- **LowCardinality**: Does not affect wire format in RowBinary (transparent wrapper)
- **Nested**: Encoded as parallel arrays, one per field

## Testing

### Integration Tests (Vitest + testcontainers)

Tests use testcontainers to spin up a real ClickHouse instance:
```bash
npm run test  # Runs all integration tests
```

Tests are organized into three categories with shared test case definitions:

1. **Smoke Tests** (`smoke.integration.test.ts`)
   - Verify parsing succeeds without value validation
   - Test cases defined in `smoke-cases.ts`
   - Parametrized for both RowBinary and Native formats

2. **Validation Tests** (`validation.integration.test.ts`)
   - Verify decoded values and AST structure
   - Test cases defined in `validation-cases.ts` with format-specific callbacks
   - Check values, children counts, byte ranges, metadata

3. **Coverage Tests** (`coverage.integration.test.ts`)
   - Analyze byte coverage of AST leaf nodes
   - Report uncovered byte ranges

### Electron e2e Tests (Playwright)

```bash
npm run test:e2e  # Builds Electron app + runs Playwright tests
```

Tests in `e2e/electron.spec.ts` launch the actual Electron app and verify:
- App window opens and UI renders
- Host input is visible (Electron mode) and Share button is hidden
- Connection settings can be edited
- Upload button is present and functional

### Test Case Interface
```typescript
interface ValidationTestCase {
  name: string;
  query: string;
  settings?: Record<string, string | number>;
  rowBinaryValidator?: (result: DecodedResult) => void;
  nativeValidator?: (result: DecodedResult) => void;
}
```

### Adding New Test Cases
1. Add query to `smoke-cases.ts` for basic parsing verification
2. Add to `validation-cases.ts` with validator callbacks for detailed checks
3. Use `bothFormats(validator)` helper when validation logic is identical
