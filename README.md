# ClickHouse Format Explorer

A tool for visualizing ClickHouse RowBinary and Native format data. Features an interactive hex viewer with AST-based type visualization. Available as a web app or Electron desktop app.

![Screenshot](.static/screenshot.png)

## Features

- **Format support**: RowBinary and Native, modular system allows adding more
- **Hex Viewer**: Virtual-scrolling hex display with ASCII column
- **AST Tree**: Collapsible tree view showing decoded structure
- **Interactive Highlighting**: Selecting a node in the tree highlights corresponding bytes in the hex view (and vice versa)
- **Full Type Support**: All ClickHouse types including Variant, Dynamic, JSON, Geo types, Nested, etc.
- **Desktop App**: Electron app that connects to your existing ClickHouse server (no bundled DB)

## Quick Start (Docker)

Run with bundled ClickHouse server:

```bash
docker build -t rowbinary-explorer .
docker run -d -p 8080:80 rowbinary-explorer
```

Open http://localhost:8080

## Desktop App

For developers who already run ClickHouse locally. Download the latest release for your platform from the [Releases](../../releases) page:

| Platform | Format |
|----------|--------|
| Windows  | `.exe` (NSIS installer) |
| macOS    | `.dmg` |
| Linux    | `.AppImage` / `.deb` |

### Configuration

The app looks for a `config.json` file next to the executable:

```json
{
  "host": "http://localhost:8123"
}
```

You can also change the host from the **Host** field in the toolbar. Changes are saved back to `config.json`.

### Building from source

```bash
npm install
npm run electron:dev    # Dev mode with hot reload
npm run electron:build  # Package installer for current platform
```

## Web Development Setup

For local web development (requires ClickHouse at `localhost:8123`):

```bash
npm install
npm run dev
```

Open http://localhost:5173

## Usage

1. Enter a SQL query in the input box
2. Click "Execute" to fetch data from ClickHouse
3. Explore the parsed data:
   - Click nodes in the AST tree to highlight bytes
   - Click bytes in the hex viewer to select the corresponding node
   - Use "Expand All" / "Collapse All" to navigate complex structures

## Example Queries

```sql
-- Basic types
SELECT 42::UInt32, 'hello'::String, [1,2,3]::Array(UInt8)

-- Complex nested structures
SELECT (1, 'foo', [1,2,3])::Tuple(id UInt32, name String, values Array(UInt8))

-- Dynamic/JSON types
SELECT '{"a": 1, "b": "hello"}'::JSON
SELECT 42::Dynamic

-- With typed JSON paths
SELECT '{"user": {"id": 123}}'::JSON(`user.id` UInt32)
```

## Tech Stack

- React + TypeScript + Vite
- Zustand (state management)
- react-window (virtualized hex viewer)
- react-resizable-panels (split pane layout)
- Electron (desktop app, optional)
- Playwright (e2e testing)
