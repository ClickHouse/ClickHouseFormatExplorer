# RowBinary Visualizer

A web-based tool for visualizing ClickHouse RowBinary data, similar to ImHex. Features an interactive hex viewer with AST-based type visualization.

![Screenshot](.static/screenshot.png)

## Features

- **Hex Viewer**: Virtual-scrolling hex display with ASCII column
- **AST Tree**: Collapsible tree view showing decoded structure
- **Interactive Highlighting**: Selecting a node in the tree highlights corresponding bytes in the hex view (and vice versa)
- **Full Type Support**: All ClickHouse types including Variant, Dynamic, JSON, Geo types, Nested, etc.

## Setup

```bash
npm install
npm run dev
```

Open http://localhost:5173

## Usage

1. Enter a SQL query in the input box
2. Click "Run Query" to fetch data from ClickHouse (expects local instance at `localhost:8123`)
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
