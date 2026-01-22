import { create } from 'zustand';
import { clickhouse, DEFAULT_QUERY } from '../core/clickhouse/client';
import { RowBinaryDecoder } from '../core/decoder/decoder';
import { AstNode, ParsedData } from '../core/types/ast';

interface AppState {
  // Query
  query: string;
  setQuery: (query: string) => void;

  // Data
  rawData: Uint8Array | null;
  parsedData: ParsedData | null;
  parseError: Error | null;
  isLoading: boolean;
  queryTiming: number | null;

  // Actions
  executeQuery: () => Promise<void>;
  loadSampleData: () => void;

  // UI state
  activeNodeId: string | null;
  hoveredNodeId: string | null;
  expandedNodes: Set<string>;

  // UI actions
  setActiveNode: (id: string | null) => void;
  setHoveredNode: (id: string | null) => void;
  toggleExpanded: (id: string) => void;
  expandAll: () => void;
  collapseAll: () => void;
}

/**
 * Collect all node IDs from parsed data (for expandAll)
 */
function collectAllNodeIds(parsedData: ParsedData): string[] {
  const ids: string[] = [];

  function visitNode(node: AstNode) {
    ids.push(node.id);
    node.children?.forEach(visitNode);
  }

  parsedData.rows.forEach((row, i) => {
    ids.push(`row-${i}`);
    row.values.forEach(visitNode);
  });

  return ids;
}

/**
 * Get the default expanded nodes (just rows)
 */
function getDefaultExpanded(parsedData: ParsedData): Set<string> {
  const expanded = new Set<string>();
  parsedData.rows.forEach((_, i) => {
    expanded.add(`row-${i}`);
  });
  return expanded;
}

export const useStore = create<AppState>((set, get) => ({
  // Initial state
  query: DEFAULT_QUERY,
  rawData: null,
  parsedData: null,
  parseError: null,
  isLoading: false,
  queryTiming: null,
  activeNodeId: null,
  hoveredNodeId: null,
  expandedNodes: new Set(),

  setQuery: (query) => set({ query }),

  executeQuery: async () => {
    const { query } = get();
    set({ isLoading: true, parseError: null, queryTiming: null });

    try {
      const { data, timing } = await clickhouse.query({ query });

      const decoder = new RowBinaryDecoder(data);
      const parsed = decoder.decode();

      set({
        rawData: data,
        parsedData: parsed,
        isLoading: false,
        queryTiming: timing,
        expandedNodes: getDefaultExpanded(parsed),
        activeNodeId: null,
        hoveredNodeId: null,
      });
    } catch (error) {
      set({
        parseError: error as Error,
        isLoading: false,
        rawData: null,
        parsedData: null,
      });
    }
  },

  loadSampleData: () => {
    // Sample data for testing without ClickHouse
    // This is the RBWNAT encoding of: SELECT 42 :: UInt32 AS num, 'hello' AS str
    const sampleData = new Uint8Array([
      // Header: 2 columns
      0x02,
      // Column names
      0x03, 0x6e, 0x75, 0x6d, // "num"
      0x03, 0x73, 0x74, 0x72, // "str"
      // Column types
      0x06, 0x55, 0x49, 0x6e, 0x74, 0x33, 0x32, // "UInt32"
      0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, // "String"
      // Row 1 data
      0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
      0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // "hello"
    ]);

    try {
      const decoder = new RowBinaryDecoder(sampleData);
      const parsed = decoder.decode();

      set({
        rawData: sampleData,
        parsedData: parsed,
        isLoading: false,
        parseError: null,
        queryTiming: null,
        expandedNodes: getDefaultExpanded(parsed),
        activeNodeId: null,
        hoveredNodeId: null,
      });
    } catch (error) {
      set({
        parseError: error as Error,
        isLoading: false,
      });
    }
  },

  setActiveNode: (id) => set({ activeNodeId: id }),
  setHoveredNode: (id) => set({ hoveredNodeId: id }),

  toggleExpanded: (id) =>
    set((state) => {
      const newExpanded = new Set(state.expandedNodes);
      if (newExpanded.has(id)) {
        newExpanded.delete(id);
      } else {
        newExpanded.add(id);
      }
      return { expandedNodes: newExpanded };
    }),

  expandAll: () =>
    set((state) => {
      if (!state.parsedData) return state;
      const allIds = collectAllNodeIds(state.parsedData);
      return { expandedNodes: new Set(allIds) };
    }),

  collapseAll: () => set({ expandedNodes: new Set() }),
}));
