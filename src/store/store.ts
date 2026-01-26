import { create } from 'zustand';
import { clickhouse, DEFAULT_QUERY } from '../core/clickhouse/client';
import { createDecoder } from '../core/decoder';
import { AstNode, ParsedData } from '../core/types/ast';
import { ClickHouseFormat } from '../core/types/formats';

interface AppState {
  // Query
  query: string;
  setQuery: (query: string) => void;

  // Format
  format: ClickHouseFormat;
  setFormat: (format: ClickHouseFormat) => void;

  // Data
  rawData: Uint8Array | null;
  parsedData: ParsedData | null;
  parseError: Error | null;
  isLoading: boolean;
  queryTiming: number | null;

  // Actions
  executeQuery: () => Promise<void>;
  loadFile: (file: File) => Promise<void>;

  // UI state
  activeNodeId: string | null;
  hoveredNodeId: string | null;
  expandedNodes: Set<string>;
  scrollToByteOffset: number | null;

  // UI actions
  setActiveNode: (id: string | null) => void;
  setHoveredNode: (id: string | null) => void;
  toggleExpanded: (id: string) => void;
  expandAll: () => void;
  collapseAll: () => void;
  scrollToHex: (byteOffset: number) => void;
  clearScrollTarget: () => void;
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

  // Row-based formats (RowBinary)
  parsedData.rows?.forEach((row, i) => {
    ids.push(`row-${i}`);
    row.values.forEach(visitNode);
  });

  // Block-based formats (Native)
  parsedData.blocks?.forEach((block, i) => {
    ids.push(`block-${i}`);
    block.columns.forEach((col, j) => {
      ids.push(`block-${i}-col-${j}`);
      col.values.forEach(visitNode);
    });
  });

  return ids;
}

/**
 * Get the default expanded nodes (rows or blocks)
 */
function getDefaultExpanded(parsedData: ParsedData): Set<string> {
  const expanded = new Set<string>();
  // Row-based formats (RowBinary)
  parsedData.rows?.forEach((_, i) => {
    expanded.add(`row-${i}`);
  });
  // Block-based formats (Native)
  parsedData.blocks?.forEach((_, i) => {
    expanded.add(`block-${i}`);
  });
  return expanded;
}

export const useStore = create<AppState>((set, get) => ({
  // Initial state
  query: DEFAULT_QUERY,
  format: ClickHouseFormat.RowBinaryWithNamesAndTypes,
  rawData: null,
  parsedData: null,
  parseError: null,
  isLoading: false,
  queryTiming: null,
  activeNodeId: null,
  hoveredNodeId: null,
  expandedNodes: new Set(),
  scrollToByteOffset: null,

  setQuery: (query) => set({ query }),
  setFormat: (format) => set({ format }),

  executeQuery: async () => {
    const { query, format } = get();
    set({ isLoading: true, parseError: null, queryTiming: null });

    try {
      const { data, timing } = await clickhouse.query({ query, format });

      const decoder = createDecoder(data, format);
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

  loadFile: async (file: File) => {
    const { format } = get();
    set({ isLoading: true, parseError: null, queryTiming: null });

    try {
      const arrayBuffer = await file.arrayBuffer();
      const data = new Uint8Array(arrayBuffer);

      const decoder = createDecoder(data, format);
      const parsed = decoder.decode();

      set({
        rawData: data,
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
        rawData: null,
        parsedData: null,
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

  scrollToHex: (byteOffset) => set({ scrollToByteOffset: byteOffset }),
  clearScrollTarget: () => set({ scrollToByteOffset: null }),
}));
