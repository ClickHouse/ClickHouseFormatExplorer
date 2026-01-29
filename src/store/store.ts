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
  activeCopyText: string | null;
  hoveredNodeId: string | null;
  expandedNodes: Set<string>;
  scrollRequest: { byteOffset: number; id: number } | null;

  // UI actions
  setActiveNode: (id: string | null, copyText?: string | null) => void;
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

/** State to clear all data before loading new data */
const getLoadingState = () => ({
  isLoading: true,
  parseError: null,
  queryTiming: null,
  rawData: null,
  parsedData: null,
  activeNodeId: null,
  activeCopyText: null,
  hoveredNodeId: null,
  expandedNodes: new Set<string>(),
  scrollRequest: null,
});

/** State after successful data load */
const getSuccessState = (data: Uint8Array, parsed: ParsedData, timing: number | null) => ({
  rawData: data,
  parsedData: parsed,
  isLoading: false,
  parseError: null,
  queryTiming: timing,
  expandedNodes: getDefaultExpanded(parsed),
});

/** State after failed data load */
const getErrorState = (error: Error) => ({
  parseError: error,
  isLoading: false,
  rawData: null,
  parsedData: null,
});

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
  activeCopyText: null,
  hoveredNodeId: null,
  expandedNodes: new Set(),
  scrollRequest: null,

  setQuery: (query) => set({ query }),
  setFormat: (format) => set({ format }),

  executeQuery: async () => {
    const { query, format } = get();
    set(getLoadingState());

    try {
      const { data, timing } = await clickhouse.query({ query, format });
      const decoder = createDecoder(data, format);
      const parsed = decoder.decode();
      set(getSuccessState(data, parsed, timing));
    } catch (error) {
      console.error('Query execution failed:', error);
      set(getErrorState(error as Error));
    }
  },

  loadFile: async (file: File) => {
    const { format } = get();
    set(getLoadingState());

    try {
      const arrayBuffer = await file.arrayBuffer();
      const data = new Uint8Array(arrayBuffer);
      const decoder = createDecoder(data, format);
      const parsed = decoder.decode();
      set(getSuccessState(data, parsed, null));
    } catch (error) {
      console.error('File load failed:', error);
      set(getErrorState(error as Error));
    }
  },

  setActiveNode: (id, copyText) => set({ activeNodeId: id, activeCopyText: copyText ?? null }),
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

  scrollToHex: (byteOffset) => {
    // Use a unique ID for each scroll request to ensure the effect always fires
    set({ scrollRequest: { byteOffset, id: Date.now() } });
  },
  clearScrollTarget: () => set({ scrollRequest: null }),
}));
