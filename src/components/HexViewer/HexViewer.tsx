import { useMemo, useCallback } from 'react';
import { FixedSizeList as List } from 'react-window';
import { useStore } from '../../store/store';
import { AstNode } from '../../core/types/ast';
import '../../styles/hex-viewer.css';

const BYTES_PER_ROW = 16;

interface HighlightInfo {
  color: string;
  isActive: boolean;
  isHovered: boolean;
}

/**
 * Build a map from byte offset to highlight info
 */
function buildHighlightMap(
  parsedData: ReturnType<typeof useStore.getState>['parsedData'],
  activeNodeId: string | null,
  hoveredNodeId: string | null
): Map<number, HighlightInfo> {
  const map = new Map<number, HighlightInfo>();
  if (!parsedData) return map;

  function visitNode(node: AstNode, depth: number) {
    const isActive = node.id === activeNodeId;
    const isHovered = node.id === hoveredNodeId;

    if (isActive || isHovered) {
      // Generate a color based on type
      const color = getTypeColor(node.type);

      for (let i = node.byteRange.start; i < node.byteRange.end; i++) {
        const existing = map.get(i);
        // Prioritize active over hovered, and deeper nodes over shallow
        if (!existing || isActive || (isHovered && !existing.isActive)) {
          map.set(i, { color, isActive, isHovered });
        }
      }
    }

    node.children?.forEach((child) => visitNode(child, depth + 1));
  }

  parsedData.rows.forEach((row) => {
    row.values.forEach((node) => visitNode(node, 0));
  });

  return map;
}

function getTypeColor(typeName: string): string {
  const baseType = typeName.split('(')[0];
  const colors: Record<string, string> = {
    UInt8: '#4fc3f7',
    UInt16: '#4fc3f7',
    UInt32: '#4fc3f7',
    UInt64: '#4fc3f7',
    Int8: '#4fc3f7',
    Int16: '#4fc3f7',
    Int32: '#4fc3f7',
    Int64: '#4fc3f7',
    Float32: '#81c784',
    Float64: '#81c784',
    String: '#ffb74d',
    Array: '#7986cb',
    Tuple: '#4db6ac',
    Map: '#f06292',
    Nullable: '#90a4ae',
    Bool: '#aed581',
    BinaryTypeIndex: '#ce93d8',
  };
  return colors[baseType] || '#9e9e9e';
}

interface HexRowProps {
  index: number;
  style: React.CSSProperties;
  data: {
    bytes: Uint8Array;
    highlightMap: Map<number, HighlightInfo>;
    onByteClick: (offset: number) => void;
  };
}

function HexRow({ index, style, data }: HexRowProps) {
  const { bytes, highlightMap, onByteClick } = data;
  const startOffset = index * BYTES_PER_ROW;
  const rowBytes = bytes.slice(startOffset, startOffset + BYTES_PER_ROW);

  const formatAddress = (offset: number) => {
    return offset.toString(16).padStart(8, '0').toUpperCase();
  };

  const formatByte = (byte: number) => {
    return byte.toString(16).padStart(2, '0').toUpperCase();
  };

  const isPrintable = (byte: number) => {
    return byte >= 0x20 && byte < 0x7f;
  };

  return (
    <div className="hex-row" style={style}>
      <div className="hex-address">{formatAddress(startOffset)}</div>
      <div className="hex-bytes">
        {Array.from(rowBytes).map((byte, i) => {
          const offset = startOffset + i;
          const highlight = highlightMap.get(offset);
          const classNames = ['hex-byte'];
          if (highlight?.isActive) classNames.push('active', 'highlighted');
          else if (highlight?.isHovered) classNames.push('hovered', 'highlighted');
          if ((i + 1) % 8 === 0 && i < BYTES_PER_ROW - 1) classNames.push('group-end');

          return (
            <span
              key={i}
              className={classNames.join(' ')}
              style={highlight ? { '--highlight-color': highlight.color } as React.CSSProperties : undefined}
              onClick={() => onByteClick(offset)}
            >
              {formatByte(byte)}
            </span>
          );
        })}
        {/* Pad with empty cells if row is not full */}
        {rowBytes.length < BYTES_PER_ROW &&
          Array.from({ length: BYTES_PER_ROW - rowBytes.length }).map((_, i) => (
            <span key={`pad-${i}`} className="hex-byte" style={{ visibility: 'hidden' }}>
              00
            </span>
          ))}
      </div>
      <div className="hex-ascii">
        {Array.from(rowBytes).map((byte, i) => {
          const offset = startOffset + i;
          const highlight = highlightMap.get(offset);
          const char = isPrintable(byte) ? String.fromCharCode(byte) : '.';
          const classNames = ['hex-ascii-char'];
          if (!isPrintable(byte)) classNames.push('non-printable');
          if (highlight?.isActive) classNames.push('active', 'highlighted');
          else if (highlight?.isHovered) classNames.push('highlighted');

          return (
            <span
              key={i}
              className={classNames.join(' ')}
              style={highlight ? { '--highlight-color': highlight.color } as React.CSSProperties : undefined}
            >
              {char}
            </span>
          );
        })}
      </div>
    </div>
  );
}

export function HexViewer() {
  const rawData = useStore((s) => s.rawData);
  const parsedData = useStore((s) => s.parsedData);
  const activeNodeId = useStore((s) => s.activeNodeId);
  const hoveredNodeId = useStore((s) => s.hoveredNodeId);
  const setActiveNode = useStore((s) => s.setActiveNode);

  const highlightMap = useMemo(
    () => buildHighlightMap(parsedData, activeNodeId, hoveredNodeId),
    [parsedData, activeNodeId, hoveredNodeId]
  );

  const handleByteClick = useCallback(
    (offset: number) => {
      // Find the deepest node containing this byte
      if (!parsedData) return;

      let deepestNode: AstNode | null = null;
      let deepestDepth = -1;

      function visitNode(node: AstNode, depth: number) {
        if (offset >= node.byteRange.start && offset < node.byteRange.end) {
          if (depth > deepestDepth) {
            deepestNode = node;
            deepestDepth = depth;
          }
        }
        node.children?.forEach((child) => visitNode(child, depth + 1));
      }

      parsedData.rows.forEach((row) => {
        row.values.forEach((node) => visitNode(node, 0));
      });

      if (deepestNode) {
        setActiveNode((deepestNode as AstNode).id);
      }
    },
    [parsedData, setActiveNode]
  );

  if (!rawData) {
    return (
      <div className="hex-viewer">
        <div className="hex-viewer-empty">No data loaded</div>
      </div>
    );
  }

  const rowCount = Math.ceil(rawData.length / BYTES_PER_ROW);

  return (
    <div className="hex-viewer">
      <List
        height={window.innerHeight - 150} // Approximate, will be resized by parent
        itemCount={rowCount}
        itemSize={22}
        width="100%"
        itemData={{
          bytes: rawData,
          highlightMap,
          onByteClick: handleByteClick,
        }}
        style={{ height: '100%' }}
      >
        {HexRow}
      </List>
    </div>
  );
}

export default HexViewer;
