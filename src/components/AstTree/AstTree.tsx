import { memo, useCallback } from 'react';
import { useStore } from '../../store/store';
import { AstNode } from '../../core/types/ast';
import '../../styles/ast-tree.css';

function getTypeColor(typeName: string): string {
  const baseType = typeName.split('(')[0];
  const colors: Record<string, string> = {
    UInt8: 'var(--type-int)',
    UInt16: 'var(--type-int)',
    UInt32: 'var(--type-int)',
    UInt64: 'var(--type-int)',
    UInt128: 'var(--type-int)',
    UInt256: 'var(--type-int)',
    Int8: 'var(--type-int)',
    Int16: 'var(--type-int)',
    Int32: 'var(--type-int)',
    Int64: 'var(--type-int)',
    Int128: 'var(--type-int)',
    Int256: 'var(--type-int)',
    Float32: 'var(--type-float)',
    Float64: 'var(--type-float)',
    BFloat16: 'var(--type-float)',
    Decimal32: 'var(--type-float)',
    Decimal64: 'var(--type-float)',
    Decimal128: 'var(--type-float)',
    Decimal256: 'var(--type-float)',
    String: 'var(--type-string)',
    FixedString: 'var(--type-string)',
    Date: 'var(--type-date)',
    Date32: 'var(--type-date)',
    DateTime: 'var(--type-date)',
    DateTime64: 'var(--type-date)',
    Time: 'var(--type-date)',
    Time64: 'var(--type-date)',
    Array: 'var(--type-array)',
    Tuple: 'var(--type-tuple)',
    Map: 'var(--type-map)',
    Nullable: 'var(--type-nullable)',
    UUID: 'var(--type-special)',
    IPv4: 'var(--type-special)',
    IPv6: 'var(--type-special)',
    Enum8: 'var(--type-enum)',
    Enum16: 'var(--type-enum)',
    Bool: 'var(--type-bool)',
    BinaryTypeIndex: 'var(--type-special)',
  };
  return colors[baseType] || 'var(--type-default)';
}

function getValueClass(value: unknown, displayValue: string): string {
  if (value === null || displayValue === 'NULL') return 'null';
  if (typeof value === 'string' || displayValue.startsWith('"')) return 'string';
  if (typeof value === 'number' || typeof value === 'bigint') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  return '';
}

function getShortType(type: string): string {
  // Shorten long type strings
  if (type.length > 20) {
    const base = type.split('(')[0];
    return base + '(...)';
  }
  return type;
}

interface AstNodeItemProps {
  node: AstNode;
  depth: number;
  columnName?: string;
}

const AstNodeItem = memo(function AstNodeItem({ node, depth, columnName }: AstNodeItemProps) {
  const activeNodeId = useStore((s) => s.activeNodeId);
  const hoveredNodeId = useStore((s) => s.hoveredNodeId);
  const expandedNodes = useStore((s) => s.expandedNodes);
  const setActiveNode = useStore((s) => s.setActiveNode);
  const setHoveredNode = useStore((s) => s.setHoveredNode);
  const toggleExpanded = useStore((s) => s.toggleExpanded);

  const isActive = node.id === activeNodeId;
  const isHovered = node.id === hoveredNodeId;
  const isExpanded = expandedNodes.has(node.id);
  const hasChildren = node.children && node.children.length > 0;
  const nodeColor = getTypeColor(node.type);

  const handleClick = useCallback(() => {
    setActiveNode(node.id);
    if (hasChildren) {
      toggleExpanded(node.id);
    }
  }, [node.id, hasChildren, setActiveNode, toggleExpanded]);

  const handleMouseEnter = useCallback(() => {
    setHoveredNode(node.id);
  }, [node.id, setHoveredNode]);

  const handleMouseLeave = useCallback(() => {
    setHoveredNode(null);
  }, [setHoveredNode]);

  const handleExpandClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      toggleExpanded(node.id);
    },
    [node.id, toggleExpanded]
  );

  const label = columnName || node.label;
  const byteCount = node.byteRange.end - node.byteRange.start;

  return (
    <>
      <div
        className={`ast-node ${isActive ? 'active' : ''} ${isHovered ? 'hovered' : ''}`}
        style={{ '--depth': depth, '--node-color': nodeColor } as React.CSSProperties}
        onClick={handleClick}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        <span
          className={`ast-expand-btn ${!hasChildren ? 'hidden' : ''}`}
          onClick={handleExpandClick}
        >
          {hasChildren ? (isExpanded ? '▼' : '▶') : ''}
        </span>
        <span className="ast-node-badge" style={{ background: nodeColor }}>
          {getShortType(node.type)}
        </span>
        {label && <span className="ast-node-label">{label}:</span>}
        <span className={`ast-node-value ${getValueClass(node.value, node.displayValue)}`}>
          {node.displayValue}
        </span>
        <span className="ast-node-bytes">
          [{node.byteRange.start}:{node.byteRange.end}] ({byteCount}B)
        </span>
      </div>
      {hasChildren && isExpanded && (
        <div className="ast-children">
          {node.children!.map((child) => (
            <AstNodeItem key={child.id} node={child} depth={depth + 1} />
          ))}
        </div>
      )}
    </>
  );
});

export function AstTree() {
  const parsedData = useStore((s) => s.parsedData);
  const expandedNodes = useStore((s) => s.expandedNodes);
  const expandAll = useStore((s) => s.expandAll);
  const collapseAll = useStore((s) => s.collapseAll);
  const toggleExpanded = useStore((s) => s.toggleExpanded);

  if (!parsedData) {
    return (
      <div className="ast-tree">
        <div className="ast-tree-empty">No data loaded</div>
      </div>
    );
  }

  return (
    <div className="ast-tree">
      <div className="ast-header">
        <button className="ast-header-btn" onClick={expandAll}>
          Expand All
        </button>
        <button className="ast-header-btn" onClick={collapseAll}>
          Collapse All
        </button>
        <span style={{ marginLeft: 'auto', color: 'var(--text-muted)', fontSize: 11 }}>
          {parsedData.totalBytes} bytes | {parsedData.rows.length} row(s) |{' '}
          {parsedData.header.columnCount} column(s)
        </span>
      </div>

      {parsedData.rows.map((row, rowIndex) => {
        const rowId = `row-${rowIndex}`;
        const isExpanded = expandedNodes.has(rowId);

        return (
          <div key={rowId} className="ast-row">
            <div className="ast-row-header" onClick={() => toggleExpanded(rowId)}>
              <span>{isExpanded ? '▼' : '▶'}</span>
              <span className="ast-row-label">Row {rowIndex}</span>
              <span className="ast-row-bytes">
                [{row.byteRange.start}:{row.byteRange.end}] (
                {row.byteRange.end - row.byteRange.start}B)
              </span>
            </div>
            {isExpanded && (
              <div className="ast-children">
                {row.values.map((node, colIndex) => (
                  <AstNodeItem
                    key={node.id}
                    node={node}
                    depth={1}
                    columnName={parsedData.header.columns[colIndex]?.name}
                  />
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default AstTree;
