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
  const scrollToHex = useStore((s) => s.scrollToHex);

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

  const handleDoubleClick = useCallback(() => {
    scrollToHex(node.byteRange.start);
  }, [node.byteRange.start, scrollToHex]);

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
        onDoubleClick={handleDoubleClick}
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
  const activeNodeId = useStore((s) => s.activeNodeId);
  const hoveredNodeId = useStore((s) => s.hoveredNodeId);
  const expandAll = useStore((s) => s.expandAll);
  const collapseAll = useStore((s) => s.collapseAll);
  const toggleExpanded = useStore((s) => s.toggleExpanded);
  const setActiveNode = useStore((s) => s.setActiveNode);
  const setHoveredNode = useStore((s) => s.setHoveredNode);
  const scrollToHex = useStore((s) => s.scrollToHex);

  if (!parsedData) {
    return (
      <div className="ast-tree">
        <div className="ast-tree-empty">No data loaded</div>
      </div>
    );
  }

  const isBlockBased = !!parsedData.blocks;
  const itemCount = parsedData.rows?.length ?? parsedData.blocks?.length ?? 0;

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
          {parsedData.totalBytes} bytes | {itemCount} {isBlockBased ? 'block(s)' : 'row(s)'} |{' '}
          {parsedData.header.columnCount} column(s)
        </span>
      </div>

      {/* RowBinary Header */}
      {parsedData.rows && (() => {
        const headerId = 'rowbinary-header';
        const isHeaderExpanded = expandedNodes.has(headerId);
        const headerByteCount = parsedData.header.byteRange.end - parsedData.header.byteRange.start;
        const columnCountId = 'rowbinary-header-colcount';

        return (
          <div className="ast-metadata-section" style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)' }}>
            <div
              className={`ast-metadata-header ${activeNodeId === headerId ? 'active' : ''} ${hoveredNodeId === headerId ? 'hovered' : ''}`}
              onClick={() => {
                setActiveNode(headerId);
                toggleExpanded(headerId);
              }}
              onDoubleClick={() => scrollToHex(parsedData.header.byteRange.start)}
              onMouseEnter={() => setHoveredNode(headerId)}
              onMouseLeave={() => setHoveredNode(null)}
              style={{ '--depth': 0 } as React.CSSProperties}
            >
              <span>{isHeaderExpanded ? '▼' : '▶'}</span>
              <span className="ast-metadata-badge">Header</span>
              <span className="ast-metadata-label">RowBinaryWithNamesAndTypes header</span>
              <span className="ast-metadata-bytes">
                [{parsedData.header.byteRange.start}:{parsedData.header.byteRange.end}] ({headerByteCount}B)
              </span>
            </div>
            {isHeaderExpanded && (
              <div className="ast-children">
                {/* Column count */}
                <div
                  className={`ast-metadata-item ${activeNodeId === columnCountId ? 'active' : ''} ${hoveredNodeId === columnCountId ? 'hovered' : ''}`}
                  style={{ '--depth': 1 } as React.CSSProperties}
                  onClick={() => setActiveNode(columnCountId)}
                  onDoubleClick={() => scrollToHex(parsedData.header.columnCountRange.start)}
                  onMouseEnter={() => setHoveredNode(columnCountId)}
                  onMouseLeave={() => setHoveredNode(null)}
                >
                  <span className="ast-metadata-badge">LEB128</span>
                  <span className="ast-metadata-label">columnCount:</span>
                  <span className="ast-metadata-value">{parsedData.header.columnCount}</span>
                  <span className="ast-metadata-bytes">
                    [{parsedData.header.columnCountRange.start}:{parsedData.header.columnCountRange.end}] (
                    {parsedData.header.columnCountRange.end - parsedData.header.columnCountRange.start}B)
                  </span>
                </div>

                {/* Column definitions */}
                {parsedData.header.columns.map((col, colIndex) => {
                  const colDefId = `rowbinary-header-col-${colIndex}`;
                  const colNameId = `rowbinary-header-col-${colIndex}-name`;
                  const colTypeId = `rowbinary-header-col-${colIndex}-type`;
                  const isColDefExpanded = expandedNodes.has(colDefId);

                  return (
                    <div key={colDefId} className="ast-metadata-section">
                      <div
                        className={`ast-metadata-header ${activeNodeId === colDefId ? 'active' : ''} ${hoveredNodeId === colDefId ? 'hovered' : ''}`}
                        onClick={() => {
                          setActiveNode(colDefId);
                          toggleExpanded(colDefId);
                        }}
                        onDoubleClick={() => scrollToHex(col.nameByteRange.start)}
                        onMouseEnter={() => setHoveredNode(colDefId)}
                        onMouseLeave={() => setHoveredNode(null)}
                        style={{ '--depth': 1 } as React.CSSProperties}
                      >
                        <span>{isColDefExpanded ? '▼' : '▶'}</span>
                        <span className="ast-column-badge" style={{ background: getTypeColor(col.typeString) }}>
                          {col.typeString.length > 20 ? col.typeString.split('(')[0] + '(...)' : col.typeString}
                        </span>
                        <span className="ast-column-name">{col.name}</span>
                        <span className="ast-metadata-bytes">
                          [{col.nameByteRange.start}:{col.typeByteRange.end}] (
                          {col.typeByteRange.end - col.nameByteRange.start}B)
                        </span>
                      </div>
                      {isColDefExpanded && (
                        <div className="ast-children">
                          <div
                            className={`ast-metadata-item ${activeNodeId === colNameId ? 'active' : ''} ${hoveredNodeId === colNameId ? 'hovered' : ''}`}
                            style={{ '--depth': 2 } as React.CSSProperties}
                            onClick={() => setActiveNode(colNameId)}
                            onDoubleClick={() => scrollToHex(col.nameByteRange.start)}
                            onMouseEnter={() => setHoveredNode(colNameId)}
                            onMouseLeave={() => setHoveredNode(null)}
                          >
                            <span className="ast-metadata-badge">String</span>
                            <span className="ast-metadata-label">name:</span>
                            <span className="ast-metadata-value">"{col.name}"</span>
                            <span className="ast-metadata-bytes">
                              [{col.nameByteRange.start}:{col.nameByteRange.end}] (
                              {col.nameByteRange.end - col.nameByteRange.start}B)
                            </span>
                          </div>
                          <div
                            className={`ast-metadata-item ${activeNodeId === colTypeId ? 'active' : ''} ${hoveredNodeId === colTypeId ? 'hovered' : ''}`}
                            style={{ '--depth': 2 } as React.CSSProperties}
                            onClick={() => setActiveNode(colTypeId)}
                            onDoubleClick={() => scrollToHex(col.typeByteRange.start)}
                            onMouseEnter={() => setHoveredNode(colTypeId)}
                            onMouseLeave={() => setHoveredNode(null)}
                          >
                            <span className="ast-metadata-badge">String</span>
                            <span className="ast-metadata-label">type:</span>
                            <span className="ast-metadata-value">"{col.typeString}"</span>
                            <span className="ast-metadata-bytes">
                              [{col.typeByteRange.start}:{col.typeByteRange.end}] (
                              {col.typeByteRange.end - col.typeByteRange.start}B)
                            </span>
                          </div>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        );
      })()}

      {/* Row-based display (RowBinary) */}
      {parsedData.rows?.map((row, rowIndex) => {
        const rowId = `row-${rowIndex}`;
        const isExpanded = expandedNodes.has(rowId);

        return (
          <div key={rowId} className="ast-row">
            <div
              className="ast-row-header"
              onClick={() => toggleExpanded(rowId)}
              onDoubleClick={() => scrollToHex(row.byteRange.start)}
            >
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

      {/* Block-based display (Native) */}
      {parsedData.blocks?.map((block, blockIndex) => {
        const blockId = `block-${blockIndex}`;
        const blockHeaderId = `block-${blockIndex}-header`;
        const isBlockExpanded = expandedNodes.has(blockId);
        const isHeaderExpanded = expandedNodes.has(blockHeaderId);
        const byteCount = block.byteRange.end - block.byteRange.start;

        // IDs for header metadata items (for hover highlighting)
        const numColsId = `block-${blockIndex}-numcols`;
        const numRowsId = `block-${blockIndex}-numrows`;

        return (
          <div key={blockId} className="ast-block">
            <div
              className="ast-block-header"
              onClick={() => toggleExpanded(blockId)}
              onDoubleClick={() => scrollToHex(block.byteRange.start)}
            >
              <span>{isBlockExpanded ? '▼' : '▶'}</span>
              <span className="ast-block-label">Block {blockIndex}</span>
              <span className="ast-block-info">
                {block.rowCount} rows × {block.columns.length} columns
              </span>
              <span className="ast-block-bytes">
                [{block.byteRange.start}:{block.byteRange.end}] ({byteCount}B)
              </span>
            </div>
            {isBlockExpanded && (
              <div className="ast-children">
                {/* Block Header Metadata */}
                <div className="ast-metadata-section">
                  <div
                    className={`ast-metadata-header ${activeNodeId === blockHeaderId ? 'active' : ''} ${hoveredNodeId === blockHeaderId ? 'hovered' : ''}`}
                    onClick={() => {
                      setActiveNode(blockHeaderId);
                      toggleExpanded(blockHeaderId);
                    }}
                    onDoubleClick={() => scrollToHex(block.header.numColumnsRange.start)}
                    onMouseEnter={() => setHoveredNode(blockHeaderId)}
                    onMouseLeave={() => setHoveredNode(null)}
                    style={{ '--depth': 1 } as React.CSSProperties}
                  >
                    <span>{isHeaderExpanded ? '▼' : '▶'}</span>
                    <span className="ast-metadata-badge">Header</span>
                    <span className="ast-metadata-label">Block metadata</span>
                    <span className="ast-metadata-bytes">
                      [{block.header.numColumnsRange.start}:{block.header.numRowsRange.end}] (
                      {block.header.numRowsRange.end - block.header.numColumnsRange.start}B)
                    </span>
                  </div>
                  {isHeaderExpanded && (
                    <div className="ast-children">
                      <div
                        className={`ast-metadata-item ${activeNodeId === numColsId ? 'active' : ''} ${hoveredNodeId === numColsId ? 'hovered' : ''}`}
                        style={{ '--depth': 2 } as React.CSSProperties}
                        onClick={() => setActiveNode(numColsId)}
                        onDoubleClick={() => scrollToHex(block.header.numColumnsRange.start)}
                        onMouseEnter={() => setHoveredNode(numColsId)}
                        onMouseLeave={() => setHoveredNode(null)}
                      >
                        <span className="ast-metadata-badge">LEB128</span>
                        <span className="ast-metadata-label">numColumns:</span>
                        <span className="ast-metadata-value">{block.header.numColumns}</span>
                        <span className="ast-metadata-bytes">
                          [{block.header.numColumnsRange.start}:{block.header.numColumnsRange.end}] (
                          {block.header.numColumnsRange.end - block.header.numColumnsRange.start}B)
                        </span>
                      </div>
                      <div
                        className={`ast-metadata-item ${activeNodeId === numRowsId ? 'active' : ''} ${hoveredNodeId === numRowsId ? 'hovered' : ''}`}
                        style={{ '--depth': 2 } as React.CSSProperties}
                        onClick={() => setActiveNode(numRowsId)}
                        onDoubleClick={() => scrollToHex(block.header.numRowsRange.start)}
                        onMouseEnter={() => setHoveredNode(numRowsId)}
                        onMouseLeave={() => setHoveredNode(null)}
                      >
                        <span className="ast-metadata-badge">LEB128</span>
                        <span className="ast-metadata-label">numRows:</span>
                        <span className="ast-metadata-value">{block.header.numRows}</span>
                        <span className="ast-metadata-bytes">
                          [{block.header.numRowsRange.start}:{block.header.numRowsRange.end}] (
                          {block.header.numRowsRange.end - block.header.numRowsRange.start}B)
                        </span>
                      </div>
                    </div>
                  )}
                </div>

                {/* Columns */}
                {block.columns.map((col) => {
                  const isColExpanded = expandedNodes.has(col.id);
                  const isColActive = col.id === activeNodeId;
                  const isColHovered = col.id === hoveredNodeId;
                  const colByteCount = col.dataByteRange.end - col.dataByteRange.start;

                  // IDs for column metadata (name and type)
                  const colNameId = `${col.id}-name`;
                  const colTypeId = `${col.id}-type`;
                  const colMetaId = `${col.id}-meta`;
                  const isColMetaExpanded = expandedNodes.has(colMetaId);

                  return (
                    <div key={col.id} className="ast-column">
                      <div
                        className={`ast-column-header ${isColActive ? 'active' : ''} ${isColHovered ? 'hovered' : ''}`}
                        onClick={() => {
                          setActiveNode(col.id);
                          toggleExpanded(col.id);
                        }}
                        onDoubleClick={() => scrollToHex(col.nameByteRange.start)}
                        onMouseEnter={() => setHoveredNode(col.id)}
                        onMouseLeave={() => setHoveredNode(null)}
                        style={{ '--depth': 1, '--node-color': getTypeColor(col.typeString) } as React.CSSProperties}
                      >
                        <span>{isColExpanded ? '▼' : '▶'}</span>
                        <span className="ast-column-badge" style={{ background: getTypeColor(col.typeString) }}>
                          {col.typeString.length > 20 ? col.typeString.split('(')[0] + '(...)' : col.typeString}
                        </span>
                        <span className="ast-column-name">{col.name}</span>
                        <span className="ast-column-count">[{col.values.length} values]</span>
                        <span className="ast-column-bytes">
                          [{col.dataByteRange.start}:{col.dataByteRange.end}] ({colByteCount}B)
                        </span>
                      </div>
                      {isColExpanded && (
                        <div className="ast-children">
                          {/* Column Metadata (name + type definition) */}
                          <div className="ast-metadata-section">
                            <div
                              className={`ast-metadata-header ${activeNodeId === colMetaId ? 'active' : ''} ${hoveredNodeId === colMetaId ? 'hovered' : ''}`}
                              onClick={() => {
                                setActiveNode(colMetaId);
                                toggleExpanded(colMetaId);
                              }}
                              onDoubleClick={() => scrollToHex(col.nameByteRange.start)}
                              onMouseEnter={() => setHoveredNode(colMetaId)}
                              onMouseLeave={() => setHoveredNode(null)}
                              style={{ '--depth': 2 } as React.CSSProperties}
                            >
                              <span>{isColMetaExpanded ? '▼' : '▶'}</span>
                              <span className="ast-metadata-badge">Meta</span>
                              <span className="ast-metadata-label">Column definition</span>
                              <span className="ast-metadata-bytes">
                                [{col.nameByteRange.start}:{col.typeByteRange.end}] (
                                {col.typeByteRange.end - col.nameByteRange.start}B)
                              </span>
                            </div>
                            {isColMetaExpanded && (
                              <div className="ast-children">
                                <div
                                  className={`ast-metadata-item ${activeNodeId === colNameId ? 'active' : ''} ${hoveredNodeId === colNameId ? 'hovered' : ''}`}
                                  style={{ '--depth': 3 } as React.CSSProperties}
                                  onClick={() => setActiveNode(colNameId)}
                                  onDoubleClick={() => scrollToHex(col.nameByteRange.start)}
                                  onMouseEnter={() => setHoveredNode(colNameId)}
                                  onMouseLeave={() => setHoveredNode(null)}
                                >
                                  <span className="ast-metadata-badge">String</span>
                                  <span className="ast-metadata-label">name:</span>
                                  <span className="ast-metadata-value">"{col.name}"</span>
                                  <span className="ast-metadata-bytes">
                                    [{col.nameByteRange.start}:{col.nameByteRange.end}] (
                                    {col.nameByteRange.end - col.nameByteRange.start}B)
                                  </span>
                                </div>
                                <div
                                  className={`ast-metadata-item ${activeNodeId === colTypeId ? 'active' : ''} ${hoveredNodeId === colTypeId ? 'hovered' : ''}`}
                                  style={{ '--depth': 3 } as React.CSSProperties}
                                  onClick={() => setActiveNode(colTypeId)}
                                  onDoubleClick={() => scrollToHex(col.typeByteRange.start)}
                                  onMouseEnter={() => setHoveredNode(colTypeId)}
                                  onMouseLeave={() => setHoveredNode(null)}
                                >
                                  <span className="ast-metadata-badge">String</span>
                                  <span className="ast-metadata-label">type:</span>
                                  <span className="ast-metadata-value">"{col.typeString}"</span>
                                  <span className="ast-metadata-bytes">
                                    [{col.typeByteRange.start}:{col.typeByteRange.end}] (
                                    {col.typeByteRange.end - col.typeByteRange.start}B)
                                  </span>
                                </div>
                              </div>
                            )}
                          </div>

                          {/* Column Values */}
                          {col.values.map((node, valueIndex) => (
                            <AstNodeItem
                              key={node.id}
                              node={node}
                              depth={2}
                              columnName={`[${valueIndex}]`}
                            />
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default AstTree;
