import { useCallback } from 'react';
import { useStore } from '../store/store';
import { DEFAULT_QUERY } from '../core/clickhouse/client';

export function QueryInput() {
  const query = useStore((s) => s.query);
  const setQuery = useStore((s) => s.setQuery);
  const executeQuery = useStore((s) => s.executeQuery);
  const loadSampleData = useStore((s) => s.loadSampleData);
  const isLoading = useStore((s) => s.isLoading);
  const parseError = useStore((s) => s.parseError);
  const queryTiming = useStore((s) => s.queryTiming);

  const handleExecute = useCallback(() => {
    executeQuery();
  }, [executeQuery]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        handleExecute();
      }
    },
    [handleExecute]
  );

  const handleReset = useCallback(() => {
    setQuery(DEFAULT_QUERY);
  }, [setQuery]);

  return (
    <div className="query-input">
      <div className="query-input-header">
        <span className="query-input-title">SQL Query</span>
        <div className="query-input-actions">
          <button
            className="query-btn secondary"
            onClick={loadSampleData}
            disabled={isLoading}
            title="Load sample data without connecting to ClickHouse"
          >
            Load Sample
          </button>
          <button className="query-btn secondary" onClick={handleReset} disabled={isLoading}>
            Reset
          </button>
          <button className="query-btn primary" onClick={handleExecute} disabled={isLoading}>
            {isLoading ? 'Running...' : 'Execute (Ctrl+Enter)'}
          </button>
        </div>
      </div>
      <textarea
        className="query-textarea"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Enter your SQL query..."
        disabled={isLoading}
        spellCheck={false}
      />
      {parseError && (
        <div className="query-error">
          <span className="query-error-icon">!</span>
          <span className="query-error-text">{parseError.message}</span>
        </div>
      )}
      {queryTiming !== null && !parseError && (
        <div className="query-timing">Query executed in {queryTiming.toFixed(0)}ms</div>
      )}
    </div>
  );
}

export default QueryInput;
