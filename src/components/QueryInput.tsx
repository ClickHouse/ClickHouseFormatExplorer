import { useCallback, useEffect, useRef, useState } from 'react';
import { useStore } from '../store/store';
import { DEFAULT_QUERY } from '../core/clickhouse/client';
import { ClickHouseFormat, FORMAT_METADATA } from '../core/types/formats';
import { encodeBase64Url } from '../core/base64url';

const isElectron = !!window.electronAPI;

export function QueryInput() {
  const query = useStore((s) => s.query);
  const setQuery = useStore((s) => s.setQuery);
  const format = useStore((s) => s.format);
  const setFormat = useStore((s) => s.setFormat);
  const executeQuery = useStore((s) => s.executeQuery);
  const loadFile = useStore((s) => s.loadFile);
  const isLoading = useStore((s) => s.isLoading);
  const parseError = useStore((s) => s.parseError);
  const queryTiming = useStore((s) => s.queryTiming);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const [shareLabel, setShareLabel] = useState('Share');
  const [hostUrl, setHostUrl] = useState('http://localhost:8123');

  // Load connection config in Electron mode
  useEffect(() => {
    if (isElectron) {
      window.electronAPI!.getConfig().then((c) => setHostUrl(c.host));
    }
  }, []);

  const handleHostChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setHostUrl(value);
    if (isElectron) {
      window.electronAPI!.saveConfig({ host: value });
    }
  }, []);

  const handleShare = useCallback(() => {
    const url = new URL(window.location.href);
    url.search = '';
    url.searchParams.set('q', encodeBase64Url(query));
    url.searchParams.set('f', format);
    navigator.clipboard.writeText(url.toString());
    setShareLabel('Copied!');
    setTimeout(() => setShareLabel('Share'), 2000);
  }, [query, format]);

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

  const handleFormatChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      setFormat(e.target.value as ClickHouseFormat);
    },
    [setFormat]
  );

  const handleUploadClick = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const handleFileChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) {
        loadFile(file);
      }
      // Reset the input so the same file can be selected again
      e.target.value = '';
    },
    [loadFile]
  );

  return (
    <div className="query-input">
      <div className="query-input-header">
        <span className="query-input-title">SQL Query</span>
        {isElectron && (
          <div className="query-host-selector">
            <label htmlFor="host-input" className="query-format-label">
              Host:
            </label>
            <input
              id="host-input"
              className="query-host-input"
              type="text"
              value={hostUrl}
              onChange={handleHostChange}
              placeholder="http://localhost:8123"
              disabled={isLoading}
            />
          </div>
        )}
        <div className="query-format-selector">
          <label htmlFor="format-select" className="query-format-label">
            Format:
          </label>
          <select
            id="format-select"
            className="query-format-select"
            value={format}
            onChange={handleFormatChange}
            disabled={isLoading}
          >
            {Object.values(ClickHouseFormat).map((fmt) => (
              <option key={fmt} value={fmt}>
                {FORMAT_METADATA[fmt].displayName}
              </option>
            ))}
          </select>
        </div>
        <div className="query-input-actions">
          <input
            ref={fileInputRef}
            type="file"
            onChange={handleFileChange}
            style={{ display: 'none' }}
            accept="*"
          />
          <button
            className="query-btn secondary"
            onClick={handleUploadClick}
            disabled={isLoading}
            title="Upload a binary file to decode"
          >
            Upload
          </button>
          {!isElectron && (
            <button className="query-btn secondary" onClick={handleShare} title="Copy shareable URL to clipboard">
              {shareLabel}
            </button>
          )}
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
