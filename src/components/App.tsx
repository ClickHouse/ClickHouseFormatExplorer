import { useEffect } from 'react';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import { HexViewer } from './HexViewer/HexViewer';
import { AstTree } from './AstTree/AstTree';
import { QueryInput, decodeBase64Url } from './QueryInput';
import { useStore } from '../store/store';
import { ClickHouseFormat } from '../core/types/formats';
import logo from '../assets/clickhouse-yellow-badge.svg';
import '../styles/app.css';

function App() {
  const setQuery = useStore((s) => s.setQuery);
  const setFormat = useStore((s) => s.setFormat);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const q = params.get('q');
    const f = params.get('f');

    if (q) {
      try {
        setQuery(decodeBase64Url(q));
      } catch {
        // ignore malformed base64
      }
    }
    if (f && Object.values(ClickHouseFormat).includes(f as ClickHouseFormat)) {
      setFormat(f as ClickHouseFormat);
    }

    if (q || f) {
      window.history.replaceState({}, '', window.location.pathname);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="app">
      <header className="app-header">
        <img src={logo} alt="ClickHouse" className="app-logo" />
        <h1 className="app-title">ClickHouse Format Explorer</h1>
      </header>

      <PanelGroup direction="vertical" autoSaveId="vertical-panels" className="app-body">
        <Panel defaultSize={20} minSize={10} maxSize={50}>
          <div className="app-query">
            <QueryInput />
          </div>
        </Panel>
        <PanelResizeHandle className="resize-handle-horizontal" />
        <Panel defaultSize={80} minSize={30}>
          <main className="app-main">
            <PanelGroup direction="horizontal" autoSaveId="main-panels">
              <Panel defaultSize={50} minSize={20}>
                <div className="panel-container">
                  <div className="panel-header">Hex View</div>
                  <div className="panel-content">
                    <HexViewer />
                  </div>
                </div>
              </Panel>
              <PanelResizeHandle className="resize-handle" />
              <Panel defaultSize={50} minSize={20}>
                <div className="panel-container">
                  <div className="panel-header">AST Tree</div>
                  <div className="panel-content">
                    <AstTree />
                  </div>
                </div>
              </Panel>
            </PanelGroup>
          </main>
        </Panel>
      </PanelGroup>
    </div>
  );
}

export default App;
