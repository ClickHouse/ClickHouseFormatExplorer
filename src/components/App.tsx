import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import { HexViewer } from './HexViewer/HexViewer';
import { AstTree } from './AstTree/AstTree';
import { QueryInput } from './QueryInput';
import '../styles/app.css';

function App() {
  return (
    <div className="app">
      <header className="app-header">
        <h1 className="app-title">ClickHouse Format Explorer</h1>
      </header>

      <div className="app-query">
        <QueryInput />
      </div>

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
    </div>
  );
}

export default App;
