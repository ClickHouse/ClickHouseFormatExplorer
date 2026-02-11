import { app, BrowserWindow, ipcMain } from 'electron';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// config.json lives next to the app:
//   dev  → project root (process.cwd())
//   prod → next to the executable
const configPath = app.isPackaged
  ? path.join(path.dirname(process.execPath), 'config.json')
  : path.join(process.cwd(), 'config.json');

interface Config {
  host: string;
}

const DEFAULT_CONFIG: Config = { host: 'http://localhost:8123' };

function loadConfig(): Config {
  try {
    return { ...DEFAULT_CONFIG, ...JSON.parse(fs.readFileSync(configPath, 'utf-8')) };
  } catch {
    return { ...DEFAULT_CONFIG };
  }
}

function saveConfig(config: Config): void {
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + '\n');
}

// Experimental type settings sent as query params to ClickHouse
const CLICKHOUSE_SETTINGS: Record<string, string> = {
  allow_experimental_variant_type: '1',
  allow_experimental_dynamic_type: '1',
  allow_experimental_json_type: '1',
  allow_suspicious_variant_types: '1',
  allow_experimental_qbit_type: '1',
  allow_suspicious_low_cardinality_types: '1',
};

function createWindow(): void {
  const win = new BrowserWindow({
    width: 1400,
    height: 900,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  // In dev, load from Vite dev server; in prod, load the built index.html
  if (process.env.VITE_DEV_SERVER_URL) {
    win.loadURL(process.env.VITE_DEV_SERVER_URL);
  } else {
    win.loadFile(path.join(__dirname, '../dist/index.html'));
  }
}

// IPC: Execute a ClickHouse query
ipcMain.handle('execute-query', async (_event, options: { query: string; format: string }) => {
  const config = loadConfig();
  const params = new URLSearchParams({
    default_format: options.format,
    ...CLICKHOUSE_SETTINGS,
  });

  const response = await fetch(`${config.host}/?${params}`, {
    method: 'POST',
    body: options.query,
    headers: { 'Content-Type': 'text/plain' },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`ClickHouse error (${response.status}): ${errorText}`);
  }

  return await response.arrayBuffer();
});

// IPC: Get config
ipcMain.handle('get-config', () => {
  return loadConfig();
});

// IPC: Save config
ipcMain.handle('save-config', (_event, config: Config) => {
  saveConfig(config);
});

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
