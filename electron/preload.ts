import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('electronAPI', {
  executeQuery: (options: { query: string; format: string }): Promise<ArrayBuffer> =>
    ipcRenderer.invoke('execute-query', options),
  getConfig: (): Promise<{ host: string }> =>
    ipcRenderer.invoke('get-config'),
  saveConfig: (config: { host: string }): Promise<void> =>
    ipcRenderer.invoke('save-config', config),
});
