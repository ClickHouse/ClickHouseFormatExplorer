import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('electronAPI', {
  executeQuery: (options: { query: string; format: string; nativeProtocolVersion?: number }): Promise<ArrayBuffer> =>
    ipcRenderer.invoke('execute-query', options),
  captureNativeProtocol: (options: { query: string }): Promise<{ c2s: Uint8Array; s2c: Uint8Array; meta?: Record<string, unknown> }> =>
    ipcRenderer.invoke('capture-native-protocol', options),
  getConfig: (): Promise<{ host: string }> =>
    ipcRenderer.invoke('get-config'),
  saveConfig: (config: { host: string }): Promise<void> =>
    ipcRenderer.invoke('save-config', config),
});
