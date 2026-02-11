import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const isElectron = !!process.env.ELECTRON;

export default defineConfig(async () => {
  const electronPlugins = [];
  if (isElectron) {
    const electron = (await import('vite-plugin-electron')).default;
    electronPlugins.push(
      ...electron([
        {
          entry: 'electron/main.ts',
          vite: {
            build: {
              outDir: 'dist-electron',
              rollupOptions: { external: ['electron'] },
            },
          },
        },
        {
          entry: 'electron/preload.ts',
          onstart({ reload }) { reload(); },
          vite: {
            build: {
              outDir: 'dist-electron',
              lib: { entry: 'electron/preload.ts', formats: ['cjs'] },
              rollupOptions: {
                external: ['electron'],
                output: { entryFileNames: 'preload.js' },
              },
            },
          },
        },
      ])
    );
  }

  return {
    plugins: [react(), ...electronPlugins],
    server: {
      proxy: {
        '/clickhouse': {
          target: 'http://localhost:8123',
          changeOrigin: true,
          rewrite: (path: string) => path.replace(/^\/clickhouse/, ''),
        },
      },
    },
  };
});
