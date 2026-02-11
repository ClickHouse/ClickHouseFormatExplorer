import { test, expect } from '@playwright/test';
import { _electron as electron } from 'playwright';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const mainPath = path.join(__dirname, '..', 'dist-electron', 'main.js');

test.describe('Electron app', () => {
  test('launches and renders the UI', async () => {
    const app = await electron.launch({ args: [mainPath] });
    const window = await app.firstWindow();

    // Wait for the app to render
    await window.waitForSelector('.query-input-title', { timeout: 10000 });

    // Verify key UI elements are present
    await expect(window.locator('.query-input-title')).toHaveText('SQL Query');
    await expect(window.locator('.query-textarea')).toBeVisible();
    await expect(window.locator('.query-btn.primary')).toBeVisible();
    await expect(window.locator('#format-select')).toBeVisible();

    // In Electron mode, the host input should be visible and Share button hidden
    await expect(window.locator('#host-input')).toBeVisible();
    await expect(window.locator('#host-input')).toHaveValue('http://localhost:8123');

    // Share button should not exist in Electron mode
    const shareButtons = window.locator('button', { hasText: 'Share' });
    await expect(shareButtons).toHaveCount(0);

    // Verify the window title
    const title = await window.title();
    expect(title).toBeTruthy();

    await app.close();
  });

  test('loads config.json and allows editing host', async () => {
    const app = await electron.launch({ args: [mainPath] });
    const window = await app.firstWindow();

    await window.waitForSelector('#host-input', { timeout: 10000 });

    // Default host from config.json should be localhost:8123
    const hostInput = window.locator('#host-input');
    await expect(hostInput).toHaveValue('http://localhost:8123');

    // Change the host
    await hostInput.fill('http://my-clickhouse:8123');

    // The value should be updated in the UI
    await expect(hostInput).toHaveValue('http://my-clickhouse:8123');

    await app.close();
  });

  test('upload button is functional', async () => {
    const app = await electron.launch({ args: [mainPath] });
    const window = await app.firstWindow();

    await window.waitForSelector('.query-input-title', { timeout: 10000 });

    // Upload button should be present and clickable
    const uploadBtn = window.locator('button', { hasText: 'Upload' });
    await expect(uploadBtn).toBeVisible();
    await expect(uploadBtn).toBeEnabled();

    await app.close();
  });
});
