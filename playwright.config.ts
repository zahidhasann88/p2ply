import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  testMatch: '**/*.spec.ts',
  fullyParallel: false,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: 'list',
  use: {
    ...devices['Desktop Chrome'],
    browserName: 'chromium',
    headless: true,
    launchOptions: { args: ['--disable-background-networking', '--disable-dev-shm-usage'] },
    trace: 'retain-on-failure',
  },
});
