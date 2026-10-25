import { expect, test, type Browser, type BrowserContext, type Page } from '@playwright/test';
import { createServer, type ViteDevServer } from 'vite';
import type { AddressInfo } from 'node:net';
import { startSignalingServer, type SignalingServer } from '../../src/server/server.js';
import type { P2Ply } from '../../src/p2ply.js';
import type { FileTransferResult } from '../../src/file-transfer/types.js';

interface MeshErrorView {
  code: string;
  message: string;
}

declare global {
  interface Window {
    __p2ply?: P2Ply<unknown>;
    __meshReady?: boolean;
    __meshStartError?: string;
    __meshMessages: unknown[];
    __meshFiles: FileTransferResult[];
    __meshErrors: MeshErrorView[];
  }
}

let vite: ViteDevServer;
let signalingServer: SignalingServer;
let origin: string;
let signalingUrl: string;

async function newReadyPage(
  browser: Browser,
  roomId: string,
): Promise<{ context: BrowserContext; page: Page }> {
  const context = await browser.newContext();
  const page = await context.newPage();
  page.on('pageerror', (error) => console.error('P2Ply browser page error:', error.message));
  page.on('console', (message) => {
    if (message.type() === 'error') console.error('P2Ply browser console:', message.text());
  });
  const query = new URLSearchParams({ signalingUrl, room: roomId });
  await page.goto(`${origin}/harness.html?${query.toString()}`);
  await expect
    .poll(() =>
      page.evaluate(() => window.__meshReady === true || Boolean(window.__meshStartError)),
    )
    .toBe(true);
  const startError = await page.evaluate(() => window.__meshStartError);
  expect(startError, 'P2Ply should start in the browser').toBeUndefined();
  return { context, page };
}

test.beforeAll(async () => {
  vite = await createServer({
    configFile: false,
    root: `${process.cwd()}/tests/e2e`,
    server: { host: '127.0.0.1', port: 0, strictPort: false },
    logLevel: 'error',
  });
  await vite.listen();
  const viteHttpServer = vite.httpServer;
  if (!viteHttpServer) throw new Error('Vite did not create an HTTP server.');
  const httpAddress = viteHttpServer.address() as AddressInfo | null;
  if (!httpAddress) throw new Error('Vite did not bind a TCP port.');
  origin = `http://127.0.0.1:${httpAddress.port}`;
  signalingServer = await startSignalingServer({
    host: '127.0.0.1',
    port: 0,
    allowedOrigins: [origin],
    logger: {
      log: (level, message, context) => console.log('signaling', level, message, context ?? {}),
    },
  });
  const signalAddress = signalingServer.httpServer.address() as AddressInfo | null;
  if (!signalAddress) throw new Error('Signaling server did not bind a TCP port.');
  signalingUrl = `ws://127.0.0.1:${signalAddress.port}`;
});

test.afterAll(async () => {
  await signalingServer?.close();
  await vite?.close();
});

test('two isolated browser contexts connect, exchange a message, and transfer a verified file', async ({
  browser,
}) => {
  test.setTimeout(60_000);
  const roomId = `browser-${Date.now()}`;
  const [a, b] = await Promise.all([newReadyPage(browser, roomId), newReadyPage(browser, roomId)]);
  try {
    const peerIds = await Promise.all([
      a.page.evaluate(() => window.__p2ply!.peerId),
      b.page.evaluate(() => window.__p2ply!.peerId),
    ]);
    const [peerA, peerB] = peerIds;
    await expect.poll(() => a.page.evaluate(() => window.__p2ply!.getPeers().length)).toBe(1);
    await expect.poll(() => b.page.evaluate(() => window.__p2ply!.getPeers().length)).toBe(1);

    try {
      await Promise.all([
        a.page.evaluate((peerId) => window.__p2ply!.connect(peerId), peerB),
        b.page.evaluate((peerId) => window.__p2ply!.connect(peerId), peerA),
      ]);
    } catch (error) {
      const diagnostics = await Promise.all(
        [a.page, b.page].map((page) =>
          page.evaluate(async () => ({
            errors: window.__meshErrors,
            peers: window.__p2ply?.getPeers(),
            diagnostics: await window.__p2ply?.getDiagnostics(),
          })),
        ),
      );
      console.error('P2Ply connection diagnostics:', JSON.stringify(diagnostics));
      throw error;
    }
    await expect
      .poll(() => a.page.evaluate((id) => window.__p2ply!.getPeer(id)?.state, peerB))
      .toBe('connected');

    const receipt = await a.page.evaluate(
      async (peerId) =>
        window.__p2ply!.send(
          peerId,
          { kind: 'chat', text: 'browser-to-browser over WebRTC' },
          { acknowledge: true },
        ),
      peerB,
    );
    expect(receipt.acknowledgedAt).toBeGreaterThan(0);
    await expect.poll(() => b.page.evaluate(() => window.__meshMessages.length)).toBe(1);
    expect(await b.page.evaluate(() => window.__meshMessages[0])).toEqual({
      kind: 'chat',
      text: 'browser-to-browser over WebRTC',
    });

    const sent = await a.page.evaluate(async (peerId) => {
      const bytes = new Uint8Array(51_337);
      for (let index = 0; index < bytes.length; index += 1) bytes[index] = (index * 17) % 251;
      const file = new File([bytes], 'playwright-proof.bin', {
        type: 'application/octet-stream',
        lastModified: 1_700_000_000_000,
      });
      const transfer = await window.__p2ply!.sendFile(peerId, file);
      const result = await transfer.completion;
      return {
        transferId: result.transferId,
        name: result.name,
        size: result.size,
        sha256: result.sha256,
      };
    }, peerB);
    expect(sent.size).toBe(51_337);
    expect(sent.sha256).toMatch(/^[0-9a-f]{64}$/);

    await expect.poll(() => b.page.evaluate(() => window.__meshFiles.length)).toBe(1);
    const received = await b.page.evaluate(async () => {
      const file = window.__meshFiles[0];
      if (!file?.blob) return undefined;
      const bytes = new Uint8Array(await file.blob.arrayBuffer());
      return {
        name: file.name,
        size: file.size,
        sha256: file.sha256,
        sample: [bytes[0], bytes[1], bytes[100], bytes.at(-1)],
      };
    });
    expect(received).toMatchObject({
      name: 'playwright-proof.bin',
      size: sent.size,
      sha256: sent.sha256,
    });
    expect(received?.sample).toEqual([0, 17, (100 * 17) % 251, ((sent.size - 1) * 17) % 251]);
  } finally {
    await Promise.all([a.context.close(), b.context.close()]);
  }
});
