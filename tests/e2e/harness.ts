import { P2Ply } from '../../src/index.js';
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

async function boot(): Promise<void> {
  window.__meshMessages = [];
  window.__meshFiles = [];
  window.__meshErrors = [];
  const params = new URLSearchParams(location.search);
  const signalingUrl = params.get('signalingUrl');
  const roomId = params.get('room') ?? 'playwright-room';
  if (!signalingUrl) throw new Error('Missing signalingUrl query parameter.');

  const mesh = new P2Ply<unknown>({
    signalingUrl,
    reconnect: { enabled: false },
    connectionTimeoutMs: 20_000,
    negotiationTimeoutMs: 20_000,
    transferTimeoutMs: 20_000,
  });
  window.__p2ply = mesh;
  mesh.on('message', ({ data }) => window.__meshMessages.push(data));
  mesh.on('file:offer', (offer) => {
    void offer.accept();
  });
  mesh.on('file:complete', ({ result }) => window.__meshFiles.push(result));
  mesh.on('error', (error) =>
    window.__meshErrors.push({ code: error.code, message: error.message }),
  );
  try {
    await mesh.start();
    await mesh.join(roomId);
    window.__meshReady = true;
  } catch (error) {
    window.__meshStartError =
      error instanceof Error ? error.message : 'Unknown P2Ply startup failure.';
    throw error;
  }
}

void boot();
