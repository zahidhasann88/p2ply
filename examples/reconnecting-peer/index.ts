import { NodeP2Ply } from '../../src/node.js';

const mesh = await NodeP2Ply.create({
  signalingUrl: process.env['P2PLY_URL'] ?? 'ws://localhost:8787',
  reconnect: { enabled: true, maxAttempts: 10, minDelayMs: 250, maxDelayMs: 20_000, jitter: 0.25 },
  peerReconnect: { maxAttempts: 4, minDelayMs: 300, maxDelayMs: 8_000, jitter: 0.2 },
});

mesh.on(
  'peer:join',
  ({ peerId }) => void mesh.connect(peerId).catch((error: unknown) => console.error(error)),
);
mesh.on('peer:state', ({ peerId, state }) => {
  console.log(`${peerId}: ${state}`);
  if (state === 'failed' || state === 'disconnected') {
    void mesh
      .reconnectPeer(peerId)
      .catch((error: unknown) => console.error(`Reconnect failed for ${peerId}:`, error));
  }
});
mesh.on('signaling:state', ({ state, attempt }) =>
  console.log(`Signaling ${state}${attempt ? ` · ${attempt}` : ''}`),
);
mesh.on('error', (error) => console.error(`[${error.code}] ${error.message}`));

await mesh.start();
await mesh.join(process.env['P2PLY_ROOM'] ?? 'resilient-room');
process.once('SIGINT', () => void mesh.close());
process.once('SIGTERM', () => void mesh.close());
