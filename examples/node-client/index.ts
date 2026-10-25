import { createInterface } from 'node:readline';
import { NodeP2Ply } from '../../src/node.js';

const signalingUrl = process.env['P2PLY_URL'] ?? 'ws://localhost:8787';
const roomId = process.env['P2PLY_ROOM'] ?? 'node-room';
const mesh = await NodeP2Ply.create({
  signalingUrl,
  reconnect: { enabled: true, maxAttempts: 8, minDelayMs: 250, maxDelayMs: 10_000 },
  peerReconnect: { maxAttempts: 3 },
});

mesh.on('peer:join', ({ peerId }) => {
  console.log(`Peer discovered: ${peerId}`);
  void mesh
    .connect(peerId)
    .catch((error: unknown) => console.error('Peer connection failed:', error));
});
mesh.on('peer:state', ({ peerId, state }) => console.log(`${peerId}: ${state}`));
mesh.on('message', ({ peerId, data }) => console.log(`${peerId.slice(0, 8)}:`, data));
mesh.on('signaling:state', ({ state, attempt }) =>
  console.log(`Signaling: ${state}${attempt ? ` (attempt ${attempt})` : ''}`),
);
mesh.on('error', (error) => console.error(`[${error.code}] ${error.message}`));

await mesh.start();
await mesh.join(roomId);
console.log(`Joined ${roomId} as ${mesh.peerId}. Enter text to broadcast; use /quit to close.`);

const input = createInterface({ input: process.stdin, output: process.stdout });
input.on('line', (line) => {
  if (line === '/quit') {
    input.close();
    void mesh.close();
    return;
  }
  void mesh
    .broadcast({ text: line, from: mesh.peerId }, { acknowledge: true })
    .catch((error: unknown) => console.error(error));
});
input.on('close', () => void mesh.close());
