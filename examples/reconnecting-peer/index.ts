import { NodeP2Ply } from '../../src/node.js';

const mesh = await NodeP2Ply.create({
  signalingUrl: process.env['P2PLY_URL'] ?? 'ws://localhost:8787',
  reconnect: { enabled: true, maxAttempts: 10, minDelayMs: 250, maxDelayMs: 20_000, jitter: 0.25 },
  peerReconnect: { auto: true, windowMs: 60_000, maxAttempts: 5 },
});

mesh.on(
  'peer:join',
  ({ peerId }) => void mesh.connect(peerId).catch((error: unknown) => console.error(error)),
);
mesh.on('peer:state', ({ peerId, state }) => console.log(`${peerId.slice(0, 8)}: ${state}`));
mesh.on('message', ({ peerId, data }) => console.log(`${peerId.slice(0, 8)} says`, data));
mesh.on('signaling:state', ({ state, attempt }) =>
  console.log(`Signaling ${state}${attempt ? ` · attempt ${attempt}` : ''}`),
);
mesh.on('error', (error) => console.error(`[${error.code}] ${error.message}`));

await mesh.start();
await mesh.join(process.env['P2PLY_ROOM'] ?? 'resilient-room');

let count = 0;
const timer = setInterval(() => {
  for (const peer of mesh.getPeers()) {
    if (peer.state !== 'connected' && peer.state !== 'reconnecting') continue;
    const n = (count += 1);
    mesh
      .send(peer.peerId, { n }, { acknowledge: true, timeoutMs: 30_000 })
      .then(() => console.log(`#${n} delivered to ${peer.peerId.slice(0, 8)}`))
      .catch((error: unknown) => console.error(`#${n} not delivered:`, error));
  }
}, 3_000);

const stop = (): void => {
  clearInterval(timer);
  void mesh.close();
};
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
