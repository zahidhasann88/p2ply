import { NodeP2Ply } from '../../src/node.js';

interface ChatMessage {
  kind: 'chat';
  text: string;
  sentAt: number;
}

function isChatMessage(value: unknown): value is ChatMessage {
  if (typeof value !== 'object' || value === null) return false;
  const candidate = value as Record<string, unknown>;
  return (
    candidate.kind === 'chat' &&
    typeof candidate.text === 'string' &&
    typeof candidate.sentAt === 'number'
  );
}

const mesh = await NodeP2Ply.create<ChatMessage>({
  signalingUrl: process.env['P2PLY_URL'] ?? 'ws://localhost:8787',
  validateMessage: isChatMessage,
});

mesh.on('message', ({ peerId, data }) => {
  console.log(`${peerId}: ${data.text} (${new Date(data.sentAt).toISOString()})`);
});
mesh.on('error', (error) => console.error(`[${error.code}] ${error.message}`));

await mesh.start();
await mesh.join(process.env['P2PLY_ROOM'] ?? 'typed-room');
for (const peer of mesh.getPeers()) {
  await mesh.connect(peer.peerId);
  await mesh.send(
    peer.peerId,
    { kind: 'chat', text: 'Hello from a typed P2Ply!', sentAt: Date.now() },
    { acknowledge: true },
  );
}
