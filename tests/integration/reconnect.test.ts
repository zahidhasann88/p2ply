import { afterEach, describe, expect, it } from 'vitest';
import { NodeP2Ply } from '../../src/node.js';
import { P2PlyError } from '../../src/errors.js';
import { startSignalingServer, type SignalingServer } from '../../src/server/server.js';
import type { P2Ply } from '../../src/p2ply.js';
import type { P2PlyOptions } from '../../src/types.js';

let server: SignalingServer | undefined;
const meshes: P2Ply[] = [];

type Internals = {
  links: Map<string, { fail(error: P2PlyError): void }>;
  signaling?: { socket?: { close(): void } };
};

async function setup(overrides: Partial<P2PlyOptions> = {}) {
  server = await startSignalingServer({ host: '127.0.0.1', port: 0 });
  const address = server.httpServer.address();
  if (!address || typeof address === 'string') throw new Error('Expected TCP address.');
  const options: P2PlyOptions = {
    signalingUrl: `ws://127.0.0.1:${address.port}`,
    reconnect: { minDelayMs: 50, maxDelayMs: 200 },
    peerReconnect: { minDelayMs: 50, maxDelayMs: 500, windowMs: 20_000 },
    connectionTimeoutMs: 12_000,
    negotiationTimeoutMs: 12_000,
    requestTimeoutMs: 15_000,
    transferTimeoutMs: 10_000,
    ...overrides,
  };
  const [a, b] = await Promise.all([NodeP2Ply.create(options), NodeP2Ply.create(options)]);
  meshes.push(a, b);
  const aSeesB = new Promise<void>((resolve) => a.on('peer:join', () => resolve()));
  await Promise.all([a.start(), b.start()]);
  await a.join('room');
  await b.join('room');
  await aSeesB;
  await a.connect(b.peerId);
  return { a, b };
}

/** Simulates a network failure on one side of the peer connection. */
function dropLink(mesh: P2Ply, peerId: string): void {
  (mesh as unknown as Internals).links
    .get(peerId)!
    .fail(new P2PlyError('CONNECTION_FAILED', 'Simulated network drop.'));
}

function waitForState(mesh: P2Ply, peerId: string, state: string): Promise<void> {
  return new Promise((resolve) => {
    if (mesh.getPeer(peerId)?.state === state) return resolve();
    const off = mesh.on('peer:state', (event) => {
      if (event.peerId === peerId && event.state === state) {
        off();
        resolve();
      }
    });
  });
}

afterEach(async () => {
  await Promise.all(meshes.splice(0).map((mesh) => mesh.close()));
  await server?.close();
  server = undefined;
});

describe('automatic peer reconnection (real Node WebRTC)', () => {
  it('reconnects after a drop and delivers messages and requests sent during the outage', async () => {
    const { a, b } = await setup();
    const messages: unknown[] = [];
    b.on('message', ({ data }) => messages.push(data));
    b.on('request', ({ data, respond }) => void respond({ echo: data }));
    const states: string[] = [];
    a.on('peer:state', ({ peerId, state }) => peerId === b.peerId && states.push(state));

    dropLink(a, b.peerId);
    expect(a.getPeer(b.peerId)?.state).toBe('reconnecting');
    const [receipt, answer] = await Promise.all([
      a.send(b.peerId, { during: 'outage' }, { acknowledge: true }),
      a.request(b.peerId, { ping: 1 }),
    ]);

    expect(receipt.acknowledgedAt).toBeGreaterThan(0);
    expect(answer).toEqual({ echo: { ping: 1 } });
    expect(messages).toEqual([{ during: 'outage' }]);
    expect(states).toContain('reconnecting');
    expect(a.getPeer(b.peerId)?.state).toBe('connected');
    await waitForState(b, a.peerId, 'connected');
  });

  it('resumes a file transfer interrupted by a drop and verifies it', async () => {
    const { a, b } = await setup();
    const input = Uint8Array.from({ length: 1_000_000 }, (_, index) => (index * 31) % 251);
    b.on('file:offer', ({ accept }) => void accept());
    const received = new Promise<{ sha256: string; blob?: Blob }>((resolve) =>
      b.on('file:complete', ({ result }) => resolve(result)),
    );
    let dropped = false;
    a.on('file:progress', ({ direction, transferredBytes }) => {
      if (direction === 'send' && transferredBytes > 400_000 && !dropped) {
        dropped = true;
        dropLink(a, b.peerId);
      }
    });

    const handle = await a.sendFile(b.peerId, new Blob([input]), { name: 'big.bin' });
    const sent = await handle.completion;
    const result = await received;

    expect(dropped).toBe(true);
    expect(result.sha256).toBe(sent.sha256);
    expect(new Uint8Array(await result.blob!.arrayBuffer())).toEqual(input);
  });

  it('reconnects peers after a signaling server disconnect', async () => {
    const { a, b } = await setup();
    const messages: unknown[] = [];
    b.on('message', ({ data }) => messages.push(data));

    (a as unknown as Internals).signaling!.socket!.close();
    await waitForState(b, a.peerId, 'reconnecting');
    await waitForState(b, a.peerId, 'connected');
    await waitForState(a, b.peerId, 'connected');
    await a.send(b.peerId, { after: 'signaling drop' }, { acknowledge: true });
    expect(messages).toEqual([{ after: 'signaling drop' }]);
  });

  it('autoConnect connects to peers that were already in the room', async () => {
    server = await startSignalingServer({ host: '127.0.0.1', port: 0 });
    const address = server.httpServer.address();
    if (!address || typeof address === 'string') throw new Error('Expected TCP address.');
    const signalingUrl = `ws://127.0.0.1:${address.port}`;
    const [a, b] = await Promise.all([
      NodeP2Ply.create({ signalingUrl }),
      NodeP2Ply.create({ signalingUrl, autoConnect: true }),
    ]);
    meshes.push(a, b);
    const errors: string[] = [];
    b.on('error', (error) => errors.push(error.code));
    await a.start();
    await a.join('room');
    await b.start();
    await b.join('room');
    await waitForState(b, a.peerId, 'connected');
    expect(errors).toEqual([]);
  });

  it('does not reconnect when the peer leaves on purpose', async () => {
    const { a, b } = await setup();
    await b.leave('room');
    expect(a.getPeer(b.peerId)).toBeUndefined();
    await expect(a.send(b.peerId, { hello: 1 })).rejects.toMatchObject({
      code: 'PEER_NOT_CONNECTED',
    });
  });

  it('gives up after the reconnect window and fails waiting sends', async () => {
    const { a, b } = await setup({
      peerReconnect: { minDelayMs: 50, maxDelayMs: 100, windowMs: 1_500 },
    });
    const errors: string[] = [];
    a.on('error', (error) => errors.push(error.code));
    // B disappears without saying goodbye (like a crashed tab) and never comes back.
    (b as unknown as { sayGoodbye(): Promise<void> }).sayGoodbye = async () => {};
    await b.close();
    await expect(a.send(b.peerId, { lost: true }, { timeoutMs: 5_000 })).rejects.toMatchObject({
      code: 'CONNECTION_FAILED',
    });
    expect(errors).toContain('CONNECTION_FAILED');
  });
});
