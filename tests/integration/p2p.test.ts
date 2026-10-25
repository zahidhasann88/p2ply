import { afterEach, describe, expect, it } from 'vitest';
import { NodeP2Ply } from '../../src/node.js';
import { startSignalingServer, type SignalingServer } from '../../src/server/server.js';
import type { P2Ply } from '../../src/p2ply.js';

let server: SignalingServer | undefined;
const meshes: P2Ply[] = [];

async function createPeer(signalingUrl: string): Promise<NodeP2Ply<unknown, unknown>> {
  const mesh = await NodeP2Ply.create({
    signalingUrl,
    reconnect: { enabled: false },
    connectionTimeoutMs: 12_000,
    negotiationTimeoutMs: 12_000,
    disconnectGraceMs: 2_000,
    requestTimeoutMs: 3_000,
    transferTimeoutMs: 8_000,
  });
  meshes.push(mesh);
  return mesh;
}

afterEach(async () => {
  await Promise.all(meshes.splice(0).map((mesh) => mesh.close()));
  await server?.close();
  server = undefined;
});

describe('real Node WebRTC integration through the local signaling server', () => {
  it('connects two real peers, exchanges acknowledged messages and requests, and transfers a verified file', async () => {
    server = await startSignalingServer({ host: '127.0.0.1', port: 0 });
    const address = server.httpServer.address();
    if (!address || typeof address === 'string') throw new Error('Expected TCP address.');
    const signalingUrl = `ws://127.0.0.1:${address.port}`;
    const [a, b] = await Promise.all([createPeer(signalingUrl), createPeer(signalingUrl)]);

    b.on('request', ({ data, respond }) => {
      const value = data as { value: number };
      if (value.value !== -1) void respond({ result: value.value + 1 });
    });
    b.on('file:offer', (offer) => {
      void offer.accept();
    });
    const receivedMessage = new Promise<unknown>((resolve) => {
      b.once('message', (event) => resolve(event.data));
    });
    const receivedFile = new Promise<Blob>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error('Timed out waiting for received file.')),
        10_000,
      );
      b.once('file:complete', ({ result }) => {
        clearTimeout(timeout);
        if (!result.blob) reject(new Error('Expected the default memory sink to return a Blob.'));
        else resolve(result.blob);
      });
    });

    await Promise.all([a.start(), b.start()]);
    await Promise.all([a.join('integration-room'), b.join('integration-room')]);
    expect(a.getPeer(b.peerId)?.state).toBe('discovered');
    expect(b.getPeer(a.peerId)?.state).toBe('discovered');

    await Promise.all([a.connect(b.peerId), b.connect(a.peerId)]);
    const receipt = await a.send(
      b.peerId,
      { kind: 'greeting', text: 'hello peer' },
      { acknowledge: true },
    );
    expect(receipt.acknowledgedAt).toBeTypeOf('number');
    expect(await receivedMessage).toEqual({ kind: 'greeting', text: 'hello peer' });
    expect(await a.request(b.peerId, { value: 41 }, { timeoutMs: 3_000 })).toEqual({ result: 42 });
    await expect(a.request(b.peerId, { value: -1 }, { timeoutMs: 100 })).rejects.toMatchObject({
      code: 'REQUEST_TIMEOUT',
    });

    const contents = Uint8Array.from({ length: 72_001 }, (_, index) => index % 239);
    const file = Object.assign(new Blob([contents.buffer], { type: 'application/octet-stream' }), {
      name: 'proof.bin',
      lastModified: 1_720_000_000_000,
    });
    const transfer = await a.sendFile(b.peerId, file);
    const sentResult = await transfer.completion;
    const blob = await receivedFile;
    const receivedBytes = new Uint8Array(await blob.arrayBuffer());
    expect(blob.size).toBe(contents.length);
    expect(receivedBytes[10_000]).toBe(contents[10_000]);
    expect(sentResult.sha256).toMatch(/^[0-9a-f]{64}$/);

    const diagnostics = await a.getDiagnostics();
    const peerDiagnostics = diagnostics.peers.find((peer) => peer.peerId === b.peerId);
    expect(peerDiagnostics?.state).toBe('connected');
    expect(peerDiagnostics?.transport).toBe('webrtc-datachannel');
    expect(peerDiagnostics?.queuedMessages).toBe(0);
  });

  it('keeps an inbound connection when outbound authorization resolves after an incoming offer', async () => {
    server = await startSignalingServer({ host: '127.0.0.1', port: 0 });
    const address = server.httpServer.address();
    if (!address || typeof address === 'string') throw new Error('Expected TCP address.');
    const signalingUrl = `ws://127.0.0.1:${address.port}`;
    let releaseOutbound!: () => void;
    let markOutboundStarted!: () => void;
    const outboundGate = new Promise<void>((resolve) => {
      releaseOutbound = resolve;
    });
    const outboundStarted = new Promise<void>((resolve) => {
      markOutboundStarted = resolve;
    });
    const a = await NodeP2Ply.create({
      signalingUrl,
      reconnect: { enabled: false },
      connectionTimeoutMs: 5_000,
      negotiationTimeoutMs: 5_000,
      disconnectGraceMs: 2_000,
      requestTimeoutMs: 3_000,
      transferTimeoutMs: 8_000,
      authorizePeer: async ({ direction }) => {
        if (direction === 'outbound') {
          markOutboundStarted();
          await outboundGate;
        }
        return true;
      },
    });
    meshes.push(a);
    const b = await createPeer(signalingUrl);
    await Promise.all([a.start(), b.start()]);
    await Promise.all([a.join('authorization-race-room'), b.join('authorization-race-room')]);

    const outboundAttempt = a.connect(b.peerId);
    void outboundAttempt.catch(() => {});
    await outboundStarted;
    const inboundConnected = new Promise<void>((resolve) => {
      let unsubscribe = (): void => {};
      unsubscribe = a.on('peer:state', ({ peerId, state }) => {
        if (peerId === b.peerId && state === 'connected') {
          unsubscribe();
          resolve();
        }
      });
      if (a.getPeer(b.peerId)?.state === 'connected') {
        unsubscribe();
        resolve();
      }
    });
    try {
      await b.connect(a.peerId);
      await inboundConnected;
    } finally {
      releaseOutbound();
    }
    await outboundAttempt;
    expect(a.getPeer(b.peerId)?.state).toBe('connected');
  }, 15_000);

  it('cleans room membership and permits an explicit peer reconnection', async () => {
    server = await startSignalingServer({ host: '127.0.0.1', port: 0 });
    const address = server.httpServer.address();
    if (!address || typeof address === 'string') throw new Error('Expected TCP address.');
    const signalingUrl = `ws://127.0.0.1:${address.port}`;
    const [a, b] = await Promise.all([createPeer(signalingUrl), createPeer(signalingUrl)]);
    await Promise.all([a.start(), b.start()]);
    await Promise.all([a.join('reconnect-room'), b.join('reconnect-room')]);
    await a.connect(b.peerId);
    expect(a.getPeer(b.peerId)?.state).toBe('connected');

    await b.leave('reconnect-room');
    expect(a.getPeer(b.peerId)).toBeUndefined();
    await expect(a.send(b.peerId, { disconnected: true })).rejects.toMatchObject({
      code: 'PEER_NOT_CONNECTED',
    });
    await b.join('reconnect-room');
    const rediscovered = new Promise<void>((resolve) => {
      if (a.getPeer(b.peerId)) resolve();
      else {
        const unsubscribe = a.on('peer:join', ({ peerId }) => {
          if (peerId === b.peerId) {
            unsubscribe();
            resolve();
          }
        });
      }
    });
    await rediscovered;
    expect(a.getPeer(b.peerId)?.state).toBe('discovered');
    await a.reconnectPeer(b.peerId);
    expect(a.getPeer(b.peerId)?.state).toBe('connected');
  });
});
