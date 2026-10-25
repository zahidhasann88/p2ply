import { describe, expect, it, vi } from 'vitest';
import { P2Ply } from '../../src/p2ply.js';
import type { PeerState } from '../../src/types.js';

const remotePeerId = '123e4567-e89b-42d3-a456-426614174001';
const duplicateId = '123e4567-e89b-42d3-a456-426614174002';

type InternalMesh = {
  links: Map<
    string,
    { currentState: PeerState; close: () => void; sendRaw?: () => Promise<number> }
  >;
  handlePeerData(peerId: string, data: unknown): Promise<void>;
};

function testMesh(
  options: ConstructorParameters<typeof P2Ply>[0] = { signalingUrl: 'ws://localhost:8787' },
): { mesh: P2Ply<unknown>; internal: InternalMesh; close: ReturnType<typeof vi.fn> } {
  const mesh = new P2Ply(options);
  const close = vi.fn();
  const internal = mesh as unknown as InternalMesh;
  internal.links.set(remotePeerId, {
    currentState: 'connected',
    close,
    sendRaw: async () => Date.now(),
  });
  return { mesh, internal, close };
}

describe('P2Ply inbound application protocol', () => {
  it('suppresses duplicate message IDs and emits a single application event', async () => {
    const { mesh, internal } = testMesh();
    const received = vi.fn();
    mesh.on('message', received);
    const envelope = JSON.stringify({
      v: 1,
      type: 'app',
      id: duplicateId,
      payload: { text: 'once' },
      ackRequested: false,
    });
    await internal.handlePeerData(remotePeerId, envelope);
    await internal.handlePeerData(remotePeerId, envelope);
    expect(received).toHaveBeenCalledTimes(1);
    expect(received).toHaveBeenCalledWith({
      peerId: remotePeerId,
      messageId: duplicateId,
      data: { text: 'once' },
    });
    await mesh.close();
  });

  it('applies optional runtime payload validation and closes peers with malformed envelopes', async () => {
    const { mesh, internal, close } = testMesh({
      signalingUrl: 'ws://localhost:8787',
      validateMessage: (value: unknown): value is { text: string } =>
        typeof value === 'object' &&
        value !== null &&
        'text' in value &&
        typeof value.text === 'string',
    });
    const errors: string[] = [];
    mesh.on('error', (error) => errors.push(error.code));
    await internal.handlePeerData(
      remotePeerId,
      JSON.stringify({ v: 1, type: 'app', id: duplicateId, payload: 7, ackRequested: false }),
    );
    expect(errors).toContain('INVALID_MESSAGE');
    expect(close).not.toHaveBeenCalled();
    await internal.handlePeerData(remotePeerId, '{broken');
    expect(errors).toContain('SIGNALING_PROTOCOL_ERROR');
    expect(close).toHaveBeenCalledTimes(1);
    await mesh.close();
  });

  it('rejects oversized incoming messages before parsing', async () => {
    const { mesh, internal, close } = testMesh({
      signalingUrl: 'ws://localhost:8787',
      maxMessageBytes: 2_048,
      fileChunkSize: 1_024,
    });
    const errors: string[] = [];
    mesh.on('error', (error) => errors.push(error.code));
    await internal.handlePeerData(remotePeerId, ' '.repeat(3_000));
    expect(errors).toContain('MESSAGE_TOO_LARGE');
    expect(close).toHaveBeenCalledOnce();
    await mesh.close();
  });
});
