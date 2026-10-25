import { describe, expect, it } from 'vitest';
import { sha256 } from '@noble/hashes/sha2.js';
import { bytesToHex } from '@noble/hashes/utils.js';
import { FileTransferManager, decodeChunk } from '../../src/file-transfer/transfer-manager.js';
import type { DataEnvelope } from '../../src/protocol/data.js';
import type { FileTransferResult } from '../../src/file-transfer/types.js';

const peerA = '123e4567-e89b-42d3-a456-426614174000';
const peerB = '123e4567-e89b-42d3-a456-426614174001';

function makeLossyPair(drop: (message: { envelope?: DataEnvelope; chunk?: number }) => boolean) {
  const pair: { sender?: FileTransferManager; receiver?: FileTransferManager } = {};
  let down = false;
  const errors: unknown[] = [];
  let resolveReceived!: (result: FileTransferResult) => void;
  const received = new Promise<FileTransferResult>((resolve) => (resolveReceived = resolve));

  const disconnect = () => {
    down = true;
    pair.sender!.pausePeer(peerB);
    pair.receiver!.pausePeer(peerA);
    setTimeout(() => {
      down = false;
      void pair.receiver!.resumePeer(peerA);
      void pair.sender!.resumePeer(peerB);
    }, 20);
  };
  const deliver = async (
    message: { envelope?: DataEnvelope; chunk?: number },
    run: () => Promise<unknown>,
  ) => {
    if (down) throw Object.assign(new Error('offline'), { code: 'PEER_NOT_CONNECTED' });
    if (drop(message)) {
      disconnect();
      return Date.now();
    }
    await run();
    return Date.now();
  };

  const shared = {
    maxFileBytes: 2_000_000,
    chunkSize: 1_024,
    maxMessageBytes: 2_048,
    maxConcurrentTransfers: 4,
    timeoutMs: 5_000,
    emitOffer: (event: { accept(): Promise<void> }) => {
      void event.accept();
      return 1;
    },
    emitProgress: () => {},
    emitError: (event: { error: unknown }) => errors.push(event.error),
  };
  pair.sender = new FileTransferManager({
    ...shared,
    sendControl: (_peer, envelope) =>
      deliver({ envelope }, () => pair.receiver!.handleEnvelope(peerA, envelope)),
    sendBinary: (_peer, data) =>
      deliver({ chunk: decodeChunk(data)!.sequence }, () =>
        pair.receiver!.handleBinary(peerA, data.slice()),
      ),
    emitComplete: () => {},
  });
  pair.receiver = new FileTransferManager({
    ...shared,
    sendControl: (_peer, envelope) =>
      deliver({ envelope }, () => pair.sender!.handleEnvelope(peerB, envelope)),
    sendBinary: async () => Date.now(),
    emitComplete: (event) => resolveReceived(event.result),
  });
  return { sender: pair.sender, receiver: pair.receiver, errors, received };
}

const input = Uint8Array.from({ length: 10_000 }, (_, index) => (index * 7) % 251);
const file = () => Object.assign(new Blob([input]), { name: 'resume.bin' });
const expectedHash = bytesToHex(sha256(input));

describe('resumable file transfers', () => {
  it('resumes after a chunk is lost in a connection drop', async () => {
    let dropped = false;
    const { sender, receiver, errors, received } = makeLossyPair(({ chunk }) => {
      if (chunk === 4 && !dropped) return (dropped = true);
      return false;
    });
    const handle = await sender.sendFile(peerB, file());
    const result = await handle.completion;
    const incoming = await received;
    expect(dropped).toBe(true);
    expect(result.sha256).toBe(expectedHash);
    expect(incoming.sha256).toBe(expectedHash);
    expect(new Uint8Array(await incoming.blob!.arrayBuffer())).toEqual(input);
    expect(errors).toEqual([]);
    await sender.close();
    await receiver.close();
  });

  it('resumes after a chunk arrived but its acknowledgement was lost', async () => {
    let dropped = false;
    const { sender, receiver, errors, received } = makeLossyPair(({ envelope }) => {
      if (envelope?.type === 'file-chunk-ack' && envelope.sequence === 6 && !dropped)
        return (dropped = true);
      return false;
    });
    const handle = await sender.sendFile(peerB, file());
    const result = await handle.completion;
    const incoming = await received;
    expect(dropped).toBe(true);
    expect(result.sha256).toBe(expectedHash);
    expect(new Uint8Array(await incoming.blob!.arrayBuffer())).toEqual(input);
    expect(errors).toEqual([]);
    await sender.close();
    await receiver.close();
  });

  it('confirms again when the final confirmation was lost', async () => {
    let dropped = false;
    const { sender, receiver, errors } = makeLossyPair(({ envelope }) => {
      if (envelope?.type === 'file-complete-ack' && !dropped) return (dropped = true);
      return false;
    });
    const handle = await sender.sendFile(peerB, file());
    expect((await handle.completion).sha256).toBe(expectedHash);
    expect(dropped).toBe(true);
    expect(errors).toEqual([]);
    await sender.close();
    await receiver.close();
  });

  it('fails paused transfers when the peer never comes back', async () => {
    const { sender, receiver } = makeLossyPair(() => false);
    const handle = await sender.sendFile(peerB, file());
    sender.pausePeer(peerB);
    await sender.cancelPeer(peerB, 'Peer did not reconnect.');
    await expect(handle.completion).rejects.toMatchObject({ code: 'TRANSFER_FAILED' });
    await sender.close();
    await receiver.close();
  });
});
