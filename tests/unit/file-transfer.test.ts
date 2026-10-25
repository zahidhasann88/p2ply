import { describe, expect, it, vi } from 'vitest';
import {
  FileTransferManager,
  decodeChunk,
  encodeChunk,
} from '../../src/file-transfer/transfer-manager.js';
import type { DataEnvelope } from '../../src/protocol/data.js';
import type { FileTransferResult } from '../../src/file-transfer/types.js';

const peerA = '123e4567-e89b-42d3-a456-426614174000';
const peerB = '123e4567-e89b-42d3-a456-426614174001';

function makePair(options: { accept?: boolean; corruptFirstChunk?: boolean } = {}) {
  const pair: { sender?: FileTransferManager; receiver?: FileTransferManager } = {};
  let didCorrupt = false;
  const emitError = vi.fn();
  const emitComplete = vi.fn();
  let resolveReceiverComplete!: (result: FileTransferResult) => void;
  const receiverComplete = new Promise<FileTransferResult>((resolve) => {
    resolveReceiverComplete = resolve;
  });

  const shared = {
    maxFileBytes: 2_000_000,
    chunkSize: 1_024,
    maxMessageBytes: 2_048,
    maxConcurrentTransfers: 4,
    timeoutMs: 5_000,
  };
  const sender = new FileTransferManager({
    ...shared,
    sendControl: async (_peer: string, envelope: DataEnvelope) => {
      await pair.receiver!.handleEnvelope(peerA, envelope);
      return Date.now();
    },
    sendBinary: async (_peer: string, data: Uint8Array) => {
      const copy = data.slice();
      if (options.corruptFirstChunk && !didCorrupt) {
        didCorrupt = true;
        copy[copy.length - 1] = copy[copy.length - 1]! ^ 0xff;
      }
      await pair.receiver!.handleBinary(peerA, copy);
      return Date.now();
    },
    emitOffer: (event) => {
      if (options.accept !== false) void event.accept();
      return 1;
    },
    emitProgress: () => {},
    emitComplete,
    emitError: (event) => emitError(event.error),
  });
  const receiver = new FileTransferManager({
    ...shared,
    sendControl: async (_peer: string, envelope: DataEnvelope) => {
      await pair.sender!.handleEnvelope(peerB, envelope);
      return Date.now();
    },
    sendBinary: async (_peer: string, data: Uint8Array) => {
      await pair.sender!.handleBinary(peerB, data);
      return Date.now();
    },
    emitOffer: (event) => {
      if (options.accept !== false) void event.accept();
      return 1;
    },
    emitProgress: () => {},
    emitComplete: (event) => {
      emitComplete(event);
      resolveReceiverComplete(event.result);
    },
    emitError: (event) => emitError(event.error),
  });
  pair.sender = sender;
  pair.receiver = receiver;
  return { sender, receiver, emitError, emitComplete, receiverComplete };
}

describe('file transfer framing and flow control', () => {
  it('encodes and decodes transfer IDs, sequences, and binary payloads', () => {
    const transferId = '123e4567-e89b-42d3-a456-426614174000';
    const payload = Uint8Array.from([1, 2, 3, 4]);
    const frame = encodeChunk(transferId, 7, payload);
    expect(decodeChunk(frame)).toMatchObject({ transferId, sequence: 7, payload });
    expect(decodeChunk(new Uint8Array([0, 1, 2]))).toBeUndefined();
  });

  it('transfers chunks, verifies SHA-256, and returns a memory-backed Blob', async () => {
    const { sender, receiver, emitComplete, receiverComplete } = makePair();
    const input = Uint8Array.from({ length: 6_123 }, (_, index) => index % 251);
    const blob = Object.assign(new Blob([input], { type: 'application/octet-stream' }), {
      name: 'payload.bin',
      lastModified: 123,
    });
    const handle = await sender.sendFile(peerB, blob);
    const result = await handle.completion;
    const received = await receiverComplete;
    expect(result.size).toBe(input.byteLength);
    expect(result.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(received.blob?.size).toBe(input.byteLength);
    expect(received.sha256).toBe(result.sha256);
    expect(emitComplete).toHaveBeenCalledTimes(2);
    expect(sender.activeCount).toBe(0);
    await sender.close();
    await receiver.close();
  });

  it('rejects corrupted file chunks after end-to-end SHA-256 verification', async () => {
    const { sender, receiver, emitError } = makePair({ corruptFirstChunk: true });
    const blob = Object.assign(new Blob([new Uint8Array(2_500)]), { name: 'corrupt.bin' });
    const handle = await sender.sendFile(peerB, blob);
    await expect(handle.completion).rejects.toMatchObject({ code: 'TRANSFER_CANCELLED' });
    expect(emitError).toHaveBeenCalled();
    await sender.close();
    await receiver.close();
  });

  it('supports cancellation while the receiver is waiting for user acceptance', async () => {
    const { sender, receiver } = makePair({ accept: false });
    const blob = Object.assign(new Blob([new Uint8Array(32)]), { name: 'cancel.bin' });
    const handle = await sender.sendFile(peerB, blob);
    const completion = handle.completion.catch((error: unknown) => error);
    await handle.cancel();
    expect(await completion).toMatchObject({ code: 'TRANSFER_CANCELLED' });
    expect(sender.activeCount).toBe(0);
    await sender.close();
    await receiver.close();
  });
});
