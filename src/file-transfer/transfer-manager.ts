import { sha256 } from '@noble/hashes/sha2.js';
import { bytesToHex } from '@noble/hashes/utils.js';
import { P2PlyError, TransferError, asP2PlyError } from '../errors.js';
import type { DataEnvelope, FileStartEnvelope } from '../protocol/data.js';
import { bytesToUuid, randomId, uuidToBytes } from '../utils/id.js';
import { deferred, withTimeout } from '../utils/async.js';
import type { Deferred } from '../utils/async.js';
import type {
  FileInput,
  FileSink,
  FileSinkFactory,
  FileTransferResult,
  TransferMetadata,
} from './types.js';

const FRAME_MAGIC_0 = 0x50;
const FRAME_MAGIC_1 = 0x32;
const FRAME_VERSION = 1;
const FRAME_HEADER_BYTES = 23;
const MAX_RECENTLY_COMPLETED = 64;

export interface TransferManagerOptions {
  maxFileBytes: number;
  chunkSize: number;
  maxMessageBytes: number;
  maxConcurrentTransfers: number;
  timeoutMs: number;
  sinkFactory?: FileSinkFactory;
  sendControl(peerId: string, envelope: DataEnvelope): Promise<number>;
  sendBinary(peerId: string, data: Uint8Array): Promise<number>;
  emitOffer(event: {
    peerId: string;
    transferId: string;
    metadata: TransferMetadata;
    accept(): Promise<void>;
    reject(reason?: string): Promise<void>;
  }): number;
  emitProgress(event: {
    peerId: string;
    transferId: string;
    direction: 'send' | 'receive';
    transferredBytes: number;
    totalBytes: number;
  }): void;
  emitComplete(event: { peerId: string; transferId: string; result: FileTransferResult }): void;
  emitError(event: { peerId: string; transferId: string; error: P2PlyError }): void;
}

interface SenderTransfer {
  kind: 'send';
  peerId: string;
  transferId: string;
  file: FileInput;
  metadata: TransferMetadata;
  ready: Deferred<void>;
  readyReceived: boolean;
  completion: Deferred<FileTransferResult>;
  currentAck?: { sequence: number; deferred: Deferred<void> };
  nextSequence: number;
  hasher: ReturnType<typeof sha256.create>;
  expectedHash?: string;
  cancelled: boolean;
  paused: boolean;
  interrupt: Deferred<never>;
  resumed?: Deferred<void>;
}

interface ReceiverTransfer {
  kind: 'receive';
  peerId: string;
  transferId: string;
  metadata: TransferMetadata;
  hasher: ReturnType<typeof sha256.create>;
  sink?: FileSink;
  receivedBytes: number;
  nextSequence: number;
  accepted: boolean;
  offerTimer?: ReturnType<typeof setTimeout>;
  activityTimer?: ReturnType<typeof setTimeout>;
  finished: boolean;
  paused: boolean;
}

type ActiveTransfer = SenderTransfer | ReceiverTransfer;

class MemorySink implements FileSink {
  private readonly chunks: ArrayBuffer[] = [];

  write(chunk: Uint8Array): void {
    const copy = new Uint8Array(chunk.byteLength);
    copy.set(chunk);
    this.chunks.push(copy.buffer);
  }

  close(): Blob {
    return new Blob(this.chunks);
  }

  abort(): void {
    this.chunks.length = 0;
  }
}

export class FileTransferManager {
  private readonly transfers = new Map<string, ActiveTransfer>();
  private readonly recentlyCompleted = new Map<string, { peerId: string; totalChunks: number }>();
  private closed = false;

  constructor(private readonly options: TransferManagerOptions) {}

  async sendFile(
    peerId: string,
    file: FileInput,
    requestedName?: string,
  ): Promise<{
    transferId: string;
    completion: Promise<FileTransferResult>;
    cancel(): Promise<void>;
  }> {
    this.assertOpen();
    if (file.size > this.options.maxFileBytes) {
      throw new TransferError(
        'TRANSFER_TOO_LARGE',
        `File is ${file.size} bytes; limit is ${this.options.maxFileBytes} bytes.`,
      );
    }
    this.assertCapacity();

    const transferId = randomId();
    const chunkSize = this.options.chunkSize;
    const name = safeFileName(requestedName ?? file.name ?? 'file');
    const metadata: TransferMetadata = {
      name,
      size: file.size,
      mimeType: safeMimeType(file.type),
      lastModified:
        Number.isSafeInteger(file.lastModified) && (file.lastModified ?? 0) >= 0
          ? file.lastModified!
          : 0,
      chunkSize,
      totalChunks: Math.ceil(file.size / chunkSize),
    };
    const state: SenderTransfer = {
      kind: 'send',
      peerId,
      transferId,
      file,
      metadata,
      ready: deferred<void>(),
      readyReceived: false,
      completion: deferred<FileTransferResult>(),
      nextSequence: 0,
      hasher: sha256.create(),
      cancelled: false,
      paused: false,
      interrupt: interruptSignal(),
    };
    void state.completion.promise.catch(() => {});
    void state.ready.promise.catch(() => {});
    this.transfers.set(transferId, state);

    try {
      await this.options.sendControl(peerId, { v: 1, type: 'file-start', transferId, metadata });
    } catch (error) {
      this.finishSenderFailure(
        state,
        asP2PlyError(error, 'TRANSFER_FAILED', 'Could not start file transfer.'),
        false,
      );
      throw error;
    }

    void this.runSender(state);
    return {
      transferId,
      completion: state.completion.promise,
      cancel: () => this.cancelTransfer(transferId),
    };
  }

  async handleEnvelope(peerId: string, envelope: DataEnvelope): Promise<boolean> {
    if (this.closed) return false;
    switch (envelope.type) {
      case 'file-start':
        await this.receiveOffer(peerId, envelope);
        return true;
      case 'file-ready': {
        const state = this.getSender(peerId, envelope.transferId);
        if (state) {
          state.readyReceived = true;
          state.ready.resolve();
        }
        return true;
      }
      case 'file-reject': {
        const state = this.getSender(peerId, envelope.transferId);
        if (state)
          this.finishSenderFailure(
            state,
            new TransferError('TRANSFER_REJECTED', envelope.reason),
            false,
          );
        return true;
      }
      case 'file-chunk-ack': {
        const state = this.getSender(peerId, envelope.transferId);
        if (state?.currentAck?.sequence === envelope.sequence) state.currentAck.deferred.resolve();
        return true;
      }
      case 'file-complete':
        await this.receiveComplete(peerId, envelope.transferId, envelope.sha256);
        return true;
      case 'file-complete-ack': {
        const state = this.getSender(peerId, envelope.transferId);
        if (state?.expectedHash) {
          const result: FileTransferResult = {
            transferId: state.transferId,
            peerId: state.peerId,
            name: state.metadata.name,
            size: state.metadata.size,
            mimeType: state.metadata.mimeType,
            sha256: state.expectedHash,
          };
          this.finishSenderSuccess(state, result);
        }
        return true;
      }
      case 'file-resume':
        await this.receiveResume(peerId, envelope.transferId);
        return true;
      case 'file-resume-ack': {
        const state = this.getSender(peerId, envelope.transferId);
        if (state?.paused && state.resumed) await this.resumeSender(state, envelope.nextSequence);
        return true;
      }
      case 'file-cancel':
        await this.receiveCancel(peerId, envelope.transferId, envelope.reason);
        return true;
      default:
        return false;
    }
  }

  async handleBinary(
    peerId: string,
    value: ArrayBuffer | ArrayBufferView | Blob,
  ): Promise<boolean> {
    if (this.closed) return false;
    let bytes: Uint8Array;
    if (value instanceof Blob) bytes = new Uint8Array(await value.arrayBuffer());
    else if (value instanceof ArrayBuffer) bytes = new Uint8Array(value);
    else bytes = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);

    const parsed = decodeChunk(bytes);
    if (!parsed) return false;
    const state = this.transfers.get(parsed.transferId);
    if (!state || state.kind !== 'receive' || state.peerId !== peerId) {
      await this.options
        .sendControl(peerId, {
          v: 1,
          type: 'file-cancel',
          transferId: parsed.transferId,
          reason: 'Unknown or inactive transfer.',
        })
        .catch(() => {});
      return true;
    }
    if (!state.accepted || !state.sink || state.finished) {
      await this.failReceiver(
        state,
        new TransferError(
          'TRANSFER_FAILED',
          'Received a file chunk before accepting the transfer.',
        ),
        true,
      );
      return true;
    }

    if (parsed.sequence < state.nextSequence) {
      await this.options.sendControl(peerId, {
        v: 1,
        type: 'file-chunk-ack',
        transferId: state.transferId,
        sequence: parsed.sequence,
      });
      return true;
    }
    const expectedLength = Math.min(
      state.metadata.chunkSize,
      state.metadata.size - state.receivedBytes,
    );
    if (
      parsed.sequence !== state.nextSequence ||
      parsed.payload.byteLength !== expectedLength ||
      parsed.payload.byteLength === 0
    ) {
      await this.failReceiver(
        state,
        new TransferError(
          'TRANSFER_FAILED',
          'File chunk sequence or length did not match the transfer metadata.',
        ),
        true,
      );
      return true;
    }

    try {
      state.hasher.update(parsed.payload);
      await state.sink.write(parsed.payload);
      state.receivedBytes += parsed.payload.byteLength;
      state.nextSequence += 1;
      this.resetActivityTimer(state);
      this.options.emitProgress({
        peerId,
        transferId: state.transferId,
        direction: 'receive',
        transferredBytes: state.receivedBytes,
        totalBytes: state.metadata.size,
      });
      await this.options.sendControl(peerId, {
        v: 1,
        type: 'file-chunk-ack',
        transferId: state.transferId,
        sequence: parsed.sequence,
      });
    } catch (error) {
      await this.failReceiver(
        state,
        asP2PlyError(error, 'TRANSFER_FAILED', 'Could not write an incoming file chunk.'),
        true,
      );
    }
    return true;
  }

  async cancelTransfer(transferId: string): Promise<void> {
    const state = this.transfers.get(transferId);
    if (!state) return;
    const reason = 'Transfer cancelled locally.';
    await this.options
      .sendControl(state.peerId, { v: 1, type: 'file-cancel', transferId, reason })
      .catch(() => {});
    if (state.kind === 'send') {
      state.cancelled = true;
      this.finishSenderFailure(state, new TransferError('TRANSFER_CANCELLED', reason), false);
    } else {
      await this.failReceiver(state, new TransferError('TRANSFER_CANCELLED', reason), false);
    }
  }

  async cancelPeer(peerId: string, reason = 'Peer connection closed.'): Promise<void> {
    const states = [...this.transfers.values()].filter((state) => state.peerId === peerId);
    for (const state of states) {
      if (state.kind === 'send')
        this.finishSenderFailure(state, new TransferError('TRANSFER_FAILED', reason), false);
      else await this.failReceiver(state, new TransferError('TRANSFER_FAILED', reason), false);
    }
  }

  pausePeer(peerId: string): void {
    for (const state of this.transfers.values()) {
      if (state.peerId !== peerId || state.paused) continue;
      state.paused = true;
      if (state.kind === 'send') {
        state.resumed = deferred<void>();
        void state.resumed.promise.catch(() => {});
        state.interrupt.reject(new P2PlyError('PEER_NOT_CONNECTED', 'Transfer paused.'));
      } else {
        if (state.activityTimer) clearTimeout(state.activityTimer);
        state.activityTimer = undefined;
      }
    }
  }

  async resumePeer(peerId: string): Promise<void> {
    const states = [...this.transfers.values()].filter(
      (state) => state.peerId === peerId && state.paused,
    );
    for (const state of states) {
      try {
        if (state.kind === 'receive') {
          state.paused = false;
          if (state.accepted) this.resetActivityTimer(state);
        } else if (!state.readyReceived) {
          await this.options.sendControl(peerId, {
            v: 1,
            type: 'file-start',
            transferId: state.transferId,
            metadata: state.metadata,
          });
          this.unpauseSender(state);
        } else {
          await this.options.sendControl(peerId, {
            v: 1,
            type: 'file-resume',
            transferId: state.transferId,
          });
        }
      } catch {
        // Still unreachable: stay paused. The caller fails the transfer if the peer never returns.
      }
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    const states = [...this.transfers.values()];
    for (const state of states) {
      if (state.kind === 'send')
        this.finishSenderFailure(
          state,
          new P2PlyError('CLOSED', 'P2Ply closed during file transfer.'),
          false,
        );
      else
        await this.failReceiver(
          state,
          new P2PlyError('CLOSED', 'P2Ply closed during file transfer.'),
          false,
        );
    }
  }

  get activeCount(): number {
    return this.transfers.size;
  }

  activeCountForPeer(peerId: string): number {
    let count = 0;
    for (const state of this.transfers.values()) if (state.peerId === peerId) count += 1;
    return count;
  }

  private async runSender(state: SenderTransfer): Promise<void> {
    const { timeoutMs } = this.options;
    try {
      while (
        !(await this.attempt(state, () =>
          this.step(
            state,
            state.ready.promise,
            timeoutMs,
            'The receiver did not accept the file transfer in time.',
          ),
        ))
      );
      while (state.nextSequence < state.metadata.totalChunks) {
        if (state.cancelled) return;
        const sequence = state.nextSequence;
        const chunk = await this.readChunk(state, sequence);
        const ack = deferred<void>();
        void ack.promise.catch(() => {});
        state.currentAck = { sequence, deferred: ack };
        const delivered = await this.attempt(state, async () => {
          await Promise.race([
            this.options.sendBinary(state.peerId, encodeChunk(state.transferId, sequence, chunk)),
            state.interrupt.promise,
          ]);
          await this.step(
            state,
            ack.promise,
            timeoutMs,
            `Timed out waiting for file chunk ${sequence} acknowledgement.`,
          );
        });
        state.currentAck = undefined;
        if (!delivered) continue;
        state.hasher.update(chunk);
        state.nextSequence = sequence + 1;
        this.options.emitProgress({
          peerId: state.peerId,
          transferId: state.transferId,
          direction: 'send',
          transferredBytes: Math.min(
            state.metadata.size,
            (sequence + 1) * state.metadata.chunkSize,
          ),
          totalBytes: state.metadata.size,
        });
      }
      state.expectedHash ??= bytesToHex(state.hasher.digest());
      const expectedHash = state.expectedHash;
      while (
        !(await this.attempt(state, async () => {
          await this.options.sendControl(state.peerId, {
            v: 1,
            type: 'file-complete',
            transferId: state.transferId,
            sha256: expectedHash,
          });
          await this.step(
            state,
            state.completion.promise,
            timeoutMs,
            'Timed out waiting for receiver integrity confirmation.',
          );
        }))
      );
    } catch (error) {
      if (this.transfers.get(state.transferId) !== state) return;
      const meshError =
        error instanceof P2PlyError
          ? error
          : asP2PlyError(error, 'TRANSFER_FAILED', 'File transfer failed.');
      await this.options
        .sendControl(state.peerId, {
          v: 1,
          type: 'file-cancel',
          transferId: state.transferId,
          reason: meshError.message.slice(0, 512),
        })
        .catch(() => {});
      this.finishSenderFailure(state, meshError, true);
    }
  }

  private async attempt(state: SenderTransfer, run: () => Promise<unknown>): Promise<boolean> {
    try {
      await run();
      return true;
    } catch (error) {
      if (!state.paused || !state.resumed) throw error;
      await state.resumed.promise;
      return false;
    }
  }

  private step<T>(
    state: SenderTransfer,
    promise: Promise<T>,
    timeoutMs: number,
    message: string,
  ): Promise<T> {
    return withTimeout(
      Promise.race([promise, state.interrupt.promise]),
      timeoutMs,
      'TRANSFER_FAILED',
      message,
    );
  }

  private async readChunk(state: SenderTransfer, sequence: number): Promise<Uint8Array> {
    const start = sequence * state.metadata.chunkSize;
    const end = Math.min(state.metadata.size, start + state.metadata.chunkSize);
    return new Uint8Array(await state.file.slice(start, end).arrayBuffer());
  }

  private async resumeSender(state: SenderTransfer, receiverNext: number): Promise<void> {
    // Only one chunk can be in flight, so the receiver is at most one chunk ahead.
    if (receiverNext < state.nextSequence || receiverNext > state.nextSequence + 1) {
      this.finishSenderFailure(
        state,
        new TransferError('TRANSFER_FAILED', 'Receiver resume position is inconsistent.'),
      );
      return;
    }
    if (receiverNext === state.nextSequence + 1) {
      state.hasher.update(await this.readChunk(state, state.nextSequence));
      state.nextSequence = receiverNext;
    }
    this.unpauseSender(state);
  }

  private unpauseSender(state: SenderTransfer): void {
    if (this.transfers.get(state.transferId) !== state) return;
    state.paused = false;
    state.interrupt = interruptSignal();
    state.resumed?.resolve();
    state.resumed = undefined;
  }

  private async receiveOffer(peerId: string, envelope: FileStartEnvelope): Promise<void> {
    const { transferId, metadata: incoming } = envelope;
    const metadata: TransferMetadata = {
      ...incoming,
      name: safeFileName(incoming.name),
      mimeType: safeMimeType(incoming.mimeType),
    };
    if (this.transfers.has(transferId)) {
      const existing = this.transfers.get(transferId);
      if (existing?.kind === 'receive' && existing.peerId === peerId && existing.accepted) {
        await this.options.sendControl(peerId, { v: 1, type: 'file-ready', transferId });
      }
      return;
    }
    if (metadata.size > this.options.maxFileBytes) {
      await this.options.sendControl(peerId, {
        v: 1,
        type: 'file-reject',
        transferId,
        reason: 'File exceeds receiver size limit.',
      });
      return;
    }
    if (
      metadata.chunkSize + FRAME_HEADER_BYTES > this.options.maxMessageBytes ||
      metadata.totalChunks !== Math.ceil(metadata.size / metadata.chunkSize)
    ) {
      await this.options.sendControl(peerId, {
        v: 1,
        type: 'file-reject',
        transferId,
        reason: 'File metadata is inconsistent or exceeds receiver limits.',
      });
      return;
    }
    if (this.transfers.size >= this.options.maxConcurrentTransfers) {
      await this.options.sendControl(peerId, {
        v: 1,
        type: 'file-reject',
        transferId,
        reason: 'Receiver has reached its concurrent transfer limit.',
      });
      return;
    }

    const state: ReceiverTransfer = {
      kind: 'receive',
      peerId,
      transferId,
      metadata,
      hasher: sha256.create(),
      receivedBytes: 0,
      nextSequence: 0,
      accepted: false,
      finished: false,
      paused: false,
    };
    this.transfers.set(transferId, state);
    state.offerTimer = setTimeout(() => {
      void this.failReceiver(
        state,
        new TransferError('TRANSFER_REJECTED', 'Incoming file offer expired.'),
        true,
      );
    }, this.options.timeoutMs);

    const listenerCount = this.options.emitOffer({
      peerId,
      transferId,
      metadata,
      accept: () => this.acceptReceiver(state),
      reject: (reason = 'Incoming file was rejected.') => this.rejectReceiver(state, reason),
    });
    if (listenerCount === 0)
      await this.rejectReceiver(state, 'No file offer handler is registered.');
  }

  private async acceptReceiver(state: ReceiverTransfer): Promise<void> {
    if (this.transfers.get(state.transferId) !== state || state.finished) return;
    if (state.accepted) return;
    try {
      state.sink = this.options.sinkFactory
        ? await this.options.sinkFactory({
            peerId: state.peerId,
            transferId: state.transferId,
            metadata: state.metadata,
          })
        : new MemorySink();
      state.accepted = true;
      if (state.offerTimer) clearTimeout(state.offerTimer);
      state.offerTimer = undefined;
      this.resetActivityTimer(state);
      await this.options.sendControl(state.peerId, {
        v: 1,
        type: 'file-ready',
        transferId: state.transferId,
      });
    } catch (error) {
      await this.failReceiver(
        state,
        asP2PlyError(error, 'TRANSFER_FAILED', 'Could not prepare an incoming file sink.'),
        true,
      );
      throw error;
    }
  }

  private async rejectReceiver(state: ReceiverTransfer, reason: string): Promise<void> {
    if (this.transfers.get(state.transferId) !== state) return;
    await this.options
      .sendControl(state.peerId, {
        v: 1,
        type: 'file-reject',
        transferId: state.transferId,
        reason: reason.slice(0, 512),
      })
      .catch(() => {});
    await this.failReceiver(state, new TransferError('TRANSFER_REJECTED', reason), false);
  }

  private async receiveComplete(
    peerId: string,
    transferId: string,
    expectedHash: string,
  ): Promise<void> {
    const state = this.getReceiver(peerId, transferId);
    if (!state && this.recentlyCompleted.get(transferId)?.peerId === peerId) {
      await this.options.sendControl(peerId, { v: 1, type: 'file-complete-ack', transferId });
      return;
    }
    if (!state || !state.accepted || state.finished) return;
    if (
      state.receivedBytes !== state.metadata.size ||
      state.nextSequence !== state.metadata.totalChunks
    ) {
      await this.failReceiver(
        state,
        new TransferError(
          'TRANSFER_FAILED',
          'Transfer ended before the advertised file size was received.',
        ),
        true,
      );
      return;
    }
    const actualHash = bytesToHex(state.hasher.digest());
    if (actualHash !== expectedHash.toLowerCase()) {
      await this.failReceiver(
        state,
        new TransferError('TRANSFER_FAILED', 'SHA-256 integrity verification failed.'),
        true,
      );
      return;
    }

    try {
      const sinkResult = await state.sink!.close({ ...state.metadata, sha256: actualHash });
      const result: FileTransferResult = {
        transferId,
        peerId,
        name: state.metadata.name,
        size: state.metadata.size,
        mimeType: state.metadata.mimeType,
        sha256: actualHash,
        ...(sinkResult instanceof Blob
          ? { blob: sinkResult }
          : sinkResult === undefined
            ? {}
            : { sinkResult }),
      };
      state.finished = true;
      this.clearReceiverTimers(state);
      this.transfers.delete(transferId);
      this.rememberCompleted(transferId, peerId, state.metadata.totalChunks);
      this.options.emitComplete({ peerId, transferId, result });
      // If this is lost, the sender resumes and asks again (see recentlyCompleted).
      await this.options
        .sendControl(peerId, { v: 1, type: 'file-complete-ack', transferId })
        .catch(() => {});
    } catch (error) {
      await this.failReceiver(
        state,
        asP2PlyError(error, 'TRANSFER_FAILED', 'Could not finalize the received file.'),
        true,
      );
    }
  }

  private async receiveResume(peerId: string, transferId: string): Promise<void> {
    const state = this.getReceiver(peerId, transferId);
    const completed = this.recentlyCompleted.get(transferId);
    if (!state && completed?.peerId === peerId) {
      await this.options.sendControl(peerId, {
        v: 1,
        type: 'file-resume-ack',
        transferId,
        nextSequence: completed.totalChunks,
      });
      return;
    }
    if (!state || !state.accepted || state.finished) {
      await this.options
        .sendControl(peerId, {
          v: 1,
          type: 'file-cancel',
          transferId,
          reason: 'Unknown or inactive transfer.',
        })
        .catch(() => {});
      return;
    }
    state.paused = false;
    this.resetActivityTimer(state);
    await this.options.sendControl(peerId, {
      v: 1,
      type: 'file-resume-ack',
      transferId,
      nextSequence: state.nextSequence,
    });
  }

  private rememberCompleted(transferId: string, peerId: string, totalChunks: number): void {
    this.recentlyCompleted.set(transferId, { peerId, totalChunks });
    if (this.recentlyCompleted.size > MAX_RECENTLY_COMPLETED) {
      const oldest = this.recentlyCompleted.keys().next().value as string | undefined;
      if (oldest) this.recentlyCompleted.delete(oldest);
    }
  }

  private async receiveCancel(peerId: string, transferId: string, reason: string): Promise<void> {
    const state = this.transfers.get(transferId);
    if (!state || state.peerId !== peerId) return;
    const error = new TransferError(
      'TRANSFER_CANCELLED',
      reason || 'Remote peer cancelled the transfer.',
    );
    if (state.kind === 'send') this.finishSenderFailure(state, error, false);
    else await this.failReceiver(state, error, false);
  }

  private async failReceiver(
    state: ReceiverTransfer,
    error: P2PlyError,
    notifyRemote: boolean,
  ): Promise<void> {
    if (this.transfers.get(state.transferId) !== state) return;
    state.finished = true;
    this.clearReceiverTimers(state);
    this.transfers.delete(state.transferId);
    try {
      await state.sink?.abort(error.message);
    } catch {
      // Do not mask the primary transfer failure with sink cleanup errors.
    }
    if (notifyRemote) {
      await this.options
        .sendControl(state.peerId, {
          v: 1,
          type: 'file-cancel',
          transferId: state.transferId,
          reason: error.message.slice(0, 512),
        })
        .catch(() => {});
    }
    this.options.emitError({ peerId: state.peerId, transferId: state.transferId, error });
  }

  private finishSenderSuccess(state: SenderTransfer, result: FileTransferResult): void {
    if (this.transfers.get(state.transferId) !== state) return;
    this.transfers.delete(state.transferId);
    state.completion.resolve(result);
    this.options.emitComplete({ peerId: state.peerId, transferId: state.transferId, result });
  }

  private finishSenderFailure(state: SenderTransfer, error: P2PlyError, emit = true): void {
    if (this.transfers.get(state.transferId) !== state) return;
    this.transfers.delete(state.transferId);
    state.cancelled = true;
    state.ready.reject(error);
    state.currentAck?.deferred.reject(error);
    state.interrupt.reject(error);
    state.resumed?.reject(error);
    state.completion.reject(error);
    if (emit) this.options.emitError({ peerId: state.peerId, transferId: state.transferId, error });
  }

  private resetActivityTimer(state: ReceiverTransfer): void {
    if (state.activityTimer) clearTimeout(state.activityTimer);
    state.activityTimer = setTimeout(() => {
      void this.failReceiver(
        state,
        new TransferError('TRANSFER_FAILED', 'Incoming file transfer timed out due to inactivity.'),
        true,
      );
    }, this.options.timeoutMs);
  }

  private clearReceiverTimers(state: ReceiverTransfer): void {
    if (state.offerTimer) clearTimeout(state.offerTimer);
    if (state.activityTimer) clearTimeout(state.activityTimer);
    state.offerTimer = undefined;
    state.activityTimer = undefined;
  }

  private getSender(peerId: string, transferId: string): SenderTransfer | undefined {
    const state = this.transfers.get(transferId);
    return state?.kind === 'send' && state.peerId === peerId ? state : undefined;
  }

  private getReceiver(peerId: string, transferId: string): ReceiverTransfer | undefined {
    const state = this.transfers.get(transferId);
    return state?.kind === 'receive' && state.peerId === peerId ? state : undefined;
  }

  private assertCapacity(): void {
    if (this.transfers.size >= this.options.maxConcurrentTransfers) {
      throw new P2PlyError(
        'SERVER_LIMIT_REACHED',
        'Maximum concurrent file transfer limit reached.',
      );
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new P2PlyError('CLOSED', 'File transfer manager has been closed.');
  }
}

function interruptSignal(): Deferred<never> {
  const signal = deferred<never>();
  void signal.promise.catch(() => {});
  return signal;
}

export function encodeChunk(transferId: string, sequence: number, payload: Uint8Array): Uint8Array {
  if (!Number.isInteger(sequence) || sequence < 0 || sequence > 0xffff_ffff)
    throw new Error('Invalid chunk sequence.');
  const frame = new Uint8Array(FRAME_HEADER_BYTES + payload.byteLength);
  frame[0] = FRAME_MAGIC_0;
  frame[1] = FRAME_MAGIC_1;
  frame[2] = FRAME_VERSION;
  frame.set(uuidToBytes(transferId), 3);
  new DataView(frame.buffer).setUint32(19, sequence, false);
  frame.set(payload, FRAME_HEADER_BYTES);
  return frame;
}

export function decodeChunk(
  frame: Uint8Array,
): { transferId: string; sequence: number; payload: Uint8Array } | undefined {
  if (
    frame.byteLength < FRAME_HEADER_BYTES ||
    frame[0] !== FRAME_MAGIC_0 ||
    frame[1] !== FRAME_MAGIC_1 ||
    frame[2] !== FRAME_VERSION
  )
    return undefined;
  try {
    return {
      transferId: bytesToUuid(frame.subarray(3, 19)),
      sequence: new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(19, false),
      payload: frame.subarray(FRAME_HEADER_BYTES),
    };
  } catch {
    return undefined;
  }
}

function safeFileName(name: string): string {
  const basename = name.replaceAll('\\', '/').split('/').pop() ?? 'file';
  const safe = stripControlCharacters(basename).trim().slice(0, 255);
  return safe || 'file';
}

function safeMimeType(value: string): string {
  const safe = stripControlCharacters(value).slice(0, 255);
  return safe || 'application/octet-stream';
}

function stripControlCharacters(value: string): string {
  return Array.from(value)
    .filter((character) => {
      const code = character.charCodeAt(0);
      return code >= 32 && !(code >= 0x7f && code <= 0x9f);
    })
    .join('');
}
