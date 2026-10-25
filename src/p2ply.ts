import { P2PlyError, QueueOverflowError, asP2PlyError } from './errors.js';
import { TypedEmitter } from './events/emitter.js';
import type { EventListener } from './events/emitter.js';
import { validateP2PlyOptions, type ValidatedP2PlyOptions } from './config/validate.js';
import { randomId } from './utils/id.js';
import { deferred, exponentialDelay, sleep, withTimeout } from './utils/async.js';
import type { Deferred } from './utils/async.js';
import { encodeJson, byteLength } from './protocol/encode.js';
import { dataEnvelopeSchema, DATA_PROTOCOL_VERSION, type DataEnvelope } from './protocol/data.js';
import {
  roomIdSchema,
  SIGNALING_PROTOCOL_VERSION,
  type ServerSignal,
} from './protocol/signaling.js';
import { SignalingClient } from './signaling/client.js';
import { PeerLink, type PeerLinkSignal } from './connection/peer-link.js';
import { FileTransferManager } from './file-transfer/transfer-manager.js';
import type { FileInput, FileTransferResult } from './file-transfer/types.js';
import type {
  MeshDiagnostics,
  MeshState,
  PeerDiagnostics,
  P2PlyEvents,
  P2PlyOptions,
  PeerState,
  PeerSummary,
  RequestOptions,
  SendOptions,
  SendReceipt,
  SignalingState,
} from './types.js';

interface PendingAck {
  peerId: string;
  deferred: Deferred<number>;
  timer: ReturnType<typeof setTimeout>;
  encoded: string;
  transmitted: boolean;
}

interface PendingRequest {
  peerId: string;
  deferred: Deferred<unknown>;
  timer: ReturnType<typeof setTimeout>;
  encoded: string;
  transmitted: boolean;
}

interface PeerOutage {
  ready: Deferred<void>;
  timer: ReturnType<typeof setTimeout>;
  attempts: number;
  waiters: number;
  repairing: boolean;
}

interface CandidateBuffer {
  peerId: string;
  sessionId: string;
  candidates: RTCIceCandidateInit[];
  timer: ReturnType<typeof setTimeout>;
}

const MAX_SEEN_MESSAGES = 4_096;
const MAX_CACHED_RESPONSES = 256;
const MAX_RECONNECT_TARGETS = 1_024;
/** Head start for the peer with the smaller ID, which reconnects first after a drop. */
const RECONNECT_HEAD_START_MS = 2_000;
const MAX_PENDING_CANDIDATE_SESSIONS = 64;
const MAX_PENDING_CANDIDATES_PER_SESSION = 256;

export class P2Ply<TMessage = unknown, TResponse = unknown> {
  readonly peerId: string;
  private readonly config: ValidatedP2PlyOptions;
  private readonly emitter: TypedEmitter<P2PlyEvents<TMessage, TResponse>>;
  private readonly joinedRooms = new Set<string>();
  private readonly peerRooms = new Map<string, Set<string>>();
  private readonly links = new Map<string, PeerLink>();
  private readonly connecting = new Map<string, Promise<void>>();
  private readonly pendingJoins = new Map<string, Deferred<void>>();
  private readonly pendingLeaves = new Map<string, Deferred<void>>();
  private readonly pendingAcks = new Map<string, PendingAck>();
  private readonly pendingRequests = new Map<string, PendingRequest>();
  private readonly seenMessages = new Map<string, true>();
  private readonly candidateBuffers = new Map<string, CandidateBuffer>();
  private readonly peerReconnectAttempts = new Map<string, number>();
  private readonly reconnectTargets = new Set<string>();
  private readonly outages = new Map<string, PeerOutage>();
  private readonly responseCache = new Map<string, DataEnvelope>();
  private signaling?: SignalingClient;
  private transferManager: FileTransferManager;
  private lifecycle: MeshState = 'idle';
  private startPromise?: Promise<void>;
  private hasConnectedSignaling = false;

  constructor(options: P2PlyOptions<TMessage>) {
    this.config = validateP2PlyOptions(options);
    this.peerId = this.config.peerId ?? randomId();
    this.emitter = new TypedEmitter((error, event) => {
      this.config.logger.log('warn', 'P2Ply event listener threw.', {
        event: String(event),
        error: error instanceof Error ? error.message : 'Unknown listener error',
      });
    });
    this.transferManager = new FileTransferManager({
      maxFileBytes: this.config.maxFileBytes,
      chunkSize: this.config.fileChunkSize,
      maxMessageBytes: this.config.maxMessageBytes,
      maxConcurrentTransfers: this.config.maxConcurrentTransfers,
      timeoutMs: this.config.transferTimeoutMs,
      ...(this.config.fileSinkFactory === undefined
        ? {}
        : { sinkFactory: this.config.fileSinkFactory }),
      sendControl: (peerId, envelope) => this.sendEnvelope(peerId, envelope),
      sendBinary: (peerId, data) => this.sendBinary(peerId, data),
      emitOffer: (event) => this.emitter.emit('file:offer', event),
      emitProgress: (event) => this.emitter.emit('file:progress', event),
      emitComplete: (event) => this.emitter.emit('file:complete', event),
      emitError: (event) => this.emitter.emit('file:error', event),
    });
  }

  get state(): MeshState {
    return this.lifecycle;
  }

  get signalingState(): SignalingState {
    return this.signaling?.state ?? 'disconnected';
  }

  get roomIds(): string[] {
    return [...this.joinedRooms];
  }

  on<K extends keyof P2PlyEvents<TMessage, TResponse>>(
    event: K,
    listener: EventListener<P2PlyEvents<TMessage, TResponse>[K]>,
  ): () => void {
    return this.emitter.on(event, listener);
  }

  once<K extends keyof P2PlyEvents<TMessage, TResponse>>(
    event: K,
    listener: EventListener<P2PlyEvents<TMessage, TResponse>[K]>,
  ): () => void {
    return this.emitter.once(event, listener);
  }

  off<K extends keyof P2PlyEvents<TMessage, TResponse>>(
    event: K,
    listener: EventListener<P2PlyEvents<TMessage, TResponse>[K]>,
  ): void {
    this.emitter.off(event, listener);
  }

  async start(): Promise<void> {
    if (this.lifecycle === 'closed' || this.lifecycle === 'closing')
      throw new P2PlyError('CLOSED', 'P2Ply has been closed.');
    if (this.lifecycle === 'started') {
      if (this.signalingState === 'connected') return;
      return this.reconnectSignaling();
    }
    if (this.startPromise) return this.startPromise;

    this.lifecycle = 'starting';
    this.startPromise = this.startSignalingClient();
    try {
      await this.startPromise;
    } catch (error) {
      if (this.lifecycle === 'starting') this.lifecycle = 'idle';
      throw error;
    } finally {
      this.startPromise = undefined;
    }
  }

  async reconnectSignaling(): Promise<void> {
    if (this.lifecycle !== 'started')
      throw new P2PlyError('INVALID_STATE', 'Call start() before reconnectSignaling().');
    await this.signaling?.close();
    this.signaling = undefined;
    await this.startSignalingClient();
  }

  async join(roomId: string): Promise<void> {
    this.assertStarted();
    const parsedRoom = roomIdSchema.safeParse(roomId);
    if (!parsedRoom.success)
      throw new P2PlyError(
        'CONFIGURATION_INVALID',
        'roomId must be a non-empty string of at most 128 characters.',
      );
    if (this.joinedRooms.has(roomId)) return;
    const existing = this.pendingJoins.get(roomId);
    if (existing) return existing.promise;
    const pending = deferred<void>();
    this.pendingJoins.set(roomId, pending);
    try {
      this.sendSignaling({ v: SIGNALING_PROTOCOL_VERSION, type: 'join', roomId });
      await withTimeout(
        pending.promise,
        this.config.signalingTimeoutMs,
        'SIGNALING_TIMEOUT',
        `Timed out joining room ${roomId}.`,
      );
    } finally {
      if (this.pendingJoins.get(roomId) === pending) this.pendingJoins.delete(roomId);
    }
  }

  async leave(roomId: string): Promise<void> {
    this.assertStarted();
    if (!this.joinedRooms.has(roomId)) return;
    const existing = this.pendingLeaves.get(roomId);
    if (existing) return existing.promise;
    const pending = deferred<void>();
    this.pendingLeaves.set(roomId, pending);
    try {
      const departing = [...this.peerRooms]
        .filter(([, rooms]) =>
          [...rooms].every((room) => room === roomId || !this.joinedRooms.has(room)),
        )
        .map(([peerId]) => peerId);
      await this.sayGoodbye(departing);
      this.sendSignaling({ v: SIGNALING_PROTOCOL_VERSION, type: 'leave', roomId });
      await withTimeout(
        pending.promise,
        this.config.signalingTimeoutMs,
        'SIGNALING_TIMEOUT',
        `Timed out leaving room ${roomId}.`,
      );
    } finally {
      if (this.pendingLeaves.get(roomId) === pending) this.pendingLeaves.delete(roomId);
    }
  }

  getPeers(): PeerSummary[] {
    return [...this.peerRooms.entries()].map(([peerId, roomSet]) => {
      const link = this.links.get(peerId);
      return {
        peerId,
        state: this.peerStateOf(peerId, link) ?? 'discovered',
        roomIds: [...roomSet],
        ...(link
          ? {
              connectionState: this.safeConnectionState(link),
              iceConnectionState: this.safeIceState(link),
              ...(link.dataChannelState === undefined
                ? {}
                : { dataChannelState: link.dataChannelState }),
            }
          : {}),
      };
    });
  }

  getPeer(peerId: string): PeerSummary | undefined {
    return this.getPeers().find((peer) => peer.peerId === peerId);
  }

  async connect(peerId: string): Promise<void> {
    return this.openLink(peerId, true);
  }

  private async openLink(peerId: string, reportErrors: boolean): Promise<void> {
    this.assertStarted();
    if (peerId === this.peerId)
      throw new P2PlyError('INVALID_STATE', 'A peer cannot connect to itself.');
    const inFlight = this.connecting.get(peerId);
    if (inFlight) return inFlight;

    const existingLink = this.links.get(peerId);
    if (existingLink?.currentState === 'connected') return;
    if (existingLink?.currentState === 'connecting')
      return this.waitForPeerLink(peerId, existingLink);

    const roomId = this.getSharedRoom(peerId);
    if (!roomId)
      throw new P2PlyError('PEER_NOT_FOUND', `Peer ${peerId} is not present in a joined room.`);
    await this.authorizePeer(peerId, roomId, 'outbound');
    this.assertStarted();
    if (this.getSharedRoom(peerId) !== roomId)
      throw new P2PlyError('PEER_NOT_FOUND', `Peer ${peerId} left room ${roomId}.`);

    let currentLink = this.links.get(peerId);
    while (currentLink) {
      if (currentLink.currentState === 'connected') return;
      if (currentLink.currentState === 'connecting')
        return this.waitForPeerLink(peerId, currentLink);
      currentLink.close();
      if (this.links.get(peerId) === currentLink) this.links.delete(peerId);
      if (!this.outages.has(peerId))
        await this.transferManager.cancelPeer(peerId, 'Peer connection is being replaced.');
      this.assertStarted();
      currentLink = this.links.get(peerId);
    }

    const link = this.createLink(peerId, roomId, randomId(), true);
    this.links.set(peerId, link);
    const connectPromise = (async () => {
      try {
        await link.startOffer();
        await link.waitUntilConnected(this.config.connectionTimeoutMs);
        this.peerReconnectAttempts.delete(peerId);
      } catch (error) {
        const meshError = asP2PlyError(
          error,
          'CONNECTION_FAILED',
          `Could not connect to peer ${peerId}.`,
        );
        if (reportErrors) this.emitError(meshError);
        throw meshError;
      }
    })();
    this.connecting.set(peerId, connectPromise);
    try {
      await connectPromise;
    } finally {
      if (this.connecting.get(peerId) === connectPromise) this.connecting.delete(peerId);
    }
  }

  private async waitForPeerLink(peerId: string, link: PeerLink): Promise<void> {
    const inFlight = this.connecting.get(peerId);
    if (inFlight) return inFlight;
    const wait = link.waitUntilConnected(this.config.connectionTimeoutMs);
    this.connecting.set(peerId, wait);
    try {
      await wait;
    } finally {
      if (this.connecting.get(peerId) === wait) this.connecting.delete(peerId);
    }
  }

  async reconnectPeer(peerId: string): Promise<void> {
    this.assertStarted();
    if (!this.getSharedRoom(peerId))
      throw new P2PlyError('PEER_NOT_FOUND', `Peer ${peerId} is not present in a joined room.`);
    this.rememberTarget(peerId);
    const outage = this.beginOutage(peerId);
    const old = this.links.get(peerId);
    if (old && old.currentState !== 'connecting') {
      old.close();
      if (this.links.get(peerId) === old) this.links.delete(peerId);
    }
    this.startRepair(peerId, outage);
    await outage.ready.promise;
  }

  async send(peerId: string, data: TMessage, options: SendOptions = {}): Promise<SendReceipt> {
    this.assertStarted();
    if (data === undefined)
      throw new P2PlyError('SERIALIZATION_FAILED', 'Undefined is not a JSON application payload.');
    if (this.config.validateMessage && !this.config.validateMessage(data))
      throw new P2PlyError('INVALID_MESSAGE', 'Application message did not pass validateMessage.');
    this.assertReachable(peerId);
    const messageId = randomId();
    const envelope: DataEnvelope = {
      v: DATA_PROTOCOL_VERSION,
      type: 'app',
      id: messageId,
      payload: data,
      ackRequested: options.acknowledge ?? false,
    };
    const encoded = encodeJson(envelope, this.config.maxMessageBytes, 'Application message');
    const acceptedAt = Date.now();
    const timeoutMs = options.timeoutMs ?? this.config.requestTimeoutMs;
    let pendingAck: PendingAck | undefined;
    if (options.acknowledge)
      pendingAck = this.createPendingAck(peerId, messageId, timeoutMs, encoded);
    try {
      const transmittedAt = await this.transmit(peerId, encoded, timeoutMs);
      if (!pendingAck) return { messageId, acceptedAt, transmittedAt };
      pendingAck.transmitted = true;
      const acknowledgedAt = await pendingAck.deferred.promise;
      return { messageId, acceptedAt, transmittedAt, acknowledgedAt };
    } catch (error) {
      if (pendingAck) this.removePendingAck(messageId, pendingAck);
      throw error;
    }
  }

  async broadcast(data: TMessage, options: SendOptions = {}): Promise<Map<string, SendReceipt>> {
    const connectedPeers = this.getPeers().filter((peer) => peer.state === 'connected');
    const results = await Promise.all(
      connectedPeers.map(
        async ({ peerId }) => [peerId, await this.send(peerId, data, options)] as const,
      ),
    );
    return new Map(results);
  }

  async request<TRequest = unknown>(
    peerId: string,
    data: TRequest,
    options: RequestOptions = {},
  ): Promise<TResponse> {
    this.assertStarted();
    if (data === undefined)
      throw new P2PlyError('SERIALIZATION_FAILED', 'Undefined is not a JSON request payload.');
    this.assertReachable(peerId);
    if (this.pendingRequests.size >= this.config.maxPendingRequests) {
      throw new P2PlyError(
        'TOO_MANY_PENDING_REQUESTS',
        'The pending request limit has been reached.',
      );
    }
    const requestId = randomId();
    const encoded = encodeJson(
      { v: DATA_PROTOCOL_VERSION, type: 'request', id: requestId, payload: data },
      this.config.maxMessageBytes,
      'Request message',
    );
    const pending = deferred<unknown>();
    void pending.promise.catch(() => {});
    const timeoutMs = options.timeoutMs ?? this.config.requestTimeoutMs;
    const timer = setTimeout(() => {
      this.pendingRequests.delete(requestId);
      pending.reject(
        new P2PlyError('REQUEST_TIMEOUT', `Request ${requestId} timed out after ${timeoutMs} ms.`),
      );
    }, timeoutMs);
    const entry: PendingRequest = { peerId, deferred: pending, timer, encoded, transmitted: false };
    this.pendingRequests.set(requestId, entry);
    try {
      await this.transmit(peerId, encoded, timeoutMs);
      entry.transmitted = true;
    } catch (error) {
      clearTimeout(timer);
      this.pendingRequests.delete(requestId);
      pending.reject(error);
    }
    return (await pending.promise) as TResponse;
  }

  async sendFile(
    peerId: string,
    file: FileInput,
    options: { name?: string } = {},
  ): Promise<{
    transferId: string;
    completion: Promise<FileTransferResult>;
    cancel(): Promise<void>;
  }> {
    this.assertStarted();
    this.requireConnectedLink(peerId);
    return this.transferManager.sendFile(peerId, file, options.name);
  }

  async cancelTransfer(transferId: string): Promise<void> {
    return this.transferManager.cancelTransfer(transferId);
  }

  async getDiagnostics(): Promise<MeshDiagnostics> {
    const peers: PeerDiagnostics[] = [];
    for (const [peerId, link] of this.links) {
      const stats = await link.getStats();
      const roomIds = [...(this.peerRooms.get(peerId) ?? [])];
      peers.push({
        peerId,
        state: this.peerStateOf(peerId, link) ?? link.currentState,
        roomIds,
        transport: 'webrtc-datachannel',
        ...(this.safeConnectionState(link) === undefined
          ? {}
          : { connectionState: this.safeConnectionState(link) }),
        ...(this.safeIceState(link) === undefined
          ? {}
          : { iceConnectionState: this.safeIceState(link) }),
        ...(link.dataChannelState === undefined ? {} : { dataChannelState: link.dataChannelState }),
        ...(stats.bytesSent === undefined ? {} : { bytesSent: stats.bytesSent }),
        ...(stats.bytesReceived === undefined ? {} : { bytesReceived: stats.bytesReceived }),
        ...(stats.roundTripTimeMs === undefined ? {} : { roundTripTimeMs: stats.roundTripTimeMs }),
        ...(stats.selectedCandidateType === undefined
          ? {}
          : { selectedCandidateType: stats.selectedCandidateType }),
        queuedMessages: link.queuedMessageCount,
        queuedBytes: link.queuedByteCount,
        activeTransfers: this.transferManager.activeCountForPeer(peerId),
        reconnectAttempts: this.peerReconnectAttempts.get(peerId) ?? 0,
        ...(link.lastError === undefined ? {} : { lastConnectionError: link.lastError }),
      });
    }
    return {
      peerId: this.peerId,
      meshState: this.lifecycle,
      signalingState: this.signalingState,
      signalingReconnectAttempts: this.signaling?.reconnectAttempts ?? 0,
      joinedRooms: [...this.joinedRooms],
      peers,
    };
  }

  async close(): Promise<void> {
    if (this.lifecycle === 'closed' || this.lifecycle === 'closing') return;
    this.lifecycle = 'closing';
    await this.sayGoodbye([...this.links.keys()]);
    await this.transferManager.close();
    for (const link of this.links.values()) link.close();
    this.links.clear();
    for (const buffer of this.candidateBuffers.values()) clearTimeout(buffer.timer);
    this.candidateBuffers.clear();
    for (const outage of this.outages.values()) {
      clearTimeout(outage.timer);
      outage.ready.reject(new P2PlyError('CLOSED', 'P2Ply closed while a peer was reconnecting.'));
    }
    this.outages.clear();
    this.reconnectTargets.clear();
    for (const pending of this.pendingAcks.values()) {
      clearTimeout(pending.timer);
      pending.deferred.reject(new P2PlyError('CLOSED', 'P2Ply closed before acknowledgement.'));
    }
    this.pendingAcks.clear();
    for (const pending of this.pendingRequests.values()) {
      clearTimeout(pending.timer);
      pending.deferred.reject(new P2PlyError('CLOSED', 'P2Ply closed before response.'));
    }
    this.pendingRequests.clear();
    for (const pending of this.pendingJoins.values())
      pending.reject(new P2PlyError('CLOSED', 'P2Ply closed during room join.'));
    for (const pending of this.pendingLeaves.values())
      pending.reject(new P2PlyError('CLOSED', 'P2Ply closed during room leave.'));
    this.pendingJoins.clear();
    this.pendingLeaves.clear();
    await this.signaling?.close();
    this.signaling = undefined;
    this.joinedRooms.clear();
    this.peerRooms.clear();
    this.lifecycle = 'closed';
  }

  private async startSignalingClient(): Promise<void> {
    const client = new SignalingClient({
      url: this.config.signalingUrl,
      peerId: this.peerId,
      ...(this.config.authToken === undefined ? {} : { authToken: this.config.authToken }),
      reconnect: this.config.reconnect,
      timeoutMs: this.config.signalingTimeoutMs,
      socketFactory: this.config.webSocketFactory,
      logger: this.config.logger,
      onMessage: (message) => this.handleSignalingMessage(message),
      onState: (state, attempt, error) => this.handleSignalingState(state, attempt, error),
    });
    this.signaling = client;
    try {
      await client.start();
      if (this.lifecycle !== 'closing' && this.lifecycle !== 'closed') this.lifecycle = 'started';
    } catch (error) {
      if (this.signaling === client) this.signaling = undefined;
      const meshError = asP2PlyError(
        error,
        'SIGNALING_UNAVAILABLE',
        'Could not start P2Ply signaling.',
      );
      this.emitError(meshError);
      throw meshError;
    }
  }

  private handleSignalingState(state: SignalingState, attempt?: number, error?: P2PlyError): void {
    this.emitter.emit('signaling:state', {
      state,
      ...(attempt === undefined ? {} : { attempt }),
      ...(error === undefined ? {} : { error }),
    });
    if (state === 'connected') {
      const isReconnect = this.hasConnectedSignaling;
      this.hasConnectedSignaling = true;
      if (isReconnect && this.lifecycle === 'started') void this.rejoinRooms();
    }
    if (
      (state === 'disconnected' || state === 'closed') &&
      this.pendingJoins.size + this.pendingLeaves.size > 0
    ) {
      this.rejectRoomOperations(
        error ??
          new P2PlyError(
            'SIGNALING_UNAVAILABLE',
            'Signaling disconnected during a room operation.',
          ),
      );
    }
    if (error)
      this.config.logger.log('warn', 'Signaling state changed with an error.', {
        state,
        code: error.code,
      });
  }

  private async handleSignalingMessage(message: ServerSignal): Promise<void> {
    try {
      switch (message.type) {
        case 'welcome':
          if (message.peerId !== this.peerId)
            throw new P2PlyError(
              'SIGNALING_PROTOCOL_ERROR',
              'Server welcome did not match this peer id.',
            );
          break;
        case 'joined':
          this.joinedRooms.add(message.roomId);
          this.applyRoomSnapshot(message.roomId, message.peers);
          this.pendingJoins.get(message.roomId)?.resolve();
          for (const peerId of message.peers) this.scheduleRepair(peerId);
          break;
        case 'left':
          this.joinedRooms.delete(message.roomId);
          for (const [peerId, rooms] of [...this.peerRooms]) {
            if (rooms.has(message.roomId)) this.removePeerRoom(peerId, message.roomId, true);
          }
          this.pendingLeaves.get(message.roomId)?.resolve();
          break;
        case 'peer-joined':
          if (this.joinedRooms.has(message.roomId))
            this.addPeerRoom(message.peerId, message.roomId);
          break;
        case 'peer-left':
          this.removePeerRoom(message.peerId, message.roomId);
          break;
        case 'offer':
          await this.handleOffer(message);
          break;
        case 'answer': {
          const link = this.links.get(message.from);
          if (link && link.roomId === message.roomId)
            await link.acceptAnswer(message.sessionId, message.description);
          break;
        }
        case 'ice-candidate':
          await this.handleCandidate(message);
          break;
        case 'error': {
          const code =
            message.code === 'RATE_LIMITED'
              ? 'RATE_LIMITED'
              : message.code === 'UNAUTHORIZED'
                ? 'UNAUTHORIZED'
                : 'REMOTE_ERROR';
          const remoteError = new P2PlyError(code, message.message, {
            details: { remoteCode: message.code },
          });
          if (message.roomId) {
            this.pendingJoins.get(message.roomId)?.reject(remoteError);
            this.pendingLeaves.get(message.roomId)?.reject(remoteError);
          }
          this.emitError(remoteError);
          break;
        }
        default:
          assertNever(message);
      }
    } catch (error) {
      this.emitError(
        asP2PlyError(error, 'SIGNALING_PROTOCOL_ERROR', 'Could not process a signaling message.'),
      );
    }
  }

  private async handleOffer(message: Extract<ServerSignal, { type: 'offer' }>): Promise<void> {
    if (
      !this.joinedRooms.has(message.roomId) ||
      !this.peerRooms.get(message.from)?.has(message.roomId)
    ) {
      this.config.logger.log('debug', 'Ignored offer outside a known joined room.', {
        peerId: message.from,
        roomId: message.roomId,
      });
      return;
    }
    try {
      await this.authorizePeer(message.from, message.roomId, 'inbound');
    } catch (error) {
      this.emitError(
        asP2PlyError(error, 'UNAUTHORIZED', 'Incoming peer connection was not authorized.'),
      );
      return;
    }

    let link = this.links.get(message.from);
    if (link?.currentState === 'connected' && link.sessionId !== message.sessionId) {
      if (this.canRecover(message.from)) this.beginOutage(message.from);
      link.close();
    }
    if (
      !link ||
      link.currentState === 'failed' ||
      link.currentState === 'closed' ||
      link.currentState === 'disconnected'
    ) {
      link?.close();
      link = this.createLink(message.from, message.roomId, message.sessionId, false);
      this.links.set(message.from, link);
    }
    if (link.roomId !== message.roomId) return;
    const accepted = await link.acceptOffer(message.sessionId, message.description);
    if (!accepted) {
      this.clearCandidateBuffer(message.from, message.sessionId);
      return;
    }
    const buffer = this.takeCandidateBuffer(message.from, message.sessionId);
    if (buffer) {
      for (const candidate of buffer.candidates)
        await link.addRemoteCandidate(message.sessionId, candidate);
    }
  }

  private async handleCandidate(
    message: Extract<ServerSignal, { type: 'ice-candidate' }>,
  ): Promise<void> {
    const link = this.links.get(message.from);
    if (link) {
      if (link.roomId !== message.roomId) return;
      if (link.sessionId === message.sessionId) {
        await link.addRemoteCandidate(message.sessionId, message.candidate);
        return;
      }
      if (link.currentState !== 'connecting') return;
    }
    this.bufferCandidate(message.from, message.sessionId, message.candidate);
  }

  private async handlePeerData(peerId: string, data: unknown): Promise<void> {
    const link = this.links.get(peerId);
    if (!link || link.currentState === 'closed' || link.currentState === 'failed') return;
    if (typeof data !== 'string') {
      const handled = await this.transferManager.handleBinary(
        peerId,
        data as ArrayBuffer | ArrayBufferView | Blob,
      );
      if (!handled) {
        this.emitError(
          new P2PlyError('SIGNALING_PROTOCOL_ERROR', 'Peer sent an unsupported binary data frame.'),
        );
        link.close();
      }
      return;
    }
    if (byteLength(data) > this.config.maxMessageBytes) {
      this.emitError(
        new P2PlyError('MESSAGE_TOO_LARGE', 'Peer sent a message above the configured size limit.'),
      );
      link.close();
      return;
    }
    let decoded: unknown;
    try {
      decoded = JSON.parse(data);
    } catch (error) {
      this.emitError(
        new P2PlyError('SIGNALING_PROTOCOL_ERROR', 'Peer sent invalid JSON.', { cause: error }),
      );
      link.close();
      return;
    }
    const parsed = dataEnvelopeSchema.safeParse(decoded);
    if (!parsed.success) {
      this.emitError(
        new P2PlyError(
          'SIGNALING_PROTOCOL_ERROR',
          'Peer sent an invalid or incompatible data protocol envelope.',
          { details: parsed.error.issues },
        ),
      );
      link.close();
      return;
    }
    const envelope = parsed.data;
    if (await this.transferManager.handleEnvelope(peerId, envelope)) return;

    switch (envelope.type) {
      case 'app': {
        const key = `${peerId}:${envelope.id}`;
        const duplicate = this.seenMessages.has(key);
        const valid = !this.config.validateMessage || this.config.validateMessage(envelope.payload);
        if (!valid) {
          this.emitError(
            new P2PlyError(
              'INVALID_MESSAGE',
              'Remote application message did not pass validateMessage.',
            ),
          );
          break;
        }
        if (!duplicate) {
          this.rememberMessage(key);
          this.emitter.emit('message', {
            peerId,
            messageId: envelope.id,
            data: envelope.payload as TMessage,
          });
        }
        if (envelope.ackRequested)
          await this.sendEnvelope(peerId, {
            v: DATA_PROTOCOL_VERSION,
            type: 'ack',
            id: envelope.id,
          });
        break;
      }
      case 'ack': {
        const pending = this.pendingAcks.get(envelope.id);
        if (pending && pending.peerId === peerId) {
          clearTimeout(pending.timer);
          this.pendingAcks.delete(envelope.id);
          pending.deferred.resolve(Date.now());
        }
        break;
      }
      case 'request':
        await this.handleRequest(peerId, envelope.id, envelope.payload);
        break;
      case 'response': {
        const pending = this.pendingRequests.get(envelope.replyTo);
        if (pending && pending.peerId === peerId) {
          clearTimeout(pending.timer);
          this.pendingRequests.delete(envelope.replyTo);
          pending.deferred.resolve(envelope.payload);
        }
        break;
      }
      case 'bye':
        this.reconnectTargets.delete(peerId);
        await this.sendEnvelope(peerId, { v: DATA_PROTOCOL_VERSION, type: 'ack', id: envelope.id });
        break;
      case 'response-error': {
        const pending = this.pendingRequests.get(envelope.replyTo);
        if (pending && pending.peerId === peerId) {
          clearTimeout(pending.timer);
          this.pendingRequests.delete(envelope.replyTo);
          pending.deferred.reject(new P2PlyError('REMOTE_ERROR', envelope.message));
        }
        break;
      }
      default:
        this.emitError(
          new P2PlyError('SIGNALING_PROTOCOL_ERROR', 'Unrecognized internal message envelope.'),
        );
        link.close();
    }
  }

  private async handleRequest(peerId: string, requestId: string, data: unknown): Promise<void> {
    const key = `${peerId}:request:${requestId}`;
    if (this.seenMessages.has(key)) {
      const cached = this.responseCache.get(key);
      if (cached) await this.sendEnvelope(peerId, cached).catch(() => {});
      return;
    }
    this.rememberMessage(key);
    let settled = false;
    const respond = async (payload: TResponse): Promise<void> => {
      if (settled) throw new P2PlyError('INVALID_STATE', 'This request has already been answered.');
      settled = true;
      if (payload === undefined)
        throw new P2PlyError('SERIALIZATION_FAILED', 'Undefined is not a JSON response payload.');
      await this.sendResponse(peerId, key, {
        v: DATA_PROTOCOL_VERSION,
        type: 'response',
        id: randomId(),
        replyTo: requestId,
        payload,
      });
    };
    const reject = async (reason = 'Request rejected.'): Promise<void> => {
      if (settled) throw new P2PlyError('INVALID_STATE', 'This request has already been answered.');
      settled = true;
      await this.sendResponse(peerId, key, {
        v: DATA_PROTOCOL_VERSION,
        type: 'response-error',
        id: randomId(),
        replyTo: requestId,
        message: reason.slice(0, 512),
      });
    };

    const listeners = this.emitter.emit('request', { peerId, requestId, data, respond, reject });
    if (listeners === 0) await reject('No request handler is registered.');
  }

  private createPendingAck(
    peerId: string,
    messageId: string,
    timeoutMs: number,
    encoded: string,
  ): PendingAck {
    if (this.pendingAcks.size >= this.config.maxPendingRequests) {
      throw new P2PlyError(
        'TOO_MANY_PENDING_REQUESTS',
        'The pending acknowledgement limit has been reached.',
      );
    }
    const ack = deferred<number>();
    void ack.promise.catch(() => {});
    const timer = setTimeout(() => {
      this.pendingAcks.delete(messageId);
      ack.reject(
        new P2PlyError(
          'REQUEST_TIMEOUT',
          `Remote P2Ply acknowledgement timed out after ${timeoutMs} ms.`,
        ),
      );
    }, timeoutMs);
    const pending: PendingAck = { peerId, deferred: ack, timer, encoded, transmitted: false };
    this.pendingAcks.set(messageId, pending);
    return pending;
  }

  private removePendingAck(messageId: string, pending: PendingAck): void {
    clearTimeout(pending.timer);
    if (this.pendingAcks.get(messageId) === pending) this.pendingAcks.delete(messageId);
    pending.deferred.reject(new P2PlyError('CONNECTION_FAILED', 'Message was not transmitted.'));
  }

  private async sendResponse(peerId: string, key: string, envelope: DataEnvelope): Promise<void> {
    this.responseCache.set(key, envelope);
    if (this.responseCache.size > MAX_CACHED_RESPONSES) {
      const oldest = this.responseCache.keys().next().value as string | undefined;
      if (oldest) this.responseCache.delete(oldest);
    }
    try {
      await this.sendEnvelope(peerId, envelope);
    } catch (error) {
      // While the peer reconnects, it repeats the request and gets the cached answer.
      if (!this.outages.has(peerId)) throw error;
    }
  }

  private async transmit(peerId: string, data: string, timeoutMs: number): Promise<number> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const link = this.links.get(peerId);
      if (link?.currentState === 'connected') {
        try {
          return await link.sendRaw(data);
        } catch (error) {
          if (!this.outages.has(peerId) || !isConnectionLoss(error)) throw error;
        }
      } else if (!this.outages.has(peerId)) {
        throw new P2PlyError('PEER_NOT_CONNECTED', `Peer ${peerId} is not connected.`);
      }
      await this.waitForReconnect(peerId, deadline);
    }
  }

  private async waitForReconnect(peerId: string, deadline: number): Promise<void> {
    const outage = this.outages.get(peerId);
    if (!outage) return;
    if (outage.waiters >= this.config.maxQueuedMessages)
      throw new QueueOverflowError(
        `Too many messages are waiting for peer ${peerId} to reconnect.`,
      );
    const remaining = deadline - Date.now();
    if (remaining <= 0)
      throw new P2PlyError('PEER_NOT_CONNECTED', `Peer ${peerId} did not reconnect in time.`);
    outage.waiters += 1;
    try {
      await withTimeout(
        outage.ready.promise,
        remaining,
        'PEER_NOT_CONNECTED',
        `Peer ${peerId} did not reconnect in time.`,
      );
    } finally {
      outage.waiters -= 1;
    }
  }

  private async sayGoodbye(peerIds: string[]): Promise<void> {
    await Promise.all(
      peerIds.map(async (peerId) => {
        this.reconnectTargets.delete(peerId);
        const link = this.links.get(peerId);
        if (link?.currentState === 'connecting') await link.waitUntilConnected(500).catch(() => {});
        if (link?.currentState !== 'connected') return;
        const id = randomId();
        const bye = encodeJson(
          { v: DATA_PROTOCOL_VERSION, type: 'bye', id },
          this.config.maxMessageBytes,
          'Bye',
        );
        try {
          const ack = this.createPendingAck(peerId, id, 500, bye);
          await link.sendRaw(bye);
          await ack.deferred.promise;
        } catch {
          // Best effort: the peer then treats the departure like a drop and gives up later.
        }
      }),
    );
  }

  private canRecover(peerId: string): boolean {
    return (
      this.lifecycle === 'started' &&
      this.config.peerReconnect.auto &&
      this.reconnectTargets.has(peerId)
    );
  }

  private rememberTarget(peerId: string): void {
    this.reconnectTargets.delete(peerId);
    this.reconnectTargets.add(peerId);
    if (this.reconnectTargets.size > MAX_RECONNECT_TARGETS) {
      const oldest = this.reconnectTargets.values().next().value as string | undefined;
      if (oldest) this.reconnectTargets.delete(oldest);
    }
  }

  private beginOutage(peerId: string): PeerOutage {
    const existing = this.outages.get(peerId);
    if (existing) return existing;
    const windowMs = this.config.peerReconnect.windowMs;
    const ready = deferred<void>();
    void ready.promise.catch(() => {});
    const outage: PeerOutage = {
      ready,
      attempts: 0,
      waiters: 0,
      repairing: false,
      timer: setTimeout(
        () =>
          this.endOutage(
            peerId,
            new P2PlyError(
              'CONNECTION_FAILED',
              `Peer ${peerId} did not reconnect within ${windowMs} ms.`,
            ),
          ),
        windowMs,
      ),
    };
    this.outages.set(peerId, outage);
    this.transferManager.pausePeer(peerId);
    const roomId = this.links.get(peerId)?.roomId ?? this.getSharedRoom(peerId);
    if (roomId) this.emitter.emit('peer:state', { peerId, roomId, state: 'reconnecting' });
    return outage;
  }

  /** Ends an outage: on success, resumes transfers and resends unconfirmed messages. */
  private endOutage(peerId: string, error?: P2PlyError): void {
    const outage = this.outages.get(peerId);
    if (!outage) return;
    clearTimeout(outage.timer);
    this.outages.delete(peerId);
    this.peerReconnectAttempts.delete(peerId);
    if (!error) {
      outage.ready.resolve();
      void this.transferManager.resumePeer(peerId);
      this.resendPending(peerId);
      return;
    }
    outage.ready.reject(error);
    void this.transferManager.cancelPeer(peerId, error.message);
    this.rejectPendingForPeer(peerId, error);
    this.emitError(error);
  }

  /** Starts reconnecting if this side is responsible and the peer can be reached. */
  private scheduleRepair(peerId: string): void {
    const outage = this.outages.get(peerId);
    if (!outage || outage.repairing) return;
    // Otherwise try again when the peer reappears in a room (see addPeerRoom and 'joined').
    if (!this.getSharedRoom(peerId) || this.signalingState !== 'connected') return;
    this.startRepair(peerId, outage);
  }

  private startRepair(peerId: string, outage: PeerOutage): void {
    if (outage.repairing) return;
    outage.repairing = true;
    void this.repairPeer(peerId, outage).finally(() => {
      outage.repairing = false;
    });
  }

  private async repairPeer(peerId: string, outage: PeerOutage): Promise<void> {
    const policy = this.config.peerReconnect;
    // The peer with the smaller ID reconnects first. The other side waits a moment, then tries
    // too, because the first side may not have noticed the drop at all. Simultaneous attempts
    // are resolved by the offer-collision handling in PeerLink.
    if (this.peerId.localeCompare(peerId) > 0 && outage.attempts === 0) {
      await sleep(RECONNECT_HEAD_START_MS);
      if (this.outages.get(peerId) !== outage || this.lifecycle !== 'started') return;
    }
    while (this.outages.get(peerId) === outage) {
      if (outage.attempts >= policy.maxAttempts) {
        this.endOutage(
          peerId,
          new P2PlyError(
            'CONNECTION_FAILED',
            `Could not reconnect to peer ${peerId} after ${policy.maxAttempts} attempts.`,
          ),
        );
        return;
      }
      outage.attempts += 1;
      this.peerReconnectAttempts.set(peerId, outage.attempts);
      await sleep(exponentialDelay(outage.attempts, policy));
      // The peer may have reconnected to us meanwhile, or left: then a later trigger resumes.
      if (this.outages.get(peerId) !== outage || this.lifecycle !== 'started') return;
      if (!this.getSharedRoom(peerId) || this.signalingState !== 'connected') return;
      try {
        await this.openLink(peerId, false);
        return; // handlePeerState('connected') ends the outage.
      } catch {
        // Try again after the next backoff delay.
      }
    }
  }

  private resendPending(peerId: string): void {
    const link = this.links.get(peerId);
    if (link?.currentState !== 'connected') return;
    for (const pending of [...this.pendingAcks.values(), ...this.pendingRequests.values()]) {
      if (pending.peerId === peerId && pending.transmitted)
        void link.sendRaw(pending.encoded).catch(() => {});
    }
  }

  /** Throws unless the peer is connected or reconnecting. */
  private assertReachable(peerId: string): void {
    if (this.links.get(peerId)?.currentState === 'connected' || this.outages.has(peerId)) return;
    throw new P2PlyError('PEER_NOT_CONNECTED', `Peer ${peerId} is not connected.`);
  }

  private peerStateOf(peerId: string, link: PeerLink | undefined): PeerState | undefined {
    if (this.outages.has(peerId) && link?.currentState !== 'connected') return 'reconnecting';
    return link?.currentState;
  }

  private async sendEnvelope(peerId: string, envelope: DataEnvelope): Promise<number> {
    const link = this.requireConnectedLink(peerId);
    const encoded = encodeJson(envelope, this.config.maxMessageBytes, 'P2Ply control message');
    return link.sendRaw(encoded);
  }

  private async sendBinary(peerId: string, data: Uint8Array): Promise<number> {
    const link = this.requireConnectedLink(peerId);
    if (data.byteLength > this.config.maxMessageBytes) {
      throw new P2PlyError(
        'MESSAGE_TOO_LARGE',
        'File chunk frame exceeds the configured message size limit.',
      );
    }
    return link.sendRaw(data);
  }

  private createLink(
    peerId: string,
    roomId: string,
    sessionId: string,
    initiator: boolean,
  ): PeerLink {
    const holder: { link?: PeerLink } = {};
    const link = new PeerLink({
      localPeerId: this.peerId,
      peerId,
      roomId,
      sessionId,
      initiator,
      rtcConfiguration: this.config.rtcConfiguration,
      rtcFactory: this.config.rtcFactory,
      negotiationTimeoutMs: this.config.negotiationTimeoutMs,
      disconnectGraceMs: this.config.disconnectGraceMs,
      maxMessageBytes: this.config.maxMessageBytes,
      maxBufferedBytes: this.config.maxBufferedBytes,
      maxQueuedMessages: this.config.maxQueuedMessages,
      maxQueuedBytes: this.config.maxQueuedBytes,
      logger: this.config.logger,
      sendSignal: (signal) => this.sendPeerSignal(signal),
      onData: (data) => {
        void this.handlePeerData(peerId, data).catch((error: unknown) =>
          this.emitError(
            asP2PlyError(error, 'SIGNALING_PROTOCOL_ERROR', 'Peer data processing failed.'),
          ),
        );
      },
      onState: (state, error) => this.handlePeerState(peerId, roomId, holder.link, state, error),
    });
    holder.link = link;
    return link;
  }

  private sendPeerSignal(signal: PeerLinkSignal): void {
    let message: Parameters<SignalingClient['send']>[0];
    if (signal.type === 'offer' || signal.type === 'answer') {
      if (!signal.description?.sdp) return;
      message = {
        v: SIGNALING_PROTOCOL_VERSION,
        type: signal.type,
        roomId: signal.roomId,
        to: signal.to,
        sessionId: signal.sessionId,
        description: { type: signal.type, sdp: signal.description.sdp },
      };
    } else {
      if (!signal.candidate?.candidate) return;
      message = {
        v: SIGNALING_PROTOCOL_VERSION,
        type: 'ice-candidate',
        roomId: signal.roomId,
        to: signal.to,
        sessionId: signal.sessionId,
        candidate: {
          candidate: signal.candidate.candidate,
          ...(signal.candidate.sdpMid === undefined ? {} : { sdpMid: signal.candidate.sdpMid }),
          ...(signal.candidate.sdpMLineIndex === undefined
            ? {}
            : { sdpMLineIndex: signal.candidate.sdpMLineIndex }),
          ...(signal.candidate.usernameFragment == null
            ? {}
            : { usernameFragment: signal.candidate.usernameFragment }),
        },
      };
    }
    try {
      this.signaling?.send(message);
    } catch (error) {
      this.emitError(
        asP2PlyError(error, 'SIGNALING_UNAVAILABLE', 'Could not send WebRTC signaling data.'),
      );
    }
  }

  private handlePeerState(
    peerId: string,
    roomId: string,
    link: PeerLink | undefined,
    state: PeerState,
    error?: P2PlyError,
  ): void {
    const isCurrent = !link || this.links.get(peerId) === link;
    this.emitter.emit('peer:state', { peerId, roomId, state, ...(error ? { error } : {}) });
    if (state === 'connected') {
      this.peerReconnectAttempts.delete(peerId);
      this.rememberTarget(peerId);
      if (isCurrent) this.endOutage(peerId);
      return;
    }
    if ((state === 'failed' || state === 'closed') && isCurrent) {
      if (this.lifecycle === 'started' && (this.outages.has(peerId) || this.canRecover(peerId))) {
        // Keep transfers and waiting sends; reconnect instead of failing them.
        if (error)
          this.config.logger.log('info', 'Peer connection dropped; reconnecting.', {
            peerId,
            code: error.code,
          });
        this.beginOutage(peerId);
        this.scheduleRepair(peerId);
        return;
      }
      void this.transferManager.cancelPeer(peerId, error?.message ?? 'Peer connection closed.');
      this.rejectPendingForPeer(
        peerId,
        error ?? new P2PlyError('PEER_NOT_CONNECTED', `Peer ${peerId} disconnected.`),
      );
    }
    if (error) this.emitError(error);
  }

  private rejectPendingForPeer(peerId: string, error: P2PlyError): void {
    for (const [id, pending] of this.pendingAcks) {
      if (pending.peerId !== peerId) continue;
      clearTimeout(pending.timer);
      this.pendingAcks.delete(id);
      pending.deferred.reject(error);
    }
    for (const [id, pending] of this.pendingRequests) {
      if (pending.peerId !== peerId) continue;
      clearTimeout(pending.timer);
      this.pendingRequests.delete(id);
      pending.deferred.reject(error);
    }
  }

  private applyRoomSnapshot(roomId: string, peers: string[]): void {
    const currentPeers = new Set(peers);
    for (const [peerId, rooms] of [...this.peerRooms]) {
      if (rooms.has(roomId) && !currentPeers.has(peerId)) this.removePeerRoom(peerId, roomId);
    }
    for (const peerId of currentPeers) {
      if (peerId !== this.peerId) this.addPeerRoom(peerId, roomId);
    }
  }

  private addPeerRoom(peerId: string, roomId: string): void {
    if (peerId === this.peerId) return;
    let rooms = this.peerRooms.get(peerId);
    if (!rooms) {
      rooms = new Set();
      this.peerRooms.set(peerId, rooms);
    }
    if (rooms.has(roomId)) return;
    rooms.add(roomId);
    this.emitter.emit('peer:join', { peerId, roomId });
    this.scheduleRepair(peerId);
    if (this.config.autoConnect && this.lifecycle === 'started') {
      void this.connect(peerId).catch((error: unknown) =>
        this.emitError(
          asP2PlyError(error, 'CONNECTION_FAILED', `Automatic connection to ${peerId} failed.`),
        ),
      );
    }
  }

  /**
   * @param weLeft true when we left the room ourselves; then the peer is not reconnected.
   * When the peer left, it may be a signaling drop, so its connection is reconnected if it returns.
   */
  private removePeerRoom(peerId: string, roomId: string, weLeft = false): void {
    const rooms = this.peerRooms.get(peerId);
    if (!rooms?.delete(roomId)) return;
    this.emitter.emit('peer:leave', { peerId, roomId });
    if (rooms.size === 0) {
      this.peerRooms.delete(peerId);
      if (weLeft) {
        this.reconnectTargets.delete(peerId);
        this.endOutage(
          peerId,
          new P2PlyError('PEER_NOT_FOUND', `Left the room shared with ${peerId}.`),
        );
      }
      const link = this.links.get(peerId);
      if (link) {
        link.close();
        this.links.delete(peerId);
      }
      if (!this.outages.has(peerId))
        void this.transferManager.cancelPeer(peerId, 'Peer left all shared rooms.');
    }
  }

  private getSharedRoom(peerId: string): string | undefined {
    const peerRooms = this.peerRooms.get(peerId);
    if (!peerRooms) return undefined;
    return [...this.joinedRooms].filter((roomId) => peerRooms.has(roomId)).sort()[0];
  }

  private async authorizePeer(
    peerId: string,
    roomId: string,
    direction: 'inbound' | 'outbound',
  ): Promise<void> {
    if (!this.config.authorizePeer) return;
    let permitted: boolean;
    try {
      permitted = await this.config.authorizePeer({ peerId, roomId, direction });
    } catch (error) {
      throw new P2PlyError('UNAUTHORIZED', `Peer ${peerId} authorization hook failed.`, {
        cause: error,
      });
    }
    if (!permitted)
      throw new P2PlyError('UNAUTHORIZED', `Connection to peer ${peerId} was denied.`);
  }

  private bufferCandidate(peerId: string, sessionId: string, candidate: RTCIceCandidateInit): void {
    const key = `${peerId}:${sessionId}`;
    let buffer = this.candidateBuffers.get(key);
    if (!buffer) {
      if (this.candidateBuffers.size >= MAX_PENDING_CANDIDATE_SESSIONS) {
        const oldestKey = this.candidateBuffers.keys().next().value as string | undefined;
        if (oldestKey) this.clearCandidateBufferByKey(oldestKey);
      }
      const timer = setTimeout(() => this.clearCandidateBufferByKey(key), 30_000);
      buffer = { peerId, sessionId, candidates: [], timer };
      this.candidateBuffers.set(key, buffer);
    }
    if (buffer.candidates.length >= MAX_PENDING_CANDIDATES_PER_SESSION) return;
    const candidateText = candidate.candidate;
    if (
      buffer.candidates.some(
        (existing) =>
          existing.candidate === candidateText &&
          existing.sdpMLineIndex === candidate.sdpMLineIndex,
      )
    )
      return;
    buffer.candidates.push(candidate);
  }

  private takeCandidateBuffer(peerId: string, sessionId: string): CandidateBuffer | undefined {
    const key = `${peerId}:${sessionId}`;
    const buffer = this.candidateBuffers.get(key);
    if (buffer) {
      clearTimeout(buffer.timer);
      this.candidateBuffers.delete(key);
    }
    return buffer;
  }

  private clearCandidateBuffer(peerId: string, sessionId: string): void {
    this.clearCandidateBufferByKey(`${peerId}:${sessionId}`);
  }

  private clearCandidateBufferByKey(key: string): void {
    const buffer = this.candidateBuffers.get(key);
    if (buffer) clearTimeout(buffer.timer);
    this.candidateBuffers.delete(key);
  }

  private rejectRoomOperations(error: P2PlyError): void {
    for (const pending of this.pendingJoins.values()) pending.reject(error);
    for (const pending of this.pendingLeaves.values()) pending.reject(error);
    this.pendingJoins.clear();
    this.pendingLeaves.clear();
  }

  private async rejoinRooms(): Promise<void> {
    if (!this.signaling || this.signaling.state !== 'connected') return;
    for (const roomId of this.joinedRooms) {
      try {
        this.sendSignaling({ v: SIGNALING_PROTOCOL_VERSION, type: 'join', roomId });
      } catch (error) {
        this.emitError(
          asP2PlyError(error, 'SIGNALING_UNAVAILABLE', `Could not rejoin room ${roomId}.`),
        );
      }
    }
  }

  private sendSignaling(message: Parameters<SignalingClient['send']>[0]): void {
    if (!this.signaling || this.signaling.state !== 'connected') {
      throw new P2PlyError('SIGNALING_UNAVAILABLE', 'Signaling is not connected.');
    }
    this.signaling.send(message);
  }

  private requireConnectedLink(peerId: string): PeerLink {
    const link = this.links.get(peerId);
    if (!link || link.currentState !== 'connected')
      throw new P2PlyError('PEER_NOT_CONNECTED', `Peer ${peerId} is not connected.`);
    return link;
  }

  private assertStarted(): void {
    if (this.lifecycle === 'closed' || this.lifecycle === 'closing')
      throw new P2PlyError('CLOSED', 'P2Ply has been closed.');
    if (this.lifecycle !== 'started')
      throw new P2PlyError('INVALID_STATE', 'Call await mesh.start() before using this method.');
  }

  private rememberMessage(key: string): void {
    this.seenMessages.set(key, true);
    if (this.seenMessages.size > MAX_SEEN_MESSAGES) {
      const oldest = this.seenMessages.keys().next().value as string | undefined;
      if (oldest) this.seenMessages.delete(oldest);
    }
  }

  private safeConnectionState(link: PeerLink): RTCPeerConnectionState | undefined {
    return link.connectionState;
  }

  private safeIceState(link: PeerLink): RTCIceConnectionState | undefined {
    return link.iceConnectionState;
  }

  private emitError(error: P2PlyError): void {
    this.emitter.emit('error', error);
    this.config.logger.log('warn', error.message, { code: error.code });
  }
}

function isConnectionLoss(error: unknown): boolean {
  return (
    error instanceof P2PlyError &&
    (error.code === 'PEER_NOT_CONNECTED' ||
      error.code === 'CONNECTION_FAILED' ||
      error.code === 'CLOSED')
  );
}

function assertNever(value: never): never {
  throw new Error(`Unhandled signaling message: ${String(value)}`);
}
