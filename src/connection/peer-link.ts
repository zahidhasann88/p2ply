import { P2PlyError, QueueOverflowError, asP2PlyError } from '../errors.js';
import type { Logger, PeerState } from '../types.js';
import type {
  DataChannelLike,
  IceCandidateLike,
  RTCPeerConnectionLike,
  RTCFactory,
} from '../adapters/webrtc.js';
import { byteLength } from '../protocol/encode.js';
import { deferred, withTimeout } from '../utils/async.js';

export interface PeerLinkSignal {
  type: 'offer' | 'answer' | 'ice-candidate';
  roomId: string;
  to: string;
  sessionId: string;
  description?: RTCSessionDescriptionInit;
  candidate?: RTCIceCandidateInit;
}

export interface PeerLinkOptions {
  localPeerId: string;
  peerId: string;
  roomId: string;
  sessionId: string;
  initiator: boolean;
  rtcConfiguration: RTCConfiguration;
  rtcFactory: RTCFactory;
  negotiationTimeoutMs: number;
  disconnectGraceMs: number;
  maxMessageBytes: number;
  maxBufferedBytes: number;
  maxQueuedMessages: number;
  maxQueuedBytes: number;
  logger: Logger;
  sendSignal(signal: PeerLinkSignal): void;
  onData(data: unknown): void;
  onState(state: PeerState, error?: P2PlyError): void;
}

interface QueuedWrite {
  data: string | ArrayBuffer | ArrayBufferView | Blob;
  size: number;
  resolve(sentAt: number): void;
  reject(error: unknown): void;
}

export class PeerLink {
  readonly peerId: string;
  readonly roomId: string;
  private pc: RTCPeerConnectionLike;
  private channel?: DataChannelLike;
  private channelWasCreatedLocally = false;
  private _sessionId: string;
  private state: PeerState = 'connecting';
  private makingOffer = false;
  private closedByUs = false;
  private ignoredOffer = false;
  private readonly polite: boolean;
  private readonly queuedWrites: QueuedWrite[] = [];
  private queuedBytes = 0;
  private pendingCandidates: RTCIceCandidateInit[] = [];
  private readonly seenCandidates = new Set<string>();
  private readonly connected = deferred<void>();
  private negotiationTimer?: ReturnType<typeof setTimeout>;
  private disconnectTimer?: ReturnType<typeof setTimeout>;
  private lastConnectionError?: string;

  constructor(private readonly options: PeerLinkOptions) {
    this.peerId = options.peerId;
    this.roomId = options.roomId;
    this._sessionId = options.sessionId;
    this.polite = options.localPeerId.localeCompare(options.peerId) > 0;
    this.pc = this.createPeerConnection();

    void this.connected.promise.catch(() => {});
    this.negotiationTimer = setTimeout(() => {
      this.fail(
        new P2PlyError(
          'CONNECTION_TIMEOUT',
          'WebRTC negotiation did not complete before the timeout.',
        ),
      );
    }, options.negotiationTimeoutMs);
  }

  get sessionId(): string {
    return this._sessionId;
  }

  get currentState(): PeerState {
    return this.state;
  }

  get dataChannelState(): RTCDataChannelState | undefined {
    return this.channel?.readyState;
  }

  get connectionState(): RTCPeerConnectionState {
    return this.pc.connectionState;
  }

  get iceConnectionState(): RTCIceConnectionState {
    return this.pc.iceConnectionState;
  }

  get lastError(): string | undefined {
    return this.lastConnectionError;
  }

  get queuedMessageCount(): number {
    return this.queuedWrites.length;
  }

  get queuedByteCount(): number {
    return this.queuedBytes;
  }

  async startOffer(): Promise<void> {
    if (this.state === 'closed' || this.state === 'failed') throw this.connectionError();
    if (this.channel) return;
    const pc = this.pc;
    this.attachChannel(pc.createDataChannel('p2ply/1', { ordered: true }), true);
    const offerSessionId = this._sessionId;
    const superseded = (): boolean => this._sessionId !== offerSessionId || this.pc !== pc;
    this.makingOffer = true;
    try {
      const offer = await pc.createOffer();
      if (superseded()) return;
      await pc.setLocalDescription(offer);
      if (superseded()) return;
      const description = pc.localDescription ?? offer;
      this.options.sendSignal({
        type: 'offer',
        roomId: this.roomId,
        to: this.peerId,
        sessionId: offerSessionId,
        description,
      });
    } catch (error) {
      if (superseded()) return;
      const meshError = asP2PlyError(
        error,
        'CONNECTION_FAILED',
        'Could not create or send a WebRTC offer.',
      );
      this.fail(meshError);
      throw meshError;
    } finally {
      if (!superseded()) this.makingOffer = false;
    }
  }

  async acceptOffer(sessionId: string, description: RTCSessionDescriptionInit): Promise<boolean> {
    if (this.state === 'closed' || this.state === 'failed') return false;
    if (this.state === 'connected' && sessionId !== this._sessionId) return false;

    const duplicateOffer =
      sessionId === this._sessionId &&
      this.pc.remoteDescription?.type === 'offer' &&
      this.pc.localDescription?.type === 'answer' &&
      this.pc.signalingState === 'stable';
    if (duplicateOffer && this.pc.localDescription) {
      this.options.sendSignal({
        type: 'answer',
        roomId: this.roomId,
        to: this.peerId,
        sessionId,
        description: this.pc.localDescription,
      });
      return true;
    }

    const offerCollision = this.makingOffer || this.pc.signalingState !== 'stable';
    this.ignoredOffer = !this.polite && offerCollision;
    if (this.ignoredOffer) return false;

    try {
      if (offerCollision && this.polite) this.replacePeerConnection();
      this._sessionId = sessionId;
      await this.pc.setRemoteDescription(description);
      await this.flushCandidates();
      const answer = await this.pc.createAnswer();
      await this.pc.setLocalDescription(answer);
      const localDescription = this.pc.localDescription ?? answer;
      this.options.sendSignal({
        type: 'answer',
        roomId: this.roomId,
        to: this.peerId,
        sessionId,
        description: localDescription,
      });
      return true;
    } catch (error) {
      const meshError = asP2PlyError(
        error,
        'CONNECTION_FAILED',
        'Could not accept the remote WebRTC offer.',
      );
      this.fail(meshError);
      throw meshError;
    }
  }

  async acceptAnswer(sessionId: string, description: RTCSessionDescriptionInit): Promise<void> {
    if (sessionId !== this._sessionId || this.state === 'closed' || this.state === 'failed') return;
    if (this.pc.signalingState !== 'have-local-offer') return;
    try {
      await this.pc.setRemoteDescription(description);
      await this.flushCandidates();
    } catch (error) {
      const meshError = asP2PlyError(
        error,
        'CONNECTION_FAILED',
        'Could not apply the WebRTC answer.',
      );
      this.fail(meshError);
      throw meshError;
    }
  }

  async addRemoteCandidate(sessionId: string, candidate: RTCIceCandidateInit): Promise<void> {
    if (
      sessionId !== this._sessionId ||
      this.state === 'closed' ||
      this.state === 'failed' ||
      this.ignoredOffer
    )
      return;
    const candidateKey = `${candidate.candidate}:${candidate.sdpMid ?? ''}:${candidate.sdpMLineIndex ?? ''}`;
    if (this.seenCandidates.has(candidateKey)) return;
    if (this.seenCandidates.size >= 512)
      this.seenCandidates.delete(this.seenCandidates.values().next().value as string);
    this.seenCandidates.add(candidateKey);
    if (!this.pc.remoteDescription) {
      if (this.pendingCandidates.length >= 256) {
        this.fail(
          new P2PlyError('CONNECTION_FAILED', 'Too many remote ICE candidates were buffered.'),
        );
        return;
      }
      this.pendingCandidates.push(candidate);
      return;
    }
    try {
      await this.pc.addIceCandidate(candidate);
    } catch (error) {
      this.options.logger.log('debug', 'A remote ICE candidate was rejected by the browser.', {
        peerId: this.peerId,
      });
      if (!this.ignoredOffer)
        this.lastConnectionError =
          error instanceof Error ? error.message : 'ICE candidate rejected';
    }
  }

  async waitUntilConnected(timeoutMs: number): Promise<void> {
    if (this.state === 'connected') return;
    if (this.state === 'failed' || this.state === 'closed') throw this.connectionError();
    await withTimeout(
      this.connected.promise,
      timeoutMs,
      'CONNECTION_TIMEOUT',
      'Peer connection did not open in time.',
    );
  }

  sendRaw(data: string | ArrayBuffer | ArrayBufferView | Blob): Promise<number> {
    const channel = this.channel;
    if (!channel || channel.readyState !== 'open' || this.state !== 'connected') {
      return Promise.reject(
        new P2PlyError('PEER_NOT_CONNECTED', `Peer ${this.peerId} is not connected.`),
      );
    }
    const size = this.dataSize(data);
    if (size > this.options.maxMessageBytes) {
      return Promise.reject(
        new P2PlyError(
          'MESSAGE_TOO_LARGE',
          `Data frame is ${size} bytes; limit is ${this.options.maxMessageBytes}.`,
        ),
      );
    }
    if (size > this.options.maxBufferedBytes) {
      return Promise.reject(
        new QueueOverflowError(
          'A single frame exceeds the configured DataChannel buffering limit.',
        ),
      );
    }
    if (
      this.queuedWrites.length >= this.options.maxQueuedMessages ||
      this.queuedBytes + size > this.options.maxQueuedBytes
    ) {
      return Promise.reject(new QueueOverflowError());
    }

    return new Promise<number>((resolve, reject) => {
      this.queuedWrites.push({ data, size, resolve, reject });
      this.queuedBytes += size;
      this.drainWrites();
    });
  }

  async getStats(): Promise<{
    bytesSent?: number;
    bytesReceived?: number;
    roundTripTimeMs?: number;
    selectedCandidateType?: 'host' | 'srflx' | 'prflx' | 'relay' | 'unknown';
  }> {
    try {
      const reports = Array.from(
        await this.pc.getStats(),
        (value) => value as Record<string, unknown>,
      );
      let bytesSent: number | undefined;
      let bytesReceived: number | undefined;
      let roundTripTimeMs: number | undefined;
      let selectedPairId: string | undefined;
      let localCandidateId: string | undefined;

      for (const report of reports) {
        if (report.type === 'data-channel') {
          if (typeof report.bytesSent === 'number') bytesSent = (bytesSent ?? 0) + report.bytesSent;
          if (typeof report.bytesReceived === 'number')
            bytesReceived = (bytesReceived ?? 0) + report.bytesReceived;
        }
        if (report.type === 'transport' && typeof report.selectedCandidatePairId === 'string') {
          selectedPairId = report.selectedCandidatePairId;
        }
        if (
          report.type === 'candidate-pair' &&
          (report.selected === true || (report.nominated === true && report.state === 'succeeded'))
        ) {
          selectedPairId = typeof report.id === 'string' ? report.id : selectedPairId;
          if (typeof report.currentRoundTripTime === 'number')
            roundTripTimeMs = report.currentRoundTripTime * 1000;
          if (typeof report.localCandidateId === 'string')
            localCandidateId = report.localCandidateId;
        }
      }

      const selectedPair = reports.find((report) => report.id === selectedPairId);
      if (selectedPair) {
        if (typeof selectedPair.currentRoundTripTime === 'number')
          roundTripTimeMs = selectedPair.currentRoundTripTime * 1000;
        if (typeof selectedPair.localCandidateId === 'string')
          localCandidateId = selectedPair.localCandidateId;
      }
      const localCandidate = reports.find((report) => report.id === localCandidateId);
      const candidateType = localCandidate?.candidateType;
      const selectedCandidateType =
        candidateType === 'host' ||
        candidateType === 'srflx' ||
        candidateType === 'prflx' ||
        candidateType === 'relay'
          ? candidateType
          : localCandidate
            ? 'unknown'
            : undefined;

      return {
        ...(bytesSent === undefined ? {} : { bytesSent }),
        ...(bytesReceived === undefined ? {} : { bytesReceived }),
        ...(roundTripTimeMs === undefined ? {} : { roundTripTimeMs }),
        ...(selectedCandidateType === undefined ? {} : { selectedCandidateType }),
      };
    } catch {
      this.options.logger.log('debug', 'Peer connection statistics are unavailable.', {
        peerId: this.peerId,
      });
      return {};
    }
  }

  close(): void {
    if (this.state === 'closed') return;
    this.closedByUs = true;
    this.clearTimers();
    this.rejectQueued(new P2PlyError('CLOSED', `Peer link to ${this.peerId} was closed.`));
    this.connected.reject(new P2PlyError('CLOSED', `Peer link to ${this.peerId} was closed.`));
    try {
      this.channel?.close();
      this.pc.close();
    } catch {
      // Browser implementations may throw when closing a failed peer connection.
    }
    this.setState('closed');
  }

  private createPeerConnection(): RTCPeerConnectionLike {
    let pc: RTCPeerConnectionLike;
    try {
      pc = this.options.rtcFactory(this.options.rtcConfiguration);
    } catch (error) {
      throw new P2PlyError('CONNECTION_FAILED', 'Could not create an RTCPeerConnection.', {
        cause: error,
      });
    }
    pc.onicecandidate = (event) => {
      if (pc === this.pc)
        this.handleLocalCandidate(event.candidate as unknown as IceCandidateLike | null);
    };
    pc.ondatachannel = (event) => {
      if (pc === this.pc) this.attachChannel(event.channel as unknown as DataChannelLike, false);
      else event.channel.close();
    };
    pc.onconnectionstatechange = () => {
      if (pc === this.pc) this.handleConnectionState();
    };
    pc.oniceconnectionstatechange = () => {
      if (pc === this.pc) this.handleIceState();
    };
    return pc;
  }

  private replacePeerConnection(): void {
    const abandoned = this.pc;
    const abandonedChannel = this.channel;
    this.pc = this.createPeerConnection();
    this.makingOffer = false;
    this.pendingCandidates = [];
    this.channel = undefined;
    this.channelWasCreatedLocally = false;
    if (abandonedChannel) {
      abandonedChannel.onopen = null;
      abandonedChannel.onclose = null;
      abandonedChannel.onerror = null;
      abandonedChannel.onmessage = null;
      abandonedChannel.onbufferedamountlow = null;
    }
    try {
      abandonedChannel?.close();
      abandoned.close();
    } catch {
      // Closing an unused connection is best-effort.
    }
  }

  private attachChannel(channel: DataChannelLike, local: boolean): void {
    if (this.channel && this.channel !== channel) {
      channel.close();
      return;
    }
    if (channel.label !== 'p2ply/1') {
      channel.close();
      this.fail(
        new P2PlyError('SIGNALING_PROTOCOL_ERROR', 'The peer opened an unsupported data channel.'),
      );
      return;
    }
    this.channel = channel;
    this.channelWasCreatedLocally = local;
    channel.binaryType = 'arraybuffer';
    channel.bufferedAmountLowThreshold = Math.floor(this.options.maxBufferedBytes / 2);
    channel.onopen = () => {
      if (this.state === 'closed' || this.state === 'failed') return;
      this.clearNegotiationTimer();
      this.setState('connected');
      this.connected.resolve();
      this.drainWrites();
    };
    channel.onclose = () => {
      if (!this.closedByUs && this.state !== 'failed') this.setState('disconnected');
    };
    channel.onerror = () => {
      this.lastConnectionError = 'DataChannel transport error';
    };
    channel.onmessage = (event) => {
      try {
        this.options.onData(event.data);
      } catch (error) {
        const meshError = asP2PlyError(
          error,
          'SIGNALING_PROTOCOL_ERROR',
          'Peer message handler failed.',
        );
        this.options.logger.log('warn', 'Peer message handler failed.', {
          peerId: this.peerId,
          code: meshError.code,
        });
      }
    };
    channel.onbufferedamountlow = () => this.drainWrites();
    if (channel.readyState === 'open') {
      channel.onopen(new Event('open'));
    }
    if (!local)
      this.options.logger.log('debug', 'Remote DataChannel attached.', { peerId: this.peerId });
  }

  private handleLocalCandidate(candidate: IceCandidateLike | null): void {
    if (!candidate || this.state === 'closed' || this.state === 'failed') return;
    const raw = candidate.toJSON?.() ?? {
      candidate: candidate.candidate,
      sdpMid: candidate.sdpMid,
      sdpMLineIndex: candidate.sdpMLineIndex,
      ...(candidate.usernameFragment ? { usernameFragment: candidate.usernameFragment } : {}),
    };
    const { sdpMLineIndex, ...value } = raw;
    if (Number.isInteger(sdpMLineIndex) && sdpMLineIndex! >= 0 && sdpMLineIndex! <= 65_535)
      Object.assign(value, { sdpMLineIndex });
    else if (value.sdpMid == null) return;
    const signal: PeerLinkSignal = {
      type: 'ice-candidate',
      roomId: this.roomId,
      to: this.peerId,
      sessionId: this._sessionId,
      candidate: value,
    };
    this.options.sendSignal(signal);
  }

  private handleConnectionState(): void {
    if (this.closedByUs) return;
    const connectionState = this.pc.connectionState;
    if (connectionState === 'connected') {
      if (this.disconnectTimer) clearTimeout(this.disconnectTimer);
      this.disconnectTimer = undefined;
      if (this.channel?.readyState === 'open') this.setState('connected');
      return;
    }
    if (connectionState === 'disconnected') {
      this.setState('disconnected');
      if (this.disconnectTimer) clearTimeout(this.disconnectTimer);
      this.disconnectTimer = setTimeout(() => {
        if (this.pc.connectionState === 'disconnected') {
          this.fail(
            new P2PlyError(
              'CONNECTION_FAILED',
              'Peer connection remained disconnected past the grace period.',
            ),
          );
        }
      }, this.options.disconnectGraceMs);
      return;
    }
    if (connectionState === 'failed') {
      this.fail(
        new P2PlyError('CONNECTION_FAILED', 'The browser reported a failed peer connection.'),
      );
      return;
    }
    if (connectionState === 'closed' && !this.closedByUs && this.state !== 'closed') {
      this.setState('closed');
    }
  }

  private handleIceState(): void {
    if (this.closedByUs) return;
    if (this.pc.iceConnectionState === 'failed') {
      this.fail(
        new P2PlyError(
          'CONNECTION_FAILED',
          'ICE connectivity checks failed. A TURN relay may be required.',
        ),
      );
    }
  }

  private async flushCandidates(): Promise<void> {
    if (!this.pc.remoteDescription) return;
    const candidates = this.pendingCandidates;
    this.pendingCandidates = [];
    for (const candidate of candidates) {
      try {
        await this.pc.addIceCandidate(candidate);
      } catch (error) {
        this.options.logger.log('debug', 'A buffered ICE candidate was rejected.', {
          peerId: this.peerId,
        });
        this.lastConnectionError =
          error instanceof Error ? error.message : 'ICE candidate rejected';
      }
    }
  }

  private drainWrites(): void {
    const channel = this.channel;
    if (!channel || channel.readyState !== 'open' || this.state !== 'connected') return;
    while (this.queuedWrites.length > 0) {
      const item = this.queuedWrites[0]!;
      if (channel.bufferedAmount + item.size > this.options.maxBufferedBytes) return;
      this.queuedWrites.shift();
      this.queuedBytes -= item.size;
      try {
        channel.send(item.data);
        item.resolve(Date.now());
      } catch (error) {
        item.reject(
          new P2PlyError('CONNECTION_FAILED', 'The DataChannel rejected a queued frame.', {
            cause: error,
          }),
        );
      }
    }
  }

  private dataSize(data: string | ArrayBuffer | ArrayBufferView | Blob): number {
    if (typeof data === 'string') return byteLength(data);
    if (data instanceof ArrayBuffer) return data.byteLength;
    if (ArrayBuffer.isView(data)) return data.byteLength;
    return data.size;
  }

  private rejectQueued(error: P2PlyError): void {
    for (const item of this.queuedWrites.splice(0)) item.reject(error);
    this.queuedBytes = 0;
  }

  private fail(error: P2PlyError): void {
    if (this.state === 'failed' || this.state === 'closed') return;
    this.lastConnectionError = error.message;
    this.closedByUs = true;
    this.clearTimers();
    this.connected.reject(error);
    this.rejectQueued(error);
    try {
      this.channel?.close();
      this.pc.close();
    } catch {
      // Ignore cleanup errors after a transport failure.
    }
    this.setState('failed', error);
  }

  private setState(state: PeerState, error?: P2PlyError): void {
    if (this.state === state && !error) return;
    this.state = state;
    this.options.onState(state, error);
  }

  private connectionError(): P2PlyError {
    return new P2PlyError(
      'CONNECTION_FAILED',
      this.lastConnectionError ?? `Peer ${this.peerId} is not connectable.`,
    );
  }

  private clearNegotiationTimer(): void {
    if (this.negotiationTimer) clearTimeout(this.negotiationTimer);
    this.negotiationTimer = undefined;
  }

  private clearTimers(): void {
    this.clearNegotiationTimer();
    if (this.disconnectTimer) clearTimeout(this.disconnectTimer);
    this.disconnectTimer = undefined;
  }
}
