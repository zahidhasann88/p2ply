import type {
  FileSinkFactory,
  FileTransferResult,
  TransferMetadata,
} from './file-transfer/types.js';
import type { RTCFactory } from './adapters/webrtc.js';
import type { WebSocketFactory } from './signaling/socket.js';
import type { P2PlyError } from './errors.js';

export type PeerState = 'connecting' | 'connected' | 'disconnected' | 'failed' | 'closed';
export type SignalingState =
  'connecting' | 'connected' | 'reconnecting' | 'disconnected' | 'closed';
export type MeshState = 'idle' | 'starting' | 'started' | 'closing' | 'closed';

export interface ReconnectOptions {
  enabled?: boolean;
  maxAttempts?: number;
  minDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
  jitter?: number;
}

export interface PeerReconnectOptions {
  maxAttempts?: number;
  minDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
  jitter?: number;
}

export interface Logger {
  log(
    level: 'debug' | 'info' | 'warn' | 'error',
    message: string,
    context?: Readonly<Record<string, unknown>>,
  ): void;
}

export interface AuthorizePeerContext {
  peerId: string;
  roomId: string;
  direction: 'inbound' | 'outbound';
}

export interface P2PlyOptions<TMessage = unknown> {
  signalingUrl: string;
  authToken?: string;
  peerId?: string;
  reconnect?: ReconnectOptions;
  peerReconnect?: PeerReconnectOptions;
  rtcConfiguration?: RTCConfiguration;
  connectionTimeoutMs?: number;
  negotiationTimeoutMs?: number;
  disconnectGraceMs?: number;
  signalingTimeoutMs?: number;
  maxMessageBytes?: number;
  maxBufferedBytes?: number;
  maxQueuedMessages?: number;
  maxQueuedBytes?: number;
  maxPendingRequests?: number;
  requestTimeoutMs?: number;
  autoConnect?: boolean;
  maxFileBytes?: number;
  fileChunkSize?: number;
  maxConcurrentTransfers?: number;
  transferTimeoutMs?: number;
  fileSinkFactory?: FileSinkFactory;
  authorizePeer?: (context: AuthorizePeerContext) => boolean | Promise<boolean>;
  validateMessage?: (value: unknown) => value is TMessage;
  logger?: Logger;
  rtcFactory?: RTCFactory;
  webSocketFactory?: WebSocketFactory;
}

export interface PeerSummary {
  peerId: string;
  state: PeerState | 'discovered';
  roomIds: string[];
  connectionState?: RTCPeerConnectionState;
  iceConnectionState?: RTCIceConnectionState;
  dataChannelState?: RTCDataChannelState;
}

export interface PeerStateEvent {
  peerId: string;
  state: PeerState;
  roomId: string;
  error?: P2PlyError;
}

export interface SendOptions {
  acknowledge?: boolean;
  timeoutMs?: number;
}

export interface SendReceipt {
  messageId: string;
  acceptedAt: number;
  transmittedAt: number;
  acknowledgedAt?: number;
}

export interface RequestOptions {
  timeoutMs?: number;
}

export interface FileTransferHandle {
  transferId: string;
  completion: Promise<FileTransferResult>;
  cancel(): Promise<void>;
}

export interface P2PlyEvents<TMessage = unknown, TResponse = unknown> {
  'peer:join': { peerId: string; roomId: string };
  'peer:leave': { peerId: string; roomId: string };
  'peer:state': PeerStateEvent;
  'signaling:state': { state: SignalingState; attempt?: number; error?: P2PlyError };
  message: { peerId: string; messageId: string; data: TMessage };
  request: {
    peerId: string;
    requestId: string;
    data: unknown;
    respond(data: TResponse): Promise<void>;
    reject(reason?: string): Promise<void>;
  };
  'file:offer': {
    peerId: string;
    transferId: string;
    metadata: TransferMetadata;
    accept(): Promise<void>;
    reject(reason?: string): Promise<void>;
  };
  'file:progress': {
    peerId: string;
    transferId: string;
    direction: 'send' | 'receive';
    transferredBytes: number;
    totalBytes: number;
  };
  'file:complete': { peerId: string; transferId: string; result: FileTransferResult };
  'file:error': { peerId: string; transferId: string; error: P2PlyError };
  error: P2PlyError;
}

export interface PeerDiagnostics {
  peerId: string;
  state: PeerState;
  roomIds: string[];
  transport: 'webrtc-datachannel';
  connectionState?: RTCPeerConnectionState;
  iceConnectionState?: RTCIceConnectionState;
  dataChannelState?: RTCDataChannelState;
  bytesSent?: number;
  bytesReceived?: number;
  roundTripTimeMs?: number;
  selectedCandidateType?: 'host' | 'srflx' | 'prflx' | 'relay' | 'unknown';
  queuedMessages: number;
  queuedBytes: number;
  activeTransfers: number;
  reconnectAttempts: number;
  lastConnectionError?: string;
}

export interface MeshDiagnostics {
  peerId: string;
  meshState: MeshState;
  signalingState: SignalingState;
  signalingReconnectAttempts: number;
  joinedRooms: string[];
  peers: PeerDiagnostics[];
}

export type { FileSinkFactory, FileTransferResult, TransferMetadata };
