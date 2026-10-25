import {
  createServer,
  type IncomingMessage,
  type Server as HttpServer,
  type ServerResponse,
} from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Duplex } from 'node:stream';
import { Buffer } from 'node:buffer';
import { WebSocket, WebSocketServer, type RawData } from 'ws';
import { P2PlyError } from '../errors.js';
import {
  clientSignalSchema,
  SIGNALING_PROTOCOL_VERSION,
  type ClientSignal,
  type PeerSignal,
  type ServerSignal,
} from '../protocol/signaling.js';
import {
  validateSignalingServerOptions,
  type SignalingServerOptions,
  type ValidatedSignalingServerOptions,
} from './config.js';
import { InMemoryPeerRegistry, type PeerRegistry, type RegisteredPeer } from './registry.js';
import { TokenBucket } from './rate-limit.js';

interface ClientState {
  socket: WebSocket;
  request: IncomingMessage;
  peerId?: string;
  authContext?: unknown;
  bucket: TokenBucket;
  connectedAt: number;
  lastPongAt: number;
  handshakeTimer?: ReturnType<typeof setTimeout>;
  closed: boolean;
}

export interface SignalingServerAddress {
  address: string;
  family: string;
  port: number;
}

export interface SignalingServer {
  readonly httpServer: HttpServer;
  readonly registry: PeerRegistry;
  listen(port?: number, host?: string): Promise<SignalingServerAddress>;
  close(): Promise<void>;
  getStats(): { connections: number; rooms: number; status: 'ready' | 'closed' };
}

export interface CreateSignalingServerOptions extends SignalingServerOptions {
  registry?: PeerRegistry;
}

export function createSignalingServer(input: CreateSignalingServerOptions = {}): SignalingServer {
  const { registry: customRegistry, ...serverInput } = input;
  const options = validateSignalingServerOptions(serverInput);
  const registry = customRegistry ?? new InMemoryPeerRegistry();
  const clients = new Map<WebSocket, ClientState>();
  let closed = false;
  let listening = false;

  const httpServer = createServer((request, response) =>
    handleHttpRequest(request, response, registry, () => closed),
  );
  const webSockets = new WebSocketServer({
    noServer: true,
    maxPayload: options.maxMessageBytes,
    perMessageDeflate: false,
    clientTracking: false,
  });

  httpServer.on('upgrade', (request, socket, head) => {
    if (closed) {
      rejectUpgrade(socket, 503, 'Service Unavailable');
      return;
    }
    if (registry.peerCount + pendingCount(clients) >= options.maxConnections) {
      rejectUpgrade(socket, 503, 'Connection Limit Reached');
      return;
    }
    let pathname: string;
    try {
      pathname = new URL(request.url ?? '/', 'http://p2ply.invalid').pathname;
    } catch {
      rejectUpgrade(socket, 400, 'Bad Request');
      return;
    }
    if (pathname !== options.path) {
      rejectUpgrade(socket, 404, 'Not Found');
      return;
    }
    if (!originIsAllowed(request, options)) {
      rejectUpgrade(socket, 403, 'Forbidden');
      return;
    }
    webSockets.handleUpgrade(request, socket, head, (webSocket) =>
      webSockets.emit('connection', webSocket, request),
    );
  });

  webSockets.on('connection', (socket: WebSocket, request: IncomingMessage) => {
    const now = Date.now();
    const state: ClientState = {
      socket,
      request,
      bucket: new TokenBucket(options.messageBurst, options.messagesPerSecond, now),
      connectedAt: now,
      lastPongAt: now,
      closed: false,
    };
    clients.set(socket, state);
    state.handshakeTimer = setTimeout(() => {
      if (!state.peerId && socket.readyState === WebSocket.OPEN)
        socket.close(1008, 'Hello required');
    }, options.handshakeTimeoutMs);
    socket.on('pong', () => {
      state.lastPongAt = Date.now();
    });
    socket.on('message', (data, isBinary) => {
      void handleMessage(state, data, isBinary, registry, clients, options);
    });
    socket.on('close', () => {
      void cleanupClient(state, registry, clients, options);
    });
    socket.on('error', () => {
      options.logger.log('debug', 'Signaling WebSocket transport error.');
    });
  });

  const heartbeat = setInterval(() => {
    const now = Date.now();
    for (const state of clients.values()) {
      if (state.closed) continue;
      if (now - state.lastPongAt > options.staleConnectionMs) {
        state.socket.close(4002, 'Heartbeat timeout');
        continue;
      }
      if (state.socket.readyState === WebSocket.OPEN) {
        try {
          state.socket.ping();
        } catch {
          state.socket.terminate();
        }
      }
    }
  }, options.heartbeatIntervalMs);
  heartbeat.unref?.();

  return {
    httpServer,
    registry,
    async listen(port = options.port, host = options.host) {
      if (closed) throw new P2PlyError('CLOSED', 'Signaling server has been closed.');
      if (listening) return addressInfo(httpServer.address());
      await new Promise<void>((resolve, reject) => {
        const onError = (error: Error): void => {
          httpServer.off('listening', onListening);
          reject(error);
        };
        const onListening = (): void => {
          httpServer.off('error', onError);
          resolve();
        };
        httpServer.once('error', onError);
        httpServer.once('listening', onListening);
        httpServer.listen(port, host);
      });
      listening = true;
      const address = addressInfo(httpServer.address());
      options.logger.log('info', 'P2Ply signaling server listening.', {
        host: address.address,
        port: address.port,
      });
      return address;
    },
    async close() {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      const sockets = [...clients.keys()];
      for (const socket of sockets) {
        try {
          socket.close(1001, 'Server shutting down');
        } catch {
          socket.terminate();
        }
      }
      for (const state of clients.values()) await cleanupClient(state, registry, clients, options);
      await new Promise<void>((resolve) => {
        webSockets.close(() => resolve());
      });
      if (httpServer.listening) {
        await new Promise<void>((resolve, reject) => {
          httpServer.close((error) => (error ? reject(error) : resolve()));
        });
      }
      listening = false;
      options.logger.log('info', 'P2Ply signaling server stopped.');
    },
    getStats() {
      return {
        connections: registry.peerCount,
        rooms: registry.roomCount,
        status: closed ? 'closed' : 'ready',
      };
    },
  };
}

export async function startSignalingServer(
  options: CreateSignalingServerOptions = {},
): Promise<SignalingServer> {
  const server = createSignalingServer(options);
  await server.listen();
  return server;
}

async function handleMessage(
  state: ClientState,
  raw: RawData,
  isBinary: boolean,
  registry: PeerRegistry,
  clients: Map<WebSocket, ClientState>,
  options: ValidatedSignalingServerOptions,
): Promise<void> {
  if (state.closed) return;
  if (!state.bucket.consume()) {
    sendError(state, 'RATE_LIMITED', 'Message rate limit exceeded.');
    state.socket.close(1008, 'Rate limit exceeded');
    return;
  }
  if (isBinary) {
    sendError(state, 'INVALID_MESSAGE', 'Signaling messages must be UTF-8 JSON text.');
    state.socket.close(1003, 'Text frames required');
    return;
  }

  let text: string;
  try {
    const bytes = rawDataToBuffer(raw);
    if (bytes.byteLength > options.maxMessageBytes) {
      state.socket.close(1009, 'Message too large');
      return;
    }
    text = bytes.toString('utf8');
  } catch {
    state.socket.close(1003, 'Invalid message encoding');
    return;
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(text);
  } catch {
    sendError(state, 'INVALID_JSON', 'Signaling message must contain valid JSON.');
    state.socket.close(1007, 'Invalid JSON');
    return;
  }
  const parsed = clientSignalSchema.safeParse(decoded);
  if (!parsed.success) {
    sendError(state, 'INVALID_MESSAGE', 'Signaling message failed protocol validation.');
    state.socket.close(1008, 'Invalid signaling protocol message');
    return;
  }

  const message = parsed.data;
  if (!state.peerId) {
    if (message.type !== 'hello') {
      sendError(state, 'HELLO_REQUIRED', 'Send hello before any other signaling message.');
      state.socket.close(1008, 'Hello required');
      return;
    }
    await registerPeer(state, message, registry, clients, options);
    return;
  }
  if (message.type === 'hello') {
    sendError(state, 'ALREADY_REGISTERED', 'This WebSocket is already registered.');
    state.socket.close(1008, 'Duplicate hello');
    return;
  }

  try {
    switch (message.type) {
      case 'join':
        await joinRoom(state, message.roomId, registry, options);
        break;
      case 'leave':
        leaveRoom(state, message.roomId, registry);
        break;
      case 'offer':
      case 'answer':
      case 'ice-candidate':
        await forwardSignal(state, message, registry, options);
        break;
      default:
        assertNever(message);
    }
  } catch (error) {
    const meshError =
      error instanceof P2PlyError
        ? error
        : new P2PlyError('REMOTE_ERROR', 'Signaling request could not be completed.', {
            cause: error,
          });
    sendError(
      state,
      meshError.code,
      meshError.message,
      'roomId' in message ? message.roomId : undefined,
    );
  }
}

async function registerPeer(
  state: ClientState,
  message: Extract<ClientSignal, { type: 'hello' }>,
  registry: PeerRegistry,
  clients: Map<WebSocket, ClientState>,
  options: ValidatedSignalingServerOptions,
): Promise<void> {
  if (registry.peerCount >= options.maxConnections) {
    sendError(
      state,
      'SERVER_LIMIT_REACHED',
      'The signaling server connection limit has been reached.',
    );
    state.socket.close(1013, 'Connection limit reached');
    return;
  }
  let authContext: unknown;
  if (options.authenticate) {
    try {
      authContext = await options.authenticate({
        request: state.request,
        peerId: message.peerId,
        ...(message.authToken === undefined ? {} : { authToken: message.authToken }),
      });
    } catch {
      authContext = false;
    }
    if (authContext === false) {
      sendError(state, 'UNAUTHORIZED', 'Authentication failed.');
      state.socket.close(1008, 'Authentication failed');
      return;
    }
  }

  const existing = registry.get(message.peerId);
  if (existing) {
    const previous = clients.get(existing.socket);
    if (previous) await cleanupClient(previous, registry, clients, options);
    try {
      existing.socket.close(4001, 'Peer session replaced');
    } catch {
      existing.socket.terminate();
    }
  }

  state.peerId = message.peerId;
  state.authContext = authContext;
  if (state.handshakeTimer) clearTimeout(state.handshakeTimer);
  state.handshakeTimer = undefined;
  const peer: RegisteredPeer = {
    peerId: message.peerId,
    socket: state.socket,
    authContext,
    rooms: new Set(),
    connectedAt: state.connectedAt,
  };
  registry.add(peer);
  send(state.socket, { v: SIGNALING_PROTOCOL_VERSION, type: 'welcome', peerId: message.peerId });
  options.logger.log('info', 'Signaling peer registered.', { peerId: message.peerId });
}

async function joinRoom(
  state: ClientState,
  roomId: string,
  registry: PeerRegistry,
  options: ValidatedSignalingServerOptions,
): Promise<void> {
  const peerId = requirePeerId(state);
  const peer = registry.get(peerId);
  if (!peer) throw new P2PlyError('UNAUTHORIZED', 'Peer session is not registered.');
  if (options.authorizeJoin && !(await options.authorizeJoin(peer.authContext, roomId))) {
    throw new P2PlyError('UNAUTHORIZED', 'Room access was denied.');
  }
  if (!peer.rooms.has(roomId) && peer.rooms.size >= options.maxRoomsPerPeer) {
    throw new P2PlyError(
      'SERVER_LIMIT_REACHED',
      'The peer room membership limit has been reached.',
    );
  }
  if (!peer.rooms.has(roomId) && registry.peersInRoom(roomId).length >= options.maxPeersPerRoom) {
    throw new P2PlyError('SERVER_LIMIT_REACHED', 'The room peer limit has been reached.');
  }
  const result = registry.join(peerId, roomId);
  send(state.socket, {
    v: SIGNALING_PROTOCOL_VERSION,
    type: 'joined',
    roomId,
    peers: result.peerIds,
  });
  if (result.joined) {
    for (const id of result.peerIds) {
      const existing = registry.get(id);
      if (existing)
        send(existing.socket, {
          v: SIGNALING_PROTOCOL_VERSION,
          type: 'peer-joined',
          roomId,
          peerId,
        });
    }
    options.logger.log('debug', 'Peer joined signaling room.', {
      peerId,
      roomId,
      roomPeerCount: result.peerIds.length + 1,
    });
  }
}

function leaveRoom(state: ClientState, roomId: string, registry: PeerRegistry): void {
  const peerId = requirePeerId(state);
  const result = registry.leave(peerId, roomId);
  if (result.left) {
    for (const id of result.peerIds) {
      const existing = registry.get(id);
      if (existing)
        send(existing.socket, { v: SIGNALING_PROTOCOL_VERSION, type: 'peer-left', roomId, peerId });
    }
  }
  send(state.socket, { v: SIGNALING_PROTOCOL_VERSION, type: 'left', roomId });
}

async function forwardSignal(
  state: ClientState,
  message: PeerSignal,
  registry: PeerRegistry,
  options: ValidatedSignalingServerOptions,
): Promise<void> {
  const fromId = requirePeerId(state);
  if (fromId === message.to) throw new P2PlyError('UNAUTHORIZED', 'A peer cannot signal itself.');
  const from = registry.get(fromId);
  const target = registry.get(message.to);
  if (!from || !target || !from.rooms.has(message.roomId) || !target.rooms.has(message.roomId)) {
    throw new P2PlyError('PEER_NOT_FOUND', 'Target peer is not available in that room.');
  }
  if (
    options.authorizeSignal &&
    !(await options.authorizeSignal(from.authContext, target.authContext, message.roomId))
  ) {
    throw new P2PlyError('UNAUTHORIZED', 'Peer signaling was denied.');
  }

  const forwarded: ServerSignal =
    message.type === 'offer'
      ? {
          v: SIGNALING_PROTOCOL_VERSION,
          type: 'offer',
          roomId: message.roomId,
          from: fromId,
          sessionId: message.sessionId,
          description: message.description,
        }
      : message.type === 'answer'
        ? {
            v: SIGNALING_PROTOCOL_VERSION,
            type: 'answer',
            roomId: message.roomId,
            from: fromId,
            sessionId: message.sessionId,
            description: message.description,
          }
        : {
            v: SIGNALING_PROTOCOL_VERSION,
            type: 'ice-candidate',
            roomId: message.roomId,
            from: fromId,
            sessionId: message.sessionId,
            candidate: message.candidate,
          };
  send(target.socket, forwarded);
  options.logger.log('debug', 'Forwarded peer signaling message.', {
    from: fromId,
    to: message.to,
    roomId: message.roomId,
    type: message.type,
  });
}

async function cleanupClient(
  state: ClientState,
  registry: PeerRegistry,
  clients: Map<WebSocket, ClientState>,
  options: ValidatedSignalingServerOptions,
): Promise<void> {
  if (state.closed) return;
  state.closed = true;
  if (state.handshakeTimer) clearTimeout(state.handshakeTimer);
  state.handshakeTimer = undefined;
  clients.delete(state.socket);
  const peerId = state.peerId;
  if (!peerId) return;
  const registered = registry.get(peerId);
  if (registered?.socket !== state.socket) return;
  const rooms = [...registered.rooms];
  registry.remove(peerId);
  for (const roomId of rooms) {
    for (const otherId of registry.peersInRoom(roomId)) {
      const other = registry.get(otherId);
      if (other)
        send(other.socket, { v: SIGNALING_PROTOCOL_VERSION, type: 'peer-left', roomId, peerId });
    }
  }
  options.logger.log('info', 'Signaling peer disconnected.', { peerId });
}

function sendError(state: ClientState, code: string, message: string, roomId?: string): void {
  if (state.socket.readyState !== WebSocket.OPEN) return;
  send(state.socket, {
    v: SIGNALING_PROTOCOL_VERSION,
    type: 'error',
    code: code.slice(0, 64),
    message: message.slice(0, 512),
    ...(roomId === undefined ? {} : { roomId }),
  });
}

function send(socket: WebSocket, message: ServerSignal): void {
  if (socket.readyState !== WebSocket.OPEN) return;
  try {
    socket.send(JSON.stringify(message));
  } catch {
    socket.close(1011, 'Signaling send failed');
  }
}

function rawDataToBuffer(raw: RawData): Buffer {
  if (Buffer.isBuffer(raw)) return raw;
  if (Array.isArray(raw)) return Buffer.concat(raw);
  if (raw instanceof ArrayBuffer) return Buffer.from(raw);
  return Buffer.from(raw as Buffer);
}

function originIsAllowed(
  request: IncomingMessage,
  options: ValidatedSignalingServerOptions,
): boolean {
  if (options.allowedOrigins === undefined) return true;
  const origin = request.headers.origin;
  if (origin === undefined) return options.allowNoOrigin;
  return options.allowedOrigins.includes(origin);
}

function rejectUpgrade(socket: Duplex, status: number, reason: string): void {
  if (socket.destroyed) return;
  socket.write(`HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  socket.destroy();
}

function handleHttpRequest(
  request: IncomingMessage,
  response: ServerResponse,
  registry: PeerRegistry,
  isClosed: () => boolean,
): void {
  const pathname = new URL(request.url ?? '/', 'http://p2ply.invalid').pathname;
  if (request.method === 'GET' && pathname === '/health') {
    response.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    });
    response.end(
      JSON.stringify({ status: 'ok', connections: registry.peerCount, rooms: registry.roomCount }),
    );
    return;
  }
  if (request.method === 'GET' && pathname === '/ready') {
    const closing = isClosed();
    response.writeHead(closing ? 503 : 200, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    });
    response.end(JSON.stringify({ status: closing ? 'closing' : 'ready' }));
    return;
  }
  response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  response.end('Not Found');
}

function pendingCount(clients: Map<WebSocket, ClientState>): number {
  let count = 0;
  for (const state of clients.values()) if (!state.peerId && !state.closed) count += 1;
  return count;
}

function requirePeerId(state: ClientState): string {
  if (!state.peerId) throw new P2PlyError('UNAUTHORIZED', 'Hello handshake required.');
  return state.peerId;
}

function addressInfo(value: string | AddressInfo | null): SignalingServerAddress {
  if (!value || typeof value === 'string')
    throw new Error('Signaling server is not listening on a TCP address.');
  return { address: value.address, family: value.family, port: value.port };
}

function assertNever(value: never): never {
  throw new Error(`Unhandled signaling message: ${String(value)}`);
}
