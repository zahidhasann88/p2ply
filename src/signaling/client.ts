import { P2PlyError, asP2PlyError } from '../errors.js';
import type { Logger, ReconnectOptions, SignalingState } from '../types.js';
import { deferred, exponentialDelay, withTimeout } from '../utils/async.js';
import type { Deferred } from '../utils/async.js';
import { encodeJson } from '../protocol/encode.js';
import {
  SIGNALING_PROTOCOL_VERSION,
  clientSignalSchema,
  serverSignalSchema,
  type ClientSignal,
  type ServerSignal,
} from '../protocol/signaling.js';
import type { WebSocketFactory, WebSocketLike } from './socket.js';

export interface SignalingClientOptions {
  url: string;
  peerId: string;
  authToken?: string;
  reconnect: Required<ReconnectOptions>;
  timeoutMs: number;
  socketFactory: WebSocketFactory;
  logger: Logger;
  onMessage(message: ServerSignal): void;
  onState(state: SignalingState, attempt?: number, error?: P2PlyError): void;
}

const READY_STATE_OPEN = 1;
const WELCOME_TIMEOUT_MESSAGE = 'Timed out waiting for the signaling server welcome message.';

export class SignalingClient {
  private socket?: WebSocketLike;
  private stopRequested = false;
  private everWelcomed = false;
  private retryAttempts = 0;
  private retryTimer?: ReturnType<typeof setTimeout>;
  private handshakeTimer?: ReturnType<typeof setTimeout>;
  private firstWelcome?: Deferred<void>;
  private _state: SignalingState = 'disconnected';

  constructor(private readonly options: SignalingClientOptions) {}

  get state(): SignalingState {
    return this._state;
  }

  get reconnectAttempts(): number {
    return this.retryAttempts;
  }

  async start(): Promise<void> {
    if (this.everWelcomed) return;
    if (this.stopRequested)
      throw new P2PlyError('CLOSED', 'This signaling client has been closed.');
    if (this.firstWelcome) return this.firstWelcome.promise;

    this.firstWelcome = deferred<void>();
    this.setState('connecting');
    this.openSocket(false);
    try {
      await withTimeout(
        this.firstWelcome.promise,
        this.options.timeoutMs,
        'SIGNALING_TIMEOUT',
        WELCOME_TIMEOUT_MESSAGE,
      );
    } catch (error) {
      await this.close();
      throw asP2PlyError(
        error,
        'SIGNALING_UNAVAILABLE',
        'Unable to connect to the signaling server.',
      );
    }
  }

  send(message: ClientSignal): void {
    const socket = this.socket;
    if (!socket || socket.readyState !== READY_STATE_OPEN) {
      throw new P2PlyError('SIGNALING_UNAVAILABLE', 'The signaling WebSocket is not connected.');
    }
    if (!clientSignalSchema.safeParse(message).success) {
      throw new P2PlyError(
        'SIGNALING_PROTOCOL_ERROR',
        `Refusing to send an invalid ${message.type} signaling message.`,
      );
    }
    const json = encodeJson(message, 128 * 1024, 'Signaling message');
    try {
      socket.send(json);
    } catch (error) {
      throw new P2PlyError('SIGNALING_UNAVAILABLE', 'Could not write to the signaling WebSocket.', {
        cause: error,
      });
    }
  }

  async close(): Promise<void> {
    if (this.stopRequested) return;
    this.stopRequested = true;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    if (this.handshakeTimer) clearTimeout(this.handshakeTimer);
    this.retryTimer = undefined;
    this.handshakeTimer = undefined;
    this.removeSocket();
    const socket = this.socket;
    this.socket = undefined;
    if (socket && socket.readyState < 2) {
      try {
        socket.close(1000, 'P2Ply closing');
      } catch {
        // Closing an already-dead browser socket is harmless.
      }
    }
    this.firstWelcome?.reject(new P2PlyError('CLOSED', 'The signaling client was closed.'));
    this.setState('closed');
  }

  private openSocket(isReconnect: boolean): void {
    if (this.stopRequested) return;
    let socket: WebSocketLike;
    try {
      socket = this.options.socketFactory(this.options.url);
    } catch (error) {
      const meshError = asP2PlyError(
        error,
        'SIGNALING_UNAVAILABLE',
        'Could not create a signaling WebSocket.',
      );
      this.options.logger.log('warn', 'Signaling socket creation failed.', {
        error: meshError.code,
      });
      if (isReconnect) this.scheduleReconnect(meshError);
      else this.firstWelcome?.reject(meshError);
      return;
    }
    this.socket = socket;

    const onOpen = (): void => {
      if (this.stopRequested || this.socket !== socket) return;
      try {
        const hello: ClientSignal = {
          v: SIGNALING_PROTOCOL_VERSION,
          type: 'hello',
          peerId: this.options.peerId,
          ...(this.options.authToken ? { authToken: this.options.authToken } : {}),
        };
        socket.send(encodeJson(hello, 8 * 1024, 'Signaling hello'));
      } catch (error) {
        const meshError = asP2PlyError(
          error,
          'SIGNALING_UNAVAILABLE',
          'Could not send signaling hello.',
        );
        this.firstWelcome?.reject(meshError);
        try {
          socket.close(1011, 'P2Ply handshake failed');
        } catch {
          // The close event will normally drive recovery.
        }
        return;
      }
      if (this.handshakeTimer) clearTimeout(this.handshakeTimer);
      this.handshakeTimer = setTimeout(() => {
        if (this.socket !== socket || this._state === 'connected') return;
        try {
          socket.close(4000, 'Signaling welcome timeout');
        } catch {
          // The reconnect path handles a socket that no longer responds.
        }
      }, this.options.timeoutMs);
    };

    const onMessage = (event: MessageEvent): void => {
      void this.handleMessage(event, socket);
    };

    const onError = (): void => {
      this.options.logger.log('warn', 'Signaling WebSocket reported a transport error.');
    };

    const onClose = (event: CloseEvent): void => {
      if (this.socket !== socket) return;
      this.removeSocket();
      this.socket = undefined;
      if (this.handshakeTimer) clearTimeout(this.handshakeTimer);
      this.handshakeTimer = undefined;

      if (!this.everWelcomed) {
        const error = new P2PlyError(
          'SIGNALING_UNAVAILABLE',
          `Signaling connection closed before welcome (code ${event.code}).`,
        );
        this.firstWelcome?.reject(error);
        if (isReconnect && !this.stopRequested) this.scheduleReconnect(error);
        return;
      }
      if (this.stopRequested) return;
      const error = new P2PlyError(
        'SIGNALING_UNAVAILABLE',
        `Signaling connection closed (code ${event.code}).`,
      );
      this.scheduleReconnect(error);
    };

    this.boundListeners = { socket, onOpen, onMessage, onError, onClose };
    socket.addEventListener('open', onOpen);
    socket.addEventListener('message', onMessage);
    socket.addEventListener('error', onError);
    socket.addEventListener('close', onClose);
  }

  private boundListeners?: {
    socket: WebSocketLike;
    onOpen: (event: Event) => void;
    onMessage: (event: MessageEvent) => void;
    onError: (event: Event) => void;
    onClose: (event: CloseEvent) => void;
  };

  private removeSocket(): void {
    const listeners = this.boundListeners;
    if (!listeners) return;
    listeners.socket.removeEventListener('open', listeners.onOpen);
    listeners.socket.removeEventListener('message', listeners.onMessage);
    listeners.socket.removeEventListener('error', listeners.onError);
    listeners.socket.removeEventListener('close', listeners.onClose);
    this.boundListeners = undefined;
  }

  private async handleMessage(event: MessageEvent, socket: WebSocketLike): Promise<void> {
    if (this.socket !== socket) return;
    let text: string;
    try {
      text = await this.toText(event.data);
    } catch (error) {
      this.protocolFailure('Signaling server sent a non-text message.', error, socket);
      return;
    }

    let decoded: unknown;
    try {
      decoded = JSON.parse(text);
    } catch (error) {
      this.protocolFailure('Signaling server sent invalid JSON.', error, socket);
      return;
    }
    const parsed = serverSignalSchema.safeParse(decoded);
    if (!parsed.success) {
      this.protocolFailure(
        'Signaling server sent an invalid protocol message.',
        parsed.error,
        socket,
      );
      return;
    }
    const message = parsed.data;
    if (message.type === 'welcome') {
      if (message.peerId !== this.options.peerId) {
        this.protocolFailure('Signaling server assigned an unexpected peer id.', undefined, socket);
        return;
      }
      if (this.handshakeTimer) clearTimeout(this.handshakeTimer);
      this.handshakeTimer = undefined;
      this.everWelcomed = true;
      this.retryAttempts = 0;
      this.setState('connected');
      this.firstWelcome?.resolve();
    } else if (!this.everWelcomed && message.type === 'error') {
      const code = message.code === 'UNAUTHORIZED' ? 'UNAUTHORIZED' : 'REMOTE_ERROR';
      this.firstWelcome?.reject(
        new P2PlyError(code, message.message, { details: { remoteCode: message.code } }),
      );
      try {
        socket.close(1008, 'Signaling request denied');
      } catch {
        // The failed start path closes the client as well.
      }
    } else if (!this.everWelcomed) {
      this.protocolFailure(
        'Signaling server sent data before the welcome handshake.',
        undefined,
        socket,
      );
      return;
    }

    try {
      this.options.onMessage(message);
    } catch (error) {
      const meshError = asP2PlyError(
        error,
        'SIGNALING_PROTOCOL_ERROR',
        'A signaling message handler failed.',
      );
      this.options.logger.log('warn', 'Signaling message handler failed.', {
        code: meshError.code,
      });
    }
  }

  private async toText(data: unknown): Promise<string> {
    if (typeof data === 'string') return data;
    if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
    if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
    if (typeof Blob !== 'undefined' && data instanceof Blob) return data.text();
    throw new Error('Expected a UTF-8 text frame.');
  }

  private protocolFailure(message: string, cause: unknown, socket: WebSocketLike): void {
    const error = new P2PlyError('SIGNALING_PROTOCOL_ERROR', message, { cause });
    this.options.logger.log('warn', message);
    this.firstWelcome?.reject(error);
    try {
      socket.close(1002, 'Invalid signaling protocol');
    } catch {
      // The connection may have already closed.
    }
  }

  private scheduleReconnect(error: P2PlyError): void {
    if (this.stopRequested) return;
    if (
      !this.options.reconnect.enabled ||
      this.retryAttempts >= this.options.reconnect.maxAttempts
    ) {
      this.setState('disconnected', undefined, error);
      return;
    }
    this.retryAttempts += 1;
    const attempt = this.retryAttempts;
    this.setState('reconnecting', attempt, error);
    if (this.retryTimer) clearTimeout(this.retryTimer);
    const delay = exponentialDelay(attempt, this.options.reconnect);
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined;
      if (this.stopRequested) return;
      this.openSocket(true);
    }, delay);
  }

  private setState(state: SignalingState, attempt?: number, error?: P2PlyError): void {
    this._state = state;
    this.options.onState(state, attempt, error);
  }
}
