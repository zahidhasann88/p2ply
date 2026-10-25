import { z } from 'zod';
import { ConfigurationError } from '../errors.js';
import type { Logger, P2PlyOptions, ReconnectOptions, PeerReconnectOptions } from '../types.js';
import { browserRTCFactory, type RTCFactory } from '../adapters/webrtc.js';
import { browserWebSocketFactory, type WebSocketFactory } from '../signaling/socket.js';

const reconnectSchema = z
  .object({
    enabled: z.boolean().default(true),
    maxAttempts: z.number().int().min(0).max(100).default(10),
    minDelayMs: z.number().int().min(10).max(60_000).default(250),
    maxDelayMs: z.number().int().min(10).max(300_000).default(30_000),
    factor: z.number().min(1).max(10).default(2),
    jitter: z.number().min(0).max(1).default(0.2),
  })
  .refine(
    (value) => value.maxDelayMs >= value.minDelayMs,
    'reconnect.maxDelayMs must be at least minDelayMs',
  );

const peerReconnectSchema = z
  .object({
    maxAttempts: z.number().int().min(1).max(20).default(3),
    minDelayMs: z.number().int().min(10).max(60_000).default(250),
    maxDelayMs: z.number().int().min(10).max(300_000).default(5_000),
    factor: z.number().min(1).max(10).default(2),
    jitter: z.number().min(0).max(1).default(0.2),
  })
  .refine(
    (value) => value.maxDelayMs >= value.minDelayMs,
    'peerReconnect.maxDelayMs must be at least minDelayMs',
  );

const iceServerSchema = z
  .object({
    urls: z.union([
      z.string().min(1).max(2_048),
      z.array(z.string().min(1).max(2_048)).min(1).max(32),
    ]),
    username: z.string().max(512).optional(),
    credential: z.unknown().optional(),
    credentialType: z.enum(['password', 'oauth']).optional(),
  })
  .passthrough();

const rtcConfigurationSchema = z
  .object({
    iceServers: z.array(iceServerSchema).max(32).optional(),
    iceTransportPolicy: z.enum(['all', 'relay']).optional(),
    bundlePolicy: z.enum(['balanced', 'max-compat', 'max-bundle']).optional(),
    rtcpMuxPolicy: z.literal('require').optional(),
    iceCandidatePoolSize: z.number().int().min(0).max(255).optional(),
  })
  .passthrough();

const optionsSchema = z
  .object({
    signalingUrl: z
      .string()
      .url()
      .refine(
        (value) => value.startsWith('ws://') || value.startsWith('wss://'),
        'signalingUrl must use ws:// or wss://',
      ),
    authToken: z.string().max(4_096).optional(),
    peerId: z.string().uuid().optional(),
    reconnect: reconnectSchema.default({
      enabled: true,
      maxAttempts: 10,
      minDelayMs: 250,
      maxDelayMs: 30_000,
      factor: 2,
      jitter: 0.2,
    }),
    peerReconnect: peerReconnectSchema.default({
      maxAttempts: 3,
      minDelayMs: 250,
      maxDelayMs: 5_000,
      factor: 2,
      jitter: 0.2,
    }),
    rtcConfiguration: rtcConfigurationSchema.default({}),
    connectionTimeoutMs: z.number().int().min(100).max(300_000).default(15_000),
    negotiationTimeoutMs: z.number().int().min(100).max(300_000).default(15_000),
    disconnectGraceMs: z.number().int().min(100).max(300_000).default(10_000),
    signalingTimeoutMs: z.number().int().min(100).max(300_000).default(10_000),
    maxMessageBytes: z
      .number()
      .int()
      .min(128)
      .max(16 * 1024 * 1024)
      .default(64 * 1024),
    maxBufferedBytes: z
      .number()
      .int()
      .min(1_024)
      .max(64 * 1024 * 1024)
      .default(1024 * 1024),
    maxQueuedMessages: z.number().int().min(1).max(100_000).default(256),
    maxQueuedBytes: z
      .number()
      .int()
      .min(1_024)
      .max(128 * 1024 * 1024)
      .default(1024 * 1024),
    maxPendingRequests: z.number().int().min(1).max(10_000).default(256),
    requestTimeoutMs: z.number().int().min(100).max(300_000).default(15_000),
    autoConnect: z.boolean().default(false),
    maxFileBytes: z
      .number()
      .int()
      .min(0)
      .max(Number.MAX_SAFE_INTEGER)
      .default(100 * 1024 * 1024),
    fileChunkSize: z
      .number()
      .int()
      .min(1_024)
      .max(256 * 1024)
      .default(16 * 1024),
    maxConcurrentTransfers: z.number().int().min(1).max(128).default(8),
    transferTimeoutMs: z.number().int().min(1_000).max(600_000).default(30_000),
    fileSinkFactory: z
      .custom<P2PlyOptions['fileSinkFactory']>(
        (value) => value === undefined || typeof value === 'function',
      )
      .optional(),
    authorizePeer: z
      .custom<P2PlyOptions['authorizePeer']>(
        (value) => value === undefined || typeof value === 'function',
      )
      .optional(),
    validateMessage: z
      .custom<P2PlyOptions['validateMessage']>(
        (value) => value === undefined || typeof value === 'function',
      )
      .optional(),
    logger: z
      .custom<Logger>(
        (value) =>
          value === undefined ||
          (typeof value === 'object' &&
            value !== null &&
            typeof (value as Logger).log === 'function'),
      )
      .optional(),
    rtcFactory: z
      .custom<RTCFactory>((value) => value === undefined || typeof value === 'function')
      .optional(),
    webSocketFactory: z
      .custom<WebSocketFactory>((value) => value === undefined || typeof value === 'function')
      .optional(),
    __messageType: z.unknown().optional(),
  })
  .strict()
  .superRefine((value, context) => {
    if (value.fileChunkSize + 23 > value.maxMessageBytes) {
      context.addIssue({
        code: 'custom',
        path: ['fileChunkSize'],
        message: 'fileChunkSize plus framing bytes must not exceed maxMessageBytes.',
      });
    }
    if (value.maxBufferedBytes < value.fileChunkSize + 23) {
      context.addIssue({
        code: 'custom',
        path: ['maxBufferedBytes'],
        message: 'maxBufferedBytes must fit at least one file chunk.',
      });
    }
  });

export interface ValidatedP2PlyOptions {
  signalingUrl: string;
  authToken?: string;
  peerId?: string;
  reconnect: Required<ReconnectOptions>;
  peerReconnect: Required<PeerReconnectOptions>;
  rtcConfiguration: RTCConfiguration;
  connectionTimeoutMs: number;
  negotiationTimeoutMs: number;
  disconnectGraceMs: number;
  signalingTimeoutMs: number;
  maxMessageBytes: number;
  maxBufferedBytes: number;
  maxQueuedMessages: number;
  maxQueuedBytes: number;
  maxPendingRequests: number;
  requestTimeoutMs: number;
  autoConnect: boolean;
  maxFileBytes: number;
  fileChunkSize: number;
  maxConcurrentTransfers: number;
  transferTimeoutMs: number;
  fileSinkFactory?: P2PlyOptions['fileSinkFactory'];
  authorizePeer?: P2PlyOptions['authorizePeer'];
  validateMessage?: P2PlyOptions['validateMessage'];
  logger: Logger;
  rtcFactory: RTCFactory;
  webSocketFactory: WebSocketFactory;
}

export function validateP2PlyOptions<TMessage = unknown>(
  options: P2PlyOptions<TMessage>,
): ValidatedP2PlyOptions {
  const parsed = optionsSchema.safeParse(options);
  if (!parsed.success) {
    throw new ConfigurationError('P2Ply configuration is invalid.', {
      cause: parsed.error,
      details: parsed.error.issues,
    });
  }
  const value = parsed.data;
  return {
    ...value,
    peerId: value.peerId,
    rtcConfiguration: value.rtcConfiguration as RTCConfiguration,
    logger: value.logger ?? { log: () => {} },
    rtcFactory: value.rtcFactory ?? browserRTCFactory,
    webSocketFactory: value.webSocketFactory ?? browserWebSocketFactory,
  };
}
