import { z } from 'zod';
import type { IncomingMessage } from 'node:http';
import type { Logger } from '../types.js';
import { ConfigurationError } from '../errors.js';

export interface ServerAuthContext {
  request: IncomingMessage;
  peerId: string;
  authToken?: string;
}

export interface SignalingServerOptions {
  host?: string;
  port?: number;
  path?: string;
  allowedOrigins?: string[];
  allowNoOrigin?: boolean;
  maxConnections?: number;
  maxPeersPerRoom?: number;
  maxRoomsPerPeer?: number;
  maxMessageBytes?: number;
  messagesPerSecond?: number;
  messageBurst?: number;
  handshakeTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  staleConnectionMs?: number;
  logger?: Logger;
  authenticate?: (context: ServerAuthContext) => unknown | false | Promise<unknown | false>;
  authorizeJoin?: (authContext: unknown, roomId: string) => boolean | Promise<boolean>;
  authorizeSignal?: (from: unknown, to: unknown, roomId: string) => boolean | Promise<boolean>;
}

const schema = z
  .object({
    host: z.string().min(1).max(255).default('0.0.0.0'),
    port: z.number().int().min(0).max(65_535).default(8787),
    path: z.string().startsWith('/').max(128).default('/'),
    allowedOrigins: z.array(z.string().url()).max(1_000).optional(),
    allowNoOrigin: z.boolean().default(true),
    maxConnections: z.number().int().min(1).max(1_000_000).default(1_000),
    maxPeersPerRoom: z.number().int().min(1).max(100_000).default(128),
    maxRoomsPerPeer: z.number().int().min(1).max(10_000).default(16),
    maxMessageBytes: z
      .number()
      .int()
      .min(128)
      .max(16 * 1024 * 1024)
      .default(64 * 1024),
    messagesPerSecond: z.number().int().min(1).max(100_000).default(60),
    messageBurst: z.number().int().min(1).max(100_000).default(120),
    handshakeTimeoutMs: z.number().int().min(100).max(300_000).default(5_000),
    heartbeatIntervalMs: z.number().int().min(1_000).max(300_000).default(30_000),
    staleConnectionMs: z.number().int().min(2_000).max(900_000).default(90_000),
    logger: z
      .custom<Logger>(
        (value) =>
          value === undefined ||
          (typeof value === 'object' &&
            value !== null &&
            typeof (value as Logger).log === 'function'),
      )
      .optional(),
    authenticate: z
      .custom<SignalingServerOptions['authenticate']>(
        (value) => value === undefined || typeof value === 'function',
      )
      .optional(),
    authorizeJoin: z
      .custom<SignalingServerOptions['authorizeJoin']>(
        (value) => value === undefined || typeof value === 'function',
      )
      .optional(),
    authorizeSignal: z
      .custom<SignalingServerOptions['authorizeSignal']>(
        (value) => value === undefined || typeof value === 'function',
      )
      .optional(),
  })
  .strict()
  .refine(
    (value) => value.staleConnectionMs > value.heartbeatIntervalMs,
    'staleConnectionMs must be greater than heartbeatIntervalMs',
  );

export interface ValidatedSignalingServerOptions {
  host: string;
  port: number;
  path: string;
  allowedOrigins?: string[];
  allowNoOrigin: boolean;
  maxConnections: number;
  maxPeersPerRoom: number;
  maxRoomsPerPeer: number;
  maxMessageBytes: number;
  messagesPerSecond: number;
  messageBurst: number;
  handshakeTimeoutMs: number;
  heartbeatIntervalMs: number;
  staleConnectionMs: number;
  logger: Logger;
  authenticate?: SignalingServerOptions['authenticate'];
  authorizeJoin?: SignalingServerOptions['authorizeJoin'];
  authorizeSignal?: SignalingServerOptions['authorizeSignal'];
}

export function validateSignalingServerOptions(
  options: SignalingServerOptions = {},
): ValidatedSignalingServerOptions {
  const parsed = schema.safeParse(options);
  if (!parsed.success)
    throw new ConfigurationError('Signaling server configuration is invalid.', {
      cause: parsed.error,
      details: parsed.error.issues,
    });
  const value = parsed.data;
  return {
    ...value,
    ...(value.allowedOrigins === undefined ? {} : { allowedOrigins: value.allowedOrigins }),
    logger: value.logger ?? { log: () => {} },
    ...(value.authenticate === undefined ? {} : { authenticate: value.authenticate }),
    ...(value.authorizeJoin === undefined ? {} : { authorizeJoin: value.authorizeJoin }),
    ...(value.authorizeSignal === undefined ? {} : { authorizeSignal: value.authorizeSignal }),
  };
}
