export type P2PlyErrorCode =
  | 'CONFIGURATION_INVALID'
  | 'INVALID_STATE'
  | 'CLOSED'
  | 'SIGNALING_UNAVAILABLE'
  | 'SIGNALING_PROTOCOL_ERROR'
  | 'SIGNALING_TIMEOUT'
  | 'PROTOCOL_VERSION_UNSUPPORTED'
  | 'UNAUTHORIZED'
  | 'PEER_NOT_FOUND'
  | 'PEER_NOT_CONNECTED'
  | 'CONNECTION_FAILED'
  | 'CONNECTION_TIMEOUT'
  | 'MESSAGE_TOO_LARGE'
  | 'INVALID_MESSAGE'
  | 'SERIALIZATION_FAILED'
  | 'QUEUE_OVERFLOW'
  | 'REQUEST_TIMEOUT'
  | 'TOO_MANY_PENDING_REQUESTS'
  | 'TRANSFER_CANCELLED'
  | 'TRANSFER_REJECTED'
  | 'TRANSFER_FAILED'
  | 'TRANSFER_TOO_LARGE'
  | 'RATE_LIMITED'
  | 'SERVER_LIMIT_REACHED'
  | 'REMOTE_ERROR';

export class P2PlyError extends Error {
  readonly code: P2PlyErrorCode;
  readonly details?: unknown;

  constructor(
    code: P2PlyErrorCode,
    message: string,
    options?: { cause?: unknown; details?: unknown },
  ) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = 'P2PlyError';
    this.code = code;
    if (options?.details !== undefined) this.details = options.details;
  }
}

export class ConfigurationError extends P2PlyError {
  constructor(message: string, options?: { cause?: unknown; details?: unknown }) {
    super('CONFIGURATION_INVALID', message, options);
    this.name = 'ConfigurationError';
  }
}

export class QueueOverflowError extends P2PlyError {
  constructor(message = 'The bounded outgoing queue is full.') {
    super('QUEUE_OVERFLOW', message);
    this.name = 'QueueOverflowError';
  }
}

export class TransferError extends P2PlyError {
  constructor(
    code: 'TRANSFER_CANCELLED' | 'TRANSFER_REJECTED' | 'TRANSFER_FAILED' | 'TRANSFER_TOO_LARGE',
    message: string,
    options?: { cause?: unknown },
  ) {
    super(code, message, options);
    this.name = 'TransferError';
  }
}

export function asP2PlyError(
  error: unknown,
  fallback: P2PlyErrorCode,
  message: string,
): P2PlyError {
  if (error instanceof P2PlyError) return error;
  if (error instanceof Error) return new P2PlyError(fallback, message, { cause: error });
  return new P2PlyError(fallback, message, { details: error });
}
