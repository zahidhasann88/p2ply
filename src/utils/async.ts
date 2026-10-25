import { P2PlyError } from '../errors.js';

export interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(reason: unknown): void;
  settled: boolean;
}

export function deferred<T>(): Deferred<T> {
  let resolvePromise!: (value: T) => void;
  let rejectPromise!: (reason: unknown) => void;
  const result: Deferred<T> = {
    promise: new Promise<T>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    }),
    resolve(value) {
      if (result.settled) return;
      result.settled = true;
      resolvePromise(value);
    },
    reject(reason) {
      if (result.settled) return;
      result.settled = true;
      rejectPromise(reason);
    },
    settled: false,
  };
  return result;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  code:
    | 'SIGNALING_TIMEOUT'
    | 'CONNECTION_TIMEOUT'
    | 'REQUEST_TIMEOUT'
    | 'TRANSFER_FAILED'
    | 'PEER_NOT_CONNECTED',
  message: string,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new P2PlyError(code, message)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

export function exponentialDelay(
  attempt: number,
  options: { minDelayMs: number; maxDelayMs: number; factor: number; jitter: number },
  random = Math.random,
): number {
  const base = Math.min(
    options.maxDelayMs,
    options.minDelayMs * options.factor ** Math.max(0, attempt - 1),
  );
  const variation = base * options.jitter;
  return Math.max(0, Math.round(base - variation + random() * variation * 2));
}
