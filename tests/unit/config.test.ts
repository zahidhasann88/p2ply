import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '../../src/errors.js';
import { validateP2PlyOptions } from '../../src/config/validate.js';

describe('P2Ply runtime configuration', () => {
  it('fills safe defaults and accepts a WebSocket signaling URL', () => {
    const config = validateP2PlyOptions({ signalingUrl: 'ws://localhost:8787' });
    expect(config.reconnect).toMatchObject({ enabled: true, maxAttempts: 10, minDelayMs: 250 });
    expect(config.peerReconnect).toMatchObject({ auto: true, windowMs: 60_000, maxAttempts: 5 });
    expect(config.fileChunkSize).toBe(16 * 1024);
    expect(config.maxQueuedMessages).toBeGreaterThan(0);
    expect(config.rtcConfiguration.iceServers).toBeUndefined();
  });

  it('rejects non-WebSocket URLs, unknown keys, and invalid peer identifiers', () => {
    expect(() => validateP2PlyOptions({ signalingUrl: 'https://example.test' })).toThrow(
      ConfigurationError,
    );
    expect(() =>
      validateP2PlyOptions({ signalingUrl: 'ws://localhost', unexpected: true } as never),
    ).toThrow(ConfigurationError);
    expect(() =>
      validateP2PlyOptions({ signalingUrl: 'ws://localhost', peerId: 'not-a-uuid' }),
    ).toThrow(ConfigurationError);
  });

  it('checks retry ranges and message/chunk sizing before constructing a mesh', () => {
    expect(() =>
      validateP2PlyOptions({
        signalingUrl: 'ws://localhost',
        reconnect: { minDelayMs: 500, maxDelayMs: 100 },
      }),
    ).toThrow(ConfigurationError);
    expect(() =>
      validateP2PlyOptions({
        signalingUrl: 'ws://localhost',
        fileChunkSize: 16 * 1024,
        maxMessageBytes: 1_000,
      }),
    ).toThrow(ConfigurationError);
  });
});
