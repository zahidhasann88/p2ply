import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '../../src/errors.js';
import { InMemoryPeerRegistry } from '../../src/server/registry.js';
import { TokenBucket } from '../../src/server/rate-limit.js';
import { validateSignalingServerOptions } from '../../src/server/config.js';

describe('signaling server primitives', () => {
  it('tracks rooms and removes memberships safely', () => {
    const registry = new InMemoryPeerRegistry();
    const a = {
      peerId: 'a',
      socket: {} as never,
      authContext: undefined,
      rooms: new Set<string>(),
      connectedAt: 1,
    };
    const b = {
      peerId: 'b',
      socket: {} as never,
      authContext: undefined,
      rooms: new Set<string>(),
      connectedAt: 2,
    };
    registry.add(a);
    registry.add(b);
    expect(registry.join('a', 'room').peerIds).toEqual([]);
    expect(registry.join('b', 'room').peerIds).toEqual(['a']);
    expect(registry.peersInRoom('room')).toHaveLength(2);
    expect(registry.leave('a', 'room')).toMatchObject({ left: true, peerIds: ['b'] });
    registry.remove('b');
    expect(registry.peerCount).toBe(1);
    expect(registry.roomCount).toBe(0);
  });

  it('enforces token-bucket limits and refills over time', () => {
    const bucket = new TokenBucket(2, 1, 1_000);
    expect(bucket.consume(1, 1_000)).toBe(true);
    expect(bucket.consume(1, 1_000)).toBe(true);
    expect(bucket.consume(1, 1_000)).toBe(false);
    expect(bucket.consume(1, 2_000)).toBe(true);
  });

  it('validates safety-related server limits', () => {
    expect(validateSignalingServerOptions({ port: 0, maxPeersPerRoom: 16 }).maxPeersPerRoom).toBe(
      16,
    );
    expect(() =>
      validateSignalingServerOptions({ heartbeatIntervalMs: 30_000, staleConnectionMs: 10_000 }),
    ).toThrow(ConfigurationError);
    expect(() => validateSignalingServerOptions({ maxMessageBytes: 32 })).toThrow();
  });
});
