import { describe, expect, it } from 'vitest';
import { dataEnvelopeSchema } from '../../src/protocol/data.js';
import { clientSignalSchema, serverSignalSchema } from '../../src/protocol/signaling.js';
import { encodeJson } from '../../src/protocol/encode.js';

const id = '123e4567-e89b-42d3-a456-426614174000';

describe('versioned protocol schemas', () => {
  it('accepts only valid versioned application and signaling envelopes', () => {
    expect(
      dataEnvelopeSchema.safeParse({
        v: 1,
        type: 'app',
        id,
        payload: { ok: true },
        ackRequested: false,
      }).success,
    ).toBe(true);
    expect(clientSignalSchema.safeParse({ v: 1, type: 'hello', peerId: id }).success).toBe(true);
    expect(serverSignalSchema.safeParse({ v: 1, type: 'welcome', peerId: id }).success).toBe(true);
  });

  it('rejects wrong versions, extra fields, and hostile SDP shapes', () => {
    expect(
      dataEnvelopeSchema.safeParse({ v: 2, type: 'app', id, payload: null, ackRequested: false })
        .success,
    ).toBe(false);
    expect(
      clientSignalSchema.safeParse({ v: 1, type: 'hello', peerId: id, admin: true }).success,
    ).toBe(false);
    expect(
      clientSignalSchema.safeParse({
        v: 1,
        type: 'offer',
        roomId: 'r',
        to: id,
        sessionId: id,
        description: { type: 'offer', sdp: 'x', extra: true },
      }).success,
    ).toBe(false);
  });

  it('measures UTF-8 bytes and reports serialization failures', () => {
    expect(() => encodeJson({ value: '🙂' }, 8, 'test')).toThrow(/configured limit/);
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(() => encodeJson(cyclic, 100, 'test')).toThrow(/serialized as JSON/);
    expect(() => encodeJson(undefined, 100, 'test')).toThrow(/JSON value/);
  });
});
