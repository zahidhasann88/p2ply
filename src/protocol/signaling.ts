import { z } from 'zod';

export const SIGNALING_PROTOCOL_VERSION = 1 as const;

export const peerIdSchema = z.string().uuid();
export const sessionIdSchema = z.string().uuid();
export const roomIdSchema = z
  .string()
  .min(1)
  .max(128)
  .refine(
    (value) => value.trim() === value && !containsControlCharacters(value),
    'Invalid room id.',
  );

function containsControlCharacters(value: string): boolean {
  return Array.from(value).some((character) => {
    const code = character.charCodeAt(0);
    return code < 32 || code === 127;
  });
}

const baseSchema = { v: z.literal(SIGNALING_PROTOCOL_VERSION) };
const descriptionSchema = z
  .object({
    type: z.enum(['offer', 'answer']),
    sdp: z.string().min(1).max(65_536),
  })
  .strict();
const candidateSchema = z
  .object({
    candidate: z.string().max(8_192),
    sdpMid: z.string().max(256).nullable().optional(),
    sdpMLineIndex: z.number().int().min(0).max(65_535).nullable().optional(),
    usernameFragment: z.string().max(256).optional(),
  })
  .strict();

export const clientSignalSchema = z.discriminatedUnion('type', [
  z
    .object({
      ...baseSchema,
      type: z.literal('hello'),
      peerId: peerIdSchema,
      authToken: z.string().max(4_096).optional(),
    })
    .strict(),
  z.object({ ...baseSchema, type: z.literal('join'), roomId: roomIdSchema }).strict(),
  z.object({ ...baseSchema, type: z.literal('leave'), roomId: roomIdSchema }).strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('offer'),
      roomId: roomIdSchema,
      to: peerIdSchema,
      sessionId: sessionIdSchema,
      description: descriptionSchema,
    })
    .strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('answer'),
      roomId: roomIdSchema,
      to: peerIdSchema,
      sessionId: sessionIdSchema,
      description: descriptionSchema,
    })
    .strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('ice-candidate'),
      roomId: roomIdSchema,
      to: peerIdSchema,
      sessionId: sessionIdSchema,
      candidate: candidateSchema,
    })
    .strict(),
]);

export const serverSignalSchema = z.discriminatedUnion('type', [
  z.object({ ...baseSchema, type: z.literal('welcome'), peerId: peerIdSchema }).strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('joined'),
      roomId: roomIdSchema,
      peers: z.array(peerIdSchema).max(10_000),
    })
    .strict(),
  z.object({ ...baseSchema, type: z.literal('left'), roomId: roomIdSchema }).strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('peer-joined'),
      roomId: roomIdSchema,
      peerId: peerIdSchema,
    })
    .strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('peer-left'),
      roomId: roomIdSchema,
      peerId: peerIdSchema,
    })
    .strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('offer'),
      roomId: roomIdSchema,
      from: peerIdSchema,
      sessionId: sessionIdSchema,
      description: descriptionSchema,
    })
    .strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('answer'),
      roomId: roomIdSchema,
      from: peerIdSchema,
      sessionId: sessionIdSchema,
      description: descriptionSchema,
    })
    .strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('ice-candidate'),
      roomId: roomIdSchema,
      from: peerIdSchema,
      sessionId: sessionIdSchema,
      candidate: candidateSchema,
    })
    .strict(),
  z
    .object({
      ...baseSchema,
      type: z.literal('error'),
      code: z.string().min(1).max(64),
      message: z.string().min(1).max(512),
      roomId: roomIdSchema.optional(),
    })
    .strict(),
]);

export type ClientSignal = z.infer<typeof clientSignalSchema>;
export type ServerSignal = z.infer<typeof serverSignalSchema>;
export type PeerSignal = Extract<ClientSignal, { type: 'offer' | 'answer' | 'ice-candidate' }>;
