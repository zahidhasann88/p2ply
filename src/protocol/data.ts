import { z } from 'zod';

export const DATA_PROTOCOL_VERSION = 1 as const;
const uuid = z.string().uuid();
const base = { v: z.literal(DATA_PROTOCOL_VERSION) };

export const dataEnvelopeSchema = z.discriminatedUnion('type', [
  z
    .object({
      ...base,
      type: z.literal('app'),
      id: uuid,
      payload: z.unknown(),
      ackRequested: z.boolean(),
    })
    .strict(),
  z.object({ ...base, type: z.literal('ack'), id: uuid }).strict(),
  z.object({ ...base, type: z.literal('request'), id: uuid, payload: z.unknown() }).strict(),
  z
    .object({ ...base, type: z.literal('response'), id: uuid, replyTo: uuid, payload: z.unknown() })
    .strict(),
  z
    .object({
      ...base,
      type: z.literal('response-error'),
      id: uuid,
      replyTo: uuid,
      message: z.string().max(512),
    })
    .strict(),
  z
    .object({
      ...base,
      type: z.literal('file-start'),
      transferId: uuid,
      metadata: z
        .object({
          name: z.string().min(1).max(255),
          size: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
          mimeType: z.string().max(255),
          lastModified: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
          chunkSize: z.number().int().min(1).max(1_048_576),
          totalChunks: z.number().int().min(0).max(4_294_967_295),
        })
        .strict(),
    })
    .strict(),
  z.object({ ...base, type: z.literal('file-ready'), transferId: uuid }).strict(),
  z
    .object({
      ...base,
      type: z.literal('file-reject'),
      transferId: uuid,
      reason: z.string().min(1).max(512),
    })
    .strict(),
  z
    .object({
      ...base,
      type: z.literal('file-chunk-ack'),
      transferId: uuid,
      sequence: z.number().int().min(0).max(4_294_967_295),
    })
    .strict(),
  z
    .object({
      ...base,
      type: z.literal('file-complete'),
      transferId: uuid,
      sha256: z.string().regex(/^[0-9a-f]{64}$/i),
    })
    .strict(),
  z.object({ ...base, type: z.literal('file-complete-ack'), transferId: uuid }).strict(),
  z
    .object({
      ...base,
      type: z.literal('file-cancel'),
      transferId: uuid,
      reason: z.string().max(512),
    })
    .strict(),
]);

export type DataEnvelope = z.infer<typeof dataEnvelopeSchema>;
export type AppEnvelope = Extract<DataEnvelope, { type: 'app' }>;
export type RequestEnvelope = Extract<DataEnvelope, { type: 'request' }>;
export type FileStartEnvelope = Extract<DataEnvelope, { type: 'file-start' }>;
