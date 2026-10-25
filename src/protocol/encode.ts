import { P2PlyError } from '../errors.js';

const textEncoder = new TextEncoder();

export function encodeJson(value: unknown, maxBytes: number, label: string): string {
  let serialized: string | undefined;
  try {
    serialized = JSON.stringify(value);
  } catch (error) {
    throw new P2PlyError('SERIALIZATION_FAILED', `${label} could not be serialized as JSON.`, {
      cause: error,
    });
  }
  if (serialized === undefined) {
    throw new P2PlyError('SERIALIZATION_FAILED', `${label} did not produce a JSON value.`);
  }
  const bytes = textEncoder.encode(serialized).byteLength;
  if (bytes > maxBytes) {
    throw new P2PlyError(
      'MESSAGE_TOO_LARGE',
      `${label} is ${bytes} bytes; the configured limit is ${maxBytes} bytes.`,
      { details: { bytes, maxBytes } },
    );
  }
  return serialized;
}

export function byteLength(value: string): number {
  return textEncoder.encode(value).byteLength;
}
