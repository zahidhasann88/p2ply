export function randomId(): string {
  const cryptoApi = globalThis.crypto;
  if (!cryptoApi?.getRandomValues) {
    throw new Error('P2Ply requires a cryptographically secure Web Crypto implementation.');
  }

  if (typeof cryptoApi.randomUUID === 'function') return cryptoApi.randomUUID();

  const bytes = cryptoApi.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function uuidToBytes(uuid: string): Uint8Array {
  const compact = uuid.replaceAll('-', '');
  if (!/^[0-9a-f]{32}$/i.test(compact)) throw new Error('Invalid UUID.');
  const bytes = new Uint8Array(16);
  for (let i = 0; i < bytes.length; i += 1)
    bytes[i] = Number.parseInt(compact.slice(i * 2, i * 2 + 2), 16);
  return bytes;
}

export function bytesToUuid(bytes: Uint8Array): string {
  if (bytes.length !== 16) throw new Error('A UUID must contain 16 bytes.');
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
