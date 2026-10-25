import { createSignalingServer } from '../../src/server/index.js';

const token = process.env['P2PLY_AUTH_TOKEN'];
if (!token) throw new Error('Set P2PLY_AUTH_TOKEN before starting this example.');

const server = createSignalingServer({
  host: process.env['P2PLY_HOST'] ?? '127.0.0.1',
  port: Number(process.env['P2PLY_PORT'] ?? 8787),
  allowedOrigins: process.env['P2PLY_ALLOWED_ORIGINS']?.split(',').map((origin) => origin.trim()),
  maxPeersPerRoom: 32,
  authenticate: ({ authToken }) => (authToken === token ? { role: 'member' } : false),
  authorizeJoin: (authContext, roomId) => Boolean(authContext) && roomId.startsWith('team-'),
  logger: {
    log: (level, message, context) => console.log(`[${level}] ${message}`, context ?? ''),
  },
});

const address = await server.listen();
console.log(`Signaling server on ws://${address.address}:${address.port} (health: /health)`);

const stop = (): void => void server.close();
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
