#!/usr/bin/env node
import { startSignalingServer } from './server.js';

async function main(): Promise<void> {
  const port = parsePort(process.env['P2PLY_PORT'] ?? process.env['PORT'] ?? '8787');
  const host = process.env['P2PLY_HOST'] ?? '0.0.0.0';
  const path = process.env['P2PLY_PATH'] ?? '/';
  const allowedOrigins = process.env['P2PLY_ALLOWED_ORIGINS']
    ? process.env['P2PLY_ALLOWED_ORIGINS']
        .split(',')
        .map((origin) => origin.trim())
        .filter(Boolean)
    : undefined;

  const server = await startSignalingServer({
    host,
    port,
    path,
    ...(allowedOrigins ? { allowedOrigins } : {}),
    allowNoOrigin: process.env['P2PLY_ALLOW_NO_ORIGIN'] !== 'false',
    logger: {
      log(level, message, context) {
        const record = {
          time: new Date().toISOString(),
          level,
          message,
          ...(context ? { context } : {}),
        };
        const output = JSON.stringify(record);
        if (level === 'error') console.error(output);
        else if (level === 'warn') console.warn(output);
        else console.log(output);
      },
    },
  });

  const address = server.httpServer.address();
  const actualPort = address && typeof address !== 'string' ? address.port : port;
  console.log(`P2Ply signaling server listening on ${host}:${actualPort}`);

  let stopping = false;
  const stop = async (): Promise<void> => {
    if (stopping) return;
    stopping = true;
    await server.close();
  };
  process.once('SIGINT', () => void stop());
  process.once('SIGTERM', () => void stop());
}

function parsePort(value: string): number {
  const port = Number.parseInt(value, 10);
  if (!Number.isInteger(port) || port < 0 || port > 65_535)
    throw new Error('P2PLY_PORT must be an integer from 0 through 65535.');
  return port;
}

void main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : 'P2Ply signaling server failed to start.');
  process.exitCode = 1;
});
