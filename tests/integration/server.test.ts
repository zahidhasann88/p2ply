import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { NodeP2Ply } from '../../src/node.js';
import { startSignalingServer } from '../../src/server/server.js';

function urlFor(server: Awaited<ReturnType<typeof startSignalingServer>>): string {
  const address = server.httpServer.address();
  if (!address || typeof address === 'string') throw new Error('Expected TCP address.');
  return `ws://127.0.0.1:${address.port}`;
}

describe('self-hosted signaling server integration', () => {
  it('serves health checks, registers peers, joins rooms, and cleans up peers', async () => {
    const server = await startSignalingServer({ host: '127.0.0.1', port: 0 });
    const url = urlFor(server);
    const response = await fetch(url.replace('ws:', 'http:') + '/health');
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ status: 'ok', connections: 0 });

    const peerId = '123e4567-e89b-42d3-a456-426614174001';
    const socket = new WebSocket(url);
    await once(socket, 'open');
    const welcome = once(socket, 'message');
    socket.send(JSON.stringify({ v: 1, type: 'hello', peerId }));
    expect(JSON.parse((await welcome)[0].toString())).toMatchObject({ type: 'welcome', peerId });
    const joined = once(socket, 'message');
    socket.send(JSON.stringify({ v: 1, type: 'join', roomId: 'docs' }));
    expect(JSON.parse((await joined)[0].toString())).toMatchObject({
      type: 'joined',
      roomId: 'docs',
      peers: [],
    });
    expect(server.getStats()).toMatchObject({ connections: 1, rooms: 1 });
    socket.close();
    await once(socket, 'close');
    await expect
      .poll(() => server.getStats(), { timeout: 2_000 })
      .toMatchObject({ connections: 0, rooms: 0 });
    await server.close();
  });

  it('rejects malformed signaling frames and enforces configured origins', async () => {
    const server = await startSignalingServer({
      host: '127.0.0.1',
      port: 0,
      allowedOrigins: ['https://allowed.example'],
    });
    const url = urlFor(server);
    const blocked = new WebSocket(url, { origin: 'https://denied.example' });
    const [, response] = await once(blocked, 'unexpected-response');
    expect(response.statusCode).toBe(403);

    const socket = new WebSocket(url, { origin: 'https://allowed.example' });
    await once(socket, 'open');
    const closed = once(socket, 'close');
    socket.send('{"v":7,"type":"hello"}');
    expect((await closed)[0]).toBe(1008);
    await server.close();
  });

  it('runs authentication and room authorization hooks and rejects denied operations', async () => {
    const server = await startSignalingServer({
      host: '127.0.0.1',
      port: 0,
      authenticate: ({ authToken }) =>
        authToken === 'secret-test-token' ? { subject: 'test-user' } : false,
      authorizeJoin: (context, roomId) => Boolean(context) && roomId === 'allowed-room',
    });
    const url = urlFor(server);
    const denied = await NodeP2Ply.create({
      signalingUrl: url,
      authToken: 'wrong-token',
      reconnect: { enabled: false },
    });
    await expect(denied.start()).rejects.toMatchObject({ code: 'UNAUTHORIZED' });
    await denied.close();

    const allowed = await NodeP2Ply.create({
      signalingUrl: url,
      authToken: 'secret-test-token',
      reconnect: { enabled: false },
    });
    await allowed.start();
    await expect(allowed.join('denied-room')).rejects.toMatchObject({ code: 'UNAUTHORIZED' });
    await allowed.join('allowed-room');
    expect(allowed.roomIds).toEqual(['allowed-room']);
    await allowed.close();
    await server.close();
  });

  it('applies per-connection signaling rate limits', async () => {
    const server = await startSignalingServer({
      host: '127.0.0.1',
      port: 0,
      messagesPerSecond: 1,
      messageBurst: 2,
    });
    const socket = new WebSocket(urlFor(server));
    await once(socket, 'open');
    const first = once(socket, 'message');
    socket.send(
      JSON.stringify({ v: 1, type: 'hello', peerId: '123e4567-e89b-42d3-a456-426614174002' }),
    );
    await first;
    const second = once(socket, 'message');
    socket.send(JSON.stringify({ v: 1, type: 'join', roomId: 'rate' }));
    await second;
    const third = once(socket, 'message');
    const closed = once(socket, 'close');
    socket.send(JSON.stringify({ v: 1, type: 'join', roomId: 'rate' }));
    expect(JSON.parse((await third)[0].toString())).toMatchObject({
      type: 'error',
      code: 'RATE_LIMITED',
    });
    expect((await closed)[0]).toBe(1008);
    await server.close();
  });
});
