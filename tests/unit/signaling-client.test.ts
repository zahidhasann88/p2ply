import { describe, expect, it } from 'vitest';
import { SignalingClient } from '../../src/signaling/client.js';
import type { WebSocketFactory, WebSocketLike } from '../../src/signaling/socket.js';

const peerId = '123e4567-e89b-42d3-a456-426614174000';

class FakeSocket {
  readyState = 0;
  private readonly listeners = new Map<string, Set<EventListener>>();
  constructor(private readonly answerHello: boolean) {
    setTimeout(() => {
      this.readyState = 1;
      this.dispatch(new Event('open'));
    }, 0);
  }

  addEventListener(type: string, listener: EventListener): void {
    const listeners = this.listeners.get(type) ?? new Set<EventListener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: EventListener): void {
    this.listeners.get(type)?.delete(listener);
  }

  send(data: string): void {
    const message = JSON.parse(data) as { type?: string };
    if (message.type === 'hello' && this.answerHello) {
      setTimeout(
        () =>
          this.dispatch(
            new MessageEvent('message', {
              data: JSON.stringify({ v: 1, type: 'welcome', peerId }),
            }),
          ),
        0,
      );
    }
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState >= 2) return;
    this.readyState = 3;
    const event = Object.assign(new Event('close'), { code, reason }) as CloseEvent;
    setTimeout(() => this.dispatch(event), 0);
  }

  forceClose(code = 1006): void {
    this.close(code, 'forced transport close');
  }

  private dispatch(event: Event): void {
    for (const listener of [...(this.listeners.get(event.type) ?? [])]) listener.call(this, event);
  }
}

function createClient(
  factory: WebSocketFactory,
  onState: (state: string) => void,
  maxAttempts = 1,
): SignalingClient {
  return new SignalingClient({
    url: 'ws://test.invalid',
    peerId,
    reconnect: { enabled: true, maxAttempts, minDelayMs: 10, maxDelayMs: 20, factor: 1, jitter: 0 },
    timeoutMs: 80,
    socketFactory: factory,
    logger: { log: () => {} },
    onMessage: () => {},
    onState: (state) => onState(state),
  });
}

describe('signaling reconnection policy', () => {
  it('reconnects after an unexpected signaling disconnect and validates the new welcome', async () => {
    const sockets: FakeSocket[] = [];
    const factory: WebSocketFactory = () => {
      const socket = new FakeSocket(true);
      sockets.push(socket);
      return socket as unknown as WebSocketLike;
    };
    const states: string[] = [];
    const client = createClient(factory, (state) => states.push(state));
    await client.start();
    expect(client.state).toBe('connected');
    sockets[0]!.forceClose();
    await viWaitFor(() => client.state === 'connected' && sockets.length === 2);
    expect(states).toContain('reconnecting');
    expect(sockets).toHaveLength(2);
    await client.close();
  });

  it('stops retrying after the configured bounded attempt count', async () => {
    const sockets: FakeSocket[] = [];
    const factory: WebSocketFactory = () => {
      const socket = new FakeSocket(sockets.length === 0);
      sockets.push(socket);
      return socket as unknown as WebSocketLike;
    };
    const client = createClient(factory, () => {}, 1);
    await client.start();
    sockets[0]!.forceClose();
    await viWaitFor(() => client.state === 'disconnected');
    expect(sockets).toHaveLength(2);
    await client.close();
  });
});

async function viWaitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 1_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for signaling state.');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
