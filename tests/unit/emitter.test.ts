import { describe, expect, it, vi } from 'vitest';
import { TypedEmitter } from '../../src/events/emitter.js';

type Events = { event: { value: number }; other: string };

describe('TypedEmitter', () => {
  it('supports subscription, unsubscription, and listener counts', () => {
    const emitter = new TypedEmitter<Events>();
    const listener = vi.fn();
    const off = emitter.on('event', listener);
    expect(emitter.listenerCount('event')).toBe(1);
    expect(emitter.emit('event', { value: 3 })).toBe(1);
    expect(listener).toHaveBeenCalledWith({ value: 3 });
    off();
    expect(emitter.listenerCount('event')).toBe(0);
    emitter.emit('event', { value: 4 });
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('runs once listeners once and isolates listener exceptions', () => {
    const onError = vi.fn();
    const emitter = new TypedEmitter<Events>(onError);
    const once = vi.fn();
    emitter.once('other', once);
    emitter.on('other', () => {
      throw new Error('consumer error');
    });
    emitter.emit('other', 'first');
    emitter.emit('other', 'second');
    expect(once).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledTimes(2);
  });
});
