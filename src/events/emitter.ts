export type EventListener<T> = (payload: T) => void;

export class TypedEmitter<Events extends object> {
  private readonly listeners = new Map<keyof Events, Set<EventListener<unknown>>>();

  constructor(
    private readonly onListenerError: (error: unknown, event: keyof Events) => void = () => {},
  ) {}

  on<K extends keyof Events>(event: K, listener: EventListener<Events[K]>): () => void {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(listener as EventListener<unknown>);
    return () => this.off(event, listener);
  }

  once<K extends keyof Events>(event: K, listener: EventListener<Events[K]>): () => void {
    const unsubscribe = this.on(event, (payload) => {
      unsubscribe();
      listener(payload);
    });
    return unsubscribe;
  }

  off<K extends keyof Events>(event: K, listener: EventListener<Events[K]>): void {
    const set = this.listeners.get(event);
    if (!set) return;
    set.delete(listener as EventListener<unknown>);
    if (set.size === 0) this.listeners.delete(event);
  }

  emit<K extends keyof Events>(event: K, payload: Events[K]): number {
    const current = this.listeners.get(event);
    if (!current) return 0;
    for (const listener of [...current]) {
      try {
        listener(payload);
      } catch (error) {
        this.onListenerError(error, event);
      }
    }
    return current.size;
  }

  listenerCount<K extends keyof Events>(event: K): number {
    return this.listeners.get(event)?.size ?? 0;
  }

  removeAllListeners(): void {
    this.listeners.clear();
  }
}
