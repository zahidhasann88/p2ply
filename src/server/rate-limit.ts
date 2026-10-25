export class TokenBucket {
  private tokens: number;
  private lastRefill: number;

  constructor(
    private readonly capacity: number,
    private readonly tokensPerSecond: number,
    now = Date.now(),
  ) {
    this.tokens = capacity;
    this.lastRefill = now;
  }

  consume(cost = 1, now = Date.now()): boolean {
    if (cost <= 0) return true;
    const elapsed = Math.max(0, now - this.lastRefill) / 1_000;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.tokensPerSecond);
    this.lastRefill = now;
    if (this.tokens < cost) return false;
    this.tokens -= cost;
    return true;
  }

  get availableTokens(): number {
    return this.tokens;
  }
}
