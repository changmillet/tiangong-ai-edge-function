export interface EmailAuthRedis {
  exists(email: string): Promise<unknown>;
  set(email: string, value: string, options: { nx: true; ex: 3600 }): Promise<unknown>;
}

export interface EmailAuthCacheOptions {
  enabled?: boolean;
  now?: () => number;
  // Test override; production callers use the fixed 10 second default.
  ttlMs?: number;
  maxEntries?: number;
  maxPendingReads?: number;
}

export class EmailAuthCacheTimeoutError extends Error {
  constructor() {
    super('Email authorization observation exceeded its local lease');
    this.name = 'EmailAuthCacheTimeoutError';
  }
}

interface PendingRead {
  deadline: number;
  promise: Promise<boolean>;
}

function parseExists(result: unknown): boolean {
  if (result !== 0 && result !== 1) throw new Error('Invalid Redis EXISTS result');
  return result === 1;
}

/** Per-isolate email authorization observations, never credentials or tokens. */
export function createEmailAuthCache(redis: EmailAuthRedis, options: EmailAuthCacheOptions = {}) {
  const enabled = options.enabled ?? true;
  const now = options.now ?? (() => performance.now());
  const ttlMs = options.ttlMs ?? 10000;
  const maxEntries = options.maxEntries ?? 1024;
  const maxPendingReads = options.maxPendingReads ?? 128;
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) throw new Error('Invalid local cache TTL');
  for (const bound of [maxEntries, maxPendingReads]) {
    if (!Number.isSafeInteger(bound) || bound < 0) throw new Error('Invalid local cache bound');
  }

  const leases = new Map<string, number>();
  const pendingReads = new Map<string, PendingRead>();

  function checkDeadline(deadline: number) {
    if (now() >= deadline) throw new EmailAuthCacheTimeoutError();
  }

  function remember(email: string, deadline: number) {
    if (maxEntries === 0) return;
    const current = leases.get(email);
    // A slow older observation must not shorten a newer lease or move its FIFO position.
    if (current !== undefined) {
      if (deadline > current) leases.set(email, deadline);
      return;
    }
    if (leases.size >= maxEntries) {
      const oldest = leases.keys().next();
      if (!oldest.done) leases.delete(oldest.value);
    }
    leases.set(email, deadline);
  }

  async function readExists(email: string, deadline: number): Promise<boolean> {
    const allowed = parseExists(await redis.exists(email));
    if (allowed) {
      checkDeadline(deadline);
      remember(email, deadline);
    }
    return allowed;
  }

  return {
    isAllowed(email: string): boolean {
      if (!enabled) return false;
      const deadline = leases.get(email);
      if (deadline === undefined) return false;
      if (now() < deadline) return true;
      leases.delete(email);
      return false;
    },
    async exists(email: string): Promise<boolean> {
      if (!enabled) return parseExists(await redis.exists(email));
      let pending = pendingReads.get(email);
      if (!pending) {
        const deadline = now() + ttlMs;
        const promise = readExists(email, deadline).finally(() => {
          // Overflow reads are untracked; never remove another request's pending entry.
          if (pendingReads.get(email)?.promise === promise) pendingReads.delete(email);
        });
        pending = { deadline, promise };
        if (pendingReads.size < maxPendingReads) pendingReads.set(email, pending);
      }
      const allowed = await pending.promise;
      // Each waiter may resume later than the shared Redis response.
      if (allowed) checkDeadline(pending.deadline);
      return allowed;
    },
    async recordSuccessfulLogin(email: string, value = ''): Promise<void> {
      const deadline = enabled ? now() + ttlMs : 0;
      const result = await redis.set(email, value, { nx: true, ex: 3600 });
      if (result !== 'OK' && result !== null) throw new Error('Invalid Redis SET result');
      // Both creation and NX no-op observe a key; neither renews an existing Redis TTL.
      if (enabled) {
        checkDeadline(deadline);
        remember(email, deadline);
      }
    },
  };
}
