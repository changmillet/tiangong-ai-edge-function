import {
  createEmailAuthCache,
  EmailAuthCacheTimeoutError,
  type EmailAuthRedis,
} from '../supabase/functions/_shared/email_auth_cache.ts';

function assert(value: unknown, message = 'Assertion failed'): asserts value {
  if (!value) throw new Error(message);
}

function equal(actual: unknown, expected: unknown) {
  assert(Object.is(actual, expected), `Expected ${String(expected)}, received ${String(actual)}`);
}

async function rejects(promise: Promise<unknown>, expected?: new () => Error) {
  try {
    await promise;
  } catch (error) {
    assert(error instanceof Error);
    if (expected) assert(error instanceof expected, `Unexpected error: ${error.name}`);
    return;
  }
  throw new Error('Expected rejection');
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function fixture() {
  let time = 0;
  const reads: string[] = [];
  const writes: { email: string; value: string; options: { nx: true; ex: 3600 } }[] = [];
  let read: (email: string) => Promise<unknown> = () => Promise.resolve(1);
  let write: () => Promise<unknown> = () => Promise.resolve('OK');
  const redis: EmailAuthRedis = {
    exists(email) {
      reads.push(email);
      return read(email);
    },
    set(email, value, options) {
      writes.push({ email, value, options });
      return write();
    },
  };
  return {
    redis,
    reads,
    writes,
    now: () => time,
    at: (value: number) => (time = value),
    read: (value: typeof read) => (read = value),
    write: (value: typeof write) => (write = value),
  };
}

Deno.test('synchronous hits skip Redis and never slide the 10 second deadline', async () => {
  const f = fixture();
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  equal(await cache.exists('a'), true);
  for (const time of [1, 5000, 9999]) {
    f.at(time);
    equal(cache.isAllowed('a'), true);
  }
  equal(f.reads.length, 1);
  f.at(10000);
  equal(cache.isAllowed('a'), false);
});

Deno.test(
  'positive Redis observation near its hour expiry allows at most 10 seconds extra',
  async () => {
    const f = fixture();
    f.at(3599999);
    f.read(() => Promise.resolve(f.now() < 3600000 ? 1 : 0));
    const cache = createEmailAuthCache(f.redis, { now: f.now });
    equal(await cache.exists('a'), true);
    f.at(3609998);
    equal(cache.isAllowed('a'), true);
    f.at(3609999);
    equal(cache.isAllowed('a'), false);
    equal(await cache.exists('a'), false);
    equal(cache.isAllowed('a'), false);
  },
);

Deno.test('Redis response delay consumes the lease captured before the read', async () => {
  const f = fixture();
  const query = deferred<unknown>();
  f.read(() => query.promise);
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  const result = cache.exists('a');
  f.at(8000);
  query.resolve(1);
  equal(await result, true);
  f.at(9999);
  equal(cache.isAllowed('a'), true);
  f.at(10000);
  equal(cache.isAllowed('a'), false);
});

Deno.test('late positive reads fail closed and clear pending state for recovery', async () => {
  const f = fixture();
  const query = deferred<unknown>();
  f.read(() => query.promise);
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  const result = cache.exists('a');
  f.at(10000);
  query.resolve(1);
  await rejects(result, EmailAuthCacheTimeoutError);
  equal(cache.isAllowed('a'), false);
  f.read(() => Promise.resolve(1));
  equal(await cache.exists('a'), true);
  equal(f.reads.length, 2);
});

Deno.test('20 same-email misses coalesce only their Redis read', async () => {
  const f = fixture();
  const query = deferred<unknown>();
  f.read(() => query.promise);
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  const results = Array.from({ length: 20 }, () => cache.exists('a'));
  equal(f.reads.length, 1);
  query.resolve(1);
  assert((await Promise.all(results)).every((allowed) => allowed));
});

Deno.test('each coalesced waiter checks the original absolute deadline', async () => {
  const f = fixture();
  const query = deferred<unknown>();
  f.read(() => query.promise);
  let checks = 0;
  const cache = createEmailAuthCache(f.redis, {
    // Read start, response validation, first waiter, then second waiter at expiry.
    now: () => (++checks < 4 ? 9999 : 19999),
  });
  const early = cache.exists('a');
  const late = cache.exists('a');
  query.resolve(1);
  equal(await early, true);
  await rejects(late, EmailAuthCacheTimeoutError);
});

Deno.test('Redis errors propagate to every waiter without caching and permit a retry', async () => {
  const f = fixture();
  const query = deferred<unknown>();
  f.read(() => query.promise);
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  const results = [cache.exists('a'), cache.exists('a')];
  query.reject(new Error('offline'));
  await Promise.all(results.map((result) => rejects(result)));
  equal(cache.isAllowed('a'), false);
  f.read(() => Promise.resolve(1));
  equal(await cache.exists('a'), true);
  equal(f.reads.length, 2);
});

Deno.test('false results never create a negative cache or coalesce later sign-ins', async () => {
  const f = fixture();
  f.read(() => Promise.resolve(0));
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  equal(await cache.exists('a'), false);
  equal(cache.isAllowed('a'), false);
  equal(await cache.exists('a'), false);
  equal(f.reads.length, 2);
  await Promise.all([cache.recordSuccessfulLogin('a'), cache.recordSuccessfulLogin('a')]);
  equal(f.writes.length, 2);
});

Deno.test('FIFO cache eviction does not promote hits', async () => {
  const f = fixture();
  const cache = createEmailAuthCache(f.redis, { now: f.now, maxEntries: 2 });
  await cache.exists('a');
  await cache.exists('b');
  equal(cache.isAllowed('a'), true);
  await cache.exists('c');
  equal(cache.isAllowed('a'), false);
  equal(cache.isAllowed('b'), true);
  equal(cache.isAllowed('c'), true);
});

Deno.test(
  'full pending map bypasses coalescing while existing tracked reads still coalesce',
  async () => {
    const f = fixture();
    const a = deferred<unknown>();
    const b = deferred<unknown>();
    f.read((email) => (email === 'a' ? a.promise : b.promise));
    const cache = createEmailAuthCache(f.redis, { now: f.now, maxPendingReads: 1 });
    const results = [cache.exists('a'), cache.exists('b'), cache.exists('b'), cache.exists('a')];
    equal(f.reads.join(','), 'a,b,b');
    a.resolve(1);
    b.resolve(1);
    assert((await Promise.all(results)).every((allowed) => allowed));
  },
);

Deno.test('different emails have independent Redis reads and local leases', async () => {
  const f = fixture();
  const a = deferred<unknown>();
  const b = deferred<unknown>();
  f.read((email) => (email === 'a' ? a.promise : b.promise));
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  const first = cache.exists('a');
  f.at(5000);
  const second = cache.exists('b');
  a.resolve(1);
  b.resolve(1);
  await Promise.all([first, second]);
  equal(f.reads.length, 2);
  f.at(10000);
  equal(cache.isAllowed('a'), false);
  equal(cache.isAllowed('b'), true);
});

Deno.test(
  'untracked read cleanup cannot remove a newer tracked request for the same email',
  async () => {
    const f = fixture();
    const first = deferred<unknown>();
    const overflow = deferred<unknown>();
    const tracked = deferred<unknown>();
    let bReads = 0;
    f.read((email) => {
      if (email === 'a') return first.promise;
      return ++bReads === 1 ? overflow.promise : tracked.promise;
    });
    const cache = createEmailAuthCache(f.redis, { now: f.now, maxPendingReads: 1 });
    const a = cache.exists('a');
    const oldB = cache.exists('b');
    first.resolve(0);
    equal(await a, false);
    const newB = cache.exists('b');
    overflow.resolve(0);
    equal(await oldB, false);
    const waiter = cache.exists('b');
    equal(bReads, 2);
    tracked.resolve(1);
    equal(await newB, true);
    equal(await waiter, true);
  },
);

Deno.test(
  'disabled L1 performs direct reads without coalescing or response-age limits',
  async () => {
    const f = fixture();
    const query = deferred<unknown>();
    f.read(() => query.promise);
    const cache = createEmailAuthCache(f.redis, { enabled: false, now: f.now });
    const results = [cache.exists('a'), cache.exists('a')];
    equal(f.reads.length, 2);
    f.at(20000);
    query.resolve(1);
    assert((await Promise.all(results)).every((allowed) => allowed));
    await cache.recordSuccessfulLogin('a');
    equal(cache.isAllowed('a'), false);
    equal(f.writes[0].options.nx, true);
  },
);

Deno.test(
  'concurrent successful logins use NX and never renew the original Redis TTL',
  async () => {
    const f = fixture();
    let expiry: number | undefined;
    f.write(() => {
      if (expiry !== undefined) return Promise.resolve(null);
      expiry = f.now() + 3600000;
      return Promise.resolve('OK');
    });
    const cache = createEmailAuthCache(f.redis, { now: f.now });
    await Promise.all([cache.recordSuccessfulLogin('a', 'one'), cache.recordSuccessfulLogin('a')]);
    equal(expiry, 3600000);
    f.at(9000);
    await cache.recordSuccessfulLogin('a', 'two');
    equal(expiry, 3600000);
    equal(f.reads.length, 0);
    for (const write of f.writes) {
      equal(write.options.nx, true);
      equal(write.options.ex, 3600);
    }
    equal(cache.isAllowed('a'), true);
    f.at(19000);
    equal(cache.isAllowed('a'), false);
  },
);

Deno.test('successful NX no-op still seeds L1 without another Redis read', async () => {
  const f = fixture();
  f.write(() => Promise.resolve(null));
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  await cache.recordSuccessfulLogin('a');
  equal(cache.isAllowed('a'), true);
  equal(f.reads.length, 0);
});

Deno.test('late successful writes cannot seed L1 or authorize from an expired lease', async () => {
  const f = fixture();
  const query = deferred<unknown>();
  f.write(() => query.promise);
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  const result = cache.recordSuccessfulLogin('a');
  f.at(10000);
  query.resolve('OK');
  await rejects(result, EmailAuthCacheTimeoutError);
  equal(cache.isAllowed('a'), false);
});

Deno.test('write network delay consumes the same fixed request-start lease', async () => {
  const f = fixture();
  const query = deferred<unknown>();
  f.write(() => query.promise);
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  const result = cache.recordSuccessfulLogin('a');
  f.at(9000);
  query.resolve(null);
  await result;
  equal(cache.isAllowed('a'), true);
  f.at(10000);
  equal(cache.isAllowed('a'), false);
});

Deno.test('failed writes never seed local authorization', async () => {
  const f = fixture();
  f.write(() => Promise.reject(new Error('offline')));
  const cache = createEmailAuthCache(f.redis, { now: f.now });
  await rejects(cache.recordSuccessfulLogin('a'));
  equal(cache.isAllowed('a'), false);
});

Deno.test('an older read or write response cannot shorten a newer local lease', async () => {
  for (const operation of ['read', 'write']) {
    const f = fixture();
    const older = deferred<unknown>();
    if (operation === 'read') f.read(() => older.promise);
    else f.write(() => older.promise);
    const cache = createEmailAuthCache(f.redis, { now: f.now });
    const result = operation === 'read' ? cache.exists('a') : cache.recordSuccessfulLogin('a');
    f.at(5000);
    f.write(() => Promise.resolve('OK'));
    await cache.recordSuccessfulLogin('a');
    older.resolve(operation === 'read' ? 1 : 'OK');
    await result;
    f.at(12000);
    equal(cache.isAllowed('a'), true);
    f.at(15000);
    equal(cache.isAllowed('a'), false);
  }
});

Deno.test('expired older responses reject without deleting a newer valid lease', async () => {
  for (const operation of ['read', 'write']) {
    const f = fixture();
    const older = deferred<unknown>();
    if (operation === 'read') f.read(() => older.promise);
    else f.write(() => older.promise);
    const cache = createEmailAuthCache(f.redis, { now: f.now });
    const result = operation === 'read' ? cache.exists('a') : cache.recordSuccessfulLogin('a');
    f.at(5000);
    f.write(() => Promise.resolve(null));
    await cache.recordSuccessfulLogin('a');
    f.at(10000);
    older.resolve(operation === 'read' ? 1 : 'OK');
    await rejects(result, EmailAuthCacheTimeoutError);
    equal(cache.isAllowed('a'), true);
    f.at(15000);
    equal(cache.isAllowed('a'), false);
  }
});

Deno.test('invalid Redis replies reject without creating local authorization', async () => {
  for (const value of [undefined, null, true, '1', 2, -1]) {
    const f = fixture();
    f.read(() => Promise.resolve(value));
    const cache = createEmailAuthCache(f.redis, { now: f.now });
    await rejects(cache.exists('a'));
    equal(cache.isAllowed('a'), false);
    f.read(() => Promise.resolve(1));
    equal(await cache.exists('a'), true);
  }
  for (const value of [undefined, false, 1, 'no']) {
    const f = fixture();
    f.write(() => Promise.resolve(value));
    const cache = createEmailAuthCache(f.redis, { now: f.now });
    await rejects(cache.recordSuccessfulLogin('a'));
    equal(cache.isAllowed('a'), false);
  }
});
