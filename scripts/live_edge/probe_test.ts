import { createProbeHandler, type Ports, validateBulkResponse } from './probe.ts';

const RUN = 'a1b2c3d4e5f60718';
const REF = 'dlqbkgbktyeqztezqzlo';
const TOKEN = 'a'.repeat(64);
const PREFIX = `live72-${RUN}-`;
const SOURCES = ['edu', 'esg', 'standard', 'sci'];
function assert(value: unknown, message = 'assertion failed'): asserts value {
  if (!value) throw new Error(message);
}
function environment() {
  const values: Record<string, string> = {
    LIVE_EDGE_RUN_ID: RUN,
    LIVE_EDGE_PROJECT_REF: REF,
    LIVE_EDGE_RUN_TOKEN: TOKEN,
    SUPABASE_URL: `https://${REF}.supabase.co`,
    PINECONE_API_KEY_US_EAST_1: 'synthetic',
    PINECONE_INDEX_NAME: 'existing-authorized-index',
    OPENSEARCH_REGION: 'synthetic',
    OPENSEARCH_DOMAIN: 'https://synthetic.invalid',
    OPENAI_API_KEY: 'synthetic',
    OPENAI_EMBEDDING_MODEL: 'synthetic',
    OPENSEARCH_LOG_INDEX_NAME: PREFIX + 'log',
  };
  for (const source of SOURCES) {
    values[`PINECONE_NAMESPACE_${source.toUpperCase()}`] = PREFIX + source;
    values[`OPENSEARCH_${source.toUpperCase()}_INDEX_NAME`] = PREFIX + source;
  }
  return values;
}
function fixture() {
  return {
    marker: `synthetic${RUN}`,
    edu_name: `synthetic${RUN}name`,
    edu_course: `synthetic${RUN}course`,
    journal_doi: `10.99999/${RUN}-edge`,
    resource_ids: {
      education: '11111111-1111-1111-1111-111111111111',
      esg: '22222222-2222-2222-2222-222222222222',
      standards: '33333333-3333-3333-3333-333333333333',
    },
  };
}
function request(body: any, token = TOKEN) {
  return new Request('https://synthetic.invalid/live-probe', {
    method: 'POST',
    headers: { authorization: 'Bearer gateway-verified-fixture', 'x-live-edge-run-token': token },
    body: JSON.stringify(body),
  });
}
function fake() {
  const namespaces = new Map<string, Map<string, any>>();
  const indexes = new Map<string, { mappings: any; documents: any[] }>();
  const writes: string[] = [];
  let embeddings = 0;
  const ports: Ports = {
    stats: async () => ({
      dimension: 3,
      namespaces: Object.fromEntries(
        [...namespaces].map(([name, records]) => [name, { recordCount: records.size }]),
      ),
    }),
    list: async (namespace) => ({
      vectors: [...(namespaces.get(namespace)?.keys() ?? [])].map((id) => ({ id })),
    }),
    fetch: async (namespace, ids) => ({
      records: Object.fromEntries(
        ids
          .filter((id) => namespaces.get(namespace)?.has(id))
          .map((id) => [id, namespaces.get(namespace)!.get(id)]),
      ),
    }),
    upsert: async (namespace, records) => {
      writes.push(`upsert:${namespace}`);
      const rows = namespaces.get(namespace) ?? new Map();
      records.forEach((record) => rows.set(record.id, record));
      namespaces.set(namespace, rows);
    },
    deleteVectors: async (namespace, ids) => {
      writes.push(`deleteVectors:${namespace}`);
      ids.forEach((id) => namespaces.get(namespace)?.delete(id));
    },
    getIndex: async (index) => indexes.get(index) ?? null,
    getDocuments: async (index) => indexes.get(index)!.documents,
    createIndex: async (index, owner) => {
      writes.push(`create:${index}`);
      indexes.set(index, { mappings: { _meta: owner }, documents: [] });
    },
    writeDocuments: async (index, records) => {
      writes.push(`bulk:${index}`);
      indexes.get(index)!.documents = records.map((record) => ({
        _id: record.id,
        _source: record.metadata,
      }));
    },
    deleteIndex: async (index) => {
      writes.push(`deleteIndex:${index}`);
      indexes.delete(index);
    },
    embed: async () => {
      embeddings++;
      return [0.1, 0.2, 0.3];
    },
    logs: async (_index, emails) =>
      emails.map((email) => ({
        email,
        service_type: 'edu_search',
        invoked_at: 1,
        top_k: 1,
        ext_k: 1,
      })),
    redis: async (_email, remove) => ({ exists: remove ? 0 : 1, ttl: remove ? -2 : 3600 }),
  };
  return { ports, namespaces, indexes, writes, embeddingCount: () => embeddings };
}
function setup(values = environment(), provider = fake()) {
  let created = 0;
  const handler = createProbeHandler(
    (name) => values[name],
    () => {
      created++;
      return provider.ports;
    },
  );
  const seed = () =>
    handler(
      request({ action: 'seed', fixture: fixture(), query_text: `synthetic retrieval ${RUN}` }),
    );
  return { handler, seed, provider, portsCreated: () => created };
}

Deno.test(
  'probe inspect reports booleans without initializing providers or needing Redis',
  async () => {
    const test = setup();
    const response = await test.handler(request({ action: 'inspect' }));
    const body = await response.json();
    assert(
      response.status === 200 &&
        Object.values(body.presence).every((value) => typeof value === 'boolean'),
    );
    assert(
      body.presence.UPSTASH_REDIS_TOKEN === false &&
        body.presence.OPENSEARCH_LOG_INDEX_NAME === true,
    );
    assert(!JSON.stringify(body).includes(TOKEN) && test.portsCreated() === 0);
  },
);
Deno.test(
  'probe refuses token bypass and production/remote target before provider calls',
  async () => {
    const test = setup();
    assert((await test.handler(request({ action: 'inspect' }, 'wrong'))).status === 401);
    assert(test.portsCreated() === 0);
    const changes: Array<Record<string, string>> = [
      {
        LIVE_EDGE_PROJECT_REF: 'qyyqlnwqwgvzxnccnbgm',
        SUPABASE_URL: 'https://qyyqlnwqwgvzxnccnbgm.supabase.co',
      },
      { SUPABASE_URL: 'https://qyyqlnwqwgvzxnccnbgm.supabase.co' },
      { REMOTE_SUPABASE_URL: 'https://qyyqlnwqwgvzxnccnbgm.supabase.co' },
      { PINECONE_NAMESPACE_EDU: 'production' },
      { OPENSEARCH_LOG_INDEX_NAME: 'function_log' },
      { OPENSEARCH_ESG_INDEX_NAME: PREFIX + 'esg-other' },
    ];
    for (const change of changes) {
      const guarded = setup({ ...environment(), ...change });
      assert((await guarded.handler(request({ action: 'inspect' }))).status === 403);
      assert(guarded.portsCreated() === 0);
    }
  },
);
Deno.test('probe requires gateway Bearer header and valid run config', async () => {
  const test = setup();
  const req = request({ action: 'inspect' });
  req.headers.delete('authorization');
  assert((await test.handler(req)).status === 401);
  const invalid = setup({ ...environment(), LIVE_EDGE_RUN_ID: 'arbitrary' });
  assert((await invalid.handler(request({ action: 'inspect' }))).status === 503);
});
Deno.test('probe bounds bytes and rejects real email namespaces before providers', async () => {
  const test = setup();
  assert(
    (await test.handler(request({ action: 'inspect', extra: 'x'.repeat(32768) }))).status === 413,
  );
  for (const emails of [
    ['real.user@example.com'],
    ['unrelated@example.invalid'],
    [RUN + '@example.invalid', ...Array(8).fill(RUN + 'x@example.invalid')],
  ]) {
    assert((await test.handler(request({ action: 'cleanup', emails }))).status === 400);
  }
  assert(test.portsCreated() === 0);
});
Deno.test(
  'seed creates owned fixtures once and preserves idempotence and real-provider branch markers',
  async () => {
    const test = setup();
    const first = await test.seed();
    assert(first.status === 200 && (await first.json()).ready);
    const writes = test.provider.writes.length;
    assert(test.provider.embeddingCount() === 1 && writes === 13);
    const second = await test.seed();
    assert(second.status === 200 && (await second.json()).ready);
    assert(test.provider.writes.length === writes && test.provider.embeddingCount() === 1);
    for (const source of SOURCES) {
      const vectors = test.provider.namespaces.get(PREFIX + source)!;
      const docs = test.provider.indexes.get(PREFIX + source)!.documents;
      assert(vectors.size === 2 && docs.length === 3);
      assert(vectors.get(PREFIX + source + '_0').metadata.text.includes('PINECONE_ONLY_' + RUN));
      assert(!docs.some((doc) => doc._id === PREFIX + source + '_0'));
      assert(
        docs
          .find((doc) => doc._id === PREFIX + source + '_2')
          ._source.text.includes('OPENSEARCH_ONLY_' + RUN),
      );
      assert(!vectors.has(PREFIX + source + '_2'));
      assert(
        docs
          .find((doc) => doc._id === PREFIX + source + '_3')
          ._source.text.includes('EXTENSION_' + RUN),
      );
    }
  },
);
Deno.test('seed refuses unknown namespace records or foreign index before all writes', async () => {
  for (const kind of ['namespace', 'index']) {
    const test = setup();
    if (kind === 'namespace')
      test.provider.namespaces.set(PREFIX + 'sci', new Map([['foreign-record', { metadata: {} }]]));
    else
      test.provider.indexes.set(PREFIX + 'sci', {
        mappings: { _meta: { owner_run_id: 'foreign' } },
        documents: [],
      });
    const result = await test.seed();
    assert(result.status === 409);
    assert(test.provider.writes.length === 0 && test.provider.embeddingCount() === 0);
  }
});
Deno.test('seed rejects changed payload rather than reusing previous fixture claims', async () => {
  const test = setup();
  await test.seed();
  const count = test.provider.writes.length;
  const result = await test.handler(
    request({ action: 'seed', fixture: fixture(), query_text: `different synthetic query ${RUN}` }),
  );
  assert(result.status === 409 && test.provider.writes.length === count);
});
Deno.test(
  'cleanup only deletes owned IDs/indexes and scoped email keys, verifies absence, repeats safely',
  async () => {
    const test = setup();
    await test.seed();
    const body = { action: 'cleanup', emails: [`live-${RUN}@example.invalid`] };
    const result = await test.handler(request(body));
    const receipt = await result.json();
    assert(result.status === 200 && receipt.cleanup_verified && receipt.cleaned);
    assert(
      test.provider.indexes.size === 0 &&
        [...test.provider.namespaces.values()].every((rows) => rows.size === 0),
    );
    assert((await (await test.handler(request(body))).json()).cleanup_verified);
  },
);
Deno.test(
  'cleanup refuses polluted owned-name namespace before deleting any resource',
  async () => {
    const test = setup();
    await test.seed();
    test.provider.writes.length = 0;
    test.provider.namespaces.get(PREFIX + 'edu')!.set(PREFIX + 'edu_9', { metadata: {} });
    const result = await test.handler(request({ action: 'cleanup' }));
    assert(
      result.status === 409 &&
        test.provider.writes.length === 0 &&
        test.provider.indexes.size === 5,
    );
  },
);
Deno.test('stats provides only scoped log records and Redis TTL', async () => {
  const test = setup();
  await test.seed();
  const result = await test.handler(
    request({ action: 'stats', emails: [`live-${RUN}@example.invalid`] }),
  );
  const body = await result.json();
  assert(
    result.status === 200 &&
      body.log_count === 1 &&
      body.log_records[0].email === `live-${RUN}@example.invalid`,
  );
  assert(body.redis[0].ttl === 3600 && body.redis[0].exists === 1);
});
Deno.test(
  'provider failure retains only operation enum and numeric HTTP status, never raw SDK text',
  async () => {
    const test = setup();
    test.provider.ports.stats = async () => {
      throw {
        meta: { statusCode: 403 },
        message: 'TOKEN-secret@example.com',
        body: { secret: 'private' },
      };
    };
    const result = await test.seed();
    const body = await result.json();
    assert(result.status === 502 && body.error === 'pinecone_stats_failed_403');
    assert(!JSON.stringify(body).includes('TOKEN') && !JSON.stringify(body).includes('private'));
  },
);
Deno.test('dimension mismatch makes no external writes', async () => {
  const test = setup();
  test.provider.ports.embed = async () => [0.1];
  const result = await test.seed();
  assert(result.status === 502 && (await result.json()).error === 'embedding_dimension_mismatch');
  assert(test.provider.writes.length === 0);
});
Deno.test('inconsistent namespace list/count/fetch never permits seed overwrite', async () => {
  const test = setup();
  await test.seed();
  test.provider.writes.length = 0;
  test.provider.ports.fetch = async () => ({ records: {} });
  const result = await test.seed();
  assert(
    result.status === 409 && (await result.json()).error === 'namespace_visibility_not_converged',
  );
  assert(test.provider.writes.length === 0 && test.provider.embeddingCount() === 1);
});
Deno.test('failed seed drains delayed peer writes before releasing the mutation lock', async () => {
  const test = setup();
  let release!: () => void;
  let entered!: () => void;
  const delayed = new Promise<void>((resolve) => (release = resolve));
  const active = new Promise<void>((resolve) => (entered = resolve));
  const original = test.provider.ports.upsert;
  test.provider.ports.upsert = async (namespace, records) => {
    if (namespace.endsWith('esg')) throw new Error('synthetic failure');
    if (namespace.endsWith('sci')) {
      entered();
      await delayed;
    }
    await original(namespace, records);
  };
  const seeding = test.seed();
  await active;
  const cleanup = await test.handler(request({ action: 'cleanup' }));
  assert(cleanup.status === 409 && (await cleanup.json()).error === 'mutation_already_running');
  release();
  assert((await seeding).status === 502);
});
Deno.test(
  'failed cleanup drains delayed peer deletions before permitting another seed',
  async () => {
    const test = setup();
    await test.seed();
    let release!: () => void;
    let entered!: () => void;
    const delayed = new Promise<void>((resolve) => (release = resolve));
    const active = new Promise<void>((resolve) => (entered = resolve));
    const original = test.provider.ports.deleteIndex;
    test.provider.ports.deleteIndex = async (index) => {
      if (index.endsWith('esg')) throw new Error('synthetic failure');
      if (index.endsWith('sci')) {
        entered();
        await delayed;
      }
      await original(index);
    };
    const cleanup = test.handler(request({ action: 'cleanup' }));
    await active;
    const seed = await test.seed();
    assert(seed.status === 409 && (await seed.json()).error === 'mutation_already_running');
    release();
    assert((await cleanup).status === 502);
  },
);
Deno.test(
  'bulk item diagnostics contain only numeric status and bounded type, never reason or body',
  () => {
    const cases = [
      {
        status: 507,
        error: { type: 'resource_limit_exception', reason: 'secret token URL credentials' },
      },
      { status: 403, error: { type: 'https://SECRET.invalid/token', reason: 'private' } },
      { status: 'secret-status', error: { type: 'index_not_found_exception', reason: 'private' } },
    ];
    const expected = [
      'opensearch_bulk_item_failed_507_resource_limit_exception',
      'opensearch_bulk_item_failed_403_unknown',
      'opensearch_bulk_item_failed_unknown_index_not_found_exception',
    ];
    for (let i = 0; i < cases.length; i++) {
      try {
        validateBulkResponse({ errors: true, items: [{ index: cases[i] }] });
        throw new Error('did not refuse');
      } catch (error: any) {
        assert(
          error.code === expected[i] &&
            !error.message.includes('secret') &&
            !error.message.includes('private'),
        );
      }
    }
    validateBulkResponse({ errors: false, items: [] });
  },
);
Deno.test('owned cleanup does not depend on overloaded AOSS document search', async () => {
  const test = setup();
  await test.seed();
  let searched = 0;
  test.provider.ports.getDocuments = async () => {
    searched++;
    throw { meta: { statusCode: 507 }, reason: 'private' };
  };
  const failed = await test.handler(request({ action: 'stats' }));
  assert(failed.status === 502 && (await failed.json()).error === 'opensearch_search_failed_507');
  searched = 0;
  const cleanup = await test.handler(request({ action: 'cleanup' }));
  const result = await cleanup.json();
  assert(
    cleanup.status === 200 &&
      result.cleanup_verified &&
      test.provider.indexes.size === 0 &&
      searched === 0,
  );
});
Deno.test(
  'single-source seed uses only one search index plus shared log and source-specific readiness',
  async () => {
    const test = setup();
    const body = {
      action: 'seed',
      source: 'edu',
      fixture: fixture(),
      query_text: `synthetic retrieval ${RUN}`,
    };
    const first = await test.handler(request(body));
    const ready = await first.json();
    assert(first.status === 200 && ready.ready && ready.source === 'edu');
    assert(
      test.provider.indexes.size === 2 &&
        test.provider.namespaces.size === 1 &&
        test.provider.writes.length === 4,
    );
    assert(Object.keys(ready.sources).join(',') === 'edu');
    const second = await test.handler(request(body));
    assert(second.status === 200 && (await second.json()).ready);
    assert(test.provider.writes.length === 4 && test.provider.embeddingCount() === 1);
    const stats = await test.handler(request({ action: 'stats', source: 'edu' }));
    assert(stats.status === 200 && (await stats.json()).ready);
  },
);
Deno.test(
  'partial cleanup keeps other phases, shared log and Redis; final cleanup removes all',
  async () => {
    const test = setup();
    for (const source of ['edu', 'esg']) {
      const seed = await test.handler(
        request({
          action: 'seed',
          source,
          fixture: fixture(),
          query_text: `synthetic retrieval ${RUN}`,
        }),
      );
      assert(seed.status === 200 && (await seed.json()).ready);
    }
    let redisOperations = 0;
    const original = test.provider.ports.redis;
    test.provider.ports.redis = async (email, remove) => {
      redisOperations++;
      return await original(email, remove);
    };
    const result = await test.handler(request({ action: 'cleanup', source: 'edu' }));
    const receipt = await result.json();
    assert(
      result.status === 200 &&
        receipt.cleanup_verified &&
        receipt.source === 'edu' &&
        receipt.shared_log_preserved,
    );
    assert(
      test.provider.indexes.has(PREFIX + 'log') &&
        test.provider.indexes.has(PREFIX + 'esg') &&
        !test.provider.indexes.has(PREFIX + 'edu'),
    );
    assert(test.provider.namespaces.get(PREFIX + 'esg')!.size === 2 && redisOperations === 0);
    const final = await test.handler(
      request({ action: 'cleanup', emails: [`live-${RUN}@example.invalid`] }),
    );
    assert(
      final.status === 200 &&
        (await final.json()).cleanup_verified &&
        test.provider.indexes.size === 0 &&
        Number(redisOperations) === 1,
    );
  },
);
Deno.test(
  'rejects arbitrary source and Redis mutation in partial cleanup before provider work',
  async () => {
    const test = setup();
    for (const body of [
      { action: 'seed', source: 'arbitrary-index' },
      { action: 'stats', source: null },
      { action: 'cleanup', source: 'edu', emails: [`live-${RUN}@example.invalid`] },
      { action: 'cleanup', source: 'edu', emails: [] },
    ]) {
      assert((await test.handler(request(body))).status === 400);
    }
    assert(test.portsCreated() === 0);
  },
);
Deno.test(
  'lagging namespace stats allow verified owned readiness and idempotent seed without rewriting',
  async () => {
    const test = setup();
    const payload = {
      action: 'seed',
      source: 'edu',
      fixture: fixture(),
      query_text: `synthetic retrieval ${RUN}`,
    };
    assert((await test.handler(request(payload))).status === 200);
    const original = test.provider.ports.stats;
    test.provider.ports.stats = async () => {
      const stats = await original();
      stats.namespaces[PREFIX + 'edu'].recordCount = 0;
      return stats;
    };
    const writes = test.provider.writes.length;
    const stats = await test.handler(request({ action: 'stats', source: 'edu' }));
    const observed = await stats.json();
    assert(stats.status === 200 && observed.ready && observed.sources.edu.namespace_count === 0);
    assert(observed.sources.edu.stats_lag === true && observed.sources.edu.vectors_visible === 2);
    const seeded = await test.handler(request(payload));
    assert(seeded.status === 200 && (await seeded.json()).ready);
    assert(test.provider.writes.length === writes && test.provider.embeddingCount() === 1);
  },
);
Deno.test(
  'reported count one and listed one but unfetched ownership still refuse all seed writes',
  async () => {
    const test = setup();
    await test.handler(
      request({
        action: 'seed',
        source: 'edu',
        fixture: fixture(),
        query_text: `synthetic retrieval ${RUN}`,
      }),
    );
    test.provider.namespaces.get(PREFIX + 'edu')!.delete(PREFIX + 'edu_1');
    test.provider.ports.fetch = async () => ({ records: {} });
    test.provider.writes.length = 0;
    const seeded = await test.handler(
      request({
        action: 'seed',
        source: 'edu',
        fixture: fixture(),
        query_text: `synthetic retrieval ${RUN}`,
      }),
    );
    assert(
      seeded.status === 409 && (await seeded.json()).error === 'namespace_visibility_not_converged',
    );
    assert(test.provider.writes.length === 0);
  },
);
Deno.test(
  'stats count exceeding owned fetched and listed records cannot conceal unobserved records',
  async () => {
    const test = setup();
    await test.handler(
      request({
        action: 'seed',
        source: 'edu',
        fixture: fixture(),
        query_text: `synthetic retrieval ${RUN}`,
      }),
    );
    test.provider.namespaces.get(PREFIX + 'edu')!.delete(PREFIX + 'edu_1');
    const original = test.provider.ports.stats;
    test.provider.ports.stats = async () => {
      const stats = await original();
      stats.namespaces[PREFIX + 'edu'].recordCount = 2;
      return stats;
    };
    test.provider.writes.length = 0;
    const seeded = await test.handler(
      request({
        action: 'seed',
        source: 'edu',
        fixture: fixture(),
        query_text: `synthetic retrieval ${RUN}`,
      }),
    );
    assert(
      seeded.status === 409 && (await seeded.json()).error === 'namespace_visibility_not_converged',
    );
    assert(test.provider.writes.length === 0);
  },
);
Deno.test(
  'zero lagged stats alone never prove cleanup when owned records remain visible',
  async () => {
    const test = setup();
    await test.handler(
      request({
        action: 'seed',
        source: 'edu',
        fixture: fixture(),
        query_text: `synthetic retrieval ${RUN}`,
      }),
    );
    const original = test.provider.ports.stats;
    test.provider.ports.stats = async () => {
      const stats = await original();
      stats.namespaces[PREFIX + 'edu'].recordCount = 0;
      return stats;
    };
    test.provider.ports.deleteVectors = async () => {};
    const cleaned = await test.handler(request({ action: 'cleanup', source: 'edu' }));
    const observed = await cleaned.json();
    assert(cleaned.status === 200 && !observed.cleanup_verified && !observed.cleaned);
    assert(
      observed.sources.edu.namespace_count === 0 &&
        observed.sources.edu.vectors_visible === 2 &&
        observed.sources.edu.stats_lag,
    );
  },
);

Deno.test('a phase does not read a previously deleted unrelated source', async () => {
  const test = setup();
  const original = test.provider.ports.getIndex;
  test.provider.ports.getIndex = async (index) => {
    if (index.endsWith('esg')) throw { meta: { statusCode: 404 } };
    return await original(index);
  };
  const body = {
    action: 'seed',
    source: 'standard',
    fixture: fixture(),
    query_text: `synthetic retrieval ${RUN}`,
  };
  const response = await test.handler(request(body));
  const result = await response.json();
  assert(
    response.status === 200 && result.ready,
    'unrelated stale resource blocked the selected phase',
  );
  assert(Object.keys(result.sources).join(',') === 'standard');
  const cleanup = await test.handler(request({ action: 'cleanup', source: 'standard' }));
  assert((await cleanup.json()).cleanup_verified);
});
