/** Temporary hosted qualification probe; never deploy to the production project.
 *
 * Deployment must retain Supabase's JWT gateway verification. A second run token
 * gates every action. Targets come exclusively from reviewed deployment env:
 * four exact run namespaces, four exact run search indexes and one log index.
 * Inspect exposes presence booleans, not values. No database rows are read here.
 *
 * Ownership is established by atomic index creation with mappings._meta and by
 * the two exact vector IDs carrying matching run/project/payload markers. Existing
 * unknown namespaces/indexes are refused before any writes. The retryable seed
 * path only fills absent/partial matching fixtures; it never claims old objects.
 * Cleanup deletes exact vector IDs (never deleteAll), exact owned indexes and
 * only supplied synthetic email keys. Verification is one readback, not a retry
 * loop; callers own bounded visibility polling and any explicit follow-up call.
 *
 * Primary vector _0 exists only in Pinecone; lexical _2 only in OpenSearch.
 * OpenSearch _1/_3 supply extK=1 neighbors. Each source has two vectors and three
 * lexical documents, grouped by the actual DB fixture UUID or journal DOI.
 * Optional source selects one of the four fixed bindings, permitting phased
 * qualification with one search index plus the shared log index. Partial cleanup
 * keeps that shared log and all Redis keys. Final unscoped cleanup removes both.
 * Cleanup trusts the atomic owned mapping, not a search request that may be
 * unavailable under AOSS overload; every deleted index still needs 404 readback.
 */
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { Client } from '@opensearch-project/opensearch';
import { AwsSigv4Signer } from '@opensearch-project/opensearch/aws';
import { Pinecone } from '@pinecone-database/pinecone';
import OpenAI from '@openai/openai';
import { Redis } from '@upstash/redis';

const PRODUCTION_REF = 'qyyqlnwqwgvzxnccnbgm';
const SOURCES = ['edu', 'esg', 'standard', 'sci'] as const;
type Source = (typeof SOURCES)[number];
type Env = (name: string) => string | undefined;
type Owner = { owner_run_id: string; owner_project_ref: string; owner_payload_hash: string };
type Fixture = {
  marker: string;
  resource_ids: { education: string; esg: string; standards: string };
  edu_course: string;
  edu_name: string;
  journal_doi: string;
};
type Config = {
  run: string;
  ref: string;
  token: string;
  prefix: string;
  namespaces: Record<Source, string>;
  indexes: Record<Source, string>;
  logIndex: string;
};
/** Narrow provider port also permits offline safety tests without network calls. */
export type Ports = {
  stats(): Promise<any>;
  list(namespace: string): Promise<any>;
  fetch(namespace: string, ids: string[]): Promise<any>;
  upsert(namespace: string, records: any[]): Promise<void>;
  deleteVectors(namespace: string, ids: string[]): Promise<void>;
  getIndex(index: string): Promise<any | null>;
  getDocuments(index: string): Promise<any[]>;
  createIndex(index: string, owner: Owner, log: boolean): Promise<void>;
  writeDocuments(index: string, documents: any[]): Promise<void>;
  deleteIndex(index: string): Promise<void>;
  embed(text: string): Promise<number[]>;
  logs(index: string, emails: string[]): Promise<any[]>;
  redis(email: string, remove: boolean): Promise<{ exists: number; ttl: number }>;
};
class Refusal extends Error {
  constructor(
    readonly code: string,
    readonly status = 409,
  ) {
    super(code);
  }
}
export function validateBulkResponse(body: any): void {
  if (body?.errors === false) return;
  const failed = Array.isArray(body?.items)
    ? body.items
        .map((item: any) => item?.index)
        .find(
          (item: any) =>
            item && (item.error || (typeof item.status === 'number' && item.status >= 300)),
        )
    : undefined;
  if (failed) {
    const status =
      Number.isInteger(failed.status) && failed.status >= 100 && failed.status <= 599
        ? String(failed.status)
        : 'unknown';
    const type =
      typeof failed.error?.type === 'string' && /^[a-z_]{1,80}$/.test(failed.error.type)
        ? failed.error.type
        : 'unknown';
    throw new Refusal(`opensearch_bulk_item_failed_${status}_${type}`, 502);
  }
  throw new Refusal('bulk_write_failed', 502);
}
function require(condition: unknown, code: string, status = 409): asserts condition {
  if (!condition) throw new Refusal(code, status);
}
function equalToken(actual: string, expected: string): boolean {
  if (actual.length !== expected.length) return false;
  let mismatch = 0;
  for (let i = 0; i < expected.length; i++)
    mismatch |= actual.charCodeAt(i) ^ expected.charCodeAt(i);
  return mismatch === 0;
}
const OPERATIONS: Record<keyof Ports, string> = {
  stats: 'pinecone_stats',
  list: 'pinecone_list',
  fetch: 'pinecone_fetch',
  upsert: 'pinecone_upsert',
  deleteVectors: 'pinecone_delete_ids',
  getIndex: 'opensearch_get_mapping',
  getDocuments: 'opensearch_search',
  createIndex: 'opensearch_create_index',
  writeDocuments: 'opensearch_bulk',
  deleteIndex: 'opensearch_delete_index',
  embed: 'openai_embedding',
  logs: 'opensearch_log_read',
  redis: 'redis_email_operation',
};
function boundedPorts(raw: Ports): Ports {
  let remaining = 80;
  const deadline = performance.now() + 120000;
  return Object.fromEntries(
    Object.entries(OPERATIONS).map(([name, operation]) => [
      name,
      async (...args: any[]) => {
        require(remaining-- > 0 && performance.now() < deadline, 'operation_budget_exhausted', 502);
        try {
          return await (raw[name as keyof Ports] as any)(...args);
        } catch (error: any) {
          if (error instanceof Refusal) throw error;
          const value = error?.meta?.statusCode ?? error?.status ?? error?.statusCode;
          const status =
            Number.isInteger(value) && value >= 100 && value <= 599 ? String(value) : 'transport';
          throw new Refusal(`${operation}_failed_${status}`, 502);
        }
      },
    ]),
  ) as Ports;
}
function config(env: Env): Config {
  const run = env('LIVE_EDGE_RUN_ID') ?? '';
  const ref = env('LIVE_EDGE_PROJECT_REF') ?? '';
  const token = env('LIVE_EDGE_RUN_TOKEN') ?? '';
  require(/^[a-f0-9]{16}$/.test(run), 'invalid_run_configuration', 503);
  require(/^[a-z]{20}$/.test(ref) && ref !== PRODUCTION_REF, 'production_or_invalid_project', 403);
  require(/^[A-Za-z0-9_-]{32,128}$/.test(token), 'invalid_run_token_configuration', 503);
  require(env('SUPABASE_URL') === `https://${ref}.supabase.co`, 'runtime_project_mismatch', 403);
  const remote = env('REMOTE_SUPABASE_URL');
  require(!remote || remote === `https://${ref}.supabase.co`, 'remote_project_mismatch', 403);
  const prefix = `live72-${run}-`;
  const namespaces = {} as Record<Source, string>;
  const indexes = {} as Record<Source, string>;
  for (const source of SOURCES) {
    const suffix = source.toUpperCase();
    namespaces[source] = env(`PINECONE_NAMESPACE_${suffix}`) ?? '';
    indexes[source] = env(`OPENSEARCH_${suffix}_INDEX_NAME`) ?? '';
    // Missing provider bindings are acceptable for presence-only inspect. Every
    // configured binding must still be exactly owned, even for inspect.
    require(!namespaces[source] ||
      namespaces[source] === prefix + source, 'unowned_namespace_configuration', 403);
    require(!indexes[source] ||
      indexes[source] === prefix + source, 'unowned_index_configuration', 403);
  }
  const logIndex = env('OPENSEARCH_LOG_INDEX_NAME') ?? '';
  require(!logIndex || logIndex === prefix + 'log', 'unowned_log_configuration', 403);
  return { run, ref, token, prefix, namespaces, indexes, logIndex };
}
const PRESENCE = [
  'SUPABASE_URL',
  'REMOTE_SUPABASE_URL',
  'OPENAI_API_KEY',
  'OPENAI_CHAT_MODEL',
  'OPENAI_EMBEDDING_MODEL',
  'PINECONE_API_KEY_US_EAST_1',
  'PINECONE_INDEX_NAME',
  'OPENSEARCH_REGION',
  'OPENSEARCH_DOMAIN',
  'AWS_ACCESS_KEY_ID',
  'AWS_SECRET_ACCESS_KEY',
  'AWS_SESSION_TOKEN',
  'UPSTASH_REDIS_URL',
  'UPSTASH_REDIS_TOKEN',
  'PINECONE_NAMESPACE_EDU',
  'PINECONE_NAMESPACE_ESG',
  'PINECONE_NAMESPACE_STANDARD',
  'PINECONE_NAMESPACE_SCI',
  'OPENSEARCH_EDU_INDEX_NAME',
  'OPENSEARCH_ESG_INDEX_NAME',
  'OPENSEARCH_STANDARD_INDEX_NAME',
  'OPENSEARCH_SCI_INDEX_NAME',
  'OPENSEARCH_LOG_INDEX_NAME',
];

function makePorts(env: Env): Ports {
  const pc = new Pinecone({
    apiKey: env('PINECONE_API_KEY_US_EAST_1')!,
    maxRetries: 0,
    fetchApi: (input: RequestInfo | URL, init?: RequestInit) =>
      fetch(input, { ...init, signal: AbortSignal.timeout(15000) }),
  });
  const vectors = pc.index(env('PINECONE_INDEX_NAME')!);
  const search = new Client({
    ...AwsSigv4Signer({
      region: env('OPENSEARCH_REGION')!,
      service: 'aoss',
      getCredentials: () => defaultProvider()(),
    }),
    node: env('OPENSEARCH_DOMAIN')!,
    maxRetries: 0,
    requestTimeout: 15000,
  });
  let openai: OpenAI | undefined;
  let redis: Redis | undefined;
  const status = (error: any) => error?.meta?.statusCode;
  return {
    stats: () => vectors.describeIndexStats(),
    list: (namespace) => vectors.namespace(namespace).listPaginated({ limit: 3 }),
    fetch: (namespace, ids) => vectors.namespace(namespace).fetch({ ids }),
    upsert: (namespace, records) => vectors.namespace(namespace).upsert({ records }),
    deleteVectors: (namespace, ids) => vectors.namespace(namespace).deleteMany({ ids }),
    getIndex: async (index) => {
      try {
        return (await search.indices.getMapping({ index })).body[index];
      } catch (error) {
        if (status(error) === 404) return null;
        throw error;
      }
    },
    getDocuments: async (index) => {
      const response = await search.search({
        index,
        body: { size: 4, track_total_hits: true, query: { match_all: {} } },
      });
      const hits = response.body.hits as any;
      require((typeof hits.total === 'number' ? hits.total : hits.total?.value) <=
        3, 'index_contains_unknown_documents');
      return hits.hits;
    },
    createIndex: async (index, owner, log) => {
      const properties: any = log
        ? {
            email: { type: 'keyword' },
            invoked_at: { type: 'long' },
            service_type: { type: 'keyword' },
            top_k: { type: 'integer' },
            ext_k: { type: 'integer' },
          }
        : {
            text: { type: 'text' },
            rec_id: { type: 'keyword' },
            doi: { type: 'keyword' },
            course: { type: 'keyword' },
            name: { type: 'keyword' },
            chapter_number: { type: 'integer' },
            title: { type: 'text' },
            company_name: { type: 'keyword' },
            page_number: { type: 'integer' },
            standard_number: { type: 'keyword' },
            organization: { type: 'keyword' },
            journal: { type: 'keyword' },
            date: { type: 'long' },
            effective_date: { type: 'long' },
            publication_date: { type: 'long' },
            owner_run_id: { type: 'keyword' },
            owner_project_ref: { type: 'keyword' },
            owner_payload_hash: { type: 'keyword' },
          };
      await search.indices.create({ index, body: { mappings: { _meta: owner, properties } } });
    },
    writeDocuments: async (index, documents) => {
      const body = documents.flatMap(({ id, metadata }) => [
        { index: { _index: index, _id: id } },
        metadata,
      ]);
      const result = await search.bulk({ body });
      validateBulkResponse(result.body);
    },
    deleteIndex: async (index) => {
      await search.indices.delete({ index });
    },
    embed: async (text) => {
      openai ??= new OpenAI({ apiKey: env('OPENAI_API_KEY')!, maxRetries: 0, timeout: 15000 });
      const result = await openai.embeddings.create({
        model: env('OPENAI_EMBEDDING_MODEL')!,
        input: text,
      });
      const embedding = result.data?.[0]?.embedding;
      require(Array.isArray(embedding) &&
        embedding.length > 0 &&
        embedding.every(Number.isFinite), 'invalid_embedding', 502);
      return embedding;
    },
    logs: async (index, emails) => {
      if (!emails.length) return [];
      const response = await search.search({
        index,
        body: {
          size: 64,
          track_total_hits: true,
          _source: ['email', 'service_type', 'invoked_at', 'top_k', 'ext_k'],
          query: { terms: { email: emails } },
        },
      });
      const hits = response.body.hits as any;
      require((typeof hits.total === 'number' ? hits.total : hits.total?.value) <=
        64, 'log_read_limit_exceeded');
      const records = hits.hits.map((hit: any) => hit._source);
      require(records.every(
        (record: any) =>
          emails.includes(record?.email) &&
          /^(edu|esg|standard|sci)_search(?::list_filter_options)?$/.test(
            record?.service_type ?? '',
          ) &&
          Number.isFinite(record?.invoked_at) &&
          Number.isInteger(record?.top_k) &&
          Number.isInteger(record?.ext_k),
      ), 'invalid_synthetic_log_record');
      return records;
    },
    redis: async (email, remove) => {
      require(env('UPSTASH_REDIS_URL') && env('UPSTASH_REDIS_TOKEN'), 'redis_not_configured', 503);
      redis ??= new Redis({
        url: env('UPSTASH_REDIS_URL')!,
        token: env('UPSTASH_REDIS_TOKEN')!,
        retry: { retries: 0 },
        signal: () => AbortSignal.timeout(15000),
      });
      if (remove) await redis.del(email);
      return { exists: await redis.exists(email), ttl: await redis.ttl(email) };
    },
  };
}
function ids(cfg: Config, source: Source, numbers: number[]): string[] {
  return numbers.map((number) => `${cfg.prefix}${source}_${number}`);
}
function owned(value: any, cfg: Config, hash?: string): boolean {
  return (
    value?.owner_run_id === cfg.run &&
    value?.owner_project_ref === cfg.ref &&
    /^[a-f0-9]{64}$/.test(value?.owner_payload_hash ?? '') &&
    (!hash || value.owner_payload_hash === hash)
  );
}
function emails(body: any, cfg: Config): string[] {
  const values = body.emails ?? [];
  require(Array.isArray(values) && values.length <= 8, 'invalid_email_scope', 400);
  for (const value of values)
    require(typeof value === 'string' &&
      value.length <= 160 &&
      /^[a-z0-9._+-]+@example\.invalid$/.test(value) &&
      value.includes(cfg.run), 'invalid_email_scope', 400);
  return [...new Set(values)] as string[];
}
function seedPayload(body: any, cfg: Config): { fixture: Fixture; query: string } {
  const f = body.fixture;
  require(f && typeof f === 'object' && !Array.isArray(f), 'invalid_fixture', 400);
  for (const key of ['marker', 'edu_course', 'edu_name', 'journal_doi']) {
    require(typeof f[key] === 'string' &&
      f[key].length > 0 &&
      f[key].length <= 512 &&
      !/[\r\n\0]/.test(f[key]), 'invalid_fixture_text', 400);
  }
  require(f.marker.includes(cfg.run) &&
    f.journal_doi.startsWith('10.99999/') &&
    f.journal_doi.includes(cfg.run), 'fixture_not_run_scoped', 400);
  const ids = f.resource_ids;
  require(ids &&
    ['education', 'esg', 'standards'].every(
      (name) =>
        typeof ids[name] === 'string' &&
        /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(ids[name]),
    ), 'invalid_fixture_ids', 400);
  const query = body.query_text;
  require(typeof query === 'string' &&
    query.length >= 16 &&
    query.length <= 1024 &&
    query.includes(cfg.run) &&
    !/[\r\n\0]/.test(query), 'invalid_query_text', 400);
  return {
    fixture: {
      marker: f.marker,
      edu_course: f.edu_course,
      edu_name: f.edu_name,
      journal_doi: f.journal_doi,
      resource_ids: { education: ids.education, esg: ids.esg, standards: ids.standards },
    },
    query,
  };
}
async function payloadHash(payload: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  const hash = await crypto.subtle.digest('SHA-256', bytes);
  return Array.from(new Uint8Array(hash), (byte) => byte.toString(16).padStart(2, '0')).join('');
}
async function settle<T>(tasks: Promise<T>[]): Promise<T[]> {
  // Drain every dispatched operation before releasing the mutation lease.
  // An early failure must not leave writes running after cleanup is admitted.
  const results = await Promise.allSettled(tasks);
  const failed = results.find((result) => result.status === 'rejected');
  if (failed?.status === 'rejected') throw failed.reason;
  return results.map((result) => (result as PromiseFulfilledResult<T>).value);
}
function documents(cfg: Config, source: Source, fixture: Fixture, query: string, owner: Owner) {
  const base: any = { ...owner, date: 1704067200 };
  if (source === 'edu')
    Object.assign(base, {
      rec_id: fixture.resource_ids.education,
      course: fixture.edu_course,
      name: fixture.edu_name,
      chapter_number: 1,
    });
  if (source === 'esg')
    Object.assign(base, {
      rec_id: fixture.resource_ids.esg,
      title: fixture.marker,
      company_name: 'Synthetic Company',
      page_number: 1,
      publication_date: 1704067200,
    });
  if (source === 'standard')
    Object.assign(base, {
      rec_id: fixture.resource_ids.standards,
      title: fixture.marker,
      standard_number: 'SYN-001',
      organization: 'Synthetic Organization',
      effective_date: 1704067200,
    });
  if (source === 'sci')
    Object.assign(base, { doi: fixture.journal_doi, journal: 'SYNTHETIC JOURNAL' });
  return {
    vector: [
      {
        id: ids(cfg, source, [0])[0],
        metadata: { ...base, text: `PINECONE_ONLY_${cfg.run} synthetic ${source} vector content` },
      },
      {
        id: ids(cfg, source, [1])[0],
        metadata: {
          ...base,
          text: `PINECONE_NEIGHBOR_${cfg.run} synthetic ${source} negative vector`,
        },
      },
    ],
    lexical: [
      {
        id: ids(cfg, source, [1])[0],
        metadata: { ...base, text: `EXTENSION_${cfg.run} vector neighbor` },
      },
      {
        id: ids(cfg, source, [2])[0],
        metadata: { ...base, text: `${query} OPENSEARCH_ONLY_${cfg.run}` },
      },
      {
        id: ids(cfg, source, [3])[0],
        metadata: { ...base, text: `EXTENSION_${cfg.run} lexical neighbor` },
      },
    ],
  };
}
async function state(
  cfg: Config,
  ports: Ports,
  hash?: string,
  forCleanup = false,
  selected?: Source,
) {
  const stats = await ports.stats();
  const sources = await Promise.all(
    (selected ? [selected] : SOURCES).map(async (source) => {
      const expected = ids(cfg, source, [0, 1]);
      const listing = await ports.list(cfg.namespaces[source]);
      const present = (listing.vectors ?? []).map((value: any) => value.id);
      require(!listing.pagination?.next &&
        present.length <= 2 &&
        present.every((id: string) => expected.includes(id)), 'namespace_contains_unknown_ids');
      const fetched = (await ports.fetch(cfg.namespaces[source], expected)).records ?? {};
      require(Object.keys(fetched).every(
        (id) => expected.includes(id) && owned(fetched[id].metadata, cfg, hash),
      ), 'namespace_records_not_owned');
      const count = stats.namespaces?.[cfg.namespaces[source]]?.recordCount ?? 0;
      require(Number.isInteger(count) &&
        count >= 0 &&
        count <= 2, 'namespace_contains_unknown_records');
      const fetchedIds = Object.keys(fetched);
      // Pinecone's aggregate stats may lag visible records. Complete ID listing
      // and fetched ownership are the mutation boundary; a greater reported
      // count still signals unobserved records and must fail closed. Never treat
      // listed-but-unfetched IDs as owned just because stats are delayed.
      require(new Set(present).size === present.length &&
        present.length === fetchedIds.length &&
        count <= fetchedIds.length &&
        present.every((id: string) =>
          fetchedIds.includes(id),
        ), 'namespace_visibility_not_converged');
      const index = await ports.getIndex(cfg.indexes[source]);
      let lexical: any[] = [];
      if (index) {
        require(owned(index.mappings?._meta, cfg, hash), 'index_not_owned');
        if (!forCleanup) {
          lexical = await ports.getDocuments(cfg.indexes[source]);
          const expectedDocs = ids(cfg, source, [1, 2, 3]);
          require(lexical.length <= 3 &&
            lexical.every(
              (doc) => expectedDocs.includes(doc._id) && owned(doc._source, cfg, hash),
            ), 'index_documents_not_owned');
        }
      }
      return {
        source,
        count,
        present: Object.keys(fetched),
        indexExists: !!index,
        lexicalCount: index && forCleanup ? null : lexical.length,
      };
    }),
  );
  const log = await ports.getIndex(cfg.logIndex);
  if (log) require(owned(log.mappings?._meta, cfg, hash), 'log_index_not_owned');
  return { sources, logExists: !!log, dimension: stats.dimension };
}
function receipt(cfg: Config, observed: Awaited<ReturnType<typeof state>>, selected?: Source) {
  const evaluated = observed.sources.filter((source) => !selected || source.source === selected);
  const sources = Object.fromEntries(
    observed.sources.map((source) => [
      source.source,
      {
        namespace_count: source.count,
        vectors_visible: source.present.length,
        stats_lag: source.count < source.present.length,
        lexical_documents: source.lexicalCount,
        index_exists: source.indexExists,
      },
    ]),
  );
  return {
    run_id: cfg.run,
    source: selected ?? 'all',
    sources,
    log_index_exists: observed.logExists,
    ready:
      observed.logExists &&
      evaluated.every((source) => source.present.length === 2 && source.lexicalCount === 3),
    cleaned:
      (!!selected || !observed.logExists) &&
      evaluated.every(
        (source) => source.count === 0 && source.present.length === 0 && !source.indexExists,
      ),
  };
}

export function createProbeHandler(
  env: Env = Deno.env.get,
  createPorts: (env: Env) => Ports = makePorts,
) {
  let mutationRunning = false;
  return async (request: Request): Promise<Response> => {
    let locked = false;
    try {
      const cfg = config(env);
      require(/^Bearer [^\s]+$/.test(
        request.headers.get('authorization') ?? '',
      ), 'gateway_bearer_required', 401);
      require(equalToken(
        request.headers.get('x-live-edge-run-token') ?? '',
        cfg.token,
      ), 'run_token_required', 401);
      require(request.method === 'POST', 'post_required', 405);
      const headerSize = request.headers.get('content-length');
      require(!headerSize ||
        (/^\d+$/.test(headerSize) && Number(headerSize) <= 32768), 'payload_too_large', 413);
      const reader = request.body?.getReader();
      require(reader, 'json_body_required', 400);
      const chunks: Uint8Array[] = [];
      let length = 0;
      while (true) {
        const part = await reader.read();
        if (part.done) break;
        length += part.value.length;
        if (length > 32768) {
          await reader.cancel();
          throw new Refusal('payload_too_large', 413);
        }
        chunks.push(part.value);
      }
      const raw = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) {
        raw.set(chunk, offset);
        offset += chunk.length;
      }
      let body: any;
      try {
        body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(raw));
      } catch (_) {
        throw new Refusal('invalid_json', 400);
      }
      require(body &&
        typeof body === 'object' &&
        !Array.isArray(body), 'json_object_required', 400);
      require(['inspect', 'seed', 'stats', 'cleanup'].includes(
        body.action,
      ), 'unsupported_action', 400);
      require(body.source === undefined ||
        SOURCES.includes(body.source), 'unsupported_source', 400);
      const selected = body.source as Source | undefined;
      if (body.action === 'inspect')
        return Response.json({
          presence: Object.fromEntries(PRESENCE.map((name) => [name, !!env(name)])),
        });
      require(SOURCES.every((source) => cfg.namespaces[source] && cfg.indexes[source]) &&
        cfg.logIndex, 'provider_bindings_incomplete', 503);
      require([
        'PINECONE_API_KEY_US_EAST_1',
        'PINECONE_INDEX_NAME',
        'OPENSEARCH_REGION',
        'OPENSEARCH_DOMAIN',
      ].every((name) => !!env(name)), 'provider_credentials_incomplete', 503);
      const scopedEmails = emails(body, cfg);
      require(!(
        body.action === 'cleanup' &&
        selected &&
        body.emails !== undefined
      ), 'partial_cleanup_cannot_delete_redis', 400);
      const payload = body.action === 'seed' ? seedPayload(body, cfg) : undefined;
      if (payload)
        require(env('OPENAI_API_KEY') &&
          env('OPENAI_EMBEDDING_MODEL'), 'embedding_configuration_incomplete', 503);
      if (body.action !== 'stats') {
        require(!mutationRunning, 'mutation_already_running');
        mutationRunning = true;
        locked = true;
      }
      let ports: Ports;
      try {
        ports = boundedPorts(createPorts(env));
      } catch (_) {
        throw new Refusal('provider_clients_initialization_failed', 502);
      }
      const hash = payload ? await payloadHash(payload) : undefined;
      const cleaning = body.action === 'cleanup';
      let observed = await state(cfg, ports, hash, cleaning, selected);
      if (body.action === 'seed' && payload && !receipt(cfg, observed, selected).ready) {
        const vector = await ports.embed(payload.query);
        require(vector.length === observed.dimension, 'embedding_dimension_mismatch', 502);
        const owner = {
          owner_run_id: cfg.run,
          owner_project_ref: cfg.ref,
          owner_payload_hash: hash!,
        };
        // All selected namespaces/index mappings were checked before writes; a
        // selected phase mutates only its one namespace and one search index.
        if (!observed.logExists) await ports.createIndex(cfg.logIndex, owner, true);
        await settle(
          observed.sources
            .filter((source) => !selected || source.source === selected)
            .map(async (source) => {
              if (!source.indexExists)
                await ports.createIndex(cfg.indexes[source.source], owner, false);
              const content = documents(cfg, source.source, payload.fixture, payload.query, owner);
              await ports.upsert(
                cfg.namespaces[source.source],
                content.vector.map((doc, i) => ({
                  ...doc,
                  values: i === 0 ? vector : vector.map((value) => -value),
                })),
              );
              await ports.writeDocuments(cfg.indexes[source.source], content.lexical);
            }),
        );
        observed = await state(cfg, ports, hash, false, selected);
      }
      if (body.action === 'cleanup') {
        await settle(
          observed.sources
            .filter((source) => !selected || source.source === selected)
            .map(async (source) => {
              if (source.present.length)
                await ports.deleteVectors(cfg.namespaces[source.source], source.present);
              if (source.indexExists) await ports.deleteIndex(cfg.indexes[source.source]);
            }),
        );
        if (!selected && observed.logExists) await ports.deleteIndex(cfg.logIndex);
        observed = await state(cfg, ports, undefined, true, selected);
      }
      const redis = await settle(
        scopedEmails.map(async (email) => ({
          email,
          ...(await ports.redis(email, body.action === 'cleanup')),
        })),
      );
      const logs =
        body.action === 'stats' && observed.logExists
          ? await ports.logs(cfg.logIndex, scopedEmails)
          : [];
      const result = receipt(cfg, observed, selected);
      return Response.json({
        ...result,
        action: body.action,
        redis,
        log_count: logs.length,
        log_records: logs,
        shared_log_preserved: body.action === 'cleanup' && !!selected && observed.logExists,
        cleanup_verified:
          body.action === 'cleanup'
            ? result.cleaned && redis.every((item) => item.exists === 0 && item.ttl === -2)
            : undefined,
      });
    } catch (error) {
      // Never forward provider messages, raw response bodies, stack traces or
      // credentials. Ambiguous provider failures require readback, not retry.
      return Response.json(
        { error: error instanceof Refusal ? error.code : 'provider_operation_failed' },
        { status: error instanceof Refusal ? error.status : 502 },
      );
    } finally {
      if (locked) mutationRunning = false;
    }
  };
}
if (import.meta.main) Deno.serve(createProbeHandler());
