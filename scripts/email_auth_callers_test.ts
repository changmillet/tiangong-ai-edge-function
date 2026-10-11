/**
 * Offline production-handler contracts. No listener or provider calls are started.
 * deno test --no-config --import-map scripts/fixtures/email_auth/import_map.json \
 *   --allow-env --allow-read=supabase/functions,scripts/fixtures/email_auth \
 *   --deny-net scripts/email_auth_callers_test.ts
 */
import {
  deniedEmails,
  observations,
  remoteEmails,
  resetObservations,
} from './fixtures/email_auth/providers.ts';

type Handler = (request: Request) => Response | Promise<Response>;
const functions = [
  'sci_search',
  'esg_search',
  'standard_search',
  'edu_search',
  'report_search',
  'patent_search',
  'textbook_search',
  'green_deal_search',
  'internal_search',
  'bigquery_search',
] as const;
type FunctionName = (typeof functions)[number];
const handlers = new Map<FunctionName, Handler>();
const disabledHandlers = new Map<FunctionName, Handler>();

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function equal(actual: unknown, expected: unknown, message: string) {
  assert(
    JSON.stringify(actual) === JSON.stringify(expected),
    `${message}: expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`,
  );
}

// Capture the actual entrypoint while importing. Restore both runtime and env afterward.
const originalServe = Deno.serve;
const originalEnabled = Deno.env.get('EMAIL_AUTH_L1_ENABLED');
const originalBigqueryKey = Deno.env.get('BIGQUERY_KEY');
let capturedHandler: Handler | undefined;
try {
  Deno.env.delete('EMAIL_AUTH_L1_ENABLED');
  Deno.env.set('BIGQUERY_KEY', btoa('{}'));
  Deno.serve = ((handler: Handler) => {
    capturedHandler = handler;
    return {};
  }) as unknown as typeof Deno.serve;
  for (const name of functions) {
    capturedHandler = undefined;
    await import(`../supabase/functions/${name}/index.ts`);
    assert(capturedHandler, `${name} did not register its Deno.serve handler`);
    handlers.set(name, capturedHandler);
  }
  Deno.env.set('EMAIL_AUTH_L1_ENABLED', 'false');
  for (const name of functions) {
    capturedHandler = undefined;
    await import(`../supabase/functions/${name}/index.ts?email-auth-disabled`);
    assert(capturedHandler, `${name} did not register its disabled-cache handler`);
    disabledHandlers.set(name, capturedHandler);
  }
} finally {
  Deno.serve = originalServe;
  if (originalEnabled === undefined) Deno.env.delete('EMAIL_AUTH_L1_ENABLED');
  else Deno.env.set('EMAIL_AUTH_L1_ENABLED', originalEnabled);
  if (originalBigqueryKey === undefined) Deno.env.delete('BIGQUERY_KEY');
  else Deno.env.set('BIGQUERY_KEY', originalBigqueryKey);
}

function invoke(
  name: FunctionName,
  email: string,
  password: string,
  body: Record<string, unknown> = {},
  extraHeaders: Record<string, string> = {},
  cacheEnabled = true,
) {
  const handler = (cacheEnabled ? handlers : disabledHandlers).get(name);
  assert(handler, `Missing ${name} handler`);
  return handler(
    new Request(`https://offline.invalid/${name}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': btoa(JSON.stringify({ email, password })),
        ...extraHeaders,
      },
      body: JSON.stringify({
        query: 'offline auth probe',
        getMeta: false,
        ...body,
      }),
    }),
  );
}

function checkLog(name: FunctionName, email: string, topK = 5, extK = 0) {
  if (name === 'bigquery_search') {
    equal(observations.logDocuments, [], 'BigQuery keeps its existing disabled log');
    assert(observations.bigqueryJobs > 0, 'BigQuery search ran after authorization');
    return;
  }
  equal(observations.logIndexes, ['function_log'], 'Real logInsert indexes the document');
  equal(observations.logDocuments.length, 1, 'Exactly one search log is written');
  const document = observations.logDocuments[0];
  equal(document.email, email, 'Logging retains the original decoded email');
  equal(document.service_type, name, 'Logging retains the service name');
  equal(document.top_k, topK, 'Logging retains the parsed topK');
  equal(document.ext_k, name === 'patent_search' ? 0 : extK, 'Logging retains extK semantics');
  assert(typeof document.invoked_at === 'number', 'Logging retains its timestamp');
  equal(
    observations.queries,
    ['offline auth probe'],
    'The decoded request query reaches rewriting',
  );
}

async function authorized(name: FunctionName, result: Response | Promise<Response>) {
  try {
    const response = await result;
    equal(response.status, 200, 'Authorized request reaches search');
  } catch (error) {
    // The existing patent endpoint throws for zero results. Keep that downstream
    // behavior visible; authorization/logging assertions below still run.
    if (
      name !== 'patent_search' ||
      !(error instanceof Error) ||
      error.message !== 'Record not found'
    ) {
      throw error;
    }
  }
}

for (const name of functions) {
  Deno.test(
    `${name}: rollback flag disables memory hits without changing original-email logs`,
    async () => {
      const email = `Original.Mixed+disabled.${name}@Example.COM`;
      remoteEmails.add(email);
      for (const password of ['original-password', 'different-password']) {
        resetObservations();
        await authorized(name, invoke(name, email, password, {}, {}, false));
        equal(observations.redisExists, [email], 'Disabled cache consults Redis on every request');
        equal(
          observations.logins,
          [],
          'Redis hit preserves legacy password semantics when disabled',
        );
        equal(observations.redisWrites, [], 'Disabled cache does not renew Redis on a hit');
        checkLog(name, email);
      }
    },
  );

  Deno.test(
    `${name}: Redis and memory cache hits keep the decoded email and legacy password semantics`,
    async () => {
      const email = `Original.Mixed+redis.${name}@Example.COM`;
      remoteEmails.add(email);
      resetObservations();
      await authorized(
        name,
        invoke(name, email, 'original-password', {
          topK: 3,
          extK: 2,
        }),
      );
      equal(observations.redisExists, [email], 'Redis receives the original email key');
      equal(observations.logins, [], 'Redis hit skips entry Supabase login');
      equal(observations.redisWrites, [], 'Redis hit does not renew the Redis marker');
      checkLog(name, email, 3, 2);

      // Remove the remote marker: the warmed isolate must authorize the next call locally.
      remoteEmails.delete(email);
      resetObservations();
      await authorized(
        name,
        invoke(name, email, 'different-password', {
          topK: 3,
          extK: 2,
        }),
      );
      equal(observations.redisExists, [], 'Memory hit short-circuits Redis');
      equal(observations.logins, [], 'Memory hit skips entry Supabase login');
      equal(observations.redisWrites, [], 'Memory hit does not write Redis');
      checkLog(name, email, 3, 2);
    },
  );

  Deno.test(
    `${name}: fresh login keeps decoded credentials and writes one NX EX 3600 marker`,
    async () => {
      const email = `Original.Mixed+fresh.${name}@Example.COM`;
      resetObservations();
      await authorized(name, invoke(name, email, 'fresh-password'));
      equal(observations.redisExists, [email], 'Fresh login probes the original Redis key');
      equal(
        observations.logins,
        [{ email, password: 'fresh-password' }],
        'Real supabaseAuth receives decoded credentials',
      );
      equal(
        observations.redisWrites,
        [{ email, value: '', options: { nx: true, ex: 3600 } }],
        'Successful login writes the existing marker atomically without renewal',
      );
      checkLog(name, email);

      resetObservations();
      await authorized(name, invoke(name, email, 'another-password'));
      equal(observations.redisExists, [], 'Fresh-login repeat does not read Redis');
      equal(observations.redisWrites, [], 'Fresh-login repeat does not renew Redis');
      equal(observations.logins, [], 'Fresh-login repeat does not log in again');
      checkLog(name, email);
    },
  );

  Deno.test(`${name}: failed authentication does not log or cache a success`, async () => {
    const email = `Original.Mixed+failure.${name}@Example.COM`;
    deniedEmails.add(email);
    try {
      resetObservations();
      const response = await invoke(name, email, 'bad-password');
      equal(response.status, 401, 'Failed fresh login rejects search');
      equal(
        observations.logins,
        [{ email, password: 'bad-password' }],
        'Failed login uses decoded credentials',
      );
      equal(observations.logDocuments, [], 'Rejected authentication never reaches logInsert');
      equal(observations.queries, [], 'Rejected authentication never reaches query generation');
      equal(observations.redisWrites, [], 'Rejected authentication never writes a success marker');
      equal(observations.bigqueryJobs, 0, 'Rejected authentication never reaches BigQuery');
    } finally {
      deniedEmails.delete(email);
    }
    resetObservations();
    await authorized(name, invoke(name, email, 'valid-password'));
    equal(observations.redisExists, [email], 'Failure was not cached as success');
    equal(
      observations.logins,
      [{ email, password: 'valid-password' }],
      'Recovery performs a new login',
    );
    checkLog(name, email);
  });
}

for (const name of ['sci_search', 'esg_search', 'standard_search'] as const) {
  const metadataBody = name === 'sci_search' ? { getMeta: true } : { meta_contains: 'probe' };
  Deno.test(
    `${name}: metadata session login remains necessary on Redis and memory hits`,
    async () => {
      const email = `Original.Mixed+metadata.${name}@Example.COM`;
      remoteEmails.add(email);
      for (const password of ['metadata-password', 'changed-password']) {
        resetObservations();
        const response = await invoke(name, email, password, metadataBody);
        equal(response.status, 200, 'Metadata request is served');
        equal(
          observations.logins,
          [{ email, password }],
          'first_login=false still obtains the metadata session',
        );
        equal(
          observations.redisWrites,
          [],
          'Metadata session login does not renew the entry marker',
        );
        checkLog(name, email);
        if (password === 'metadata-password') {
          equal(observations.redisExists, [email], 'First metadata request uses Redis');
        } else {
          equal(observations.redisExists, [], 'Second metadata request uses memory');
        }
        if (name !== 'sci_search') {
          equal(
            observations.metadata,
            [name === 'esg_search' ? 'esg_full_text' : 'standards_full_text'],
            'Metadata RPC still executes',
          );
        }
      }

      const freshEmail = `Original.Mixed+metadatafresh.${name}@Example.COM`;
      resetObservations();
      const freshResponse = await invoke(name, freshEmail, 'fresh-password', metadataBody);
      equal(freshResponse.status, 200, 'Fresh metadata request is served');
      equal(
        observations.logins,
        [
          {
            email: freshEmail,
            password: 'fresh-password',
          },
        ],
        'first_login=true avoids a redundant metadata login',
      );
      checkLog(name, freshEmail);
    },
  );
}

Deno.test(
  'edu_search: filter options still require a user session and preserve original-email logging',
  async () => {
    const email = 'Original.Mixed+eduoptions@Example.COM';
    remoteEmails.add(email);
    resetObservations();
    const response = await invoke('edu_search', email, 'metadata-password', {
      action: 'list_filter_options',
      fields: ['course'],
    });
    equal(response.status, 200, 'Filter options request is served');
    equal(
      observations.logins,
      [{ email, password: 'metadata-password' }],
      'Filter options use a real session even for a cached email',
    );
    equal(observations.redisExists, [], 'Filter options retain their separate user-session path');
    equal(
      observations.redisWrites,
      [{ email, value: '', options: { nx: true, ex: 3600 } }],
      'Filter options successful session login writes a non-renewing one-hour marker',
    );
    equal(observations.metadata, ['edu_meta'], 'Filter options query edu_meta');
    equal(observations.logIndexes, ['function_log'], 'Filter options use real logInsert');
    equal(
      observations.logDocuments[0]?.email,
      email,
      'Filter options retain original decoded email',
    );
    equal(
      observations.logDocuments[0]?.service_type,
      'edu_search:list_filter_options',
      'Filter options retain their service name',
    );
  },
);

Deno.test('explicit credential headers keep precedence over decoded API-key fields', async () => {
  const email = 'Header.Mixed+precedence@Example.COM';
  resetObservations();
  const response = await invoke(
    'report_search',
    'Encoded.Other@Example.COM',
    'decoded-password',
    {},
    {
      email,
      password: 'header-password',
    },
  );
  equal(response.status, 200, 'Header credentials authorize search');
  equal(
    observations.logins,
    [{ email, password: 'header-password' }],
    'Explicit headers retain precedence',
  );
  checkLog('report_search', email);
});

Deno.test(
  'edu_search: cached email cannot skip failed filter-options session authentication',
  async () => {
    const email = 'Original.Mixed+eduoptionsfailed@Example.COM';
    remoteEmails.add(email);
    deniedEmails.add(email);
    try {
      resetObservations();
      const response = await invoke('edu_search', email, 'bad-password', {
        action: 'list_filter_options',
        fields: ['course'],
      });
      equal(response.status, 401, 'Filter options reject failed session authentication');
      equal(observations.logins, [{ email, password: 'bad-password' }], 'Session login still runs');
      equal(observations.redisWrites, [], 'Failed session authentication does not write Redis');
      equal(observations.logDocuments, [], 'Failed session authentication does not log a search');
      equal(observations.metadata, [], 'Failed session authentication does not query metadata');
    } finally {
      deniedEmails.delete(email);
    }
  },
);
