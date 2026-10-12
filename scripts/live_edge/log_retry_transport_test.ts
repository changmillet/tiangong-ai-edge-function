/** Offline loopback HTTP tests use the actual project SDK and actual SigV4 signer.
 * The server is a synthetic document store, not live AOSS qualification.
 */
import { AwsSigv4Signer } from '@opensearch-project/opensearch/aws';
import { beginCapture, Client, finishCapture, retryConnection } from './log_retry_transport.ts';
import { createLogRetryCanaryHandler } from './log_retry_canary.ts';

const RUN = '0123456789abcdef';
const INDEX = `live72-${RUN}-log`;
const TOKEN = 'a'.repeat(64);
function assert(value: unknown, message = 'assertion failed'): asserts value {
  if (!value) throw new Error(message);
}
function document() {
  return {
    email: `live74-${RUN}@example.invalid`,
    invoked_at: 1700000000000,
    service_type: 'sci_search',
    top_k: 1,
    ext_k: 1,
  };
}
function signer() {
  return AwsSigv4Signer({
    region: 'us-east-1',
    service: 'aoss',
    getCredentials: async () => ({
      accessKeyId: 'synthetic-access',
      secretAccessKey: 'synthetic-secret',
    }),
  });
}
async function backend() {
  const docs = new Map<string, unknown>();
  const writes: Array<{ id: string; signed: boolean; status: number }> = [];
  const server = Deno.serve(
    { hostname: '127.0.0.1', port: 0, onListen: () => {} },
    async (request) => {
      const path = new URL(request.url).pathname;
      const id = path.split('/').at(-1)!;
      assert(path.startsWith('/' + INDEX + '/_doc/') && ['PUT', 'POST'].includes(request.method));
      const body = await request.json();
      const status = docs.has(id) ? 200 : 201;
      docs.set(id, body);
      writes.push({
        id,
        signed: request.headers.get('authorization')?.startsWith('AWS4-HMAC-SHA256') === true,
        status,
      });
      return Response.json(
        { _index: INDEX, _id: id, result: status === 201 ? 'created' : 'updated' },
        { status },
      );
    },
  );
  return { origin: `http://127.0.0.1:${server.addr.port}`, docs, writes, server };
}
Deno.test(
  'real signed SDK repeats committed write after first ACK becomes503 with one stable UUID',
  async () => {
    const store = await backend();
    const doc = document();
    const capture = beginCapture(store.origin, INDEX, doc, 1);
    const id = '11111111-1111-4111-8111-111111111111';
    try {
      const client = new Client({ ...signer(), node: store.origin });
      await client.index({ index: INDEX, id, body: doc });
      const receipt = await finishCapture(capture);
      assert(
        receipt.verified && receipt.backend_attempt_count === 2 && receipt.invocation_count === 1,
      );
      assert(
        store.docs.size === 1 &&
          store.writes.length === 2 &&
          store.writes.every((write) => write.id === id && write.signed),
      );
      assert(
        receipt.attempts[0].backend_status === 201 && receipt.attempts[0].delivered_status === 503,
      );
      assert(
        receipt.attempts[1].backend_status === 200 && receipt.attempts[1].delivered_status === 200,
      );
      assert(
        receipt.attempts.every(
          (attempt) => attempt.request_id === id && attempt.response_id === id,
        ),
      );
      assert(
        !JSON.stringify(receipt).includes('synthetic-secret') &&
          !JSON.stringify(receipt).includes(doc.email),
      );
    } finally {
      await finishCapture(capture);
      await store.server.shutdown();
    }
  },
);
Deno.test(
  'two independent identical documents use distinct UUIDs plus one retry and leave exactly2documents',
  async () => {
    const store = await backend();
    const doc = document();
    const capture = beginCapture(store.origin, INDEX, doc, 2);
    const ids = ['11111111-1111-4111-8111-111111111111', '22222222-2222-4222-8222-222222222222'];
    try {
      const clients = ids.map(() => new Client({ ...signer(), node: store.origin }));
      await Promise.all(
        ids.map((id, index) => clients[index].index({ index: INDEX, id, body: doc })),
      );
      const receipt = await finishCapture(capture);
      assert(
        receipt.verified && receipt.backend_attempt_count === 3 && receipt.invocation_count === 2,
      );
      assert(
        store.docs.size === 2 &&
          store.writes.length === 3 &&
          store.writes.every((write) => write.signed),
      );
      assert(
        [...store.docs.values()].every((value) => JSON.stringify(value) === JSON.stringify(doc)),
      );
      assert(new Set(receipt.results.map((result) => result.request_id)).size === 2);
      assert(receipt.attempts.filter((attempt) => attempt.faulted).length === 1);
    } finally {
      await finishCapture(capture);
      await store.server.shutdown();
    }
  },
);
Deno.test(
  'shim refuses unowned logindex without sending network and strips backend scope errors',
  async () => {
    const store = await backend();
    const capture = beginCapture(store.origin, INDEX, document(), 1);
    try {
      const client = new Client({ ...signer(), node: store.origin });
      let rejected = false;
      try {
        await client.index({
          index: 'function_log',
          id: '11111111-1111-4111-8111-111111111111',
          body: document(),
        });
      } catch (error) {
        rejected = String(error).includes('canary_transport_scope_refused');
      }
      assert(rejected && store.writes.length === 0);
      assert(!(await finishCapture(capture)).verified);
    } finally {
      await finishCapture(capture);
      await store.server.shutdown();
    }
  },
);
Deno.test('signed subclass refuses another origin, query or body before delegating', async () => {
  const capture = beginCapture('https://owned.invalid', INDEX, document(), 1);
  let delegated = 0;
  class Base {
    url = new URL('https://owned.invalid');
    request(_params: unknown, _callback: unknown) {
      delegated++;
      return { abort() {} };
    }
  }
  const Guarded = retryConnection(Base, capture);
  try {
    for (const variation of [
      { path: '/other/_doc/11111111-1111-4111-8111-111111111111' },
      { querystring: 'arbitrary=1' },
      { body: JSON.stringify({ ...document(), email: 'real.person@example.com' }) },
      { method: 'GET' },
    ]) {
      let denied = false;
      const connection = new Guarded();
      connection.request(
        {
          method: 'PUT',
          path: `/${INDEX}/_doc/11111111-1111-4111-8111-111111111111`,
          body: JSON.stringify(document()),
          ...variation,
        },
        (error: any) => {
          denied = !!error;
        },
      );
      assert(denied);
    }
    const foreign = new Guarded();
    foreign.url = new URL('https://another.invalid');
    let originDenied = false;
    foreign.request(
      {
        method: 'PUT',
        path: `/${INDEX}/_doc/11111111-1111-4111-8111-111111111111`,
        body: JSON.stringify(document()),
      },
      (error: any) => {
        originDenied = !!error;
      },
    );
    assert(originDenied);
    assert(delegated === 0 && capture.attempts.length === 0);
  } finally {
    await finishCapture(capture);
  }
});
function environment() {
  return {
    LIVE_EDGE_RUN_ID: RUN,
    LIVE_EDGE_PROJECT_REF: 'dlqbkgbktyeqztezqzlo',
    LIVE_EDGE_RUN_TOKEN: TOKEN,
    SUPABASE_URL: 'https://dlqbkgbktyeqztezqzlo.supabase.co',
    OPENSEARCH_DOMAIN: 'https://synthetic.us-east-1.aoss.amazonaws.com',
    OPENSEARCH_LOG_INDEX_NAME: INDEX,
    OPENSEARCH_REGION: 'us-east-1',
    AWS_ACCESS_KEY_ID: 'synthetic',
    AWS_SECRET_ACCESS_KEY: 'synthetic',
  };
}
function request(body: unknown, token = TOKEN) {
  return new Request('https://synthetic.invalid/canary', {
    method: 'POST',
    headers: { authorization: 'Bearer gateway-verified', 'x-live-edge-run-token': token },
    body: JSON.stringify(body),
  });
}
Deno.test(
  'hosted handler rejects production/token/arbitrary fields without invoking actual logger',
  async () => {
    let invoked = 0;
    const values: Record<string, string> = environment();
    const handler = createLogRetryCanaryHandler(
      (name) => values[name],
      () => {
        invoked++;
      },
    );
    assert((await handler(request({ action: 'retry_once' }, 'bad'))).status === 401);
    assert(
      (await handler(request({ action: 'retry_once', email: 'callerchosen@example.invalid' })))
        .status === 400,
    );
    assert((await handler(request({ action: 'unknown' }))).status === 400);
    values.LIVE_EDGE_PROJECT_REF = 'qyyqlnwqwgvzxnccnbgm';
    values.SUPABASE_URL = 'https://qyyqlnwqwgvzxnccnbgm.supabase.co';
    assert((await handler(request({ action: 'retry_once' }))).status === 403 && invoked === 0);
  },
);
