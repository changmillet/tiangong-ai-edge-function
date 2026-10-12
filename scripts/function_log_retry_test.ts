/**
 * The logger's actual OpenSearch module graph plus loopback acknowledgement loss.
 * No Client.index, signer, credentials provider or retry implementation is mocked.
 * deno test --no-lock --config supabase/functions/deno.json \
 *   --allow-env --allow-sys=osRelease --allow-run=node --allow-net=127.0.0.1 scripts/function_log_retry_test.ts
 */
import { Transport } from '@opensearch-project/opensearch';
import logInsert from '../supabase/functions/_shared/supabase_function_log.ts';

// Derive metadata from the same alias, including any version in its mapping.
// Never mix a pinned observer with an unversioned production Client import.
const sdkSpecifier = import.meta.resolve('@opensearch-project/opensearch');
const { default: sdkPackage } = await import(new URL('./package.json', sdkSpecifier).href, {
  with: { type: 'json' },
});
console.info(`Actual logger OpenSearch module: ${sdkSpecifier}; version ${sdkPackage.version}`);

type Mode = 'commit503' | 'commitDrop' | 'healthy' | 'reject';
type Attempt = { method: string; path: string; id: string; body: Record<string, unknown> };
type RuntimeGlobal = typeof globalThis & { EdgeRuntime?: { waitUntil(task: Promise<void>): void } };

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function equal(actual: unknown, expected: unknown, message: string) {
  assert(
    JSON.stringify(actual) === JSON.stringify(expected),
    `${message}; observed=${JSON.stringify(actual)}, expected=${JSON.stringify(expected)}`,
  );
}

async function until(condition: () => boolean | Promise<boolean>, timeout = 5000) {
  const deadline = performance.now() + timeout;
  while (!(await condition())) {
    if (performance.now() > deadline) throw new Error('Loopback transport readiness timed out');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function backend(mode: Mode) {
  const attempts: Attempt[] = [];
  const documents = new Map<string, Record<string, unknown>>();
  // Native Node socket destruction makes lost-ack behavior explicit, without
  // relying on Deno's node:http server compatibility layer.
  const source = `
import {createServer} from 'node:http';
const attempts=[],documents=new Map();
const server=createServer((request,response)=>{
 if(request.method==='HEAD'||(request.method==='GET'&&request.url==='/')){response.writeHead(200);response.end();return;}
 if(request.method==='GET'&&request.url==='/state'){
  response.writeHead(200,{'content-type':'application/json'});
  response.end(JSON.stringify({attempts,documents:[...documents]}));return;
 }
 let raw='';request.on('data',chunk=>raw+=chunk);
 request.on('end',()=>{
  const path=request.url,id=path.split('/')[3]||'auto-'+(attempts.length+1),body=JSON.parse(raw);
  attempts.push({method:request.method,path,id,body});
  if(process.env.MODE==='reject'){
   response.writeHead(403,{'content-type':'application/json'});
   response.end(JSON.stringify({error:{type:'security_exception',reason:'PRIVATE_UPSTREAM_BODY_MUST_NOT_BE_LOGGED'},status:403}));return;
  }
  documents.set(id,body);
  if(attempts.length===1&&process.env.MODE==='commitDrop'){request.socket.destroy();return;}
  if(attempts.length===1&&process.env.MODE==='commit503'){
   response.writeHead(503,{'content-type':'application/json'});
   response.end(JSON.stringify({error:{type:'unavailable',reason:'committed before lost acknowledgement'},status:503}));return;
  }
  response.writeHead(201,{'content-type':'application/json'});
  response.end(JSON.stringify({_index:'retry-test',_id:id,result:'created',_version:1}));
 });
});
server.listen(0,'127.0.0.1',()=>process.stdout.write(JSON.stringify({origin:'http://127.0.0.1:'+server.address().port})+'\\n'));
`;
  const process = new Deno.Command('node', {
    args: ['--input-type=module', '-e', source],
    clearEnv: true,
    env: { PATH: Deno.env.get('PATH') ?? '', MODE: mode },
    stdout: 'piped',
    stderr: 'null',
  }).spawn();
  const reader = process.stdout.getReader();
  const ready = await reader.read();
  assert(ready.value, 'Native loopback server did not start');
  const { origin } = JSON.parse(new TextDecoder().decode(ready.value));
  return {
    attempts,
    documents,
    origin,
    async refresh() {
      const response = await fetch(origin + '/state');
      const state = await response.json();
      attempts.splice(0, attempts.length, ...state.attempts);
      documents.clear();
      for (const [id, document] of state.documents) documents.set(id, document);
    },
    async close() {
      process.kill('SIGTERM');
      await process.status;
      await reader.cancel();
      reader.releaseLock();
    },
  };
}

async function withLogger(
  mode: Mode,
  callback: (fixture: Awaited<ReturnType<typeof backend>>, tasks: Promise<void>[]) => Promise<void>,
  background = true,
) {
  const fixture = await backend(mode);
  const environment = {
    OPENSEARCH_DOMAIN: fixture.origin,
    OPENSEARCH_REGION: 'us-east-1',
    OPENSEARCH_LOG_INDEX_NAME: 'retry-test',
    AWS_ACCESS_KEY_ID: 'SYNTHETIC_LOOPBACK_ACCESS_KEY',
    AWS_SECRET_ACCESS_KEY: 'synthetic-loopback-secret',
    AWS_SESSION_TOKEN: '',
    AWS_PROFILE: '',
    AWS_DEFAULT_PROFILE: '',
    AWS_WEB_IDENTITY_TOKEN_FILE: '',
    AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: '',
    AWS_CONTAINER_CREDENTIALS_FULL_URI: '',
  };
  const previous = Object.keys(environment).map((key) => Deno.env.get(key));
  const runtime = globalThis as RuntimeGlobal;
  const previousRuntime = runtime.EdgeRuntime;
  const tasks: Promise<void>[] = [];
  // Observe settlement of the actual SDK promise even before the logger has
  // background registration. The real transport and its retry code still run.
  const sdkTasks: Promise<unknown>[] = [];
  const originalRequest = Transport.prototype.request;
  Transport.prototype.request = function (
    this: Transport,
    ...arguments_: Parameters<typeof originalRequest>
  ) {
    const callback = arguments_[2];
    if (typeof callback === 'function') {
      // AwsSigv4Signer delegates in callback mode, whose thenable has no
      // underlying promise. Observe its real completion callback instead.
      sdkTasks.push(
        new Promise<void>((resolve) => {
          arguments_[2] = (error, response) => {
            const returned = callback(error, response);
            resolve();
            return returned;
          };
        }),
      );
      return originalRequest.apply(this, arguments_);
    }
    const operation = originalRequest.apply(this, arguments_);
    sdkTasks.push(Promise.resolve(operation).catch(() => {}));
    return operation;
  } as typeof originalRequest;
  try {
    for (const [key, value] of Object.entries(environment)) Deno.env.set(key, value);
    if (background)
      runtime.EdgeRuntime = {
        waitUntil(task) {
          tasks.push(task);
        },
      };
    else delete runtime.EdgeRuntime;
    await callback(fixture, tasks);
    await Promise.all(tasks);
  } finally {
    await Promise.all(sdkTasks);
    Transport.prototype.request = originalRequest;
    if (previousRuntime === undefined) delete runtime.EdgeRuntime;
    else runtime.EdgeRuntime = previousRuntime;
    for (const [index, key] of Object.keys(environment).entries()) {
      const value = previous[index];
      if (value === undefined) Deno.env.delete(key);
      else Deno.env.set(key, value);
    }
    await fixture.close();
  }
}

const document = {
  email: 'retry-only@example.invalid',
  invoked_at: 123456789,
  service_type: 'sci_search',
  top_k: 1,
  ext_k: 1,
};

for (const mode of ['commit503', 'commitDrop'] as const) {
  Deno.test(`real SDK ${mode}: retry preserves one document per logger invocation`, async () => {
    await withLogger(mode, async (fixture, tasks) => {
      const returned = logInsert(document.email, document.invoked_at, document.service_type, 1, 1);
      equal(returned, undefined, 'Logger should retain its nonblocking void return');
      await until(
        async () => {
          await fixture.refresh();
          return fixture.attempts.length >= 2;
        },
        mode === 'commitDrop' ? 45000 : 5000,
      );
      await Promise.all(tasks);
      equal(fixture.attempts.length, 2, 'The real SDK should make exactly one retry');
      equal(
        fixture.documents.size,
        1,
        'Committed acknowledgement loss duplicated a logger invocation',
      );
      assert(
        fixture.attempts.every((attempt) => attempt.method === 'PUT'),
        'Retries must use the explicit-ID PUT route',
      );
      equal(
        fixture.attempts[0].id,
        fixture.attempts[1].id,
        'The SDK retry changed the document ID',
      );
      assert(/^[0-9a-f-]{36}$/.test(fixture.attempts[0].id), 'The logger event ID is not a UUID');
      for (const attempt of fixture.attempts)
        equal(attempt.body, document, 'The original five log fields changed');
    });
  });
}

Deno.test('identical legitimate logger invocations retain distinct document IDs', async () => {
  await withLogger('healthy', async (fixture, tasks) => {
    logInsert(document.email, document.invoked_at, document.service_type, 1, 1);
    logInsert(document.email, document.invoked_at, document.service_type, 1, 1);
    await until(async () => {
      await fixture.refresh();
      return fixture.attempts.length === 2;
    });
    await Promise.all(tasks);
    equal(
      fixture.documents.size,
      2,
      'Independent invocations were collapsed by their identical fields',
    );
    assert(
      fixture.attempts[0].id !== fixture.attempts[1].id,
      'Independent invocations reused an ID',
    );
    for (const attempt of fixture.attempts)
      equal(attempt.body, document, 'Legitimate log document changed');
  });
});

Deno.test(
  'background registration preserves void return and handles rejected writes safely',
  async () => {
    const originalError = console.error;
    const messages: unknown[][] = [];
    console.error = (...arguments_: unknown[]) => {
      messages.push(arguments_);
    };
    try {
      await withLogger('reject', async (fixture, tasks) => {
        const returned = logInsert(
          document.email,
          document.invoked_at,
          document.service_type,
          1,
          1,
        );
        equal(returned, undefined, 'Background registration blocked or changed the logger return');
        equal(tasks.length, 1, 'Supabase background lifecycle did not receive the logging task');
        await Promise.all(tasks);
        await fixture.refresh();
        equal(fixture.attempts.length, 1, 'Forbidden response should not be retried');
        equal(fixture.documents.size, 0, 'Rejected request wrote a document');
        equal(messages.length, 1, 'Rejected logging did not produce one handled diagnostic');
        assert(
          !JSON.stringify(messages).includes('PRIVATE_UPSTREAM_BODY_MUST_NOT_BE_LOGGED'),
          'Raw provider diagnostics leaked into the log',
        );
      });
    } finally {
      console.error = originalError;
    }
  },
);

Deno.test('logger still works without a Supabase background runtime', async () => {
  await withLogger(
    'healthy',
    async (fixture) => {
      equal(
        logInsert(document.email, document.invoked_at, document.service_type, 1, 1),
        undefined,
        'Local logger return changed',
      );
      await until(async () => {
        await fixture.refresh();
        return fixture.attempts.length === 1;
      });
      equal(fixture.documents.size, 1, 'Absent EdgeRuntime prevented logging');
    },
    false,
  );
});
