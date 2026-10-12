/** Hosted test-only entry. JWT verification must remain enabled at the gateway.
 *
 * CANARY import map ONLY:
 *   @opensearch-project/opensearch -> ./log_retry_transport.ts
 * The shim imports the raw npm:@opensearch-project/opensearch specifier (same
 * package selection as the normal map), bypassing alias recursion. Keep the
 * existing /aws alias and all other mappings unchanged. Never use this
 * map for real business functions. Deploy a wrapper that imports this module and
 * calls Deno.serve(createLogRetryCanaryHandler()) explicitly.
 *
 * The actual shared logger is invoked unchanged. Only its first observed real
 * 200/201 acknowledgement is changed to 503; the real SDK sends the retry. The
 * endpoint returns wire/result UUIDs and statuses, never document/credential
 * bodies. Verify document cardinality with the separately guarded probe/log
 * readback, filtered by this receipt's synthetic email and invoked_at.
 */
import logInsert from '../../supabase/functions/_shared/supabase_function_log.ts';
import { beginCapture, finishCapture } from './log_retry_transport.ts';

type Env = (name: string) => string | undefined;
const PRODUCTION_REF = 'qyyqlnwqwgvzxnccnbgm';
class Refusal extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
  ) {
    super(code);
  }
}
function require(value: unknown, code: string, status = 403): asserts value {
  if (!value) throw new Refusal(code, status);
}
function tokenEqual(left: string, right: string) {
  if (left.length !== right.length) return false;
  let result = 0;
  for (let i = 0; i < left.length; i++) result |= left.charCodeAt(i) ^ right.charCodeAt(i);
  return result === 0;
}

export function createLogRetryCanaryHandler(env: Env = Deno.env.get, logger = logInsert) {
  let active = false;
  return async (request: Request) => {
    let owned = false;
    try {
      const ref = env('LIVE_EDGE_PROJECT_REF') ?? '';
      const run = env('LIVE_EDGE_RUN_ID') ?? '';
      const token = env('LIVE_EDGE_RUN_TOKEN') ?? '';
      require(/^[a-z]{20}$/.test(ref) && ref !== PRODUCTION_REF, 'invalid_or_production_project');
      require(env('SUPABASE_URL') === `https://${ref}.supabase.co`, 'runtime_project_mismatch');
      require(!env('REMOTE_SUPABASE_URL') ||
        env('REMOTE_SUPABASE_URL') === `https://${ref}.supabase.co`, 'remote_project_mismatch');
      require(/^[a-f0-9]{16}$/.test(run), 'invalid_run_configuration');
      require(/^[A-Za-z0-9_-]{32,128}$/.test(token), 'invalid_run_token_configuration');
      require(/^Bearer [^\s]+$/.test(
        request.headers.get('authorization') ?? '',
      ), 'gateway_bearer_required', 401);
      require(tokenEqual(
        request.headers.get('x-live-edge-run-token') ?? '',
        token,
      ), 'run_token_required', 401);
      require(request.method === 'POST', 'post_required', 405);
      const reader = request.body?.getReader();
      require(reader, 'json_body_required', 400);
      const bytes: number[] = [];
      while (true) {
        const value = await reader.read();
        if (value.done) break;
        if (bytes.length + value.value.length > 1024) {
          await reader.cancel();
          throw new Refusal('payload_too_large', 413);
        }
        bytes.push(...value.value);
      }
      let body: any;
      try {
        body = JSON.parse(new TextDecoder().decode(new Uint8Array(bytes)));
      } catch (_) {
        throw new Refusal('invalid_json', 400);
      }
      require(body &&
        typeof body === 'object' &&
        !Array.isArray(body) &&
        Object.keys(body).length === 1 &&
        ['retry_once', 'two_distinct'].includes(body.action), 'invalid_action_payload', 400);
      require(!active, 'canary_already_running', 409);
      active = true;
      owned = true;
      const index = env('OPENSEARCH_LOG_INDEX_NAME') ?? '';
      require(index === `live72-${run}-log`, 'unowned_log_index');
      const origin = new URL(env('OPENSEARCH_DOMAIN') ?? '');
      require(origin.protocol === 'https:' &&
        origin.hostname.endsWith('.aoss.amazonaws.com') &&
        !origin.username &&
        !origin.password &&
        origin.pathname === '/' &&
        !origin.search &&
        !origin.hash, 'invalid_aoss_origin');
      require(env('AWS_ACCESS_KEY_ID') &&
        env('AWS_SECRET_ACCESS_KEY') &&
        env('OPENSEARCH_REGION'), 'signing_environment_incomplete', 503);
      const email = `live74-${run}@example.invalid`;
      const invoked_at = Date.now();
      const document = { email, invoked_at, service_type: 'sci_search', top_k: 1, ext_k: 1 };
      const invocations = body.action === 'retry_once' ? 1 : 2;
      const capture = beginCapture(origin.origin, index, document, invocations);
      let result;
      try {
        for (let i = 0; i < invocations; i++) logger(email, invoked_at, 'sci_search', 1, 1);
      } finally {
        result = await finishCapture(capture);
      }
      return Response.json(
        { action: body.action, run_id: run, email, invoked_at, ...result },
        { status: result.verified ? 200 : 502 },
      );
    } catch (error) {
      return Response.json(
        { error: error instanceof Refusal ? error.code : 'canary_execution_failed' },
        { status: error instanceof Refusal ? error.status : 502 },
      );
    } finally {
      if (owned) active = false;
    }
  };
}
if (import.meta.main) Deno.serve(createLogRetryCanaryHandler());
