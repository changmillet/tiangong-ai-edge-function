// Setup type definitions for built-in Supabase Runtime APIs
import '@supabase/functions-js/edge-runtime.d.ts';

import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { Client } from '@opensearch-project/opensearch';
import { AwsSigv4Signer } from '@opensearch-project/opensearch/aws';

function logInsert(
  email: string,
  invoked_at: number,
  service_type: string,
  top_k: number = 0,
  ext_k = 0,
) {
  const opensearch_region = Deno.env.get('OPENSEARCH_REGION') ?? '';
  const opensearch_domain = Deno.env.get('OPENSEARCH_DOMAIN') ?? '';
  const log_index_name = Deno.env.get('OPENSEARCH_LOG_INDEX_NAME')?.trim() || 'function_log';

  const opensearchClient = new Client({
    ...AwsSigv4Signer({
      region: opensearch_region,
      service: 'aoss',

      getCredentials: () => {
        const credentialsProvider = defaultProvider();
        return credentialsProvider();
      },
    }),
    node: opensearch_domain,
  });

  const document = {
    email,
    invoked_at,
    service_type,
    top_k,
    ext_k,
  };

  // SDK retries must address the same document after a committed write whose
  // acknowledgement was lost. Independent logger calls receive distinct IDs.
  const request = { index: log_index_name, id: crypto.randomUUID(), body: document };
  const task = opensearchClient
    .index(request)
    .then(() => {})
    .catch((error) => {
      // Provider errors can contain signed headers and raw upstream bodies.
      console.error('Error inserting log:', error instanceof Error ? error.name : 'UnknownError');
    });

  const edgeRuntime = (
    globalThis as typeof globalThis & { EdgeRuntime?: { waitUntil(task: Promise<void>): void } }
  ).EdgeRuntime;
  if (typeof edgeRuntime?.waitUntil === 'function') edgeRuntime.waitUntil(task);
}

export default logInsert;
