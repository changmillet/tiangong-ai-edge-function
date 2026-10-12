/**
 * Real logInsert construction with provider boundaries replaced; no network.
 * deno test --no-config --import-map scripts/fixtures/email_auth/import_map.json \
 *   --allow-env=OPENSEARCH_LOG_INDEX_NAME,OPENSEARCH_REGION,OPENSEARCH_DOMAIN \
 *   --deny-net scripts/isolated_function_log_test.ts
 */
import logInsert from '../supabase/functions/_shared/supabase_function_log.ts';
import { observations, resetObservations } from './fixtures/email_auth/providers.ts';

function equal(actual: unknown, expected: unknown, message: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) throw new Error(message);
}

function withLogIndex(index: string | undefined, callback: () => void) {
  const names = ['OPENSEARCH_LOG_INDEX_NAME', 'OPENSEARCH_REGION', 'OPENSEARCH_DOMAIN'];
  const previous = names.map((name) => Deno.env.get(name));
  try {
    if (index === undefined) Deno.env.delete(names[0]);
    else Deno.env.set(names[0], index);
    Deno.env.set('OPENSEARCH_REGION', 'us-east-1');
    Deno.env.set('OPENSEARCH_DOMAIN', 'http://127.0.0.1:1');
    resetObservations();
    callback();
  } finally {
    for (let i = 0; i < names.length; i++) {
      if (previous[i] === undefined) Deno.env.delete(names[i]);
      else Deno.env.set(names[i], previous[i]!);
    }
  }
}

Deno.test('unset log index preserves function_log and the original document', () => {
  withLogIndex(undefined, () => {
    logInsert('synthetic-log@example.invalid', 123456789, 'edu_search', 2, 1);
    equal(observations.logIndexes, ['function_log'], 'Default log index changed');
    equal(
      observations.logDocuments,
      [
        {
          email: 'synthetic-log@example.invalid',
          invoked_at: 123456789,
          service_type: 'edu_search',
          top_k: 2,
          ext_k: 1,
        },
      ],
      'Original log document fields changed',
    );
  });
});

Deno.test('isolated log index routes writes without touching function_log', () => {
  withLogIndex('  e2e-72-synthetic-log  ', () => {
    logInsert('synthetic-log@example.invalid', 123456789, 'sci_search');
    equal(observations.logIndexes, ['e2e-72-synthetic-log'], 'Isolated log index ignored');
    equal(
      observations.logDocuments,
      [
        {
          email: 'synthetic-log@example.invalid',
          invoked_at: 123456789,
          service_type: 'sci_search',
          top_k: 0,
          ext_k: 0,
        },
      ],
      'Isolated logging changed document defaults',
    );
  });
});

Deno.test('blank log index retains the existing default', () => {
  for (const blank of ['', ' \t\n ']) {
    withLogIndex(blank, () => {
      logInsert('synthetic-log@example.invalid', 123456789, 'standard_search');
      equal(observations.logIndexes, ['function_log'], 'Blank log index changed the default');
    });
  }
});
