---
docType: runbook
scope: repo
status: current
authoritative: true
owner: edge-function
language: en
whenToUse: 'When qualifying deployed KB search consumers against real external providers.'
whenToUpdate: 'When target guards, probe ownership, provider stages or qualification evidence change.'
checkPaths:
  [
    scripts/live_edge/**,
    scripts/isolated_function_log_test.ts,
    supabase/functions/_shared/supabase_function_log.ts,
    .github/workflows/docpact.yml,
  ]
lastReviewedAt: 2026-10-12
lastReviewedCommit: baf61f6839a45becf0c57c414c4b3c5f4b9eca20
---

# Isolated hosted qualification

This harness qualifies EDU, ESG, standards and SCI through actual deployed Edge Functions, real Auth/PostgREST and real OpenAI/Pinecone/OpenSearch/Upstash calls. It is a bounded operator workflow, not a production health monitor. The Python fixture guard deliberately binds the reviewed branch in `fixtures.BRANCH_REF`. A future run needs a newly verified data-free branch and a reviewed binding; never replace the binding with a production project or disable its checks.

## Environment and source identity

1. Create a data-free Supabase development branch. Record its Management API branch ID, project reference, parent, `with_data=false` and healthy status. Verify the database catalog against the reviewed database migration chain and verify the selected business tables and Auth users are empty.
2. Download the current four deployed function packages to private storage and compare their transitive sources with the intended test package. Record source hashes, deployment versions, bundle hashes and JWT settings. Source similarity alone is not a deployed-runtime result. Keep unrelated deployment drift out of a qualification change.
3. A new branch copies functions without external secrets. Remove its inherited function copies before installing provider credentials. Only deploy the four selected handlers plus the restricted probe on this temporary branch.
4. Explicitly bind `REMOTE_SUPABASE_URL` and `REMOTE_SUPABASE_PUBLISHABLE_KEY` to the branch. Configure the real provider credentials from a private local file, without copying all host environment variables. The runtime uses `PINECONE_API_KEY_US_EAST_1`, `PINECONE_INDEX_NAME`, `OPENSEARCH_DOMAIN`, `OPENSEARCH_REGION`, AWS credentials and the OpenAI settings. Upstash needs its HTTPS REST URL and REST token, not a TCP `redis://` connection string.
5. Allocate a fresh 16-hex run ID. All four Pinecone namespaces and corresponding OpenSearch index names must be exactly `live72-<run>-edu`, `-esg`, `-standard` and `-sci`. Set `OPENSEARCH_LOG_INDEX_NAME=live72-<run>-log`, `LIVE_EDGE_PROJECT_REF`, `LIVE_EDGE_RUN_ID` and a random `LIVE_EDGE_RUN_TOKEN`. The probe rejects missing, default or unrelated targets. Use `EMAIL_AUTH_L1_ENABLED=false` to make real Redis behavior observable.
6. Set secrets before deployment. Explicitly set `verify_jwt=true` in the test deployment's config for every function, including the probe; omitting `--no-verify-jwt` does not necessarily reset a previous false value. Verify deployed metadata. This extra branch JWT gate is distinct from each handler's existing email/password/cache authorization.

Use the versioned Supabase CLI's `--help` to discover actual flags. Deploy named functions with an explicit project ref and the production import map. The probe entrypoint can be a private wrapper:

```typescript
import { createProbeHandler } from './probe.ts';
Deno.serve(createProbeHandler());
```

## Private target and fixtures

Keep a regular 0600 target JSON outside the checkout, with no symlink components. It contains schema version 1; branch/project/parent IDs; `with_data=false`; run ID and prefix; the exact branch API and database URLs; anon, publishable and service keys; and the random probe token. No environment file is implicitly loaded by the fixture or handler checker. Database URL host/user binding and libpq override parameters are checked before connections. The fixture manifest records allocated UUIDs and synthetic Auth identities before any write.

Use the existing KB Python environment with psycopg2 for fixture operations:

```bash
python scripts/live_edge/fixtures.py setup --target /private/path/target.json
python scripts/live_edge/check.py --target /private/path/target.json --source edu
```

Do not invoke the checker before the corresponding external fixtures are ready. All HTTP checks send the branch anon JWT plus `apikey`; cold authorization failure must come from the handler rather than the gateway. EDU explicitly distinguishes its existing hot email-cache acceptance from cold password rejection.

## Provider preparation and phased checks

Invoke the probe over HTTPS with the branch JWT and `x-live-edge-run-token`. First verify that a missing JWT is rejected by the gateway and a missing run token is rejected by the probe. `inspect` exposes only configuration-presence booleans.

`seed` receives the private fixture's nonsecret marker/resource IDs/course/name/ DOI plus `query_text`, which must contain the run ID. It generates a real OpenAI embedding, two Pinecone vectors and three OpenSearch documents per selected consumer. Distinct `PINECONE_ONLY_`, `OPENSEARCH_ONLY_` and `EXTENSION_` markers prove both retrieval sources and adjacent-chunk expansion contributed to the response. Only synthetic payloads go to these providers.

Prefer `source: "edu"`, then `esg`, `standard` and `sci`, instead of preparing all four at once. For each source:

1. Call `seed` once with that source. Partial failure is not permission to loop writes. Read back state and diagnose before a further mutation.
2. Use bounded `stats` reads until actual owned vectors and lexical documents are visible. Pinecone's index statistics can lag list/fetch/query visibility. A lower count is recorded as `stats_lag`; every listed vector still needs a fetched ownership record. Missing ownership evidence is never treated as an empty namespace, and a higher unexplained count is refused.
3. Run `check.py --source <source>`. Per-source HTTP budgets are 5/3/3/2, with a 180-second deadline. Assertions cover real metadata facets/filtering/hydration, both provider markers, neighbor expansion and negative cases. Raw responses remain in private per-source receipts; aggregate their separate summaries.
4. Read `stats` with the two exact synthetic emails. Verify log records contain the original synthetic email, expected service types and numeric fields. Logs are asynchronous, so use bounded read-back. Logs precede retrieval and cannot themselves prove provider success; HTTP 200 cannot prove logging.
5. Call source-scoped `cleanup`. It removes only that source's exact vector IDs and owned search index, retaining the shared log index and Redis keys. Do not pass emails to a source-scoped cleanup. Verify absence before the next source.

This keeps at most one test retrieval index plus the dedicated log index active. The probe uses exact ownership metadata and bounded calls. It drains every parallel mutation before releasing its local operation lock. The operator must serialize requests; a per-isolate lock is not a distributed lock.

On AOSS 507 or timeouts, stop writes, retain error metadata and reduce test load. AWS describes 507 as an internal error generally associated with OCU overload or scaling delay ([official error guidance](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-getting-started.html)). That description does not prove a specific run's root cause. Do not change shared capacity, access policy or production indexes through this workflow. Cleanup uses owned index mappings and deletion read-back rather than depending on a healthy search operation.

## Public Crossref exception

Only when separately authorized, `crossref_check.py` qualifies the actual KB collector against at most two public papers and four Crossref GETs. It leaves the KB collector and skill sources unchanged, bounds live pagination at the transport boundary, and records source hashes. Duplicate insertion uses a clearly labelled local replay of the captured real HTTP responses; it is not a second live query. The optional `--confirm-existing-abstracts` action reads only those same two DOIs. No fabricated abstract is substituted when Crossref returns none. Other provider fallbacks are disabled for this narrowly authorized Crossref check.

```bash
python scripts/live_edge/crossref_check.py --target /private/path/target.json
python scripts/live_edge/crossref_check.py --target /private/path/target.json --confirm-existing-abstracts
```

Public metadata and raw responses stay private and are never repository fixtures. The script deletes its exact paper/ISSN records and verifies absence, preserving other synthetic consumer fixtures in the same branch.

## Final cleanup and offline gates

After every phase, unscoped probe `cleanup` removes any remaining owned vectors, search indexes, log index and exact synthetic email keys. Require empty list/fetch and zero count, index 404 responses and Redis EXISTS=0/TTL=-2; repeat read-back or idempotent cleanup only within a fixed budget. Then run fixture cleanup, verify own database rows and Auth users/sessions are absent, remove the temporary branch and private credential files, and preserve nonsecret receipts. No namespace-wide `deleteAll`, wildcard delete or table truncate is used.

```bash
python scripts/live_edge/fixtures.py cleanup --target /private/path/target.json
python scripts/live_edge/test_safety.py
python scripts/live_edge/crossref_check.py --self-test
deno test --no-lock --config supabase/functions/deno.json --allow-env --allow-sys=osRelease --deny-net scripts/live_edge/probe_test.ts
```

The offline probe tests substitute provider ports and prohibit network access; they are safety contracts, not live qualification. The CI workflow runs those contracts, Python guard tests and isolated logging tests without credentials.
