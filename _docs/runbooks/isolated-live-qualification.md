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
    scripts/function_log_retry_test.ts,
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

## Logger retry idempotence and controlled canary

Each `logInsert` invocation generates one UUID and supplies it as the OpenSearch document ID. SDK retries after a committed write with a lost acknowledgement address that same document. The original five document fields and index default remain unchanged. Two independent invocations get distinct UUIDs even when their email, timestamp and other fields are identical. This is per-logger-invocation retry idempotence, not HTTP-wide deduplication or an exactly-once delivery claim. The historical SCI duplicate is consistent with an acknowledgement-loss retry; its specific cause was not uniquely established by the previous receipts.

Logging retains its nonblocking void return. When available, the logger registers the handled write promise with `EdgeRuntime.waitUntil`; local runtimes without that API still work. Background registration protects the task lifecycle after a response, but does not provide a durable queue. Failed writes log only an error name, without upstream bodies or signed request metadata.

The real-SDK loopback regression first reproduced two documents with the old automatic-ID logger after both a commit-then-503 and a commit-then-socket-drop. The fixed logger left one document after two attempts in each case, and retained two documents for two legitimate identical invocations. It also checks background registration, safely handled rejection, unchanged document fields and absent `EdgeRuntime` compatibility. The test imports `Transport` through the same alias as the actual logger and prints the resolved module/version. Runtime SDK aliases remain unpinned; the regression does not assume a fixed package version or mix a pinned test observer with a different production client. The socket-drop case uses the client's normal timeout and can take about 30 seconds.

For the hosted controlled canary, after verifying a new data-free branch:

1. Read back the AOSS collection capability. The explicit document-ID request must be supported; do not change a collection type or shared AWS policy.
2. Allocate the exact owned log index and configure the existing branch/ref/run guards and random token. Deploy an explicit wrapper calling `Deno.serve(createLogRetryCanaryHandler())`, with gateway JWT verification on. Verify its transitive source and deployment hashes before invoking it.
3. Apply `scripts/live_edge/log_retry_import_map.json` only to the canary. Its scoped connection wrapper delegates to the real SDK and real SigV4 connection. After the first actual backend 200/201 acknowledgement, it delivers a single synthetic 503 to the SDK so the SDK retries the real committed write. Normal business functions must retain their normal import map.
4. Submit only `{"action":"retry_once"}` with the branch JWT and run token. Require two backend attempts, one invocation, one stable request/response UUID, and exactly one matching stored log document. Read back only this synthetic email and `invoked_at`; a canary HTTP 200 alone is insufficient.
5. Submit `{"action":"two_distinct"}`. Require three attempts (one injected retry), two distinct invocation UUIDs and exactly two matching documents even though their original five fields are identical. Check the current SDK version from the receipt rather than asserting a fixed version.
6. Clean the exact owned log index, verify absence, and remove the temporary canary/branch and credential files following the final cleanup procedure.

The canary is a test endpoint, not a production fault injector. Its offline transport suite uses a synthetic loopback document store and the actual SDK; those results are distinct from hosted AOSS evidence.

### Reproducible private canary package

From the Edge repository root, assemble this layout in a new private deployment directory. The nested shared-helper path preserves the canary's unchanged relative import. Existing branch configuration and credentials remain outside the public checkout:

```text
deployment/supabase/functions/log-retry-canary/
  index.ts
  deno.json
  scripts/live_edge/log_retry_canary.ts
  scripts/live_edge/log_retry_transport.ts
  supabase/functions/_shared/supabase_function_log.ts
```

The wrapper must use a normal formatted static import. A minified wrapper with no spaces was not discovered correctly by the CLI package uploader:

```typescript
import { createLogRetryCanaryHandler } from './scripts/live_edge/log_retry_canary.ts';
Deno.serve(createLogRetryCanaryHandler());
```

Copy the three listed source modules without changing their bytes. Copy the normal `supabase/functions/deno.json` into the function directory, replacing only this mapping:

```json
{
  "@opensearch-project/opensearch": "./scripts/live_edge/log_retry_transport.ts"
}
```

Keep every other mapping, especially the `/aws` signer, unchanged. Add or verify this section in the private deployment root's `supabase/config.toml`:

```toml
[functions.log-retry-canary]
verify_jwt = true
```

From that private deployment root, after validating the exact preview identity, deploy only the named function:

```bash
supabase functions deploy log-retry-canary --project-ref VERIFIED_PREVIEW_REF --import-map supabase/functions/log-retry-canary/deno.json
```

Do not omit the function name, pass `--prune`, or reuse this mapping for normal SCI/search deployments. Download/read back the deployed source package and verify all staged file hashes, its wrapper, import map and gateway JWT setting. An API bundle upload does not inherit the repository's root `deno.lock`; the normal npm aliases are unversioned. Record SDK versions actually returned by the canary rather than inferring a pin from a local lockfile or test run.

## Public Crossref exception

Only when separately authorized, `crossref_check.py` qualifies the actual KB collector against at most two public papers and four Crossref GETs. It leaves the KB collector and skill sources unchanged, bounds live pagination at the transport boundary, and records source hashes. Duplicate insertion uses a clearly labelled local replay of the captured real HTTP responses; it is not a second live query. The optional `--confirm-existing-abstracts` action reads only those same two DOIs. No fabricated abstract is substituted when Crossref returns none. Other provider fallbacks are disabled for this narrowly authorized Crossref check.

```bash
python scripts/live_edge/crossref_check.py --target /private/path/target.json
python scripts/live_edge/crossref_check.py --target /private/path/target.json --confirm-existing-abstracts
```

Public metadata and raw responses stay private and are never repository fixtures. The script deletes its exact paper/ISSN records and verifies absence, preserving other synthetic consumer fixtures in the same branch.

### Positive real-provider abstract backfill

The historical PLOS sample contained no public abstracts. To qualify a positive write, obtain explicit authorization for the separate two-paper sample recorded in `POSITIVE_SAMPLE`, then use a newly verified empty branch:

```bash
python scripts/live_edge/crossref_check.py --target /private/path/target.json --positive-abstract-sample
```

This operation permits exactly four fresh Crossref GETs: two single-work metadata requests and two resolver requests for the same reviewed DOIs. It does not replay cached positive responses or contact fallback providers. The original metadata skill converts real records; controlled empty-abstract rows get explicit UUID/DOI ownership intent before insertion. The actual KB candidate query must find two rows, the actual resolver must return the reviewed abstract fingerprints, and the guarded writer must update two fields. Repeating the same real abstracts must update zero fields and report both existing values preserved. The candidate query must then find zero eligible owned rows.

Require exact UUID-and-DOI cleanup and absence read-back, including on failure. After an interrupted run, use the private manifest to retry cleanup without another provider request:

```bash
python scripts/live_edge/crossref_check.py --target /private/path/target.json --cleanup-positive-abstracts
```

Save public response bodies, fingerprints and source hashes outside Git. If a reviewed paper no longer supplies its expected abstract, stop and report the observed limitation; do not synthesize text or expand the sample without scope. This sample's positive results do not replace the historical no-data receipt.

## Final cleanup and offline gates

After every phase, unscoped probe `cleanup` removes any remaining owned vectors, search indexes, log index and exact synthetic email keys. Require empty list/fetch and zero count, index 404 responses and Redis EXISTS=0/TTL=-2; repeat read-back or idempotent cleanup only within a fixed budget. Then run fixture cleanup, verify own database rows and Auth users/sessions are absent, remove the temporary branch and private credential files, and preserve nonsecret receipts. No namespace-wide `deleteAll`, wildcard delete or table truncate is used.

```bash
python scripts/live_edge/fixtures.py cleanup --target /private/path/target.json
python scripts/live_edge/test_safety.py
python scripts/live_edge/crossref_check.py --self-test
deno test --no-lock --config supabase/functions/deno.json --allow-env --allow-sys=osRelease --allow-run=node --allow-net=127.0.0.1 scripts/function_log_retry_test.ts
deno test --no-lock --config supabase/functions/deno.json --allow-env --allow-sys=osRelease --allow-net=127.0.0.1 scripts/live_edge/log_retry_transport_test.ts
deno test --no-lock --config supabase/functions/deno.json --allow-env --allow-sys=osRelease --deny-net scripts/live_edge/probe_test.ts
```

The offline probe tests substitute provider ports and prohibit network access; they are safety contracts, not live qualification. The two retry suites allow only loopback HTTP, use synthetic signing credentials and observe the real SDK. The first also runs a native Node loopback backend. Dependency resolution can fetch SDK packages, but no real provider endpoint is permitted at runtime. CI installs the existing Node 22 baseline and runs these suites, Python guards, positive-abstract budget checks and isolated logging tests without cloud secrets. Its test job needs only read access to repository contents.

## Qualification observed on 2026-10-12

Tracked delivery: [edge-functions #72](https://github.com/tiangong-ai/edge-functions/issues/72). The data-free branch `dlqbkgbktyeqztezqzlo` under the Knowledge Base parent matched the reviewed 40-migration catalog (24 tables, 72 functions, 11 enums). Selected business tables and Auth users were verified empty before synthetic setup.

- Four deployed consumers passed 13 HTTP requests and 33 assertions: EDU 5/13, ESG 3/7, standards 3/7 and SCI 2/6 (requests/assertions). Both provider-specific content markers and adjacent chunks appeared; ESG/standards metadata no-match stopped retrieval; SCI sources contained the actual test-table DOI/title/authors.
- Calls used real OpenAI, Pinecone, OpenSearch and Upstash with no provider doubles. The model was explicitly `gpt-6-luna`, with `text-embedding-3-small` embeddings. Cold invalid credentials were rejected by handlers; existing hot-email cache acceptance was tested separately. Redis TTL was observed decreasing across phases rather than being reset by each request.
- Nine synthetic log documents represented eight distinct invocation records; SCI had two documents with identical invocation fields. No exactly-once logging claim is made and no logging retry policy was changed. All expected service types and the original synthetic email were present. A size-zero, read-only query of production `function_log`, restricted to the run's synthetic emails, returned zero matches and no production rows.
- The first all-at-once fixture seed hit AOSS bulk errors/507 and a timeout. Its resources were fully removed before qualification resumed one consumer at a time. Tests also observed stale Pinecone aggregate counts and stale AOSS visibility during phase transitions. The probe verifies actual owned records, reports lag honestly and does not query unrelated sources during a phase.
- Crossref used four real GETs for two explicitly approved public papers. Actual KB ingestion inserted two then zero on labelled local response replay. Both single-work responses lacked abstracts: real no-data backfill skipping was verified, but a successful real-provider abstract update was not claimed.
- All owned vectors, indexes/logs and Redis keys were removed, with repeated cleanup and a final independent read-back after branch deletion. The test database's four consumer tables, journals/ISSN rows, Auth users and sessions were empty before branch removal. Production function versions and bundle hashes were unchanged; production functions, business data and AWS policies were not modified.

The test packages retained the captured production sources except for the optional log-index setting. Two production OpenAI helpers had an older unset-model default; their captured bytes were retained in the isolated package and the model was explicitly configured. Thus this qualifies the observed configured behavior, not the production unset-model fallback. All four downloaded test packages matched their staged source hashes. Test deployments added a JWT gateway on top of the existing handler authorization; the probe additionally required a random token.

| Function | Isolated version | Bundle SHA-256 |
| --- | --- | --- |
| `edu_search` | 5 | `0aa33181e3cf3db0142d4fa66b0d54c28bb1d338875bb0d349f85ec35449de8e` |
| `esg_search` | 5 | `019b47bc4bd21c8eac846ee40161c6c3b8b85bd8be3fc92b5c7367fba7cfb666` |
| `standard_search` | 5 | `62ac143ec1720e6fea98ea04d646f103c96f3489f0b7f6671748230a64ba6ac2` |
| `sci_search` | 5 | `e552c2112c4a6b1ffc5362f36c55663f0afe460b40dc66bb3aa885cb36ba01dd` |
| `live-edge-probe` | 6 | `95dd20cc58eb24fa74b009d6560876d359fa132bd1a33ee8f9dc209ef83b1775` |

Offline gates passed 105 tests: 21 cache, 49 handler/log, 25 probe, six target/HTTP guard and four Crossref budget tests. Strict docpact and full-branch explicit-diff lint passed. Private source/provider receipts remain outside the public repository; no credentials or raw public-paper responses are committed.

Sanitized aggregate receipt SHA-256: `72476e9d37bb59fe983b0f9692353a8bc78a66f1bebf1603e062da5b84c0f072`.

## Follow-up qualification on 2026-10-12

[Edge-functions #74](https://github.com/tiangong-ai/edge-functions/issues/74) completed the following isolated qualification. Branch `qujtuhjbamgzzkixrlay` was data-free, matched the 40-migration catalog and had empty selected business/Auth tables before synthetic setup. Only the three reviewed functions below were deployed, all with JWT verification enabled.

- Normal SCI used its normal import map and real OpenAI/Pinecone/OpenSearch/Redis/PostgreSQL services. Two rounds passed four HTTP requests and 12 assertions: cold auth rejection, both backend markers, adjacent chunks and metadata hydration. The two successful searches produced two distinct timestamps and two UUID log documents, one per invocation.
- The separate canary used the actual shared logger and signed SDK, injecting one acknowledgement fault after observing a real successful backend write. `retry_once` made two backend attempts with one UUID; read-back found exactly one document. `two_distinct` invoked the logger twice with identical fields and timestamp; three backend attempts retained two distinct UUIDs, and read-back found exactly those two documents. Both negative gateway/token checks returned 401; no unmatched canary-email document existed. The fault was injected by test transport code, not an observed spontaneous provider outage.
- The canary resolved SDK **3.9.0**. The existing local cache initially resolved **3.6.0**; all five logger loopback tests also passed with a private temporary 3.9.0 map, including committed-write/socket-loss and independent identical calls. Repository import mappings and dependency versions were not changed. These are observed versions, not an assertion that hosted deployment inherits the root lockfile.
- The two public papers below supplied real abstracts through four fresh Crossref GETs: two metadata requests and two requests through the actual resolver. Actual KB candidates went from two to zero; the writer updated two rows, replay updated zero and preserved both values. Exact UUID/DOI cleanup, repeated cleanup and an independent UUID-or-DOI absence query found zero residual rows. Discovery used five earlier read-only requests; those cached responses were not used for the positive run.
- Cleanup removed all run-owned vectors, search/log indexes and three exact synthetic Redis keys. A read-only provider check after branch deletion confirmed zero vectors, missing indexes, zero logs and Redis EXISTS=0/TTL=-2. Before deletion, all five selected business tables plus Auth users/sessions were empty. The branch was then absent from the parent listing. Task credential copies and raw public response caches were removed; the user's source configuration was retained.
- The downloaded SCI entrypoint, logger and normal import map matched reviewed staging/checkout bytes. All five downloaded canary files matched staging. The captured production SCI package was unchanged across 14 files. No production function deployment, production business-data export/mutation or infrastructure policy change was performed.

| Public DOI | Clean abstract characters | SHA-256 |
| --- | --- | --- |
| `10.1128/jcm.12.1.60-62.1980` | 1,411 | `cbb529e80d200eff7440d7e884cdcdc9beb6b0bfc0ee1407a0d8a0f12280f7d2` |
| `10.31168/2305-6754.2023.1.05` | 1,519 | `9f4810a6946e4e8f247bf35d9fce522269ed16778f7ccd2ae0e49c0807992b66` |

| Isolated function | Version | Bundle SHA-256 |
| --- | --- | --- |
| `sci_search` | 1 | `9022116035d760d45fe1c426d842a6ad7dc914bb355569fb768b511acb642624` |
| `live-edge-probe` | 1 | `725a7d268dab9b3a120ae7bfdc8f0d466fd01acbeaba56ffa98619359ac4cd63` |
| `log-retry-canary` | 1 | `42cb94a7cdf5c89b852fbfc2fcaf3725c16f8041ca531e941b3618cbcb6a03b0` |

The regression matrix passed 123 tests (21 cache, 46 handler, three log routing, six target/HTTP guards, 25 probe, 12 Crossref, five logger and five canary), plus five additional logger tests against the observed hosted SDK version. Independent local-app review confirmed the mechanism and loopback results; CI and deployment-documentation gaps it identified were resolved. Its initial lockfile-based hosted-version assumption was superseded by the canary receipt. The prior run had no transport trace, so its historical duplicate cause is not uniquely established. The fix guarantees neither HTTP-wide exactly-once execution nor delivery after exhausted retries/runtime termination.

Sanitized aggregate receipt SHA-256: `1c130a2d47acac3a053bd57096ec131efc911918c2a9d6ea4e75229b20605bc5`.
