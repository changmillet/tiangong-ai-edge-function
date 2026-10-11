---
docType: runbook
scope: repo
status: current
authoritative: true
owner: edge-function
language: en
whenToUse: 'When developing, validating, or deploying edge functions.'
whenToUpdate: 'When setup commands, local serve flow, validation, or deployment commands change.'
checkPaths:
  - README.md
  - package.json
  - deno.json
  - Dockerfile
  - supabase/**
lastReviewedAt: 2026-10-11
lastReviewedCommit: fcb951e8a564f9e1e6cef470ae9a6c6a71145347
---

# Edge Function Development Runbook

## Setup

1. Use Node.js 22 from `.nvmrc`.
2. Install Deno as described in `README.md`.
3. Run `npm install`.
4. Copy `.env.example` to `.env.local` for root-level tooling.
5. Copy `supabase/.env.example` to `supabase/.env.local` before running `npm start`.
6. Configure `UPSTASH_REDIS_URL` and `UPSTASH_REDIS_TOKEN` when validating `course_search` Bearer authorization caching; leave them unset to force live RPC verification.

## Local Serve

Run:

```bash
npm start
```

This serves Supabase functions with:

```bash
supabase functions serve --env-file ./supabase/.env.local --no-verify-jwt
```

## Validation

Run:

```bash
npm run lint
docpact validate-config --root . --strict
```

Use `test.example.http` or a REST client for endpoint checks when function behavior changes.

## Email authorization cache validation and rollback

Run the pure cache contracts and actual-handler contracts without provider access:

```bash
deno test --no-config scripts/email_auth_cache_test.ts
deno test --no-config --import-map scripts/fixtures/email_auth/import_map.json --allow-env --allow-read=supabase/functions,scripts/fixtures/email_auth --deny-net scripts/email_auth_callers_test.ts
```

The `email-auth-contracts` CI job runs both commands with Deno 2.1.4, matching the documented local/runtime baseline.

The handler tests substitute only provider SDKs/generation/embedding with fixtures; the request handlers, email decoder, shared authorization helper and `logInsert` document construction remain real. They must cover Redis/memory/login paths, rejected auth, the original email in logged documents, and session-dependent metadata branches. Confirm the exact command/result before delivery; the fixtures are not live retrieval qualification.

Use a module-initialized `EMAIL_AUTH_L1_ENABLED=false` to disable local caching/coalescing. It is an instance setting; redeploy a target to make a rollback deterministic. Redis writes still use NX EX 3600 and never extend an existing entry. No new dependency or Redis data migration is required. An existing Redis result may be accepted from local memory for up to ten seconds after deletion/expiry; this is the explicitly accepted policy. Never serve a local entry after its own fixed deadline or extend it on a hit/error.

Before updating an existing hosted function, save its current source package, import map, verify_jwt setting, version and source hashes outside Git. Compare the deployed package to the checkout and exclude unrelated drift from the deployment. Validate the exact shared helper against real Redis using only newly generated synthetic email keys and a restricted test endpoint. Do not delete/alter real users' email keys. Keep a hard request/deadline budget and stop on unexpected auth, logging or business errors or degraded project health.

Use legitimate, explicitly scoped test credentials for a business canary with small topK/extK. Keep passwords, API keys and provider responses outside Git; preserve only sanitized validation evidence in the delivery Issue/PR. If a temporary Auth account is required, create a preconfirmed test account without sending an email and delete it after validation. Check that `function_log` still records the original test email for the canary where read access permits; offline logging tests are not proof of live log persistence.

Rollback redeploys the saved pre-change package with its original import map and verify_jwt setting. It must not guess the old version from a default branch or overwrite unrelated shared-file changes. Record actual canary targets, deployed source identities, observations, cleanup and any rollback in the tracked delivery record. A successful canary does not imply every function has been deployed.

## Query Rewrite Model Evaluation

Set `OPENAI_CHAT_MODEL=gpt-6-luna` for both query rewriting and general text generation. There is no separate rewrite-model setting. Rewriting retains fixed `none` reasoning, `low` verbosity and `temperature=0`; general generation leaves reasoning and sampling to model defaults. The shared layer must not inject a temperature when none was supplied, because Luna rejects it with default reasoning. `OPENAI_EMBEDDING_MODEL` remains independent. An unset, empty, or whitespace-only chat-model value selects the shared Luna default in both structured entrypoints; a failed request does not automatically switch models. Validate all text-generation callers and the rewrite helpers' none/low compatibility before choosing another model. This code change does not itself deploy functions or update remote secrets.

Run the offline request contracts and the six-fixture live opt-in smoke using the commands in README. The smoke checks raw schema fields before sanitization, supplied identifiers and exclusions in both query fields, alias bounds, and the provider's actual model/configuration receipt. It makes at most six requests without retries; inspect the default dry output before spending on live calls. Raw reports belong outside Git. Use a valid existing credential with a local function server and small `topK`/`extK` values for real retrieval checks; do not bypass authorization or replace retrieval backends with mocks when claiming retrieval qualification.

### Luna qualification observed on 2026-10-10

Model-selection evidence comes from [LCA PR #465](https://github.com/tiangong-lca/edge-functions/pull/465). Local verification was limited to integration compatibility:

- The initial offline production-caller request checks covered all four profiles on Responses and Chat, rewrite-specific request options, and unchanged embedding selection. The current contract test additionally verifies that all four rewrite profiles and all three generation helpers follow the same `OPENAI_CHAT_MODEL`, including trimmed, blank and unset values. It uses offline fixtures and sends no provider requests. Chat parameter transport is verified with mocks; live qualification used Responses, selected by the current SDK.
- The initial six live Luna calls found one CAS omission in `semantic_query`. The shared prompt was strengthened to preserve every supplied identifier/edition in both fields without invented expansions. A new six-call run passed every raw/sanitized schema and semantic probe; all responses reported Luna, none/low and zero reasoning tokens. Other existing prompts and schemas were retained.
- Two authenticated local `edu_search` requests with the same Chinese nitrogen/phosphorus-removal query and `topK=3, extK=0` used real configured OpenAI, Pinecone and OpenSearch backends. The pre-migration baseline and Luna both returned HTTP 200 and the same ordered two relevant documents. Provider receipts confirmed the requested rewrite models and unchanged `text-embedding-3-small` embeddings. This is historical retrieval smoke evidence, not a current fallback recommendation or a statistical latency, cost or full-corpus quality result.
- Live backend responses and receipts were retained outside Git. Only sanitized outcomes are recorded here and in the delivery Issue/PR. Remote deployment remains a separate operation.

### Legacy candidate matrix

Use `scripts/eval_query_rewrite_models.ts` when comparing OpenAI chat models for query rewrite behavior. The script reuses production rewrite prompts and schemas, compares candidates against `gpt-4.1-mini`, and writes reports to `/tmp/tiangong-eval` unless `--output-prefix` is provided.

```bash
set -a; . ./supabase/.env.local; set +a
deno run --allow-env --allow-net --allow-read --allow-write \
  --config supabase/functions/deno.json \
  scripts/eval_query_rewrite_models.ts --dry-run
```

Use `--models=<id,id>` to restrict the matrix. No optional candidate is currently registered; `--include-optional` is retained for CLI compatibility. The script only reports a suggested `OPENAI_CHAT_MODEL`; it does not mutate production configuration.

## Deployment

Use the Supabase deployment commands in `README.md` for individual functions. Pass `--import-map supabase/functions/deno.json` so the remote bundler resolves shared Deno and npm import aliases. Docker packaging is not currently a validated path: `Dockerfile` references `supabase/functions/main` and `supabase/functions/import_map.json`, which are not present. Fix and validate the Dockerfile before using Docker deployment commands.
