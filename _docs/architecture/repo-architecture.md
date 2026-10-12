---
docType: architecture
scope: repo
status: current
authoritative: true
owner: edge-function
language: en
whenToUse: 'When changing Supabase Edge Functions, support scripts, or deployment packaging.'
whenToUpdate: 'When function topology, shared utilities, deployment targets, or runtime assumptions change.'
checkPaths:
  - supabase/functions/**
  - supabase/config.toml
  - Dockerfile
  - package.json
lastReviewedAt: 2026-10-12
lastReviewedCommit: baf61f6839a45becf0c57c414c4b3c5f4b9eca20
---

# Edge Function Architecture

## Overview

The repository packages Supabase Edge Functions for TianGong AI search and generation APIs. Local development is driven by the Supabase CLI through `npm start`, which serves functions with `supabase/.env.local`.

## Key Paths

- `supabase/functions/_shared/**`: shared function utilities.
- `supabase/functions/*_search/**`: search endpoints, including ESG, science, KB course, education, reports, standards, patents, textbooks, Green Deal, internal content, and BigQuery-backed search.
- `supabase/functions/info_extract/**`: information extraction endpoint.
- `supabase/functions/question_generation/**`: question generation endpoint.
- `supabase/functions/kg_generate/**`: knowledge graph generation endpoint.
- `supabase/config.toml`: local Supabase project configuration.
- `scripts/eval_search_quality.ts`: search quality evaluation helper.
- `scripts/eval_query_rewrite_models.ts`: non-production query rewrite model evaluation helper for comparing OpenAI chat model candidates against the `gpt-4.1-mini` baseline.
- `scripts/query_rewrite_smoke.ts`: six fixed live opt-in rewrite probes through the production helpers, without a model matrix or judge.
- `scripts/query_rewrite_test.ts`: offline request contracts for shared text-model selection, rewrite-specific options, and independent embeddings.
- `Dockerfile`: unvalidated container packaging; it currently references missing `supabase/functions/main` and `supabase/functions/import_map.json`.

## Runtime Shape

The repo uses Node.js tooling for local commands, Deno for Supabase function runtime behavior, and the Supabase CLI for serving and deployment. Function environment values are derived from `.env.example` and `supabase/.env.example`; real local and production secrets must not be committed.

Query rewriting and all structured text generation share `OPENAI_CHAT_MODEL`; configure it as `gpt-6-luna` to select Luna for both. The multilingual and English rewrite helpers fix reasoning to `none` and verbosity to `low`, transported as Responses `reasoning.effort`/`text.verbosity` or Chat `reasoning_effort`/`verbosity`, and explicitly retain `temperature=0`. The shared layer sends temperature only when supplied: general generation uses model defaults for reasoning and sampling, avoiding Luna's rejection of temperature with its default reasoning mode. Embeddings retain `OPENAI_EMBEDDING_MODEL`. Existing default/report/regulatory schemas and sanitize behavior remain intact. Rewrite prompts explicitly retain supplied identifiers and editions in both semantic and lexical fields; unknown abbreviations are not expanded. No shared output-token cap is introduced.

An unset, empty, or whitespace-only `OPENAI_CHAT_MODEL` selects the shared default, `gpt-6-luna`, in both structured entrypoints. The direct helper also trims model names. A request failure propagates through the existing error path without automatic model failover. Alternative models require explicit configuration and validation across generation and rewrite callers, including none/low compatibility.

`course_search` performs Bearer API-key authorization inside the function through the `verify_kb_api_key` RPC. When `UPSTASH_REDIS_URL` and `UPSTASH_REDIS_TOKEN` are configured, successful authorization contexts are cached for up to 15 minutes, capped by token expiry; Redis failures fall back to live RPC verification. Its aggregated `documents` response keeps `content` and `source` and also returns the originating KB `document_id` plus source metadata `tags`, so callers do not need to reverse-engineer record IDs or discard document labels.

## Legacy email authorization cache

`_shared/email_auth_cache.ts` implements a small positive cache for `esg_search`, `sci_search`, `report_search`, normal `edu_search`, `standard_search`, `textbook_search`, `patent_search`, `green_deal_search`, `internal_search`, and `bigquery_search`. Each function owns its own module-scope instance; no cross-worker sharing is implied. Parsing still produces the original email, and existing `logInsert(email, ...)` calls and the log document's `email` field are unchanged. BigQuery's already-commented log call stays commented.

The accepted authorization policy is intentionally email-based: once Redis contains that email, its existence authorizes the request without rechecking the supplied password. This change does not claim credential-bound authentication or migrate API keys. The synchronous hot path reads only an email/deadline map. Positive deadlines are fixed at Redis-request start plus 10,000 ms and never slide on hits. The map holds at most 1,024 FIFO entries. Up to 128 distinct pending Redis reads are tracked for same-email coalescing; overflow follows the original direct-read path. False/error results are not cached. Every coalesced caller checks the original deadline before accepting a positive observation; delayed positive results beyond that deadline fail instead of creating a fresh lease.

Successful password validation creates a missing Redis marker with `SET email value NX EX 3600`. A successful NX no-op preserves the existing marker/value/expiry and can seed the same bounded local lease. The EDU metadata-login path uses this non-renewing write too, while retaining its actual user-session creation. Only real login in the request sets `first_login`; cached authorization does not replace session-dependent metadata logins in SCI, ESG or Standard. These downstream logins and existing business logging remain separate costs.

`EMAIL_AUTH_L1_ENABLED=false` is read when the function instance is initialized. It bypasses both local storage and Redis-read coalescing without removing NX expiry preservation. Redeploy for a deterministic switch; do not assume already-running instances reread environment configuration. This feature does not modify MCP's GET/user-ID cache or scoped `course_search` Bearer caching. Other applications writing the same Redis email key can independently affect its lifetime; the ten-second guarantee is relative to the Redis observation made by this helper, not a promise to control external writers.

## Integration Points

- MCP server calls these edge functions through configured Supabase deployment URLs.
- KB and unstructure repositories produce or maintain data that these functions query, but this repo only owns the API execution surface.

The shared logger accepts `OPENSEARCH_LOG_INDEX_NAME`; unset or blank values retain `function_log`. Isolated qualification can direct synthetic logs to a separate index without changing production defaults or document fields. The write remains asynchronous, so successful logging requires bounded read-back evidence.
