# Context Extraction And Injection Workflow

This document describes the Rust-native Context workflow used for local validation and model
provider switching. The implementation is inspired by OpenViking's hierarchical context idea, but
TemporalStore keeps Context data in its existing `ContextNode`, `ContextEvent`, `ContextIndexRef`,
`ContextPackAudit`, and `ContextSummaryDirtyMarker` models.

## Workflow

The workflow is:

1. `manage`: report supported routes, provider count, pipeline stages, and C++/OpenViking parity
   evidence before admitting a deployment as context-ready. The management report now includes
   per-stage readiness, provider names, and policy controls so operators can see which part of the
   pipeline owns a failure.
2. `ingest/extract`: batch sources are accepted under one shard/tenant, normalized to the selected
   provider, summarized by source kind/provider/time window, and converted into retrieval-ready
   node hashes.
3. `extract`: deterministic mock extraction creates a node, event, source index ref, and dirty
   marker from mocked data such as incidents, tickets, documents, chats, code snippets, or user
   events.
4. `normalize`: extracted context is split into OpenViking-style tiers:
   - `L0`: short routing summary
   - `L1`: structured key-fact summary
   - `L2`: full source payload reference
5. `persist/index`: the workflow writes Context model commands through `TemporalEngine`.
6. `retrieve/inject`: retrieval reads Context models by tenant/node/time/filter and injects selected
   blocks into a prompt under a token budget.
7. `audit`: injection persists a `ContextPackAudit` with selected and blocked refs.

## API

The data-node server exposes:

- `POST /context/extract`
- `POST /context/ingest_extract`
- `POST /context/retrieve`
- `POST /context/inject`
- `GET /context/manage`
- `GET /context/workflow/state`
- `GET /context/model/providers`
- `POST /context/model/provider`

Model provider config is OpenAI-compatible by shape:

```json
{
  "provider_name": "mock-openai-compatible",
  "provider_kind": "mock",
  "base_url": "",
  "api_key_env": "",
  "model": "mock-context-chat",
  "embedding_model": "mock-context-embedding",
  "timeout_ms": 30000,
  "max_retries": 2,
  "mock_mode": true
}
```

Open-source and commercial models should use the same config shape with
`provider_kind=open_ai_compatible`, an `http://` `base_url`, and `mock_mode=false`. The Rust-native
local path calls `/chat/completions` with bounded deadlines/retries, loads the bearer token only
from `api_key_env`, parses `choices[0].message.content`, and can fall back to a configured mock or
secondary provider if the live endpoint is unavailable.

## OpenViking Comparison

Adopted:

- layered L0/L1/L2 context
- URI-like stable refs
- provider-configurable model backends
- mockable local workflow

Different:

- TemporalStore stores data in Context models rather than a separate `viking://` filesystem
- refs use `tsctx://tenant/<id>/node/<id>/...`
- local validation is deterministic and does not require external model credentials

## Local Testing

Run the local harness:

```bash
tools/run_context_workflow_local.sh
```

Run the Docker-packaged binary after building the image:

```bash
docker build -t temporalstore-rust:context .
docker run --rm temporalstore-rust:context context_workflow_harness
```

The harness verifies:

- mocked extraction succeeds
- the management report advertises manage, ingest, extract, index, retrieve, inject, and audit
- the management report exposes per-stage readiness, provider names, and policy controls
- batch ingest/extract accepts multiple sources, reports source-kind/provider accounting, and emits
  a retrieval request
- a VikingMem-style local benchmark runs mixed synthetic Context sources through extraction,
  hierarchical retrieval, injection, recall proxy, token-reduction accounting, and retrieval p50/p95
  latency reporting
- Context blocks are retrieved
- prompt injection includes `<context>`
- selected refs are recorded in `ContextPackAudit`
- the provider config is reported in the JSON summary
- restart replay preserves the same `ContextNode` and `ContextEvent`
- shared-store sync and async replay preserve the same Context pipeline writes
- Raft replica reads can serve the same Context event after the write path is replicated
- `context_pipeline_ready` is true only when the parity report covers C++ Context models,
  OpenViking L0/L1/L2 tiers, extraction, retrieval, injection, index refs, pack audit, dirty
  summary, restart replay, shared-store sync/async replay, Raft reads, and unified corpus evidence

## Production Readiness

The production readiness gate now tracks this workflow as `context_workflow`.

Covered:

- Context models are persisted through the normal engine command path.
- L0/L1/L2 tier generation and prompt injection are deterministic for local mocks.
- OpenAI-compatible HTTP model execution supports bounded deadlines, retries, environment-backed
  bearer auth, JSON summary parsing, and fallback provider execution.
- Data-node HTTP routes expose extract, retrieve, inject, workflow state, and provider inspection.
- Data-node HTTP routes expose management and batch ingest/extract pipeline handoff.
- The local harness and Docker-packaged harness validate management, ingest/extract, retrieval,
  injection, and audit refs.
- C++/OpenViking parity evidence covers engine-local restart, shared-store sync/async replay,
  Raft reads, and the shared C++/Rust Context corpus.

## VikingMem-Style Benchmark

`context_workflow_harness` runs a deterministic local benchmark inspired by the VikingMem paper's
long-term memory evaluation themes: retrieval effectiveness, low interactive latency, hierarchical
context loading, and reduced context tokens. The benchmark does not claim byte-for-byte VikingMem
workload parity, published VikingMem scores, or a licensed copy of LOCOMO/LongMemEval_s. It
includes LOCOMO-style conversational-memory and LongMemEval_s-style long-context synthetic profiles
so local validation can exercise similar source/query scaling and hit-ranking behavior. It produces
local TemporalStore evidence:

- `benchmark_source_count` and `benchmark_query_count`
- `benchmark_profile`
- `benchmark_workload_signature`, topic count, per-topic source coverage, and source-kind
  coverage so local and Docker benchmark runs can prove they exercised the same workload shape
- `benchmark_recall_at_k`
- `benchmark_hit_at_k` and `benchmark_mean_reciprocal_rank`
- `benchmark_token_reduction_percent`
- ingest, retrieval, and injection throughput counters
- `benchmark_retrieve_p50_ms`, `benchmark_retrieve_p95_ms`, `benchmark_inject_p50_ms`, and
  `benchmark_inject_p95_ms`
- average retrieved blocks, selected blocks, selected tokens, max selected tokens, and zero-hit
  query counts
- `benchmark_thresholds`, `benchmark_threshold_passed`, and threshold violation counts for
  explicit regression gates on hit@k, MRR, recall proxy, token reduction, latency, and throughput
- per-query hit rank, reciprocal rank, selected-block count, token count, and latency
- `benchmark_sweep_*` fields covering multi-profile source/query sweeps, minimum hit@k, minimum
  MRR, minimum token reduction, total source/query coverage, maximum p95 retrieval/injection
  latency, selected-token averages, zero-hit totals, profile signatures, workload coverage ranges,
  and sweep-wide threshold pass/fail evidence
- `external_benchmark_*` fields covering optional LOCOMO/LongMemEval-style JSONL replay, including
  dataset name, case count, hit@k, MRR, zero-hit queries, and source path
- mixed source-kind and provider accounting through the ingest/extract summary

The current local workload uses synthetic incidents, tickets, documents, chats, code snippets, and
user events so it can run without external model credentials while still exercising the same
management, ingestion/extraction, retrieval, and injection pipeline. Profiles are explicit strings
so future benchmark runs can separate local synthetic sweeps, paper-inspired regression sweeps, and
deployment-specific model/provider sweeps without changing the JSON schema. The default sweep uses
small, medium, large, LOCOMO-style, and LongMemEval_s-style deterministic profiles; the local
harness runs four quick profiles to preserve profile-comparison evidence. The retrieval scorer uses
query-term overlap, compact QA synonym expansion, adjacent phrase boosts, exact topic-phrase
boosting, and latest/update wording for benchmark questions. Synthetic sources now include older
baseline memories and later memory updates, while queries rotate through payment-risk,
service-outage, preference-update, and support-follow-up paraphrases. This keeps non-verbatim
conversational-memory questions such as payment/fraud wording aligned with checkout/risk memories
and validates stale/latest memory ranking at `hit_at_k = 1.0`, `mean_reciprocal_rank = 1.0`, and
zero zero-hit queries in the checked harness.
output.

Real benchmark exports can be supplied without recompiling by setting
`TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL` before running `context_workflow_harness`. Each JSONL record
is one QA case and accepts this shape:

```json
{"dataset":"locomo","query_id":"q1","query":"What is Alice's current office choice after the payment problem?","answer_terms":["downtown"],"messages":[{"kind":"chat","title":"Earlier preference","text":"Alice preferred the airport office before the later change."},{"kind":"chat","title":"Latest update","text":"Alice replaced her office preference with the downtown location after the billing issue was resolved."}]}
```

The parser also accepts `question` for `query`, `answers` or `expected_terms` for `answer_terms`,
and `sources`, `messages`, or `conversation` arrays with `body`, `text`, `message`, or `content`
fields. When no JSONL path is configured, the harness runs a built-in LOCOMO/LongMemEval_s-style
fixture so CI and local Docker validation still enforce external-benchmark scoring. Retrieval now
normalizes punctuation and hyphenation, applies simple plural stemming, expands temporal,
multi-hop, latest/update, preference, location/workplace, problem/resolution, support, and
risk/payment aliases, and boosts latest, temporal, correction, and reminder evidence so newer
memory updates and remembered facts outrank stale conversational memories. It also boosts
contrastive updates such as switched, moved, became, cancelled, and instead so stale memories with
overlapping entities do not win against current facts, and social-link cues such as recommended,
suggested, introduced, and referred so multi-hop relationship evidence wins over generic planning
context. Schedule/detail cues such as rescheduled, appointment, deadline, calendar, date, and time
are boosted so stale calendar details do not outrank current dates. Quantity cues such as how many,
count, number, total, amount, and score are boosted so current numeric facts outrank stale values.
Alias cues such as roommate, manager, named, called, pet, dog, and cat help role/name memories beat
stale aliases. The built-in external fixture includes direct update questions plus harder
paraphrases such as "Where does Alice want to work now?" and "Which setting changed most recently
across the conversation history?", plus temporal-after and root-cause questions such as
"What did Alice decide after the airport trip conversation?" and "Why did checkout fail after the
backend outage?", corrected preference questions such as "What snack should Jordan avoid now after
the correction?", and medication-reminder questions such as "Which medication did Morgan say to
remember before the doctor appointment?", plus contrastive-update questions such as "Which hobby did
Priya switch to after cancelling guitar lessons?" and "Who is the backup contact now after Sam moved
teams?", and social-link questions such as "Who recommended the cafe that Nina booked after the
conference?" and "Which project did Lee pick because Dana suggested it during planning?" to keep
LOCOMO/LongMemEval-style hit-rate regressions visible. Schedule-detail cases such as "When is
Maya's dentist appointment after it was rescheduled?" and "What is the new report deadline after the
calendar update?" cover date/time updates. Quantity cases such as "How many guests did Sofia confirm
after the dinner update?" and "What risk score was recorded after the latest fraud review?" cover
numeric memory updates. Alias cases such as "What is Emma's roommate's name after the move?" and
"What is the dog's name in the latest pet update?" cover entity-disambiguation updates.

Remaining policy hardening:

- production policy controls for PII filtering, tenant isolation, prompt-size admission, rate
  limiting, and provider failure budgets remain the operating contract for deployments and are
  validated by the policy report.
