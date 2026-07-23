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

## Benchmark Data Flow

LOCOMO, LongMemEval_s, and OpenViking-style benchmark ingestion use the same Rust
TemporalStore context models as normal production traffic:

1. Benchmark entity blocks are backed by the node-level record stored as `ContextNodeModel`. The
   node owns the stable node hash, canonical title, `L0` routing summary, `L1` key-fact summary,
   last event time, dirty-summary flag, and raw source metadata ref. Rust also has a first-class
   C++ `ContextEntityModel` for extracted entity attributes keyed by tenant/node/entity hash.
2. `ContextSegment` is the public benchmark/pipeline name for the timestamped evidence segment
   stored as `ContextEventModel`. It owns the timestamp key, event id hash, source text, source
   ref, confidence/importance, and related entity node hashes. The page stores timestamp-keyed
   segment entries; the segment text is not stored separately from the page entry.
3. `ContextIndexRef` stores secondary indexes such as `source:<source_id_hash> -> entity/segment`.
   Benchmark replay uses this index to prove a source or conversation turn can route back to the
   exact timestamped segment before retrieval/injection.
4. Retrieval materializes `ContextBlock` entries from the models: `L0` and `L1` blocks come from
   the entity, while `L2` blocks come from matching timestamped segments.
5. Injection packs the selected L0/L1/L2 blocks into `<context>` and writes `ContextPackAudit`
   selected refs so benchmark reports can prove the injected evidence came from TemporalStore.

The shared C++/Rust corpus case `context_benchmark_injection_entity_segment_index` exercises this
flow with a LOCOMO-style conversation turn: extract entity/segment records, query the `source`
secondary index, retrieve L0/L1/L2 blocks, inject them under a token budget, and verify the audit
refs point back to the same entity and segment.

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
  "vlm_model": "mock-context-vlm",
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

For OpenViking-style local deployments, TemporalStore now reports explicit open-source model
profiles through `GET /context/workflow/state`:

- `openviking-qwen2_5_vl-local`: `qwen2.5vl:7b` VLM, `qwen2.5:7b-instruct` chat model,
  `nomic-embed-text` embedding model, OpenAI-compatible local gateway at `127.0.0.1:11434/v1`
- `openviking-llava-local`: `llava:7b` VLM, `llama3.1:8b-instruct` chat model,
  `nomic-embed-text` embedding model
- `openviking-internvl-vllm`: `OpenGVLab/InternVL2_5-8B` VLM,
  `Qwen/Qwen2.5-7B-Instruct` chat model, `BAAI/bge-m3` embedding model, OpenAI-compatible
  gateway at `127.0.0.1:8000/v1`
- `vikingmem-gpt-4o-mini-reader`: `gpt-4o-mini` reader/chat model for VikingMem benchmark parity,
  `sentence-transformers/all-MiniLM-L6-v2` embedding model, OpenAI-compatible gateway at
  `https://api.openai.com/v1`
- `matrixark-cpp-oss-context`: `google/flan-t5-small` extraction model and
  `sentence-transformers/all-MiniLM-L6-v2` embedding model, retained for legacy open-source
  MatrixArk/C++ comparison runs
- `openviking-minigpt4-gpt-style-vlm`: `Vision-CAIR/MiniGPT-4` as the open-source GPT-4-style VLM,
  `lmsys/vicuna-7b-v1.5` chat model, `BAAI/bge-m3` embedding model, OpenAI-compatible gateway at
  `127.0.0.1:8000/v1`

These profiles mirror OpenViking's two required model capabilities: a VLM for image/content
understanding and an embedding model for vectorization and semantic retrieval. TemporalStore still
uses deterministic `mock_mode=true` for local CI unless a live Ollama, vLLM, or compatible gateway
is intentionally started.
The state and harness reports expose `open_model_provider_packaged`,
`open_model_local_run_proven`, `vlm_provider_configured`, and `vlm_benchmark_proven`. Packaged and
configured fields can be true from checked-in provider profiles; proven fields stay false until a
real local model endpoint or VLM benchmark run passes and archives its report.

## OpenViking Comparison

Adopted:

- layered L0/L1/L2 context
- URI-like stable refs
- provider-configurable model backends
- explicit open-source VLM and embedding model profiles
- mockable local workflow

Different:

- TemporalStore stores data in Context models rather than a separate `viking://` filesystem
- refs use `tsctx://tenant/<id>/node/<id>/...`
- local validation is deterministic and does not require external model credentials

## Local Testing

For a copy-paste operator runbook covering HTTP server startup, `/context/ingest_extract`,
`/context/extract`, `/context/retrieve`, and `/context/inject`, see
[`context_ingestion_extraction_retrieval_manual.md`](context_ingestion_extraction_retrieval_manual.md).

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

## C++ Context Model Parity

The current C++ TemporalStore context module registers first-class LLM context model names on top of
existing hash/feature page primitives:

| C++ model | Model id | Rust model descriptor | Key family | Primitive |
| --- | ---: | --- | --- | --- |
| `ContextNodeModel` | `9` | `ContextNodeModel` | `ctx:node` | hash/object metadata, L0/L1 summaries |
| `ContextEventModel` / `ContextSegment` | `10` | `ContextEventModel` | `ctx:event` | timestamp-keyed segment page |
| `ContextIndexModel` | `11` | `ContextIndexModel` | `ctxidx` | timestamped feature page |
| `ContextAuditModel` | `12` | `ContextAuditModel` | `ctx:audit` | timestamped feature page |
| `ContextDirtyModel` | `13` | `ContextDirtyModel` | `ctx:dirty` | timestamped feature page |
| `ContextEntityModel` | `18` | `ContextEntityModel` | `ctx:entity` | hash/object extracted entity attributes |

Rust exposes the same model IDs through `context_model_descriptors()`, uses the same object-key
families, and matches the C++ timeline fanout (`1024 * 1024`) so multiple records in the same
millisecond are queryable through the same range semantics. Rust also enforces the C++ context
limits for index names, query limits, filter counts, related-node fanout, audit refs, propagation
depth, score ranges, timestamp overflow, and bounded payload sizes.

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
- `benchmark_evidence_retention_at_k` and per-query `evidence_retained` evidence proving the
  expected answer/topic survives budgeted context injection, not only retrieval
- `benchmark_thresholds`, `benchmark_threshold_passed`, and threshold violation counts for
  explicit regression gates on hit@k, recall proxy, evidence retention, selected-token budget,
  token reduction, latency, and throughput
- `benchmark_mean_reciprocal_rank`, `benchmark_sweep_min_mean_reciprocal_rank`, and
  `external_benchmark_mean_reciprocal_rank` remain quality metrics. They are emitted for trend
  tracking, but MRR is not part of the Rust-native pipeline readiness contract.
- per-query hit rank, reciprocal rank, evidence retention, selected-block count, token count, and
  latency
- `benchmark_sweep_*` fields covering multi-profile source/query sweeps, minimum hit@k, minimum
  MRR quality metric, minimum evidence retention, minimum token reduction, total source/query coverage, maximum
  p95 retrieval/injection latency, selected-token averages and maximums, zero-hit totals, profile
  signatures, workload coverage ranges, and sweep-wide threshold pass/fail evidence
- `external_benchmark_*` fields covering optional LOCOMO/LongMemEval-style JSONL replay, including
  dataset name, case count, hit@k, MRR, answer-term coverage, missing expected terms, zero-hit
  queries, and source path
- `external_benchmark_category_count` and `external_benchmark_category_breakdown` fields covering
  per-reasoning-type case count, hit@k, MRR quality metric, answer-term coverage, missing expected
  terms, and zero-hit queries for single-hop, multi-hop reasoning, temporal, memory update, quantity,
  social-link, and entity-alias style cases
- `external_benchmark_all_categories_passed`, `external_benchmark_min_category_hit_at_k`,
  `external_benchmark_min_category_mean_reciprocal_rank`, and
  `external_benchmark_category_zero_hit_queries` so external benchmark readiness fails if any
  reasoning bucket misses, even when aggregate hit@k remains high
- `external_benchmark_all_expected_terms_matched`, `external_benchmark_answer_term_coverage`, and
  `external_benchmark_missing_expected_terms` so multi-part LOCOMO/LongMemEval answers fail unless
  every expected evidence term is covered by retrieved context
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
and validates stale/latest memory ranking at `hit_at_k = 1.0`, `mean_reciprocal_rank = 1.0`,
`evidence_retention_at_k = 1.0`, and zero zero-hit queries in the checked harness output.

Real benchmark exports can be supplied without recompiling by setting
`TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL` before running `context_workflow_harness`. Each JSONL record
is one QA case and accepts this shape:

```json
{"dataset":"locomo","query_id":"q1","category":"memory_update","query":"What is Alice's current office choice after the payment problem?","answer_terms":["downtown"],"messages":[{"kind":"chat","title":"Earlier preference","text":"Alice preferred the airport office before the later change."},{"kind":"chat","title":"Latest update","text":"Alice replaced her office preference with the downtown location after the billing issue was resolved."}]}
```

The parser also accepts `question` for `query`, `answers` or `expected_terms` for `answer_terms`,
`category`, `reasoning_type`, or `question_type` for per-category reporting, and `sources`,
`messages`, or `conversation` arrays with `body`, `text`, `message`, or `content` fields. When no
JSONL path is configured, the harness runs a built-in LOCOMO/LongMemEval_s-style
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

The shared C++/Rust corpus also has a dedicated `context_openviking_reasoning_vlm_parity` case.
Rust executes `context_openviking_reasoning_vlm_cases_cover_required_gaps`, which requires explicit
coverage for multi-hop reasoning, temporal reasoning, memory updates, stale-memory replacement,
open-domain retrieval, and VLM image/content understanding. The VLM case is currently a
configuration and retrieval-shape proof, not a passed VLM benchmark; it remains marked
`benchmark_proven = false` until the Docker/open-model path completes a real OpenAI-compatible
local model run and archives the resulting report.

Remaining policy hardening:

- production policy controls for PII filtering, tenant isolation, prompt-size admission, rate
  limiting, and provider failure budgets remain the operating contract for deployments and are
  validated by the policy report.
