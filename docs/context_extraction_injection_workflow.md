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
workload parity or published VikingMem scores. It produces local TemporalStore evidence:

- `benchmark_source_count` and `benchmark_query_count`
- `benchmark_profile`
- `benchmark_recall_at_k`
- `benchmark_hit_at_k` and `benchmark_mean_reciprocal_rank`
- `benchmark_token_reduction_percent`
- ingest, retrieval, and injection throughput counters
- `benchmark_retrieve_p50_ms` and `benchmark_retrieve_p95_ms`
- per-query hit rank, reciprocal rank, selected-block count, token count, and latency
- `benchmark_sweep_*` fields covering multi-profile source/query sweeps, minimum hit@k, minimum
  MRR, minimum token reduction, total source/query coverage, and maximum p95 retrieval latency
- mixed source-kind and provider accounting through the ingest/extract summary

The current local workload uses synthetic incidents, tickets, documents, chats, code snippets, and
user events so it can run without external model credentials while still exercising the same
management, ingestion/extraction, retrieval, and injection pipeline. Profiles are explicit strings
so future benchmark runs can separate local synthetic sweeps, paper-inspired regression sweeps, and
deployment-specific model/provider sweeps without changing the JSON schema. The default sweep uses
small, medium, and large deterministic profiles; the local harness currently runs a smaller
two-profile sweep to keep validation quick while preserving profile-comparison evidence.

Remaining policy hardening:

- production policy controls for PII filtering, tenant isolation, prompt-size admission, rate
  limiting, and provider failure budgets remain the operating contract for deployments and are
  validated by the policy report.
