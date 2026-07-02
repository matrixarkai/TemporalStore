# TemporalStore Context Extension Local Test Setup

This document describes local validation for the C++ context extension used by
MatrixArk.

## Repository

Windows path:

```text
<workspace>\Codex\2026-06-07\what-s-the-topology-for-all\temporalstore-service-fix
```

WSL path:

```bash
<workspace>/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix
```

Branch:

```bash
codex/llm-context-temporalstore
```

Remote:

```text
https://github.com/bjmeetsfo/TemporalStore.git
```

## What To Test

The context extension test covers:

- node upsert and get
- event write and query
- secondary index write, scoped query, limits, de-duplication, and AND intersection
- summary dirty marker write and query
- context-pack audit write and query
- invalid production inputs
- inclusive time-bucket query behavior

The scale E2E validates the LLM context models across the full pipeline:

```text
ContextNode      explicit upsert/get, child listing after API, stream, and resource ingest
ContextEvent     API, batch, stream, resource extraction, feedback, timeline query, retrieval
ContextEntity    API, batch, stream, resource extraction, feedback update, retrieval
ContextIndex     declared status/event_type/project indexes, scoped hash indexes, AND intersection
ContextSummary   async-style L0 refresh, as-of query, summary embeddings, retrieval summary refs
```

The scale E2E also validates the minimal AI-agent envelope used by Cursor-like
integrations: message, resource, and feedback inputs with `messages`, `scope`,
`metadata`, and optional hook evidence. MatrixArk always extracts and
canonicalizes those envelopes; the feedback case carries a previous context-pack
reference so short confirmation can be classified. Those envelopes are captured
through hook metadata in the E2E, covering
`before_llm`, `resource_added`, and `after_llm`.

- `first_write_only` idempotent event writes
- `current_valid_only` filtering for expired and future events
- child ref write and child listing
- embedding upsert, validation, and query by reference
- tree traversal with bounded child scoring
- L0/L1 summary write and as-of query
- temporal compression write and query
- entity upsert, get, and query

For the entity extraction/update workflow and detailed `ContextChildRef`
traversal behavior, see:

```text
docs/CONTEXT_ENTITY_AND_CHILDREF_WORKFLOW.md
```

For temporal compression behavior and local debugging, see:

```text
docs/CONTEXT_COMPRESSION_WORKFLOW.md
```

For the minimal AI-agent caller format, see:

```text
src/extension/context/AI_AGENT_CONTEXT_ENVELOPE.md
```

## Fast Static Checks

Run:

```bash
cd <workspace>/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix
git diff --check -- src/extension/context/test.cc src/extension/context/implement.cc src/extension/context/interface.proto
```

## Reused Ubuntu 22 Build Environment

Use the local dependency cache rather than downloading or editing third-party sources.

```bash
cd <workspace>/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix

env \
  BYTESTORE_COMPAT_INCLUDE_DIR="<workspace>/Codex/2026-06-06/set-up-wsl-with-ubuntu-2022/work/cmake-glue/compat-include" \
  BRPC_STATIC_LIBRARY="<repo>-main-no-deps/build-ubuntu22/release/_open_source_brpc/output/lib/libbrpc.a" \
  EXTRA_CMAKE_ARGS="-DTEMPORALSTORE_USE_ROOT_CMAKE_GLUE=ON" \
  BUILD_TARGETS=context_module \
  BUILD_TYPE=Release \
  JOBS=2 \
  ./tools/build_ubuntu22.sh
```

To generate and run the context mini-cluster test target, enable tests:

```bash
env \
  BYTESTORE_COMPAT_INCLUDE_DIR="<workspace>/Codex/2026-06-06/set-up-wsl-with-ubuntu-2022/work/cmake-glue/compat-include" \
  BRPC_STATIC_LIBRARY="<repo>-main-no-deps/build-ubuntu22/release/_open_source_brpc/output/lib/libbrpc.a" \
  EXTRA_CMAKE_ARGS="-DTEMPORALSTORE_USE_ROOT_CMAKE_GLUE=ON" \
  BCACHE2_BUILD_TESTS=ON \
  BUILD_TARGETS=context_module_test \
  BUILD_TYPE=Release \
  JOBS=2 \
  ./tools/build_ubuntu22.sh
```

If the binary is produced:

```bash
./build-ubuntu22/test-release/src/extension/context/context_module_test --gtest_brief=1
```

Expected current result:

```text
[==========] 7 tests from 1 test suite ran.
[  PASSED  ] 7 tests.
```

## Unified And UI Checks

Run these before pushing context runtime or UI changes:

```bash
python3 -m json.tool sdk/unified/temporalstore_unified_corpus.json >/tmp/temporalstore_unified_corpus.json
python3 tools/run_temporalstore_unified_tests.py --validate-only
bash tools/run_cpp_unified_context_contract.sh sdk/unified/temporalstore_unified_corpus.json
python3 -m json.tool tools/temporalstore-monitoring-ui/health.json >/tmp/temporalstore_ui_health.json
node --check tools/temporalstore-monitoring-ui/app.js
python3 -m unittest tools.test_monitoring_ui_context_ops tools.test_render_health_from_results
```

The monitoring UI has dedicated `End-to-End Parity` and `UI Production Readiness`
sections. Keep both sections green when changing extraction, ingestion, retrieval,
resource parsing, compression, or context-pack replay behavior.

## Local Extraction/Ingestion/Query Scale E2E

Use this local scale gate when changing the MatrixArk-style context pipeline,
especially extraction, API ingestion, batch ingestion, stream ingestion, query
planning, resource retrieval, feedback memory, or token-budget packing.

The runner generates a temporary unified corpus and executes the same case
through both the C++ unified context contract and the Rust unified mock proxy
test. It does not require Docker or a running TemporalStore service.

By default, the runner requests open-source models:

```text
query/summary embedding encoder: sentence-transformers/all-MiniLM-L6-v2
resource VLM model: Salesforce/blip-image-captioning-base
```

If `sentence-transformers`, `transformers`, `torch`, and Pillow are installed, the
runner uses the local OSS encoder for query vectors, leaf L0 summary embeddings,
and resource summary embeddings. If they are not installed, it records
`effective_model_provider=open_source_fallback` and uses a deterministic local
embedding fallback so the unified C++/Rust contract can still run in minimal CI.

Install local OSS model dependencies when validating the model path:

```bash
python3 -m pip install -r tools/context_oss_models_requirements.txt
```

Then require real local models:

```bash
python3 tools/run_context_pipeline_scale_e2e.py \
  --events-per-lane 50 \
  --require-models
```

If direct Hugging Face access is blocked, download the embedding model from
ModelScope into the repo-local ignored cache, then run the Docker OSS-model gate:

```bash
python3 -m pip install --user modelscope

python3 tools/download_context_oss_models.py \
  --source modelscope \
  --skip-vlm

EVENTS_PER_LANE=5 tools/run_context_pipeline_docker_oss_models.sh
```

The Docker runner mounts `.local/context-oss-models` read-only and passes the
local model path to `--embedding-model`, so the E2E does not need to reach
`huggingface.co` at runtime. The current E2E only requires VLM packages for
readiness; it does not load VLM weights unless resource-image tests are added.

```bash
cd <workspace>/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix

python3 tools/run_context_pipeline_scale_e2e.py \
  --events-per-lane 500 \
  --write-results /tmp/context_pipeline_scale_e2e_500.json
```

The generated case covers:

- API ingest with duplicate idempotency-key replay.
- batch ingest of approval events.
- stream ingest of incident events with duplicate offset replay.
- OSS-provider query, collection L0, leaf L0, and resource summary embeddings.
- model-produced query understanding plans from raw query plus hints.
- scope, time-window, and filter planning before TemporalStore traversal.
- TemporalStore embedding writes for collection, leaf, and resource nodes.
- layer-wise vector traversal from root to collection to leaf with global top-k.
- time/filter query over ingested approval events.
- time/filter query over stream-ingested incident events.
- algorithmic staleness scoring after event/resource retrieval.
- retrieval from the context tree under `max_prompt_tokens`.
- Markdown-style resource chunk ingestion and resource retrieval.
- resource-derived event extraction that can update `ContextEntity`.
- feedback ingestion after a model answer that can update `ContextEntity`.
- AI-agent message, resource, and feedback envelopes validated before C++ and
  Rust execution.
- Hook-captured agent events with source, hook id, observed time, trigger, and
  idempotency key.
- always-on extraction for raw message/resource ingestion, plus confirmation
  feedback with a prior ContextPack reference.
- async-style `ContextSummary` refresh after API, batch, stream, resource, and
  feedback lanes.
- summary embedding storage and retrieval through TemporalStore `ContextEmbedding`.
- non-destructive `ContextCompressionEvent` writes for older approval and
  incident windows.
- compression queries that return compression ids and source event ids for replay.
- second-turn query that uses both events and resource evidence.

Observed deterministic 50-event debug result on this branch:

```json
{
  "status": "passed",
  "corpus": "temporalstore_context_pipeline_scale_e2e",
  "case": "context_pipeline_scale_e2e",
  "expected": {
    "events_per_lane": 50,
    "requested_model_provider": "deterministic",
    "effective_model_provider": "deterministic",
    "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
    "summary_embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
    "vlm_model": "Salesforce/blip-image-captioning-base",
    "embedding_backend": "deterministic-local-fallback",
    "vlm_backend": "metadata-only-vlm-fallback",
    "embedding_dim": 16,
    "api_events": 1,
    "batch_events": 50,
    "stream_events": 50,
    "resource_chunks": 1,
    "resource_extracted_events": 1,
    "entity_records": 2,
    "summary_records": 7,
    "summary_embedding_refs": 6,
    "summary_refs_in_context_packs": 5,
    "compression_records": 2,
    "compression_source_event_refs": 103,
    "feedback_events": 1,
    "agent_envelopes": 3,
    "agent_envelope_kinds": [
      "message",
      "resource",
      "feedback"
    ],
    "hook_captured_envelopes": 3,
    "hook_types": [
      "before_llm",
      "resource_added",
      "after_llm"
    ],
    "agent_always_extract_envelopes": 3,
    "confirmation_requires_context": true,
    "total_expected_events": 103,
    "tree_shape": "root/collection/leaf",
    "layer_traversal": "global_topk_per_depth",
    "query_understanding": "model",
    "staleness_scoring": "algorithmic_freshness_v1",
    "token_budgeting": "max_prompt_tokens",
    "pipeline": [
      "raw_query_plus_hints",
      "model_query_understanding",
      "scope_time_filter_planning",
      "context_node_traversal",
      "context_event_resource_retrieval",
      "algorithmic_staleness_scoring",
      "token_budgeting",
      "context_pack"
    ],
    "steps": 63
  },
  "timings": {
    "cpp_schema_contract_s": 2.9775275780120865,
    "rust_unified_mock_s": 0.8856430830201134
  },
  "rust_executed": true
}
```

For faster pre-commit validation, run:

```bash
python3 tools/run_context_pipeline_scale_e2e.py --events-per-lane 50
```

For local Docker scale testing with cached OSS models and preserved debug
artifacts, see `docs/CONTEXT_LOCAL_DOCKER_SCALE_TESTING.md`.

Observed Docker result with cached OSS model:

```json
{
  "status": "passed",
  "corpus": "temporalstore_context_pipeline_scale_e2e",
  "case": "context_pipeline_scale_e2e",
  "expected": {
    "events_per_lane": 5,
    "effective_model_provider": "open_source",
    "embedding_model": "/models/sentence-transformers/all-MiniLM-L6-v2",
    "embedding_backend": "sentence-transformers",
    "vlm_backend": "transformers-vlm-ready",
    "embedding_dim": 384,
    "resource_extracted_events": 1,
    "entity_records": 2,
    "summary_records": 7,
    "summary_embedding_refs": 6,
    "summary_refs_in_context_packs": 6,
    "compression_records": 2,
    "compression_source_event_refs": 13,
    "agent_envelopes": 3,
    "agent_envelope_kinds": [
      "message",
      "resource",
      "feedback"
    ],
    "hook_captured_envelopes": 3,
    "hook_types": [
      "before_llm",
      "resource_added",
      "after_llm"
    ],
    "agent_always_extract_envelopes": 3,
    "confirmation_requires_context": true,
    "total_expected_events": 13,
    "query_understanding": "model",
    "staleness_scoring": "algorithmic_freshness_v1",
    "token_budgeting": "max_prompt_tokens",
    "steps": 63
  },
  "rust_executed": true
}
```

Current 50-event result:

```text
C++ unified context contract passed: cases=1 context_steps=63
Rust unified corpus proxy contract: 1 passed
agent_envelopes=3
agent_envelope_kinds=message/resource/feedback
hook_captured_envelopes=3
hook_types=before_llm/resource_added/after_llm
agent_always_extract_envelopes=3
total_expected_events=103
summary_records=7
summary_embedding_refs=6
summary_refs_in_context_packs=5
compression_records=2
compression_source_event_refs=103
requested_model_provider=deterministic
effective_model_provider=deterministic
tree_shape=root/collection/leaf
layer_traversal=global_topk_per_depth
```

Current agent-envelope smoke result:

```text
python3 tools/run_context_pipeline_scale_e2e.py --events-per-lane 5 \
  --write-results /tmp/context_pipeline_scale_e2e_agent.json

C++ unified context contract passed: cases=1 context_steps=63
Rust unified corpus proxy contract: 1 passed
agent_envelopes=3
agent_envelope_kinds=message/resource/feedback
hook_captured_envelopes=3
hook_types=before_llm/resource_added/after_llm
agent_always_extract_envelopes=3
total_expected_events=13
summary_records=7
compression_records=2
```
