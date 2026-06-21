# Context Ingestion, Extraction, And Retrieval Manual

This manual shows how to run the Rust-native context pipeline locally. It covers the fastest
validation path, the HTTP server path, and copy-paste JSON for ingestion, extraction, retrieval, and
prompt injection.

The workflow is:

1. Ingest raw sources.
2. Extract Context models: `ContextNode`, `ContextEvent`, `ContextIndexRef`, and dirty-summary
   markers.
3. Retrieve ranked L0/L1/L2 context blocks.
4. Optionally inject selected blocks into a prompt and audit the selected refs.

## Prerequisites

Run commands from the repository root:

```bash
cd /mnt/c/Users/Deeproute/Documents/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore
```

Useful tools:

- `cargo`
- `curl`
- `jq`

The local workflow defaults to the mock OpenAI-compatible provider, so no model key is required.

## Fast Local Validation

Use the harness when you only need to prove the full local workflow works:

```bash
tools/run_context_workflow_local.sh
```

The script runs:

- `context_workflow_harness`
- JSON validation through `tools/validate_aws_validation_log.py`
- focused context workflow unit tests

The harness output should include:

- `context_pipeline_ready: true`
- `ingest_extract_ready: true`
- `retrieve_pipeline_ready: true`
- `benchmark_ready: true`
- `external_benchmark_ready: true`

## Start The HTTP Server

Use a dedicated data directory so repeated manual runs do not mix with other tests:

```bash
export TS_SERVER_BIND_ADDR=127.0.0.1:17002
export TS_SERVER_ADDR=127.0.0.1:17002
export TS_META_ADDR=127.0.0.1:17001
export TS_SHARD_ID=1
export TS_CACHE_DIR=/tmp/temporalstore-context-manual/cache
export TS_PAGE_STORE_DIR=/tmp/temporalstore-context-manual/pages
export TS_INDEX_DIR=/tmp/temporalstore-context-manual/indexes

cargo run -p temporalstore-rust --bin server
```

The server attempts metaserver registration by default. For this manual context flow, registration
failure is not fatal as long as the server prints:

```text
temporalstore server listening on 127.0.0.1:17002
```

In another shell:

```bash
export TS_URL=http://127.0.0.1:17002
```

Check management and provider state:

```bash
curl -s "$TS_URL/context/manage" | jq .
curl -s "$TS_URL/context/workflow/state" | jq .
curl -s "$TS_URL/context/model/providers" | jq .
```

## Request Fields

Common fields:

- `shard_id`: loaded shard id. Use `1` for the default local server.
- `tenant_hash`: numeric tenant id for context isolation.
- `source_kind`: one of `document`, `chat`, `ticket`, `code`, `incident`, `user_event`.
- `timestamp_ms`: event timestamp in milliseconds.
- `tiers`: optional retrieval tiers, usually `["l0", "l1", "l2"]`.
- `provider`: optional model provider config. Omit it for deterministic mock mode.

## Extract One Source

`/context/extract` extracts one source and returns the generated node hash and event URI.

```bash
cat >/tmp/context-extract.json <<'JSON'
{
  "shard_id": 1,
  "tenant_hash": 20260619,
  "source_kind": "chat",
  "source_id": "chat-alice-office-1",
  "title": "Alice office update",
  "body": "Latest conversation: Alice replaced her office preference with the downtown location after the billing issue was resolved.",
  "timestamp_ms": 1000
}
JSON

curl -s -X POST "$TS_URL/context/extract" \
  -H 'content-type: application/json' \
  --data @/tmp/context-extract.json | tee /tmp/context-extract-response.json | jq .
```

Capture the node hash:

```bash
export NODE_HASH="$(jq -r '.node.node_hash' /tmp/context-extract-response.json)"
echo "$NODE_HASH"
```

## Ingest And Extract A Batch

`/context/ingest_extract` accepts multiple sources, extracts each source, and returns a
ready-to-run `retrieve_request`.

```bash
cat >/tmp/context-ingest-extract.json <<'JSON'
{
  "shard_id": 1,
  "tenant_hash": 20260619,
  "query": "Where does Alice want to work now?",
  "start_time_ms": 0,
  "end_time_ms": 10000,
  "max_events": 16,
  "sources": [
    {
      "shard_id": 1,
      "tenant_hash": 20260619,
      "source_kind": "chat",
      "source_id": "alice-old-office",
      "title": "Earlier office preference",
      "body": "Earlier memory: Alice preferred the airport office before the later change.",
      "timestamp_ms": 1000
    },
    {
      "shard_id": 1,
      "tenant_hash": 20260619,
      "source_kind": "chat",
      "source_id": "alice-current-office",
      "title": "Current office preference",
      "body": "Latest update: Alice now wants the downtown location as her office preference after the payment issue was resolved.",
      "timestamp_ms": 2000
    },
    {
      "shard_id": 1,
      "tenant_hash": 20260619,
      "source_kind": "ticket",
      "source_id": "alice-support-ticket",
      "title": "Support follow-up",
      "body": "Support follow-up: the billing issue was resolved and the agent recorded Alice's current workplace preference.",
      "timestamp_ms": 3000
    }
  ]
}
JSON

curl -s -X POST "$TS_URL/context/ingest_extract" \
  -H 'content-type: application/json' \
  --data @/tmp/context-ingest-extract.json | tee /tmp/context-ingest-extract-response.json | jq .
```

Check that ingestion succeeded:

```bash
jq '{ok: .status.ok, accepted, failed, node_hashes, retrieve_request}' \
  /tmp/context-ingest-extract-response.json
```

## Retrieve Context

Reuse the `retrieve_request` returned by batch ingest:

```bash
jq '.retrieve_request' /tmp/context-ingest-extract-response.json \
  >/tmp/context-retrieve.json

curl -s -X POST "$TS_URL/context/retrieve" \
  -H 'content-type: application/json' \
  --data @/tmp/context-retrieve.json | tee /tmp/context-retrieve-response.json | jq .
```

Inspect ranked context blocks:

```bash
jq '.blocks[] | {tier, event_time_ms, text}' /tmp/context-retrieve-response.json
```

To write a manual retrieval request, provide `node_hashes` explicitly:

```bash
cat >/tmp/context-retrieve-manual.json <<JSON
{
  "shard_id": 1,
  "tenant_hash": 20260619,
  "node_hashes": [$NODE_HASH],
  "query": "Where does Alice want to work now?",
  "start_time_ms": 0,
  "end_time_ms": 10000,
  "max_events": 16,
  "min_confidence": 0.0,
  "min_importance": 0.0,
  "tiers": ["l0", "l1", "l2"]
}
JSON
```

## Inject Context Into A Prompt

`/context/inject` runs retrieval and packs selected blocks into a prompt under a token budget.

```bash
jq '{
  retrieve: .retrieve_request,
  prompt: "Answer using only the supplied TemporalStore context: Where does Alice want to work now?",
  session_hash: 9001,
  query_id: "manual-alice-office",
  max_prompt_tokens: 256
}' /tmp/context-ingest-extract-response.json >/tmp/context-inject.json

curl -s -X POST "$TS_URL/context/inject" \
  -H 'content-type: application/json' \
  --data @/tmp/context-inject.json | tee /tmp/context-inject-response.json | jq .
```

Inspect selected refs and the final prompt:

```bash
jq '{selected: [.selected_blocks[] | {tier, uri}], prompt: .injected_prompt}' \
  /tmp/context-inject-response.json
```

## Run With A Live OpenAI-Compatible Provider

Mock mode is the default. To use a live OpenAI-compatible endpoint in a request, set an environment
variable for the API key and include a provider object:

```bash
export CONTEXT_API_KEY='replace-with-real-key'
```

```json
{
  "provider": {
    "provider_name": "local-openai-compatible",
    "provider_kind": "open_ai_compatible",
    "base_url": "http://127.0.0.1:8080/v1",
    "api_key_env": "CONTEXT_API_KEY",
    "model": "context-chat-model",
    "embedding_model": "context-embedding-model",
    "vlm_model": "context-vlm-model",
    "timeout_ms": 30000,
    "max_retries": 2,
    "mock_mode": false
  }
}
```

The provider is request-scoped for extraction/injection. Do not put raw credentials in request JSON;
only set `api_key_env`.

For an OpenViking-style open-source VLM deployment, point the same OpenAI-compatible shape at a
local gateway such as Ollama or vLLM:

```json
{
  "provider": {
    "provider_name": "openviking-open-source-vlm",
    "provider_kind": "open_ai_compatible",
    "base_url": "http://127.0.0.1:11434/v1",
    "api_key_env": "OPENVIKING_MODEL_API_KEY",
    "model": "qwen2.5:7b-instruct",
    "embedding_model": "nomic-embed-text",
    "vlm_model": "qwen2.5vl:7b",
    "timeout_ms": 30000,
    "max_retries": 2,
    "mock_mode": false
  }
}
```

`GET /context/workflow/state` also reports ready-to-use open-source profiles for
`qwen2.5vl:7b`, `llava:7b`, `OpenGVLab/InternVL2_5-8B`, and the GPT-style
`Vision-CAIR/MiniGPT-4` profile. Use `mock_mode=true` with the same profile names for deterministic
Docker validation when a live VLM server is not running.

To match the open-source model setup used by the C++/MatrixArk LOCOMO path in the "LLM Specific
TemporalStore Use Cases" thread, use `matrixark-cpp-oss-context`:

```json
{
  "provider": {
    "provider_name": "matrixark-cpp-oss-context",
    "provider_kind": "open_ai_compatible",
    "base_url": "http://127.0.0.1:8000/v1",
    "api_key_env": "MATRIXARK_MODEL_API_KEY",
    "model": "google/flan-t5-small",
    "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
    "vlm_model": "none",
    "timeout_ms": 30000,
    "max_retries": 2,
    "mock_mode": false
  }
}
```

For an open-source GPT-4-style VLM profile:

```json
{
  "provider": {
    "provider_name": "openviking-open-source-gpt-vlm",
    "provider_kind": "open_ai_compatible",
    "base_url": "http://127.0.0.1:8000/v1",
    "api_key_env": "OPENVIKING_MODEL_API_KEY",
    "model": "lmsys/vicuna-7b-v1.5",
    "embedding_model": "BAAI/bge-m3",
    "vlm_model": "Vision-CAIR/MiniGPT-4",
    "timeout_ms": 30000,
    "max_retries": 2,
    "mock_mode": false
  }
}
```

## Run External LOCOMO / LongMemEval-Style Replay

To replay external benchmark-style JSONL:

```bash
export TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL=/path/to/context-benchmark.jsonl
cargo run -p temporalstore-rust --bin context_workflow_harness \
  | tee /tmp/context-workflow-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-context-workflow-validation \
  --log /tmp/context-workflow-validation.log
```

Each JSONL record can use `query` or `question`, `answer_terms` or `expected_terms`, optional
`category`, `reasoning_type`, or `question_type`, and `sources`, `messages`, or `conversation`
arrays. Category labels are reported in `external_benchmark_category_breakdown`, and the harness
fails `external_benchmark_ready` unless every category has hit@k 1.0, zero missing expected terms,
and zero zero-hit queries. MRR remains reported as a quality metric, not a readiness blocker.
All `answer_terms` / `expected_terms` are treated as required evidence terms; the run also fails
unless `external_benchmark_answer_term_coverage` is 1.0 and
`external_benchmark_missing_expected_terms` is 0.
For the built-in VikingMem-style sweep, `benchmark_evidence_retention_at_k` and
`benchmark_sweep_min_evidence_retention_at_k` must also stay at 1.0, which means each expected
answer/topic remains in the selected injected context within the configured prompt-token budget.

```json
{"dataset":"locomo","query_id":"q1","category":"memory_update","query":"Where does Alice want to work now?","answer_terms":["downtown location"],"messages":[{"kind":"chat","title":"Old memory","text":"Alice wanted the airport office before the later update."},{"kind":"chat","title":"Latest memory","text":"Alice now wants the downtown location as her office preference."}]}
```

## Troubleshooting

- `node_hash_required`: retrieval needs at least one `node_hash`. Use `/context/ingest_extract` and
  reuse `.retrieve_request`, or capture `.node.node_hash` from `/context/extract`.
- Empty `blocks`: check `start_time_ms`, `end_time_ms`, `max_events`, and `tiers`.
- Bad enum error: use snake_case enum values such as `chat`, `ticket`, `incident`, `user_event`,
  `l0`, `l1`, and `l2`.
- Live provider failures: verify `base_url`, `api_key_env`, `mock_mode=false`, and the endpoint's
  `/chat/completions` compatibility.
- Server cannot register with metaserver: for this local context-only manual, the server can still
  serve context routes if it is listening on `TS_SERVER_BIND_ADDR`.
