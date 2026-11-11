# Memory API Examples

TemporalStore exposes a non-versioned memory API through the Rust proxy.

## Python

```bash
python3 -m pip install -e sdk/python
export TEMPORALSTORE_BASE_URL=http://127.0.0.1:17000
```

```python
from temporalstore.llm_context import Memory

memory = Memory()
added = memory.add(
    "Use compact embeddings and preserve the model hash.",
    user_id="alice",
    agent_id="assistant",
    run_id="session-42",
)

found = memory.search("How should vectors be stored?", user_id="alice", limit=8)
memory.update(added["results"][0]["id"], "Use int8 when quality remains acceptable.")
history = memory.history(added["results"][0]["id"])
```

Search results are ordered and carry `metadata.record_type` with one of
`matched_chunk`, `raw_message`, or `entity`.

## HTTP

```bash
curl -s http://127.0.0.1:17000/memories \
  -H 'content-type: application/json' \
  -d '{
    "messages": [{"role": "user", "content": "Prefer compact embeddings."}],
    "user_id": "alice",
    "agent_id": "assistant",
    "run_id": "session-42"
  }'

curl -s http://127.0.0.1:17000/memories/search \
  -H 'content-type: application/json' \
  -d '{"query":"vector storage","user_id":"alice","limit":8}'
```

Other routes:

| Method | Route |
| --- | --- |
| `POST` | `/memories/list` |
| `POST` | `/memories/users` |
| `GET` | `/memories/{id}` |
| `PUT` | `/memories/{id}` |
| `DELETE` | `/memories/{id}` |
| `GET` | `/memories/{id}/history` |
| `POST` | `/memories/{id}/feedback` |
| `POST` | `/memories/batch/update` |
| `POST` | `/memories/batch/delete` |
| `POST` | `/memories/delete_all` |
| `POST` | `/memories/reset` |

## Large-corpus profiling

Run an isolated local metadata, data, and proxy process from this repository:

```bash
bash scripts/mem0_scale_local.sh \
  --records 100000 --batch-size 512 --tenant-shards 16 \
  --users 1000 --searches 200 --payload-bytes 512
```

The runner builds release binaries, leaves normal 30-second storage maintenance enabled, and
writes `report.json`, checkout details, binary hashes, and three service logs under
`target/mem0-scale-local`. Corpus files live in a separate `data` directory, so reports and logs
are not counted as stored corpus bytes. Processes stop and corpus files are removed on exit;
`--keep-data` preserves them, and a later run refuses to overwrite an existing data directory.
Use `--work-dir` and `--port-base` to select another isolated location and port range.
`--skip-build` reuses existing binaries; checkout details alone do not identify their source,
so the runner also records binary hashes and whether the build was skipped.

`mem0_scale_harness` is a Rust workload driver for the complete memory API. It creates a unique
corpus, optionally partitions it across tenant scopes, ingests generated context through the
normal extraction path, runs retrieval and every management route, and writes latency, throughput,
process CPU/RSS, and optional on-disk amplification measurements.

```bash
cargo run --release -p temporalstore-rust --bin mem0_scale_harness -- \
  --addr 127.0.0.1:17000 \
  --records 1000000 \
  --batch-size 512 \
  --tenant-shards 128 \
  --users 10000 \
  --searches 1000 \
  --service-pids "$(pgrep -n proxy),$(pgrep -n server)" \
  --data-root /path/to/temporalstore-data \
  --out target/mem0-scale/report.json
```

Set `TS_MEM0_PROVIDER_JSON` or pass `--provider-json` to exercise a configured model provider.
Without it, the harness uses the deterministic local path so storage and serving costs can be
measured without model latency dominating the result.

Search targets rotate through ingested batches and use the tenant that received each target.
Missing, empty, or over-limit result arrays fail the workload instead of counting as successful
low-latency searches. This checks candidate availability and bounds, not semantic relevance or
exact-target recall. Per-route sample counts are included; management routes with one sample do
not establish a reliable tail-latency distribution. Storage bytes are measured after management
operations, not at the peak of ingestion.

## Filtered listing

Filtered `/memories/list` calls keep the initial ordered-page fast path. If that page cannot
fill the result limit, selective queries can use existing identity postings instead of walking
the rest of the tenant catalog. The indexed attempt hydrates at most 512 candidates per page
and 4,096 in total, retaining only the requested newest results. Timestamp ties retain the
existing memory-id ordering. Dense queries need no identity lookup when the first page suffices.
Unavailable counts, incomplete or malformed postings, and exceeded candidate budgets fall back
to the ordered scan. This adds no persisted index or migration.

## Local model allocation check

The embedding client validates the complete batch plan before making model calls. An invalid
tail therefore does not spend inference on earlier inputs in that call. Output assembly reuses
the first response buffer without reserving and discarding a second full output matrix.

Run the offline allocation and admission regressions with:

```bash
cargo test --release -p temporalstore-rust --test ollama_embedding_allocation -- --nocapture
```

For an embedding-only check against an already installed Ollama model:

```bash
TEMPORALSTORE_OLLAMA_EMBEDDING_MODEL=all-minilm \
  cargo test --release -p temporalstore-rust --test ollama_embedding_allocation \
  embedding_output_reserves_aggregate_storage_once_with_live_model -- --ignored --nocapture
```

This diagnostic writes no stored memories and does not change the serving model. Its counters
measure output-sized allocation requests on the caller thread, not process RSS, model quality,
or end-to-end extraction/retrieval performance.
