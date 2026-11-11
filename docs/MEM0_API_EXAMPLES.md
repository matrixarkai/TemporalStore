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

## Explicit Memories

Set `infer:false` on add when the supplied text is already a memory. This skips summary
and entity inference while retaining the selected embedding model, durable context records,
indexes, history, and retrieval. System-role turns are skipped; text whitespace, message role,
and optional actor name are preserved. Without the flag, add retains its inferred path.
Memory HTTP bodies are bounded to 16 MiB and add accepts at most 1,000 messages per call.

```bash
curl -sS http://127.0.0.1:17000/memories -H 'Content-Type: application/json' -d '{
  "infer": false,
  "user_id": "alice",
  "messages": [{"role":"user","content":"Use durable local storage."}],
  "provider": {"provider_kind":"ollama","mock_mode":false,
    "base_url":"http://127.0.0.1:11434","embedding_model":"all-minilm"}
}'
```

Single and batch updates encode the supplied memory directly without summary-model calls.
Metadata-only updates skip all model calls and context writes, retain the current context
revision and embedding, and update the catalog, history, and list ordering. They remain
available during an encoder outage; in a mixed batch, a content-encoding failure does not
prevent independent metadata-only entries from committing.
The Python client accepts `memory.update(id, metadata={"category":"storage"})` without
supplying text, or `memory.update(id, text="New memory text")` to replace content.
Pass `--metadata-updates 200` to `scripts/mem0_scale_local.sh` to measure repeated
single and 16-memory batch metadata updates separately. The driver verifies that the
sampled memory retains its text and context revision and exposes the latest metadata.
Footprint snapshots after ingestion and before/after metadata updates separate corpus
storage from update growth. The final `storage_amplification` includes all mutations and
retained history in the run, not just ingestion bytes.
Use the same encoder configuration for add, update, and search. Explicit ingestion and updates
use a versioned protobuf proxy-to-data-node request and acknowledgment. An unsupported endpoint,
invalid acknowledgment, or incomplete source commit fails without falling back to inference.
The Mem0-compatible HTTP envelopes remain JSON; no JSON encoding is nested inside this new
backend request. Existing inferred context routes are unchanged.
The explicit endpoint validates source counts before protobuf message allocation and retains
only one 64-source extraction-report window while building its compact acknowledgment.

For the Python client, use `Memory().add(text, user_id="alice", infer=False, provider=config)`.
Explicit-memory throughput is a separate workload mode, not an inferred-extraction result:

```bash
bash scripts/mem0_scale_local.sh --infer false --records 100000 --batch-size 128 \
  --tenant-shards 1 --users 2 --searches 200 --payload-bytes 512
```

The report includes its `infer` mode. Progress counters describe benchmark acknowledgments,
not ingested message contents. The serving path adds no external ingestion log.

### Index Log Footprint

Index-log compression remains opt-in. Set `TS_INDEX_LOG_COMPRESSION_ENABLED=1` to use the
existing zstd codec. `TS_INDEX_LOG_COMPRESSION_LEVEL=1` prioritizes encode CPU; level 3
remains the default. The supported level range is 1 through 22.
`TS_INDEX_LOG_COMPRESSION_MIN_BYTES=16384` keeps compression off records smaller than
16 KiB; without an override the existing 256-byte threshold is retained. A compressed
record is used only when it is smaller. WAL durability and index-log contents are unchanged.
`TS_INDEX_LOG_COMPRESSION_ENABLED=0` disables compression for future writes. Binaries used for rollback
must understand the existing compressed codec: disabling compression does not convert
records already on disk. This controls durable bytes, not the batch algorithm's candidate
count or working-set size.

## Model Selection On Updates

Single-memory updates accept the same optional `provider` object as add and search. Batch
updates accept a top-level `provider`; individual entries can override it. Entries using
different providers are extracted in separate groups, so one encoder configuration cannot
be applied to another entry accidentally. A null entry-level provider inherits the batch
provider. Omitting provider selection keeps the server default.

When selecting an Ollama model per request, send the same provider configuration on add,
update, and search. Updates do not infer or persist credentials or model settings in the
memory catalog. The scale driver forwards its configured provider to both update routes.

Batch extraction uses the configured transport retry budget. Once a model transport fails,
it stops the remaining chunks of that invocation instead of issuing a new model request for
every source. Valid partial responses still retry only missing sources; malformed content
can still recover through single-source extraction. An explicitly configured fallback
provider remains available. This bounds failure amplification without replacing failed
extraction with an implicit mock result.

Ollama extraction uses closed output schemas: L0 is limited to 220 characters, L1 to
1,200 characters, and each source has at most eight entities. Entity names and values
also have generation limits; normalization retains the existing 256-byte name and
1,024-byte value limits without splitting UTF-8. Each batch schema is sized to its actual
source window and source ordinals. The model is instructed to put durable facts in entities
as well as summaries and use an empty entity array only for sources without durable information.
Batch items that copy another source's complete text into a summary or entity are retried
individually when the copied text is at least 32 bytes and is absent from their own source.
This guard detects verbatim leakage, not every possible semantic attribution error.

An opt-in Rust model check exercises both single-source and batched extraction without
writing memories. It requires an already installed Ollama chat model:

```bash
TS_TEST_OLLAMA_CHAT_MODEL=qwen2.5:3b cargo test -p temporalstore-rust --lib \
  live_ollama_summary_keeps_durable_facts_in_entities -- --ignored --nocapture
```

The check uses a 30-second request deadline. A cold model load can consume that budget;
successful warm-model extraction is not evidence that cold-start or full Mem0 flows pass.

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

## Index Maintenance

Generic batched hash writes retain evidence that the writer filed the applied fields. They
skip duplicate post-write filing while preserving the existing per-field layout, WAL payloads,
and storage mode. This does not pack small catalog records into larger read units.

Tombstone reconciliation uses sorted temporary addresses rather than a tree. The allocation
probe for 8,192 deleted blocks measured 608,144 allocation bytes and 759 calls for the tree,
versus 196,608 bytes and 12 calls for the compact lookup. These are helper allocation
measurements, not process RSS or endpoint latency. Reproduce them with:

```bash
cargo test -p temporalstore-rust --features alloc-probe --lib \
  tombstone_reconcile_cost_tests -- --test-threads=1 --nocapture
```

## Filtered listing

Filtered `/memories/list` calls keep the initial ordered-page fast path. If that page cannot
fill the result limit, selective queries can use existing identity postings instead of walking
the rest of the tenant catalog. The indexed attempt hydrates at most 512 candidates per page
and 4,096 in total, retaining only the requested newest results. Timestamp ties retain the
existing memory-id ordering. Dense queries need no identity lookup when the first page suffices.
Unavailable counts, incomplete or malformed postings, and exceeded candidate budgets fall back
to the ordered scan. This adds no persisted index or migration.

## Identity-scoped search

Selective `/memories/search` requests use existing identity postings to find their node candidates
before context retrieval. The indexed attempt uses the same 512-candidate page and 4,096-candidate
total limits as filtered listing. The data server's `/context/retrieve/nodes` route treats its node
set as a strict constraint: an empty set remains empty, and unrelated hot nodes are not added.
Catalog rows already read for identity validation are reused when rendering results. Empty scoped
queries skip query embedding entirely. Searches do not initiate an index rebuild.
Mem0 IDs are resolved from these catalog rows and their current node hashes, not from deprecated
source fields omitted by compact persisted nodes. This keeps the IDs usable by get/update/delete
without restoring redundant hot lineage fields. Ordinary
context retrieval retains automatic tenant candidate selection. Unavailable or incomplete indexes
and over-budget identities
retain the existing tenant-search fallback, so this does not yet make large identity populations
an identity-filtered vector search.

Upgrade data servers to support `/context/retrieve/nodes` before upgrading proxies that use it.
Search results must resolve to a visible, current Mem0 catalog record; anonymous context blocks
are not returned as memories. With compact legacy data and unavailable identity candidates, the
tenant-search fallback may underfill when compact data has not had its node lookup backfilled. Generic context
blocks remain available through the context API. The scale harness rejects empty and duplicate
memory IDs rather than counting anonymous blocks as successful memory search candidates.

## Compact Node Lookup

New Mem0 writes maintain a durable node-to-memory hash. Each entry has a 16-character node hash
and an eight-byte memory-ID suffix (24 payload bytes, excluding storage and wire overhead).
The tenant prefix comes from the request scope. Updates replace the old node mapping; deletes
and reset remove it. Catalog identity, tombstones, and current node hashes still validate every
lookup result, so an old revision cannot return newly updated text.

Unscoped searches and identities above the candidate budget resolve only the nodes selected by
context retrieval. Their catalog hydration is bounded by that result set, not the tenant corpus.
This lookup does not make tenant-wide vector selection identity-aware: very selective identities
above the scoped budget can still underfill and need a filtered vector-selection path.

Existing stores can backfill without putting a full catalog scan on the search path:

```bash
curl -s http://127.0.0.1:17000/memories/indexes/rebuild \
  -H 'content-type: application/json' \
  -d '{"scope":{"account_id":"account","tenant_id":"tenant"},"after":"","max_pages":1}'
```

Send the returned `next` string as `after` until `complete` is true. Each page reads at most 512
ordered entries; a request defaults to one page and accepts at most 64. Cursor retries overwrite
the same mappings. Writes are split into at most 128 mappings per backend call. Backfill validates
response shapes and backend status before advancing the
cursor. An uncertain write outcome can retry the identical map overwrite at most twice, with
bounded backoff; `write_retries` reports how many retries occurred. Non-idempotent commands are
not part of this retry path. The ordered index must already be ready; otherwise the route returns 409. Current Mem0
stores create that index during normal ingestion. New writes maintain mappings during backfill;
the lookup is persisted through TemporalStore's normal WAL and storage, with no external log.

The local scale runner accepts `--skip-build --bin-dir /absolute/path/to/binaries` to use a
captured set of `metaserver`, `server`, `proxy`, and `mem0_scale_harness` executables. This avoids
switching binaries halfway through a run when other builds share the normal release directory.
The runner records the SHA-256 of each executable in `binaries.sha256`.

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
