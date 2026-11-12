# Inferred Memory Facts

The Rust Mem0-compatible API distinguishes two operations:

- `infer=false` stores explicit messages without calling the chat model.
- `infer=true` runs the existing context extraction and creates one memory for
  each extracted entity identity. It does not catalog the original conversation
  as an inferred memory. A source with no facts creates no memory rows.

For example, one source describing an editor and a language preference can
produce two memories, such as `editor: Vim editing` and `language: Rust services`,
each with its own ID, history, metadata and deletion behavior. These example
values are deterministic route-test output, not a model-quality result.

## Data Flow

The public Mem0 JSON request remains compatible with JSON clients. Internally,
the proxy sends a bounded protobuf source request to `/context/ingest_facts`.
The data service uses the common extraction pipeline and returns a protobuf
receipt containing source identity, status, node identity and fact text/hash.
The schema is in [the protocol file](../proto/temporalstore/v1/temporalstore.proto).

The receipt does not serialize node/event bodies, raw source text or vectors.
The original source and extracted context models remain in TemporalStore.
The adapter reuses existing entity retrieval and vectors, without a second
model call or embedding pass. Transient fact text is retained while extraction
results are projected; vectors are not cloned for the receipt. Legacy JSON
context reports do not gain the transient fact field.

The request is bounded to 1000 sources and a 16-MiB body. A receipt allows up to
eight facts per source, with names up to 256 bytes and values up to 1024 bytes.
The receiver counts repeated sources and facts before protobuf allocation.
It then checks source acknowledgments, source/node identity, entity identity,
field sizes and source cardinality. Missing, partial or inconsistent receipts
fail without falling back to raw-message storage. Provider failure codes are
preserved. The catalog is written in pages of at most 128 memories.

Projection does not clone the raw source body for each fact. Before backend or
model work, JSON-encoded metadata size multiplied by input count and maximum fact fanout
must fit 16 MiB (fanout one for explicit memories, eight for inference).
Requests exceeding that budget return a bad-request error. Metadata is still
stored on each memory; this bounds duplication rather than introducing a shared
metadata store.
The preflight uses a bounded serde writer instead of allocating a second
encoded metadata buffer.

Message normalization moves owned text, role and actor-name strings out of JSON
values. Inference trims Unicode whitespace in place; explicit messages preserve
their original bytes. When building an inferred extraction request, the adapter
moves each source body out of its temporary memory record. The temporary record
keeps its ID, identity, timestamps and metadata for later fact projection, but no
longer retains a duplicate raw body while the request is in flight.

This removes input-string copies, not every allocation: public JSON request
bodies, protobuf encoding, metadata, persisted context models and explicit-memory
catalog content still have their own storage. It does not change model work,
deadlines, retries, write ordering or history retention.

Facts sharing an entity identity within a source use the final persisted value.
Cross-source and cross-session reconciliation are not implemented by this change.
Separate source occurrences can still produce separate memory IDs.
Extraction retains the existing source-window policy; conversation-wide model
reasoning is not introduced by this adapter.

## Retrieval And Mutation

Fact records use a versioned value in the existing `context_ref` string; the
stored memory-record protobuf tags do not change. Lookup entries distinguish
entities belonging to the same source node. Legacy explicit-memory lookups
remain readable. Candidate node IDs are deduplicated before retrieval.

Scoped and unscoped search hydrate the matched fact, not another entity from
the same source. Metadata-only updates preserve that association. A content
update uses the existing encoder-only revision path; deleting one fact removes
its lookup rather than a sibling's lookup. Mem0 projection deletion does not
purge the original context source or its historical context models.

Catalog pages are not a single all-or-nothing transaction. Failed or unknown
writes can require metadata repair. Concurrent update/delete fencing and
historical garbage collection are not resolved here.

## Rollout

Deploy data services providing `/context/ingest_facts` first. Use a coordinated
proxy upgrade: older proxy readers do not understand entity-specific context
strings, so mixed-version fact writers are not supported. An updated proxy
fails closed against an older data service. Existing inferred rows containing
raw text are not automatically rewritten or migrated.

Configure a real model when using inference. The deterministic mock provider
extracts no entities. Use `infer=false` for explicit memories without a model.
No request deadline, retry setting or default model changes.

For a local Ollama service with these models already installed:

```bash
curl -sS http://127.0.0.1:17000/memories \
  -H 'Content-Type: application/json' \
  --data '{"messages":"Remember that I use Rust for services and Vim for editing.",
    "user_id":"alice","infer":true,
    "provider":{"provider_name":"local-ollama","provider_kind":"ollama",
      "base_url":"http://127.0.0.1:11434","model":"qwen2.5:3b",
      "embedding_model":"all-minilm:latest","mock_mode":false,
      "timeout_ms":30000,"max_retries":0}}'
```

Mock mode must be disabled to exercise a real model. These model names are an
example configuration, not a quality or latency guarantee.

## Validation And Measurement

```bash
cargo test -p temporalstore-rust --lib inferred_memory_ -- --test-threads=1
cargo test -p temporalstore-rust --lib memory_input_ -- --test-threads=1
cargo test -p temporalstore-rust --bin server context_explicit_route -- --test-threads=1
cargo test -p temporalstore-rust --bin mem0_scale_harness memory_add_receipt -- --test-threads=1
```

The benchmark report separates `acknowledged_source_records` (input messages)
from `ingested_records` (created memories). Inference can create zero or several
memories per source. An inferred fixture producing no memories cannot pass a
serving benchmark merely by exercising empty operations; it is marked incomplete.
The legacy `ingest_records_per_sec` rate counts created memory rows.

Generated-corpus route checks are not semantic recall or extraction-quality
measurements. Model throughput, total model CPU/RSS, distributed serving and
large-corpus performance require separate measurement. No new performance
improvement is claimed from functional tests alone.

Seven input-ownership checks cover text/object inputs, Unicode trimming, field
precedence, role filtering, empty inputs, protobuf round trips and fact projection
after the raw body is moved. In the eight-source, 32-KiB-per-source fixture, the
request reuses the original 256 KiB of body buffers while the temporary records
retain zero body capacity. Pointer/capacity checks establish that component-level
ownership property; they are not process RSS or end-to-end CPU measurements.
