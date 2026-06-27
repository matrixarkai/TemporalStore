# MatrixArk Native C++/Rust Context Boundary

MatrixArk keeps the Python layer for MCP, agent glue, model providers, resource parsing, PDF/VLM tooling, and benchmark scripts. The native TemporalStore implementations own the storage-facing context contract.

## Native Responsibilities

Both C++ and Rust should implement the same behavior behind the shared test corpus:

- Adapter contract and canonical record encoding.
- Batch writes for high-throughput ingestion.
- Context tree traversal and bounded layer-by-layer scoring.
- Secondary-index filtering before expensive scoring.
- ContextPack assembly and audit buffering.
- Metrics-ready operation reports.
- Resource registry records: `ResourceManifest`, `ResourceChunk`, summaries, embeddings, indexes.
- Skill registry records: `SkillManifest`, `SkillSection`, status, precedence, permissions, selected skill instructions.

## Current Rust Surface

`sdk/rust/temporalstore/src/bin/matrixark_record_log.rs` supports the original string/hash operations plus MatrixArk-native record envelope operations:

- `batch_hset`
- `matrixark_append_records`
- `matrixark_batch_append_records`
- `write_matrixark_record`
- `write_matrixark_records`
- `read_matrixark_record`
- `read_matrixark_records`

Every command result includes `elapsed_ms` so MCP and benchmark callers can report native storage latency without measuring from Python only.

Records are stored with this canonical shape:

```text
key   = matrixark:record:{record_type}:{tenant_hash}
field = record_id | node_hash | event_id_hash | entity_hash | resource_hash |
        chunk_hash | skill_hash | section_hash | summary_hash | ref_hash |
        query_id_hash | compression_id_hash
value = JSON record
```

This lets the Python MCP server keep its model-facing code while Rust and C++ share the same persisted record identity.

## MatrixArk Batch Append Contract

The production hot path now has a named MatrixArk append boundary:

```json
{
  "op": "matrixark_batch_append_records",
  "entries": [
    {"key": "matrixark:mcp:records:000000", "field": "00000000000000000001", "value": "{...record bundle...}"},
    {"key": "matrixark:mcp:context_event_by_ingestion_time:123", "field": "1780000000000:456", "value": "{...context event...}"}
  ],
  "key": "matrixark:mcp:record_count",
  "value": "42"
}
```

Python still owns MCP envelopes, extraction, parsing, and record materialization.
After materialization, Python sends one batch to TemporalStore. The native
backend handles routing, sync/async storage behavior, oplog/persistence, and
backpressure. The existing compact record-log layout remains compatible because
the batch contains the same sharded record fields plus the optional count update.

Backend status:

- Rust long-lived gateway: implements `matrixark_append_records` and
  `matrixark_batch_append_records` as first-class ops.
- C++ direct SDK: exposes `temporalstore_matrixark_batch_append_records` in the
  C ABI, and the Python SDK prefers that symbol when the loaded library provides
  it. The current C++ implementation batches at the MatrixArk API boundary and
  lowers each record to TemporalStore hash writes; a deeper server-side
  multi-field append path remains the next storage-engine optimization.
- Python adapter: materializes records, then calls the native
  `matrixark_batch_append_records` boundary when available. Older libraries still
  fall back to `batch_hset` / `hset` so deployment upgrades are rolling-safe.

## Current C++ Shared Gate

`TemporalStoreTestCorpus/runners/cpp/cpp_unified_context_contract.cc` is the C++ contract runner. It now validates resource manifests, skill manifests, skill sections, token-budgeted skill retrieval, context summaries/embeddings, events, entities, indexes, compression, and ContextPack audit behavior against shared JSON cases.

## Validation

```bash
# C++ shared contract
cd /root/src/github-services/TemporalStoreTestCorpus
bash runners/cpp/run_cpp_unified_context_contract.sh

# Rust record envelope and batch command surface
cd /root/src/github-services/TemporalStore/sdk/rust/temporalstore
LD_LIBRARY_PATH=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib:$LD_LIBRARY_PATH \
  cargo test --bin matrixark_record_log --no-default-features --features direct
```

## Responsibility Split Check

The intended production split is now explicit in code:

- Python MCP: API envelopes, auth/access checks, model/extraction glue, resource
  parsing, request shaping, and benchmark orchestration.
- C++/Rust TemporalStore: append queue entry point, batch append boundary,
  WAL/oplog persistence, shared-store or Raft routing, prefix reads/scans,
  secondary-index filtering targets, cache/persistence/eviction behavior, and
  backend metrics.

C++ and Rust still differ in depth: Rust has a long-lived record gateway with
MatrixArk batch commands; C++ now has the C ABI batch boundary and should next
push the loop below `HSet` into a native append queue/batch-write engine path.
