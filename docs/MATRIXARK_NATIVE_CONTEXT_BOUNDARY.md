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
