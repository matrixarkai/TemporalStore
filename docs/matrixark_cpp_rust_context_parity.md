# MatrixArk C++ / Rust Context Parity

## Policy

The C++ TemporalStore context model is the canonical serving implementation for
MatrixArk context storage. The Rust SDK must stay feature-aware and contract
compatible with the same context surface, even when Rust only reaches it through
proxy/direct SDK bindings.

Any new C++ context command, record type, or serving behavior must update the
shared corpus in:

```bash
sdk/unified/temporalstore_unified_corpus.json
```

The corpus is the parity contract between:

- C++ context contract runner: `tools/cpp_unified_context_contract.cc`
- Python schema validator: `tools/run_temporalstore_unified_tests.py`
- Rust SDK parity test: `sdk/rust/temporalstore/tests/unified_corpus.rs`

## Covered Context Surfaces

The current shared corpus covers 11 MatrixArk context cases and 43 context steps:

- context tree, child refs, node embeddings, and traversal
- ContextEvent write/query and ContextPack construction
- ContextEntity upsert/query and current-state retrieval
- ContextIndex write/query and AND filtering
- raw API ingestion with idempotency
- batch ingestion
- stream ingestion with replay-offset skip
- resource chunk ingestion/query and resource-derived event extraction
- feedback ingestion
- ContextSummary plus L0 embedding assertion
- dirty summary markers
- ContextCompressionEvent with source-event audit
- token-budget retrieval parity
- model/provider config parity for OSS embedding/reranker metadata

## Required Gate

Run this before claiming C++ and Rust context parity:

```bash
bash tools/run_rust_unified_tests.sh
```

The wrapper calls the unified test API:

```bash
python3 tools/run_unified_parity_tests.py \
  --corpus sdk/unified/temporalstore_unified_corpus.json \
  --result-dir /tmp/temporalstore-unified-parity
```

The unified runner writes one JSON report and one Markdown report:

```bash
/tmp/temporalstore-unified-parity/unified_parity_report.json
/tmp/temporalstore-unified-parity/unified_parity_report.md
```

You can still run individual stages while debugging:

```bash
python3 tools/run_temporalstore_unified_tests.py \
  --corpus sdk/unified/temporalstore_unified_corpus.json \
  --validate-only

bash tools/run_cpp_unified_context_contract.sh \
  sdk/unified/temporalstore_unified_corpus.json

cd sdk/rust/temporalstore && \
  TEMPORALSTORE_UNIFIED_CORPUS=/root/src/github-services/TemporalStore/sdk/unified/temporalstore_unified_corpus.json \
  cargo test --no-default-features --features proxy --test unified_corpus
```

## Development Rule

When C++ adds or changes a context feature:

1. Add or update one corpus command under `sdk/unified/temporalstore_unified_corpus.json`.
2. Add the command kind to `coverage.required_command_kinds`.
3. If it is a new product-level behavior, add or update a named case in `coverage.required_case_names`.
4. Make sure `tools/run_rust_unified_tests.sh` passes.
5. Only then claim Rust feature parity.

This keeps Rust from silently lagging behind C++ as MatrixArk adds context nodes,
events, entities, summaries, compression, indexes, and model-backed extraction
metadata. See `docs/temporalstore_unified_test_contract.md` for the exact input, output, and runner API.
