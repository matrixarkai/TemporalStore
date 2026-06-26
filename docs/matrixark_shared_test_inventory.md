# MatrixArk Shared Test Inventory

This repo now treats `third_party/TemporalStoreTestCorpus` as the canonical home
for MatrixArk context pipeline tests, benchmarks, parity runners, and translated
test cases. The local duplicate MatrixArk wrappers under `tools/` were removed.

## Current Counts

Generated with:

```bash
cd third_party/TemporalStoreTestCorpus
python3 tools/count_test_inventory.py --consumer-repo /root/src/github-services/TemporalStore --json
```

| Category | Count | Notes |
| --- | ---: | --- |
| Shared corpus total | 43 | 20 case/manifest files + 23 shared runners |
| C++-specific tests | 72 | Native C++/onebox tests plus non-MatrixArk tool tests |
| Rust-specific tests | 1 | `sdk/rust/temporalstore/tests/unified_corpus.rs` |
| Local MatrixArk wrappers | 0 | Moved to shared corpus or removed |

## How To Run Shared MatrixArk Tests

Run shared tests from the shared repo and point them at the consumer repo:

```bash
cd /root/src/github-services/TemporalStoreTestCorpus
TEMPORALSTORE_CONSUMER_REPO=/root/src/github-services/TemporalStore \
PYTHONPATH=/root/src/github-services/TemporalStore \
python3 tools/test_matrixark_mcp_server.py
```

The same pattern applies to shared benchmark/parity runners such as:

```bash
TEMPORALSTORE_CONSUMER_REPO=/root/src/github-services/TemporalStore \
PYTHONPATH=/root/src/github-services/TemporalStore \
python3 tools/run_matrixark_context_storage_benchmark.py --backend local --events 40 --queries 4 --ingest-mode batch --batch-size 20
```

For Rust parity, use the same shared corpus plus the Rust adapter/backend
selection in the runner arguments or environment. The Rust repo-specific harness
remains `sdk/rust/temporalstore/tests/unified_corpus.rs`.
