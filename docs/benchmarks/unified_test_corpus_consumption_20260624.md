# Unified Test Corpus Consumption - 2026-06-24

## Goal

Consume the newer shared unified test corpus from `bjmeetsfo/TemporalStore` / external corpus wiring without merging the full Rust/open-source branch into the C++ `main` tree.

## What Changed

- `tools/run_temporalstore_unified_tests.py` now resolves the corpus from:
  1. `TEMPORALSTORE_TEST_CORPUS`
  2. `third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
  3. `../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
  4. `../TemporalStore/compat/unified_temporalstore_cases.json`
  5. `../TemporalStore/sdk/unified/temporalstore_unified_corpus.json`
  6. local fallback `sdk/unified/temporalstore_unified_corpus.json`
- Added `tools/validate_temporalstore_test_corpus_dependency.py` to enforce external corpus wiring.
- Added `tools/compare_unified_cpp_rust_case_reports.py`, required by the newer 127-case corpus contract.
- Added `docs/temporalstore_test_corpus_external_repo.md` with usage instructions.

## Validation

Local fallback validation:

```bash
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

Result:

```text
validated temporalstore-cpp-rust-context-parity schema=1 cases=11 path=/root/src/github-services/TemporalStore/sdk/unified/temporalstore_unified_corpus.json
TemporalStore C++ unified corpus hook passed.
```

External corpus validation, using the newer corpus extracted from `origin/rust-main:compat/unified_temporalstore_cases.json`:

```bash
TEMPORALSTORE_TEST_CORPUS=/tmp/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json   python3 tools/run_temporalstore_unified_tests.py --validate-only
```

Result:

```text
validated temporalstore-unified-cpp-rust-corpus schema=1 cases=127 path=/tmp/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json
```

Dependency guard:

```bash
python3 tools/validate_temporalstore_test_corpus_dependency.py   --external-corpus /tmp/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json   --require-external   --allow-drift
```

Result:

```text
validated TemporalStore test corpus dependency external=/tmp/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json cases=127 steps=226 sha256=50ddea813e6f68ddb0083124420368c8cb9474cc7f63661754dfb3164b984d9a local_cases=11 local_sha256=0f1a69ca08abcb4418d6cf92e245f1e6e84f9d8ad7ed95be99d4c30ea255c115
```

## Current Transition State

The external corpus has 127 cases / 226 steps. The embedded C++ fallback has 11 cases / 43 steps. Use `--allow-drift` until the local fallback is intentionally updated or the standalone corpus repository is pinned in CI.
