# TemporalStore Unified Test Corpus Consumption

MatrixArk/TemporalStore product behavior tests should be consumed from one shared corpus instead of duplicated separately for C++ and Rust.

## Preferred Resolution Order

`tools/run_temporalstore_unified_tests.py` resolves the corpus in this order:

1. `TEMPORALSTORE_TEST_CORPUS`
2. `third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
3. `../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
4. `../TemporalStore/compat/unified_temporalstore_cases.json`
5. `../TemporalStore/sdk/unified/temporalstore_unified_corpus.json`
6. local fallback `sdk/unified/temporalstore_unified_corpus.json`

The fourth and fifth entries let this checkout consume the newer corpus from a sibling `bjmeetsfo/TemporalStore` checkout while the standalone corpus repo is being finalized.

## Commands

Validate whatever corpus the runner resolves:

```bash
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

Validate a specific external corpus:

```bash
TEMPORALSTORE_TEST_CORPUS=/path/to/unified_temporalstore_cases.json   python3 tools/run_temporalstore_unified_tests.py --validate-only
```

Check dependency wiring:

```bash
python3 tools/validate_temporalstore_test_corpus_dependency.py --require-external --allow-drift
```

Use `--allow-drift` while the local C++ fallback still has fewer cases than the external corpus. Remove that flag once both repos pin the same corpus commit.

## Contract

New cross-language product behavior must be added to the shared corpus first. C++-only and Rust-only tests should remain local only for implementation internals, transport-specific code, or temporary migration gaps.
