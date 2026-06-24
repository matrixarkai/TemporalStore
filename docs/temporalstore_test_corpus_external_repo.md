# TemporalStore Unified Test Corpus Consumption

MatrixArk/TemporalStore product behavior tests now consume the shared `TemporalStoreTestCorpus` repository instead of owning duplicate local corpus JSON.

## Resolution Order

`tools/run_temporalstore_unified_tests.py` resolves the corpus in this order:

1. `TEMPORALSTORE_TEST_CORPUS`
2. `third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
3. `../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`

The embedded local fallback `sdk/unified/temporalstore_unified_corpus.json` was removed. Initialize the submodule or set `TEMPORALSTORE_TEST_CORPUS` before running unified tests.

## Setup

```bash
git submodule update --init third_party/TemporalStoreTestCorpus
```

or:

```bash
git clone https://github.com/bjmeetsfo/TemporalStoreTestCorpus.git ../TemporalStoreTestCorpus
```

## Commands

Validate the external corpus:

```bash
python3 tools/validate_temporalstore_test_corpus_dependency.py --require-external
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

Run the C++ consumer wrapper:

```bash
python3 tools/run_unified_parity_tests.py --result-dir /tmp/temporalstore-unified-parity-external-cpp
```

The Rust repository should run its native adapter against the same external corpus. The C++ wrapper can still run the legacy Rust SDK stage with `--run-rust`, but it is not the canonical Rust parity path.

## MatrixArk Context Runner Ownership

The lightweight C++ MatrixArk context contract runner now lives in `TemporalStoreTestCorpus/runners/cpp/`. This repo keeps only `tools/run_cpp_unified_context_contract.sh`, a consumer wrapper that resolves the external corpus and delegates to the shared runner.

## Removed Local Duplicate Tests

The C++ common/string/hash/set extension gtests were migrated into executable
shared cases in `TemporalStoreTestCorpus` and removed from this repo:

- `src/extension/common/test.cc` -> `common_module_delete_ttl_expire_setex_lifecycle`
- `src/extension/string/test.cc` -> `string_set_get_nx_xx_flags`
- `src/extension/hash/test.cc` -> `hash_get_multi_get_missing_and_existing_fields`,
  `hash_incrby_invalid_and_overflow_edges`, and
  `hash_getall_len_delete_lifecycle`
- `src/extension/set/test.cc` -> `set_module_add_members_card_membership_remove`

The removal manifest is maintained in the shared repo:

```text
TemporalStoreTestCorpus/cases/cpp/cpp_extension_local_test_removal_manifest.json
```

## Contract

New cross-language product behavior should be added to `TemporalStoreTestCorpus` first. Local C++ or Rust tests should remain only for implementation internals, transport-specific code, or temporary migration gaps.
