# TemporalStoreTestCorpus External Repo

The shared C++/Rust product-behavior corpus lives in a standalone repository
named `TemporalStoreTestCorpus`.

Local checkout used by this workspace:

```text
../TemporalStoreTestCorpus
```

Canonical corpus path:

```text
TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json
```

The in-repo fallback remains available during transition:

```text
compat/unified_temporalstore_cases.json
```

## Resolution Order

`tools/run_temporalstore_unified_tests.py` resolves the corpus in this order:

1. `TEMPORALSTORE_TEST_CORPUS`
2. `third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
3. `../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
4. `compat/unified_temporalstore_cases.json`

Explicit external validation:

```bash
python3 tools/run_temporalstore_unified_tests.py \
  --validate-only \
  --corpus ../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json
```

or:

```bash
TEMPORALSTORE_TEST_CORPUS=../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json \
  python3 tools/run_temporalstore_unified_tests.py --validate-only
```

Dependency guard:

```bash
python3 tools/validate_temporalstore_test_corpus_dependency.py --require-external
```

During the transition, that guard compares the external corpus SHA-256 against
the local fallback unless `--allow-drift` is explicitly passed.

## Setup

```bash
git clone https://github.com/bjmeetsfo/TemporalStoreTestCorpus.git ../TemporalStoreTestCorpus
```

or, when pinned as a submodule:

```bash
git submodule update --init third_party/TemporalStoreTestCorpus
```

## C++ Consumption

The C++ repo should pin the same `TemporalStoreTestCorpus` commit and emit the
same result shape documented by:

```text
TemporalStoreTestCorpus/schemas/unified_result.schema.json
```

Until the native C++ adapter executes every case, C++ static surface gates remain
temporary blockers in the corpus metadata.

## MatrixArk Context Runner Ownership

The lightweight C++ MatrixArk context contract runner lives in
`TemporalStoreTestCorpus/runners/cpp/`. Consumer repos should keep only thin
wrappers that resolve the external corpus and delegate to the shared runner.

## Removed Local Duplicate Tests

Common/string/hash/set extension behavior was migrated into executable shared
cases in `TemporalStoreTestCorpus`:

- `common_storage_pool_uri_guardrail`
- `stream_object_store_backend_detection`
- `common_module_delete_ttl_expire_setex_lifecycle`
- `string_set_get_nx_xx_flags`
- `hash_get_multi_get_missing_and_existing_fields`
- `hash_incrby_invalid_and_overflow_edges`
- `hash_getall_len_delete_lifecycle`
- `set_module_add_members_card_membership_remove`

The removal manifest is maintained in the shared repo:

```text
TemporalStoreTestCorpus/cases/cpp/cpp_extension_local_test_removal_manifest.json
```

## Migration Rule

New cross-language product behavior should be added to `TemporalStoreTestCorpus`
first. Local C++ or Rust tests should remain only for implementation internals,
transport-specific code, or temporary migration gaps.
