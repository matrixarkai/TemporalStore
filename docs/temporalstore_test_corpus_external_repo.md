# TemporalStoreTestCorpus External Repo

The shared C++/Rust product-behavior corpus is moving to a standalone repository
named `TemporalStoreTestCorpus`.

Local checkout used for the first seed:

```text
../TemporalStoreTestCorpus
```

Seeded source:

```text
compat/unified_temporalstore_cases.json
```

External target:

```text
TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json
```

## Rust Consumption

The Rust runner keeps the local corpus as a fallback, but can consume the
external repository directly:

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

## C++ Consumption

The C++ repo should pin the same `TemporalStoreTestCorpus` commit and emit the
same result shape documented by:

```text
TemporalStoreTestCorpus/schemas/unified_result.schema.json
```

Until the native C++ adapter executes every case, C++ static surface gates remain
temporary blockers in the corpus metadata.

## Migration Rule

New product behavior tests should be added to `TemporalStoreTestCorpus` first.
Rust-only and C++-only tests should remain local only for implementation
internals, transport-specific surfaces, or temporary pending-migration gaps.
