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

The Rust runner now resolves the corpus in this order:

1. `TEMPORALSTORE_TEST_CORPUS`
2. `third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
3. `../TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
4. `compat/unified_temporalstore_cases.json` local fallback

The local fallback exists only for the transition window while both repositories
are being wired. Once `TemporalStoreTestCorpus` is created remotely and pinned in
both code repos, CI should run with `--require-external`.

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

During the transition, that guard also checks the external corpus SHA-256 against
the local fallback unless `--allow-drift` is explicitly passed.

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

Remote target:

```text
https://github.com/bjmeetsfo/TemporalStoreTestCorpus.git
```

Current blocker: GitHub currently returns `Repository not found` for that remote,
so the seed repository exists locally but cannot be pushed until the GitHub repo
is created or access is granted.
