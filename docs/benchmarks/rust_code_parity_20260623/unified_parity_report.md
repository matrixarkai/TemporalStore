# TemporalStore Unified Parity Report

- status: `passed`
- corpus: `<repo>/third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
- cases: `11`
- command kinds: `33`
- context steps: `43`

## Stages

| Stage | Status | Duration | Command |
| --- | --- | ---: | --- |
| python_schema | `passed` | `0.052` | `/usr/bin/python3 tools/run_temporalstore_unified_tests.py --corpus <repo>/third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json --validate-only` |
| cpp_context_contract | `passed` | `3.084` | `bash tools/run_cpp_unified_context_contract.sh <repo>/third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json` |
| rust_unified_corpus | `passed` | `0.078` | `cargo test --no-default-features --features proxy --test unified_corpus` |

## Input / Output Contract

- Input is one JSON corpus with `schema_version`, `coverage`, `cases`, `steps`, and `command.kind`.
- Output is one JSON report plus this Markdown report.
- Python validates schema and API shape.
- C++ validates behavior against the same command sequence.
- Rust validates the same corpus, required cases, and required command kinds.

