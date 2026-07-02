# TemporalStore Unified Parity Report

- status: `passed`
- corpus: `<repo>/third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`
- cases: `127`
- command kinds: `66`
- context steps: `14`

## Stages

| Stage | Status | Duration | Command |
| --- | --- | ---: | --- |
| python_schema | `passed` | `0.052` | `/usr/bin/python3 tools/run_temporalstore_unified_tests.py --corpus <repo>/third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json --validate-only` |
| cpp_context_contract | `passed` | `2.545` | `bash tools/run_cpp_unified_context_contract.sh <repo>/third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json` |
| rust_unified_corpus | `skipped` | `0.0` | `cargo test --no-default-features --features proxy --test unified_corpus` |

## Input / Output Contract

- Input is one JSON corpus with `schema_version`, `coverage`, `cases`, `steps`, and `command.kind`.
- Output is one JSON report plus this Markdown report.
- Python validates schema and API shape.
- C++ validates behavior against the same command sequence.
- Rust validation is run by the Rust repo against the same external corpus; this C++ wrapper can run its legacy Rust SDK stage with `--run-rust`.

