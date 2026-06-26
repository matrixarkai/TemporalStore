# Context Benchmark Truth And Report Contract

This shared C++/Rust benchmark contract keeps LOCOMO and LongMemEval_s results strict about
what was actually proven.

## Evidence Labels

- `deterministic_engineering`: deterministic reader/scorer evidence. Useful for regression gates,
  but not paper-comparable.
- `live_reader`: a configured OpenAI-compatible or OSS reader endpoint was called and fallback was
  disabled.
- `paper_comparable`: real dataset, live reader calls, full Rust TemporalStore replay, archived
  prompts/model metadata, thresholds, latency, and category breakdown all passed in the same report.

## Required Report Shape

Both Rust and C++ benchmark adapters should emit
`matrixark_vikingmem_context_benchmark_report_v1` with:

- dataset name and hash
- case count
- reader mode and model/provider metadata
- Hit@K, reader hit rate, MRR, and token reduction
- p50 and p95 latency
- category breakdown
- whether all ingestion, extraction, retrieval, and replay paths used Rust TemporalStore
- whether the run was Python-only diagnostic glue
- archived report path

No doc or readiness gate should claim VikingMem paper parity from fixture data, deterministic-only
reader results, bounded Rust replay, or Python-only diagnostics.
