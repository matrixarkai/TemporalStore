# MatrixArk LongMemEval C++ vs Rust TemporalStore Parity - 2026-06-23

## Result

LongMemEval is now runnable through both C++ and Rust TemporalStore backends with controlled benchmark slices.

The important harness gap fixed here: `--question-limit` now limits LongMemEval ingestion too. Before this change, a command such as `--question-limit 20` still ingested all 500 LongMemEval items before retrieval. That made small parity runs accidentally become large ingestion stress tests.

New benchmark controls:

- `--session-limit`: maximum total sessions to ingest.
- `--sessions-per-item-limit`: maximum sessions per LongMemEval question/item.
- For LongMemEval, `--question-limit N` now ingests only the first `N` items unless `--conversation-limit` is explicitly set.

## Dataset

Official cleaned local LongMemEval_s file:

```text
/root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json
items/questions: 500
sessions: 23867
turns: 246750
```

## Clean Backend Parity Runs

All runs used:

```text
embedding provider: hash
understanding provider: rules
reader/judge: deterministic debug path
max_context_tokens: 1200
batch_size: 40
MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES=32768
```

### Q1 Full Sessions

This ingests the first official LongMemEval item with all its sessions.

| Metric | C++ TemporalStore | Rust TemporalStore |
| --- | ---: | ---: |
| status | completed | completed |
| elapsed seconds | 10.71 | 12.50 |
| questions | 1 | 1 |
| sessions | 53 | 53 |
| turns ingested | 550 | 550 |
| ingestion throughput turns/sec | 167.021 | 137.294 |
| context recall | 1.0000 | 1.0000 |
| final debug judge score | 0.0000 | 0.0000 |
| avg prompt tokens | 1200 | 1200 |
| p50 retrieval ms | 216.231 | 141.791 |
| compression hidden answers | 0 | 0 |

### Q20 With 5 Sessions Per Item

This is the current clean C++/Rust parity slice for LongMemEval.

| Metric | C++ TemporalStore | Rust TemporalStore |
| --- | ---: | ---: |
| status | completed | completed |
| elapsed seconds | 16.66 | 16.66 |
| questions | 20 | 20 |
| sessions | 100 | 100 |
| turns ingested | 977 | 977 |
| ingestion throughput turns/sec | 111.377 | 114.644 |
| context recall | 0.9500 | 0.9500 |
| final debug judge score | 0.1000 | 0.1000 |
| answer support hit | 0.1000 | 0.1000 |
| evidence session recall | 0.0500 | 0.0500 |
| avg prompt tokens | 1113.65 | 1113.65 |
| answer-bearing token density | 0.017151 | 0.017151 |
| p50 retrieval ms | 54.147 | 66.681 |
| p95 retrieval ms | 92.873 | 91.983 |
| compression hidden answers | 0 | 0 |

The low judge/evidence score is expected for this controlled slice: only the first 5 sessions per question are ingested, so most answer evidence sessions are intentionally absent. The useful result is backend parity and stable bounded LongMemEval execution.

## Remaining Gaps

The following larger shapes still exceed the current single-node local deployment:

- q20 with all sessions: times out during ingestion.
- q20 with 10 sessions per item: server EOF / not-connected during ingestion, even with `--max-message-chars 800`.
- full 500-question official LongMemEval_s: not attempted after the q20/s10 boundary failed.

This is no longer a Python-vs-C++/Rust mapping issue. The current gap is sustained write/load shaping for LongMemEval's much larger session count.

## Next Fixes

1. Add native multi-field batch write in the C++ and Rust SDK paths, not only Python-level record bundles.
2. Add ingestion checkpoints per item/session so LongMemEval can resume after service restarts.
3. Move retrieval audit writes off the request critical path or batch them after checkpoint intervals.
4. Add a LongMemEval profile that increases `sessions-per-item-limit` gradually: 5 -> 10 -> 20 -> all.
5. Run C++ and Rust separately for every larger profile to avoid shared single-node pressure.

## Artifact Pointers

Committed lightweight reports:

```text
docs/benchmarks/longmemeval_cpp_rust_parity_20260623/cpp_q1/
docs/benchmarks/longmemeval_cpp_rust_parity_20260623/rust_q1/
docs/benchmarks/longmemeval_cpp_rust_parity_20260623/cpp_q20_s5/
docs/benchmarks/longmemeval_cpp_rust_parity_20260623/rust_q20_s5/
```

Large generated `context_packs.jsonl`, `result.json`, `hypotheses.jsonl`, and `judge.jsonl` files are local debugging artifacts and are not committed by default.
