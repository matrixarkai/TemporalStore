# Rust TemporalStore LOCOMO And LongMemEval Benchmark Metrics

The broader Rust-vs-C++ TemporalStore parity status is summarized in
[`rust_vs_cpp_temporalstore_parity_report.md`](rust_vs_cpp_temporalstore_parity_report.md). This
page is the benchmark-specific evidence for the Rust TemporalStore ingestion, context storage,
retrieval, and replay path.

## Summary

Rust TemporalStore was used as the benchmark ingestion, context event storage, retrieval, and replay
backend for both LOCOMO and LongMemEval_s. Python was used only as the dataset conversion, runner,
reader scoring, and report-emission wrapper.

Run date: 2026-06-21

Native checkout used for faster Rust builds:

```bash
/home/vj/temporalstore-rust-native
```

Raw reports were written under the ignored local artifact directory:

```bash
/home/vj/temporalstore-rust-native/benchmark_reports/full_rust_benchmarks
```

These reports are deterministic-reader engineering evidence. They are not a VikingMem paper-comparable
live-reader claim because no live GPT-4o-mini or OpenAI-compatible reader endpoint was configured for
this run.

## Production-Performance Wording

The correct current wording is: **Rust is feature-correct for parity testing; C++
remains the production-performance baseline.**

Rust should not be described as production-performance parity until the benchmark
evidence proves all of the following:

- Rust is moved fully off CLI-per-operation paths into a long-lived native
  backend or gateway.
- Full official LOCOMO and full official LongMemEval_s run on both C++ and Rust.
- Both backends use the same OSS embedding model, reader, judge, token budget,
  storage mode, and benchmark config.
- Each run saves canonical artifacts: result JSON, report JSON/Markdown,
  hypotheses JSONL, ContextPack JSONL, judge JSONL, and backend metrics.
- The comparison covers recall, judge score, token use, p50/p95/p99 latency,
  QPS, errors, and fallback flags.
- The unified corpus covers async oplog, batch append, Redis surface,
  proxy/client, multi-node, and Raft mode.

This page records useful Rust benchmark evidence, but the production baseline
remains C++ until those proof gates are green.

## Commands

LOCOMO:

```bash
cd /home/vj/temporalstore-rust-native
python3 tools/run_locomo_90_hit_rate.py \
  --input /mnt/c/root/matrixark_benchmarks/data/locomo10.json \
  --threshold-profile locomo_full \
  --reader-mode deterministic \
  --require-rust-temporalstore \
  --require-full-rust-temporalstore-replay \
  --rust-temporalstore-release \
  --rust-temporalstore-batch-size 16 \
  --rust-temporalstore-source-pack-size 32 \
  --rust-temporalstore-timeout-seconds 1200 \
  --rust-temporalstore-score-tolerance 0 \
  --rust-temporalstore-jsonl benchmark_reports/full_rust_benchmarks/locomo_full_rust_context.jsonl \
  --rust-temporalstore-report benchmark_reports/full_rust_benchmarks/locomo_full_rust_backend.json \
  --report benchmark_reports/full_rust_benchmarks/locomo_full_rust_report.json \
  --misses benchmark_reports/full_rust_benchmarks/locomo_full_rust_misses.jsonl
```

LongMemEval_s:

```bash
cd /home/vj/temporalstore-rust-native
python3 tools/run_longmemeval_s_full_path.py \
  --input /mnt/c/root/matrixark_benchmarks/data/longmemeval_s_helamem.json \
  --threshold-profile longmemeval_full \
  --reader-mode deterministic \
  --require-rust-temporalstore \
  --require-full-rust-temporalstore-replay \
  --rust-temporalstore-release \
  --rust-temporalstore-batch-size 0 \
  --rust-temporalstore-source-pack-size 0 \
  --rust-temporalstore-timeout-seconds 2400 \
  --rust-temporalstore-score-tolerance 0 \
  --rust-temporalstore-jsonl benchmark_reports/full_rust_benchmarks/longmemeval_s_full_rust_context.jsonl \
  --rust-temporalstore-report benchmark_reports/full_rust_benchmarks/longmemeval_s_full_rust_backend.json \
  --report benchmark_reports/full_rust_benchmarks/longmemeval_s_full_rust_report.json \
  --misses benchmark_reports/full_rust_benchmarks/longmemeval_s_full_rust_misses.jsonl
```

## Dataset Inputs

| Dataset | Path | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| LOCOMO | `/mnt/c/root/matrixark_benchmarks/data/locomo10.json` | `2,805,274` | `79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4` |
| LongMemEval_s | `/mnt/c/root/matrixark_benchmarks/data/longmemeval_s_helamem.json` | `15,388,478` | `821a2034d219ab45846873dd14c14f12cfe7776e73527a483f9dac095d38620c` |

## Rust TemporalStore Evidence

| Metric | LOCOMO | LongMemEval_s |
| --- | ---: | ---: |
| `all_pipelines_use_rust_temporalstore` | `true` | `true` |
| `python_only_diagnostic` | `false` | `false` |
| `rust_temporalstore_backend_ready` | `true` | `true` |
| `rust_temporalstore_context_event_ingest_ready` | `true` | `true` |
| `rust_temporalstore_full_replay_ready` | `true` | `true` |
| `rust_temporalstore_direct_source_scoring` | `false` | `false` |
| `rust_temporalstore_ingested_source_sets` | `10` | `500` |
| `rust_temporalstore_retrieved_source_sets` | `10` | `500` |
| Full replay all cases | `true` | `true` |
| Full replay all sources | `true` | `true` |
| Rust/Python Hit@K delta | `0.0` | `0.0` |
| Rust/Python case count on par | `true` | `true` |
| Rust backend elapsed ms | `298,257.979041` | `2,275,845.539744` |
| Rust build profile | `release` | `release` |

LOCOMO used source packing to preserve all source text while reducing replay row count from
`1,475,584` source rows to `46,782` packed source rows. LongMemEval_s did not use source packing.

## Top-Level Benchmark Metrics

| Metric | LOCOMO | LongMemEval_s |
| --- | ---: | ---: |
| `case_count` | `1,542` | `500` |
| `conversation_count` | `10` | `500` |
| `source_count` | `9,363` | `10,960` |
| `benchmark_hit_at_k` | `0.9474708171206225` | `1.0` |
| `benchmark_recall_at_k` | `0.9474708171206225` | `1.0` |
| `benchmark_mean_reciprocal_rank` | `0.5273212063669817` | `1.0` |
| `reader_hit_rate` | `0.8767833981841764` | `0.986` |
| `reader_answer_coverage` | `0.8719620628334321` | `0.8845671267252195` |
| `answer_term_coverage` | `0.7611144042679312` | `0.6386449184441656` |
| `benchmark_token_reduction_percent` | `83.71398170966064` | `81.39026159904991` |
| `benchmark_retrieval_p50_ms` | `25.995519999696626` | `12.322865500209446` |
| `benchmark_retrieval_p95_ms` | `55.247234500006925` | `25.298554849405267` |
| `benchmark_reader_p50_ms` | `5.7480564998968475` | `2.184772999953566` |
| `benchmark_reader_p95_ms` | `21.575811199954845` | `6.307644950402391` |
| `benchmark_threshold_passed` | `true` | `true` |
| `zero_hit_queries` | `81` | `0` |
| `reader_zero_hit_queries` | `190` | `7` |

## LOCOMO Category Breakdown

| Category | Cases | Hit@K | Reader Hit | Answer-Term Coverage | MRR | Zero-Hit Queries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `category_1` | `282` | `0.9397163120567376` | `0.8652482269503546` | `0.5149700598802395` | `0.3516359883615353` | `17` |
| `category_2` | `321` | `0.9626168224299065` | `0.8442367601246106` | `0.8362068965517241` | `0.6634021427905966` | `12` |
| `category_3` | `96` | `0.9479166666666666` | `0.71875` | `0.40310077519379844` | `0.316034425428858` | `5` |
| `category_4` | `841` | `0.9441141498216409` | `0.9108204518430439` | `0.8775743707093822` | `0.5583946399004905` | `47` |
| `category_5` | `2` | `1.0` | `1.0` | `1.0` | `0.5333333333333333` | `0` |

Weak LOCOMO areas remain `category_3` reader/answer synthesis and `category_1` answer-term
coverage. Retrieval still clears the full LOCOMO threshold with `Hit@K=0.9474708171206225`.

## LongMemEval_s Category Breakdown

| Category | Cases | Hit@K | Reader Hit | Answer-Term Coverage | MRR | Zero-Hit Queries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `knowledge_update` | `78` | `1.0` | `0.9871794871794872` | `0.7596153846153846` | `1.0` | `0` |
| `multi_session` | `133` | `1.0` | `0.9774436090225563` | `0.49162011173184356` | `1.0` | `0` |
| `single_session_assistant` | `56` | `1.0` | `0.9821428571428571` | `0.9411764705882353` | `1.0` | `0` |
| `single_session_preference` | `30` | `1.0` | `1.0` | `0.5` | `1.0` | `0` |
| `single_session_user` | `70` | `1.0` | `1.0` | `0.7619047619047619` | `1.0` | `0` |
| `temporal_reasoning` | `133` | `1.0` | `0.9849624060150376` | `0.606694560669456` | `1.0` | `0` |

Weak LongMemEval_s areas are answer-term coverage for `multi_session` and
`single_session_preference`. Retrieval is perfect on this run with `Hit@K=1.0` and zero zero-hit
queries.

## Paper-Comparable Status

`reader_mode_effective=deterministic` for both benchmark runs, and `paper_comparable_claim_ready`
is `false`. A VikingMem paper-comparable report still requires a live GPT-4o-mini or compatible
OpenAI-style reader endpoint, real model calls, and archived provider/model metadata. The Rust
TemporalStore backend requirement itself passed for both datasets.

## 2026-06-21 Score Optimization Pass

The deterministic reader was tightened for VikingMem-style cases that were still weaker than
retrieval: charity-event aggregation, normalized `pre-1920` / `gin-based` / `gardening-related`
phrases, ordinal bottle recall, and age-difference questions. The full Rust TemporalStore replay was
rerun after the patch.

LongMemEval_s improved. LOCOMO stayed score-neutral while continuing to pass the full Rust replay
gate.

| Metric | LOCOMO Before | LOCOMO After | LongMemEval_s Before | LongMemEval_s After |
| --- | ---: | ---: | ---: | ---: |
| `case_count` | `1,542` | `1,542` | `500` | `500` |
| `benchmark_hit_at_k` | `0.9474708171206225` | `0.9474708171206225` | `1.0` | `1.0` |
| `reader_hit_rate` | `0.8767833981841764` | `0.8767833981841764` | `0.986` | `0.996` |
| `reader_answer_coverage` | `0.8719620628334321` | `0.8719620628334321` | `0.8845671267252195` | `0.890840652446675` |
| `answer_term_coverage` | `0.7611144042679312` | `0.7611144042679312` | `0.6386449184441656` | `0.6386449184441656` |
| `benchmark_mean_reciprocal_rank` | `0.5273212063669817` | `0.5273212063669817` | `1.0` | `1.0` |
| `benchmark_token_reduction_percent` | `83.71398170966064` | `83.71398170966064` | `81.39026159904991` | `81.39026159904991` |
| `reader_zero_hit_queries` | `190` | `190` | `7` | `2` |
| `zero_hit_queries` | `81` | `81` | `0` | `0` |
| `benchmark_threshold_passed` | `true` | `true` | `true` | `true` |
| `rust_temporalstore_full_replay_ready` | `true` | `true` | `true` | `true` |

Optimized LongMemEval_s raw report:

```bash
/home/vj/temporalstore-rust-native/benchmark_reports/optimized_rust_benchmarks/longmemeval_s_full_rust_report.json
```

Optimized LOCOMO raw report:

```bash
/home/vj/temporalstore-rust-native/benchmark_reports/optimized_rust_benchmarks/locomo_full_rust_report.json
```

Remaining gap versus VikingMem: LOCOMO Category 3 and Category 1 still need stronger temporal
multi-hop synthesis and broader list/inference answer synthesis. LongMemEval_s retrieval is perfect
on this run, but answer-term coverage remains weaker for `multi_session` and
`single_session_preference`.
