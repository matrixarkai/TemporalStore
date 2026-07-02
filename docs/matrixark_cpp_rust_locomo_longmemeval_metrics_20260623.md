# MatrixArk C++ vs Rust Benchmark Metrics - 2026-06-23

This run compares MatrixArk benchmark behavior on the C++ TemporalStore direct backend and the Rust TemporalStore record-log backend for LOCOMO and LongMemEval_s.

## Environment

- Repo: <repo>
- Branch: main
- LOCOMO data: /root/matrixark_benchmarks/data/locomo10.json
- LongMemEval_s data: /root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json
- C++ library: <repo>/output-ubuntu22/release/sdk/lib/libbcache2.so
- Rust CLI: <repo>/sdk/rust/temporalstore/target/release/matrixark_record_log
- Reader: deterministic CI reader, not paper-style LLM reader
- Judge: deterministic local support judge, not paper-style GPT judge
- Token budget: 1200 context tokens
- Ingest mode: batch, batch size 40
- Storage log mode: sharded compact count log

Important: these numbers must not be compared as official VikingMem parity scores because the reader, judge, prompt, and scoring protocol are not matched to the paper.

## Run Status

| Dataset | Backend | Status | Artifact directory |
| --- | --- | --- | --- |
| LOCOMO | C++ direct | Completed, 100 questions | docs/benchmarks/cpp_rust_locomo_longmem_20260623/cpp_locomo_q100/ |
| LOCOMO | Rust record-log | Partial, 50 questions before outer timeout | docs/benchmarks/cpp_rust_locomo_longmem_20260623/rust_locomo_q100/ |
| LongMemEval_s | C++ direct | Blocked before artifacts | docs/benchmarks/cpp_rust_locomo_longmem_20260623/cpp_longmem_q20_s5/ |
| LongMemEval_s | Rust record-log | Blocked before artifacts | docs/benchmarks/cpp_rust_locomo_longmem_20260623/rust_longmem_q20_s5/ |

## LOCOMO Metrics

| Metric | C++ direct LOCOMO q100 | Rust LOCOMO q100 partial |
| --- | ---: | ---: |
| Questions completed | 100 | 50 |
| Sessions ingested | 38 | 38 |
| Turns ingested | 788 | 788 |
| Context recall | 100.00% | 100.00% |
| Final judge score | 62.00% | 60.00% |
| Answer support hit | 62.00% | 60.00% |
| Answer quality under budget | 62.00% | 60.00% |
| Exact substring answer hit | 10.00% | 6.00% |
| Evidence session recall | 40.00% | 40.00% |
| Compression hidden answers | 0 | 0 |
| Compression safety | pass | pass |
| Avg retrieval latency | 163.39 ms | 162.39 ms |
| p50 retrieval latency | 167.36 ms | 164.42 ms |
| p95 retrieval latency | 208.14 ms | 208.25 ms |
| Ingestion elapsed | 4235 ms | 4454 ms |
| Ingestion throughput | 186.07 turns/sec | 176.92 turns/sec |
| Avg prompt tokens | 1199.10 | 1199.12 |
| Answer-bearing token density | 1.26% | 1.34% |
| Judge score per 1K tokens | 0.5171 | 0.5004 |
| Selected tokens | 119,910 | 59,956 |
| Answer-bearing tokens | 1,510 | 805 |
| Dropped over-budget tokens | 627,815 | 316,167 |

## Failure Buckets

| Failure bucket | C++ direct LOCOMO q100 | Rust LOCOMO q100 partial |
| --- | ---: | ---: |
| Context recall miss | 0 | 0 |
| Retrieval miss | 0 | 0 |
| Temporal/entity miss | 0 | 0 |
| Compression hidden answer | 0 | 0 |
| Evidence session miss | 60 | 30 |
| Evidence miss with context | 58 | 28 |
| Reader support miss | 38 | 20 |
| Reader miss with evidence | 7 | 4 |
| Exact substring reader miss | 90 | 47 |
| Token budget pressure | 46 | 23 |

## Commands

C++ LOCOMO:

    python3 tools/run_matrixark_dataset_benchmark.py --dataset locomo --data-path /root/matrixark_benchmarks/data/locomo10.json --artifact-dir docs/benchmarks/cpp_rust_locomo_longmem_20260623/cpp_locomo_q100 --artifact-prefix cpp_locomo_q100_20260623 --backend temporalstore-direct --storage-prefix matrixark:bench:20260623:cpp:locomo:q100 --request-timeout-ms 120000 --io-timeout-ms 120000 --batch-size 40 --question-limit 100 --conversation-limit 2 --max-context-tokens 1200

Rust LOCOMO:

    python3 tools/run_matrixark_dataset_benchmark.py --dataset locomo --data-path /root/matrixark_benchmarks/data/locomo10.json --artifact-dir docs/benchmarks/cpp_rust_locomo_longmem_20260623/rust_locomo_q100 --artifact-prefix rust_locomo_q100_20260623 --backend temporalstore-rust --storage-prefix matrixark:bench:20260623:rust:locomo:q100 --request-timeout-ms 120000 --io-timeout-ms 120000 --batch-size 40 --question-limit 100 --conversation-limit 2 --max-context-tokens 1200

C++ LongMemEval_s attempted:

    python3 tools/run_matrixark_dataset_benchmark.py --dataset longmemeval_s --data-path /root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json --artifact-dir docs/benchmarks/cpp_rust_locomo_longmem_20260623/cpp_longmem_q20_s5 --artifact-prefix cpp_longmem_q20_s5_20260623 --backend temporalstore-direct --storage-prefix matrixark:bench:20260623:cpp:longmem:q20:s5 --request-timeout-ms 120000 --io-timeout-ms 120000 --batch-size 40 --question-limit 20 --sessions-per-item-limit 5 --max-context-tokens 1200

The C++ LongMemEval_s run failed before canonical artifacts were written:

    RuntimeError: Internal: Request server failed[E112]Not connected to 127.0.0.1:18001 yet

At inspection time, neither 127.0.0.1:18000 nor 127.0.0.1:18001 was listening, so this is a live C++ service availability issue, not a MatrixArk mapping error.

## Interpretation

The completed LOCOMO results show the C++ and Rust logical storage paths are close on the measured subset:

- Retrieval quality is aligned: both reached 100% context recall and 40% evidence-session recall.
- Judge score is close: C++ 62.00%, Rust partial 60.00%.
- Retrieval latency is close for completed questions: both have p95 around 208 ms.
- C++ completed the 100-question run cleanly; Rust produced a valid 50-question partial run but did not finish the full q100 command before the outer timeout.

The main quality gap is now reader/evidence quality under a fixed prompt budget, not basic context recall:

- Exact substring answer hit is low because the deterministic reader is weak.
- Token budget pressure is high because LOCOMO answer evidence is often broad and the 1200-token pack drops many candidate turns.
- The next score jump needs OSS/OpenAI reader and judge parity, answer-dense packing, and stronger evidence selection.

## Current Blockers

1. LongMemEval_s C++ direct requires a live C++ service.
   The compact direct path completed LOCOMO, but LongMemEval_s hit a connection path that requires 127.0.0.1:18001. The service was not listening during this run.

2. Rust full-run throughput still needs a long-lived service path.
   The Rust backend is feature-aligned on the completed subset, but the current record-log path is slower and did not finish q100 before the outer timeout.

3. Official score parity still needs model-backed reader/judge.
   Deterministic CI reader/judge is useful for regression testing, but VikingMem-style benchmark claims require a comparable LLM reader and judge protocol.

## Next Steps

1. Start or repair the live C++ service endpoints before LongMemEval_s:
   - metaserver on 127.0.0.1:18000
   - nodeserver on 127.0.0.1:18001

2. Rerun LongMemEval_s with both backends after service health is confirmed.

3. Replace the Rust per-operation CLI path with a long-lived Rust gateway or binding for benchmark parity.

4. Add a model-backed reader/judge run:
   - OSS instruct reader for local runs
   - OpenAI-compatible reader/judge for paper-style runs

5. Keep all benchmark artifacts canonical:
   - result.json
   - report.json
   - report.md
   - hypotheses.jsonl
   - context_packs.jsonl
   - judge.jsonl
