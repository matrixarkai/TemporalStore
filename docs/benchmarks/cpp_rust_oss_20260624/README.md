# MatrixArk C++ vs Rust Benchmark Slice With OSS Reader/Judge

Run date: 2026-06-24

This run compares MatrixArk over the C++ TemporalStore direct backend and the
Rust TemporalStore record-log backend on the same LOCOMO and LongMemEval_s
controlled slices.

## Configuration

- Reader: `openai-compatible:qwen2.5:1.5b` through local Ollama
  `http://127.0.0.1:11434/v1`
- Judge: `openai-compatible:qwen2.5:1.5b` through local Ollama
- Token budget: `1200`
- Ingest mode: batch, `batch_size=40`
- C++ backend: `temporalstore-direct`
- Rust backend: `temporalstore-rust`
- Dataset files:
  - LOCOMO: `/root/matrixark_benchmarks/data/locomo10.json`
  - LongMemEval_s: `/root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json`

Important caveat: the benchmark runner currently still reports
`embedding_model=hashing:hashing-local`; it does not expose an OSS embedding
provider flag yet. These results therefore prove C++ vs Rust parity with an OSS
reader/judge, but not full VikingMem-style OSS embedding parity.

## LOCOMO q5 / conversation-limit 1

| Metric | C++ direct | Rust record-log |
| --- | ---: | ---: |
| Questions run | 5 | 5 |
| Answerable questions | 5 | 5 |
| Sessions | 19 | 19 |
| Turns ingested | 419 | 419 |
| Context recall | 100.00% | 100.00% |
| Final judge score | 0.00% | 0.00% |
| Answer quality under budget | 0.00% | 0.00% |
| Answer support hit | 0.00% | 0.00% |
| Answer substring hit | 20.00% | 20.00% |
| Evidence session recall | 20.00% | 20.00% |
| Answer-bearing token density | 0.7167% | 1.0500% |
| Judge score / 1K tokens | 0.0000 | 0.0000 |
| Avg prompt tokens | 1200.0 | 1200.0 |
| Selected tokens | 6000 | 6000 |
| Dropped over-budget tokens | 20588 | 20588 |
| Avg retrieval latency | 143.89 ms | 155.84 ms |
| p95 retrieval latency | 165.42 ms | 170.24 ms |
| Ingestion throughput | 239.84 turns/s | 227.72 turns/s |
| Ingestion elapsed | 1747 ms | 1840 ms |
| Compression hidden answers | 0 | 0 |

Artifact directories:

- C++: `/root/src/github-services/TemporalStore/docs/benchmarks/cpp_rust_oss_20260624/locomo_cpp_oss_q5`
- Rust: `/root/src/github-services/TemporalStore/docs/benchmarks/cpp_rust_oss_20260624/locomo_rust_oss_q5`

## LongMemEval_s q5 / sessions-per-item-limit 5

| Metric | C++ direct | Rust record-log |
| --- | ---: | ---: |
| Questions run | 5 | 5 |
| Answerable questions | 5 | 5 |
| Sessions | 25 | 25 |
| Turns ingested | 218 | 218 |
| Context recall | 80.00% | 80.00% |
| Final judge score | 20.00% | 20.00% |
| Answer quality under budget | 20.00% | 20.00% |
| Answer support hit | 20.00% | 20.00% |
| Answer substring hit | 0.00% | 0.00% |
| Evidence session recall | 0.00% | 0.00% |
| Answer-bearing token density | 3.7940% | 4.5862% |
| Judge score / 1K tokens | 0.2085 | 0.2085 |
| Avg prompt tokens | 959.4 | 959.4 |
| Selected tokens | 4797 | 4797 |
| Dropped over-budget tokens | 15656 | 15656 |
| Avg retrieval latency | 42.18 ms | 46.78 ms |
| p95 retrieval latency | 51.15 ms | 64.28 ms |
| Ingestion throughput | 88.91 turns/s | 83.43 turns/s |
| Ingestion elapsed | 2452 ms | 2613 ms |
| Compression hidden answers | 0 | 0 |

Artifact directories:

- C++: `/root/src/github-services/TemporalStore/docs/benchmarks/cpp_rust_oss_20260624/longmem_cpp_oss_q5_s5`
- Rust: `/root/src/github-services/TemporalStore/docs/benchmarks/cpp_rust_oss_20260624/longmem_rust_oss_q5_s5`

## Takeaways

- C++ and Rust produce the same retrieval-quality metrics on these controlled
  slices.
- Rust is slightly slower but close on this run:
  - LOCOMO p95: `170.24 ms` vs C++ `165.42 ms`
  - LongMemEval_s p95: `64.28 ms` vs C++ `51.15 ms`
- Ingestion throughput is also close:
  - LOCOMO: Rust is about `5.05%` lower than C++
  - LongMemEval_s: Rust is about `6.16%` lower than C++
- The OSS Qwen 1.5B reader/judge path is functional, but quality is weak on
  LOCOMO. The context recall is high, so the remaining score gap is mostly
  reader/judge quality and answer packing, not backend storage correctness.
- The next parity gap is adding a real OSS embedding provider path to the
  benchmark runner and MatrixArk service so `nomic-embed-text` or
  `sentence-transformers/all-MiniLM-L6-v2` replaces `hashing:hashing-local`.

## Commands Used

The four runs used the shared benchmark runner:

```bash
cd /root/src/github-services/TemporalStoreTestCorpus
OPENAI_API_KEY=dummy python3 tools/run_matrixark_dataset_benchmark.py \
  --consumer-repo /root/src/github-services/TemporalStore \
  --dataset locomo \
  --data-path /root/matrixark_benchmarks/data/locomo10.json \
  --backend temporalstore-direct \
  --reader-provider openai-compatible \
  --judge-provider openai-compatible \
  --reader-model qwen2.5:1.5b \
  --judge-model qwen2.5:1.5b \
  --openai-base-url http://127.0.0.1:11434/v1 \
  --question-limit 5 \
  --conversation-limit 1
```

The Rust run changes only `--backend temporalstore-rust`. LongMemEval_s changes
`--dataset longmemeval_s`, `--data-path
/root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json`, and
uses `--question-limit 5 --sessions-per-item-limit 5`.
