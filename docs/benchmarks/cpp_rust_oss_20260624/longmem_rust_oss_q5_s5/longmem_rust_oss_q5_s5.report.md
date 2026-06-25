# MatrixArk longmemeval_s TemporalStore Benchmark

- backend: `temporalstore-rust`
- metaserver: `127.0.0.1:18000`
- storage prefix: `matrixark:bench:oss:20260624:longmem_rust_oss_q5_s5`
- questions: `5`
- turns ingested: `218`
- sessions: `25`
- context recall: `0.8000`
- answer hit: `0.0000`
- answer support hit: `0.2000`
- final judge score debug: `0.2000`
- answer-bearing token density: `0.0459`
- judge score per 1K tokens: `0.2085`
- evidence session recall: `0.0000`
- ingestion throughput turns/sec: `83.429`
- retrieval p50 ms: `56.811`
- retrieval p95 ms: `64.279`
- reader: `openai-compatible:qwen2.5:1.5b`
- judge: `openai-compatible:qwen2.5:1.5b`

MatrixArk scores should be compared separately from VikingMem paper numbers until the same dataset, reader, judge, prompt, and scoring protocol are used.
