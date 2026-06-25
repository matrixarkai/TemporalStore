# MatrixArk locomo TemporalStore Benchmark

- backend: `temporalstore-rust`
- metaserver: `127.0.0.1:18000`
- storage prefix: `matrixark:bench:oss:20260624:locomo_rust_oss_q5`
- questions: `5`
- turns ingested: `419`
- sessions: `19`
- context recall: `1.0000`
- answer hit: `0.2000`
- answer support hit: `0.0000`
- final judge score debug: `0.0000`
- answer-bearing token density: `0.0105`
- judge score per 1K tokens: `0.0000`
- evidence session recall: `0.2000`
- ingestion throughput turns/sec: `227.717`
- retrieval p50 ms: `159.629`
- retrieval p95 ms: `170.239`
- reader: `openai-compatible:qwen2.5:1.5b`
- judge: `openai-compatible:qwen2.5:1.5b`

MatrixArk scores should be compared separately from VikingMem paper numbers until the same dataset, reader, judge, prompt, and scoring protocol are used.
