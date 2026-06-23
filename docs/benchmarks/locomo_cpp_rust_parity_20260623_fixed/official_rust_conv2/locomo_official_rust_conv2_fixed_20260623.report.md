# MatrixArk locomo TemporalStore Benchmark

- backend: `temporalstore-rust`
- metaserver: `127.0.0.1:18000`
- storage prefix: `matrixark:locomo:official:rust:conv2:fixed:20260623solo2`
- questions: `100`
- turns ingested: `788`
- sessions: `38`
- context recall: `1.0000`
- answer hit: `0.1000`
- answer support hit: `0.6200`
- final judge score debug: `0.6200`
- answer-bearing token density: `0.0126`
- judge score per 1K tokens: `0.5171`
- evidence session recall: `0.4000`
- ingestion throughput turns/sec: `286.754`
- retrieval p50 ms: `83.262`
- retrieval p95 ms: `206.703`
- reader: `deterministic:matrixark-context-substring-v1`
- judge: `deterministic:matrixark-local-support-v1`

MatrixArk scores should be compared separately from VikingMem paper numbers until the same dataset, reader, judge, prompt, and scoring protocol are used.
