# MatrixArk locomo TemporalStore Benchmark

- backend: `temporalstore-rust`
- metaserver: `127.0.0.1:18000`
- storage prefix: `matrixark:dataset:locomo:locomo_rust`
- storage mode: `multi_node`
- oplog mode: `async`
- replication mode: `shared_store`
- audit mode: `buffered`
- record bundle max bytes: `65536`
- embedding: `hash:matrixark-local-token-hash-v1`
- embedding execution: `runtime_unconfirmed`
- embedding runtime confirmed: `False`
- questions: `20`
- turns ingested: `788`
- sessions: `38`
- context recall: `0.9500`
- answer hit: `0.1000`
- answer support hit: `0.6500`
- final judge score debug: `0.6500`
- answer-bearing token density: `0.0033`
- judge score per 1K tokens: `0.1369`
- evidence session recall: `0.7000`
- ingestion throughput turns/sec: `360.641`
- retrieval p50 ms: `524.749`
- retrieval p95 ms: `611.237`
- reader: `deterministic:matrixark-context-substring-v1`
- judge: `deterministic:matrixark-local-support-v1`
- embedding: `hash:matrixark-local-token-hash-v1`

MatrixArk scores should be compared separately from VikingMem paper numbers until the same dataset, reader, judge, prompt, and scoring protocol are used.
