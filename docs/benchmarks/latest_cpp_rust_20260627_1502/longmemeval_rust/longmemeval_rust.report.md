# MatrixArk longmemeval_s TemporalStore Benchmark

- backend: `temporalstore-rust`
- metaserver: `127.0.0.1:18000`
- storage prefix: `matrixark:dataset:longmemeval_s:longmemeval_rust`
- storage mode: `multi_node`
- oplog mode: `async`
- replication mode: `shared_store`
- audit mode: `buffered`
- record bundle max bytes: `65536`
- embedding: `hash:matrixark-local-token-hash-v1`
- embedding execution: `runtime_unconfirmed`
- embedding runtime confirmed: `False`
- questions: `10`
- turns ingested: `2042`
- sessions: `200`
- context recall: `1.0000`
- answer hit: `0.2000`
- answer support hit: `0.5000`
- final judge score debug: `0.5000`
- answer-bearing token density: `0.0025`
- judge score per 1K tokens: `0.0500`
- evidence session recall: `0.1000`
- ingestion throughput turns/sec: `171.136`
- retrieval p50 ms: `1519.078`
- retrieval p95 ms: `1916.716`
- reader: `deterministic:matrixark-context-substring-v1`
- judge: `deterministic:matrixark-local-support-v1`
- embedding: `hash:matrixark-local-token-hash-v1`

MatrixArk scores should be compared separately from VikingMem paper numbers until the same dataset, reader, judge, prompt, and scoring protocol are used.
