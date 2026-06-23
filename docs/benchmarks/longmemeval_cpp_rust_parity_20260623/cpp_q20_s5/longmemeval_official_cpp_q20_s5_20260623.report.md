# MatrixArk longmemeval_s TemporalStore Benchmark

- backend: `temporalstore-direct`
- metaserver: `127.0.0.1:18000`
- storage prefix: `matrixark:longmem:official:cpp:q20:s5:20260623`
- questions: `20`
- turns ingested: `977`
- sessions: `100`
- context recall: `0.9500`
- answer hit: `0.1000`
- answer support hit: `0.1000`
- final judge score debug: `0.1000`
- answer-bearing token density: `0.0172`
- judge score per 1K tokens: `0.0898`
- evidence session recall: `0.0500`
- ingestion throughput turns/sec: `111.377`
- retrieval p50 ms: `54.147`
- retrieval p95 ms: `92.873`
- reader: `deterministic:matrixark-context-substring-v1`
- judge: `deterministic:matrixark-local-support-v1`

MatrixArk scores should be compared separately from VikingMem paper numbers until the same dataset, reader, judge, prompt, and scoring protocol are used.
