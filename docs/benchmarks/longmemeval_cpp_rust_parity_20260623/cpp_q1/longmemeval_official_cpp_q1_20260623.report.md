# MatrixArk longmemeval_s TemporalStore Benchmark

- backend: `temporalstore-direct`
- metaserver: `127.0.0.1:18000`
- storage prefix: `matrixark:longmem:official:cpp:q1:20260623`
- questions: `1`
- turns ingested: `550`
- sessions: `53`
- context recall: `1.0000`
- answer hit: `0.0000`
- answer support hit: `0.0000`
- final judge score debug: `0.0000`
- answer-bearing token density: `0.0108`
- judge score per 1K tokens: `0.0000`
- evidence session recall: `0.0000`
- ingestion throughput turns/sec: `167.021`
- retrieval p50 ms: `216.231`
- retrieval p95 ms: `216.231`
- reader: `deterministic:matrixark-context-substring-v1`
- judge: `deterministic:matrixark-local-support-v1`

MatrixArk scores should be compared separately from VikingMem paper numbers until the same dataset, reader, judge, prompt, and scoring protocol are used.
