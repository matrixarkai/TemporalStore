# Data Type Testing With Trace Logging

Run date: 2026-05-27

## Cluster

- Build: `<repo>/build-ubuntu22`
- Metaservers: 2
- Data servers: 2
- Replica count: 2
- Storage URI: local file store under the WSL runtime directory
- Log levels: metaserver `0`, server `0`
- Benchmark ops per case: `5`

## Functional Coverage

`data_type_functional.out` passed all covered modules:

- STRING: `Set` / `Get`
- COMMON: `Expire` / `Ttl` / `Del`
- HASH: `HSet` / `HGet`
- SET: `SAdd` / `SMembers`
- FEATURE: add/query time sequence points
- IPS: add/query instance feature
- RISK: count-window `Hset` / `Hquery`

Both stderr files are empty.

## Smoke Latency

These numbers are small local smoke measurements, not a saturation benchmark.

| Module | Operation | Avg us | P50 us | P95 us | P99 us |
| --- | --- | ---: | ---: | ---: | ---: |
| STRING | ingest_set | 8336 | 7886 | 15695 | 15695 |
| STRING | query_get | 852 | 637 | 1353 | 1353 |
| COMMON | ingest_expire | 7697 | 8090 | 11240 | 11240 |
| COMMON | query_ttl | 788 | 832 | 870 | 870 |
| HASH | ingest_hset | 12371 | 10972 | 19170 | 19170 |
| HASH | query_hget | 1738 | 1092 | 4165 | 4165 |
| SET | ingest_sadd | 7021 | 5540 | 10997 | 10997 |
| SET | query_smembers | 738 | 757 | 1077 | 1077 |
| FEATURE | ingest_add_point | 5369 | 4718 | 7919 | 7919 |
| FEATURE | query_window_one_point | 948 | 749 | 1593 | 1593 |
| IPS | ingest_add_instance | 6985 | 6006 | 10480 | 10480 |
| IPS | query_last_instance | 1099 | 896 | 2558 | 2558 |
| RISK | ingest_hset_count | 10587 | 8518 | 20648 | 20648 |
| RISK | query_hquery_1h_count | 1614 | 1019 | 3933 | 3933 |

## Trace Logging

`trace_summary.txt` contains sampled trace lines for:

- metaserver manage RPCs
- server load and GetInfo RPCs
- command traces with `ModuleId`, `FunctionId`, `TraceId`, and `TimeTracer`

Full logs are under `logs/metaserver1`, `logs/metaserver2`, `logs/server1`, and `logs/server2`.

## Notes

The data-type examples now pin reads to the primary partition. That makes the functional and latency tests deterministic after writes; otherwise a read can immediately hit a secondary before replay catches up and return `NotFound`.
