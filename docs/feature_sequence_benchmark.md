# TemporalStore Sequence Feature Benchmark

This benchmark exercises the real Feature module path for long sequence features: ingest timestamped rows into one object per entity key, then query bounded time windows with no filters, one protobuf-field filter, and multi-field filters.

## Code Path

- Benchmark client: `src/client/example/feature_sequence_benchmark.cc`
- Runner script: `tools/run_feature_sequence_benchmark_ubuntu22.sh`
- Feature API: `src/extension/feature/interface.proto`
- Feature implementation: `src/extension/feature/implement.cc`
- Sequence object model: `src/model/feature_model.h`

The stored value is a serialized protobuf-compatible sequence row with these default fields:

| Field | Type | Example |
|---|---:|---|
| `gid` | `uint64` | item/campaign/content id |
| `action_type` | `uint32` | click/view/purchase action bucket |
| `duration` | `uint32` | dwell or event duration |
| `author_id` | `uint64` | author/seller/merchant id |

## Query Semantics From Code

`FeatureModel` stores one sequence object as an ordered `PersistentMap<uint64_t, string>`, where the map key is timestamp and the value is the serialized row.

Window query behavior:

1. Seek with `LowerBound(start_ts)`.
2. Scan forward while `ts < end_ts`.
3. Stop after `count` scanned rows.
4. Apply optional filters to each scanned row.
5. Append matching rows to `QueryResponse.point_list`.

Filter behavior:

- Filters are strings such as `action_type = 3`, `duration > 120`, `gid < 1002048`.
- Supported operators in the current code are `=`, `!=`, `>`, `<`.
- Multiple filters are ANDed.
- Filters decode the stored protobuf row and evaluate numeric fields.
- There is no secondary index in the current Feature implementation, so filtered windows still scan candidate rows in timestamp order.
- Repeating the same field in multiple filters is not a real range predicate today, because the filter map is keyed by field name and later entries overwrite earlier ones.
- `AggQueryRequest` exists in the proto, but there is no registered `AGGQUERY` implementation in the current code path.

The default `feature_max_size` is `5000`; after an add, old rows are truncated with `DelBegins` if the object exceeds that limit.

## Measured Workload

Environment:

- WSL2 Ubuntu 22.04
- Debug/O0 build
- 2 metaservers, 2 data servers, 2 replicas
- Primary-pinned reads
- Local file-backed storage

Dataset:

- 8 sequence keys
- 5,000 timestamped rows per key
- 40,000 total rows
- 100 query operations per query case
- 8 client threads

Raw results:

- `docs/benchmarks/feature_sequence_20260527/feature_sequence.csv`
- `docs/benchmarks/feature_sequence_20260527/launcher.log`

## Results

| Phase | Window rows | Filters | Total returned/written | QPS | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| ingest add sequence rows | n/a | 0 | 40,000 written | 2 req/s | 3.405s | 3.438s | 3.438s |
| window 100 no filter | 100 | 0 | 10,000 returned | 182 req/s | 24.3ms | 49.7ms | 57.8ms |
| window 1000 action eq 3 | 1,000 | 1 | 20,000 returned | 61 req/s | 119.1ms | 182.3ms | 187.1ms |
| window 1000 complex filters | 1,000 | 3 | 5,552 returned | 56 req/s | 131.8ms | 172.2ms | 174.5ms |
| full window complex filters | 5,000 | 3 | 313,703 returned | 9 req/s | 741.6ms | 1.176s | 1.194s |

## Takeaways

Small bounded windows are the good path. A 100-row unfiltered query stayed in tens of milliseconds in this Debug WSL run.

Filters are functional and correct, but they are scan-and-decode filters. A 1,000-row filtered window moved into roughly 120-175ms p50/p99 in this local Debug build.

Large full-window filtered scans are expensive. The 5,000-row, 3-filter case reached around 742ms p50 and 1.19s p99. This is the expected shape for a timestamp scan with per-row protobuf decode and no secondary index.

For production-style feature serving, keep online windows bounded, use count limits deliberately, and use specialized aggregate/risk-style models when the serving request needs counts/sums over long windows instead of raw sequence rows.

## Re-run

```bash
KEYS=8 ROWS_PER_KEY=5000 QUERY_OPS=100 THREADS=8 WARMUP_SECONDS=8 \
  bash tools/run_feature_sequence_benchmark_ubuntu22.sh
```

The runner starts a temporary local cluster, waits for partition readiness, runs the benchmark, writes `feature_sequence.csv`, and cleans up the cluster.
