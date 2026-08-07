# High-Cardinality FeatureAggregate Example

`feature_aggregate_scale_harness` is a runnable, in-process demonstration that
TemporalStore serves **aggregated features** correctly and quickly over a
**high-cardinality** keyspace. It is the Rust counterpart of the C++
`src/client/example/temporal_aggregate_scale_benchmark.cc` reference, but runs
entirely against `TemporalEngine` (no cluster, no external services), so it is a
deterministic local correctness + performance smoke.

See also: [Feature sequences and aggregates](blog_feature_sequences_and_aggregates.md).

## What it demonstrates

FeatureAggregate lives inside the Feature capability and computes serving-time
aggregates over timestamped observations. Many recommendation / ads / search
systems need those aggregates at **several cardinalities at once**:

```
feature:content_interaction:user:{u}
feature:content_interaction:user:{u}:category:{c}
feature:content_interaction:user:{u}:author:{a}
feature:campaign_delivery:campaign:{c}      # hot, high-volume
```

The harness builds tens of thousands of such distinct aggregate keys, ingests
timestamped observations into each, and then runs serving-time
`FeatureAggQuery` over time windows using the first-release exact aggregate set:
`count`, `sum`, `min`, `max`, `avg`, `first`, `last`.

Every aggregate result is checked against an independent in-harness ground
truth. **Any disagreement is a hard failure** (the process exits non-zero), so a
green run is also an exactness proof.

## Phases

1. **Ingest** — append observations across all keys (async durability), then
   drain the background flush.
2. **Cold sweep** — one `FeatureAggQuery` per key across the whole
   high-cardinality keyspace (`count`/`sum`/`last`); verify each result.
3. **Hot serving** — repeated aggregates over a bounded active working set (the
   realistic online pattern: recent entities are hot), reporting steady-state
   p50/p95/p99 and QPS.

## Run

```bash
cargo run --release -p temporalstore-rust --bin feature_aggregate_scale_harness -- \
    --users 4000 --categories 8 --authors 64 --obs-per-key 8 \
    --campaigns 200 --campaign-obs 300 --hot-keys 512 --rounds 40
```

Flags: `--users --categories --authors --obs-per-key --campaigns --campaign-obs
--hot-keys --rounds --settle-ms --cold-sample`. On a slow or heavily loaded
host, cap the cold sweep with `--cold-sample N` to keep runtime bounded; the
hot-serving phase still exercises many thousands of queries.

Output is CSV:

```
system,phase,keys,observations,queries,mismatches,qps,avg_us,p50_us,p95_us,p99_us,max_us
```

## Result (verified)

Correctness holds at every scale exercised (0 mismatches at 126 and 2,450
distinct keys, across `count`/`sum`/`max`/`avg`/`last`, full-window and bounded
recent-tail windows, and across 24,000 repeated hot-serving queries).

Example run (126 keys, 1,320 observations), hot-serving `sum`:

| phase | queries | mismatches | p50_us | p95_us |
| --- | --- | --- | --- | --- |
| cold_sweep_count | 126 | 0 | ~2,400 | ~12,000 |
| hot_serving_sum | 24,000 | 0 | ~1,000 | ~7,000 |

> Latency is host- and load-dependent. The numbers above were captured on a
> shared WSL host running at ~2x CPU oversubscription (load average ~30 from
> concurrent builds), so they are conservative upper bounds; on an unloaded host
> the per-query cost is well under a millisecond. The headline claim the harness
> proves deterministically is **exactness at high cardinality**.
