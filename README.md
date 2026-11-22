# TemporalStore

**Open-source Rust storage for online features, LLM context, and control state.**

Feature serving and LLM context are equally important, first-class workloads.
Serve latest values, long histories, high-cardinality aggregates, WindowReduce
features, frequency caps, and agent memory in one engine. Context ingestion,
retrieval, ranking, token-budgeted packing, APIs, and SDKs are in TemporalStore.

**Apache-2.0, available worldwide for use, modification, commercial deployment,
and contributions under the license.**
[Documentation](https://github.com/matrixarkai/TemporalStore/tree/main/docs) |
[Issues](https://github.com/matrixarkai/TemporalStore/issues)

## Implementation entry points

**On this page:** [Quick start](#quick-start) |
[Use cases](#use-cases) | [Recorded benchmarks](#recorded-benchmark-highlights) |
[Durability](#architecture-and-durability) | [Deployment boundaries](#deployment-boundaries) |
[Contributing](#build-and-contribute).

| Area | Start reading |
|---|---|
| Native Feast provider and supported online-store behavior | [Provider guide](integrations/feast/feast_temporalstore_command/README.md) |
| Storage engine composition | [Engine module](crates/temporalstore-rust/src/engine.rs) |
| Write-ahead log implementation | [WAL module](crates/temporalstore-rust/src/wal.rs) |

The provider guide separates latest values, optional history, aggregates, and tracked
teardown. Enabling one path does not establish the throughput or storage cost of the
others; choose the API and acknowledgement policy before measuring a workload.

## Use cases

| Workload | What you can build | Start here |
|---|---|---|
| **Feast online serving** | Materialize features and retrieve current entity values through Feast, with explicit timestamp, expiry, and acknowledgement policies | [Feast integration](https://github.com/matrixarkai/TemporalStore/tree/main/integrations/feast) |
| **Latest features and profiles** | User/item profiles, lookup features, hashes, and online state for recommendation, search, ads, and personalization | [One-cluster architecture](https://github.com/matrixarkai/TemporalStore/blob/main/docs/TEMPORALSTORE_ONE_CLUSTER_TEMPORAL_FEATURES.md) |
| **Long-sequence features** | Recent clicks, sessions, interactions, and typed behavior histories; timestamp-window reads and count-based retention | [Sequences and aggregates](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_feature_sequences_and_aggregates.md) |
| **High-cardinality features** | Separate user-item, user-category, user-author, device, or campaign histories; exact windowed count, sum, average, min/max, and first/last | [Aggregate example](https://github.com/matrixarkai/TemporalStore/blob/main/docs/feature_aggregate_high_cardinality_example.md) |
| **WindowReduce Features** | Merge cell samples, reduce member statistics over a time window, and rank candidates by counts, ratios, or weighted scores | [Window Reducer model](#windowreduce-features), [worked example](docs/WINDOW_REDUCE_WORKED_EXAMPLE.md) |
| **Control state** | Set/get online state, atomic single-key counters, quotas, pacing, suppression, and risk/fraud decisions | [Control-state guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_control_state_frequency_caps.md) |
| **Frequency caps** | Per-user, campaign, or placement exposure limits using windowed counters | [Frequency-cap guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_control_state_frequency_caps.md) |
| **LLM context and agent memory** | Conversations, resources, entities, reusable memory, retrieval, and token-budgeted context assembly | [Context guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/context_management_on_temporalstore.md) |
| **LLM KV-cache storage** | Serve LMCache/vLLM L2 KV blocks with raw blob transfer and command-backed metadata and expiry | [LMCache integration](integrations/lmcache/README.md) |
| **Collections, blobs, and streams** | Membership/deduplication, sorted sets, lists, multipart blobs, range reads, and batch/stream ingestion | [Command surface](https://github.com/matrixarkai/TemporalStore/blob/main/docs/redis_compatibility_matrix.md) |

### Feast API support

The [native Feast provider](integrations/feast/feast_temporalstore_command/README.md)
is an online store and materialization target, not an offline training store.

| API or workflow | Support |
|---|---|
| `get_online_features` and its async form | Latest-value serving through bounded compact projections; compatible views share entity planning. |
| `online_write_batch` and its async form | Materialization with timestamp suppression, optional TTL, and bounded requests; atomic timestamp checks apply to the eligible latest-only route. |
| `query_feature_aggregate` / `query_feature_aggregates` | Provider extensions for retained history; numeric reducers require integer features and the selected history layout. |
| Precomputed vector read/write | Store and fetch vector-valued features; this does not provide provider vector search. |
| `update` / `teardown` | Schemaless updates and tracked cleanup; exact teardown requires key tracking before materialization. |

Feast async calls change client scheduling, not the node's persistence policy.
Eligible latest-only materialization uses interleaved atomic row groups by
default, with a final synchronous durability fence and older-node fallback.
Set `interleave_materialization: false` to keep the original atomic batch route.
See [latency, amplification, and allocation choices](integrations/feast/feast_temporalstore_command/README.md#latency-amplification-and-allocations)
before comparing latest-only serving, history queries, or another backend. Provider
vector-search hooks and offline point-in-time retrieval are not implemented here.

### Configure Feast

Start the data node with the [quick start](#quick-start). From this checkout,
install Feast and enable the native provider on Python's import path:

```bash
pip install feast msgpack 'aiohttp>=3.9,<4'
export PYTHONPATH="$PWD/integrations/feast${PYTHONPATH:+:$PYTHONPATH}"
```

In your existing Feast repository, keep its project, registry, provider, entity
definitions, and offline sources. Set its `feature_store.yaml` online-store block:

```yaml
online_store:
  type: feast_temporalstore_command.TemporalStoreCommandOnlineStore
  host: 127.0.0.1
  port: 17102
  shard_id: 1
  max_batch_commands: 512
  max_write_batch_commands: 4096
  max_batch_request_bytes: 4194304
  skip_dedup: false
  adaptive_timestamp_prefetch: true
  key_ttl_seconds: null
  write_feature_history: false
  track_keys_for_teardown: false
```

Use the reachable data-node address and assigned shard for your deployment.
The configuration above selects latest-value serving without history, expiry,
or teardown tracking. Set a positive `key_ttl_seconds` to enable whole-key expiry;
enable `track_keys_for_teardown` before writing if exact cleanup is required.
History and tracking add storage work and can change the write route.

Run Feast's normal workflow from that repository, with the provider import path
still exported:

```bash
feast apply
feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)"
```

Read using `FeatureStore(repo_path=".").get_online_features(...)` with your
registered feature names and entity rows. TemporalStore serves the materialized
online values; Feast's configured offline source and materialization engine
remain responsible for loading them. Async API calls do not relax the node's
durability policy. See the [provider configuration and API guide](integrations/feast/feast_temporalstore_command/README.md)
for history, aggregates, async calls, older-node behavior, and cleanup limits.

### Read features with Feast

After registering and materializing a feature view, use Feast's public APIs.
Replace `driver_stats:trips` and `driver_id` with a registered feature and its
entity join key in your repository:

```python
import asyncio
from feast import FeatureStore

store = FeatureStore(repo_path=".")
request = {
    "features": ["driver_stats:trips"],
    "entity_rows": [{"driver_id": 1001}, {"driver_id": 1002}],
}

values = store.get_online_features(**request).to_dict()
print(values)

async def read_features():
    response = await store.get_online_features_async(**request)
    return response.to_dict()

async_values = asyncio.run(read_features())
print(async_values)
```

In an existing async application, await `read_features()` on its running event
loop instead of calling `asyncio.run`. An entity with no materialized value
returns a missing feature, not a numeric zero. Both APIs serve the latest online
values; enabling history does not turn them into point-in-time training queries.
Async reads reuse loop-owned HTTP pools. Reuse the store during serving and await
`store._get_provider().online_store.aclose()` once at application shutdown;
applications managing loops manually must shut down async generators before
closing those loops. Async writes retain their blocking-worker path.
See the [provider API coverage](integrations/feast/feast_temporalstore_command/README.md#what-of-feasts-onlinestore-contract-is-covered)
for supported hooks and the separate history and aggregate extensions.

### Choose a Feast materialization path

Use the [native provider guide](integrations/feast/feast_temporalstore_command/README.md)
to configure these independently; enabling latest-value serving does not enable them all.

| Requirement | Configuration and consequence |
|---|---|
| Reject stale latest-value updates | Keep `skip_dedup: false`; common latest-only, shared-layout conditional routes compare timestamps and mutate under one server shard guard. |
| Expire entity state | Set `key_ttl_seconds`; rejected stale rows do not refresh expiry on the conditional TTL route. |
| Retain feature history | Enable `write_feature_history`; this adds writes and uses a different path from latest-only materialization. |
| Enumerate keys during teardown | Enable `track_keys_for_teardown` before materializing; without tracking, teardown cannot enumerate existing rows. |
| Reduce serialization for mostly stale batches | Keep `adaptive_timestamp_prefetch: true`; a per-thread routing hint can add a fresh timestamp pre-read, while eligible survivors still use server-side conditional writes. |

History, tracking, mixed layouts, and older-node fallbacks can use a separate
timestamp-read and write sequence. That fallback is not an atomic conditional
update across concurrent writers. `skip_dedup: true` disables stored-timestamp
suppression; out-of-order materialization can then replace newer values.

The native provider defaults to 4,096 write commands per request, also bounded
by 4 MiB of encoded data; reads retain the 512-command response bound. A caller
batch can therefore span several requests, not one atomic transaction. Measure
the selected history, tracking, TTL, and acknowledgement configuration separately,
including maintenance and retained WAL bytes, not just accepted rows per second.

Eligible synchronous shared-row writes now leave cold packed payloads out of the
payload cache until a read requests them. Updates to an already cached frame can
refresh it eagerly, using one lookup of the first selected field rather than a
scan of the projection. This reduces write-side copying without increasing the
cache budget; cold reads can instead fetch and decompress a WAL group.

Set `TS_HASH_WRITE_AROUND=0` before startup to retain eager admission. Async,
bulk, replay, consensus-apply, and memory-limited paths keep their existing
behavior. Location registration and durability barriers are unchanged. See
[indexed WAL reads and cache admission](docs/indexed_wal_reads.md) for eligibility
and memory costs; compare mixed reads and writes, not write QPS alone.

### WindowReduce Features

The Window Reducer (WindowReduce) is a cell-based model within Feature. It stores
timestamped samples of members, labels, and numeric lanes. Writes merge lanes with declared reducers.
`WindowReduceRank` selects a window, filters, reduces per member, scores, returns
bounded top-k, and projects fields; it can also restrict the members it reads,
end a trailing window at the newest stored sample, guarantee members in chosen
lanes, and score by a decayed sum. `WindowReduceFanInRank` ranks the merged cells of
several keys and `WindowReduceRankBatch` answers several independent reads in one
request. `WindowReduceRollup` folds old samples into coarser time buckets and can cap
each label group, so a key written every minute does not keep every sample. A
retried write carries a token, so sending it twice does not count it twice. Build
engagement rankings, conversion rates, and multi-signal features alongside
scalar aggregates, with explicit schema and query bounds.

The vocabulary is small. Nothing in the engine knows what the names mean; a
deployment declares them once, in the header its first write carries, and every
later write is checked against that header.

| Term | Meaning |
|---|---|
| **Window Reducer** (WindowReduce) | The model: samples of cells under one key, merged by lane reducers on write and reduced over a time window on read. |
| **Entity** and **dimensions** | An entity (a `u64`, text or bytes) plus up to eight named dimensions become one stored feature key, so a read by those values is one lookup. |
| **Sample** | The set of cells stored at one timestamp of that key. |
| **Cell** | A **member** (an id or bytes), up to four named `u64` **labels**, and one to sixteen named `i64` **lanes**. |
| **Lane reducer** | The verb that merges two values of a lane for one member: `Sum`, `Max`, `Min`, `First`, `Last` or `Count`. |

#### Use cases

Each scenario below is exercised by a test named in the last column.

| Scenario | How the Window Reducer is used | Test |
|---|---|---|
| **Per-user engagement counters by channel and content kind** | Entity = user; dimensions = `channel`, `kind`; members = items shown; lanes = `impressions`, `clicks` (both `Sum`). Writes at the same timestamp merge into one sample and the receipt says what was created and what merged. | `a_worked_example_of_window_reduce` |
| **Per-item rolling ratios** | `WindowReduceRank` over `Last { duration_ms: 60_000 }` reduces each item across the window and scores it by `Ratio` (click rate) or `Wilson` (a lower confidence bound that does not trust a rate seen once). In the example the two scores order the same three items oppositely. | `a_worked_example_of_window_reduce` |
| **Best items for a placement** | A predicate on a label (`slot < 3`) cuts cells before a member's samples are reduced, then the read ranks the rest. | `a_worked_example_of_window_reduce` |
| **Rolling history compacted into coarser buckets** | `WindowReduceRollup` with `older_than_ms` and `bucket_ms` folds minute samples into one sample per hour. Each lane folds by its own verb: summed lanes add, a `Max` lane keeps the peak, a `Last` lane keeps the newest value. Rolling an already-rolled bucket again changes nothing. | `a_worked_example_of_a_rollup` |
| **Bounded member sets per label** | The rollup's optional cap keeps the best `keep` cells of each label group, scored like a ranked read, so a long-lived key does not grow without bound. | `a_worked_example_of_a_rollup` |
| **Ranking inside a chosen set of members** | A member set on the read: `include` names the members to read and `exclude` those to leave out (exclusion wins), applied before a member's samples are reduced. Used for "rank these candidates" and "rank everything except what was already shown". | `a_member_set_changes_the_answer_exactly_as_an_independent_fold_says` |
| **A window that follows the data, not the clock** | `anchor: newest` ends a `Last` window at the newest stored sample, so a key whose writes stopped (a paused or replayed feed) reads the same whenever it is read. | `an_anchored_window_reads_the_newest_sample_whatever_the_clock_says` |
| **Guaranteed minimum per lane** | A quota keeps at least so many members with a non-zero `clicks` value among the top-k, even when the best-ranked members have none. It never returns more than `top_k` rows. | `a_quota_selects_exactly_as_a_fold_of_the_ranking_says` |
| **Recency-weighted scores** | `Decay { lane, half_life_ms }` scores a member by its stored values, each halved for every half-life of age from the newest sample of the window. No clock is read. | `a_decay_score_answers_its_hand_computed_value_live_and_after_a_restart` |
| **One question over several keys** | When `channel` is a dimension, "the best items for this user over every channel" spans keys. `WindowReduceFanInRank` merges the cells of up to 64 keys before it scores, so the answer is the one a single key holding all their samples would give. | `a_worked_example_of_a_ranked_read_across_the_channels_of_one_user` |
| **Several independent reads in one request** | `WindowReduceRankBatch` carries up to 64 reads, each with its own key and spec, and answers them in request order; one failing read fails no other. | `a_batch_of_reads_answers_each_as_the_single_read_would_in_request_order_and_one_failing_read_fails_no_other` |
| **A write that can be sent twice** | A merge-reduce append may carry a `retry_token`. The same token on the same key answers the first receipt with `replayed` set and writes nothing; a key remembers its last 16 tokens. | `a_repeated_token_answers_the_first_receipt_and_writes_nothing`, `the_ring_holds_sixteen_through_the_engine_and_the_seventeenth_token_evicts_the_first` |
| **Rolling history in tiers** | The rollup fold accepts an ordered list of tiers, finest first: recent history stays at a fine bucket and older history gets a coarser one, with an optional catch-all that folds everything older into one sample at stamp 0. Today this is the pure function inside the crate; its command is not on `main` yet (see In progress). | `a_tiered_rollup_equals_an_independent_fold_with_a_bucket_per_tier` (unit test, `engine::window_reduce_rollup::tier_tests`) |
| **Multi-signal features from one key** | One header can declare up to sixteen lanes with different reducers (counts, maxima, latest values), read back together in one ranked read with `Weighted` scores. | `a_window_reduce_rollup_is_a_command`, `a_window_reduce_ranked_read_is_a_command` |

The tests are in `crates/temporalstore-rust/src/engine/tests/`. The write, read
and rollup walk-throughs, with the numbers each step produces, are in
[Window Reducer: merged, windowed, ranked statistics](docs/WINDOW_REDUCE_WORKED_EXAMPLE.md).

#### Commands

| Command | What it does |
|---|---|
| `FeatureAppendWithPolicy` with `MergeReduce` | Merges the request's cells into the sample stored at each point's timestamp, lane by lane. Answers a receipt: samples written, cells created, cells merged. Mismatch with the stored header is refused naming the differing field. |
| `WindowReduceRank` | Window (`Between`, `Last`, `Since`, `LastN`, `NewestRows`), named predicates over labels, lanes or `member`, a score (`Lane`, `Ratio`, `Wilson`, `Member`, `Time`, `Weighted`, `Decay`), `top_k`, and optional projected fields. Optional controls, left out of the encoding when unused: a member set (`members`: `include`, `exclude`), an `anchor` (`request` or `newest`), and a `quota`. A name the key does not declare is refused. |
| `WindowReduceFanInRank` | The same read over up to 64 keys that share one spec. The cells of every key are merged before they are scored; the answer says how many keys held a sample in the window. Keys are read on one shard. |
| `WindowReduceRankBatch` | Up to 64 independent reads, each with its own key and spec, answered in request order. A read that cannot be served answers with its reason and fails no other. |
| `WindowReduceRollup` | Folds every sample stamped strictly before `older_than_ms` into `bucket_ms` buckets (cells merge by member, oldest first) and optionally caps each group. Answers samples and cells before and after, and cells dropped. A rollup that would change nothing writes nothing. |
| `WindowReduceRemove` | Removes cells from one key by time window (stamps, inclusive), member set (include and exclude) and predicates over a label, a lane or `member`, at most 8, ANDed. Answers samples before and after, cells in the window, and cells removed. A sample left with no cell is removed; a request that restricts nothing is refused and one that matches nothing writes nothing. |

The shape of each request, copied from the tests:

```rust
// Write: merge cells into the sample at a timestamp.
Command::FeatureAppendWithPolicy {
    key: key.to_string(),
    points: vec![FeaturePoint { timestamp_ms, value: encode_value(&header(), cells).unwrap() }],
    policy: FeatureWritePolicy::MergeReduce,
    retry_token: None, // Some(token) makes a resend answer the first receipt
}

// Read: rank the last minute by click rate.
Command::WindowReduceRank {
    key: key.to_string(),
    spec: WindowReduceRankSpec {
        window: WindowReduceWindow::Last { duration_ms: 60_000 },
        filters,
        score: WindowReduceScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
        top_k: 10,
        project: Vec::new(),
        members: CellMemberFilter::default(),
        anchor: WindowReduceAnchor::Request,
        quota: WindowReduceQuota::default(),
    },
}

// The same read, narrowed: only these members, none of those, the window ends at the newest
// stored sample, and at least two members with a click are guaranteed a place.
let narrowed = WindowReduceRankSpec {
    members: CellMemberFilter {
        include: Some(vec![CellMemberId::U64(1), CellMemberId::U64(2), CellMemberId::U64(3)]),
        exclude: vec![CellMemberId::U64(2)],
    },
    anchor: WindowReduceAnchor::Newest,
    quota: WindowReduceQuota { lanes: vec![WindowReduceLaneQuota { lane: "clicks".to_string(), count: 2 }], others: 0 },
    ..WindowReduceRankSpec::new(
        WindowReduceWindow::Last { duration_ms: 60_000 },
        WindowReduceScore::Lane { lane: "impressions".to_string() },
        10,
    )
};

// Recency-weighted: clicks halved for every 10 minutes of age from the newest sample.
let decayed = WindowReduceScore::Decay { lane: "clicks".to_string(), half_life_ms: 600_000 };

// One question over every channel of one user: the keys are merged before they are scored.
Command::WindowReduceFanInRank { keys: vec![email_key.clone(), push_key.clone(), feed_key], spec: narrowed.clone() }

// Independent reads, answered in request order.
Command::WindowReduceRankBatch {
    queries: vec![
        WindowReduceRankBatchItem { key: email_key.clone(), spec: narrowed.clone() },
        WindowReduceRankBatchItem { key: push_key, spec: narrowed },
    ],
}

// Roll old samples into hours, keeping the best item of each slot.
Command::WindowReduceRollup {
    key: key.to_string(),
    spec: WindowReduceRollupSpec {
        older_than_ms: hour,
        bucket_ms: hour,
        cap: Some(WindowReduceCapSpec {
            group_by: Some("slot".to_string()),
            score: WindowReduceScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
            keep: 1,
        }),
    },
}
```

#### Limits

A request over a limit is refused, not truncated.

| Limit | Value |
|---|---|
| Dimensions per key, labels per cell, lanes per cell | 8, 4, 16 |
| Member bytes, entity bytes, text dimension bytes | 256 each |
| Field name bytes | 64 |
| Cells per point of a write request, cells per sample | 1,024, 4,096 |
| Cells in all the points of one merge-reduce command | 262,144 |
| `top_k` | 1,000 |
| Samples in one ranked window | 512 (a wider window is refused, not truncated) |
| Predicates per read, lanes in a `Weighted` score | 8, 8 |
| Members in a read's `include` set, and in its `exclude` set | 1,024 each, as sent |
| Lanes in a quota, members a quota reserves (lane counts plus `others`) | 16, 1,000 |
| Half-life of a `Decay` score | 1 ms to ten years |
| Keys, samples and cells in one fan-in read | 64, 1,024, 262,144 |
| Reads in one batch | 64 |
| Retry tokens a key remembers | the last 16 |
| Samples below one rollup cutoff | 1,024 (roll a longer history in steps by moving the cutoff forward) |
| Cells kept per group by a rollup cap | 1 to 4,096 |

A rollup bucket whose members would exceed the 4,096 cells of one sample is
refused; use a smaller bucket. A key has one time-to-live, as every feature key
does; a time-to-live per group is not provided. The in-process C-string door
refuses `MergeReduce` because it cannot carry a binary value; use the proxy
client.

#### Measured write cost

A merge-reduce write spends a fixed number of allocations whatever the cell
count: 130.9 per write at 50, 150 and 1,000 cells, down from 687.8, 1,699.7 and
10,211.8 (release build, `alloc-probe` feature, 8-byte key, local measurement).
Source: the `#[ignore]` measurement `a_merge_write_does_not_allocate_per_cell`
and pull request #3076. This counts allocations, not latency or throughput.

#### In progress

Open pull request, not yet part of `main`: a typed reducer for feature aggregates
(#2864). Until it merges, treat it as unavailable. The Window Reducer under its
`WindowReduce*` names, its rollup command with tiers, the member set, anchor,
quota, decay score, fan-in and batch reads and the retry ring described above are
on `main`; the wire messages and the translator that maps them are described in
[Window Reducer protocol](docs/WINDOW_REDUCE_PROTOCOL.md). The stored key tag
`cells` and the retry item kind `cell_retry` keep their original spelling because
they are written to disk.

Feast remains a first-class integration, not a replacement for native clients.
Use the [Rust SDK](https://github.com/matrixarkai/TemporalStore/tree/main/sdk/rust/temporalstore),
[Python context SDK](https://github.com/matrixarkai/TemporalStore/tree/main/sdk/python),
HTTP APIs, or the documented RESP surface for the corresponding workloads.

The Rust memory API uses `infer=false` for explicit messages without a chat-model
call. With a configured model, `infer=true` creates separate memories from
extracted facts rather than cataloging raw conversations. Internal inference
uses protobuf and bounded fact receipts, reusing existing entity vectors.
See [inferred memory behavior and coordinated rollout](docs/MEMORY_INFERRED_FACTS.md).

## Recorded benchmark highlights

Selected verified results from checked-in reports, not a new benchmark run.

For a service comparison, report both rows/s and requests/s, batch size, read/write
concurrency, and p50/p95/p99 latency. Keep acknowledgement policy, replication,
payloads, cache budget, and storage medium explicit. A warm read-only run or an
asynchronously accepted write is not a substitute for a durable mixed workload.
Report physical bytes written per accepted logical byte separately from retained
storage per live logical byte; expiry and reclamation change the latter over time.

| Workload | Recorded result | Scope |
|---|---|---|
| Feature-history ingestion | **14,965-15,260 events/s** | Durable commit; 1,024 entities x 64 events; batches of 256; default 16 MiB cache |
| Batched feature aggregates | **4,207 queries/s**, batch p99 **16.906 ms** | 64 queries/call; repeated runs and exact value checks |
| Wide feature reads | **12,289 warm rows/s** | Async commit; 4,096 entities; 12,296 bytes/row; batch 10; isolated tmpfs; maintenance disabled |
| Binary feature transport | **24.99% fewer request bytes; 4.93x faster client encoding** | Same 4,096-command, 12,296-byte-payload probe against JSON; not server throughput |
| Context quality | **6.466/10 augmented**, 4.879 managed-only, 4.117 local-only | Qwen2.5 7B; four judged turns from a ten-turn experiment |

These are local release-build observations, not distributed capacity or
maintenance-under-load claims. Operation units and durability differ; context
quality is model-judged and does not imply universal quality or token savings.
Conditions, controls, and reproduction commands:
[feature-serving and Feast report](https://github.com/matrixarkai/TemporalStore/blob/main/docs/feast_online_store_benchmark.md),
[context results](https://github.com/matrixarkai/TemporalStore/blob/main/docs/benchmarks/local_context_token_quality/multiturn_3arm_multiturn_qwen7b.json),
[benchmark methodology](https://github.com/matrixarkai/TemporalStore/blob/main/docs/benchmarks/README.md).

## Quick start

Prerequisites: Git, Docker, and Docker Compose.

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
docker compose -f docker-compose.single-node.yml up --build -d
curl -fsS http://127.0.0.1:17102/health
```

The Compose setup runs a metaserver on `17101` and a data node on `17102`.
A named volume preserves data across container restarts. `/health` checks process
liveness, not whether every shard can serve requests.

To stop this local deployment while retaining its named data volume:

```bash
docker compose -f docker-compose.single-node.yml down
```

Do not add `--volumes` when retaining data. Use the same checkout and Compose
project name when starting it again so that Compose selects the existing volume.

[Linux](https://github.com/matrixarkai/TemporalStore/blob/main/docs/linux_deploy.md) |
[Windows Docker](https://github.com/matrixarkai/TemporalStore/blob/main/docs/windows_docker_install.md) |
[macOS](https://github.com/matrixarkai/TemporalStore/blob/main/docs/macos_deploy.md).

## Architecture and durability

TemporalStore owns temporal indexes, feature/context APIs, packed blocks, caching,
persistence, and maintenance. Replicated deployments add placement and consensus.
Asynchronous acceptance and confirmed persistence provide different guarantees;
choose settings for the required recovery.

Eligible synchronous shared-row batches release the engine mutation guard before
waiting for their shared WAL commit barrier. Successful writes still wait for a
barrier covering their sequence; readers can observe an in-flight update before
the writer receives durable success. Read visibility alone therefore does not
prove crash durability, and a commit error is not a rollback guarantee. Async,
replay, Raft-application, and index-persisting paths have their own commit rules.
See the [batch implementation](crates/temporalstore-rust/src/engine/stream_batch_methods.rs).

Feature-write batches reuse append state and capture the slab target once per
operation, rather than repeatedly checking the same configuration and directory.
The next operation can observe an updated target. This reduces repeated work;
it does not disable sync barriers, change the WAL format, or establish a service
latency improvement. See the [append path](crates/temporalstore-rust/src/block_store/block_store_append.rs)
and [storage configuration](crates/temporalstore-rust/src/storage_config.rs).

Feast materialization can split one caller batch into several native requests.
Do not treat that caller batch as one atomic transaction. Compare accepted rows,
request counts, durability, and concurrent reader work together when evaluating
batch limits; smaller requests can improve scheduling while adding WAL records
and synchronization work. No universal throughput or latency win is implied.

Maintenance spans preparation, checkpoint/dump, expiry, eviction, compaction,
page/slab, index and WAL reclaim, delayed destruction, and restore. Both workload
families require tested recovery and replication fences, plus measured per-stage
CPU, memory, I/O amplification, throughput, and tail latency. This remains
qualification work, not a blanket production-readiness claim.

[Shared storage](https://github.com/matrixarkai/TemporalStore/blob/main/docs/shared_store_replication.md) |
[Raft and snapshots](https://github.com/matrixarkai/TemporalStore/blob/main/docs/raft_replication_and_snapshots.md) |
[Indexed WAL reads](https://github.com/matrixarkai/TemporalStore/blob/main/docs/indexed_wal_reads.md).

## Deployment boundaries

- Series retention is count-based; whole-key TTL is not per-point age retention.
- General aggregates can scan the requested window; accelerated paths have explicit limits.
- Bounded embedding scoring is not a graph-based ANN index. The built-in hash embedding is for tests; encoder changes require compatible model identity and re-embedding.
- Validate authentication, exposure, backups, replication, and acknowledgement settings before deployment.
- Formats can change before a stable milestone. Pin revisions and test retained-data upgrades.

The engine is Rust-native. Development targets `main`; `rust-main` is retired.
There is no brpc/thrift compatibility layer. Validate production-readiness claims
with deployment guides and readiness reports for the chosen configuration.

## Six open-source projects

All six projects are public under **Apache-2.0**, available worldwide for anyone
to use, fork, modify, test, and contribute to under the license. The storage,
cache, consensus, and object-store parts are Rust; MatrixArk is Python
companion tooling. MatrixAdsDemo is a standalone Python demonstration using
generated data. Context-management code and serving remain in TemporalStore.

| Project | Role |
|---|---|
| [TemporalStore](https://github.com/matrixarkai/TemporalStore) | Rust feature, context, and control-state serving engine |
| [MatrixCache](https://github.com/matrixarkai/matrixcache) | Rust multi-layer cache |
| [MatrixRaft](https://github.com/matrixarkai/matrixraft) | Rust consensus library |
| [MatrixObject](https://github.com/matrixarkai/MatrixObject) | Rust distributed object storage |
| [MatrixArk](https://github.com/matrixarkai/matrixark) | Optional Python ingestion, retrieval, and feedback tooling |
| [MatrixAdsDemo](https://github.com/matrixarkai/MatrixAdsDemo) | Standalone ads retrieval, prediction, and auction demonstration |

## Build and contribute

```bash
cargo check -p temporalstore-rust --all-targets
cargo test -p temporalstore-rust --lib --tests -- --test-threads=1
cargo fmt --all -- --check
```

Development targets `main`. Anyone may open an
[issue](https://github.com/matrixarkai/TemporalStore/issues) or
[pull request](https://github.com/matrixarkai/TemporalStore/pulls).
[Contribution guide](https://github.com/matrixarkai/TemporalStore/blob/main/CONTRIBUTING.md) |
[Code of conduct](https://github.com/matrixarkai/TemporalStore/blob/main/CODE_OF_CONDUCT.md) |
[Security policy](https://github.com/matrixarkai/TemporalStore/blob/main/docs/SECURITY.md).

## License

[Apache License 2.0](https://github.com/matrixarkai/TemporalStore/blob/main/LICENSE) |
[NOTICE](https://github.com/matrixarkai/TemporalStore/blob/main/NOTICE) |
[Third-party notices](https://github.com/matrixarkai/TemporalStore/blob/main/docs/THIRD-PARTY-NOTICES.md) |
[Trademarks](https://github.com/matrixarkai/TemporalStore/blob/main/docs/TRADEMARKS.md).
