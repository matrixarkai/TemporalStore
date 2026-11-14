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
| **WindowReduce Features** | Merge cell snapshots, reduce member statistics over a time window, and rank candidates by counts, ratios, or weighted scores | [Cell-based feature model](#windowreduce-features), [worked example](docs/CELLS_WORKED_EXAMPLE.md) |
| **Control state** | Set/get online state, atomic single-key counters, quotas, pacing, suppression, and risk/fraud decisions | [Control-state guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_control_state_frequency_caps.md) |
| **Frequency caps** | Per-user, campaign, or placement exposure limits using windowed counters | [Frequency-cap guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_control_state_frequency_caps.md) |
| **LLM context and agent memory** | Conversations, resources, entities, reusable memory, retrieval, and token-budgeted context assembly | [Context guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/context_management_on_temporalstore.md) |
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
See [latency, amplification, and allocation choices](integrations/feast/feast_temporalstore_command/README.md#latency-amplification-and-allocations)
before comparing latest-only serving, history queries, or another backend. Provider
vector-search hooks and offline point-in-time retrieval are not implemented here.

### Configure Feast

Start the data node with the [quick start](#quick-start). From this checkout,
install Feast and enable the native provider on Python's import path:

```bash
pip install feast msgpack
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

A cell-based model within Feature stores timestamped snapshots of members,
labels, and numeric lanes. Writes merge lanes with declared reducers.
`CellRankQuery` selects a window, filters, reduces per member, scores, returns
bounded top-k, and projects fields. `CellRollup` folds old snapshots into coarser
time buckets and can cap each label group, so a key written every minute does
not keep every snapshot. Build engagement rankings, conversion rates,
and multi-signal features alongside scalar aggregates, with explicit schema and
query bounds.

The vocabulary is small. Nothing in the engine knows what the names mean; a
deployment declares them once, in the header its first write carries, and every
later write is checked against that header.

| Term | Meaning |
|---|---|
| **Entity** and **dimensions** | An entity (a `u64`, text or bytes) plus up to eight named dimensions become one stored feature key, so a read by those values is one lookup. |
| **Snapshot** | The set of cells stored at one timestamp of that key. |
| **Cell** | A **member** (an id or bytes), up to four named `u64` **labels**, and one to sixteen named `i64` **lanes**. |
| **Lane reducer** | The verb that merges two values of a lane for one member: `Sum`, `Max`, `Min`, `First`, `Last` or `Count`. |

#### Use cases

Each scenario below is exercised by a test named in the last column.

| Scenario | How the cell model is used | Test |
|---|---|---|
| **Per-user engagement counters by channel and content kind** | Entity = user; dimensions = `channel`, `kind`; members = items shown; lanes = `impressions`, `clicks` (both `Sum`). Writes at the same timestamp merge into one snapshot and the receipt says what was created and what merged. | `a_worked_example_of_cells` |
| **Per-item rolling ratios** | `CellRankQuery` over `Last { duration_ms: 60_000 }` reduces each item across the window and scores it by `Ratio` (click rate) or `Wilson` (a lower confidence bound that does not trust a rate seen once). In the example the two scores order the same three items oppositely. | `a_worked_example_of_cells` |
| **Best items for a placement** | A predicate on a label (`slot < 3`) cuts cells before a member's snapshots are reduced, then the read ranks the rest. | `a_worked_example_of_cells` |
| **Rolling history compacted into coarser buckets** | `CellRollup` with `older_than_ms` and `bucket_ms` folds minute snapshots into one snapshot per hour. Each lane folds by its own verb: summed lanes add, a `Max` lane keeps the peak, a `Last` lane keeps the newest value. Rolling an already-rolled bucket again changes nothing. | `a_worked_example_of_a_rollup` |
| **Bounded member sets per label** | The rollup's optional cap keeps the best `keep` cells of each label group, scored like a ranked read, so a long-lived key does not grow without bound. | `a_worked_example_of_a_rollup` |
| **Multi-signal features from one key** | One header can declare up to sixteen lanes with different reducers (counts, maxima, latest values), read back together in one ranked read with `Weighted` scores. | `a_cell_rollup_is_a_command`, `a_ranked_cell_read_is_a_command` |

The tests are in `crates/temporalstore-rust/src/engine/tests/`. The write, read
and rollup walk-throughs, with the numbers each step produces, are in
[Cells: merged, windowed, ranked statistics](docs/CELLS_WORKED_EXAMPLE.md).

#### Commands

| Command | What it does |
|---|---|
| `FeatureAppendWithPolicy` with `MergeReduce` | Merges the request's cells into the snapshot stored at each point's timestamp, lane by lane. Answers a receipt: snapshots written, cells created, cells merged. Mismatch with the stored header is refused naming the differing field. |
| `CellRankQuery` | Window (`Between`, `Last`, `Since`, `LastN`), named predicates over labels, lanes or `member`, a score (`Lane`, `Ratio`, `Wilson`, `Member`, `Time`, `Weighted`), `top_k`, and optional projected fields. A name the key does not declare is refused. |
| `CellRollup` | Folds every snapshot stamped strictly before `older_than_ms` into `bucket_ms` buckets (cells merge by member, oldest first) and optionally caps each group. Answers snapshots and cells before and after, and cells dropped. A rollup that would change nothing writes nothing. |

The shape of each request, copied from the tests:

```rust
// Write: merge cells into the snapshot at a timestamp.
Command::FeatureAppendWithPolicy {
    key: key.to_string(),
    points: vec![FeaturePoint { timestamp_ms, value: encode_value(&header(), cells).unwrap() }],
    policy: FeatureWritePolicy::MergeReduce,
}

// Read: rank the last minute by click rate.
Command::CellRankQuery {
    key: key.to_string(),
    spec: CellRankSpec {
        window: CellWindow::Last { duration_ms: 60_000 },
        filters,
        score: CellScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
        top_k: 10,
        project: Vec::new(),
    },
}

// Roll old snapshots into hours, keeping the best item of each slot.
Command::CellRollup {
    key: key.to_string(),
    spec: CellRollupSpec {
        older_than_ms: hour,
        bucket_ms: hour,
        cap: Some(CellCapSpec {
            group_by: Some("slot".to_string()),
            score: CellScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
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
| Cells per write request, cells per snapshot | 1,024, 4,096 |
| `top_k` | 1,000 |
| Snapshots in one ranked window | 512 (a wider window is refused, not truncated) |
| Predicates per read, lanes in a `Weighted` score | 8, 8 |
| Snapshots below one rollup cutoff | 1,024 (roll a longer history in steps by moving the cutoff forward) |
| Cells kept per group by a rollup cap | 1 to 4,096 |

A rollup bucket whose members would exceed the 4,096 cells of one snapshot is
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

Open pull requests, not yet part of `main`: a time-decayed score for ranked
reads (#3090), a ranked read that guarantees members with a non-zero value in chosen lanes (#3094), a ranked read that merges the
cells of several keys (#3093), a per-key retry ring so a retried write can be
recognized (#3087), the wire messages and translator for these commands (#3088),
tiered rollup relative to the newest snapshot (#3095), and a typed reducer for
feature aggregates (#2864). Until they merge, treat them as unavailable.

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
cache, consensus, and object-store components are Rust; MatrixArk is Python
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
