# TemporalStore

**Open-source Rust storage for online features, LLM context, and control state.**

Feature serving and LLM context are equally important, first-class workloads.
Serve latest values, long histories, high-cardinality aggregates, ranked window
features, frequency caps, and agent memory from the same temporal storage engine.
Context management is implemented in TemporalStore, including ingestion,
retrieval, ranking, token-budgeted packing, and its APIs and SDKs.

**Apache-2.0, available worldwide for use, modification, commercial deployment,
and contributions under the license.**
[Documentation](https://github.com/matrixarkai/TemporalStore/tree/main/docs) |
[Issues](https://github.com/matrixarkai/TemporalStore/issues)

## Use cases

| Workload | What you can build | Start here |
|---|---|---|
| **Feast online serving** | Materialize features and retrieve current entity values through Feast, with explicit timestamp, expiry, and acknowledgement policies | [Feast integration](https://github.com/matrixarkai/TemporalStore/tree/main/integrations/feast) |
| **Latest features and profiles** | User/item profiles, lookup features, hashes, and online state for recommendation, search, ads, and personalization | [One-cluster architecture](https://github.com/matrixarkai/TemporalStore/blob/main/docs/TEMPORALSTORE_ONE_CLUSTER_TEMPORAL_FEATURES.md) |
| **Long-sequence features** | Recent clicks, sessions, interactions, and typed behavior histories; timestamp-window reads and count-based retention | [Sequences and aggregates](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_feature_sequences_and_aggregates.md) |
| **High-cardinality features** | Separate user-item, user-category, user-author, device, or campaign histories; exact windowed count, sum, average, min/max, and first/last | [Aggregate example](https://github.com/matrixarkai/TemporalStore/blob/main/docs/feature_aggregate_high_cardinality_example.md) |
| **WindowReduce Features** | Merge cell snapshots, reduce member statistics over a time window, and rank candidates by counts, ratios, or weighted scores | [Cell-based feature model](#windowreduce-features) |
| **Control state** | Set/get online state, atomic single-key counters, quotas, pacing, suppression, and risk/fraud decisions | [Control-state guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_control_state_frequency_caps.md) |
| **Frequency caps** | Per-user, campaign, or placement exposure limits using windowed counters | [Frequency-cap guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_control_state_frequency_caps.md) |
| **LLM context and agent memory** | Conversations, resources, entities, reusable memory, retrieval, and token-budgeted context assembly | [Context guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/context_management_on_temporalstore.md) |
| **Collections, blobs, and streams** | Membership/deduplication, sorted sets, lists, multipart blobs, range reads, and batch/stream ingestion | [Command surface](https://github.com/matrixarkai/TemporalStore/blob/main/docs/redis_compatibility_matrix.md) |

### WindowReduce Features

A cell-based model within Feature: each timestamped snapshot contains members,
labels, and named numeric lanes. Writes merge lanes using their declared reducers.
`CellRankQuery` selects a window, filters cells, reduces statistics per member,
scores, returns a bounded top-k, and projects the requested fields. This supports
engagement rankings, conversion rates, and multi-signal behavior features without
building a separate serving-time aggregation service. Query and schema bounds
are explicit; ordinary scalar feature aggregates remain available alongside it.

Feast remains a first-class integration, not a replacement for native clients.
Use the [Rust SDK](https://github.com/matrixarkai/TemporalStore/tree/main/sdk/rust/temporalstore),
[Python context SDK](https://github.com/matrixarkai/TemporalStore/tree/main/sdk/python),
HTTP APIs, or the documented RESP surface for the corresponding workloads.

## Recorded benchmark highlights

Selected verified results from checked-in reports, not a new benchmark run.

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
[Linux](https://github.com/matrixarkai/TemporalStore/blob/main/docs/linux_deploy.md) |
[Windows Docker](https://github.com/matrixarkai/TemporalStore/blob/main/docs/windows_docker_install.md) |
[macOS](https://github.com/matrixarkai/TemporalStore/blob/main/docs/macos_deploy.md).

## Architecture and durability

TemporalStore owns temporal indexes, feature and context APIs, packed blocks,
caching, persistence, and storage maintenance. Replicated deployments add metadata
placement and consensus. Asynchronous acceptance and a confirmed persistence
barrier provide different guarantees; choose settings for the required recovery.

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

## Five open-source projects

All five projects are public under **Apache-2.0**, available worldwide for anyone
to use, fork, modify, test, and contribute to under the license. The storage,
cache, consensus, and object-store components are Rust; MatrixArk is Python
companion tooling. Context-management code and serving remain in TemporalStore.

| Project | Role |
|---|---|
| [TemporalStore](https://github.com/matrixarkai/TemporalStore) | Rust feature, context, and control-state serving engine |
| [MatrixCache](https://github.com/matrixarkai/matrixcache) | Rust multi-layer cache |
| [MatrixRaft](https://github.com/matrixarkai/matrixraft) | Rust consensus library |
| [MatrixObject](https://github.com/matrixarkai/MatrixObject) | Rust distributed object storage |
| [MatrixArk](https://github.com/matrixarkai/matrixark) | Optional Python ingestion, retrieval, and feedback tooling |

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
