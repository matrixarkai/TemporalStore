# TemporalStore

**Open-source Rust storage for LLM context, temporal features, and control state.**

Context management is the flagship use case: ingest conversations and resources,
maintain reusable memory, and retrieve relevant context within a token budget.
Long sequences, feature aggregates, and frequency caps use the same storage engine.

**Apache-2.0, available worldwide for use, modification, commercial deployment,
and contributions under the license.**
[Website](https://temporalstore.ai) |
[Documentation](https://github.com/matrixarkai/TemporalStore/tree/main/docs) |
[Issues](https://github.com/matrixarkai/TemporalStore/issues)

## Quick start

Prerequisites: Git, Docker, and Docker Compose. Build and start a local node from
the public source:

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
docker compose -f docker-compose.single-node.yml up --build -d
curl -fsS http://127.0.0.1:17102/health
```

The Compose setup runs a metaserver on port `17101` and a data node on `17102`.
Data lives in a named Docker volume and survives container restarts. `/health`
checks process liveness; it is not proof that every shard can serve requests.

Installation: [Linux](https://github.com/matrixarkai/TemporalStore/blob/main/docs/linux_deploy.md) |
[Windows Docker](https://github.com/matrixarkai/TemporalStore/blob/main/docs/windows_docker_install.md) |
[macOS](https://github.com/matrixarkai/TemporalStore/blob/main/docs/macos_deploy.md).

## What it serves

| Capability | Use cases | Start here |
|---|---|---|
| Context | Agent memory, conversations, resources, entities, and budgeted retrieval | [Context guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/context_management_on_temporalstore.md) |
| Features | Time-keyed observations, long sequences, windowed reads, and aggregates | [Feature guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_feature_sequences_and_aggregates.md) |
| Control state | Frequency caps, counters, quotas, pacing, suppression, and risk control | [Control-state guide](https://github.com/matrixarkai/TemporalStore/blob/main/docs/blog_control_state_frequency_caps.md) |

### LLM context

```text
Messages and resources -> ingestion -> extraction and summaries
                      -> candidate selection -> ranking -> token-budgeted pack
```

Context retrieval combines lexical matching with supplied embeddings. Bounded
candidate selection precedes exact scoring and text hydration; selected blocks
are deduplicated and packed after charging the caller's prompt against the budget.
The built-in hash embedding is for tests, not a substitute for a semantic encoder.
Changing encoders requires compatible model identity and re-embedding existing data.

Use the [Python context SDK](https://github.com/matrixarkai/TemporalStore/tree/main/sdk/python)
or [MatrixArk tooling](https://github.com/matrixarkai/matrixark) for ingestion and
retrieval orchestration. Model and parser configuration remain explicit.

### Temporal features and control state

Feature sequences support timestamp windows and count-based retention. Aggregates
include count, sum, average, min/max, and first/last; an optional sum-family rollup
ladder accelerates supported wide-window queries. Whole-key TTL is separate from
per-point retention. Control state provides atomic single-key updates and
windowed counters.

The [Feast integration](https://github.com/matrixarkai/TemporalStore/tree/main/integrations/feast)
offers online feature serving. Native clients include a
[Rust SDK](https://github.com/matrixarkai/TemporalStore/tree/main/sdk/rust/temporalstore),
HTTP command APIs, and a documented
[RESP surface](https://github.com/matrixarkai/TemporalStore/blob/main/docs/redis_compatibility_matrix.md).
Consult the command matrix rather than assuming complete Redis compatibility.

## Architecture and durability

```text
Clients / MatrixArk
        |
TemporalStore: temporal indexes, serving paths, append-structured storage
        |
MatrixCache + MatrixRaft
        |
Local disk / shared filesystem / object-store adapter
```

The engine owns indexed reads, packed blocks, caching, persistence, and storage
maintenance. Distributed deployments add metadata placement and replication.
Cache eviction releases memory; durable space reclamation requires safe storage
maintenance and must respect live indexes and follower progress.

Acknowledgement policy matters: an asynchronous acceptance is not the same as a
confirmed persistence barrier. Choose and verify storage and replication settings
for the required recovery guarantees; do not compare their write rates as if
they provided identical durability.

See [shared storage](https://github.com/matrixarkai/TemporalStore/blob/main/docs/shared_store_replication.md),
[Raft replication](https://github.com/matrixarkai/TemporalStore/blob/main/docs/raft_replication_and_snapshots.md),
and [indexed WAL reads](https://github.com/matrixarkai/TemporalStore/blob/main/docs/indexed_wal_reads.md).

## Recorded benchmarks

Selected measurements from the checked-in reports below, not a new benchmark run.
These are local release-build results on a shared 16-core host, not multi-machine
capacity claims. Rates use different operation units and are not interchangeable.

| Workload | Recorded result | Conditions |
|---|---|---|
| Feature-history ingestion | **14,965-15,260 events/s** across two valid runs | Durable commit; 1,024 entities x 64 events; write batches of 256; default 16 MiB cache |
| Batched history aggregates | **4,207 queries/s**; batch-call p99 **16.906 ms** | 64 queries/call; three repeats: 4,207 / 4,714 / 4,187 queries/s; all value checks passed |
| Wide feature reads | **4,585 async / 9,344 durable-commit rows/s** | Eight readers; ten rows/call; 4,096 entities; 12,296-byte payload/row; tmpfs storage directories; maintenance disabled |

The wide-read figures are separate observed runs, not evidence that durable
commit makes reads faster. Tmpfs results do not establish persistent-device
durability or maintenance-under-load performance. Aggregate p99 is per batch
call, not per individual query. Controls, cleanup checks, and reproduction
commands are in the [feature-serving benchmark report](https://github.com/matrixarkai/TemporalStore/blob/main/docs/feast_online_store_benchmark.md).

Context quality and token cost depend on the reader, workload, and baseline.
The checked-in Qwen2.5 7B multi-turn experiment measured quality on four of ten turns:
local-only **4.117/10**, managed-only **4.879/10**, and augmented **6.466/10**.
Across all ten turns, cumulative input counts were **1,665 / 29,996 / 16,341**
respectively: the managed arms used more tokens on these short conversations.
This is a small model-judged experiment, not a general quality or token-saving
guarantee. See the [raw results](https://github.com/matrixarkai/TemporalStore/blob/main/docs/benchmarks/local_context_token_quality/multiturn_3arm_multiturn_qwen7b.json)
and [benchmark methodology](https://github.com/matrixarkai/TemporalStore/blob/main/docs/benchmarks/README.md).

## Deployment boundaries

- Series retention is count-based; whole-key TTL does not provide per-point age retention.
- General aggregates can scan the requested window; accelerated rollups cover a narrower surface.
- Bounded embedding selection is not a graph-based ANN index or a guarantee of constant retrieval cost.
- Local benchmark results do not prove distributed throughput, failover capacity, or production readiness.
- Review authentication, network exposure, backup/recovery, and acknowledgement settings before deployment.
- Stored formats can change before a stable milestone. Pin revisions and validate upgrades against retained data.

The engine is Rust-native. Development targets `main`; `rust-main` is retired.
There is no brpc/thrift compatibility layer. Validate production-readiness claims
with the deployment guides and readiness reports for the chosen configuration.

## Build and contribute

From a Rust source checkout:

```bash
cargo check -p temporalstore-rust --all-targets
cargo test -p temporalstore-rust --lib --tests -- --test-threads=1
cargo fmt --all -- --check
```

Anyone may open an [issue](https://github.com/matrixarkai/TemporalStore/issues) or
[pull request](https://github.com/matrixarkai/TemporalStore/pulls).
Read the [contribution guide](https://github.com/matrixarkai/TemporalStore/blob/main/CONTRIBUTING.md)
and [code of conduct](https://github.com/matrixarkai/TemporalStore/blob/main/CODE_OF_CONDUCT.md).
Report vulnerabilities through the [security policy](https://github.com/matrixarkai/TemporalStore/blob/main/docs/SECURITY.md).

## Related projects

These five Apache-2.0 repositories form the TemporalStore stack:

| Project | Role |
|---|---|
| [TemporalStore](https://github.com/matrixarkai/TemporalStore) | Rust temporal serving and storage engine |
| [MatrixArk](https://github.com/matrixarkai/matrixark) | Python context tooling and orchestration |
| [MatrixCache](https://github.com/matrixarkai/matrixcache) | Multi-layer cache |
| [MatrixRaft](https://github.com/matrixarkai/matrixraft) | Rust consensus library |
| [MatrixObject](https://github.com/matrixarkai/MatrixObject) | Distributed object storage |

## License

[Apache License 2.0](https://github.com/matrixarkai/TemporalStore/blob/main/LICENSE) |
[NOTICE](https://github.com/matrixarkai/TemporalStore/blob/main/NOTICE) |
[Third-party notices](https://github.com/matrixarkai/TemporalStore/blob/main/docs/THIRD-PARTY-NOTICES.md) |
[Trademarks](https://github.com/matrixarkai/TemporalStore/blob/main/docs/TRADEMARKS.md).
