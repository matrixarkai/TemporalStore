# Distributed Raft Readiness

## Current Status

Distributed Raft now has a separate-node deployment wrapper around the existing data-node Raft
contracts, but it is not production complete yet.

The Rust code currently has:

- production data-node Raft runtime options with OpenRaft/raft-rs engine selection
- `raft_node`, raft-enabled `server`, and `metaserver` process startup construct production runtime
  options with `ProductionRaftEngineKind::OpenRaft` by default
- feature-gated OpenRaft data-node and metaserver adapter with durable log state, state-machine
  apply, snapshot metadata, read-index checks, membership changes, leader transfer, and restart
  recovery tests
- production metaserver Raft runtime options with failover/catch-up timer and stale-server detection
- WAL-backed production runtime startup and recovery
- authenticated production RPC runtime construction over the existing Raft HTTP transport
- mTLS configuration validation for production deployments
- plaintext transport allowed only for local chaos tests
- timer supervisor for election ticks, heartbeat cadence, and follower catch-up
- leader heartbeat loop sends network AppendEntries to secondary data-node processes so restarted
  secondaries can catch up from the leader's log and local WAL state
- multi-process chaos plan validation for crash/restart and partition scenarios
- in-process data-node Raft model used by the runtime FSM/tests
- in-process metaserver Raft model behind `ProductionMetaRaftRuntime`
- majority commit behavior
- primary/leader promotion
- follower catch-up
- scale up/down model
- explicit replication health reports with per-voter lag
- explicit apply health reports with per-node commit-to-applied lag, slow-applier lists, and
  Prometheus `temporalstore_raft_node_apply_lag` gauges for data-node and metaserver Raft
- distributed `POST /raft/apply_health` on standalone `raft_node`, raft-enabled `server`, and
  local harness nodes so operators and tests can verify commit-to-apply convergence over HTTP
- networked `POST /raft/membership/apply` on standalone `raft_node` and raft-enabled `server` for
  safe joint-consensus voter changes
- metaserver-owned data-Raft membership workflow report for learner add, catch-up verification,
  promotion, leader transfer, and voter removal
- catch-up heartbeat helper for recovered live followers
- safe data-node scale-up that returns only after the new replica is caught up
- safe data-node scale-down that rejects quorum loss and promotes only a caught-up successor
- primary crash failover report for deterministic secondary promotion
- equivalent metaserver health, catch-up, safe scale, and failover APIs
- equivalent metaserver membership plan/apply reports for add/remove/replace voters
- read-index and leader-transfer guards
- local snapshot install
- chunked snapshot install that assembles multiple `InstallSnapshotChunkRequest` messages before
  replacing follower state
- data-node snapshot lifecycle reports for freeze, flush, manifest verification, checksum
  verification, install, tail replay, and rollback decisions
- joint-consensus membership safety model requiring old and new voter majorities
- `RaftRpcRuntime` wrapper with max-inflight backpressure, retries, retry backoff, and
  runtime counters for attempts, successes, failures, retries, inflight requests, and
  backpressure rejections
- Raft RPC metadata with request id, deadline, and auth token, plus an authenticated transport wrapper
- receive-side peer RPC auth enforcement on `raft_node`, raft-enabled `server`, and the local
  distributed harness for AppendEntries, RequestVote, InstallSnapshot, and snapshot chunks
- randomized heartbeat/election scheduler model
- external snapshot references on Raft install-snapshot requests, so a leader can attach the
  immutable S3/MinIO snapshot manifest URI, checksum, and byte size while keeping Raft log catch-up
  as the source for recent writes
- external snapshot bootstrap rejects stale snapshots before download/verify work when the target
  replica already has a higher local commit index
- operator-triggered external snapshot bootstrap on standalone `raft_node` and raft-enabled
  `server` through gated `POST /raft/admin/bootstrap_external_snapshot`: the node downloads a
  S3/MinIO-compatible snapshot ref through the snapshot-store abstraction, verifies manifest,
  checksum, byte size, shard id, and log index, installs it into the target replica engine state,
  records the external snapshot ref, and catches up from the leader log
- durable local metaserver mutation log and replay for HTTP/admin mutations when
  `TS_META_MUTATION_LOG` is set
- Raft transport message contracts:
  - `AppendEntriesRequest`
  - `VoteRequest`
  - `InstallSnapshotRequest`
  - `InstallSnapshotChunkRequest`
- local receive-path validation for AppendEntries and RequestVote
- HTTP transport smoke coverage for AppendEntries, RequestVote, and InstallSnapshot
- local timeout tick election and pre-vote coverage
- append-only local Raft WAL records with checksum validation, fsync on append, latest-valid
  recovery, and corrupt-tail truncation
- in-progress joint-consensus membership persisted in local Raft WAL and restored after restart
- WAL-backed Raft clusters that automatically persist committed writes, leadership changes,
  membership changes, catch-up, RPC receive state, and snapshot installs without requiring callers
  to remember a manual `persist_wal()` call
- bounded local WAL retention for WAL-backed clusters, using `RaftConfig.max_disk_replicate_log_num`
  to compact old durable records while preserving latest recovery state
- installed Raft snapshot payload and snapshot-index floor persisted into WAL-backed data-node records,
  so restart can recover state even after pre-snapshot log entries are trimmed
- durable apply/snapshot fence persisted in every new WAL-backed node record, validating restored
  commit index, applied index, installed snapshot floor, and first retained log index before replay
- storage-aware apply fence persisted in every new WAL-backed node record, validating shard id,
  Raft term, committed/applied index, snapshot id, storage epoch, and checksum before replay
- snapshot-floor log matching for post-compaction AppendEntries continuity, so leaders continue log
  indexes after an installed snapshot and followers can match `prev_log_index`/term against either
  retained log entries or the installed snapshot boundary
- snapshot-tail catch-up for new and lagging data-node replicas after leader compaction; new and
  lagging replicas receive the leader snapshot floor before replaying retained post-snapshot log
  entries
- compacted-entry rejection on data-node AppendEntries apply, so entries at or below an installed
  snapshot floor are ignored instead of replayed over snapshotted state
- metaserver snapshot-floor election and catch-up, so compacted metaserver voters remain electable
  and new voters inherit the installed snapshot boundary before replaying retained meta-log entries
- leader election rejects stale candidates unless their log is up-to-date with a voting majority
- deterministic ByteRaft-style snapshot trigger reports for data-node and metaserver Raft when
  applied log bytes since the latest snapshot floor exceed `max_applied_log_bytes`
- RequestVote receive path updates higher terms and clears prior votes before grant/reject decisions
- ByteRaft-derived readiness guard covering config/election guards, durable WAL hard state, joint
  membership, linearizable and bounded reads, learner promotion, leader transfer, snapshot
  bootstrap, replication lag/catch-up, failover, operator status/local-status/metrics, and
  RPC retry/backpressure/auth/deadline behavior, bounded WAL retention, applied-log-byte snapshot
  triggers, durable apply/snapshot fencing, snapshot-floor log matching, snapshot-tail catch-up, and
  compacted-entry rejection, metaserver snapshot-floor election, and operator control routes
- ByteRaft-style process-path admin evidence through `ByteRaftRuntimeAdminReport`: per-peer
  match/next index, append request/accept/reject counters, inflight bytes/entries,
  append queue depth/max depth, active apply backpressure, memory replicate-byte backpressure,
  oversized-log rejection, out-of-order append handling, append/reorder queue depth, reorder
  accept/release/reject counters, snapshot send/install state, snapshot send
  attempt/complete/failure counters, snapshot send elapsed/timeout counters, snapshot install
  start/complete/reject/rollback counters, install received/total chunks, install progress,
  chunk retry, rate-limit, membership-change snapshot evidence, compacted-log rejoin evidence,
  retry/backpressure counters,
  leader-transfer target state, request/accept/reject/complete counters, and
  elapsed/timeout counters,
  offline elapsed time, offline-timeout reached state, offline-timeout rejection counters,
  pre-vote/election rejection counters, read-index plus lease-read
  request/accept/reject counters, stale follower read/write rejection, WAL segment retention,
  retained WAL bytes, active segment bytes, retained record count, WAL first/last sequence, and process-path
  admin-status completeness. The report now also emits a `capability_matrix` with explicit
  rows for `per_peer_replication_pipeline_state`, `reorder_queue_runtime`,
  `snapshot_sender_downloader_lifecycle`, `lease_read_index_pre_vote_semantics`,
  `wal_segment_lifecycle`, and `admin_status_surface`. The same fields are now exposed through the standalone Raft node and
  raft-enabled data-node Prometheus surfaces so operator status/metrics evidence is tied to the
  process path. Append request construction now enforces configured in-flight entry/byte limits and
  rejects saturated peer pipelines with explicit backpressure. Top-level proposal paths record
  oversized-log and memory-byte rejections even when the command is rejected before append fanout.
  Append receive now rejects batches that exceed configured apply-batch bytes or reorder windows,
  records out-of-order append rejection, and records accepted/released/rejected entries.
  Snapshot sender construction now
  rejects concurrent single-shot or chunked transfers for the same peer, records chunk rate-limit
  pressure, records install progress and duplicate chunk retries, and keeps rollback diagnostics
  for membership-change snapshots and follower rejoin after compacted logs. Per-peer pipeline,
  snapshot lifecycle, and read-safety state is
  maintained as runtime state and persisted through the local WAL restore path, instead of being only
  reconstructed at report time. This is Rust-native OpenRaft/raft-rs readiness evidence, not direct
  C++ ByteRaft FFI.
- ByteRaft-style membership-role evidence is explicit: witnesses participate in quorum but cannot
  serve data or become leader, learners can serve caught-up reads but do not count for quorum, and
  `add_learner_with_auto_promote` catches up and promotes a learner to voter while preserving
  `auto_promoted_from_learner` evidence in admin/local-status reports. The
  `/raft/control/byteraft_local_status` route exposes per-node local status joined with peer
  pipeline state, including pending joint-consensus state that survives WAL-backed rolling restart
  before safe commit.
- strict shared-store WAL gap rejection
- partition/heal chaos coverage in the local model
- tests for the above behavior

The readiness API is:

```rust
temporalstore_rust::distributed_raft_readiness()
```

With the `openraft-engine` feature enabled, it returns `complete = true` and
`production_ready = true` for the Raft replication slice once the local real-process harnesses have
emitted durable data-node and metaserver rollout evidence. That evidence now covers data-node
applied Raft index atomicity with storage mutations and snapshot install, metaserver-owned learner
add/catch-up/promotion/leader-movement/removal for data-node Raft groups, production mTLS process
selection, and external packet-loss/disk-pressure/process-chaos validation.

Production callers should use the hard guard:

```rust
temporalstore_rust::require_production_raft_ready()
```

or validate an explicit deployment mode:

```rust
temporalstore_rust::validate_raft_deployment_mode(
    temporalstore_rust::RaftDeploymentMode::ProductionDistributed,
)
```

Production distributed Raft mode is mandatory. `distributed_raft_readiness()` reports
`RaftDeploymentMode::ProductionDistributed`, and `validate_raft_deployment_mode(LocalModel)` fails
closed because the local model is test-only. Local Raft fixtures remain available for unit tests,
compatibility tests, and harness validation, but they are not an accepted runtime or deployment
mode.

The Rust production target is Rust-native behavior parity: keep OpenRaft/raft-rs as the production
path and borrow ByteRaft semantics, safety contracts, metrics, admin surfaces, and tests. The
ByteRaft-derived evidence is now paired with the Rust OpenRaft process rollout evidence. Direct C++
ByteRaft FFI is not part of the readiness target.

The local WAL now has the applied-index/storage/snapshot atomicity contract represented as durable
apply/snapshot and storage-aware apply fences. The `raft_secondary_replication_harness` validates
those fences through real `raft_node` OS-process restart, external snapshot bootstrap, membership
changes, and failover.

## Production Runtime Surface

The production runtime surface is:

- `ProductionRaftRuntimeOptions`
- `ProductionRaftRuntime::start`
- `ProductionRaftRuntime::transport`
- `ProductionRaftRuntime::propose`
- `ProductionRaftRuntime::start_timer_loop`
- `ProductionMetaRaftRuntime::start`
- `ProductionMetaRaftRuntime::start_timer_loop`
- `ProductionMetaRaftRuntime::validate_ready`
- `ProductionRaftSecurity::mtls`
- `ProductionRaftChaosPlan`
- `raft_node` binary with `/raft/propose`, `/raft/read`, `/raft/status`,
  `/raft/membership/apply`, gated local-chaos `/raft/apply_health`, `/raft/admin/*` endpoints,
  local peer-block chaos controls, external snapshot bootstrap, and peer `/raft/*` transport
  endpoints
- raft-enabled `server` binary exposes the same `/raft/status`, `/raft/apply_health`,
  `/raft/membership/apply`, `/raft/admin/*`, external snapshot bootstrap, and peer `/raft/*`
  control/transport surfaces while serving TemporalStore requests
- `raft_secondary_replication_harness` binary that starts real `raft_node` OS processes, kills and
  restarts a secondary, injects a network partition with stale-read rejection, heals and verifies
  follower catch-up, forces a lagging follower while majority writes continue, heals and verifies
  catch-up reads, applies safe membership scale-down and scale-up through `/raft/membership/apply`,
  sends real TCP `/raft/request_vote` requests for unauthorized-peer rejection, stale-candidate
  rejection, and caught-up-candidate grant, rolling-restarts every voter from its WAL, verifies
  post-restart replication, kills the original leader, triggers surviving-node failover, and
  verifies post-failover reads
- `run_raft_shared_cases.py` validates every shared C++/Rust Raft corpus case has Rust process or
  harness evidence and C++ required paths. Its Rust combined mode runs the data-node plus metaserver
  parity gate once instead of treating individual corpus rows as production proof by name alone.
- The combined Raft parity summary now promotes metaserver scheduler execution coverage,
  OpenRaft metaserver process rollout, and metaserver-owned data-Raft membership into first-class
  evidence fields. Validation requires learner add, catch-up verification, promotion, leader
  transfer, voter removal, follower-lag/failover/scale-up/scale-down/secondary-replication flags,
  stale scheduler token rejection, and persisted metaserver Raft replay evidence.

Production deployments should use `ProductionRaftSecurity::mtls`. Local chaos tests can use
`ProductionRaftSecurity::plaintext_for_local_chaos` only when
`allow_plaintext_for_local_chaos = true`.

## Remaining Outside This Raft Slice

- AWS or other external multi-node SLO runs with real metaserver, proxy, client, and data-node
  deployments remain part of the broader `scale_testing` gate.
- legacy C++ wire compatibility remains out of scope for the Rust-native Raft process path.
- Non-Raft service API TLS/auth, dashboards, autoscale, and deployment automation remain tracked by
  the broader deployment-ops gate.

## C++ Raft Test Case Cross-Reference

The current C++ unified corpus includes these Raft/replication cases:

| C++ corpus case | C++ runner | Rust validation path |
| --- | --- | --- |
| `storage_data_raft_replication_gtest` | `cmake --build build-ubuntu22/release --target data_raft_replication_test -j2` | `cargo run -p temporalstore-rust --bin distributed_raft_harness` plus `tools/validate_aws_validation_log.py --job temporalstore-raft-validation` |
| `raft_data_node_scale_failover_snapshot` | `tools/run_data_raft_2node_scale_ubuntu22.sh`, `tools/run_data_raft_failover_ubuntu22.sh`, `tools/run_data_raft_snapshot_restore_ubuntu22.sh` | `distributed_raft_harness`, `raft_secondary_replication_harness`, and `external_chaos_gate --profile quick` cover scale down/up, leader transfer, snapshot bootstrap, secondary restart catch-up, and leader-crash failover |
| `raft_data_node_mixed_rw_and_membership` | `tools/run_data_raft_mixed_rw_ubuntu22.sh`, `tools/run_data_raft_scale_up_down_ubuntu22.sh` | `distributed_raft_harness` validates post-transfer writes, scale-down writes/reads, scale-up writes/reads, and replica reads; `raft_secondary_replication_harness` validates partition/heal and lagging-follower catch-up |
| `raft_data_node_leader_election_failover` | `tools/run_data_raft_failover_ubuntu22.sh` | `raft_secondary_replication_harness` names leader election and failover as a separate shared scenario and keeps the evidence on the process harness path |
| `raft_data_node_snapshot_restart_follower_lag` | `tools/run_data_raft_snapshot_restore_ubuntu22.sh`, `tools/build_secondary_visibility_lag_benchmark.sh` | `distributed_raft_harness` plus `raft_secondary_replication_harness` cover snapshot install, restart recovery, lagging follower observation, and catch-up |
| `raft_data_node_membership_secondary_reads` | `tools/run_data_raft_scale_up_down_ubuntu22.sh`, `tools/run_data_raft_mixed_rw_ubuntu22.sh` | `raft_secondary_replication_harness` covers membership add/promote/remove and secondary-read visibility as an explicit shared case |
| `raft_metaserver_membership_failover_snapshot` | `tools/run_metaserver_raft_membership_ubuntu22.sh`, `tools/run_metaserver_raft_failover_ubuntu22.sh`, `tools/run_metaserver_raft_snapshot_restore_ubuntu22.sh` | `metaserver_raft_harness` plus `production_meta_raft_runtime_matches_cpp_multinode_control_and_fault_contract` cover membership list/add/remove, read-index wait, snapshot trigger/restore, lagging voter tail catch-up after stale snapshot install, leader transfer, failover, unsupported-role rejection, and no-majority rejection; strict production readiness still blocks on networked metaserver scheduler orchestration across real data-node Raft groups |
| `raft_metaserver_leader_snapshot_restart` | `tools/run_metaserver_raft_failover_ubuntu22.sh`, `tools/run_metaserver_raft_snapshot_restore_ubuntu22.sh` | `metaserver_raft_harness` names leader/failover, snapshot install, and restart recovery as a separate shared scenario |
| `raft_metaserver_membership_add_promote_remove` | `tools/run_metaserver_raft_membership_ubuntu22.sh` | `metaserver_raft_harness` names learner add, catch-up, promote, leader transfer, and voter remove as a separate shared scenario |
| `raft_openraft_process_rollout_evidence` | `tools/run_raft_production_gate_ubuntu22.sh`, `tools/run_raft_stress_suite_ubuntu22.sh` | Rust unit/readiness evidence verifies local mode is rejected for deployment and production readiness depends on harness-derived OpenRaft data-node and metaserver process reports: `real_process_path_evidence_validated=true`, spawned process counts, independent WAL/snapshot dirs, observed requests, read-index responses, restart recovery for every process, per-node log-store inspection for every process, WAL first/last sequence status, WAL segment release-rule evidence, WAL fsync/backpressure evidence, restart log-store comparison evidence, FSM apply atomicity, apply-fence recovery, snapshot-install apply-fence recovery, deterministic storage/WAL/snapshot crash recovery, bounded-stale reads under partition, follower lease expiration, and ByteRaft-derived process-semantics evidence for per-peer pipeline state, snapshot lifecycle, WAL lifecycle, failover, membership, and secondary lag where applicable |
| `raft_production_gate` | `tools/run_raft_production_gate_ubuntu22.sh` | `tools/run_storage_raft_production_readiness.sh` is the Rust storage/Raft local gate, and `tools/run_raft_distributed_parity.sh` is the Rust Raft-only parity gate for data-node plus metaserver multi-node behavior; strict production mode still fails until networked OpenRaft rollout and real multi-process log-store validation are complete |

Focused C++ Raft-case-driven Rust validation:

```bash
python3 tools/run_cpp_raft_cases_on_rust.py \
  --cpp-repo /path/to/cpp/TemporalStore \
  --artifact-dir /tmp/temporalstore-cpp-raft-cases-on-rust
```

This command uses the unified C++ Raft case names above to verify C++ required paths, write a
case-to-Rust-runner mapping report, and execute the Rust data-node plus metaserver Raft parity gate.

## June 17, 2026 Local Multi-Node Validation

The Rust multi-node Raft checks were rerun against the C++ coverage above:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
cargo run -p temporalstore-rust --bin distributed_raft_harness -- \
  --root /tmp/temporalstore-raft-validation-now/distributed \
  > /tmp/temporalstore-raft-validation-now/distributed.json
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-validation \
  --log /tmp/temporalstore-raft-validation-now/distributed.json

CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
cargo run -p temporalstore-rust --bin metaserver_raft_harness -- \
  --root /tmp/temporalstore-metaserver-raft-now \
  > /tmp/temporalstore-metaserver-raft-now/metaserver.json
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-metaserver-raft-validation \
  --log /tmp/temporalstore-metaserver-raft-now/metaserver.json

CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness -- \
  --root /tmp/temporalstore-raft-secondary-now \
  --heartbeat-ms 25 \
  > /tmp/temporalstore-raft-secondary-now/secondary.json
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-secondary-validation \
  --log /tmp/temporalstore-raft-secondary-now/secondary.json

CARGO_TARGET_DIR=/tmp/temporalstore-local-validation-target \
cargo run -p temporalstore-rust --bin external_chaos_gate -- \
  --root /tmp/temporalstore-external-chaos-raft-now \
  --profile quick \
  > /tmp/temporalstore-external-chaos-raft-now.json

TS_RAFT_PARITY_ARTIFACT_DIR=/tmp/temporalstore-raft-distributed-parity-now \
TS_RAFT_PARITY_TIMEOUT=240s \
tools/run_raft_distributed_parity.sh
```

Results:

- `distributed_raft_harness`: JSON validation passed.
- `metaserver_raft_harness`: JSON validation passed, including stale-snapshot lagging-voter tail
  catch-up for shard `56`.
- `raft_secondary_replication_harness`: JSON validation passed.
- `tools/run_raft_distributed_parity.sh`: combined data-node plus metaserver JSON validation
  passed.
- `external_chaos_gate --profile quick`: `production_ready_slice=true`, `scenario_count=6`,
  `passed_count=6`.

Evidence from the validated outputs:

- Multi-node proposal, post-transfer write, post-scale-down write, and post-scale-up write all
  returned `ok`.
- Post-snapshot data-node re-scale-down and re-scale-up writes returned `ok`, with reads converged
  to `after-rescale-down` on voters 1/2/3 and `after-rescale-up` on voters 1/2/3/4.
- Replica reads returned `replicated-value` on all checked replicas.
- Secondary restart catch-up returned `v1`, `v3`, and `v4` on nodes 1, 2, and 3.
- Isolated partition reads were rejected with `leader is not available`; after heal, the follower
  read returned `v-partition`.
- Lagging follower observation saw lag `3`; all catch-up reads returned `v-lag-0`, `v-lag-1`, and
  `v-lag-2`.
- Leader-crash failover returned `ok`; surviving nodes read `v5`.
- Metaserver post-failover replacement changed voters to `[10, 12, 13]` and served
  `meta-after-replace`; the follow-up scale-down changed voters to `[10, 13]` and served
  `meta-after-second-scale-down`.

## Repeated Data-Node Raft Check

The repeated audit still finds that data-node distributed Raft is not production complete.

Covered today:

- separate `raft_node` process wrapper
- HTTP Raft transport endpoints for propose/read/status and peer messages
- networked `/raft/membership/apply` endpoint for safe add/remove/replace voter changes on
  standalone raft nodes and raft-enabled data servers, validated by the real OS-process harness for
  scale down and scale up
- networked `/raft/admin/bootstrap_external_snapshot` endpoint for operator-driven replica
  bootstrap from a S3/MinIO-compatible snapshot ref on both standalone raft nodes and raft-enabled
  data servers
- distributed harness coverage for process-boundary snapshot publish plus bootstrap: a follower is
  held behind, the leader publishes a S3/MinIO-compatible snapshot over HTTP, the follower restores
  through `/raft/admin/bootstrap_external_snapshot`, and the harness verifies the restored value
  through `/raft/read`
- local OS-process harness coverage for network partition stale-read rejection, heal, and catch-up
- local OS-process harness coverage for lagging-follower observation, majority-side writes, heal,
  and catch-up reads
- local OS-process harness coverage for rolling restart of every voter with WAL recovery and
  post-restart replication
- local majority commit, catch-up, safe reads, promotion, safe scale up, and safe scale down
- commit-to-apply lag observability for data-node and metaserver Raft groups, including HTTP
  apply-health checks in the process/runtime harnesses
- WAL-backed local model recovery for commits, leadership, and membership
- local snapshot and chunked snapshot message behavior

Still missing:

- production OpenRaft durable log-store rollout across real processes beyond the feature-gated
  local adapter tests
- validation that the metaserver-owned membership scheduler drives real data-node Raft groups
  through follower lag, failover, scale up/down, and secondary replication after applying
  `/raft/membership/apply` tasks
- production engine snapshot install with freeze/flush/download/verify/install lifecycle; the local
  external snapshot path now has process-level admin routes, stale-local-state preflight before
  download, manifest/checksum/size verification, and target replica engine-state install
- external chaos tests that inject packet loss and disk pressure
- AWS multi-node validation with real metaserver, proxy, client, and data-node processes

The executable readiness gate for this repeated check is:

```rust
temporalstore_rust::production_readiness_report()
```

The executable local external-chaos gate is:

```bash
cargo build -p temporalstore-rust --bins
cargo run -p temporalstore-rust --bin external_chaos_gate -- --profile quick
```

The executable Raft-only C++ parity gate for both data-node and metaserver multi-node behavior is:

```bash
tools/run_raft_distributed_parity.sh
```

It runs `distributed_raft_harness`, `raft_secondary_replication_harness`, and
`metaserver_raft_harness`, then uses `build_raft_distributed_parity_summary.py` to validate a
combined `raft-distributed-parity.json` report with data-node replica reads, follower-write
rejection, membership scale down/up, external snapshot restore, secondary restart/partition/lag/
failover, post-snapshot re-scale down/up, and metaserver
membership/read-index/snapshot/lagging-voter catch-up/failover/replacement/scale-down/no-majority
checks. The full
`run_storage_raft_production_readiness.sh` gate also builds and validates the same combined summary
from its already-produced harness artifacts.

The quick profile composes the OS-process Raft secondary harness, the networked
distributed Raft harness, the storage modes harness, the storage production harness, and the
storage dump/load fault matrix harness. It proves process
kill/restart, partition-style stale-read rejection, lag/heal catch-up, rolling
restart, membership/snapshot transfer, sync and async shared-store replay, and
local Raft WAL restore through executable scenarios. The full profile also runs
a small local scale pass with failover and shared-store comparison. This closes
the local external-chaos gate, but it does not replace future host-level packet
loss and disk-full validation.

It now has separate areas for:

- `data_node_distributed_raft`
- `fault_tolerance`
- `scale_testing`

## Separate Node Run

Start three local Raft nodes as separate OS processes:

```bash
TS_RAFT_NODE_ID=1 TS_RAFT_BIND_ADDR=127.0.0.1:19001 TS_RAFT_WAL_DIR=target/raft-node-1 \
TS_RAFT_NODES='1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003' \
cargo run -p temporalstore-rust --bin raft_node

TS_RAFT_NODE_ID=2 TS_RAFT_BIND_ADDR=127.0.0.1:19002 TS_RAFT_WAL_DIR=target/raft-node-2 \
TS_RAFT_NODES='1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003' \
cargo run -p temporalstore-rust --bin raft_node

TS_RAFT_NODE_ID=3 TS_RAFT_BIND_ADDR=127.0.0.1:19003 TS_RAFT_WAL_DIR=target/raft-node-3 \
TS_RAFT_NODES='1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003' \
cargo run -p temporalstore-rust --bin raft_node
```

Then send writes to the leader node:

```bash
curl -s http://127.0.0.1:19001/raft/propose \
  -H 'content-type: application/json' \
  -d '{"command":{"kind":"string_set","key":"k","value":[118]}}'
```

## Local Data-Node Raft Harnesses

For a one-command local proof that the data-node Raft HTTP transport replicates between four
separate node endpoints and writes local WAL segment files, run:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin distributed_raft_harness
```

The harness starts four data-node Raft runtimes on different loopback ports, proposes a write
through node 1, waits until replica reads return the replicated value, rejects a direct follower
write, transfers leadership, scales voters down and back up, waits for every node's
`/raft/apply_health` report to reach zero apply lag, and prints a JSON report with each node
address, Raft status, apply health, replica read result, and WAL files.

For a stronger process-level secondary replication check, build all binaries and run:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo build -p temporalstore-rust --bins

CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness
```

This harness starts three real `raft_node` OS processes with `TS_RAFT_ENABLE_LOCAL_ADMIN=true`,
writes through the leader, kills secondary node 3, writes while that secondary is down, restarts
node 3 with the same local WAL directory, waits until all nodes can read the pre-stop, while-down,
and after-restart keys, rolling-restarts voters 3, 2, and 1 from their existing WAL directories,
verifies a fresh replicated write after each restart, injects partition and lagging-follower phases,
sends real `/raft/request_vote` messages that reject a wrong-token peer request, reject a stale
candidate, and grant a caught-up candidate, then kills leader node 1, marks it dead in the
surviving local-chaos runtimes, triggers failover on node 2, writes through the new leader, and
verifies the surviving replicas can read the post-failover key.

## Current Test Coverage

The local model is covered by unit and integration tests:

```text
raft_replicates_committed_write_to_majority_and_followers
raft_rejects_write_without_majority
raft_follower_catches_up_after_outage
raft_rejects_electing_stale_replica_until_it_catches_up
raft_transport_append_entries_catches_up_lagging_replica
raft_transport_rejects_stale_append_entries_and_behind_vote
raft_leader_lease_expiry_blocks_linearizable_reads_and_writes_until_heartbeat
request_vote_higher_term_resets_prior_vote_before_decision
request_vote_higher_term_updates_term_even_when_candidate_log_is_behind
raft_hard_state_membership_and_snapshot_transport_are_exposed
raft_install_snapshot_request_carries_external_snapshot_reference
streaming_snapshot_chunks_install_only_after_all_chunks_arrive
joint_consensus_requires_old_and_new_majorities_before_commit_or_write
joint_consensus_state_survives_wal_restore_and_still_requires_both_majorities
raft_rpc_runtime_retries_transport_errors_and_releases_inflight
raft_rpc_runtime_attaches_auth_and_deadline_metadata
raft_scheduler_randomizes_election_timeout_and_emits_heartbeats
partition_chaos_majority_side_continues_and_healed_replica_catches_up
replication_health_reports_lag_and_heartbeat_catches_up_secondary
safe_scale_up_adds_replica_only_after_catchup
safe_scale_down_rejects_quorum_loss_and_promotes_caught_up_leader_successor
primary_crash_promotes_caught_up_secondary_and_old_primary_recovers
local_raft_wal_persists_hard_state_membership_and_entries
local_raft_wal_recovers_latest_valid_record_and_truncates_corrupt_tail
raft_cluster_recovers_committed_state_from_local_wal
wal_backed_raft_cluster_auto_persists_commits_leadership_and_membership
wal_backed_raft_cluster_compacts_wal_tail_but_recovers_latest_state
data_raft_snapshot_trigger_compacts_applied_log_bytes
distributed_raft_readiness_reports_remaining_production_blockers
production_raft_mode_is_blocked_until_real_engine_and_chaos_exist
production_raft_runtime_validates_security_timer_and_chaos_contracts
production_raft_runtime_replicates_over_separate_http_nodes
raft_secondary_replication_harness
metaserver_raft_harness
metaserver_raft_health_catchup_safe_scale_and_failover_work
metaserver_raft_snapshot_trigger_compacts_applied_log_bytes
metaserver_raft_promotes_follower_after_leader_failure_and_keeps_metadata_available
metaserver_raft_rejects_reads_and_writes_without_majority
metaserver_mutation_log_recovers_routes_tables_and_state_changes
```

These tests cover the production wrapper, runtime validation, timers, RPC/auth construction, local
chaos contracts, local model behavior, and transport contracts.
