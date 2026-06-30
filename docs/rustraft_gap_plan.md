# RustRaft Gap Plan

RustRaft is the TemporalStore-owned Rust Raft compatibility and readiness layer.
The goal is to make TemporalStore independent of legacy upstream Raft naming while
keeping the operational semantics that production storage needs: leader-only
writes, bounded reads, durable hard state, safe membership, snapshots, failover,
and observable apply lag.

## What Is Implemented Now

- `RustRaft` now lives as the workspace library crate `crates/rustraft`.
- `crates/temporalstore-rust` consumes `rustraft` through a path dependency, so
  local TemporalStore builds and tests no longer depend on a pinned external git
  revision for the RustRaft contract.
- The library owns:
  - `RustRaftSemanticRequirement`
  - `RustRaftParityContract`
  - `RustRaftParityReport`
  - `RustRaftReadinessEvidence`
  - `RustRaftReadinessSnapshot`
  - `RustRaftStorage`
  - `RustRaftTransport`
  - RustRaft RPC request/response structs for append, vote, install snapshot,
    and read-index
  - RustRaft status, metric-name, process evidence, and rollout report structs
  - RustRaft safety helpers for read safety, learner promotion, and append
    compacted-entry rejection
  - `rustraft_parity_contract`
  - `rustraft_parity_report`
  - `rustraft_production_readiness_report`, which fails closed unless semantic
    readiness, peer pipeline, snapshot lifecycle, WAL lifecycle, data-node
    rollout, and metaserver rollout evidence are all present
- `temporalstore-rust` converts `RaftDistributedReadiness` into a
  `RustRaftReadinessSnapshot`, then asks the `rustraft` library to build the
  report.
- `temporalstore-rust` re-exports the RustRaft production readiness report/input
  so deployment gates can call the shared library contract through the normal
  TemporalStore Rust API.
- `temporalstore-rust` keeps app-specific data-node/metaserver state-machine,
  HTTP, topology, and durable storage glue, but aliases reusable RustRaft process
  evidence and rollout report types from the library.
- Shared corpus and Rust tests use `raft_rustraft_*` case names.
- OpenRaft is not part of the RustRaft contract.

## Remaining Gaps

| Gap | Why It Matters | Target Implementation | Shared Gate |
|---|---|---|---|
| Native log runtime | Core contract and RPC/status types are separate, but production log application still lives inside `temporalstore-rust`. | Move reusable membership planner/state transitions and snapshot-floor state machine into `crates/rustraft`; keep storage engine calls in TemporalStore adapters. | RustRaft unit tests plus TemporalStore integration tests. |
| Transport abstraction | The trait and message contract now live in `crates/rustraft`; production HTTP wiring is still TemporalStore-specific. | Make data-node/metaserver transport clients implement `RustRaftTransport` directly. | Shared Raft transport contract cases. |
| Snapshot lifecycle | Snapshot floor, chunk retry, stale chunk rejection, and tail catch-up are still tested mostly through TemporalStore. | Add library-level snapshot state machine and fault tests. | `raft_rustraft_snapshot_lifecycle_depth`. |
| Membership workflow | Learner catch-up, promote, remove, transfer leader, and joint membership need a reusable library state model. | Add membership planner/state transitions to the `RustRaft` repo; TemporalStore metaserver consumes it. | `raft_rustraft_leader_transfer_high_write_fault_harness` and membership cases. |
| Metrics model | RustRaft metric names/status snapshots live in the library; TemporalStore still emits many app-specific metrics. | Route Raft dashboard panels through RustRaft metric-name constants where possible. | Grafana/Prometheus parity checks. |
| Fault harness API | Fault cases are currently driven by TemporalStore harnesses. | Add a library-level deterministic harness for partitions, packet loss, slow WAL, restart, compaction, and snapshot install. | `raft_rustraft_*_fault_harness` cases. |
| Storage adapter boundary | Durable storage remains TemporalStore-specific. | Define RustRaft storage traits for log append/read, hard state, snapshots, and tombstoned compacted entries. | Storage recovery and compaction gates. |
| Production evidence wiring | The library gate exists, but live deployment scripts still need to feed all runtime evidence into it automatically. | Wire data-node/metaserver harness outputs into `rustraft_production_readiness_report` and fail CI/deployment if the report is blocked. | RustRaft production readiness artifact validation. |

## Implementation Order

1. Keep `crates/rustraft` as the stable public RustRaft contract crate.
2. Move pure contract/state types first; keep TemporalStore process and storage code
   where it is until the library boundary is stable.
3. Keep RustRaft transport and storage traits independent of TemporalStore
   process code.
4. Add library-level deterministic state-machine tests for read-index, stale leader,
   learner promotion, snapshot floor, and compacted-entry rejection.
5. Make TemporalStore data-node and metaserver code consume the shared RustRaft
   traits and reports.
6. Run shared corpus gates for data-node Raft, metaserver Raft, multi-node,
   failover, snapshot restore, membership, and read safety.

## Non-Goals For This Step

- This step does not replace the C++ dependency checkout or its upstream build
  flags. Those remain compatibility plumbing until C++ is independently moved to
  a RustRaft-named wrapper.
- This step does not claim production consensus replacement by itself. It creates
  the reusable Rust library boundary and keeps the existing TemporalStore tests as
  the proof path.
