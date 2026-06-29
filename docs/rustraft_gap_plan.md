# RustRaft Gap Plan

RustRaft is the TemporalStore-owned Rust Raft compatibility and readiness layer.
The goal is to make TemporalStore independent of legacy upstream Raft naming while
keeping the operational semantics that production storage needs: leader-only
writes, bounded reads, durable hard state, safe membership, snapshots, failover,
and observable apply lag.

## What Is Implemented Now

- `crates/rustraft` is a standalone Rust library consumed by
  `crates/temporalstore-rust`.
- The library owns:
  - `RustRaftSemanticRequirement`
  - `RustRaftParityContract`
  - `RustRaftParityReport`
  - `RustRaftReadinessEvidence`
  - `RustRaftReadinessSnapshot`
  - `rustraft_parity_contract`
  - `rustraft_parity_report`
- `temporalstore-rust` converts `RaftDistributedReadiness` into a
  `RustRaftReadinessSnapshot`, then asks the `rustraft` library to build the
  report.
- Shared corpus and Rust tests use `raft_rustraft_*` case names.
- OpenRaft is not part of the RustRaft contract.

## Remaining Gaps

| Gap | Why It Matters | Target Implementation | Shared Gate |
|---|---|---|---|
| Native log runtime | The contract is now separate, but runtime code still lives inside `temporalstore-rust`. | Move reusable log entry, hard-state, membership, snapshot-floor, and read-index primitives into `crates/rustraft`. | RustRaft unit tests plus TemporalStore integration tests. |
| Transport abstraction | Production data-node and metaserver paths need a stable RPC contract independent of the app crate. | Add RustRaft transport traits for append, vote, install-snapshot, and read-index. | Shared Raft transport contract cases. |
| Snapshot lifecycle | Snapshot floor, chunk retry, stale chunk rejection, and tail catch-up are still tested mostly through TemporalStore. | Add library-level snapshot state machine and fault tests. | `raft_rustraft_snapshot_lifecycle_depth`. |
| Membership workflow | Learner catch-up, promote, remove, transfer leader, and joint membership need a reusable library state model. | Add membership planner/state transitions to `crates/rustraft`; TemporalStore metaserver consumes it. | `raft_rustraft_leader_transfer_high_write_fault_harness` and membership cases. |
| Metrics model | Runtime metrics are emitted in TemporalStore-specific structures. | Add RustRaft metric names and status snapshots so C++ and Rust deployments share dashboards. | Grafana/Prometheus parity checks. |
| Fault harness API | Fault cases are currently driven by TemporalStore harnesses. | Add a library-level deterministic harness for partitions, packet loss, slow WAL, restart, compaction, and snapshot install. | `raft_rustraft_*_fault_harness` cases. |
| Storage adapter boundary | Durable storage remains TemporalStore-specific. | Define RustRaft storage traits for log append/read, hard state, snapshots, and tombstoned compacted entries. | Storage recovery and compaction gates. |

## Implementation Order

1. Keep `crates/rustraft` as the stable public RustRaft contract crate.
2. Move pure contract/state types first; keep TemporalStore process and storage code
   where it is until the library boundary is stable.
3. Add RustRaft transport and storage traits without changing production behavior.
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

