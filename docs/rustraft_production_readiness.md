# RustRaft Production Readiness

RustRaft is the standalone TemporalStore-owned Raft contract library in the
separate `RustRaft` repository. In this TemporalStore repo,
`crates/temporalstore-rust` consumes it through the pinned external Git
dependency in `crates/temporalstore-rust/Cargo.toml`; the duplicate local
workspace crate was removed so TemporalStore only carries its runtime adapter
and integration code. It does not replace the data-node or metaserver by
itself. It defines the production semantics that those runtimes must prove.

## Status Labels

`rustraft_parity_report` checks the semantic contract and returns:

- `blocked`: one or more required production semantics are missing.
- `production_ready`: all required semantics are present and OpenRaft is absent
  from the public RustRaft contract.

The report also returns semantic `production_blockers`, for example:

```json
["durability:storage_apply_fence"]
```

`rustraft_production_readiness_report` is the stricter production gate. It wraps
the parity report and also requires runtime evidence for peer pipeline behavior,
snapshot lifecycle, WAL lifecycle, data-node process rollout, and metaserver
process rollout. Missing evidence fails closed with precise blockers such as:

```json
[
  "pipeline:append_backpressure",
  "wal:compaction",
  "data_node:operational_semantics",
  "metaserver:read_index"
]
```

These blockers are meant for CI, readiness checks, and deployment gates.

## Required Evidence

RustRaft production readiness requires evidence in these categories:

| Category | Evidence |
|---|---|
| `safety` | leader write authority, snapshot floor/log matching, compacted entry rejection, metaserver snapshot-floor election safety |
| `durability` | storage apply fence tied to durable apply index state |
| `transport` | AppendEntries, Vote, InstallSnapshot, and ReadIndex contracts |
| `snapshot` | trigger policy, apply fence, snapshot plus retained-tail catch-up |
| `membership` | learner catch-up before promotion and metaserver-owned membership workflow |
| `observability` | operator status for leader, term, commit, apply, peer state, and lag |

## TemporalStore Gates

TemporalStore consumes the separate RustRaft library and validates:

- RustRaft is OpenRaft-free at the public contract boundary.
- Generic Raft contract primitives such as `RustRaftNodeId`,
  `RustRaftRole`, and `RustRaftReplicaRole` live in the RustRaft library; the
  TemporalStore runtime keeps compatibility aliases but no longer owns those
  definitions.
- Each requirement has a category and readiness field.
- Missing required evidence fails closed with category-qualified blockers.
- Runtime production readiness uses `rustraft_production_readiness_report`, not
  scattered ad-hoc checks.
- The production gate requires peer pipeline, snapshot lifecycle, WAL lifecycle,
  data-node rollout, and metaserver rollout evidence.
- Current data-node/metaserver readiness reports `production_ready`.
- C++ RustRaft/DataRaft-style semantics execute in Rust through the shared
  corpus case `raft_cpp_rustraft_data_raft_semantics_in_rust`.

The intended production rule is:

```text
if production_status != "production_ready":
  block production Raft claim
  print production_blockers
```

For production releases, use:

```text
rustraft_production_readiness_report({
  readiness,
  peer_pipeline,
  snapshot_lifecycle,
  wal_lifecycle,
  data_node_rollout,
  metaserver_rollout
})
```

If any field is missing, the report stays `blocked`; the library does not infer
production readiness from API presence alone.

## Metaserver Failover Proof

The focused metaserver failover gate is:

```bash
cargo run -p temporalstore-rust --bin metaserver_raft_harness -- \
  --root /tmp/temporalstore-rustraft-metaserver-failover/metaserver-raft \
  > /tmp/temporalstore-rustraft-metaserver-failover/metaserver-raft.json

python3 tools/validate_aws_validation_log.py \
  --job temporalstore-metaserver-raft-validation \
  --log /tmp/temporalstore-rustraft-metaserver-failover/metaserver-raft.json
```

The harness proves:

- initial metaserver voters are online and ready;
- namespace writes commit before and after leader movement;
- a transferred leader can be marked unavailable and a surviving majority elects
  a new leader;
- the post-failover namespace is visible through the metaserver state machine;
- snapshot restore plus tail catch-up works for a lagging voter;
- membership replacement and scale-down keep route reads visible;
- writes fail closed when the remaining live nodes do not form a majority;
- scheduler/membership/data-Raft ownership coverage reports `ready=true`.

Latest local proof on June 29, 2026:

| Field | Value |
|---|---|
| `initial_membership` | `[10, 11, 12]` |
| `membership_after_add` | `[10, 11, 12, 13]` |
| `membership_after_remove` | `[10, 11, 13]` |
| `leader_before_transfer` | `10` |
| `leader_after_transfer` | `11` |
| `leader_after_failover` | `10` |
| `namespace_after_failover_visible` | `true` |
| `unavailable_without_majority` | `true` |
| `post_replace_route_read` | `meta-after-replace` |
| `post_scale_down_route_read` | `meta-after-second-scale-down` |
| `scheduler_execution_coverage.ready` | `true` |
| `temporal_raft_process_rollout.ready` | `true` |

## Current Build Status

The focused RustRaft library tests and TemporalStore RustRaft consumer tests pass
against the separate `RustRaft` checkout. Broader distributed/data-node Raft
parity remains covered by `tools/run_raft_distributed_parity.sh`, which runs
data-node distributed Raft, secondary replication, metaserver Raft, and the
combined parity summary.
