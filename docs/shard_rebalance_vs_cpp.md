# Shard Rebalance Vs C++ TemporalStore

## C++ Reference Behavior

The C++ metaserver has a richer table/partition model than the Rust rewrite. The relevant pieces are:

- table owns partition sets
- partition set owns partition replicas for each partition unit
- each partition has expected placement, actual placement, state, role, and version
- membership has partition-set version, unit versions, primary id, active ids, frozen ids, and placements
- balance routine runs periodically, default interval is 5 minutes
- balance task computes partition count per node, safe line, and high-load nodes
- balance task freezes a bounded number of normal partitions per round
- freeze is skipped if the partition cannot be moved safely
- C++ avoids removing a primary in `PROMOTE_DERIVED` mode unless a derived/new primary can take over
- metaserver proposes freeze/load/membership changes through Raft
- data servers receive `Load`, `Unload`, and `UpdateMembership`
- secondaries use shared-store bootstrap plus primary-pull/shared-store streams to catch up

Important C++ files checked:

- `/home/vj/src/temporalstore/src/metaserver_v2/scheduler/balance_table_task.cc`
- `/home/vj/src/temporalstore/src/metaserver_v2/meta/table.cc`
- `/home/vj/src/temporalstore/src/metaserver_v2/meta/partition.cc`
- `/home/vj/src/temporalstore/src/protocol/metaserver.proto`
- `/home/vj/src/temporalstore/src/protocol/server.proto`

## Rust Implementation Added

Rust now has a first metaserver-side rebalance model in:

```text
crates/temporalstore-rust/src/rebalance.rs
```

It implements:

- `ShardReplica` with shard id, replica id, node id, role, state, and load version
- `RebalanceController` with membership version and replica registry
- node load calculation from normal replicas
- rebalance round planner with:
  - all known nodes
  - max moves per round
  - partition count safe gap
  - C++-style balance metrics report with `balance_partition_count`, `safe_line`,
    `total_normal`, `placement_fail_count`, and placement failure reasons
  - overloaded source detection
  - least-loaded target selection
  - target deduplication so one node does not host the same shard twice
  - primary safety check
- move lifecycle:
  - `begin_move` -> `LoadTarget`
  - `finish_target_load` -> `UpdateMembership` + `FreezeSource`
  - `finish_source_freeze` -> `UnloadSource`
  - `rollback_move` for failed target loads

This mirrors the C++ control-plane shape:

```text
high-load node -> choose normal replica -> load target -> update membership -> freeze source -> unload source
```

The Rust implementation expects the data movement itself to use the shared-store checkpoint/oplog path that already exists.

## Tests Added

The Rust tests cover:

- moving one shard replica from overloaded node to low-load node
- bounded moves per round
- membership update and freeze/unload steps
- lonely primary is not moved
- target node cannot already host the same shard
- balance metrics expose per-node partition counts and the computed safe line
- placement failure metrics count primary-safety skips when overloaded replicas cannot be moved

## Still Missing Vs C++

The Rust rebalance model is still smaller than C++. Missing production pieces:

- namespace/table/partition-set hierarchy
- partition unit relation and placement-set update logic
- expected placement vs actual placement reconciliation
- location hierarchy such as region/VDC/VAU/tag
- host dedup placement rule
- low-load candidate filter
- task scheduler and background balance interval
- Raft-backed metaserver mutation log for rebalance operations
- server heartbeat/load reporting to drive real placement decisions
- server state handling: normal, frozen, decommissioned, dropped
- finish-load and finish-membership RPC handlers
- retry/idempotency for load/freeze/unload
- move rollback after partial target load or failed source freeze
- election policy variants beyond a primary safety guard
- derived partition versioning
- partition id bit layout and version/index rollover
- production export of balance metrics to the metaserver metrics endpoint and dashboards

## Recommended Next Step

The next useful chunk is to connect `RebalanceController` to the existing `MetaRaftCluster` model so rebalance decisions are persisted through metaserver Raft commands, then drive `TemporalEngine::load_shard_with`, shared-store checkpoint restore, membership update, and unload as an end-to-end rebalance workflow.
