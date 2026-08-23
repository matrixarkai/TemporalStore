# Metaserver Production Raft And Meta Management Contract

This page captures the production metaserver responsibilities that must be proven before we call the
TemporalStore metaserver production-ready. The shape follows the root/meta-server split from the
MatrixObject design: the metaserver owns cluster metadata, placement, topology readiness, and
membership orchestration; data nodes own serving data.

## Required Root/Meta Responsibilities

- Namespace/table lifecycle is Raft-committed and survives leader failover.
- Slot and shard placement are assigned through metaserver state, not local data-node guesses.
- Primary placement and replacement are explicit scheduler decisions.
- Topology readiness waits for slot/primary assignment, membership health, and applied-index catch-up.
- Server heartbeat/liveness feeds scheduler repair decisions.
- Missing-primary repair, under-replication repair, stale-dead-server repair, load/reload/unload, and safe-mode cooldown are covered.
- Metaserver membership add/remove is Raft-backed and rejects unsupported learner/witness roles until the production workflow supports them.
- Metaserver-owned data-Raft membership executes learner add, catch-up, promotion, leader transfer, and voter removal.
- Snapshot restore and lagging-voter catch-up preserve committed route/namespace state.
- No-majority writes fail closed.

## Canonical Report Shape

Production evidence should expose the following top-level metaserver section, either directly or through
the normalized storage/Raft proof:

```json
{
  "metaserver": {
    "raft_backend": "temporal_raft",
    "namespace_table_lifecycle": {},
    "slot_assignment": {},
    "primary_placement": {},
    "topology_readiness": {},
    "heartbeat_liveness": {},
    "scheduler_repair": {},
    "membership": {},
    "snapshot_restore": {},
    "failover": {},
    "meta_owned_data_raft_membership": {}
  }
}
```

The current local gate validates these responsibilities through `metaserver_raft_harness`,
`build_raft_distributed_conformance_summary.py`, `validate_aws_validation_log.py`, and
`validate_metaserver_production_meta_management.py`. Full global production readiness still requires
multi-process deployment evidence, transport security, and external chaos evidence.
