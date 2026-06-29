# RustRaft Metaserver Failover Proof

This report records the focused metaserver failover gate for the RustRaft-based
TemporalStore metaserver runtime.

## Command

```bash
cargo run -p temporalstore-rust --bin metaserver_raft_harness -- \
  --root /tmp/temporalstore-rustraft-metaserver-failover-latest/metaserver-raft \
  > /tmp/temporalstore-rustraft-metaserver-failover-latest/metaserver-raft.json

python3 tools/validate_aws_validation_log.py \
  --job temporalstore-metaserver-raft-validation \
  --log /tmp/temporalstore-rustraft-metaserver-failover-latest/metaserver-raft.json
```

## Result

Status: passed.

| Check | Value |
|---|---|
| Initial voters | `[10, 11, 12]` |
| Membership after add | `[10, 11, 12, 13]` |
| Membership after remove | `[10, 11, 13]` |
| Leader before transfer | `10` |
| Leader after transfer | `11` |
| Leader after failover | `10` |
| Namespace visible after failover | `true` |
| No-majority write rejected | `true` |
| Route read after voter replacement | `meta-after-replace` |
| Route read after scale-down | `meta-after-second-scale-down` |
| Scheduler coverage ready | `true` |
| Meta process rollout ready | `true` |

## What The Harness Proves

1. A three-node metaserver RustRaft group starts with a live majority.
2. A namespace write commits before snapshot/failover.
3. A voter is added, learner-style unsupported roles fail closed, and membership
   replacement is applied safely.
4. A snapshot can restore a lagging voter while preserving the snapshot floor.
5. Tail entries after the snapshot remain invisible until catch-up, then become
   readable after log replay.
6. Leadership can transfer to a target voter.
7. When the transferred leader is marked unavailable, a surviving majority
   elects a new leader and accepts a namespace write.
8. Membership replacement and scale-down after failover keep shard routes
   readable.
9. When the cluster loses majority, writes fail closed instead of committing
   unsafe metadata.

## Artifact

Raw artifact:

```text
/tmp/temporalstore-rustraft-metaserver-failover-latest/metaserver-raft.json
```

The artifact includes the full metaserver membership, snapshot, failover,
scheduler, data-Raft ownership, and rollout evidence.
