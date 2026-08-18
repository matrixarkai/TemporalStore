# Raft write-path fsync amplification — investigation notes

Base: origin/main `17398c2e` (task quoted `ae97d938`; same write path, re-based off current main).
Files: `crates/temporalstore-rust/src/raft.rs`, `raft/cluster_replication.rs`,
`raft/cluster_read_status.rs`, `raft/cluster_inner.rs`, `raft/local_wal.rs`.

## Where every raft-write fdatasync comes from

All raft durability funnels through ONE call: `RaftClusterInner::persist_configured_wal()`
(`raft/cluster_inner.rs:648`). It does:

```
for (node_id, record) in self.wal_records() {      // <-- iterates ALL nodes in the map
    wal.persist_node_segmented(...)                // <-- exactly ONE sync_data() per node
}
```

`persist_node_segmented_with_fsync_threshold` (`local_wal.rs:76`) does exactly one
`file.sync_data()` (line 137) per node record. So:

    fdatasync per persist_configured_wal() call  =  number of nodes in inner.nodes

In the live 3-node cluster each process holds the full 3-node map (built by
`restore_single_shard_from_wal(..., voter_ids(), ...)`, `production_runtime.rs:102` →
`voter_ids()` returns all nodes). So **every `persist_configured_wal()` = 3 fdatasync on the
leader box**, and it is called many times per client write.

### Amplification dimension 1 — `persist_configured_wal` is called on VOLATILE-only mutations

Per client write via `propose_distributed_one` (`raft.rs:4034`), on the leader:

| call site | file:line | what it actually mutates | durable? |
|---|---|---|---|
| `build_append_entries_request` (×live followers) | cluster_replication.rs:233 | `pipeline_state` inflight/queue counters | NO |
| `record_append_entries_response` (main wait loop, ×responses) | cluster_replication.rs:270 | `pipeline_state.match_index/next_index` | NO |
| `record_append_entries_response` (try_recv drain, ×late) | cluster_replication.rs:270 | same | NO |
| commit block | raft.rs:4266 | leader `log` grew + `commit_index` advanced | **YES** |
| post-commit heartbeat loop `record_append_entries_response` (×followers) | cluster_replication.rs:270 | `pipeline_state` | NO |

Gross ≈ (2 + 2 + 2 + 1 + 2) calls × 3 nodes ≈ **27–30 fdatasync/write** (more with catch_up /
snapshot retries → the ~45 gross the benchmark saw). Only ONE of those calls (the commit block)
persists anything Raft requires to be durable.

`match_index` / `next_index` are **volatile leader state** — canonical Raft reinitialises them on
every leader election (§5.3, Fig 2 "Volatile state on leaders"). Persisting them, and fsyncing
3 node records to do it, on every AppendEntries response is pure amplification.

### Amplification dimension 2 — idle read-index / tick fsync storm (~67×/sec even idle)

- `read_index_accounted` (`cluster_read_status.rs:239`) calls `persist_configured_wal()` on the
  **accepted** path just to bump observability counters in `read_safety_state`
  (`read_index_requests/accepted`). The health/heartbeat loop issues read-index ~67×/sec →
  ~200 fdatasync/sec on an otherwise idle leader. `read_safety_state` is metrics only — not
  hard-state, log, membership, or snapshot.
- `advance_time_ms` (the timer tick, `cluster_read_status.rs:91`) persists on every time advance;
  it only refreshes offline/lease/transfer **timeout** state (volatile `pipeline_state`).

### Amplification dimension 3 — persisting ALL N node records

Even the one legitimate commit-block persist fsyncs 3 records (leader + 2 follower *mirror*
records). On a real per-process deployment only the LOCAL node's record is authoritative for that
process's crash recovery; the peer mirror records are re-learned from replication on restart.
(In the distributed write path only the leader's own record actually changes per write, so
dimension-2 coalescing already collapses this to 1 — see below.)

## What is safe to coalesce vs what must stay

Durability/safety invariants that MUST be preserved (a persist MUST happen before the ack when
any of these change):
- **hard_state**: `current_term`, `voted_for`, `commit_index` — durable before responding to
  RequestVote / AppendEntries and before acking a committed write.
- **log entries** — a committed (client-acked) entry must be on disk on a quorum.
- **installed_snapshot / joint_membership / membership / replica_role / apply & storage fences /
  latest_external_snapshot_ref** — membership + snapshot safety.

Safe to NOT fsync (reconstructable / volatile, never gates correctness):
- `pipeline_state` (match_index, next_index, inflight/queue depths, all perf + snapshot-progress
  counters) — reinitialised on election / re-driven on restart.
- `read_safety_state` (read-index & lease accounting counters) — pure metrics.

## The fix (all behind default-OFF gates; byte-identical when OFF)

**Fix A — `TS_RAFT_WAL_COALESCE` (durable-fingerprint skip).** `persist_configured_wal` computes,
per node, a fingerprint over the record with `pipeline_state` + `read_safety_state` cleared. If a
node's fingerprint is unchanged since its last persisted value, skip its fsync. This is *driven by
whether durable state changed*, so it can never skip a persist durability requires (a false
"changed" only costs an extra safe fsync; the skip only fires when hard_state/log/membership/
snapshot/fences are all identical). Effect: idle read-index/tick storm → 0; per-response
match_index fsyncs → 0; a committed write → exactly the commit-block persist of the ONE node whose
log+commit_index advanced = **~1 fdatasync/write**.

**Fix B — configurable replication deadline.** New `RaftConfig.replication_deadline_ms`
(default 5000 = byte-identical) replaces the hardcoded `Duration::from_secs(5)` in
`propose_distributed_one`. The cluster can drop it (e.g. 500 ms) so a lagging/rejecting follower
no longer freezes the proposer for a full 5 s. Safe: on timeout the propose returns
`NoMajority` (retryable) — it never commits anything it shouldn't.

**Fix C — `TS_RAFT_PROPOSE_SERIALIZE` (in-order propose).** Concurrent proposes append under the
write lock (sequential indices) but release it before the network phase, so their AppendEntries
race and arrive out of order at followers → `prev_log` mismatch → reject → each stalls the full
deadline. A per-cluster serialize mutex held across the append+replicate+commit critical section
forces proposes to commit in log order, eliminating the mismatch/stall. Safe: pure serialisation,
no change to what commits.

Fold-in: wt-raftevt widened `max_inflights_replicate` 128→1024 as a plain default; kept at 128
here to stay byte-identical — the cluster sets 1024 via config alongside the gates.
