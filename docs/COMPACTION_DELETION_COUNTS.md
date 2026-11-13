# Compaction Deletion Counts

Compaction reports the number of dirty object keys that no longer exist before and
after relocation. That total does not require the lifecycle report's live-block
inventory, object identities, owner sets or collision results.

The compaction path counts these keys directly with the same `record_exists`
predicate used by the full unfiltered lifecycle report. Associated control-state
records still keep a logical key live. The public lifecycle report and selected
bucket reports are unchanged.

This removes report-only block walks and temporary ownership trees from the shard
write-lock region. It does not move or remove ownership validation, relocation,
index publication, fsync barriers or reclaim fences, and does not change the round
budgets or scheduler interval.

Tests compare the count with full lifecycle reports across live/missing dirty keys,
associated records and an 8192-block fixture. The full report is a positive control
for block materialization and identity inversion; the count must perform neither.
These work counts alone do not prove an end-to-end latency improvement.
