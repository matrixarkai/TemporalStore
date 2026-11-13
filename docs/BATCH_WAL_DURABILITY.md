# Batch WAL Durability

Normal batch execution uses the same synchronous durability source as single
execution: the WAL. It still writes the served-index delta, but no longer waits
for an additional index-log barrier in the default WAL-recovery mode. The
legacy recovery mode retains its synchronous index-log barrier. Raft application
continues using its existing durability source. Checkpoint/catalog barriers,
segment sealing and retention fences are unchanged.

Every acknowledged synchronous batch must still pass its WAL barrier. Shared-row
batches that register a pending commit continue waiting for that commit after
releasing the engine guard. No request deadline, retry policy or async-storage
default changes. No WAL or index encoding changes.

A bounded live diagnostic on the published runtime captured a foreground
metadata-batch request inside fdatasync, called by index-log append, after a
405,183-us failed request. The maintenance thread was waiting for that request's
shard lock. The debugger detached with TracerPid zero and the owned services and
temporary data were removed. That is one blocking-path sample, not proof that
all timeouts or the earlier nine-second add tail share this cause. Timings after
debugger attachment are not performance results.

Three subprocess recovery scenarios and their child fixture cover:

- Loss of block files, cache, base index and index log after acknowledged mixed
  mutations and packed hash batches, retaining the WAL.
- Loss of the index log and real post-checkpoint block tails, retaining the
  checkpoint's block prefix and WAL.
- Legacy recovery with its required index-log barriers retained.

The writer checks actual barrier counters and exits without Rust destructors.
The parent then removes the deferred state and starts a fresh recovery process.
Default recovery must report actual WAL replay. Readback checks all packed
values, exact counter totals, deleted fields and set membership. This is
an explicit deferred-state loss model, not a physical power-cut test.

The counted CI filter executes the three parent scenarios plus their subprocess
fixture. Macro latency, CPU and memory gains require a fresh clean-release run;
the source change and recovery tests alone do not establish those gains.
