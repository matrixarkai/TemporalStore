# Count Deltas in Mixed Memory Writes

Memory adds update catalog rows, ordering members and identity counts in one native
batch. A positive identity count changes one sorted-set member. Its index-log record
must describe that member, not force every other command in the batch to restate its
entire object.

`ZSetCountAdjust` uses the incremental block path when the member remains present
after the write. The existing container-state carry records its actual score and
address. A count that reaches zero still uses snapshot semantics so recovery removes
the member. No stored format, durability mode, deadline or retry policy changes.

The regression fixture writes eight hash fields, eight ordering members and one
identity count. It emits 17 index items with either 32 or 2,048 pre-existing catalog
rows. The recovery fixture covers positive and negative adjustments, zero removal,
absent decrements, unrelated members and mixed hash writes. It reopens without a
clean unload so a final index snapshot cannot hide an incorrect delta.

```bash
cargo test -p temporalstore-rust --lib zset_count_adjust -- --nocapture --test-threads=1
```

These checks establish bounded delta output and count recovery, not full-corpus API
latency, throughput or model-inference performance. Run the serving workload separately
with the intended encoder, corpus size, payload size and unchanged request deadlines.
