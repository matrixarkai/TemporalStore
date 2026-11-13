# Container Batch Writes

Scalar set and hash writers distinguish newly appended frames from existing addresses
being refiled. When a fresh frame vacates no previous block, placement can file it
without searching the object's existing block refs. Overwrites that vacate a block,
packed frames and replay retain their existing handling. The recording operation still
obtains a filing witness before changing the resident map.

Set writes already file their blocks and clear matching tombstones. Single-command
and batch paths do not repeat that filing during post-write maintenance.

Within a batch, dirty-object metadata and bucket generations are updated per mutation.
Block dirty propagation runs once per touched object before the shard write guard is
released. Readers cannot observe the intermediate state. The temporary key set is
bounded by batch work; there is no new per-record resident index. Single-command
dirty propagation is unchanged.

No stored format, request deadline, retry policy, model or durability mode changes.

```bash
cargo test -p temporalstore-rust --lib set_batch_work_tests -- --nocapture --test-threads=1
cargo test -p temporalstore-rust --lib recorded_map_invariant -- --test-threads=1
cargo test -p temporalstore-rust --lib write_after_fold -- --test-threads=1
cargo test -p temporalstore-rust --lib shared_hash_fields -- --test-threads=1
```

The work-count checks cover synchronous and asynchronous writes, fresh set and hash
appends, rewrites, remove/re-add, previously clean blocks, rejected batch commands,
one-record staging and recovery without a final unload. End-to-end serving performance
must be measured separately with the intended encoder and full corpus.
