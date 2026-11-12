# Streamed Bucket Summaries

The summary-only storage path folds live entries as they are visited, instead
of retaining a shard-wide `Vec<LiveBlockEntry>` before calculating bucket totals.
This path is used by lifecycle planning and other callers that request summaries
without per-bucket digests.

The indexed collecting and streaming paths use the same live-entry visitor. Indexed
row selection, tombstone handling, released-bucket supplementation and model-map
fallbacks are unchanged. The summary fold preserves routing, block and byte
counts, dirty generations, object counts, slab sets and compaction watermarks.
Digest-enabled calls still retain their existing entry list and compute both
summaries and generation digests from that same snapshot.

## Resource Scope

For a resident bucket index, streaming removes the temporary entry vector whose
payload scales with the number of live blocks. It still creates an entry at a
time and maintains the bucket-level maps needed to produce complete summaries.
It does not add a persistent summary cache or another per-object index.

Released-bucket and model-map-only paths retain their existing model-map
materialization. Model-map-only collecting callers return that original list
directly instead of copying it into another vector. The scan remains proportional
to shard size and still runs
under the existing read guard. This is not a bounded lock-hold guarantee or a
claim that all large-corpus mutation stalls have been resolved.

No scheduling interval, dump threshold, WAL format, snapshot fence, retention
policy or public response schema changes. Full summary fields and digest checks
are preserved; the optimization does not substitute missing reports or zeroes.

## Checks

```bash
cargo test -p temporalstore-rust --lib --features alloc-probe streamed_bucket_summaries -- --test-threads=1 --nocapture
cargo test -p temporalstore-rust --lib a_plan_lists_the_shards_blocks_once -- --test-threads=1
```

The four counted checks cover updated/deleted mixed-model rows, released buckets
on a narrow routing range, the model-map-only fallback, and allocator-counted
resident-index work. The allocation check warms both paths, compares complete
summary results, and requires the streaming call to avoid at least one retained
entry payload for 4096 resident rows. Its byte counters are allocation requests,
not process RSS or a measured end-to-end latency improvement. Counters are
process-global, so the checks run serially with the counting allocator enabled.
The same check also rejects another per-entry vector in model-map-only collection,
with a fixed 4096-byte allowance for bookkeeping.

The existing combined-summary/digest checks remain required. Large-corpus API
latency, CPU, memory and correctness need separate clean-runtime measurements;
an allocation check alone does not establish serving performance.
