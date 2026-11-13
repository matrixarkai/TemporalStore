# Maintenance report work

The storage report walk builds a temporary, borrowed lookup of deleted block
identities for each bucket. Looking up each deleted block no longer searches the
bucket's entire tombstone sequence. For T tombstone rows and B block entries in a
bucket, identity lookup takes expected O(T + B) work rather than O(T * B).

The lookup keeps the first row for each physical block, just as the scalar reader
does. Equal addresses in different buckets remain independent. The existing
handle lookup still names misplaced live blocks so ownership validation can report
them. No persisted format, WAL retention fence, timeout, or retry policy changes.

This is a CPU/temporary-memory tradeoff, not a reduction in resident index size.
Only the current bucket's deleted identities are indexed; the map is dropped
before the next bucket. The existing whole-shard entry vector is unchanged.

Live-only mutations also preserve already-reconciled tombstone rows. A known fresh
append, or removal of an actual live block, updates layout counts immediately
without rebuilding the deleted-block membership set. Deleted-block removal,
tombstone sweeps, unknown/replayed installs, and rebuilds keep full reconciliation.
This does not defer work across commands or weaken the final bucket invariants.

Periodic schedulers execute the same lifecycle actions without constructing the
ownership diagnostics and sample vectors that their callers discard. Dump
manifests remain available internally for dirty-state clearing and index-GC
fences. Retention checks, cache actions, stage counters and timer intervals are
unchanged. Explicit `apply_storage_lifecycle` and `run_storage_manager_once`
calls still return full diagnostics; an internal timer result does not expose an
incomplete diagnostic report as a full one.

Periodic compaction likewise omits before/after slab surveys, utility policies and
model-layout diagnostics. It still validates ownership, uses the same relocation
budgets and resume anchor, rebuilds the index and ownership, and publishes the
durable index under the write guard after the block durability barrier. Failed
partial relocations keep their durable publication and resume behavior. Explicit
compaction and manager requests retain complete compaction reports.

Served-index Msgpack serialization uses a bounded 64 KiB writer into the Zstd
frame instead of retaining the full raw index and then copying its compressed
payload into a second container. Level 1 limits compression work during durable
publication under the shard write guard. The container magic, codec, struct
version and decoded state are unchanged; existing level-3 frames remain
readable. Output-size effects depend on the index contents.

Hash-field serialization also borrows its sorted names and addresses directly.
It preserves the map wire shape without cloning the field vector, copying each
field name, or constructing a temporary hash table during index publication.

Ownership rebuilds sort their existing block-filing vector in the same order as
the stored block refs. Each object's refs then arrive in ascending order, so
fresh ref vectors append rather than repeatedly moving a growing suffix. Both
rebuild entry points keep the existing filing door, duplicate handling, deleted
block exclusion and ref counters. The in-place sort costs O(B log B) for B
filings; no resident index or persisted field is added.

## Checks

```bash
cargo test -p temporalstore-rust --lib maintenance_tombstone_walk_tests -- --test-threads=1
cargo test -p temporalstore-rust --lib live_block_layout_tests -- --test-threads=1
cargo test -p temporalstore-rust --lib lifecycle_report_work_tests -- --test-threads=1
cargo test -p temporalstore-rust --lib maintenance_report_tests -- --test-threads=1
cargo test -p temporalstore-rust --lib compaction_execution_tests -- --test-threads=1
cargo test -p temporalstore-rust --features alloc-probe --lib index_encode_work_tests -- --test-threads=1 --nocapture
cargo test -p temporalstore-rust --features alloc-probe --lib hash_field_serialize_tests -- --test-threads=1 --nocapture
cargo test -p temporalstore-rust --lib rebuilt_ref_filing_tests -- --test-threads=1 --nocapture
cargo test -p temporalstore-rust --lib bucket_filing_range -- --test-threads=1
cargo test -p temporalstore-rust --lib reclaim -- --test-threads=1
cargo test -p temporalstore-rust --lib proxy::memory -- --test-threads=1
```

The Mem0 workload runs ingestion, embedding search, metadata updates, and mutation
routes against separate local TemporalStore services. Ollama must already serve
the selected models:

```bash
MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=1 bash scripts/mem0_scale_local.sh \
  --records 16384 --batch-size 128 --users 1 --tenant-shards 1 \
  --searches 256 --metadata-updates 1600 --payload-bytes 512 --infer false \
  --provider-json '{"provider_name":"local","provider_kind":"ollama","base_url":"http://127.0.0.1:11434","model":"qwen2.5:7b","embedding_model":"all-minilm","mock_mode":false,"timeout_ms":30000,"max_retries":0}'
```

Check every operation's error count in the generated report. Nonempty search
results alone do not establish expected-document recall. Routes sampled once do
not establish a latency distribution. Explicit ingestion with `--infer false`
does not establish extraction performance; inferred ingestion must be evaluated
separately. A shared development machine also does not establish isolated CPU or
memory service-level targets.
