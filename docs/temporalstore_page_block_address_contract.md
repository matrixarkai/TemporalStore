# TemporalStore Block Address Contract

TemporalStore has one block tier. The older word `page` names that same tier;
it does not describe a second logical or physical layer. Use **block** in new
implementation names and documentation. A slab holds block bytes, and a bucket
owns index entries. MatrixObject has its own chunk/page/fragment hierarchy;
that hierarchy does not define TemporalStore's storage layout.

The filename of this document remains stable for existing links.

## Address and index implementation

[`BlockAddress`](../crates/temporalstore-rust/src/block_store.rs) contains one
64-bit address word: the slab id occupies the high 32 bits and the offset the
low 32 bits. `ElementEntry` holds that address together with length, block id,
and presence bits. Its in-memory size is 16 bytes, checked in the source.
Use its accessors rather than treating report samples as a serialized address.

The block id identifies a block of an object. Zero is a valid id. Absence of an
id and absence of a generation are distinct states; neither may be replaced by
a zero sentinel. Generation and routing are derived using the current address
and bucket rules, rather than introducing another page identity.

[`BlockIndexNamedBy`, `BlockEntryAt`, and `StoredBlockEntry`](../crates/temporalstore-rust/src/engine/engine_storage_state.rs)
encode or decode the block index. The enclosing map supplies the object identity;
an index entry's stored payload carries `address`, `dirty`, and `deleted`.
There is no separate `PageIndex` storage tier or independent `PageAddress` type.

## Stored-data compatibility

Renaming implementation symbols does not authorize changing stored keys,
checksums, format stamps, or recovery behavior.

- The bucket-node writer still emits `page_index`. Its reader accepts
  `block_index`, `page_index`, and `page_refs` for that field.
- Older accepted field spellings are decoded through explicit aliases. Keep
  those aliases when changing Rust names.
- The address word is stored under `a`. The earlier split address representation
  (`ps` and `o`, or `page_segment_id` and `offset`) is rejected by the current
  decoder. That existing format boundary is independent of this naming cleanup;
  renaming must not turn a rejected address into a different accepted address.
- Keep the committed compatibility corpus and durable-format stamps unchanged
  for a naming-only change.

See the address codec in [`block_store.rs`](../crates/temporalstore-rust/src/block_store.rs)
and the bucket-node codec in [`state.rs`](../crates/temporalstore-rust/src/engine/engine_storage_state.rs).

## Published reports

[`PublicStorageContract`](../crates/temporalstore-rust/src/engine/engine_reports.rs)
maps both `page_address` and `block_address` to `ElementEntry`. Published
`page_index_entry` and `block_index_entry` labels and their sample structures
remain available to existing report consumers. They are report views, not
instructions to create two storage tiers.

`PublicStorageFeatureShapes` retains the published field lists, including
`page_address_fields`, `block_address_fields`, and the index-entry field lists.
These lists describe report projections. They do not define the fields or
memory layout of the Rust `BlockAddress` or `ElementEntry` structures.

Likewise, older `page_*` statistics remain where they mirror published `block_*`
statistics. Both names must describe the same measurement. Do not remove a
published key, change its value, or add a `legacy_alias` field as a side effect
of renaming an internal field. New implementation symbols should use block,
slab, or bucket according to the object they describe.

## Remaining page spellings

The remaining spellings have specific compatibility or integration roles:

- Published report structures and fields, including `StoragePageAddressSample`,
  `StoragePageIndexEntrySample`, `page_address`, and the mirrored statistics.
- The deprecated `TemporalEngine::page_store` forwarding method.
- MatrixCache's `CacheKey::page_with_slot`, `page_with_slot_generation`, and
  `invalidate_page_segment` API calls. Their names belong to that dependency.
- MatrixObject's `page_size` setting, which describes its actual page tier.
- Compatibility test names and accepted stored field strings.

These are not permission to introduce page-named TemporalStore implementation
types. Preserve their behavior when updating callers or dependency versions.

## Validation

The executable naming checks live in
[`page_and_block_name_one_tier.rs`](../crates/temporalstore-rust/src/engine/tests/page_and_block_name_one_tier.rs).
They verify that both address labels resolve to one type, that published
statistics agree across the two spellings using real reads and writes, and
that normalization operates on whole name segments. Unrelated words such as
`homepage` and `stoppage` must remain untouched.

Run the checks from the repository root:

```sh
cargo test --release -p temporalstore-rust --lib page_and_block_name_one_tier -- --test-threads=1
cargo test --release -p temporalstore-rust --lib block_entry_names -- --test-threads=1
cargo test --release -p temporalstore-rust --lib projection_names_a_block -- --test-threads=1
```

Each command must run at least one test. The block-entry and projection tests
exercise stored entries and report naming; they complement the address-label
and mirrored-statistic checks. A successful name scan alone cannot establish
that stored data still decodes correctly.
