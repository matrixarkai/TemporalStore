# TemporalStore Page/Block Address Contract

This is the shared storage-address contract for and Rust TemporalStore. It defines
the public report/API vocabulary for page addresses, block addresses, logical indexes,
and GC metadata. and Rust may keep different private implementation details, but
public reports, conformance tests, metrics, and compatibility docs should use these names.

## Goals

- Give and Rust one vocabulary for page/block storage.
- Make page/block index reports comparable across both engines.
- Prevent public naming drift such as `page_store` in one backend and `block_store`
  in another when the concept is the same serving/durable page layer.
- Provide a stable target for shared conformance tests, recovery tests, compaction tests,
  and storage lifecycle reports.

## Canonical Address Types

### PageAddress

`PageAddress` identifies a logical page slice inside the TemporalStore serving
storage layer.

Required fields:

```json
{
  "shard_id": 0,
  "zone_id": 0,
  "segment_id": 0,
  "page_id": 0,
  "offset": 0,
  "length": 0,
  "generation": 0
}
```

Field meanings:

| field | meaning |
|---|---|
| `shard_id` | Shard/partition that owns the logical record range. |
| `zone_id` | Storage zone or local/shared durable placement group. |
| `segment_id` | Segment/file/blob within the zone. |
| `page_id` | Monotonic page identifier within the shard/segment namespace. |
| `offset` | Byte offset of the page payload or page slice. |
| `length` | Byte length of the page payload or page slice. |
| `generation` | Rewrite/compaction generation used to reject stale addresses. |

Ordering must be stable by:

```text
shard_id, zone_id, segment_id, page_id, offset, generation
```

### BlockAddress

`BlockAddress` identifies the physical durable bytes that back one page or page
slice.

Required fields:

```json
{
  "shard_id": 0,
  "zone_id": 0,
  "block_id": 0,
  "offset": 0,
  "length": 0,
  "checksum": 0
}
```

Field meanings:

| field | meaning |
|---|---|
| `shard_id` | Shard/partition that owns the physical block reference. |
| `zone_id` | Durable storage zone or shared-store placement group. |
| `block_id` | Durable block/blob/file identifier. |
| `offset` | Byte offset inside the block. |
| `length` | Byte length of the physical payload. |
| `checksum` | Payload checksum used for corruption detection. |

## Canonical Index Types

### PageIndex

`PageIndex` maps logical object or timestamp ranges to `PageAddress` values.

Required logical key:

```text
model/table/object_key[/field][/timestamp_range]
```

Required value:

```json
{
  "page_addresses": [],
  "min_timestamp_ms": 0,
  "max_timestamp_ms": 0,
  "append_watermark": 0,
  "generation": 0
}
```

Expected use:

- Timestamp-keyed models, including context events and feature/sequence-style
  records, use `min_timestamp_ms` and `max_timestamp_ms` for range lookup.
- Object-style models use the object key plus append watermark to find the
  current page chain.
- Compaction may rewrite page addresses, but must advance `generation` or
  `append_watermark` so stale readers can be detected.

### BlockIndex

`BlockIndex` maps physical block ranges to durable locations.

Required value:

```json
{
  "block_address": {},
  "storage_uri": "",
  "created_at_ms": 0,
  "sealed_at_ms": 0,
  "generation": 0,
  "checksum": 0
}
```

Expected use:

- Local mode may use file paths or local block ids.
- Shared-store/cloud mode may use object-store, shared file, or block-service
  locations.
- Public reports should expose `BlockIndex`, not backend-specific names such as
  local page segment internals.

### ObjectIndexEntry

`ObjectIndexEntry` maps `{model/table/object_key}` to the current page chain or
segment list. Internal code may still call this an object index, but public
reports use `ObjectIndexEntry`.

Required value:

```json
{
  "model": "",
  "table": "",
  "object_key": "",
  "page_chain": [],
  "segment_ids": [],
  "append_watermark": 0,
  "generation": 0
}
```

Expected use:

- The hot read path should use `ObjectIndexEntry -> PageIndexEntry -> PageAddress`.
- Recovery should rebuild or validate `ObjectIndexEntry` from page/block manifests and
  append logs.
- and Rust should report the same object-index shape even if their internal
  slot map or shard map implementation differs.

### Stream, Segment, Extent, And Slot

`Stream` is the public append-log or blob-stream concept used for durable record
ordering and replay. Backend terms such as `oplog` or `stream_blob` are private
implementation names and must be emitted only under `compatibility_aliases`.

`Segment` is the public sealed or writable storage segment concept. A segment may
map to a local file, shared-store blob, stream blob, or page segment internally.

`Extent` is a contiguous physical byte range inside a durable block or segment.

`Slot` is the public ownership/routing unit that binds logical records, page
references, tombstones, and dirty generations to a shard-owned lifecycle lane.

### Tombstone, GcEligibility, And FollowerCursorSafety

`Tombstone` records logical delete evidence. `GcEligibility` records whether a
record or page/block is safe to compact or reclaim. `FollowerCursorSafety`
records whether Raft/shared-store followers, snapshots, or replay cursors still
need stale pages or blocks.

Required value:

```json
{
  "object_key": "",
  "page_addresses": [],
  "deleted_at_ms": 0,
  "evict_after_ms": 0,
  "compaction_eligible_after_ms": 0,
  "generation": 0,
  "reason": ""
}
```

Required safety gates before physical reclaim:

- logical tombstone is durable;
- no live `ObjectIndexEntry` or `PageIndexEntry` points to the page address;
- no snapshot/replay/audit retention needs the raw record;
- `FollowerCursorSafety` proves no Raft/shared-store follower cursor still needs
  the page/block;
- compaction generation is newer than the stale page generation.

## Read/Write Behavior Conformance

and Rust must expose the same logical read/write behavior even if their
private page-store or block-store internals differ.

### Write Path

Canonical write flow:

```text
append record
-> route shard
-> choose page
-> append to page buffer
-> update page index
-> flush page/block
-> update block index
-> publish append watermark
```

Canonical report sequence:

```json
{
  "storage_write_sequence": [
    "append_record",
    "route_shard_slot",
    "choose_page",
    "append_page_buffer",
    "update_page_index",
    "flush_page_block_segment",
    "update_block_index",
    "publish_append_watermark"
  ]
}
```

Required write sequence step names:

- `append_record`
- `route_shard_slot`
- `choose_page`
- `append_page_buffer`
- `update_page_index`
- `flush_page_block_segment`
- `update_block_index`
- `publish_append_watermark`

Required behavior per step:

| step | required conformance behavior |
|---|---|
| append record | Accept a typed record with logical model/table/object key, optional field, optional timestamp, payload bytes, and write options. |
| route shard | Derive the same shard/partition decision from the shared routing key. Context records should use the agreed placement key, not broad tenant scans. |
| choose page | Pick or allocate a writable page using the same target page/block sizing policy and generation semantics. |
| append to page buffer | Append without mutating old page bytes in place; produce a new or updated `PageAddress` with offset, length, and generation. |
| update page index | Update `PageIndex` so logical object/timestamp lookups can find the new `PageAddress`. |
| flush page/block | Persist according to write mode: async may acknowledge after durable queue/oplog admission, sync must wait for configured durability. |
| update block index | Update `BlockIndex` with durable physical location, checksum, and generation. |
| publish append watermark | Advance and publish `append_watermark` after index state and durability requirements are satisfied. |

Required write-path outputs:

```json
{
  "shard_id": 0,
  "slot": "slot:0",
  "placement_key": "context:t=...|u=...|s=...|:node=...",
  "page_address": {},
  "block_address": {},
  "append_watermark": 0,
  "batch_watermark": 0,
  "durability": "async|sync|raft",
  "storage_family": "local|shared_store|raft",
  "write_mode": "async|sync",
  "index_generation": 0,
  "records_appended": 0
}
```

Required write result fields:

- `shard_id`
- `slot`
- `placement_key`
- `page_address`
- `block_address`
- `append_watermark`
- `durability`
- `storage_family`
- `write_mode`
- `index_generation`
- `batch_watermark`
- `records_appended`

Required write-path metrics:

- `append_queue_wait_ms`
- `append_engine_ms`
- `append_queue_depth`
- `append_batch_size`
- `append_batch_bytes`
- `append_coalesced_writes`
- `append_durability_failures`
- `append_watermark`
- `page_writes`
- `block_writes`
- `bytes_written`

Conformance rules:

- The write result must contain canonical `PageAddress`, `BlockAddress`, and
  `append_watermark` fields in public reports.
- `placement_key` and `slot` must make the shard/slot route visible enough to
  compare and Rust write placement decisions.
- `storage_family`, `write_mode`, and `durability` must match when and Rust
  are run under the same test config.
- and Rust may acknowledge at different internal points only when
  `storage_options.write_mode` differs; same write mode must have the same
  durability contract.
- A write must not publish an append watermark before the corresponding
  `PageIndex` and `BlockIndex` state is recoverable.
- Failed writes must not leave public indexes pointing to unflushed or
  checksum-invalid block data.
- Batch append must preserve per-record logical ordering within the same
  shard/object/timestamp range, publish a `batch_watermark`, and report
  `records_appended`.
- `append_durability_failures` must be zero for conformance acceptance.

### Read Path

Canonical read flow:

```text
logical key/timestamp range
-> object/page index lookup
-> page address list
-> block index lookup
-> page read
-> decode records
-> return filtered result
```

Canonical report sequence:

```json
{
  "storage_read_sequence": [
    "logical_key_timestamp_range",
    "object_page_index_lookup",
    "page_address_list",
    "block_index_lookup",
    "page_read",
    "decode_records",
    "return_filtered_result"
  ]
}
```

Required read sequence step names:

- `logical_key_timestamp_range`
- `object_page_index_lookup`
- `page_address_list`
- `block_index_lookup`
- `page_read`
- `decode_records`
- `return_filtered_result`

Required read result fields:

- `logical_key`
- `timestamp_range`
- `object_index_entry`
- `page_index_entries`
- `page_addresses`
- `block_index_entries`
- `records_decoded`
- `records_returned`
- `tombstones_filtered`
- `stale_generations_filtered`
- `filter_policy`

Required read-path metrics:

- `object_page_index_lookup_count`
- `object_page_index_lookup_ms`
- `page_address_count`
- `block_index_lookup_count`
- `block_index_lookup_ms`
- `page_reads`
- `decode_records_ms`
- `records_decoded`
- `records_returned`
- `tombstones_filtered`
- `stale_generations_filtered`

Required behavior:

- Point reads may use `ObjectIndexEntry` to resolve the current logical object chain,
  then must use `PageIndex -> BlockIndex` for durable page lookup.
- Timestamp range reads must use `PageIndex` range lookup before reading blocks.
- `PageIndex` returns ordered `PageAddress` values for the logical key or
  timestamp range; `BlockIndex` resolves each page's durable physical location
  before any record bytes are decoded.
- Reads must reject stale generations and tombstoned records unless an explicit
  replay/debug policy asks for retained historical data.
- `return_filtered_result` must report `records_decoded`, `records_returned`,
  `tombstones_filtered`, and `stale_generations_filtered`; `records_returned`
  must not exceed `records_decoded`.
- Cold scans must use no-cache/no-promote reads by default.

### Cold Scan Path

Canonical cold scan flow:

```text
timestamp range
-> page index scan
-> no-cache page read
-> bounded decode
-> no hot-cache promotion
```

Canonical report sequence:

```json
{
  "storage_cold_scan_sequence": [
    "timestamp_page_index_scan",
    "no_cache_page_read",
    "bounded_decode",
    "no_hot_cache_promotion"
  ]
}
```

Required cold scan sequence step names:

- `timestamp_page_index_scan`
- `no_cache_page_read`
- `bounded_decode`
- `no_hot_cache_promotion`

Required cold scan result fields:

- `timestamp_range`
- `page_index_scan`
- `no_cache_page_reads`
- `decode_batch_limit`
- `decode_byte_limit`
- `deadline_ms`
- `records_decoded`
- `records_returned`
- `hot_cache_promotions`
- `cache_fill`
- `promotion_policy`

Required cold scan metrics:

- `cold_scan_no_cache_reads`
- `cold_scan_page_index_scan_count`
- `cold_scan_page_index_scan_ms`
- `cold_scan_page_reads`
- `cold_scan_decode_records_ms`
- `cold_scan_records_decoded`
- `cold_scan_records_returned`
- `cold_scan_decode_batch_limit`
- `cold_scan_decode_byte_limit`
- `hot_cache_promotions`

Required behavior:

- Cold lifecycle scans must start from a timestamp range and use `PageIndex`
  range iteration rather than warming a serving object cache.
- Page reads must be marked no-cache/no-promote by default when
  `TS_COLD_SCAN_NO_CACHE_FILL=true`.
- Decode work must be bounded by batch size, byte budget, and deadline so
  compression, backfill, audit, or GC workers cannot starve serving retrieval.
- `storage_cold_scan_contract.cache_fill` must be `false` and
  `storage_cold_scan_contract.promotion_policy` must be `no_promote`.
- `storage_cold_scan_contract.hot_cache_promotions` must be `0`.
- `storage_cold_scan_contract.records_returned` must not exceed
  `storage_cold_scan_contract.records_decoded`.
- Cold scan reads may write warm summaries, tombstones, or compaction metadata,
  but raw source pages must not enter hot LRU/admission unless an explicit
  replay/query path reinforces them.

## Public Config Conformance

and Rust must expose the same public storage tuning knobs. The names below
are the public contract; each backend may map them into private gflags, typed
configs, or environment readers internally.

| knob | meaning | default |
|---|---|---:|
| `TS_CONTEXT_PAGE_TARGET_BYTES` | Target packed context timestamp page size. | `65536` |
| `TS_BLOCK_SLAB_TARGET_BYTES` | Target durable block/segment size before rolling. | `1073741824` |
| `TS_STORAGE_ZONE_SIZE` | Storage zone target used by lifecycle and placement. | `10485760` |
| `TS_STREAM_MAX_BLOB_SIZE` | Stream/blob cap; effective segment target is the lower of this and block segment target. | `10485760` |
| `TS_COMPACTION_WATERMARK_BYTES` | Compaction scheduling/reclaim watermark. | `268435456` |
| `TS_COLD_SCAN_NO_CACHE_FILL` | Default no-cache/no-promote behavior for cold lifecycle scans. | `true` |
| `TS_PAGE_INDEX_CACHE_BYTES` | Page-index cache budget for object/range lookup metadata. | `67108864` |
| `TS_BLOCK_INDEX_CACHE_BYTES` | Block-index cache budget for physical address metadata. | `67108864` |

Required report shape:

```json
{
  "effective_storage_tuning": {
    "TS_CONTEXT_PAGE_TARGET_BYTES": 65536,
    "TS_BLOCK_SLAB_TARGET_BYTES": 1073741824,
    "TS_STORAGE_ZONE_SIZE": 10485760,
    "TS_STREAM_MAX_BLOB_SIZE": 10485760,
    "TS_COMPACTION_WATERMARK_BYTES": 268435456,
    "TS_COLD_SCAN_NO_CACHE_FILL": true,
    "TS_PAGE_INDEX_CACHE_BYTES": 67108864,
    "TS_BLOCK_INDEX_CACHE_BYTES": 67108864
  }
}
```

Conformance rules:

- launchers must map `TS_STORAGE_ZONE_SIZE` to `--storage_zone_size` and
  `TS_STREAM_MAX_BLOB_SIZE` to `--stream_max_blob_size` until native gflags use
  the public names directly.
- Rust must read all eight names through its typed storage tuning config.
- Benchmark and scale reports must include `effective_storage_tuning` at the
  top-level config and per backend.
- A conformance gate should fail if one backend reports a missing knob or a different
  effective value under the same run config.

## Normalize Naming

Public APIs, reports, metrics, docs, conformance tests, and externally visible JSON
must use the canonical names below. Private implementation names may still exist
inside or Rust, but they must be translated before data leaves the backend.

Canonical names:

- `PageAddress`
- `BlockAddress`
- `PageIndexEntry`
- `BlockIndexEntry`
- `ObjectIndexEntry`
- `StorageZone`
- `Stream`
- `Segment`
- `Extent`
- `Slot`
- `AppendWatermark`
- `CompactionWatermark`
- `Tombstone`
- `GcEligibility`
- `FollowerCursorSafety`

Canonical field names:

- `page_address`
- `block_address`
- `page_index_entry`
- `block_index_entry`
- `object_index_entry`
- `storage_zone`
- `stream`
- `segment`
- `extent`
- `slot`
- `append_watermark`
- `compaction_watermark`
- `tombstone`
- `gc_eligibility`
- `follower_cursor_safety`

Public feature shape keys:

- `page_address_fields`: `shard_id`, `zone_id`, `segment_id`, `page_id`, `offset`, `length`, `generation`
- `block_address_fields`: `shard_id`, `zone_id`, `block_id`, `offset`, `length`, `checksum`
- `page_index_entry_fields`: `logical_key`, `timestamp_range`, `page_addresses`, `append_watermark`, `generation`
- `block_index_entry_fields`: `page_address`, `block_address`, `extent`, `checksum`, `generation`
- `object_index_entry_fields`: `model`, `table`, `object_key`, `page_chain`, `tombstone`, `generation`
- `storage_zone_fields`: `zone_id`, `total_bytes`, `used_bytes`, `stale_bytes`, `segments`
- `stream_fields`: `stream_id`, `segments`, `rollover_count`, `sealed_segment_count`
- `segment_fields`: `segment_id`, `extent`, `start_offset`, `sealed`, `generation`
- `extent_fields`: `extent`, `block_range`, `reclaim_state`, `generation`
- `slot_fields`: `slot_id`, `dirty_generation`, `object_refs`, `page_refs`, `tombstones`, `owner_mismatch_count`
- `append_watermark_fields`: `shard_id`, `slot_id`, `log_index`, `timestamp_ms`
- `compaction_watermark_fields`: `shard_id`, `safe_generation`, `safe_timestamp_ms`, `follower_floor`
- `tombstone_fields`: `ref`, `generation`, `deleted_at_ms`, `reason`
- `gc_eligibility_fields`: `ref`, `eligible_after_ms`, `has_tombstone`, `follower_safe`, `reclaimable_bytes`
- `follower_cursor_safety_fields`: `min_follower_cursor`, `blocked_reclaim_bytes`, `safe_to_reclaim`

Avoid drifting pairs in public output:

| drifting pair | canonical public name | note |
|---|---|---|
| `page_store` vs `block_store` | `StorageZone`, `PageIndexEntry`, `BlockIndexEntry` | Use implementation-specific store names only in private logs or migration notes. |
| `zone` vs `extent` | `StorageZone` for placement, `Extent` for contiguous physical byte ranges | Do not use one backend's private name as the public contract. |
| `stream blob` vs `page segment` | `Stream`, `Segment` | A segment may map to a local file, shared blob, or stream blob. |
| `oplog` vs `wal` | `Stream`, `AppendWatermark` | Public reports should describe append-log order through shared stream/watermark terms. |
| `ShardStats.page_store` vs `ShardStats.block_store` | `ShardStats.storage_zone`, `ShardStats.page_index`, `ShardStats.block_index` | Shard stats must be comparable between and Rust. |

Compatibility aliases are allowed only when all three conditions are true:

1. The canonical field is present in the same report.
2. The alias is clearly marked as `legacy_alias` or appears only in a migration
   section.
3. The conformance gate ignores the alias and validates only the canonical field.

New public report fields must not introduce backend-specific names such as:

- `page_store` for but `block_store` for Rust;
- `page_segment_id` when the report means canonical `segment_id`;
- `oplog`, `oplog_id`, or `oplog_sequence` when the report means canonical
  `AppendWatermark` or append-log lifecycle metrics;
- `extent_id` when the report means canonical `block_id`, `segment_id`, or
  `extent` without stating the mapping.

## Migration Strategy

### Phase 1: Shared Schema And Aliases

Phase 1 is documentation and compatibility mapping. and Rust must both
publish the shared schema in docs and tests before changing production report
payloads.

Required Phase 1 outputs:

- canonical schema for `PageAddress`, `BlockAddress`, `PageIndexEntry`,
  `BlockIndexEntry`, `ObjectIndexEntry`, `StorageZone`, `Stream`, `Segment`,
  `Extent`, `Slot`, `AppendWatermark`, `CompactionWatermark`, `Tombstone`,
  `GcEligibility`, `FollowerCursorSafety`, and page/block metrics;
- explicit alias map for old names such as `page_store`, `block_store`,
  `page_segment_id`, `zone_id`, `extent_id`, `stream_blob`, `oplog`,
  `oplog_id`, and `oplog_sequence`;
- fail-closed validators for the shared page-address corpus, public storage
  knobs, and page/block metric names;
- report examples showing both the canonical field and any compatibility alias.

Phase 1 rules:

- Aliases may be read by tools and dashboards, but conformance gates must validate
  the canonical fields.
- New reports must include canonical fields even when old alias fields are still
  emitted for compatibility.
- Compatibility aliases must be marked as `legacy_alias` or placed under a
  `compatibility_aliases` object so they cannot be confused with the public
  contract.

### Phase 2: Rename/Report Compatibility

Phase 2 changes public reports and APIs to prefer canonical names without
breaking existing consumers.

Required Phase 2 behavior:

- and Rust report parsers accept old and new field names.
- Existing old report fields remain readable through adapters or are emitted
  under `compatibility_aliases` for one compatibility window.
- Report parsers normalize old and new field names into the same in-memory
  shape before comparison.
- CI verifies that old reports still parse, new reports can include canonical
  names, and mixed conformance report pairs compare on canonical fields only.
- Deprecation warnings identify alias usage, but do not fail old-report parsing
  until the compatibility window ends.

### Phase 3: Rust Public Struct Names

Phase 3 updates Rust public structs, report DTOs, metrics payloads, and
documentation to match the/shared public names.

Required Phase 3 behavior:

- Rust public output uses `PageAddress`, `BlockAddress`, `PageIndexEntry`,
  `BlockIndexEntry`, `StorageZone`, `Segment`, `Extent`, `AppendWatermark`,
  and `CompactionWatermark`.
- Rust private implementation names may remain internally, but conversion to
  public DTOs must happen before data leaves the backend.
- Rust compatibility deserializers continue reading old report fields through
  `compatibility_aliases`.

### Phase 4: Report Shape Conformance

Phase 4 updates public reports to emit the same canonical shape as Rust.

Required Phase 4 behavior:

- reports include the same canonical fields, nesting, metric names, and
  effective config fields as Rust.
- compatibility aliases remain available for old dashboards and benchmark
  artifacts during the compatibility window.
- conformance comparison reports normalize both sides and show alias usage as a
  warning, not as a separate metric family.

### Phase 5: Shared Tests

Phase 5 makes the shared tests the source of truth for both engines.

Required Phase 5 coverage:

- shared page-address compatibility corpus;
- shared page/block metrics conformance validator;
- old-report compatibility fixtures;
- and Rust native tests for page split, compaction rewrite, tombstones,
  no-promote cold scans, crash/restart index rebuild, and watermark behavior;
- comparison tests that prove canonicalized and Rust reports are equivalent.

### Phase 6: Drift Gates

Phase 6 turns compatibility into enforcement.

Required Phase 6 gates:

- fail if public fields drift between and Rust;
- fail if effective storage config fields drift or are missing;
- fail if page/block metric names drift or are missing;
- fail if canonical fields are absent from new reports;
- fail if alias fields appear outside `compatibility_aliases` after the
  compatibility window;
- fail if broad legacy report paths are used in production-performance conformance
  claims.

Removal gate:

- remove or hide alias output only after dashboards, benchmark reports, portal
  pages, tests, Rust tests, replay/audit tooling, and conformance gates all read
  the canonical schema directly.

## conformance Mapping

| Canonical term | current/private source | Rust current/private source | Public output |
|---|---|---|---|
| `PageAddress` | `partition::PageIndex.address` plus page metadata | page-envelope address metadata | `PageAddress` |
| `BlockAddress` | page-store stream/blob location | `BlockAddress` in block/page segment layer | `BlockAddress` |
| `PageIndexEntry` | partition index slot pages | core/shard index page refs | `PageIndexEntry` |
| `BlockIndexEntry` | page-store zone/stream manifest | block-store extent/segment manifest | `BlockIndexEntry` |
| `ObjectIndexEntry` | model/object slot layout | shard model maps/core index | `ObjectIndexEntry` |
| `Tombstone` / `GcEligibility` | deleted/dirty page and delayed destroy state | tombstone/GC report state | `Tombstone` / `GcEligibility` |
| `FollowerCursorSafety` | follower cursor and snapshot retention state | raft/shared-store cursor safety state | `FollowerCursorSafety` |

## Required Conformance Tests

Shared conformance conformance cases must cover:

- encode/decode `PageAddress`;
- encode/decode `BlockAddress`;
- stable address ordering;
- timestamp range lookup through `PageIndex`;
- slot lookup through `Slot` index;
- object lookup through `ObjectIndexEntry`;
- page address lookup through `PageIndexEntry`;
- durable location lookup through `BlockIndexEntry`;
- page split and page-chain update;
- compaction rewrite preserving logical records;
- stale generation rejection;
- tombstone creation before physical reclaim;
- restart/recovery rebuilding `PageIndex`, `BlockIndex`, and `ObjectIndexEntry`;
- cold scan using no-cache/no-promote reads.

The shared `compat/page_address_compatibility_corpus.json` corpus covers the
PageAddress and BlockAddress subset that both and Rust must consume:

- encode/decode `PageAddress`;
- encode/decode `BlockAddress`;
- stable ordering by `{shard_id, zone_id, segment_id, page_id, offset}`;
- stable `BlockAddress` ordering by `{shard_id, zone_id, block_id, offset}`;
- timestamp range -> page address lookup;
- slot index maps `Slot` -> object refs and page refs;
- object index maps `{model/table/object_key}` -> current page chain;
- page index maps logical timestamp/key ranges -> page addresses;
- block index maps page addresses -> physical durable locations;
- page split behavior;
- page compaction rewrite preserving logical records;
- tombstone filtering that skips stale records on normal reads;
- cold scan reads that do not warm the serving cache;
- crash/restart rebuild of `PageIndex`, `BlockIndex`, and `ObjectIndexEntry`.

Lifecycle conformance reports must also include `storage_index_contract` with these
required fields:

- `page_address_codec`
- `block_address_codec`
- `stable_order`
- `slot_index`
- `object_index_entry`
- `page_index`
- `block_index`
- `required_behaviors`
- `page_address_encode_decode`
- `block_address_encode_decode`
- `stable_order_verified`
- `timestamp_range_lookup_verified`
- `slot_index_entry_count`
- `slot_object_ref_count`
- `slot_page_ref_count`
- `object_index_entry_count`
- `page_index_entry_count`
- `block_index_entry_count`
- `restart_rebuild_verified`
- `unreadable_page_refs`
- `checksum_mismatches`

Required `storage_index_contract.required_behaviors` values:

- `page_address_encode_decode`
- `page_address_stable_order`
- `timestamp_range_page_lookup`
- `slot_index_maps_slot_to_object_page_refs`
- `object_index_maps_model_table_object_key_to_page_chain`
- `page_index_maps_logical_ranges_to_page_addresses`
- `block_index_maps_page_addresses_to_durable_locations`
- `restart_rebuilds_page_block_object_indexes`

`tools/validate_page_address_compatibility_corpus.py` is the lightweight
fail-closed validator for this shared corpus. Native and Rust tests should
use the same corpus for engine-specific storage/index assertions.

## Metrics

Both and Rust should expose the same metric names:

- `page_index_lookup_count`
- `page_index_lookup_ms`
- `page_index_cache_hit_rate`
- `block_index_lookup_count`
- `block_index_lookup_ms`
- `block_index_cache_hit_rate`
- `page_reads`
- `page_writes`
- `block_reads`
- `block_writes`
- `bytes_read`
- `bytes_written`
- `compaction_reclaimed_bytes`
- `cold_scan_no_cache_reads`
- `hot_cache_promotions`
- `append_watermark`
- `compaction_watermark`

`tools/validate_page_block_metrics_conformance.py` validates that this canonical
metric set is present in the shared contract and in conformance scale report
artifacts.

## Storage Lifecycle Metrics

Stream, zone, eviction, GC, reclaim, compaction, and StorageManager reports must
also use one shared lifecycle metric vocabulary:

## Multi-Layer Cache Contract

and Rust may implement cache internals differently, but public reports must
use one layered cache vocabulary:

- `memory_object_cache`
- `page_index_cache`
- `block_index_cache`
- `disk_block_cache`
- `shared_store_read_through`

Canonical cache semantics:

- `lookup_hot_to_cold`: normal reads check hot memory first, then index caches,
  then disk/local block cache, then shared-store read-through.
- `refill_from_durable_on_miss`: a serving miss can refill from durable
  page/block data after checksum and generation validation.
- `invalidate_on_append_watermark`: append watermark changes invalidate affected
  parsed/index/cache entries.
- `invalidate_on_compaction_watermark`: compaction watermark changes invalidate
  stale page/block generations.
- `cold_scan_no_promote`: cold lifecycle scans must not promote pages into the
  hot serving cache.
- `writeback_backpressure_reported`: async cache writeback must report queue
  depth and rejection/backpressure counters.

Canonical cache metrics:

- `memory_cache_hits`
- `memory_cache_misses`
- `page_index_cache_hits`
- `page_index_cache_misses`
- `block_index_cache_hits`
- `block_index_cache_misses`
- `disk_cache_hits`
- `disk_cache_misses`
- `shared_store_read_throughs`
- `cache_refills`
- `cache_invalidations`
- `cache_writeback_queue_depth`
- `cache_writeback_rejections`

Lifecycle conformance reports must also include `storage_cache_contract` with these
required fields:

- `layers`
- `semantics`
- `metrics`
- `hot_to_cold_lookup`
- `durable_refill_on_miss`
- `append_watermark_invalidation`
- `compaction_watermark_invalidation`
- `cold_scan_no_promote`
- `writeback_backpressure_measured`
- `cache_refills`
- `cache_invalidations`
- `cache_writeback_queue_depth`
- `cache_writeback_rejections`
- `hot_cache_promotions`

Required cache contract behavior:

- `hot_to_cold_lookup` must be `true`.
- `durable_refill_on_miss` must be `true`.
- `append_watermark_invalidation` must be `true`.
- `compaction_watermark_invalidation` must be `true`.
- `cold_scan_no_promote` must be `true`.
- `writeback_backpressure_measured` must be `true`.
- `hot_cache_promotions` must be `0` for cold scan no-promote conformance.

Canonical StorageManager/StoreManager lifecycle phases:

- `prepare`
- `reclaim`
- `evict`
- `expire`
- `page_gc`
- `block_gc`
- `compaction`
- `index_gc`
- `delayed_destroy`
- `follower_cursor_safety`
- `watermark_progress`

Required StorageManager/StoreManager contract fields:

- `manager_identity`
- `native_public_name`
- `rust_public_name`
- `phase_order`
- `phase_metrics`
- `phase_counts`
- `loop_metric`
- `loop_ms`
- `phase_order_enforced`
- `missing_phase_count`

Required manager identity values:

- `manager_identity`: `StorageManager/StoreManager`
- `native_public_name`: `StorageManager`
- `rust_public_name`: `StoreManager`
- `loop_metric`: `storage_manager_loop_ms`
- `phase_order_enforced`: `true`
- `missing_phase_count`: `0`

Required phase-to-metric mapping:

- `prepare` -> `storage_manager_prepare_count`
- `reclaim` -> `storage_manager_reclaim_count`
- `evict` -> `storage_manager_evict_count`
- `expire` -> `storage_manager_expire_count`
- `page_gc` -> `storage_manager_page_gc_count`
- `block_gc` -> `storage_manager_block_gc_count`
- `compaction` -> `storage_manager_compaction_count`
- `index_gc` -> `storage_manager_index_gc_count`
- `delayed_destroy` -> `storage_manager_delayed_destroy_count`
- `follower_cursor_safety` -> `storage_manager_follower_cursor_safety_count`
- `watermark_progress` -> `storage_manager_watermark_progress_count`

- `storage_manager_prepare_count`
- `storage_manager_reclaim_count`
- `storage_manager_evict_count`
- `storage_manager_expire_count`
- `storage_manager_page_gc_count`
- `storage_manager_block_gc_count`
- `storage_manager_compaction_count`
- `storage_manager_index_gc_count`
- `storage_manager_delayed_destroy_count`
- `storage_manager_follower_cursor_safety_count`
- `storage_manager_watermark_progress_count`
- `storage_manager_loop_ms`
- `stream_rollover_count`
- `segment_open_count`
- `segment_sealed_count`
- `storage_zone_total_bytes`
- `storage_zone_used_bytes`
- `storage_zone_stale_bytes`
- `append_log_replay_records`
- `append_log_reclaimed_records`
- `slot_dirty_generation_count`
- `slot_tombstone_count`
- `slot_stale_ref_count`
- `slot_owner_mismatch_count`
- `page_index_rebuild_count`
- `block_index_rebuild_count`
- `object_index_rebuild_count`
- `cache_admissions`
- `cache_evictions`
- `cache_rehydrates`
- `memory_cache_hits`
- `memory_cache_misses`
- `page_index_cache_hits`
- `page_index_cache_misses`
- `block_index_cache_hits`
- `block_index_cache_misses`
- `disk_cache_hits`
- `disk_cache_misses`
- `shared_store_read_throughs`
- `cache_refills`
- `cache_invalidations`
- `cache_writeback_queue_depth`
- `cache_writeback_rejections`
- `cold_scan_no_cache_reads`
- `hot_cache_promotions`
- `tombstone_records`
- `stale_page_tombstones`
- `stale_block_tombstones`
- `stale_pages_rewritten`
- `stale_pages_skipped`
- `stale_blocks_rewritten`
- `stale_blocks_skipped`
- `delayed_destroy_backlog`
- `follower_cursor_retention_floor`
- `reclaimable_bytes`
- `compaction_reclaimed_bytes`
- `physical_reclaimed_bytes`
- `physical_reclaim_errors`
- `append_watermark`
- `compaction_watermark`

`tools/validate_storage_lifecycle_conformance.py` validates that this canonical
lifecycle metric set is present in the shared contract and scale report runner.
When given `--native-report` and `--rust-report`, it also verifies that both reports
carry the same public storage tuning fields and lifecycle metric names.

Canonical lifecycle reports must expose the same top-level shape for and
Rust before comparison tools accept them:

- `effective_storage_tuning`
- `public_storage_contract`
- `public_storage_feature_shapes`
- `storage_write_contract`
- `storage_read_contract`
- `storage_cold_scan_contract`
- `storage_manager_contract`
- `storage_index_contract`
- `storage_cache_contract`
- `storage_reclaim_contract`
- `storage_safety_snapshot`
- `storage_watermark_snapshot`
- `storage_gc_snapshot`
- `storage_index_snapshot`
- `storage_topology_snapshot`
- `storage_read_sequence`
- `storage_cold_scan_sequence`
- `storage_lifecycle_phases`
- `storage_lifecycle_metrics`
- `storage_cache_layers`
- `storage_cache_semantics`
- `storage_reclaim_semantics`
- `storage_write_sequence`
- `storage_reclaim_scope`

`storage_safety_snapshot` is the compact safety gate for append and compaction
watermarks, tombstone evidence, reclaimable bytes, follower-cursor blockers, and
physical reclaim errors. `storage_gc_snapshot` is the more specific GC/reclaim
view and must carry `tombstone_records`, `stale_page_tombstones`,
`stale_block_tombstones`, `gc_eligible_record_count`, `reclaimable_bytes`,
`compaction_reclaimed_bytes`, `physical_reclaimed_bytes`,
`physical_reclaim_errors`, `follower_cursor_retention_floor`,
`follower_cursor_blocked_reclaim_count`, and `follower_cursor_safe_to_reclaim`.
`storage_index_snapshot` must carry both counters and bounded examples:
`page_index_entry_samples`, `block_index_entry_samples`, and
`object_index_entry_samples`. These samples prove public `PageIndexEntry`,
`BlockIndexEntry`, and `ObjectIndexEntry` shape without dumping full indexes or
warming cold pages.
`storage_gc_snapshot` must follow the same rule: counters remain authoritative,
and bounded `tombstone_samples`, `gc_eligibility_samples`, and
`follower_cursor_safety_samples` prove public `Tombstone`, `GcEligibility`, and
`FollowerCursorSafety` shape without materializing every reclaim candidate.
`storage_watermark_snapshot` carries scalar watermarks plus bounded
`append_watermark_samples` and `compaction_watermark_samples` so live reports
prove public `AppendWatermark` and `CompactionWatermark` shape without dumping
every slot or follower cursor.
`storage_topology_snapshot` likewise carries bounded `storage_zone_samples`,
`stream_samples`, `segment_samples`, `extent_samples`, and `slot_samples` so
and Rust prove the same public `StorageZone`, `Stream`, `Segment`, `Extent`,
and `Slot` shape while keeping topology reports compact.

`tools/compare_storage_lifecycle_reports.py` is the operator-facing wrapper for
live conformance report comparison and uses the same fail-closed contract.
By default, it also validates
`compat/storage_lifecycle_report_pair_corpus.json`, a synthetic conformance report
pair that proves `page_store`, `block_store`, stream/blob, and page-segment
aliases are accepted only under `compatibility_aliases` and compared through the
canonical public shape.

Lifecycle conformance is intentionally stricter than cache eviction conformance:

- cache eviction only proves memory pressure relief;
- tombstone metadata proves logical delete eligibility;
- compaction and GC prove live-record rewrite and stale-generation exclusion;
- physical reclaim is complete only when reclaimable bytes and reclaimed bytes
  are reported with zero physical reclaim errors;
- cold scan conformance requires no-cache/no-promote reads and no hot-cache admission.

Canonical reclaim semantics:

- `cache_eviction_memory_only`: `cache_evictions` may be non-zero without any
  physical reclaim; this only proves memory was relieved.
- `logical_tombstone_required`: physical reclaim must have durable tombstone
  evidence such as `tombstone_records`, `stale_page_tombstones`, or
  `stale_block_tombstones`.
- `stale_pages_blocks_rewritten_or_skipped`: physical reclaim must prove stale
  pages/blocks were either rewritten into live generations or safely skipped via
  `stale_pages_rewritten`, `stale_pages_skipped`, `stale_blocks_rewritten`, or
  `stale_blocks_skipped`.
- `reclaimed_bytes_reported`: physical reclaim must report
  `physical_reclaimed_bytes` and `compaction_reclaimed_bytes`.
- `physical_reclaim_errors_zero`: physical reclaim is not complete unless
  `physical_reclaim_errors` is zero.

Lifecycle reports must also expose the fail-closed `storage_reclaim_contract`
block. and Rust compare this block directly after normalizing any private
implementation names into the public storage contract:

```json
{
  "storage_reclaim_contract": {
    "cache_eviction_frees_memory_only": true,
    "logical_gc_marks_expired_deletable": true,
    "physical_reclaim_requires_compaction_or_safe_skip": true,
    "cache_evictions": 2,
    "tombstone_records": 1,
    "stale_page_tombstones": 1,
    "stale_block_tombstones": 1,
    "stale_pages_rewritten": 1,
    "stale_pages_skipped": 1,
    "stale_blocks_rewritten": 1,
    "stale_blocks_skipped": 1,
    "reclaimable_bytes": 4096,
    "compaction_reclaimed_bytes": 2048,
    "physical_reclaimed_bytes": 2048,
    "physical_reclaim_errors": 0
  }
}
```

The boolean fields are required evidence, not labels. `cache_eviction_frees_memory_only`
must be true because cache eviction frees memory without proving durable reclaim.
`logical_gc_marks_expired_deletable` must be true because logical GC only marks
records as expired/deletable. `physical_reclaim_requires_compaction_or_safe_skip`
must be true because physical reclaim is complete only after stale pages/blocks
are tombstoned and then compacted, rewritten, or safely skipped. If
`physical_reclaimed_bytes` is positive, reports must also include tombstone
evidence, stale page/block rewrite-or-skip evidence, positive
`compaction_reclaimed_bytes`, and zero `physical_reclaim_errors`.

Canonical reclaim ownership:

```json
{
  "storage_reclaim_scope": {
    "owner": "temporalstore_storage_lifecycle",
    "matrixark_context_gc_role": "marks_logical_raw_event_eligibility_only",
    "physical_reclaim_context_specific": false
  }
}
```

General storage-level page/block reclaim is not MatrixArk-context-specific.
MatrixArk context GC may mark raw context events as logically eligible for
eviction or compression, but TemporalStore's storage lifecycle owns physical
page/block tombstone handling, compaction, delayed destroy, and reclaimed-byte
accounting for every data model.

Shared proof requirements:

- tombstones survive compaction and remain available to debug/replay policy;
- stale page/block generations are ignored by normal reads after compaction;
- cold scans use no-cache/no-promote reads and do not warm the serving cache;
- crash/restart rebuilds `PageIndex`, `BlockIndex`, and `ObjectIndexEntry`;
- physical reclaim is only complete when stale pages/blocks are tombstoned,
  rewritten or skipped safely, and reclaimed bytes are reported.

## Nine-Phase Conformance Gate

`tools/validate_storage_engine_9_phase_conformance.py` is the umbrella gate for the
storage-engine conformance loop. It runs the focused validators in the same phase
order used by the conformance conformance plan:

1. canonical public contract;
2. read/write/cold-scan sequences;
3. `StorageManager`/`StoreManager` lifecycle;
4. page/block/slot/index behavior;
5. multi-layer cache behavior;
6. eviction, GC, compaction, and reclaim;
7. public config conformance;
8. metrics/report conformance;
9. shared storage/proxy/Raft evidence.

The gate should be used after every storage lifecycle change:

```bash
python tools/validate_storage_engine_9_phase_conformance.py
```

To repeat the full phase sequence for soak-style proof:

```bash
python tools/validate_storage_engine_9_phase_conformance.py --loops 9
```

The umbrella gate does not replace native or Rust tests. It composes the
shared validators so CI and local development fail at a named phase boundary
instead of relying on a manual checklist.

## Acceptance

This contract is satisfied when and Rust:

- emit the same public address/index field names;
- can convert private storage metadata into the canonical report shape;
- pass the shared page/block/index conformance cases;
- expose the same storage lifecycle metrics;
- reject public report changes that reintroduce backend-specific naming drift;
- encode the same logical `PageAddress`;
- rebuild `PageIndex`, `BlockIndex`, and `ObjectIndexEntry` after restart;
- preserve tombstone metadata through compaction and ignore stale generations
  during normal reads;
- expose the same page/block config;
- produce equivalent page/block index summaries from the same corpus;
- report the same multi-layer cache layers, lookup/refill/invalidation
  semantics, and cache/writeback counters;
- measure cold scans, cache admission, eviction, compaction, GC, and physical
  reclaim identically.
