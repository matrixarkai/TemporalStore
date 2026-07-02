# TemporalStore Page/Block Address Contract

This is the shared storage-address contract for C++ and Rust TemporalStore. It defines
the public report/API vocabulary for page addresses, block addresses, logical indexes,
and GC metadata. C++ and Rust may keep different private implementation details, but
public reports, parity tests, metrics, and compatibility docs should use these names.

## Goals

- Give C++ and Rust one vocabulary for page/block storage.
- Make page/block index reports comparable across both engines.
- Prevent public naming drift such as `page_store` in one backend and `block_store`
  in another when the concept is the same serving/durable page layer.
- Provide a stable target for shared parity tests, recovery tests, compaction tests,
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

### ObjectIndex

`ObjectIndex` maps `{model/table/object_key}` to the current page chain or segment
list.

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

- The hot read path should use `ObjectIndex -> PageIndex -> PageAddress`.
- Recovery should rebuild or validate `ObjectIndex` from page/block manifests and
  append logs.
- C++ and Rust should report the same object-index shape even if their internal
  slot map or shard map implementation differs.

### Tombstone/GC Metadata

`TombstoneGcMetadata` records logical delete and physical reclaim eligibility.

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
- no live `ObjectIndex` or `PageIndex` points to the page address;
- no snapshot/replay/audit retention needs the raw record;
- no Raft/shared-store follower cursor still needs the page/block;
- compaction generation is newer than the stale page generation.

## Read/Write Behavior Parity

C++ and Rust must expose the same logical read/write behavior even if their
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

Required behavior per step:

| step | required parity behavior |
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
  "page_address": {},
  "block_address": {},
  "append_watermark": 0,
  "durability": "async|sync|raft",
  "index_generation": 0
}
```

Parity rules:

- The write result must contain canonical `PageAddress`, `BlockAddress`, and
  `append_watermark` fields in public reports.
- C++ and Rust may acknowledge at different internal points only when
  `storage_options.write_mode` differs; same write mode must have the same
  durability contract.
- A write must not publish an append watermark before the corresponding
  `PageIndex` and `BlockIndex` state is recoverable.
- Failed writes must not leave public indexes pointing to unflushed or
  checksum-invalid block data.
- Batch append must preserve per-record logical ordering within the same
  shard/object/timestamp range and publish a batch watermark.

### Read Path

Canonical read flow:

```text
logical key/timestamp range
-> object/page index lookup
-> page address list
-> block index lookup
-> page/block read
-> decode records
-> apply tombstone/generation filters
```

Required behavior:

- Point reads use `ObjectIndex -> PageIndex -> BlockIndex`.
- Timestamp range reads use `PageIndex` range lookup before reading blocks.
- Reads must reject stale generations and tombstoned records unless an explicit
  replay/debug policy asks for retained historical data.
- Cold scans must use no-cache/no-promote reads by default.

## Normalize Naming

Public APIs, reports, metrics, docs, parity tests, and externally visible JSON
must use the canonical names below. Private implementation names may still exist
inside C++ or Rust, but they must be translated before data leaves the backend.

Canonical names:

- `PageAddress`
- `BlockAddress`
- `PageIndexEntry`
- `BlockIndexEntry`
- `ObjectIndex`
- `TombstoneGcMetadata`
- `StorageZone`
- `Segment`
- `Extent`
- `AppendWatermark`
- `CompactionWatermark`

Canonical field names:

- `page_address`
- `block_address`
- `page_index_entry`
- `block_index_entry`
- `object_index`
- `storage_zone`
- `segment`
- `extent`
- `append_watermark`
- `compaction_watermark`

Avoid drifting pairs in public output:

| drifting pair | canonical public name | note |
|---|---|---|
| `page_store` vs `block_store` | `StorageZone`, `PageIndexEntry`, `BlockIndexEntry` | Use implementation-specific store names only in private logs or migration notes. |
| `zone` vs `extent` | `StorageZone` for placement, `Extent` for contiguous physical byte ranges | Do not use one backend's private name as the public contract. |
| `stream blob` vs `page segment` | `Segment` | A segment may map to a local file, shared blob, or stream blob. |
| `ShardStats.page_store` vs `ShardStats.block_store` | `ShardStats.storage_zone`, `ShardStats.page_index`, `ShardStats.block_index` | Shard stats must be comparable between C++ and Rust. |

Compatibility aliases are allowed only when all three conditions are true:

1. The canonical field is present in the same report.
2. The alias is clearly marked as `legacy_alias` or appears only in a migration
   section.
3. The parity gate ignores the alias and validates only the canonical field.

New public report fields must not introduce backend-specific names such as:

- `page_store` for C++ but `block_store` for Rust;
- `page_segment_id` when the report means canonical `segment_id`;
- `extent_id` when the report means canonical `block_id`, `segment_id`, or
  `extent` without stating the mapping.

## C++/Rust Mapping

| Canonical term | C++ current/private source | Rust current/private source | Public output |
|---|---|---|---|
| `PageAddress` | `partition::PageIndex.address` plus page metadata | page-envelope address metadata | `PageAddress` |
| `BlockAddress` | page-store stream/blob location | `BlockAddress` in block/page segment layer | `BlockAddress` |
| `PageIndexEntry` | partition index slot pages | core/shard index page refs | `PageIndexEntry` |
| `BlockIndexEntry` | page-store zone/stream manifest | block-store extent/segment manifest | `BlockIndexEntry` |
| `ObjectIndex` | model/object slot layout | shard model maps/core index | `ObjectIndex` |
| `TombstoneGcMetadata` | deleted/dirty page and delayed destroy state | tombstone/GC report state | `TombstoneGcMetadata` |

## Required Parity Tests

Shared C++/Rust parity cases must cover:

- encode/decode `PageAddress`;
- encode/decode `BlockAddress`;
- stable address ordering;
- timestamp range lookup through `PageIndex`;
- object lookup through `ObjectIndex`;
- page split and page-chain update;
- compaction rewrite preserving logical records;
- stale generation rejection;
- tombstone creation before physical reclaim;
- restart/recovery rebuilding `PageIndex`, `BlockIndex`, and `ObjectIndex`;
- cold scan using no-cache/no-promote reads.

## Metrics

Both C++ and Rust should expose the same metric names:

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

## Acceptance

This contract is satisfied when C++ and Rust:

- emit the same public address/index field names;
- can convert private storage metadata into the canonical report shape;
- pass the shared page/block/index parity cases;
- expose the same storage lifecycle metrics;
- reject public report changes that reintroduce backend-specific naming drift.
