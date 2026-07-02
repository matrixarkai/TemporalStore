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
-> page index lookup
-> page address list
-> block index lookup
-> page read
-> decode records
-> apply tombstone/generation filters
```

Required behavior:

- Point reads may use `ObjectIndex` to resolve the current logical object chain,
  then must use `PageIndex -> BlockIndex` for durable page lookup.
- Timestamp range reads must use `PageIndex` range lookup before reading blocks.
- `PageIndex` returns ordered `PageAddress` values for the logical key or
  timestamp range; `BlockIndex` resolves each page's durable physical location
  before any record bytes are decoded.
- Reads must reject stale generations and tombstoned records unless an explicit
  replay/debug policy asks for retained historical data.
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

Required behavior:

- Cold lifecycle scans must start from a timestamp range and use `PageIndex`
  range iteration rather than warming a serving object cache.
- Page reads must be marked no-cache/no-promote by default when
  `TS_COLD_SCAN_NO_CACHE_FILL=true`.
- Decode work must be bounded by batch size, byte budget, and deadline so
  compression, backfill, audit, or GC workers cannot starve serving retrieval.
- Cold scan reads may write warm summaries, tombstones, or compaction metadata,
  but raw source pages must not enter hot LRU/admission unless an explicit
  replay/query path reinforces them.

## Public Config Parity

C++ and Rust must expose the same public storage tuning knobs. The names below
are the public contract; each backend may map them into private gflags, typed
configs, or environment readers internally.

| knob | meaning | default |
|---|---|---:|
| `TS_CONTEXT_PAGE_TARGET_BYTES` | Target packed context timestamp page size. | `65536` |
| `TS_BLOCK_SEGMENT_TARGET_BYTES` | Target durable block/segment size before rolling. | `1073741824` |
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
    "TS_BLOCK_SEGMENT_TARGET_BYTES": 1073741824,
    "TS_STORAGE_ZONE_SIZE": 10485760,
    "TS_STREAM_MAX_BLOB_SIZE": 10485760,
    "TS_COMPACTION_WATERMARK_BYTES": 268435456,
    "TS_COLD_SCAN_NO_CACHE_FILL": true,
    "TS_PAGE_INDEX_CACHE_BYTES": 67108864,
    "TS_BLOCK_INDEX_CACHE_BYTES": 67108864
  }
}
```

Parity rules:

- C++ launchers must map `TS_STORAGE_ZONE_SIZE` to `--storage_zone_size` and
  `TS_STREAM_MAX_BLOB_SIZE` to `--stream_max_blob_size` until native gflags use
  the public names directly.
- Rust must read all eight names through its typed storage tuning config.
- Benchmark and scale reports must include `effective_storage_tuning` at the
  top-level config and per backend.
- A parity gate should fail if one backend reports a missing knob or a different
  effective value under the same run config.

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

## Migration Strategy

### Phase 1: Shared Schema And Aliases

Phase 1 is documentation and compatibility mapping. C++ and Rust must both
publish the shared schema in docs and tests before changing production report
payloads.

Required Phase 1 outputs:

- canonical schema for `PageAddress`, `BlockAddress`, `PageIndexEntry`,
  `BlockIndexEntry`, `ObjectIndex`, `TombstoneGcMetadata`, watermarks, and
  page/block metrics;
- explicit alias map for old names such as `page_store`, `block_store`,
  `page_segment_id`, `zone_id`, `extent_id`, and `stream_blob`;
- fail-closed validators for the shared page-address corpus, public storage
  knobs, and page/block metric names;
- report examples showing both the canonical field and any compatibility alias.

Phase 1 rules:

- Aliases may be read by tools and dashboards, but parity gates must validate
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

- C++ and Rust report parsers accept old and new field names.
- Existing old report fields remain readable through adapters or are emitted
  under `compatibility_aliases` for one compatibility window.
- Report parsers normalize old and new field names into the same in-memory
  shape before comparison.
- CI verifies that old reports still parse, new reports can include canonical
  names, and mixed C++/Rust report pairs compare on canonical fields only.
- Deprecation warnings identify alias usage, but do not fail old-report parsing
  until the compatibility window ends.

### Phase 3: Rust Public Struct Names

Phase 3 updates Rust public structs, report DTOs, metrics payloads, and
documentation to match the C++/shared public names.

Required Phase 3 behavior:

- Rust public output uses `PageAddress`, `BlockAddress`, `PageIndexEntry`,
  `BlockIndexEntry`, `StorageZone`, `Segment`, `Extent`, `AppendWatermark`,
  and `CompactionWatermark`.
- Rust private implementation names may remain internally, but conversion to
  public DTOs must happen before data leaves the backend.
- Rust compatibility deserializers continue reading old report fields through
  `compatibility_aliases`.

### Phase 4: C++ Report Shape Parity

Phase 4 updates C++ public reports to emit the same canonical shape as Rust.

Required Phase 4 behavior:

- C++ reports include the same canonical fields, nesting, metric names, and
  effective config fields as Rust.
- C++ compatibility aliases remain available for old dashboards and benchmark
  artifacts during the compatibility window.
- C++/Rust comparison reports normalize both sides and show alias usage as a
  warning, not as a separate metric family.

### Phase 5: Shared Tests

Phase 5 makes the shared tests the source of truth for both engines.

Required Phase 5 coverage:

- shared page-address compatibility corpus;
- shared page/block metrics parity validator;
- old-report compatibility fixtures;
- C++ and Rust native tests for page split, compaction rewrite, tombstones,
  no-promote cold scans, crash/restart index rebuild, and watermark behavior;
- comparison tests that prove canonicalized C++ and Rust reports are equivalent.

### Phase 6: Drift Gates

Phase 6 turns compatibility into enforcement.

Required Phase 6 gates:

- fail if public fields drift between C++ and Rust;
- fail if effective storage config fields drift or are missing;
- fail if page/block metric names drift or are missing;
- fail if canonical fields are absent from new reports;
- fail if alias fields appear outside `compatibility_aliases` after the
  compatibility window;
- fail if broad legacy report paths are used in production-performance parity
  claims.

Removal gate:

- remove or hide alias output only after dashboards, benchmark reports, portal
  pages, C++ tests, Rust tests, replay/audit tooling, and parity gates all read
  the canonical schema directly.

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

The shared `compat/page_address_compatibility_corpus.json` corpus covers the
PageAddress subset that both C++ and Rust must consume:

- encode/decode `PageAddress`;
- stable ordering by `{shard_id, zone_id, segment_id, page_id, offset}`;
- timestamp range -> page address lookup;
- page split behavior;
- page compaction rewrite preserving logical records;
- tombstone filtering that skips stale records on normal reads;
- cold scan reads that do not warm the serving cache;
- crash/restart rebuild of `PageIndex` and `BlockIndex`.

`tools/validate_page_address_compatibility_corpus.py` is the lightweight
fail-closed validator for this shared corpus. Native C++ and Rust tests should
use the same corpus for engine-specific storage/index assertions.

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

`tools/validate_page_block_metrics_parity.py` validates that this canonical
metric set is present in the shared contract and in C++/Rust scale report
artifacts.

## Acceptance

This contract is satisfied when C++ and Rust:

- emit the same public address/index field names;
- can convert private storage metadata into the canonical report shape;
- pass the shared page/block/index parity cases;
- expose the same storage lifecycle metrics;
- reject public report changes that reintroduce backend-specific naming drift;
- encode the same logical `PageAddress`;
- rebuild `PageIndex` and `BlockIndex` after restart;
- expose the same page/block config;
- produce equivalent page/block index summaries from the same corpus;
- measure cold scans, cache admission, compaction, and GC identically.
