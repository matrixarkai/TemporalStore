# TemporalStore Runtime Tuning

TemporalStore smoke, scale, and SSD blockcache tests read server runtime knobs from
environment variables. This keeps the binaries unchanged while letting local smoke,
AWS scale tests, and cache experiments use different sizes.

The shared defaults live in `tools/temporalstore_runtime_env.sh`.

## Storage Stream Sizing

| Environment variable | Default in smoke | Default in 3-node scale | Meaning |
| --- | ---: | ---: | --- |
| `TEMPORALSTORE_STORAGE_EXTENT_SIZE` | `10485760` | `268435456` | Extent size for storage streams. Larger values reduce extent/blob switching under high write QPS. |
| `TEMPORALSTORE_STREAM_MAX_BLOB_SIZE` | `10485760` | `268435456` | Maximum stream blob size. Larger values reduce frequent blob freeze/open overhead. |
| `TEMPORALSTORE_STORAGE_ASYNC` | `false` | `false` | Whether storage writes use async mode. |
| `TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH` | `0` | `0` | WAL bytes to buffer before dump/replay visibility. Use carefully because it directly affects secondary lag. |

For AWS scale runs, start with 256 MB:

```bash
TEMPORALSTORE_STORAGE_EXTENT_SIZE=$((256 * 1024 * 1024)) \
TEMPORALSTORE_STREAM_MAX_BLOB_SIZE=$((256 * 1024 * 1024)) \
bash tools/run_shared_file_3node_scale_ubuntu22.sh
```

For heavier ingestion, test 512 MB or 1 GB before increasing batch delays:

```bash
TEMPORALSTORE_STORAGE_EXTENT_SIZE=$((512 * 1024 * 1024)) \
TEMPORALSTORE_STREAM_MAX_BLOB_SIZE=$((512 * 1024 * 1024)) \
bash tools/run_shared_file_3node_scale_ubuntu22.sh
```

## Secondary Replay Tuning

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `TEMPORALSTORE_REPLICATOR_OUT_OF_SYNC_S` | `10` smoke, `120` scale | Maximum tolerated replay lag before reads are marked out of sync. |
| `TEMPORALSTORE_REPLICATOR_LOOP_INTERVAL_US` | `1000` | Sleep between replay loops. Lower values reduce lag but spend more CPU. |
| `TEMPORALSTORE_REPLICATOR_MAX_OPLOG_PER_LOOP` | `20000` | WAL records replayed per loop. |
| `TEMPORALSTORE_REPLICATOR_MAX_INDEXLOG_PER_LOOP` | `20000` | Index-log records replayed per loop. |
| `TEMPORALSTORE_REPLICATOR_UPDATE_REMOTE_INTERVAL_MS` | `20` | Remote metadata refresh interval. Lower values improve freshness but add overhead. |

Low-lag secondary testing:

```bash
TEMPORALSTORE_REPLICATOR_LOOP_INTERVAL_US=500 \
TEMPORALSTORE_REPLICATOR_UPDATE_REMOTE_INTERVAL_MS=5 \
TEMPORALSTORE_REPLICATOR_MAX_OPLOG_PER_LOOP=50000 \
TEMPORALSTORE_REPLICATOR_MAX_INDEXLOG_PER_LOOP=50000 \
bash tools/run_shared_file_3node_scale_ubuntu22.sh
```

## SSD Blockcache Tuning

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `TEMPORALSTORE_ENABLE_BLOCKCACHE` | `true` | Enables blockcache. |
| `TEMPORALSTORE_BLOCKCACHE_DRAM_CAPACITY` | `8388608` | DRAM blockcache capacity in bytes. |
| `TEMPORALSTORE_BLOCKCACHE_SSD_CAPACITY` | `67108864` | SSD blockcache capacity in bytes. |
| `TEMPORALSTORE_BLOCKCACHE_SSD_PATH` | `/tmp/temporalstore-server-ssd-cache` | SSD cache directory. On AWS this can point to an EBS/NVMe mount. |
| `TEMPORALSTORE_BLOCKCACHE_CLEAR_SSD_FOLDER` | `false` | Whether to clear existing SSD cache files on startup. |

Example with 64 MB DRAM and 2 GB SSD cache:

```bash
TEMPORALSTORE_BLOCKCACHE_DRAM_CAPACITY=$((64 * 1024 * 1024)) \
TEMPORALSTORE_BLOCKCACHE_SSD_CAPACITY=$((2 * 1024 * 1024 * 1024)) \
TEMPORALSTORE_BLOCKCACHE_SSD_PATH=/mnt/ssd-cache/temporalstore \
bash tools/run_ssd_blockcache_smoke_ubuntu22.sh
```

## Routing Slot Range (a large lever on resident memory)

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `TS_SHARD_START_ROUTING_BUCKET` | `0` | First routing bucket this shard owns. `TS_SHARD_START_ROUTING_SLOT` is the previous name and is still read. |
| `TS_SHARD_END_ROUTING_BUCKET` | `1023` | Last routing bucket this shard owns. `TS_SHARD_END_ROUTING_SLOT` is the previous name and is still read. |

**The default is `1023` — 1,024 buckets — and it was `4294967295` up to this
release.** The whole `u32` range is 4.29 billion buckets, so every key landed alone
in a bucket of its own by construction and this page then told an operator to set
1,024 before the first ingest: the shipped default was not the configuration the
documentation said to run. The new value was chosen by sweeping 255 / 1,023 / 4,095
/ 65,535 at two corpus sizes rather than copied from the example below; the table
under *What the sweep says* is that measurement.

**An existing store keeps the range it was built on.** A store records its routing
range beside its index (`shard-<id>.routing-range.json`), and a load honours that
file rather than the default — see *Changing the range on a populated store* below.
So this default reaches new stores only, and no upgrade re-ranges anything.

### What the old default cost, and how this was first found

A routing bucket is derived by hashing the key, so on the whole `u32` range every key
landed in a bucket of its own and each one materialized a `BucketNode` carrying its
own page index and object sets. All of that per-bucket machinery was then paid **per
record**. Counted in-process at 20,000 records on the old default, `bucket_map` held
20,000 buckets — one per record.

Sharing buckets was first measured as whole-process resident memory on 40,000
records, a 4-CPU node, sampled after the writes drained. Kept because it is the only
figure here taken at the process level rather than on the bucket map alone:

| `TS_SHARD_END_ROUTING_BUCKET` | buckets | resident / record (256 B values) | resident / record (1.2 KB values) | disk / record |
| --- | ---: | ---: | ---: | ---: |
| `4294967295` (the old default) | 4294967295 | 5552 B | 5843 B | unchanged |
| `1023` (the default now) | 1024 | **3071 B** | **3195 B** | unchanged |
| `255` | 256 | 3049 B | — | unchanged |

**About 45% less resident memory at no cost on disk**, and it plateaus by 1,024
buckets: 255 buys 22 B a record more. For a store of 4 million records that is roughly
24 GB against 13 GB. The plateau is the same conclusion the bucket-map sweep below
reaches on its own instrument, and the sweep is the one to read for a choice between
candidates — it separates the two byte columns, the allocation count, and the costs
this whole-process figure cannot see.

### What the sweep says

Measured on the bucket map with a counting allocator, routed string keys, store path
held at 15 characters, every distribution taken as a histogram with percentiles and a
MAX rather than as a mean. **Both allocator columns are reported**: `ALLOC_BYTES`
charges what the caller asked for and `ALLOC_CHUNK_BYTES` charges what the allocator
actually set aside, and a container change moves the rounding between them.

40,000 routed records:

| end | buckets | pages a bucket (p50 / MAX) | request B / record | chunk B / record | allocations / record | dump + release unit | read path, entries / lookup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `255` | 256 | 156 / 168 | 113.8 (−58.9%) | **114.1 (−59.2%)** | 0.0201 (−86.1%) | 168 pages | 6.42 |
| `1023` | 1,024 | 39 / 50 | 119.3 (−56.9%) | **120.2 (−57.0%)** | 0.0803 (−44.5%) | 50 pages | 4.54 |
| `4095` | 4,096 | 10 / 21 | 140.7 (−49.2%) | 144.1 (−48.4%) | 0.3207 (+121.9%) | 21 pages | 2.91 |
| `65535` | 28,120 occupied of 65,536 | 1 / 6 | 243.0 (−12.3%) | 252.7 (−9.5%) | 0.7649 (+429.1%) | 6 pages | 0.83 |
| `4294967295` | 40,000 | 1 / 1 | 277.0 | 279.3 | 0.1446 | 1 page | 0.00 |

4,000 routed records, where the same bucket counts produce a tenth of the fill:

| end | buckets | pages a bucket (p50 / MAX) | request B / record | chunk B / record | allocations / record |
| ---: | ---: | ---: | ---: | ---: | ---: |
| `255` | 256 | 16 / 21 | 130.3 (−53.2%) | 132.6 (−52.8%) | 0.2008 (+38.2%) |
| `1023` | 1,024 | 4 / 8 | 183.4 (−34.1%) | 191.9 (−31.6%) | 0.7625 (+425.0%) |
| `4095` | 2,754 occupied of 4,096 | 1 / 4 | 245.4 (−11.8%) | 256.2 (−8.7%) | 0.8480 (+483.8%) |
| `65535` | 3,752 occupied of 65,536 | 1 / 2 | 253.4 (−9.0%) | 257.9 (−8.1%) | 0.3103 (+113.6%) |
| `4294967295` | 4,000 | 1 / 1 | 278.3 | 280.7 | 0.1452 |

**Why `1023` and not `255`, which is better on every byte column.** The byte saving
*saturates* — `1023` is within 5% of the floor `255` reaches — while the dump and
release unit grows *linearly in the corpus and without bound*, because the fill is
`records / bucket-count`. At 40,000 records `255` costs a 168-page dump-and-release
unit against `1023`'s 50; at 400,000 it would be 1,680 against 500. So the right
default is the widest range that still reaches the amortisation floor.

**And no fixed bucket count is right for every corpus.** The fill a range produces
moves with the record count, which the engine does not know when it loads a shard:
`1023` sits at 3.91 pages a bucket for a 4,000-record store and 39.06 for a
40,000-record one. A store much smaller than 40,000 records is better served by a
narrower range and one much larger by a wider one, and the figures above are what to
choose from.

**A prior version of this table had the sign wrong.** It reported 413.1 B a page at
4,000 records on `1023` — a *loss* of 25.4% — against the 183.4 measured now. The
`Many` arm of the page index was a `BTreeMap` when that figure was taken and is a
flat sorted `Vec` now, which changed the sign of the byte column while leaving the
allocation column almost exactly as it was (+425% then, +425.0% now). Nothing
failed, because the guard behind the figure deliberately asserts the *mechanism*
and not the sign.

**And a routing bucket is the unit of more than memory.** It is the unit of
eviction victim selection, of cache invalidation, of the dump's bucket budget, of
the dirty set drained when a dump manifest becomes durable, and of the write-ahead
and index log reclaim floor. All of those coarsen by exactly the number of keys a
bucket comes to hold: a dump of one bucket drains one key at the default range and
eight at 4,000 records on `1023`, one hot key holds the log floor for every key
sharing its bucket, and one evicted bucket takes every key in it. Reads are
unaffected -- they resolve through the model maps, not the bucket index.

`0..1023` is now the default, so a new store needs no flags at all. Set them only to
choose something *other* than the default — a narrower range for a store that will
stay small, or a wider one for a store much larger than 40,000 records:

```bash
TS_SHARD_START_ROUTING_BUCKET=0 \
TS_SHARD_END_ROUTING_BUCKET=4095 \
matrixark_rust_datanode
```

### Changing the range on a populated store

**A load refuses when the range disagrees with the range the store was built on.**
A page's bucket is written onto its address when the page is appended, and a reopened
range is consulted only for an address that carries no bucket of its own — so
changing the range on a populated store re-files nothing, and every page stays where
the old range put it. The engine therefore records the range a store was built under
in `shard-<id>.routing-range.json` beside the base index, reads it *before* decoding
anything, and refuses the load with `routing_range_mismatch` when the two disagree,
naming both ranges and the file. Re-ingest into a new store to adopt a different
range.

A store written before the engine recorded the range carries no such file. It can
only have been built on the whole `u32` keyspace, which was the only default, so it
is **honoured on that range** — the requested range is overridden, the inference is
logged at `warn`, and the file is written so the next load does not have to make it
again. An upgrade therefore changes nothing about an existing store.

This is what the refusal prevents, driven both ways over 2,000 records before it
existed:

* **Widening** (`1023` then the default) is safe. Every record readable, every page
  present, nothing outside the new range — because the wide range contains the
  narrow one, not because anything moved. The store stayed on its 938 buckets.
* **Narrowing** (the default then `1023`) leaves the store on its original 2,000
  buckets, and **all 2,000 of them sit above the shard's own end**. Every record is
  still readable, which is what makes this quiet: the pages are simply outside every
  per-bucket sweep the shard runs — the dump's bucket selection, eviction's victim
  sampling, the reclaim floor and the release pass all enumerate the bucket map
  against the shard's own range.

On a fresh store it is safe: sampled reads returned no missing and no mismatched
values after the writes, after a dump, and after a restart that recovered from the
on-disk artifacts.

Both arms are now unreachable through a normal load: the narrowing one is refused,
and the widening one is refused too. Widening leaves every page *inside* the new
range, so it loses nothing — but it routes every subsequent write to a bucket a
re-read of the same key would not compute, so it is not a safe operation either and
is refused for the same reason.

The range also bounds how finely slots can be divided between shards, so keep it
comfortably above the shard count you expect to grow into.

## Keyword Index Coverage (the largest lever on resident memory)

Resident memory tracks the **number of records**, and for resource/skill ingest
most records are index postings, not content. Counted over four ~1.3 MB CN/EN
md+json documents at 1000-token chunks with posting lists on:

| record type | per document | per chunk | share of records |
| --- | ---: | ---: | ---: |
| `context_index` | 3571 | 7.66 | **75.8%** |
| `resource_chunk` | 466 | 1.00 | 9.9% |
| `context_embedding` | 466 | 1.00 | 9.9% |
| `skill_section` | 206 | 0.44 | 4.4% |

So how many keywords each chunk indexes decides the memory bill — more than the
chunk size, and more than whether per-chunk vectors are stored. Four caps apply
together and the smallest wins, so raise or lower them as a set:

| Environment variable | Default |
| --- | ---: |
| `MATRIXARK_INDEX_KEYWORD_LIMIT` | `12` |
| `MATRIXARK_MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK` | `6` |
| `MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_CHUNK` | `10` |
| `MATRIXARK_MAX_SECONDARY_INDEX_TERMS_PER_RECORD` | `10` |

Measured over 200 sampled chunks, with resident memory projected at the measured
~2.7 KB per record for a 10,000-document corpus:

| cap | records / doc | terms indexed | 10k-doc records | projected resident |
| ---: | ---: | ---: | ---: | ---: |
| `12` | 1555 | 9.4% | 15.6M | 39 GB |
| `25` | 1768 | 19.4% | 17.7M | 44 GB |
| `50` | 2412 | 38.1% | 24.1M | 61 GB |
| `100` | 4640 | 75.4% | 46.4M | 117 GB |
| `200` | 4708 | 100.0% | 47.1M | 118 GB |
| `400` | 4708 | 100.0% | 47.1M | 118 GB |

Coverage is **position-dependent**, and that decides the choice. Keywords are taken
in document order, so a lower cap keeps the start of a chunk and drops the end.
Planting a distinctive phrase at four depths in each of 121 chunks and asking
whether it appears in an emitted index record:

| cap | 10% in | 50% in | 90% in | 97% in |
| ---: | ---: | ---: | ---: | ---: |
| `12` | 17% | 6% | 0% | 0% |
| `25` | 30% | 12% | 9% | 3% |
| `50` | 98% | 12% | 12% | 12% |
| `100` | 100% | 99% | 64% | 36% |
| `200` | 100% | 100% | 100% | 100% |

**`100` is the worst choice on this curve.** It costs 117 GB against 118 GB for
`200` — within a percent — and finds only 36% of phrases near the end of a chunk
where `200` finds all of them. Either pay for `200` and index everything, or drop
to `50` or the `12` default and accept that only the opening of each chunk is
searchable. There is no useful middle.

That matters most exactly where large chunks are used: the encoder's 128-token
window embeds only the first fraction of a 1000-token chunk, so the keyword index
is the *only* way to reach the rest of it.

### Pair the cap with the chunk size

The cap has to exceed the number of distinct terms in a chunk, and that number
grows with the chunk. The same measurement across chunk sizes:

| chunk tokens | cap | 10% in | 50% in | 90% in | 97% in | index terms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 240 | `50` | 100% | 91% | 45% | 33% | 2738 |
| 240 | `100` | 100% | 100% | 100% | 100% | 3761 |
| 240 | `200` | 100% | 100% | 100% | 100% | 3761 |
| 500 | `100` | 100% | 100% | 98% | 82% | 1784 |
| 500 | `200` | 100% | 100% | 100% | 100% | 1855 |
| 1000 | `100` | 100% | 99% | 64% | 36% | 972 |
| 1000 | `200` | 100% | 100% | 100% | 100% | 1193 |

A 240-token chunk is fully covered by `100` — raising it to `200` changes nothing
because the chunk has no more terms to index. A 1000-token chunk needs `200`.

**Larger chunks are the cheaper way to buy full reach.** Over the same source
text, 1000-token chunks at `200` index 1193 terms where 240-token chunks at `100`
index 3761 — about a third of the records for identical reach at every depth.
With posting lists on, index records track distinct terms, so that ratio is the
memory ratio. Prefer big chunks with a high cap over small chunks with a low one.

```bash
# Index everything: every phrase reachable, ~118 GB resident for 10k documents.
MATRIXARK_INDEX_KEYWORD_LIMIT=200 MATRIXARK_MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK=200 MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_CHUNK=200 MATRIXARK_MAX_SECONDARY_INDEX_TERMS_PER_RECORD=200 MATRIXARK_INDEX_POSTING_LISTS=1 matrixark_rust_datanode
```

To halve the memory instead, use `50` across all four — accepting that only the
opening of each chunk is searchable. Do not stop at `100`: it pays `200`'s memory
for a third of its reach.

Keep `MATRIXARK_INDEX_POSTING_LISTS=1` whenever coverage is raised: it stores one
record per term carrying its posting list instead of one per (term, chunk), which
is what makes any coverage above the default affordable.

### Sizing a corpus

A ~1.3 MB document at 1000-token chunks is about **4,700 records**, not a few
hundred — roughly 466 chunks, each becoming ~10 records. A 10,000-document corpus
is therefore ~47M records at full coverage, and resident memory, not ingest time,
is what limits it to a single node. Measured with four parallel client processes,
that corpus ingests in about 3.6 hours; it does not fit in one node's memory at
cap 100 or above.

## Why This Matters

The earlier 10 MB/256 MB constants changed performance behavior:

- Too-small stream blobs cause frequent blob switching/freezing/opening during high-QPS writes.
- Too-large blobs can delay persistence and make recovery streams heavier.
- Replicator loop and batch sizes trade CPU for secondary freshness.
- Blockcache capacity should match the actual memory and SSD budget of the node.
- The routing slot range decides how many records share a slot, and the per-slot
  structures are much of what resident memory is made of.
- The keyword index caps decide how many records exist at all: for resource and
  skill ingest, index postings are ~76% of them, which makes coverage the largest
  single lever on resident memory.

Use environment variables per run, then record the exact values with the benchmark result folder.
