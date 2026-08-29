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
| `TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH` | `0` | `0` | Oplog bytes to buffer before dump/replay visibility. Use carefully because it directly affects secondary lag. |

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
| `TEMPORALSTORE_REPLICATOR_MAX_OPLOG_PER_LOOP` | `20000` | Oplog records replayed per loop. |
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
| `TS_SHARD_START_ROUTING_SLOT` | `0` | First routing slot this shard owns. |
| `TS_SHARD_END_ROUTING_SLOT` | `4294967295` | Last routing slot this shard owns. |

A routing slot is derived by hashing the key, so with the full `u32` range every
key lands in a slot of its own and each one materializes a `BucketNode` carrying
its own page index and object sets. All of that per-slot machinery is then paid
**per record**. Counted in-process at 20,000 records, `bucket_map` held 20,000
slots — one per record.

Narrowing the range makes records share slots. Measured on 40,000 records, a
4-CPU node, resident memory sampled after the writes drained:

| `TS_SHARD_END_ROUTING_SLOT` | slots | resident / record (256 B values) | resident / record (1.2 KB values) | disk / record |
| --- | ---: | ---: | ---: | ---: |
| default | 4294967295 | 5552 B | 5843 B | unchanged |
| `1023` | 1024 | **3071 B** | **3195 B** | unchanged |
| `255` | 256 | 3049 B | — | unchanged |

**About 45% less resident memory at no cost on disk**, and the benefit plateaus by
1024 slots, so there is little reason to go narrower. For a store of 4 million
records that is roughly 24 GB against 13 GB.

```bash
TS_SHARD_START_ROUTING_SLOT=0 \
TS_SHARD_END_ROUTING_SLOT=1023 \
matrixark_rust_datanode
```

**Set this before the first ingest.** Slot ids are durable — a bucket dump
manifest records the `slot_ids` it covers — so changing the range on a populated
store remaps keys to different slots. On a fresh store it is safe: sampled reads
returned no missing and no mismatched values after the writes, after a dump, and
after a restart that recovered from the on-disk artifacts.

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

| cap | records / doc | terms indexed | tail term reachable | 10k-doc records | projected resident |
| ---: | ---: | ---: | ---: | ---: | ---: |
| `12` | 1555 | 9.4% | 59% | 15.6M | 39 GB |
| `25` | 1768 | 19.4% | 76% | 17.7M | 44 GB |
| `50` | 2412 | 38.1% | 91% | 24.1M | 61 GB |
| `100` | 4640 | 75.4% | 98% | 46.4M | 117 GB |
| `200` | 4708 | 100.0% | 98% | 47.1M | 118 GB |
| `400` | 4708 | 100.0% | 98% | 47.1M | 118 GB |

Two columns because they answer different questions. *Terms indexed* is the share
of a chunk's distinct terms that are searchable at all. *Tail term reachable* is
whether a phrase in the last tenth of a chunk can be found — the case that makes
large chunks worth using, since the encoder's 128-token window embeds only the
first fraction of a 1000-token chunk.

**Do not set these above 100.** Coverage of the tail saturates there: `200` and
`400` cost the same memory as `100` and find nothing more. Going from `100` to
`50` halves resident memory for seven points of tail recall, which is usually the
right trade for a large corpus. The `12` default is cheap but reaches only 59% of
tails, which is why specific-phrase lookups miss on long chunks.

```bash
MATRIXARK_INDEX_KEYWORD_LIMIT=50 MATRIXARK_MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK=50 MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_CHUNK=50 MATRIXARK_MAX_SECONDARY_INDEX_TERMS_PER_RECORD=50 MATRIXARK_INDEX_POSTING_LISTS=1 matrixark_rust_datanode
```

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
