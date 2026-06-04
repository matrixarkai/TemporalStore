# TemporalStore Runtime Tuning

TemporalStore smoke, scale, and SSD blockcache tests read server runtime knobs from
environment variables. This keeps the binaries unchanged while letting local smoke,
AWS scale tests, and cache experiments use different sizes.

The shared defaults live in `tools/temporalstore_runtime_env.sh`.

## Storage Stream Sizing

| Environment variable | Default in smoke | Default in 3-node scale | Meaning |
| --- | ---: | ---: | --- |
| `TEMPORALSTORE_STORAGE_ZONE_SIZE` | `10485760` | `268435456` | Zone size for storage streams. Larger values reduce zone/blob switching under high write QPS. |
| `TEMPORALSTORE_STREAM_MAX_BLOB_SIZE` | `10485760` | `268435456` | Maximum stream blob size. Larger values reduce frequent blob freeze/open overhead. |
| `TEMPORALSTORE_STORAGE_ASYNC` | `false` | `false` | Whether storage writes use async mode. |
| `TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH` | `0` | `0` | Oplog bytes to buffer before dump/replay visibility. Use carefully because it directly affects secondary lag. |

For AWS scale runs, start with 256 MB:

```bash
TEMPORALSTORE_STORAGE_ZONE_SIZE=$((256 * 1024 * 1024)) \
TEMPORALSTORE_STREAM_MAX_BLOB_SIZE=$((256 * 1024 * 1024)) \
bash tools/run_shared_file_3node_scale_ubuntu22.sh
```

For heavier ingestion, test 512 MB or 1 GB before increasing batch delays:

```bash
TEMPORALSTORE_STORAGE_ZONE_SIZE=$((512 * 1024 * 1024)) \
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

## Why This Matters

The earlier 10 MB/256 MB constants changed performance behavior:

- Too-small stream blobs cause frequent blob switching/freezing/opening during high-QPS writes.
- Too-large blobs can delay persistence and make recovery streams heavier.
- Replicator loop and batch sizes trade CPU for secondary freshness.
- Blockcache capacity should match the actual memory and SSD budget of the node.

Use environment variables per run, then record the exact values with the benchmark result folder.
