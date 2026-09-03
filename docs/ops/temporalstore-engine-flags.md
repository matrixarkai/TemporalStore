# Engine flags

Every `TS_*` variable the engine reads, grouped by what it decides. Generated from the source
by `tools/build_engine_flag_inventory.py` and checked by
`test_matrixark_engine_flag_inventory.py`, because a hand-kept list of this many knobs is wrong
within a week and its staleness is silent.

## Why this exists

There are 98 of them, and **none is dead**: all 55 accessor functions that read one have a
non-test caller. So the length of this list is not a cleanup backlog -- every flag changes
behaviour, and retiring one is a decision about behaviour rather than tidying.

What the list gives an owner deciding that is: how many files each flag reaches (the code its
non-default path keeps alive), whether its own documentation calls that path legacy, and
whether a customer can set it at all.

| flags | count |
|---|---|
| total | 98 |
| offered on the portal | 16 |
| documented as keeping an older path alive | 6 |
| reaching more than two files | 3 |
| whose doc comment is really about another flag | 0 |

## topology (33)

Where this node is and what it talks to. Set by whoever provisions the node; not tenant-facing and not tuning.

| flag | files | portal | keeps an older path |
|---|---|---|---|
| `TS_META_ADDR` | 3 | — | — |
| `TS_PROXY_ADDR` | 3 | — | — |
| `TS_SHARD_ID` | 3 | — | — |
| `TS_META_RAFT_NODES` | 2 | — | — |
| `TS_RAFT_NODES` | 2 | — | — |
| `TS_RAFT_WAL_DIR` | 2 | — | — |
| `TS_BLOB_STORE_DIR` | 1 | — | — |
| `TS_CACHE_DIR` | 1 | — | — |
| `TS_CLUSTER_ID` | 1 | — | — |
| `TS_DISTRIBUTED` | 1 | — | — |
| `TS_INDEX_DIR` | 1 | — | — |
| `TS_MATRIXOBJECT_BUCKET` | 1 | — | — |
| `TS_MATRIXOBJECT_ENDPOINT` | 1 | — | — |
| `TS_MATRIXOBJECT_STORE_DIR` | 1 | — | — |
| `TS_META_BIND_ADDR` | 1 | — | — |
| `TS_PAGE_STORE_DIR` | 1 | — | — |
| `TS_PROXY_ADVERTISED_ADDR` | 1 | — | — |
| `TS_PROXY_BIND_ADDR` | 1 | — | — |
| `TS_PROXY_LOCATION` | 1 | — | — |
| `TS_RAFT_BIND_ADDR` | 1 | — | — |
| `TS_REDIS_ADDR` | 1 | — | — |
| `TS_REDIS_BIND_ADDR` | 1 | — | — |
| `TS_SERVER_ADDR` | 1 | — | — |
| `TS_SERVER_ADVERTISE_ADDR` | 1 | — | — |
| `TS_SERVER_BIND_ADDR` | 1 | — | — |
| `TS_SERVER_LOCATION` | 1 | — | — |
| `TS_SERVER_NODE_ID` | 1 | — | — |
| `TS_SHARD_URI` | 1 | — | — |
| `TS_SHARED_STORE_CLUSTER_ID` | 1 | — | — |
| `TS_SHARED_STORE_DIR` | 1 | — | — |
| `TS_SHARED_STORE_URI` | 1 | — | — |
| `TS_STANDALONE` | 1 | — | — |
| `TS_STORAGE_BACKEND` | 1 | — | — |

## credential (1)

Secrets. Never a form field, never in a launch artifact.

| flag | files | portal | keeps an older path |
|---|---|---|---|
| `TS_META_ADMIN_TOKEN` | 1 | — | — |

## durability (25)

What is written, when it is flushed, and what is reclaimed. The escape hatches here trade throughput for a more conservative barrier.

| flag | files | portal | keeps an older path |
|---|---|---|---|
| `TS_WAL_BINARY_FRAME` | 2 | — | — |
| `TS_WAL_LEGACY_RECOVERY` | 2 | — | — |
| `TS_BARRIER_PROFILE_WRITES` | 1 | — | — |
| `TS_BLOCK_IN_WAL` | 1 | — | — |
| `TS_CROSS_SHARD_RECLAIM_GUARD` | 1 | — | yes |
| `TS_DATA_NODE_LIFECYCLE_SNAPSHOT` | 1 | — | — |
| `TS_INDEX_DUMP_WAL_GAP_BYTES` | 1 | yes | — |
| `TS_META_SCHEDULER_SNAPSHOT` | 1 | — | — |
| `TS_RAFT_OVERLAP_LEADER_BARRIER` | 1 | — | — |
| `TS_RAFT_WAL_BINARY_RECORDS` | 1 | — | — |
| `TS_RAFT_WAL_DELTA_ENTRIES` | 1 | — | yes |
| `TS_SHARED_STORE_FENCE` | 1 | — | — |
| `TS_WAL_BINARY_RECORDS` | 1 | — | — |
| `TS_WAL_COMMIT_DELAY_US` | 1 | — | — |
| `TS_WAL_DATA_ONLY` | 1 | — | — |
| `TS_WAL_ITEM_METADATA` | 1 | — | — |
| `TS_WAL_OUTCOME_ITEMS` | 1 | — | — |
| `TS_WAL_OUTCOME_STRICT` | 1 | — | — |
| `TS_WAL_PREALLOCATE` | 1 | — | — |
| `TS_WAL_PREALLOCATE_CHUNK` | 1 | — | — |
| `TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS` | 1 | — | yes |
| `TS_WAL_RECLAIM_MIN_COPY_BYTES` | 1 | — | — |
| `TS_WAL_RECLAIM_MIN_FREED_PERCENT` | 1 | — | — |
| `TS_WAL_RESIDENT_PAGES` | 1 | — | — |
| `TS_WAL_SEGMENT_BYTES` | 1 | — | — |

## format (9)

The shape of what is written. Readers generally accept both shapes, which is what makes these safe to flip and hard to retire.

| flag | files | portal | keeps an older path |
|---|---|---|---|
| `TS_INDEX_CATALOG_FOLD` | 1 | — | yes |
| `TS_INDEX_CODEC` | 1 | — | — |
| `TS_INDEX_LOG_BINARY` | 1 | — | — |
| `TS_LEGACY_ARRAY_BYTES` | 1 | — | yes |
| `TS_NODE_SUMMARY_VECTOR` | 1 | yes | — |
| `TS_PROXY_BINARY_VERSION` | 1 | — | — |
| `TS_RAFT_BINARY_REPLICATION` | 1 | — | — |
| `TS_VECTOR_INT8` | 1 | yes | — |
| `TS_VECTOR_SCALED` | 1 | yes | — |

## capacity (15)

Sizes, ceilings and intervals. The tuning a deployment actually reaches for.

| flag | files | portal | keeps an older path |
|---|---|---|---|
| `TS_BLOCK_INDEX_CACHE_BYTES` | 1 | yes | — |
| `TS_BLOCK_SEGMENT_TARGET_BYTES` | 1 | yes | — |
| `TS_CACHE_MEMORY_BYTES` | 1 | — | — |
| `TS_COMPACTION_WATERMARK_BYTES` | 1 | yes | — |
| `TS_CONTEXT_PAGE_TARGET_BYTES` | 1 | yes | — |
| `TS_DISTRIBUTED_RAFT_CATCHUP_TIMEOUT_SECS` | 1 | — | — |
| `TS_INDEX_DUMP_OPLOG_GAP_BYTES` | 1 | — | — |
| `TS_MATRIXOBJECT_PROBE_TIMEOUT_MS` | 1 | — | — |
| `TS_MAX_RETAINED_FINISHED_JOBS` | 1 | yes | — |
| `TS_METRICS_MAX_SLOT_SERIES` | 1 | yes | — |
| `TS_PAGE_INDEX_CACHE_BYTES` | 1 | yes | — |
| `TS_SERVER_HEARTBEAT_INTERVAL_MS` | 1 | — | — |
| `TS_SHARED_STORE_MAX_PENDING` | 1 | — | yes |
| `TS_STORAGE_ZONE_SIZE` | 1 | yes | — |
| `TS_STREAM_MAX_BLOB_SIZE` | 1 | yes | — |

## diagnostic (1)

Extra evidence for someone investigating. Off by default.

| flag | files | portal | keeps an older path |
|---|---|---|---|
| `TS_SCALE_STORAGE_ROOT` | 1 | — | — |

## behaviour (14)

Everything else that changes what the engine does.

| flag | files | portal | keeps an older path |
|---|---|---|---|
| `TS_PHASE1_FLAT` | 2 | — | — |
| `TS_CACHE_DISK_TIER` | 1 | — | — |
| `TS_COLD_SCAN_NO_CACHE_FILL` | 1 | yes | — |
| `TS_DATA_RAFT_READ_MODE` | 1 | — | — |
| `TS_HOT_PAGE_SPILL` | 1 | — | — |
| `TS_MALLOC_TRIM` | 1 | yes | — |
| `TS_META_FD_PHI_THRESHOLD` | 1 | — | — |
| `TS_META_MUTATION_LOG` | 1 | — | — |
| `TS_PROXY_INGESTION_ACCOUNT` | 1 | — | — |
| `TS_PROXY_NAMESPACE` | 1 | — | — |
| `TS_RAFT_FOLLOWER_PIPELINE` | 1 | — | — |
| `TS_SCRATCH_SWEEP` | 1 | yes | — |
| `TS_SERVER_RAFT_READ_MODE` | 1 | — | — |
| `TS_TABLE_NAME` | 1 | — | — |

