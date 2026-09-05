# Engine flags

Every environment variable the engine reads -- `TS_*`, `MATRIXARK_*` and
`TEMPORALSTORE_*` -- grouped by what it decides. Generated from the source
by `tools/build_engine_flag_inventory.py` and checked by
`test_matrixark_engine_flag_inventory.py`, because a hand-kept list of this many knobs is wrong
within a week and its staleness is silent.

## Why this exists

There are 308 of them, read by 105 functions.

Deleting unreachable code is not the lever. An earlier version of this document argued that
by asserting every accessor had a caller -- true when it was hand-checked at 55, and carried
forward unexamined ever since. Computing it instead named four functions as uncalled that
plainly are: Rust reaches a function by more than one syntax (a generic definition,
`unwrap_or_else(name)` passing it as a value, serde naming it in a string) and a name scan
sees none of those. So the claim is neither asserted nor computed here.

What shortens it is a narrower question, asked per flag: **does anything anywhere select the
off position?** Not a test, not a config file, not a launch profile, not a portal setting. A
flag no one can be shown to turn off is not a switch, it is a branch -- and the live side can
be made unconditional, taking the dead side with it. That is a safe edit exactly when reads do
not consult the flag: if the decoder already accepts both shapes, retiring the writer strands
nothing already written.

What the list gives an owner asking that is: how many files each flag reaches (the code its
non-default path keeps alive), whether its own documentation calls that path legacy, and --
the two columns that answer the question -- its **default**, where that can be read off the
source, and **who sets it**: a test, the shipped config file, a launch profile, a Python
launcher, a test harness, or the customer portal. `nothing` means no place in this
repository gives it a value, so whatever its non-default path does, nothing here asks for it.

That column is literal about WHERE, not about WHAT: it does not compare the value set against
the default, because the default lives in Rust and this reads text. `config` is a prompt to go
look; `nothing` is an answer.

The **default** column reads two shapes: a function that reads one flag and returns a bool,
and a single statement that does the same inline. The second was added because the first
made the row below it -- flags defaulting on that nothing sets -- report zero while
`MATRIXARK_RUST_PROXY_ASYNC_CACHE_WARM_ON_LOAD` sat inline in the middle of a function,
defaulting on, set by nothing. A statement is the unit rather than a window of lines because
the `!` that inverts `!matches!(env::var(X)...)` sits above the name but inside the same
statement: a statement always carries its own negation, a window carries it only by luck.
Anything else is blank, and a blank means go and look.

| flags | count |
|---|---|
| total | 308 |
| booleans whose default this could read off the source | 43 |
| **defaulting on, and set by nothing** | 9 |
| offered on the portal | 23 |
| **that nothing in this repository sets** | 184 |
| documented as keeping an older path alive | 6 |
| reaching more than two files | 16 |
| whose doc comment is really about another flag | 43 |

## topology (38)

Where this node is and what it talks to. Set by whoever provisions the node; not tenant-facing and not tuning.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `TS_META_ADDR` | — | config, launch | 3 | — |
| `TS_PROXY_ADDR` | — | launch, script | 3 | — |
| `TS_RAFT_NODES` | — | harness, script | 3 | — |
| `TS_RAFT_NODE_ID` | — | harness, script | 3 | — |
| `TS_RAFT_SHARD_ID` | — | harness, script | 3 | — |
| `TS_RAFT_WAL_DIR` | — | config, harness, script | 3 | — |
| `TS_SHARD_ID` | — | config, launch | 3 | — |
| `TS_META_RAFT_NODES` | — | config | 2 | — |
| `TS_RAFT_BIND_ADDR` | — | harness, script | 2 | — |
| `MATRIXARK_CONTEXT_EVENT_FANOUT_NODES` | — | nothing | 1 | — |
| `MATRIXARK_TEMPORALSTORE_PROXY_ADDR` | — | nothing | 1 | — |
| `TS_BLOB_STORE_DIR` | — | config, launch, script | 1 | — |
| `TS_CACHE_DIR` | — | config, launch, script | 1 | — |
| `TS_CLUSTER_ID` | — | launch | 1 | — |
| `TS_DISTRIBUTED` | — | config, launch, script | 1 | — |
| `TS_INDEX_DIR` | — | config, launch, script | 1 | — |
| `TS_MATRIXOBJECT_BUCKET` | — | config, launch | 1 | — |
| `TS_MATRIXOBJECT_ENDPOINT` | — | config | 1 | — |
| `TS_MATRIXOBJECT_STORE_DIR` | — | config | 1 | — |
| `TS_META_BIND_ADDR` | — | config | 1 | — |
| `TS_META_RAFT_NODE_ID` | — | nothing | 1 | — |
| `TS_PAGE_STORE_DIR` | — | config, launch, script | 1 | — |
| `TS_PROXY_ADVERTISED_ADDR` | — | config | 1 | — |
| `TS_PROXY_BIND_ADDR` | — | config | 1 | — |
| `TS_PROXY_LOCATION` | — | nothing | 1 | — |
| `TS_REDIS_ADDR` | — | launch | 1 | — |
| `TS_REDIS_BIND_ADDR` | — | config | 1 | — |
| `TS_SERVER_ADDR` | — | launch | 1 | — |
| `TS_SERVER_ADVERTISE_ADDR` | — | config, launch | 1 | — |
| `TS_SERVER_BIND_ADDR` | — | config | 1 | — |
| `TS_SERVER_LOCATION` | — | config | 1 | — |
| `TS_SERVER_NODE_ID` | — | config, launch | 1 | — |
| `TS_SHARD_URI` | — | test | 1 | — |
| `TS_SHARED_STORE_CLUSTER_ID` | — | launch | 1 | — |
| `TS_SHARED_STORE_DIR` | — | config, script | 1 | — |
| `TS_SHARED_STORE_URI` | — | nothing | 1 | — |
| `TS_STANDALONE` | — | config, launch, script | 1 | — |
| `TS_STORAGE_BACKEND` | — | config, launch, script | 1 | — |

## cluster policy (39)

What the metaserver does about nodes it cannot reach -- conviction, freezing, rebalancing, failover. Cluster-wide, and never per shard.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `TS_META_FORBID_SELF_CLEARING_CONVICTION` | — | nothing | 2 | — |
| `MATRIXARK_TEMPORALSTORE_META_SYNC_DEADLINE_MS` | — | nothing | 1 | — |
| `TS_AUTO_REBALANCE_DATA_MOVE` | — | nothing | 1 | — |
| `TS_META_ADAPTIVE_FAILURE_DETECTOR` | — | nothing | 1 | — |
| `TS_META_AUTO_REBALANCE` | — | config | 1 | — |
| `TS_META_AUTO_REBALANCE_BALANCE` | — | nothing | 1 | — |
| `TS_META_CONVICT_ENABLED` | — | nothing | 1 | — |
| `TS_META_CONVICT_ON_REBOOT` | — | nothing | 1 | — |
| `TS_META_CONVICT_PROXIES` | — | nothing | 1 | — |
| `TS_META_CONVICT_SAFE_MODE` | — | nothing | 1 | — |
| `TS_META_FD_PHI_THRESHOLD` | — | nothing | 1 | — |
| `TS_META_FD_SAMPLE_CAPACITY` | — | nothing | 1 | — |
| `TS_META_FORBID_ORPHANING_SHARDS` | — | nothing | 1 | — |
| `TS_META_FREEZE_AGING` | — | nothing | 1 | — |
| `TS_META_MUTATION_LOG` | — | nothing | 1 | — |
| `TS_META_PLACEMENT_AWARE_REBALANCE` | — | nothing | 1 | — |
| `TS_META_PROXY_CALIBRATION` | — | nothing | 1 | — |
| `TS_META_PROXY_FREEZE_COOLDOWN_MS` | — | nothing | 1 | — |
| `TS_META_PROXY_FREEZE_MS` | — | nothing | 1 | — |
| `TS_META_PROXY_RETENTION_MS` | — | nothing | 1 | — |
| `TS_META_RAFT` | — | config, script | 1 | — |
| `TS_META_RAFT_ELECTION_TICK_MS` | — | nothing | 1 | — |
| `TS_META_REBALANCE_LOCATION_SCOPED` | — | nothing | 1 | — |
| `TS_META_REBALANCE_PER_TABLE` | — | nothing | 1 | — |
| `TS_META_REBALANCE_SAFE_GAP` | — | nothing | 1 | — |
| `TS_META_RETENTION_GC` | — | nothing | 1 | — |
| `TS_META_RETENTION_MS` | — | nothing | 1 | — |
| `TS_META_SERVER_FREEZE_COOLDOWN_MS` | — | nothing | 1 | — |
| `TS_META_SERVER_FREEZE_MS` | — | nothing | 1 | — |
| `TS_META_SERVER_RETENTION_MS` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_CHECK` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_REBOOT_GRACE_MS` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_SETTLE_GRACE_MS` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_WINDOW_MS` | — | nothing | 1 | — |
| `TS_META_STALE_AFTER_MS` | — | nothing | 1 | — |
| `TS_META_TABLE_FREEZE_MS` | — | nothing | 1 | — |
| `TS_META_TABLE_RETENTION_MS` | — | nothing | 1 | — |
| `TS_META_TASK_SCHEDULER_BASE_POSTPONE_MS` | — | nothing | 1 | — |
| `TS_RAFT_AUTO_FAILOVER` | — | config, script | 1 | — |

## credential (5)

Secrets. Never a form field, never in a launch artifact.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `TS_RAFT_AUTH_TOKEN` | — | harness, script | 2 | — |
| `MATRIXARK_HOOK_MAX_CONTEXT_TOKENS` | — | nothing | 1 | — |
| `MATRIXARK_MODEL_API_KEY` | — | nothing | 1 | — |
| `TS_API_AUTH_TOKEN` | — | nothing | 1 | — |
| `TS_META_ADMIN_TOKEN` | — | nothing | 1 | — |

## durability (25)

What is written, when it is flushed, and what is reclaimed. The escape hatches here trade throughput for a more conservative barrier.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `TS_WAL_LEGACY_RECOVERY` | on | config, harness, script | 3 | — |
| `MATRIXARK_BULK_INGEST_EXPECTED_WAL_COMMANDS` | — | test | 2 | — |
| `TS_RAFT_SNAPSHOT_CHECK_INTERVAL_MS` | — | nothing | 2 | — |
| `MATRIXARK_RUST_PROXY_LOG_RECLAIM_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_BARRIER_PROFILE_WRITES` | — | nothing | 1 | — |
| `TS_BLOCK_IN_WAL` | on | test | 1 | yes |
| `TS_CROSS_SHARD_RECLAIM_GUARD` | on | config | 1 | yes |
| `TS_DATA_NODE_LIFECYCLE_SNAPSHOT` | — | nothing | 1 | — |
| `TS_INDEX_DUMP_WAL_GAP_BYTES` | — | portal | 1 | — |
| `TS_META_RAFT_SNAPSHOT_CHECK_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_SCHEDULER_SNAPSHOT` | — | nothing | 1 | — |
| `TS_RAFT_WAL_BINARY_RECORDS` | on | nothing | 1 | — |
| `TS_WAL_BINARY_FRAME` | on | launch, test | 1 | — |
| `TS_WAL_BINARY_RECORDS` | on | launch, test | 1 | — |
| `TS_WAL_COMMIT_DELAY_US` | — | config | 1 | — |
| `TS_WAL_DATA_ONLY` | on | launch, test | 1 | — |
| `TS_WAL_OUTCOME_ITEMS` | on | launch, test | 1 | — |
| `TS_WAL_OUTCOME_STRICT` | off | nothing | 1 | — |
| `TS_WAL_PREALLOCATE` | on | launch, test | 1 | — |
| `TS_WAL_PREALLOCATE_CHUNK` | — | nothing | 1 | — |
| `TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS` | — | test | 1 | yes |
| `TS_WAL_RECLAIM_MIN_COPY_BYTES` | — | nothing | 1 | — |
| `TS_WAL_RECLAIM_MIN_FREED_PERCENT` | — | nothing | 1 | — |
| `TS_WAL_RESIDENT_PAGES` | — | test | 1 | — |
| `TS_WAL_SEGMENT_BYTES` | — | nothing | 1 | — |

## format (8)

The shape of what is written. Readers generally accept both shapes, which is what makes these safe to flip and hard to retire.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `TS_INDEX_BINARY` | on | launch, test | 1 | — |
| `TS_INDEX_CATALOG_FOLD` | on | config | 1 | yes |
| `TS_INDEX_CODEC` | — | test | 1 | — |
| `TS_INDEX_LOG_BINARY` | on | nothing | 1 | — |
| `TS_NODE_SUMMARY_VECTOR` | on | test, portal | 1 | — |
| `TS_PROXY_BINARY_VERSION` | — | nothing | 1 | — |
| `TS_VECTOR_INT8` | off | test, portal | 1 | — |
| `TS_VECTOR_SCALED` | on | launch, script, test, portal | 1 | — |

## capacity (80)

Sizes, ceilings and intervals. The tuning a deployment actually reaches for.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `TS_RAFT_HEARTBEAT_INTERVAL_MS` | — | harness, script | 3 | — |
| `TS_PAGE_STORE_COMPRESSION_MIN_BYTES` | — | config, test | 2 | — |
| `TS_PROXY_CONTEXT_IO_TIMEOUT_MS` | — | nothing | 2 | — |
| `TS_RAFT_MAX_CATCHUP_ENTRIES_PER_HEARTBEAT` | — | nothing | 2 | — |
| `MATRIXARK_BACKFILL_CACHE_BYTES` | — | nothing | 1 | — |
| `MATRIXARK_BACKFILL_MAX_BODY` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_COMPRESSION_MAX_AGE_MS` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_COMPRESSION_MAX_RAW_EVENTS` | — | nothing | 1 | — |
| `MATRIXARK_EMBED_DRAINER_INTERVAL_MS` | — | config, launch, portal | 1 | — |
| `MATRIXARK_EMBED_DRAINER_MAX_EVENTS` | — | nothing | 1 | — |
| `MATRIXARK_EMBED_DRAINER_MAX_NODES_PER_PASS` | — | nothing | 1 | — |
| `MATRIXARK_RETRIEVAL_MAX_CANDIDATES` | — | nothing | 1 | — |
| `MATRIXARK_RUST_PROXY_CACHE_BYTES` | — | nothing | 1 | — |
| `MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_MIN_BYTES` | — | test | 1 | — |
| `MATRIXARK_TEMPORALSTORE_PROXY_CONNECT_TIMEOUT_MS` | — | nothing | 1 | — |
| `MATRIXARK_TEMPORALSTORE_PROXY_IO_TIMEOUT_MS` | — | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_INGEST_CHUNK_SIZE` | — | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_MAX_EVENTS` | — | nothing | 1 | — |
| `TS_BLOB_CHUNK_BYTES` | — | config | 1 | — |
| `TS_BLOB_PEER_FETCH_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_BLOCK_INDEX_CACHE_BYTES` | — | config, portal | 1 | — |
| `TS_BLOCK_SEGMENT_TARGET_BYTES` | — | config, portal | 1 | — |
| `TS_CACHE_MEMORY_BYTES` | — | config, launch | 1 | — |
| `TS_COMPACTION_WATERMARK_BYTES` | — | config, portal | 1 | — |
| `TS_CONTEXT_PAGE_TARGET_BYTES` | — | config, portal | 1 | — |
| `TS_DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG` | — | nothing | 1 | — |
| `TS_DATA_RAFT_READ_INDEX_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_DISTRIBUTED_RAFT_CATCHUP_TIMEOUT_SECS` | — | nothing | 1 | — |
| `TS_EVICT_POOL_SIZE` | — | nothing | 1 | — |
| `TS_INDEX_DUMP_OPLOG_GAP_BYTES` | — | config | 1 | — |
| `TS_MATRIXOBJECT_FLUSH_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_MATRIXOBJECT_PROBE_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_MAX_RETAINED_FINISHED_JOBS` | — | portal | 1 | — |
| `TS_META_AUTO_REBALANCE_CONNECT_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_META_AUTO_REBALANCE_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_AUTO_REBALANCE_IO_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_META_CONVICT_CRITICAL_RATIO_PERCENT` | — | nothing | 1 | — |
| `TS_META_CONVICT_MIN_ABNORMAL` | — | nothing | 1 | — |
| `TS_META_CONVICT_WARNING_RATIO_PERCENT` | — | nothing | 1 | — |
| `TS_META_FAILURE_DETECTOR_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_FD_INITIAL_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_FD_MAX_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_FD_MAX_ROUND_PAUSE_MS` | — | nothing | 1 | — |
| `TS_META_FREEZE_AGING_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_FREEZE_AGING_MAX_DROPS` | — | nothing | 1 | — |
| `TS_META_PROXY_CALIBRATION_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_PROXY_CALIBRATION_MAX_CHANGES` | — | nothing | 1 | — |
| `TS_META_RAFT_HEARTBEAT_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_RETENTION_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_RETENTION_MAX_PURGES` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_CONNECT_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_IO_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_META_SHARD_DIVERGENCE_MAX_MOVES` | — | nothing | 1 | — |
| `TS_META_TASK_SCHEDULER_MAX_INFLIGHT` | — | nothing | 1 | — |
| `TS_META_TASK_SCHEDULER_MAX_POSTPONE_MS` | — | nothing | 1 | — |
| `TS_META_TASK_SCHEDULER_MAX_RETRY_TIMES` | — | nothing | 1 | — |
| `TS_METRICS_MAX_SLOT_SERIES` | — | portal | 1 | — |
| `TS_PAGE_INDEX_CACHE_BYTES` | — | config, portal | 1 | — |
| `TS_PROXY_AUTO_REGISTER_MIN_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_PROXY_CONNECT_TIMEOUT_MS` | — | config | 1 | — |
| `TS_PROXY_DROP_PERCENT` | — | nothing | 1 | — |
| `TS_PROXY_HEARTBEAT_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_PROXY_HEARTBEAT_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_PROXY_IO_TIMEOUT_MS` | — | config | 1 | — |
| `TS_PROXY_MAX_INFLIGHT_REQUESTS` | — | nothing | 1 | — |
| `TS_PROXY_MAX_INFLIGHT_WRITE_REQUESTS` | — | nothing | 1 | — |
| `TS_PROXY_MAX_RETRIES` | — | config | 1 | — |
| `TS_PROXY_TOPOLOGY_CHECK_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_RAFT_AUTO_FAILOVER_CONNECT_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_RAFT_AUTO_FAILOVER_INTERVAL_MS` | — | nothing | 1 | — |
| `TS_RAFT_AUTO_FAILOVER_IO_TIMEOUT_MS` | — | nothing | 1 | — |
| `TS_RAFT_MAX_APPLIED_LOG_BYTES` | — | nothing | 1 | — |
| `TS_RAFT_MAX_INFLIGHTS_REPLICATE` | — | test | 1 | — |
| `TS_SERVER_HEARTBEAT_INTERVAL_MS` | — | config | 1 | — |
| `TS_SERVER_MAX_BACKGROUND_QUEUE_DEPTH` | — | config | 1 | — |
| `TS_SERVER_MAX_QUEUE_DEPTH` | — | config | 1 | — |
| `TS_SHARED_STORE_MAX_PENDING` | — | nothing | 1 | yes |
| `TS_STORAGE_ZONE_SIZE` | — | config, portal | 1 | — |
| `TS_STREAM_MAX_BLOB_SIZE` | — | config, portal | 1 | — |

## context (27)

The memory pipeline: what gets extracted, embedded, drained and packed. The surface a deployment tunes for recall rather than for throughput.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `MATRIXARK_BACKFILL_CHECKPOINT_ONLY` | — | nothing | 1 | — |
| `MATRIXARK_BACKFILL_FLUSH_EVERY_ACCEPTED` | — | nothing | 1 | — |
| `MATRIXARK_BACKFILL_FLUSH_EVERY_SESSION` | — | nothing | 1 | — |
| `MATRIXARK_BACKFILL_GLOBAL_SESSION` | — | nothing | 1 | — |
| `MATRIXARK_BACKFILL_RAW_FIRST` | — | nothing | 1 | — |
| `MATRIXARK_BACKFILL_SKIP_COVERED_SESSIONS` | — | nothing | 1 | — |
| `MATRIXARK_BACKFILL_SUB_BATCH` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_COMPRESSION_ENABLED` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_COMPRESSION_KEEP_RECENT` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_COMPRESSION_WINDOW` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_EVENT_QUERY_OVERFETCH` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_EVENT_SCAN_CAP` | — | nothing | 1 | — |
| `MATRIXARK_CONTEXT_HYBRID_LEXICAL` | on | nothing | 1 | — |
| `MATRIXARK_CONTEXT_PACK_INCLUDE_SCORES` | off | nothing | 1 | — |
| `MATRIXARK_CONTEXT_SECONDARY_INDEX` | off | test | 1 | — |
| `MATRIXARK_EMBEDDING_MODEL` | — | config, script, portal | 1 | — |
| `MATRIXARK_EMBED_API_KEY_ENV` | — | nothing | 1 | — |
| `MATRIXARK_EMBED_BASE_URL` | — | config, script | 1 | — |
| `MATRIXARK_EMBED_DEFER_ON_FAILURE` | on | nothing | 1 | yes |
| `MATRIXARK_EMBED_DRAINER` | — | config, launch, portal | 1 | — |
| `MATRIXARK_EMBED_DRAINER_BATCH` | — | config, portal | 1 | — |
| `MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT` | — | launch | 1 | — |
| `MATRIXARK_REQUIRE_MODEL_EMBEDDINGS` | off | config, portal | 1 | — |
| `MATRIXARK_REQUIRE_MODEL_SUMMARIES` | off | config, portal | 1 | — |
| `MATRIXARK_RETRIEVAL_TRAVERSAL_TOP_K` | — | nothing | 1 | — |
| `TS_PROXY_CONTEXT_FIRST_SHARD` | — | nothing | 1 | — |
| `TS_PROXY_CONTEXT_SHARD_COUNT` | — | nothing | 1 | — |

## identity (6)

Who the caller is, not what the engine does. Supplied per request or per process; nothing here is a switch.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `MATRIXARK_ACCOUNT_ID` | — | launch | 2 | — |
| `MATRIXARK_AGENT_NAME` | — | launch | 2 | — |
| `MATRIXARK_TENANT_ID` | — | launch | 2 | — |
| `MATRIXARK_USER_ID` | — | launch, script | 2 | — |
| `TEMPORALSTORE_AGENT_NAME` | — | launch | 2 | — |
| `MATRIXARK_SESSION_ID` | — | nothing | 1 | — |

## diagnostic (6)

Extra evidence for someone investigating. Off by default.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE` | off | test | 1 | — |
| `MATRIXARK_CONTEXT_RETRIEVE_TRACE` | off | nothing | 1 | — |
| `MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG` | off | script | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY` | off | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_TRACE` | off | nothing | 1 | — |
| `TS_SCALE_STORAGE_ROOT` | — | nothing | 1 | — |

## benchmark (8)

Read only by the benchmark harnesses. Never consulted on a serving path.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `TEMPORALSTORE_CONTEXT_BENCHMARK_ALL_SOURCE_REPLAY` | off | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_COMPACT_SOURCE_REPLAY` | on | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING` | off | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY` | off | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL` | — | script | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_SELECTED_ID_LIMIT` | — | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_SOURCE_ORDER_RANKING` | off | nothing | 1 | — |
| `TEMPORALSTORE_CONTEXT_BENCHMARK_STORED_RECORD_SCORING` | on | nothing | 1 | — |

## behaviour (66)

Everything else that changes what the engine does.

| flag | default | set by | files | keeps an older path |
|---|---|---|---|---|
| `MATRIXARK_BULK_INGEST` | off | config, test, portal | 6 | — |
| `MATRIXARK_EAGER_CACHE_WARM_ON_LOAD` | on | config | 5 | — |
| `TS_RAFT_ALLOW_PLAINTEXT` | — | harness, script | 4 | — |
| `TS_RAFT_ELECTION_TICK_MS` | — | harness, script | 3 | — |
| `TS_RAFT_ENABLE_LOCAL_ADMIN` | — | harness, script | 3 | — |
| `TS_RAFT_RPC_DEADLINE_MS` | — | harness, script | 3 | — |
| `TS_RAFT_RPC_RETRIES` | — | harness, script | 3 | — |
| `MATRIXARK_BULK_INGEST_REPLAY_FROM_SEQUENCE` | — | config, test | 2 | — |
| `TEMPORALSTORE_RUST_CODEX_HOOK_ROOT` | — | launch | 2 | — |
| `TS_PAGE_STORE_COMPRESSION_ENABLED` | — | config, launch, test | 2 | — |
| `TS_PAGE_STORE_COMPRESSION_LEVEL` | — | config, test | 2 | — |
| `TS_PHASE1_FLAT` | on | test | 2 | — |
| `MATRIXARK_MONOTONIC_RECORD_COUNT` | on | nothing | 1 | — |
| `MATRIXARK_RUST_PROXY_ASYNC_CACHE_WARM_ON_LOAD` | on | nothing | 1 | — |
| `MATRIXARK_RUST_PROXY_ASYNC_STORAGE` | off | launch, test | 1 | — |
| `MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_ENABLED` | — | test | 1 | — |
| `MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_LEVEL` | — | test | 1 | — |
| `MATRIXARK_RUST_SDK_MODE` | off | nothing | 1 | — |
| `MATRIXARK_TEMPORALSTORE_RUST_ROOT` | — | launch, script, test | 1 | — |
| `TEMPORALSTORE_RUST_CODEX_EVENT_LOG` | — | launch | 1 | — |
| `TEMPORALSTORE_RUST_CODEX_EVENT_LOG_ENABLE` | — | nothing | 1 | — |
| `TS_BLOB_PEER_FETCH` | — | nothing | 1 | — |
| `TS_BLOB_RUNTIME_THREADS` | — | nothing | 1 | — |
| `TS_CACHE_DISK_TIER` | — | test | 1 | — |
| `TS_COLD_SCAN_NO_CACHE_FILL` | — | config, portal | 1 | — |
| `TS_DATA_RAFT_READ_MODE` | — | config | 1 | — |
| `TS_EVICT_SAMPLES` | — | nothing | 1 | — |
| `TS_EVICT_SCAN_TURNS` | — | nothing | 1 | — |
| `TS_MALLOC_TRIM` | on | test, portal | 1 | — |
| `TS_MANIFEST_SHORT_FIELD_NAMES` | on | nothing | 1 | — |
| `TS_MATRIXOBJECT_CHECKPOINT_ON_START` | — | nothing | 1 | — |
| `TS_MATRIXOBJECT_FLUSH_BATCH` | — | nothing | 1 | — |
| `TS_MATRIXOBJECT_NETWORKED_CHECKPOINT_ON_START` | — | nothing | 1 | — |
| `TS_MATRIXOBJECT_SYNC_FLUSH` | — | nothing | 1 | — |
| `TS_PROXY_BACKEND_CONTINUOUS_FAILED_TIME_MS` | — | nothing | 1 | — |
| `TS_PROXY_CONFIG_VERSION` | — | nothing | 1 | — |
| `TS_PROXY_ENFORCE_INGESTION_ACCOUNT` | — | nothing | 1 | — |
| `TS_PROXY_INGESTION_ACCOUNT` | — | nothing | 1 | — |
| `TS_PROXY_NAMESPACE` | — | nothing | 1 | — |
| `TS_PROXY_PIN_PRIMARY_READS` | — | nothing | 1 | — |
| `TS_PROXY_REFRESH_ROUTE_ON_BACKEND_ERROR` | — | nothing | 1 | — |
| `TS_PROXY_ROUTE_CACHE_TTL_MS` | — | nothing | 1 | — |
| `TS_PROXY_SERVICE_REGISTRY_TTL_MS` | — | nothing | 1 | — |
| `TS_PROXY_SERVING_MODE` | — | nothing | 1 | — |
| `TS_RAFT_CA_CERT_PATH` | — | nothing | 1 | — |
| `TS_RAFT_CA_PATH` | — | nothing | 1 | — |
| `TS_RAFT_CERT_PATH` | — | nothing | 1 | — |
| `TS_RAFT_KEY_PATH` | — | nothing | 1 | — |
| `TS_RAFT_REPLICATION_DEADLINE_MS` | — | test | 1 | — |
| `TS_RAFT_SECURITY_MODE` | — | nothing | 1 | — |
| `TS_RAFT_TRANSPORT_SECURITY` | — | nothing | 1 | — |
| `TS_SCRATCH_SWEEP` | on | portal | 1 | — |
| `TS_SERVER_JOIN_EMPTY` | — | nothing | 1 | — |
| `TS_SERVER_RAFT` | — | nothing | 1 | — |
| `TS_SERVER_RAFT_READ_MODE` | — | nothing | 1 | — |
| `TS_SERVER_READONLY` | — | nothing | 1 | — |
| `TS_SERVER_WORKER_THREADS` | — | config | 1 | — |
| `TS_SHARD_END_ROUTING_SLOT` | — | test | 1 | — |
| `TS_SHARD_LOAD_VERSION` | — | test | 1 | — |
| `TS_SHARD_READONLY` | — | test | 1 | — |
| `TS_SHARD_READ_BURST` | — | nothing | 1 | — |
| `TS_SHARD_READ_QPS` | — | nothing | 1 | — |
| `TS_SHARD_START_ROUTING_SLOT` | — | test | 1 | — |
| `TS_SHARD_WRITE_BURST` | — | nothing | 1 | — |
| `TS_SHARD_WRITE_QPS` | — | nothing | 1 | — |
| `TS_TABLE_NAME` | — | test | 1 | — |

## outside this document (6)

`sdk/rust/temporalstore` is a second Rust tree, carrying its own copy of the
proxy implementation -- a different file, not a stale duplicate. The root
manifest excludes it from the workspace, so `cargo check --all-targets` does not
build it, here or in CI, and it is a client of the engine rather than part of
it. These are the variables it reads that this document does not cover. The list
is computed, so one more cannot appear quietly.

- `MATRIXARK_RUST_FILTERED_SCAN_CACHE_ENTRIES`
- `MATRIXARK_RUST_METRICS_PATH`
- `MATRIXARK_RUST_PROXY_DISABLE_LEGACY_PACK_FALLBACK`
- `MATRIXARK_RUST_PROXY_DISABLE_SDK_NATIVE_PACK`
- `MATRIXARK_RUST_SCAN_RECORD_CACHE_ENTRIES`
- `TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API`

