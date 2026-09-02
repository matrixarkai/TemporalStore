# Engine monitoring coverage

What each family on the engine dashboard watches, which alert speaks for it, and — the part that
matters when something is wrong — **what a blank panel there actually means**.

A blank panel is the most misleading thing a dashboard can show. It looks like a quiet system and is
usually a query pointed at a process that is not being scraped. Every family below names the process
that emits it, so "no data" can be diagnosed rather than believed.

All of these come from the **engine** job (`/metrics` on each process's own service port), not from
the gateway's `/v1/metrics`. Those are different surfaces; see `CUSTOMER_PORTAL.md`.

`tools/validate_grafana_metrics_conformance.py` checks this document against the dashboard, the
alert rules and the Rust sources, so a family described here that no longer exists — or one that
exists and is not described — fails.

---

## readiness

Whether the deployment considers itself fit to serve, and what is blocking it if not.

`temporalstore_production_readiness_ready` is the single gauge to alert on; `_blockers` and
`_service_blockers` say what is holding it. Emitted by the readiness reporter on the data node.

**Blank means:** the readiness reporter never ran. That is not "ready" — it is "unknown", and the
two look identical on a graph.

## raft

Commit progress, follower lag, and whether a majority exists at all.

`temporalstore_raft_cluster_has_majority` is the one that matters most: below a majority the cluster
serves reads and refuses writes, which presents to a customer as writes silently failing rather than
as an outage. `_node_lag` and `_node_apply_lag` separate "behind on the log" from "behind on
applying it" — a follower can be caught up on one and not the other.

**Alerts:** `TemporalStoreRaftMajorityLost`, `TemporalStoreRaftSlowFollower`,
`TemporalStoreRaftApplyStuck`.

**Blank means:** the raft nodes are not being scraped. A single-node or standalone deployment emits
nothing here by design — that is expected, not a fault.

## metaserver_scheduler

The metaserver's work queue and topology version.

`temporalstore_meta_scheduler_queue_depth` rising without bound is the shape to watch: the scheduler
is accepting work faster than it completes it, and shard placement falls behind.
`temporalstore_meta_topology_version` changing tells you a placement decision happened, which is
half of "why did latency move at 14:00".

**Alerts:** `TemporalStoreSchedulerBacklogHigh`, `TemporalStoreSchedulerRetriesHigh`.

**Blank means:** no metaserver, i.e. a standalone deployment, or the metaserver is not scraped.

## proxy_client

Route cache health, backend selection, and whether the proxy is serving at all.

`temporalstore_proxy_serving_mode` is the fast answer to "is traffic being served"; the route-cache
counters explain a latency change that has no matching change in backend load. Quarantine events
mean the proxy has taken a backend out of rotation.

**Alerts:** `TemporalStoreProxyRouteQuarantineHigh`, `TemporalStoreProxyNotServing`.

**Blank means:** clients are reaching the data node directly, with no proxy in the path.

## storage_cache

Object and page lifecycle, slot occupancy, and cache pressure.

`temporalstore_object_manager_objects` and `_page_refs` are the working-set size;
`temporalstore_storage_slot_bytes` is what that costs on disk. Cache miss pressure is the metric
that moves before read latency does, which makes it the useful early signal.

**Alerts:** `TemporalStoreStorageCacheBlockers`, `TemporalStoreBlockStoreReadErrors`,
`TemporalStoreCacheMissPressure`.

**A rename that left two panels blank:** the dashboard queried
`temporalstore_block_store_extent_bytes` and `_extent_oldest_age_ms` while the engine emits
`_band_bytes` and `_band_oldest_age_ms`. "Extent" became "band" and the dashboard was not updated,
so both panels read empty on every deployment — which looks like a store with nothing in it. The
dashboard now uses the current names.

## data_node

Runtime queues, dirty state, and lifecycle snapshots on the data node.

`temporalstore_data_node_runtime_queue_depth` and its background counterpart are the backpressure
signal — foreground rising means requests are queuing, background rising means maintenance is
falling behind. `_dirty_objects` is unflushed state, so a rising floor there is a durability
question, not a performance one.

**Alerts:** `TemporalStoreDataNodeReadinessBlocked`, `TemporalStoreDataNodeRuntimeQueueHigh`,
`TemporalStoreLifecycleSnapshotFailures`.

**Blank means:** the data node is not being scraped. Since it serves the store, this is the first
target to check.

## ingestion

Import lag, throughput and dead letters.

`temporalstore_ingestion_kafka_max_lag` is the tail that matters — average lag hides one partition
falling behind. `temporalstore_ingestion_dead_letters` is non-zero only when records were given up
on, so it should be alerted rather than watched.

**Alerts:** `TemporalStoreIngestionDeadLetters`.

**Blank means:** no streaming ingestion is configured. Deployments that import through the gateway
see nothing here and are working correctly.

## secondary_replication

Replica replay progress on the receiving side.

Carried on the engine dashboard's replay panel. This family has no dedicated alert: replay failures
surface through the data node's lifecycle counters, and duplicating them here would produce two
pages for one fault.

**Blank means:** no secondary replica is configured.

## matrixark_backend

The backend the gateway actually calls: throughput, latency, errors and record counts.

`matrixark_backend_ready` is the liveness answer, `matrixark_backend_command_latency_ms` the
distribution behind any "it feels slow" report, and `matrixark_backend_errors_total` versus
`_timeouts_total` separates a backend refusing work from one not answering in time.

**Alerts:** `MatrixArkBackendNotReady`, `MatrixArkBackendErrorsHigh`.

**Blank means:** the gateway is not reaching this backend — which is also what a misconfigured
`MATRIXARK_DATANODE_URL` looks like.

## scale_slo

The scale harness's own verdict: write and read p99, throughput, and remaining error budget.

Written by `ops_scale_readiness_harness`, not by a serving process, so these appear only after a
scale run. `temporalstore_scale_error_budget_remaining` reaching zero is the SLO having been spent,
which is a release decision rather than an incident.

**Alerts:** `TemporalStoreScaleSloRegression`.

**Blank means:** no scale run has been performed against this deployment. That is the normal state
for a production cluster and is not a fault.
