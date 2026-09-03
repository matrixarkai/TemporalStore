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


## Alerts removed because they could never fire

Two rules watched metrics that nothing declares. A rule on an unemitted metric is worse than no
rule: the panel is blank, the alert is silent, and silence reads as health. They are removed rather
than left in place, and recorded here so the loss of intent is visible instead of implied.

| removed rule | watched | why it could not fire |
|---|---|---|
| `TemporalStoreReplicaReplayFailures` | `temporalstore_replica_replay_loop_consecutive_failures` | No subsystem publishes it. The raft follower pipeline keeps a `consecutive_failures` counter, but that counts append-entries send failures to a peer -- a different subsystem from the shared-store secondary replay this rule described, so exporting it under this name would have made the alert fire on the wrong thing. |
| `TemporalStoreScaleSloRegression` | `temporalstore_scale_write_p99_us`, `temporalstore_scale_read_p99_us` | Neither is declared. Both names appear in `ops_scale_readiness_harness.rs`, but only inside a hardcoded list of families it EXPECTS the dashboards and alerts to mention -- a statement of intent, not an emission. |

**What is no longer watched:** secondary replica replay health, and the scale harness p99 SLO. Both
were already unwatched; removing the rules only stops them claiming otherwise. Restoring either
means emitting the metric first, then re-adding the rule.

**The guard that now prevents this:** `validate_grafana_metrics_conformance.py` fails when a rule's
metrics are undeclared. "Declared" means a `# HELP` or `# TYPE` line -- deliberately narrower than
"the name appears in the Rust source", which is what let these two hide, since the expectations
list satisfies the looser test. The check asserts its own extent first: it refuses to pass on an
empty declaration scan, because every comparison below it would then trivially succeed.


## Panels removed because they were blank on every deployment

The same two families owned two of the twelve dashboard panels, and every metric behind them is
undeclared, so both rendered empty wherever this dashboard was loaded.

| removed panel | queried | state |
|---|---|---|
| Secondary Replication Replay | `temporalstore_replica_replay_loop_consecutive_failures`, `_enabled`, `_events_total`, `_next_delay_ms` | none declared |
| Scale SLO | `temporalstore_scale_write_p99_us`, `_read_p99_us`, `_throughput_ops`, `_error_budget_remaining` | none declared |

### How both got through

Each family's spec entry set `"rust": []`. The validator reads that as "this family requires no
engine-side metric", so it confirmed the panels existed and the alert rules existed, and never asked
whether anything produced the numbers. Both families were then reported ready.

`check_families_state_their_emission` now fails on an empty `rust` list. An empty list is not the
same as no requirement: if a family genuinely has no engine-side metric, it does not belong in a
spec whose entire purpose is tying a panel to an emission.

The scale-harness readiness check carried the same names and additionally verified them against a
document path that no longer exists, so that check returned false unconditionally. Its lists and
its path are corrected here.


## What the production readiness gate does and does not mean

`temporalstore_production_readiness_ready` is the dashboard's headline stat and carries two alerts.
It is computed from **build and configuration state only**.

Measured over the readiness surface: 25 functions and 1,922 lines reached from
`production_readiness_report()`, with **zero runtime reads** -- no atomics, no filesystem, no
clock, no network. The single environment input is raft security configuration. Nine of the
sub-reports are built from constants; 146 hardcoded `true` values sit among them.

That is a legitimate thing to compute -- it answers "was this binary built and configured with
every capability area present". It is not a health signal, and its name, its dashboard position and
its alert wording all read as one. **A green gate is not evidence the cluster is serving.** It
reports the same value for a healthy cluster and for one with every node down.

The panel title, the metric's HELP text and both alert descriptions now say so.
`test_matrixark_readiness_gate_is_build_state.py` pins it: if a runtime read is ever added to that
surface, the test fails and asks for the wording to be revisited, so the description cannot quietly
become wrong in the other direction.


## Storage maintenance, and the one thing these panels cannot tell you

Ten `temporalstore_storage_manager_*` families are declared and, until now, none appeared on any
dashboard. They describe the five phases that decide whether a store grows without bound:
`reclaim_wal`, `index_gc`, `expire`, `evict`, `compact`.

That matters more than a normal coverage hole, because **nothing in the node schedules the cycle**.
`start_storage_manager_scheduler` exists and has exactly one caller, a test; the cycle is reachable
only at `POST /server/storage/manager/cycle`. On a long-lived deployment nobody posts to, none of
those phases has ever run.

### What the alerts can and cannot see

Every one of these metrics is a gauge describing the LAST cycle, so:

| condition | detectable | how |
|---|---|---|
| never ran | yes | `absent(...phase_applied)` -- the metric does not exist until a cycle reports |
| a phase failed | yes | `...phase_errors > 0` |
| a phase is switched off | yes | `...phase_enabled == 0` |
| **ran once, then stopped** | **no** | nothing records WHEN the last cycle was |

The last row is the honest gap. A gauge of the last cycle's results looks identical whether that
cycle was a minute ago or a month ago, so a node that ran maintenance once at startup and never
again is indistinguishable from a healthy one. Closing it needs a timestamp -- a
`..._last_run_seconds` gauge, or a counter that increases per cycle so `increase()` over a window
means something. Both are engine changes and neither is made here.

`absent()` rather than `increase() == 0` for the never-ran case is deliberate: `increase()` over a
metric that does not exist returns no series, so a rule written that way would never fire on the
condition it names. That is the exact defect removed from this file earlier -- an alert that reads
as coverage and cannot fire.
