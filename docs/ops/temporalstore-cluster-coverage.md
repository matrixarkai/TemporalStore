# Cluster control plane coverage

What `temporalstore-cluster-dashboard.json` watches, which process exports it, and what a blank
panel means.

**Scrape target: the metaserver**, on its own `/metrics` listener. Import this dashboard against the
gateway and every panel comes up empty — which reads as a quiet cluster rather than as a query
aimed at the wrong process.

## Why this dashboard exists

The metaserver is the control plane. It declares 41 metric families and **4 of them were on any
dashboard**: topology version, scheduler queue depth, scheduler executions, and inventory. A
customer running raft or shared storage could watch traffic and storage and had no way to see
whether the cluster agreed with itself.

## Panels

| panel | answers | blank means |
|---|---|---|
| Topology Version And What Each Server Applied | is every server on the current topology | metaserver not scraped |
| Topology Lag | how far the slowest server is behind | no servers registered, or metaserver not scraped |
| Safety Switches | is the cluster in safe mode, is metadata change muted, is the detector paused | metaserver not scraped |
| Convictions | how many resources have been convicted, and how many convictions are held | metaserver not scraped |
| Damage Severity | severity of detected damage, and abnormal resource count | metaserver not scraped |
| Shard Divergence | shards found diverged, and divergence checks skipped | metaserver not scraped |
| Placement Shortfall | replicas the placer could not satisfy | metaserver not scraped |
| Calibration Shortfall | groups and proxies short after calibration, and whether calibration capped | metaserver not scraped |
| Retention Blocked Or Capped | whether retention is refusing to advance | metaserver not scraped |
| Retention Purged | what retention has actually reclaimed | metaserver not scraped |
| Freeze Aging | frozen resources aged out, and whether aging capped | metaserver not scraped |
| Metaserver Requests | control-plane request volume by kind | metaserver not scraped |
| Topology Query Latency And Failures | cost and reliability of topology queries | metaserver not scraped |
| Scheduler Queue And Executions | control-plane backlog and execution results | metaserver not scraped |
| Per Server: Records And Stored Bytes | per-server load, as each server reports it | no servers registered |
| Resource State And Inventory | resource counts by state | metaserver not scraped |
| Proxy Restarts And Attachments | proxies restarting under the control plane | no proxies attached |
| Mutation Apply Failures | control-plane mutations that failed to apply | metaserver not scraped |

## The panel worth reading first

**Topology Version And What Each Server Applied.** The engine's own HELP text for
`temporalstore_meta_server_applied_topology` says to compare it against
`temporalstore_meta_topology_version`. A server whose applied version sits below the current one has
not taken the latest topology, and until it does it is working from a stale view of the cluster.
Nothing watched that comparison before this dashboard existed.

## Why no panel here can be blank for the usual reason

Every expression names a family the engine declares via `# HELP` or `# TYPE`, and the conformance
validator now enforces that across **all** dashboards rather than one. It previously read only
`temporalstore-dashboard.json`, so a panel added to any other file was checked by nothing.

That strict check found one live defect on the way in:
`temporalstore_block_store_band_oldest_age_ms`, one target among five on the page-store panel, is
declared nowhere. The panel rendered with four series and no indication the fifth was impossible —
a missing series is less visible than a blank panel, not more. That target is removed.
