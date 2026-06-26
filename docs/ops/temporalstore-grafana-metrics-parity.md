# TemporalStore Grafana Metrics Parity

This page defines the Rust Grafana/Prometheus parity contract against the C++ TemporalStore ops surface. Rust stays Rust-native: HTTP/JSON, RESP, tonic, Rust page/log formats, and OpenRaft are the production path. Metrics parity means the same operational questions can be answered from Grafana and alerts, not that Rust emits byte-for-byte identical C++ metric names.

Executable validation:

```bash
python3 tools/validate_grafana_metrics_parity.py
```

The validator checks:

- `docs/ops/temporalstore-dashboard.json` contains the required Grafana target expressions.
- `docs/ops/temporalstore-alerts.yml` contains the required alert names.
- Rust emitters expose the required Prometheus metric families, except scale SLO fields that are emitted by scale harness evidence.

## Parity Families

### readiness

C++ parity question: is production readiness blocked, and which service/area owns the blocker?

Rust metrics:

- `temporalstore_production_readiness_ready`
- `temporalstore_production_readiness_blockers`
- `temporalstore_production_readiness_service_ready`
- `temporalstore_production_readiness_service_blockers`

Grafana panels cover global readiness, area blockers, and service blockers. Alerts cover blocked readiness and high blocker counts.

### raft

C++/ByteRaft parity question: is the group healthy, are followers lagging, is apply stuck, and is the leader lease safe?

Rust metrics:

- `temporalstore_raft_cluster_commit_index`
- `temporalstore_raft_cluster_live_voters`
- `temporalstore_raft_cluster_has_majority`
- `temporalstore_raft_leader_lease_valid`
- `temporalstore_raft_node_commit_index`
- `temporalstore_raft_node_applied_index`
- `temporalstore_raft_node_lag`
- `temporalstore_raft_node_apply_lag`

Grafana panels cover commit/apply lag, majority, and lease validity. Alerts cover majority loss, split-brain risk, slow followers, and stuck apply.

### metaserver_scheduler

C++ parity question: is the control-plane scheduler backlogged, retrying, or applying topology changes?

Rust metrics:

- `temporalstore_meta_requests_total`
- `temporalstore_meta_inventory`
- `temporalstore_meta_topology_version`
- `temporalstore_meta_scheduler_queue_depth`
- `temporalstore_meta_scheduler_executions_total`

Grafana panels cover scheduler queue depth, executions by result, topology version, and inventory. Alerts cover high backlog and retry/failure growth.

### proxy_client

C++ parity question: are routes fresh, are backends quarantined, and is admission policy active?

Rust metrics:

- `temporalstore_proxy_requests_total`
- `temporalstore_proxy_route_cache_entries`
- `temporalstore_proxy_route_cache_events_total`
- `temporalstore_proxy_backend_events_total`
- `temporalstore_proxy_serving_mode`
- `temporalstore_proxy_drop_percent`
- `temporalstore_proxy_metric_family_parity`
- `temporalstore_proxy_service_registry_state`
- `temporalstore_proxy_service_registry_events_total`

Grafana panels cover route cache, backend failures, serving mode, drop percent, service-registry
freshness, and explicit C++-surface to Rust-family parity. Alerts cover continuous backend
failures and degraded/not-serving policy.

Proxy C++ metric/surface mapping:

| C++ surface | Rust Prometheus family | Grafana panel |
| --- | --- | --- |
| `common::metrics::CounterHolder` proxy command/admission counters | `temporalstore_proxy_requests_total{kind}` | Proxy Requests And Admission |
| proxy route cache hit/miss/refresh counters | `temporalstore_proxy_route_cache_events_total{kind}` | Proxy Route Cache |
| proxy current route cache size | `temporalstore_proxy_route_cache_entries` | Proxy Route Cache |
| proxy backend/metaserver errors | `temporalstore_proxy_backend_events_total{kind}` | Proxy Backend Health |
| proxy serving mode and desired policy | `temporalstore_proxy_serving_mode{mode}` | Proxy Serving Policy |
| proxy deterministic drop percent | `temporalstore_proxy_drop_percent` | Proxy Serving Policy |
| heartbeat service registration freshness | `temporalstore_proxy_service_registry_state{state}` | Proxy Service Registry |
| heartbeat registration and heartbeat outcomes | `temporalstore_proxy_service_registry_events_total{kind}` | Proxy Service Registry |

The Rust proxy also exposes `/proxy/metrics_parity` and `/ProxyService/GetMetricsParity` so
readiness tooling can inspect the same mapping without scraping text metrics.

### storage_cache

C++ storage parity question: are object/page/slot ownership, page-store health, and cache pressure visible?

Rust metrics:

- `temporalstore_object_manager_objects`
- `temporalstore_object_manager_page_refs`
- `temporalstore_object_manager_dirty_slots`
- `temporalstore_storage_slot_page_refs`
- `temporalstore_storage_slot_bytes`
- `temporalstore_cache_operations_total`
- `temporalstore_cache_bytes`
- `temporalstore_page_store_operations_total`
- `temporalstore_page_store_zone_bytes`

Grafana panels cover object manager ownership, slot refs/bytes, cache operations/bytes, page-store operations, and stale-zone bytes. Alerts cover storage/cache readiness blockers, page-store read/corruption errors, and sustained cache miss pressure.

### data_node

C++ parity question: are node lifecycle persistence and runtime queues healthy during load/reload/unload/restart?

Rust metrics:

- `temporalstore_data_node_runtime_jobs_total`
- `temporalstore_data_node_runtime_queue_depth`
- `temporalstore_data_node_runtime_background_queue_depth`
- `temporalstore_data_node_dirty_objects`
- `temporalstore_data_node_lifecycle_snapshot_events_total`

Grafana panels cover foreground/background runtime queue depth, dirty objects, and lifecycle snapshot events. Alerts cover blocked data-node readiness, queue pressure, and lifecycle snapshot failures.

### ingestion

C++ parity question: are Kafka/Flink ingestion lag, checkpoint state, accepted records, and dead letters visible?

Rust metrics:

- `temporalstore_ingestion_records_total`
- `temporalstore_ingestion_kafka_lag`
- `temporalstore_ingestion_kafka_max_lag`
- `temporalstore_ingestion_dead_letters`
- `temporalstore_ingestion_flink_checkpoint_state`
- `temporalstore_ingestion_flink_checkpoints`

Grafana panels cover lag, records by outcome, dead letters, and Flink checkpoint states. Alerts cover any persisted dead letters or dead-letter outcomes.

### secondary_replication

C++ parity question: are secondary replay loops enabled, failing, or delayed enough to affect reads and retention?

Rust metrics:

- `temporalstore_replica_replay_loop_enabled`
- `temporalstore_replica_replay_loop_events_total`
- `temporalstore_replica_replay_loop_consecutive_failures`
- `temporalstore_replica_replay_loop_next_delay_ms`

Grafana panels cover replay enablement, events, consecutive failures, and next delay. Alerts cover consecutive replay failures.

### scale_slo

C++ parity question: does the multi-node scale harness report p99 latency, throughput, error budget, and replay/failover evidence?

Scale SLO metrics are harness evidence fields, not normal process `/metrics` endpoint families:

- `temporalstore_scale_write_p99_us`
- `temporalstore_scale_read_p99_us`
- `temporalstore_scale_throughput_ops`
- `temporalstore_scale_error_budget_remaining`

Grafana panels and alerts reserve these names for Docker/AWS scale report ingestion. The ops scale readiness harness remains responsible for producing and validating the actual SLO evidence.


### matrixark_backend

C++/Rust MatrixArk parity question: are the storage-facing backends serving the same context pipeline SLOs for traffic, latency, records, audit buffering, and readiness?

Rust metrics:

- `matrixark_backend_qps`
- `matrixark_backend_commands_total`
- `matrixark_backend_errors_total`
- `matrixark_backend_timeouts_total`
- `matrixark_backend_command_latency_ms`
- `matrixark_backend_command_latency_ms_bucket`
- `matrixark_backend_records_written_total`
- `matrixark_backend_records_read_total`
- `matrixark_context_records_total`
- `matrixark_backend_audit_buffered_records`
- `matrixark_backend_audit_flush_failures_total`
- `matrixark_backend_ready`

C++ metrics use the same backend-normalized `matrixark_backend_*` families through the C++ TemporalStore direct adapter, so Grafana can compare `backend="cpp"` and `backend="rust"` without panel forks.
