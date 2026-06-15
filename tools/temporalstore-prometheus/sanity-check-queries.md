# Sanity-check PromQL for TemporalStore `/vars`

Use these in Prometheus (`http://localhost:9090`) after 30–60 seconds.

1. Collector + target healthy

```promql
up{job="temporalstore_node_exporter"}
```

2. Textfile collector has fresh data

```promql
time() - node_textfile_mtime_seconds
```

3. TemporalStore `/vars` scrape targets are healthy

```promql
temporalstore_vars_exporter_target_up
```

```promql
temporalstore_service_role_up
```

```promql
sum(temporalstore_vars_exporter_scrape_errors_total)
```

```promql
temporalstore_vars_exporter_target_samples_scraped
```

```promql
temporalstore_vars_exporter_role_samples_scraped
```

4. We can see server metrics and meta metrics separately

```promql
count({__name__=~"bcache2_.*"})
```

```promql
sum by (service_role, source) ({__name__=~"bcache2_server_.*|temporalstore_nodeserver_.*"})
```

```promql
sum by (service_role, source) ({__name__=~"bcache2_metaserver_.*"})
```

```promql
sum by (service_role, source) (temporalstore_vars_exporter_target_samples_scraped{service_role="proxy"})
```

5. Core runtime counters/throughput should move with traffic

```promql
sum by (service_role, source) (increase({__name__=~".*_success.*"}[5m]))
```

```promql
sum by (service_role, source) (increase({__name__=~".*_qps.*"}[1m]))
```

```promql
sum by (iteration) (temporalstore_client_validation_up)
```

6. Client/proxy retry pressure should be visible and failures should stay zero

```promql
sum by (service_role, phase) (temporalstore_client_retry_attempts_total)
```

```promql
sum by (service_role, phase) (temporalstore_client_retry_failures_total)
```

```promql
sum by (service_role, phase) (temporalstore_proxy_retry_attempts_total)
```

```promql
sum by (service_role, phase) (temporalstore_proxy_retry_failures_total)
```

7. Ingestion queue/backpressure visibility from replay gates

```promql
sum by (source) (temporalstore_ingestion_committed_records_total)
```

```promql
sum by (source) (temporalstore_ingestion_retries_total)
```

```promql
sum by (source) (temporalstore_ingestion_dead_letters_total)
```

```promql
max by (source) (temporalstore_ingestion_backpressure_records)
```

```promql
min by (source) (temporalstore_ingestion_validation_up)
```

8. Raft/fault-tolerance gate evidence from the textfile collector

```promql
temporalstore_raft_gate_case_pass
```

```promql
temporalstore_raft_gate_production_ready
```

```promql
temporalstore_raft_gate_production_check_pass
```

```promql
temporalstore_raft_gate_metaserver_failover_ms
```

```promql
temporalstore_raft_gate_data_post_failover_write_read_ms
```

```promql
temporalstore_raft_gate_data_background_failover_active_at_kill
```

```promql
temporalstore_raft_gate_data_background_failover_zero_errors
```

```promql
temporalstore_raft_gate_data_background_failover_elapsed_ms
```

```promql
temporalstore_raft_gate_data_post_failover_after_write_raft_max_apply_lag
```

```promql
temporalstore_raft_gate_data_post_failover_after_write_raft_max_fatal_events
```

```promql
temporalstore_raft_gate_data_secondary_visibility_p99_us
```

```promql
temporalstore_raft_gate_metaserver_membership_metaserver_membership_add_remove_ms
```

```promql
temporalstore_raft_gate_metaserver_membership_membership_nodes_after_add
```

```promql
temporalstore_raft_gate_metaserver_membership_membership_nodes_after_remove
```

```promql
temporalstore_raft_gate_data_membership_after_scale_up_active_replicas
```

```promql
temporalstore_raft_gate_data_membership_server3_after_scale_up_voter_count
```

```promql
temporalstore_raft_gate_data_membership_server3_after_scale_up_fatal_event_count
```

```promql
temporalstore_raft_gate_data_membership_after_scale_up_raft_max_apply_lag
```

```promql
temporalstore_raft_gate_data_membership_after_scale_down_raft_max_apply_lag
```

```promql
temporalstore_raft_gate_data_membership_after_drop_raft_max_apply_lag
```

```promql
temporalstore_raft_gate_data_membership_scale_down_server3_active_partitions
```

```promql
temporalstore_raft_gate_data_membership_drop_server3_active_partitions
```

```promql
temporalstore_raft_gate_snapshot_snapshot_file_count_after_restart
```

8. Watch hot storage/repl/cmd classes

```promql
sum by (source) ({__name__=~".*(index|slot|page|replicator|gc|oplogger).*"})
```

If these series stay empty, re-check:

- `VARS_TARGETS` points to running TemporalStore ports
- Local cluster is actually started (default ports `18001/18000`)
- Exporter container can reach `host.docker.internal`
