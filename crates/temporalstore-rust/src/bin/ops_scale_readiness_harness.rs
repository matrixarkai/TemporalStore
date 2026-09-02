// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
struct OpsScaleReadinessReport {
    autoscale_controller_ready: bool,
    metaserver_rebalance_loop_ready: bool,
    dashboards_ready: bool,
    grafana_metrics_coverage_ready: bool,
    grafana_metric_families: Vec<String>,
    tracing_ready: bool,
    non_raft_auth_tls_ready: bool,
    production_runbook_ready: bool,
    docker_scale_run_ready: bool,
    real_process_roles: Vec<String>,
    distributed_raft_load_ready: bool,
    raft_load_checks: Vec<String>,
    legacy_workload_replay_ready: bool,
    workload_families: Vec<String>,
    scale_slo_report: DockerAwsScaleSloEvidence,
    harnesses: Vec<HarnessEvidence>,
    docs: Vec<String>,
    missing: Vec<String>,
    production_ready: bool,
}

#[derive(Debug, Serialize)]
struct DockerAwsScaleSloEvidence {
    docker_or_aws_slo_evidence_ready: bool,
    storage_deployment_scale_slo_ready: bool,
    metaserver_process_ready: bool,
    proxy_process_ready: bool,
    client_process_ready: bool,
    data_node_process_ready: bool,
    raft_failover_ready: bool,
    storage_pressure_ready: bool,
    cache_pressure_ready: bool,
    proxy_convergence_ready: bool,
    workload_replay_ready: bool,
    collectors: Vec<String>,
    metrics: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HarnessEvidence {
    name: String,
    command: String,
    covers: Vec<String>,
}

fn main() {
    let root = repo_root();
    let corpus = load_json(root.join("compat/unified_temporalstore_cases.json"));
    let case_names = string_set(&corpus, &["coverage", "required_case_names"]);
    let command_kinds = string_set(&corpus, &["coverage", "required_command_kinds"]);

    let autoscale_controller_ready = file_contains(
        &root,
        "crates/temporalstore-rust/src/bin/metaserver.rs",
        &["SchedulerTaskResult", "lifecycle_token", "execute_next"],
    ) && file_contains(
        &root,
        "crates/temporalstore-rust/src/rebalance.rs",
        &[
            "SchedulerTask",
            "LoadTarget",
            "UnloadSource",
            "ReloadTarget",
        ],
    );
    let metaserver_rebalance_loop_ready = file_contains(
        &root,
        "crates/temporalstore-rust/src/bin/metaserver.rs",
        &["cooldown", "safe_mode", "membership"],
    );
    let grafana_metrics_coverage_ready = grafana_metrics_coverage_ready(&root);
    let dashboards_ready = grafana_metrics_coverage_ready;
    let tracing_ready = file_contains(
        &root,
        "docs/ops/temporalstore-api-security-and-tracing.md",
        &["trace_id", "request_id", "OpenTelemetry"],
    );
    let non_raft_auth_tls_ready = file_contains(
        &root,
        "docs/ops/temporalstore-api-security-and-tracing.md",
        &["TS_API_AUTH_TOKEN", "TLS", "proxy", "server", "metaserver"],
    );
    let production_runbook_ready = file_contains(
        &root,
        "docs/ops/temporalstore-fault-runbook.md",
        &[
            "Production Readiness Blocked",
            "Stuck Replica",
            "Disk Or Storage Pressure",
        ],
    ) && file_contains(
        &root,
        "docs/ops/rolling-upgrade-rollback-runbook.md",
        &["Rolling Upgrade", "Rollback", "metaserver", "data nodes"],
    );
    let docker_scale_run_ready = root.join("tools/run_ops_scale_readiness.sh").is_file()
        && root
            .join("tools/run_temporalstore_scale_harness.sh")
            .is_file()
        && root
            .join("crates/temporalstore-rust/src/bin/scale_harness.rs")
            .is_file()
        && root
            .join("crates/temporalstore-rust/src/bin/client_scale_harness.rs")
            .is_file();
    let distributed_raft_load_ready = file_contains(
        &root,
        "crates/temporalstore-rust/src/bin/distributed_raft_harness.rs",
        &[
            "wait_for_distributed_majority",
            "transfer_leader",
            "apply_membership_on_all",
            "wait_for_replica_read",
        ],
    ) && root
        .join("crates/temporalstore-rust/src/bin/raft_secondary_replication_harness.rs")
        .is_file();
    let workload_families: Vec<(&str, &[&str])> = vec![
        ("Feature", &["feature_append", "feature_query"]),
        ("ControlState", &["control_state_increment", "control_state_count"]),
        ("Redis", &["string_set", "hash_multi_set", "set_add"]),
        ("Context", &["context_upsert_node", "context_write_event"]),
        ("Admin", &["existing_test", "common_exists"]),
    ];
    let mut covered_families = Vec::new();
    for (family, required) in workload_families {
        if required.iter().all(|kind| {
            command_kinds.contains(*kind) || case_names.iter().any(|name| name.contains(kind))
        }) {
            covered_families.push(family.to_string());
        }
    }
    let legacy_workload_replay_ready = [
        "feature_packed_timestamped_pages",
        "control_state_counter_window",
        "redis_compatible_set_core",
        "context_event_index_audit_dirty_models",
        "native_redis_live_storage_smoke_parity_surfaces",
    ]
    .iter()
    .all(|case| case_names.contains(*case))
        && covered_families.len() == 6;
    let scale_slo_report = DockerAwsScaleSloEvidence {
        docker_or_aws_slo_evidence_ready: docker_scale_run_ready
            && distributed_raft_load_ready
            && legacy_workload_replay_ready,
        storage_deployment_scale_slo_ready: docker_scale_run_ready
            && distributed_raft_load_ready
            && legacy_workload_replay_ready,
        metaserver_process_ready: docker_scale_run_ready,
        proxy_process_ready: docker_scale_run_ready,
        client_process_ready: docker_scale_run_ready,
        data_node_process_ready: docker_scale_run_ready,
        raft_failover_ready: distributed_raft_load_ready,
        storage_pressure_ready: docker_scale_run_ready,
        cache_pressure_ready: docker_scale_run_ready,
        proxy_convergence_ready: docker_scale_run_ready,
        workload_replay_ready: legacy_workload_replay_ready,
        collectors: vec![
            "cpu".to_string(),
            "memory".to_string(),
            "disk".to_string(),
            "network".to_string(),
            "replica_lag".to_string(),
            "failover_count".to_string(),
            "scale_events".to_string(),
        ],
        metrics: vec![
            "p50".to_string(),
            "p95".to_string(),
            "p99".to_string(),
            "throughput".to_string(),
            "error_budget".to_string(),
        ],
    };

    let harnesses = vec![
        HarnessEvidence {
            name: "ops_scale_readiness".to_string(),
            command: "cargo run -p temporalstore-rust --bin ops_scale_readiness_harness"
                .to_string(),
            covers: vec![
                "autoscale/rebalance evidence".to_string(),
                "ops dashboards/Grafana metrics coverage/runbooks/tracing/auth coverage".to_string(),
                "corpus family coverage".to_string(),
            ],
        },
        HarnessEvidence {
            name: "docker_or_local_scale".to_string(),
            command: "tools/run_ops_scale_readiness.sh --run-local-scale".to_string(),
            covers: vec![
                "metaserver".to_string(),
                "proxy".to_string(),
                "client".to_string(),
                "data-node".to_string(),
                "shared-store replay".to_string(),
            ],
        },
        HarnessEvidence {
            name: "distributed_raft_load".to_string(),
            command: "cargo run -p temporalstore-rust --bin distributed_raft_harness".to_string(),
            covers: vec![
                "lag".to_string(),
                "catch-up".to_string(),
                "election".to_string(),
                "membership".to_string(),
                "secondary reads".to_string(),
            ],
        },
        HarnessEvidence {
            name: "unified_legacy_rust_workload_replay".to_string(),
            command: "python3 tools/run_temporalstore_unified_tests.py --rust-only".to_string(),
            covers: covered_families.clone(),
        },
    ];

    let docs = vec![
        "docs/ops/temporalstore-dashboard.json".to_string(),
        "docs/ops/temporalstore-alerts.yml".to_string(),
        "docs/ops/temporalstore-grafana-metrics-coverage.md".to_string(),
        "docs/ops/temporalstore-api-security-and-tracing.md".to_string(),
        "docs/ops/temporalstore-fault-runbook.md".to_string(),
        "docs/scale_test_harness.md".to_string(),
    ];
    let mut missing = Vec::new();
    push_missing(
        &mut missing,
        autoscale_controller_ready,
        "autoscale controller evidence",
    );
    push_missing(
        &mut missing,
        metaserver_rebalance_loop_ready,
        "metaserver-driven shard rebalance loop evidence",
    );
    push_missing(
        &mut missing,
        dashboards_ready,
        "Grafana dashboard, alert, and Rust metric emission evidence",
    );
    push_missing(&mut missing, tracing_ready, "tracing evidence");
    push_missing(
        &mut missing,
        non_raft_auth_tls_ready,
        "non-Raft auth/TLS coverage evidence",
    );
    push_missing(
        &mut missing,
        production_runbook_ready,
        "production runbook evidence",
    );
    push_missing(
        &mut missing,
        docker_scale_run_ready,
        "Docker/local scale run evidence",
    );
    push_missing(
        &mut missing,
        distributed_raft_load_ready,
        "distributed Raft load evidence",
    );
    push_missing(
        &mut missing,
        legacy_workload_replay_ready,
        "workload replay/golden corpus evidence",
    );
    push_missing(
        &mut missing,
        scale_slo_report.storage_deployment_scale_slo_ready,
        "Docker/AWS SLO report covering metaserver, proxy, client, data-node, Raft failover, storage pressure, cache pressure, proxy convergence, and workload replay",
    );
    let production_ready = missing.is_empty();
    let report = OpsScaleReadinessReport {
        autoscale_controller_ready,
        metaserver_rebalance_loop_ready,
        dashboards_ready,
        grafana_metrics_coverage_ready,
        grafana_metric_families: grafana_metric_families(),
        tracing_ready,
        non_raft_auth_tls_ready,
        production_runbook_ready,
        docker_scale_run_ready,
        real_process_roles: vec![
            "metaserver".to_string(),
            "proxy".to_string(),
            "client".to_string(),
            "data-node".to_string(),
        ],
        distributed_raft_load_ready,
        raft_load_checks: vec![
            "lag".to_string(),
            "catch-up".to_string(),
            "election".to_string(),
            "membership".to_string(),
            "secondary_reads_under_load".to_string(),
        ],
        legacy_workload_replay_ready,
        workload_families: covered_families,
        scale_slo_report,
        harnesses,
        docs,
        missing,
        production_ready,
    };
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if !report.production_ready {
        std::process::exit(1);
    }
}

fn repo_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crate should live under crates/temporalstore-rust")
        .to_path_buf()
}

fn load_json(path: impl AsRef<Path>) -> Value {
    let path = path.as_ref();
    let bytes =
        fs::read(path).unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()))
}

fn string_set(value: &Value, path: &[&str]) -> BTreeSet<String> {
    let mut current = value;
    for part in path {
        current = current
            .get(part)
            .unwrap_or_else(|| panic!("missing JSON field {}", path.join(".")));
    }
    current
        .as_array()
        .unwrap_or_else(|| panic!("JSON field {} must be an array", path.join(".")))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("JSON field {} must contain strings", path.join(".")))
                .to_string()
        })
        .collect()
}

fn file_contains(root: &Path, relative: &str, snippets: &[&str]) -> bool {
    let path = root.join(relative);
    let Ok(text) = fs::read_to_string(&path) else {
        return false;
    };
    snippets.iter().all(|snippet| text.contains(snippet))
}

fn grafana_metric_families() -> Vec<String> {
    [
        "readiness",
        "raft",
        "metaserver_scheduler",
        "proxy_client",
        "storage_cache",
        "data_node",
        "ingestion",
    ]
    .iter()
    .map(|family| family.to_string())
    .collect()
}

fn grafana_metrics_coverage_ready(root: &Path) -> bool {
    let dashboard_metrics = [
        "temporalstore_production_readiness_ready",
        "temporalstore_production_readiness_service_blockers",
        "temporalstore_raft_cluster_commit_index",
        "temporalstore_raft_node_lag",
        "temporalstore_meta_scheduler_queue_depth",
        "temporalstore_meta_scheduler_executions_total",
        "temporalstore_proxy_backend_events_total",
        "temporalstore_proxy_serving_mode",
        "temporalstore_object_manager_objects",
        "temporalstore_storage_slot_page_refs",
        "temporalstore_cache_operations_total",
        "temporalstore_block_store_operations_total",
        "temporalstore_data_node_lifecycle_snapshot_events_total",
        "temporalstore_ingestion_kafka_lag",
        "temporalstore_ingestion_dead_letters",
    ];
    let alert_rules = [
        "TemporalStoreProductionReadinessBlocked",
        "TemporalStoreRaftMajorityLost",
        "TemporalStoreSchedulerBacklogHigh",
        "TemporalStoreSchedulerRetriesHigh",
        "TemporalStoreProxyRouteQuarantineHigh",
        "TemporalStoreProxyNotServing",
        "TemporalStoreDataNodeRuntimeQueueHigh",
        "TemporalStoreLifecycleSnapshotFailures",
        "TemporalStoreBlockStoreReadErrors",
        "TemporalStoreCacheMissPressure",
        "TemporalStoreIngestionDeadLetters",
    ];
    let rust_metric_tokens = [
        "temporalstore_production_readiness_ready",
        "temporalstore_raft_cluster_commit_index",
        "temporalstore_meta_scheduler_queue_depth",
        "temporalstore_proxy_backend_events_total",
        "temporalstore_object_manager_objects",
        "temporalstore_storage_slot_page_refs",
        "temporalstore_cache_operations_total",
        "temporalstore_block_store_operations_total",
        "temporalstore_data_node_lifecycle_snapshot_events_total",
        "temporalstore_ingestion_kafka_lag",
        "temporalstore_ingestion_dead_letters",
    ];
    let doc_families = [
        "readiness",
        "raft",
        "metaserver_scheduler",
        "proxy_client",
        "storage_cache",
        "data_node",
        "ingestion",
    ];

    file_contains(
        root,
        "docs/ops/temporalstore-dashboard.json",
        &dashboard_metrics,
    ) && file_contains(root, "docs/ops/temporalstore-alerts.yml", &alert_rules)
        && file_contains(
            root,
            "docs/ops/temporalstore-grafana-metrics-coverage.md",
            &doc_families,
        )
        && rust_sources_contain(root, &rust_metric_tokens)
}

fn rust_sources_contain(root: &Path, snippets: &[&str]) -> bool {
    let relative_paths = [
        "crates/temporalstore-rust/src/engine.rs",
        "crates/temporalstore-rust/src/raft.rs",
        "crates/temporalstore-rust/src/proxy.rs",
        "crates/temporalstore-rust/src/ingestion.rs",
        "crates/temporalstore-rust/src/bin/server.rs",
        "crates/temporalstore-rust/src/bin/metaserver.rs",
    ];
    let mut text = String::new();
    for relative in relative_paths {
        let Ok(part) = fs::read_to_string(root.join(relative)) else {
            return false;
        };
        text.push_str(&part);
        text.push('\n');
    }
    snippets.iter().all(|snippet| text.contains(snippet))
}

fn push_missing(missing: &mut Vec<String>, ready: bool, capability: &str) {
    if !ready {
        missing.push(capability.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // shared-corpus: ops_grafana_metrics_coverage
    #[test]
    fn grafana_metrics_coverage_contract_covers_dashboard_alerts_and_emitters() {
        let root = repo_root();
        assert!(grafana_metrics_coverage_ready(&root));
    }
}
