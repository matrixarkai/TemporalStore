#!/usr/bin/env python3
"""Validate Rust Grafana/Prometheus parity evidence against C++ ops families."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "docs" / "ops" / "temporalstore-dashboard.json"
ALERTS = ROOT / "docs" / "ops" / "temporalstore-alerts.yml"
DOC = ROOT / "docs" / "ops" / "temporalstore-grafana-metrics-parity.md"
RUST_SOURCES = [
    ROOT / "crates" / "temporalstore-rust" / "src" / "engine.rs",
    ROOT / "crates" / "temporalstore-rust" / "src" / "raft.rs",
    ROOT / "crates" / "temporalstore-rust" / "src" / "proxy.rs",
    ROOT / "crates" / "temporalstore-rust" / "src" / "ingestion.rs",
    ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "server.rs",
    ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "metaserver.rs",
    ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "ops_scale_readiness_harness.rs",
    ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "matrixark_record_log.rs",
]


METRIC_FAMILIES = {
    "readiness": {
        "dashboard": [
            "temporalstore_production_readiness_ready",
            "temporalstore_production_readiness_blockers",
            "temporalstore_production_readiness_service_blockers",
        ],
        "alerts": [
            "TemporalStoreProductionReadinessBlocked",
            "TemporalStoreProductionReadinessBlockersHigh",
        ],
        "rust": [
            "temporalstore_production_readiness_ready",
            "temporalstore_production_readiness_blockers",
            "temporalstore_production_readiness_service_ready",
            "temporalstore_production_readiness_service_blockers",
        ],
    },
    "raft": {
        "dashboard": [
            "temporalstore_raft_cluster_commit_index",
            "temporalstore_raft_cluster_has_majority",
            "temporalstore_raft_node_lag",
            "temporalstore_raft_node_apply_lag",
            "temporalstore_raft_leader_lease_valid",
        ],
        "alerts": [
            "TemporalStoreRaftMajorityLost",
            "TemporalStoreRaftSlowFollower",
            "TemporalStoreRaftApplyStuck",
        ],
        "rust": [
            "temporalstore_raft_cluster_commit_index",
            "temporalstore_raft_cluster_live_voters",
            "temporalstore_raft_cluster_has_majority",
            "temporalstore_raft_leader_lease_valid",
            "temporalstore_raft_node_commit_index",
            "temporalstore_raft_node_applied_index",
            "temporalstore_raft_node_lag",
            "temporalstore_raft_node_apply_lag",
        ],
    },
    "metaserver_scheduler": {
        "dashboard": [
            "temporalstore_meta_scheduler_queue_depth",
            "temporalstore_meta_scheduler_executions_total",
            "temporalstore_meta_topology_version",
        ],
        "alerts": [
            "TemporalStoreSchedulerBacklogHigh",
            "TemporalStoreSchedulerRetriesHigh",
        ],
        "rust": [
            "temporalstore_meta_requests_total",
            "temporalstore_meta_inventory",
            "temporalstore_meta_topology_version",
            "temporalstore_meta_scheduler_queue_depth",
            "temporalstore_meta_scheduler_executions_total",
        ],
    },
    "proxy_client": {
        "dashboard": [
            "temporalstore_proxy_route_cache_entries",
            "temporalstore_proxy_route_cache_events_total",
            "temporalstore_proxy_backend_events_total",
            "temporalstore_proxy_serving_mode",
            "temporalstore_proxy_drop_percent",
            "temporalstore_proxy_metric_family_parity",
        ],
        "alerts": [
            "TemporalStoreProxyRouteQuarantineHigh",
            "TemporalStoreProxyNotServing",
        ],
        "rust": [
            "temporalstore_proxy_requests_total",
            "temporalstore_proxy_route_cache_entries",
            "temporalstore_proxy_route_cache_events_total",
            "temporalstore_proxy_backend_events_total",
            "temporalstore_proxy_serving_mode",
            "temporalstore_proxy_drop_percent",
            "temporalstore_proxy_metric_family_parity",
        ],
    },
    "storage_cache": {
        "dashboard": [
            "temporalstore_object_manager_objects",
            "temporalstore_object_manager_page_refs",
            "temporalstore_storage_slot_page_refs",
            "temporalstore_storage_slot_bytes",
            "temporalstore_cache_operations_total",
            "temporalstore_block_store_operations_total",
            "temporalstore_block_store_extent_bytes",
        ],
        "alerts": [
            "TemporalStoreStorageCacheBlockers",
            "TemporalStoreBlockStoreReadErrors",
            "TemporalStoreCacheMissPressure",
        ],
        "rust": [
            "temporalstore_object_manager_objects",
            "temporalstore_object_manager_page_refs",
            "temporalstore_object_manager_dirty_slots",
            "temporalstore_storage_slot_page_refs",
            "temporalstore_storage_slot_bytes",
            "temporalstore_cache_operations_total",
            "temporalstore_cache_bytes",
            "temporalstore_block_store_operations_total",
            "temporalstore_block_store_extent_bytes",
        ],
    },
    "data_node": {
        "dashboard": [
            "temporalstore_data_node_runtime_queue_depth",
            "temporalstore_data_node_runtime_background_queue_depth",
            "temporalstore_data_node_dirty_objects",
            "temporalstore_data_node_lifecycle_snapshot_events_total",
        ],
        "alerts": [
            "TemporalStoreDataNodeReadinessBlocked",
            "TemporalStoreDataNodeRuntimeQueueHigh",
            "TemporalStoreLifecycleSnapshotFailures",
        ],
        "rust": [
            "temporalstore_data_node_runtime_jobs_total",
            "temporalstore_data_node_runtime_queue_depth",
            "temporalstore_data_node_runtime_background_queue_depth",
            "temporalstore_data_node_dirty_objects",
            "temporalstore_data_node_lifecycle_snapshot_events_total",
        ],
    },
    "ingestion": {
        "dashboard": [
            "temporalstore_ingestion_kafka_lag",
            "temporalstore_ingestion_kafka_max_lag",
            "temporalstore_ingestion_records_total",
            "temporalstore_ingestion_dead_letters",
            "temporalstore_ingestion_flink_checkpoint_state",
            "temporalstore_ingestion_flink_checkpoints",
        ],
        "alerts": [
            "TemporalStoreIngestionDeadLetters",
        ],
        "rust": [
            "temporalstore_ingestion_records_total",
            "temporalstore_ingestion_kafka_lag",
            "temporalstore_ingestion_kafka_max_lag",
            "temporalstore_ingestion_dead_letters",
            "temporalstore_ingestion_flink_checkpoint_state",
            "temporalstore_ingestion_flink_checkpoints",
        ],
    },
    "secondary_replication": {
        "dashboard": [
            "temporalstore_replica_replay_loop_enabled",
            "temporalstore_replica_replay_loop_events_total",
            "temporalstore_replica_replay_loop_consecutive_failures",
            "temporalstore_replica_replay_loop_next_delay_ms",
        ],
        "alerts": [
            "TemporalStoreReplicaReplayFailures",
        ],
        "rust": [
            "temporalstore_replica_replay_loop_enabled",
            "temporalstore_replica_replay_loop_events_total",
            "temporalstore_replica_replay_loop_consecutive_failures",
            "temporalstore_replica_replay_loop_next_delay_ms",
        ],
    },
    "matrixark_backend": {
        "dashboard": [
            "matrixark_backend_qps",
            "matrixark_backend_commands_total",
            "matrixark_backend_command_latency_ms",
            "matrixark_backend_records_written_total",
            "matrixark_backend_records_read_total",
            "matrixark_backend_ready",
        ],
        "alerts": [
            "MatrixArkBackendNotReady",
            "MatrixArkBackendErrorsHigh",
        ],
        "rust": [
            "matrixark_backend_qps",
            "matrixark_backend_commands_total",
            "matrixark_backend_errors_total",
            "matrixark_backend_timeouts_total",
            "matrixark_backend_command_latency_ms",
            "matrixark_backend_command_latency_ms_bucket",
            "matrixark_backend_records_written_total",
            "matrixark_backend_records_read_total",
            "matrixark_context_records_total",
            "matrixark_backend_audit_buffered_records",
            "matrixark_backend_audit_flush_failures_total",
            "matrixark_backend_ready",
        ],
    },
    "scale_slo": {
        "dashboard": [
            "temporalstore_scale_write_p99_us",
            "temporalstore_scale_read_p99_us",
            "temporalstore_scale_throughput_ops",
            "temporalstore_scale_error_budget_remaining",
        ],
        "alerts": [
            "TemporalStoreScaleSloRegression",
        ],
        "rust": [],
    },
}


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def dashboard_text() -> str:
    data = json.loads(read(DASHBOARD))
    return json.dumps(data, sort_keys=True)


def rust_metric_text() -> str:
    return "\n".join(read(path) for path in RUST_SOURCES if path.exists())


def metric_names(text: str) -> set[str]:
    return set(re.findall(r"(?:temporalstore|matrixark)_[A-Za-z0-9_]+", text))


def main() -> int:
    dash = dashboard_text()
    alerts = read(ALERTS)
    docs = read(DOC) if DOC.exists() else ""
    rust = rust_metric_text()
    dash_names = metric_names(dash)
    alert_names = metric_names(alerts)
    rust_names = metric_names(rust)
    missing: list[str] = []
    family_reports = {}
    for family, requirements in METRIC_FAMILIES.items():
        family_missing = []
        for name in requirements["dashboard"]:
            if name not in dash_names:
                family_missing.append(f"dashboard:{name}")
        for name in requirements["alerts"]:
            if name not in alerts:
                family_missing.append(f"alert:{name}")
        for name in requirements["rust"]:
            if name not in rust_names:
                family_missing.append(f"rust:{name}")
        if family not in docs:
            family_missing.append(f"doc_family:{family}")
        if family_missing:
            missing.extend(f"{family}:{item}" for item in family_missing)
        family_reports[family] = {
            "dashboard_metrics": requirements["dashboard"],
            "alert_rules": requirements["alerts"],
            "rust_metrics": requirements["rust"],
            "ready": not family_missing,
            "missing": family_missing,
        }

    report = {
        "schema": "temporalstore_grafana_metrics_parity_report_v1",
        "dashboard": str(DASHBOARD.relative_to(ROOT)),
        "alerts": str(ALERTS.relative_to(ROOT)),
        "doc": str(DOC.relative_to(ROOT)),
        "families": family_reports,
        "grafana_metrics_parity_ready": not missing,
        "missing": missing,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
