#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate Rust Grafana/Prometheus parity evidence against ops families."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "docs" / "ops" / "temporalstore-dashboard.json"
ALERTS = ROOT / "docs" / "ops" / "temporalstore-alerts.yml"
# The coverage doc this is checked against. The previous path named a file that has never
# existed, and `DOC.exists()` turned that into ten separate "family is undocumented" failures
# with one root cause -- a missing file reported as a missing description, ten times over.
DOC = ROOT / "docs" / "ops" / "temporalstore-grafana-metrics-coverage.md"
# Files known to emit metrics when this discovery was written. Kept as a FLOOR: if the walk below
# stops returning one of these, something has moved and the scan has silently narrowed, which is the
# failure this whole guard exists to prevent.
RUST_SOURCE_FLOOR = (
    "engine.rs",
    "raft.rs",
    "proxy.rs",
    "ingestion.rs",
    "bin/server.rs",
    "bin/metaserver.rs",
    "bin/ops_scale_readiness_harness.rs",
    "bin/matrixark_rust_proxy.rs",
    "bin/matrixark_rust_direct_sdk.rs",
    # NOT under bin/: the hand-maintained list said bin/matrixark_rust_proxy_impl.rs, which does
    # not exist, and the loader skipped it with `if path.exists()` -- so a file with 33 series was
    # silently absent from a scan that reported itself complete.
    "matrixark_rust_proxy_impl.rs",
)

RUST_SRC_ROOT = ROOT / "crates" / "temporalstore-rust" / "src"


def _discover_rust_sources() -> list:
    """Every non-test Rust file under the crate, not a hand-maintained list.

    The list this replaces named ten files while twenty emit Prometheus series, so the validator
    reported conformance over 15% of its subject. A file added later -- or a metric moved into a
    submodule, which is how `bin/server/metrics.rs` came to hold 26 of them -- would never be seen.

    Test modules are excluded: a series named only inside a `#[cfg(test)]` fixture is not something
    a deployment emits, and counting it would let a panel query a series that exists only in tests.
    """
    out = []
    for path in sorted(RUST_SRC_ROOT.rglob("*.rs")):
        parts = path.relative_to(RUST_SRC_ROOT).parts
        if "tests" in parts or path.name.startswith("test_"):
            continue
        out.append(path)
    return out


RUST_SOURCES = _discover_rust_sources()


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
        ],
        "alerts": [
        ],
        "rust": [
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


# Queried by a panel or an alert and emitted by nothing, so that panel is blank on every
# deployment. Tracked here rather than left inside a mass failure, because a validator that always
# fails is one nobody runs -- which is how this one came to be red and unnoticed in the first place.
# Each entry is either a panel to remove or a metric to add; neither is a decision this script makes.
KNOWN_UNEMITTED = {
    # Its siblings (`temporalstore_block_store_operations_total`, the slot gauges) are emitted; this
    # one never was. Documented in docs/ops/temporalstore-grafana-metrics-coverage.md.
    "storage_cache:rust:temporalstore_block_store_extent_bytes",
}


def check_scan_extent() -> list:
    """Names from the floor that discovery no longer returns.

    Reported as a failure rather than a warning. A guard whose reach shrinks reports success over
    less and less, and the report reads identically either way.
    """
    found = {str(p.relative_to(RUST_SRC_ROOT)).replace("\\", "/") for p in RUST_SOURCES}
    return [name for name in RUST_SOURCE_FLOOR if name not in found]


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
    # A known gap stays in the report -- it is not hidden -- but does not fail the run. A gap that
    # gets FIXED does fail, so the list cannot go stale the way the one it replaces did.
    unexpected = [m for m in missing if m not in KNOWN_UNEMITTED]
    fixed_but_listed = sorted(KNOWN_UNEMITTED - set(missing))
    if fixed_but_listed:
        print("These are listed as known-unemitted but now resolve: %s. Remove them from "
              "KNOWN_UNEMITTED." % ", ".join(fixed_but_listed), file=sys.stderr)
    lost = check_scan_extent()
    if lost:
        print("SCAN NARROWED: these files used to be scanned and are no longer discovered: %s"
              % ", ".join(lost), file=sys.stderr)
    return 0 if (not unexpected and not lost and not fixed_but_listed) else 1


if __name__ == "__main__":
    sys.exit(main())
