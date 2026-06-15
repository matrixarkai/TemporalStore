#!/usr/bin/env python3
"""Summarize local raft gate results as JSON and Prometheus text."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def parse_key_values(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in read_text(path).splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            out[key] = value
    return out


def parse_csv_rows(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(encoding="utf-8", errors="replace", newline="") as fh:
            return [dict(row) for row in csv.DictReader(fh)]
    except OSError:
        return []


def parse_repeated_csv_sections(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    header: list[str] | None = None
    for raw in read_text(path).splitlines():
        line = raw.strip()
        if not line:
            continue
        cols = next(csv.reader([line]))
        if not cols:
            continue
        if cols[0] in {"config", "phase", "background", "system"}:
            header = cols
            continue
        if header is None:
            continue
        row = {header[i]: cols[i] for i in range(min(len(header), len(cols)))}
        rows.append(row)
    return rows


def parse_topology_csv(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for raw in read_text(path).splitlines():
        line = raw.strip()
        if not line:
            continue
        cols = next(csv.reader([line]))
        if len(cols) < 6:
            continue
        rows.append(
            {
                "partition_id": cols[0],
                "server_port": cols[1],
                "role": cols[2],
                "state": cols[3],
                "membership_state": cols[4],
                "primary_state": cols[5],
            }
        )
    return rows


def to_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def labels_to_text(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    parts = []
    for key, value in sorted(labels.items()):
        safe = str(value).replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'{key}="{safe}"')
    return "{" + ",".join(parts) + "}"


def prom_line(name: str, value: float | int, labels: dict[str, str] | None = None) -> str:
    return f"{name}{labels_to_text(labels or {})} {value}"


def extract_data_failover(case_dir: Path) -> dict[str, Any]:
    text = read_text(case_dir / "stdout.log")
    match = re.search(
        r"PASS primary-down failover write/read succeeded after (\d+) attempts, (\d+) ms",
        text,
    )
    result: dict[str, Any] = {}
    if match:
        result["post_failover_attempts"] = int(match.group(1))
        result["post_failover_write_read_ms"] = int(match.group(2))

    for key in (
        "background_failover_enabled",
        "background_failover_active_at_kill",
        "background_failover_exit_code",
        "background_failover_errors",
        "background_failover_zero_errors",
        "background_failover_elapsed_ms",
    ):
        match = re.search(rf"^{re.escape(key)}=([0-9]+)", text, re.MULTILINE)
        if match:
            result[key] = int(match.group(1))

    partition_path = case_dir / "cluster" / "post_failover_partition.json"
    try:
        data = json.loads(read_text(partition_path))
        info = data.get("info", [{}])[0]
        units = info.get("set_info", {}).get("membership", {}).get("units", [])
        if units:
            result["promoted_primary_id"] = int(units[0].get("primary_id", 0))
            result["active_replicas"] = len(units[0].get("active_id_list", []))
            result["frozen_replicas"] = len(units[0].get("frozen_id_list", []))
    except (ValueError, TypeError, IndexError):
        pass

    visibility_rows = parse_repeated_csv_sections(case_dir / "runner" / "baseline_secondary_visibility.out")
    for row in visibility_rows:
        if row.get("phase") == "secondary_visibility_lag_after_primary_set":
            result["secondary_visibility_samples"] = to_number(row.get("samples"))
            result["secondary_visibility_errors"] = to_number(row.get("errors"))
            result["secondary_visibility_p95_us"] = to_number(row.get("p95_us"))
            result["secondary_visibility_p99_us"] = to_number(row.get("p99_us"))

    for phase in ("pre_failover", "post_failover_before_write", "post_failover_after_write"):
        rows = parse_csv_rows(case_dir / "runner" / f"{phase}_raft_lag.csv")
        if not rows:
            continue
        max_apply_lag = 0.0
        max_fatal_events = 0.0
        running_replicas = 0
        leader_replicas = 0
        for row in rows:
            max_apply_lag = max(max_apply_lag, to_number(row.get("apply_lag")) or 0.0)
            max_fatal_events = max(max_fatal_events, to_number(row.get("fatal_event_count")) or 0.0)
            running_replicas += 1 if (to_number(row.get("running")) or 0.0) == 1 else 0
            leader_replicas += 1 if (to_number(row.get("leader")) or 0.0) == 1 else 0
        result[f"{phase}_raft_max_apply_lag"] = max_apply_lag
        result[f"{phase}_raft_max_fatal_events"] = max_fatal_events
        result[f"{phase}_raft_running_replicas"] = running_replicas
        result[f"{phase}_raft_leader_replicas"] = leader_replicas
    return result


def extract_data_2node(case_dir: Path) -> dict[str, Any]:
    rows = parse_csv_rows(case_dir / "run" / "results.csv")
    result: dict[str, Any] = {"thread_results": rows}
    best_set_qps = 0.0
    best_get_qps = 0.0
    max_errors = 0.0
    max_exit_code = 0.0
    max_set_p95_us = 0.0
    max_set_p99_us = 0.0
    max_get_p95_us = 0.0
    max_get_p99_us = 0.0
    for row in rows:
        best_set_qps = max(best_set_qps, to_number(row.get("set_qps")) or 0.0)
        best_get_qps = max(best_get_qps, to_number(row.get("get_qps")) or 0.0)
        max_errors = max(max_errors, to_number(row.get("errors")) or 0.0)
        max_exit_code = max(max_exit_code, to_number(row.get("exit_code")) or 0.0)
        max_set_p95_us = max(max_set_p95_us, to_number(row.get("set_p95_us")) or 0.0)
        max_set_p99_us = max(max_set_p99_us, to_number(row.get("set_p99_us")) or 0.0)
        max_get_p95_us = max(max_get_p95_us, to_number(row.get("get_p95_us")) or 0.0)
        max_get_p99_us = max(max_get_p99_us, to_number(row.get("get_p99_us")) or 0.0)
    if rows:
        result["best_set_qps"] = best_set_qps
        result["best_get_qps"] = best_get_qps
        result["max_errors"] = max_errors
        result["max_exit_code"] = max_exit_code
        result["max_set_p95_us"] = max_set_p95_us
        result["max_set_p99_us"] = max_set_p99_us
        result["max_get_p95_us"] = max_get_p95_us
        result["max_get_p99_us"] = max_get_p99_us
    return result


def extract_data_mixed_rw(case_dir: Path) -> dict[str, Any]:
    rows = parse_repeated_csv_sections(case_dir / "run" / "mixed_visibility.out")
    result: dict[str, Any] = {"visibility_rows": rows}
    secondary_rows = [
        row for row in rows
        if str(row.get("phase", "")).startswith("secondary_")
    ]
    background_rows = [
        row for row in rows
        if row.get("background") in {"writes", "reads"}
    ]
    max_errors = 0.0
    max_p95_us = 0.0
    max_p99_us = 0.0
    max_background_errors = 0.0
    max_background_exit_code = 0.0
    for row in secondary_rows:
        max_errors = max(max_errors, to_number(row.get("errors")) or 0.0)
        max_p95_us = max(max_p95_us, to_number(row.get("p95_us")) or 0.0)
        max_p99_us = max(max_p99_us, to_number(row.get("p99_us")) or 0.0)
    for row in background_rows:
        max_background_errors = max(max_background_errors, to_number(row.get("errors")) or 0.0)
        max_background_exit_code = max(
            max_background_exit_code,
            to_number(row.get("exit_code")) or 0.0,
        )
    if secondary_rows or background_rows:
        result["secondary_phase_count"] = len(secondary_rows)
        result["background_phase_count"] = len(background_rows)
        result["max_errors"] = max_errors
        result["max_p95_us"] = max_p95_us
        result["max_p99_us"] = max_p99_us
        result["max_background_errors"] = max_background_errors
        result["max_background_exit_code"] = max_background_exit_code
    return result


def extract_data_snapshot_restore(case_dir: Path) -> dict[str, Any]:
    kv = parse_key_values(case_dir / "run" / "summary.txt")
    result: dict[str, Any] = {}
    for key in (
        "snapshot_file_count_before_restart",
        "snapshot_file_count_after_restart",
        "applied_index_file_count",
        "wal_file_count",
    ):
        value = to_number(kv.get(key))
        if value is not None:
            result[key] = value
    return result


def extract_metaserver_membership(case_dir: Path) -> dict[str, Any]:
    kv = parse_key_values(case_dir / "run" / "summary.txt")
    result: dict[str, Any] = {}
    for key in (
        "node3_applied_index_after_add",
        "metaserver_membership_add_remove_ms",
    ):
        value = to_number(kv.get(key))
        if value is not None:
            result[key] = value
    for key in (
        "initial_leader",
        "leader_after_remove",
        "node3_stale_read_namespace",
        "removed_node_port_down",
        "namespace_before_add",
        "namespace_after_remove",
    ):
        if key in kv:
            result[key] = kv[key]

    try:
        after_add = json.loads(read_text(case_dir / "run" / "membership_after_add.json"))
        after_remove = json.loads(read_text(case_dir / "run" / "membership_after_remove_write.json"))
        result["membership_nodes_after_add"] = len(after_add.get("nodes", []))
        result["membership_nodes_after_remove"] = len(after_remove.get("nodes", []))
    except (TypeError, ValueError):
        pass
    return result


def extract_data_membership(case_dir: Path) -> dict[str, Any]:
    kv = parse_key_values(case_dir / "run" / "summary.txt")
    result: dict[str, Any] = {}
    for key in (
        "scale_up_server3_partition_id",
        "scale_down_server3_active_partitions",
        "drop_server3_active_partitions",
    ):
        value = to_number(kv.get(key))
        if value is not None:
            result[key] = value

    for phase in ("baseline", "after_scale_up", "after_scale_down", "after_drop"):
        rows = parse_topology_csv(case_dir / "run" / f"topology_{phase}.csv")
        if not rows:
            continue
        result[f"{phase}_active_replicas"] = sum(
            1 for row in rows if row.get("membership_state") == "active"
        )
        result[f"{phase}_normal_replicas"] = sum(
            1 for row in rows if row.get("state") == "P_NORMAL"
        )
        result[f"{phase}_primary_replicas"] = sum(
            1 for row in rows if row.get("primary_state") == "primary"
        )

        lag_rows = parse_csv_rows(case_dir / "run" / f"{phase}_raft_lag.csv")
        if lag_rows:
            result[f"{phase}_raft_max_apply_lag"] = max(
                to_number(row.get("apply_lag")) or 0.0 for row in lag_rows
            )
            result[f"{phase}_raft_max_fatal_events"] = max(
                to_number(row.get("fatal_event_count")) or 0.0 for row in lag_rows
            )
            result[f"{phase}_raft_running_replicas"] = sum(
                1 for row in lag_rows if (to_number(row.get("running")) or 0.0) == 1
            )
            result[f"{phase}_raft_leader_replicas"] = sum(
                1 for row in lag_rows if (to_number(row.get("leader")) or 0.0) == 1
            )

    try:
        status = json.loads(read_text(case_dir / "run" / "server3_raft_status_after_scale_up.json"))
        for key in (
            "voter_count",
            "learner_count",
            "fatal_event_count",
            "committed_index",
            "applied_index",
            "pending_config_change_index",
        ):
            value = to_number(status.get(key, 0))
            if value is not None:
                result[f"server3_after_scale_up_{key}"] = value
        result["server3_after_scale_up_running"] = 1 if status.get("running") is True else 0
        result["server3_after_scale_up_leader"] = 1 if status.get("leader") is True else 0
    except (TypeError, ValueError):
        pass
    return result


def extract_metaserver_failover(case_dir: Path) -> dict[str, Any]:
    kv = parse_key_values(case_dir / "run" / "summary.txt")
    diagnostics = parse_key_values(case_dir / "run" / "diagnostics_summary.txt")
    result: dict[str, Any] = {}
    for key in (
        "initial_leader",
        "leader_after_kill",
        "metaserver_failover_ms",
        "namespace_before",
        "namespace_after",
    ):
        if key in kv:
            result[key] = kv[key]
    if "metaserver_failover_ms" in result:
        number = to_number(result["metaserver_failover_ms"])
        if number is not None:
            result["metaserver_failover_ms"] = number
    metrics_path = case_dir / "run" / "metrics_summary.txt"
    result["vars_scrape_summary"] = read_text(metrics_path).splitlines()
    for key in (
        "expected_running_count",
        "alive_count",
        "unexpected_down_count",
        "port_up_count",
        "fatal_log_line_count",
    ):
        value = to_number(diagnostics.get(key))
        if value is not None:
            result[f"diagnostics_{key}"] = value
    if "diagnostic_reason" in diagnostics:
        result["diagnostic_reason"] = diagnostics["diagnostic_reason"]
    return result


def summarize(result_dir: Path) -> dict[str, Any]:
    cases_path = result_dir / "cases.csv"
    cases = parse_csv_rows(cases_path)
    summary: dict[str, Any] = {
        "result_dir": str(result_dir),
        "cases": cases,
        "passed": sum(1 for row in cases if row.get("status") == "pass"),
        "failed": sum(1 for row in cases if row.get("status") != "pass"),
        "case_metrics": {},
    }
    case_metrics: dict[str, Any] = summary["case_metrics"]
    for row in cases:
        name = row.get("case", "")
        case_dir = Path(row.get("result_dir", ""))
        if name == "metaserver_failover":
            case_metrics[name] = extract_metaserver_failover(case_dir)
        elif name == "metaserver_membership":
            case_metrics[name] = extract_metaserver_membership(case_dir)
        elif name == "data_failover":
            case_metrics[name] = extract_data_failover(case_dir)
        elif name == "data_membership":
            case_metrics[name] = extract_data_membership(case_dir)
        elif name == "data_2node_scale":
            case_metrics[name] = extract_data_2node(case_dir)
        elif name == "data_mixed_rw":
            case_metrics[name] = extract_data_mixed_rw(case_dir)
        elif name == "data_snapshot_restore":
            case_metrics[name] = extract_data_snapshot_restore(case_dir)
    return summary


def write_prometheus(summary: dict[str, Any], path: Path) -> None:
    lines: list[str] = [
        "# HELP temporalstore_raft_gate_case_pass Whether a raft gate case passed.",
        "# TYPE temporalstore_raft_gate_case_pass gauge",
    ]
    for row in summary.get("cases", []):
        labels = {"case": row.get("case", ""), "iteration": row.get("iteration", "")}
        lines.append(prom_line("temporalstore_raft_gate_case_pass", 1 if row.get("status") == "pass" else 0, labels))
        seconds = to_number(row.get("seconds"))
        if seconds is not None:
            lines.append(prom_line("temporalstore_raft_gate_case_seconds", seconds, labels))

    metrics = summary.get("case_metrics", {})
    meta = metrics.get("metaserver_failover", {})
    if isinstance(meta, dict) and isinstance(meta.get("metaserver_failover_ms"), (int, float)):
        lines.append(prom_line("temporalstore_raft_gate_metaserver_failover_ms", meta["metaserver_failover_ms"]))
    if isinstance(meta, dict):
        for key in (
            "diagnostics_expected_running_count",
            "diagnostics_alive_count",
            "diagnostics_unexpected_down_count",
            "diagnostics_port_up_count",
            "diagnostics_fatal_log_line_count",
        ):
            value = meta.get(key)
            if isinstance(value, (int, float)):
                lines.append(prom_line(f"temporalstore_raft_gate_metaserver_failover_{key}", value))

    meta_membership = metrics.get("metaserver_membership", {})
    if isinstance(meta_membership, dict):
        for key in (
            "node3_applied_index_after_add",
            "metaserver_membership_add_remove_ms",
            "membership_nodes_after_add",
            "membership_nodes_after_remove",
        ):
            value = meta_membership.get(key)
            if isinstance(value, (int, float)):
                lines.append(prom_line(f"temporalstore_raft_gate_metaserver_membership_{key}", value))

    data_failover = metrics.get("data_failover", {})
    if isinstance(data_failover, dict):
        for key in (
            "post_failover_attempts",
            "post_failover_write_read_ms",
            "background_failover_enabled",
            "background_failover_active_at_kill",
            "background_failover_exit_code",
            "background_failover_errors",
            "background_failover_zero_errors",
            "background_failover_elapsed_ms",
            "active_replicas",
            "frozen_replicas",
            "secondary_visibility_errors",
            "secondary_visibility_p95_us",
            "secondary_visibility_p99_us",
            "pre_failover_raft_max_apply_lag",
            "pre_failover_raft_max_fatal_events",
            "pre_failover_raft_running_replicas",
            "pre_failover_raft_leader_replicas",
            "post_failover_before_write_raft_max_apply_lag",
            "post_failover_before_write_raft_max_fatal_events",
            "post_failover_before_write_raft_running_replicas",
            "post_failover_before_write_raft_leader_replicas",
            "post_failover_after_write_raft_max_apply_lag",
            "post_failover_after_write_raft_max_fatal_events",
            "post_failover_after_write_raft_running_replicas",
            "post_failover_after_write_raft_leader_replicas",
        ):
            value = data_failover.get(key)
            if isinstance(value, (int, float)):
                lines.append(prom_line(f"temporalstore_raft_gate_data_{key}", value))

    data_membership = metrics.get("data_membership", {})
    if isinstance(data_membership, dict):
        for key in (
            "scale_up_server3_partition_id",
            "scale_down_server3_active_partitions",
            "drop_server3_active_partitions",
            "baseline_active_replicas",
            "after_scale_up_active_replicas",
            "after_scale_down_active_replicas",
            "after_drop_active_replicas",
            "after_scale_up_primary_replicas",
            "baseline_raft_max_apply_lag",
            "baseline_raft_max_fatal_events",
            "after_scale_up_raft_max_apply_lag",
            "after_scale_up_raft_max_fatal_events",
            "after_scale_down_raft_max_apply_lag",
            "after_scale_down_raft_max_fatal_events",
            "after_drop_raft_max_apply_lag",
            "after_drop_raft_max_fatal_events",
            "after_scale_up_raft_running_replicas",
            "after_scale_down_raft_running_replicas",
            "after_drop_raft_running_replicas",
            "server3_after_scale_up_voter_count",
            "server3_after_scale_up_learner_count",
            "server3_after_scale_up_fatal_event_count",
            "server3_after_scale_up_committed_index",
            "server3_after_scale_up_applied_index",
            "server3_after_scale_up_pending_config_change_index",
            "server3_after_scale_up_running",
            "server3_after_scale_up_leader",
        ):
            value = data_membership.get(key)
            if isinstance(value, (int, float)):
                lines.append(prom_line(f"temporalstore_raft_gate_data_membership_{key}", value))

    data_scale = metrics.get("data_2node_scale", {})
    if isinstance(data_scale, dict):
        for key in (
            "best_set_qps",
            "best_get_qps",
            "max_errors",
            "max_exit_code",
            "max_set_p95_us",
            "max_set_p99_us",
            "max_get_p95_us",
            "max_get_p99_us",
        ):
            value = data_scale.get(key)
            if isinstance(value, (int, float)):
                lines.append(prom_line(f"temporalstore_raft_gate_2node_{key}", value))

    data_mixed = metrics.get("data_mixed_rw", {})
    if isinstance(data_mixed, dict):
        for key in (
            "secondary_phase_count",
            "background_phase_count",
            "max_errors",
            "max_p95_us",
            "max_p99_us",
            "max_background_errors",
            "max_background_exit_code",
        ):
            value = data_mixed.get(key)
            if isinstance(value, (int, float)):
                lines.append(prom_line(f"temporalstore_raft_gate_mixed_rw_{key}", value))

    data_snapshot = metrics.get("data_snapshot_restore", {})
    if isinstance(data_snapshot, dict):
        for key in (
            "snapshot_file_count_before_restart",
            "snapshot_file_count_after_restart",
            "applied_index_file_count",
            "wal_file_count",
        ):
            value = data_snapshot.get(key)
            if isinstance(value, (int, float)):
                lines.append(prom_line(f"temporalstore_raft_gate_snapshot_{key}", value))

    assertions = summary.get("production_assertions", {})
    if isinstance(assertions, dict):
        lines.append(prom_line("temporalstore_raft_gate_production_ready", 1 if assertions.get("passed") else 0))
        for check in assertions.get("checks", []):
            if not isinstance(check, dict):
                continue
            labels = {"check": str(check.get("name", ""))}
            lines.append(prom_line("temporalstore_raft_gate_production_check_pass", 1 if check.get("passed") else 0, labels))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def add_check(checks: list[dict[str, Any]], name: str, passed: bool, detail: str) -> None:
    checks.append({"name": name, "passed": passed, "detail": detail})


def validate_production_assertions(
    summary: dict[str, Any],
    max_metaserver_failover_ms: float,
    max_data_failover_write_read_ms: float,
    max_secondary_visibility_p99_us: float,
    max_post_failover_apply_lag: float,
    max_2node_scale_p99_us: float,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    metrics = summary.get("case_metrics", {})

    add_check(checks, "all_cases_passed", summary.get("failed", 0) == 0, f"failed={summary.get('failed', 0)}")

    meta_failover = metrics.get("metaserver_failover", {})
    value = meta_failover.get("metaserver_failover_ms") if isinstance(meta_failover, dict) else None
    add_check(
        checks,
        "metaserver_failover_bounded",
        isinstance(value, (int, float)) and value <= max_metaserver_failover_ms,
        f"value={value},max={max_metaserver_failover_ms}",
    )
    add_check(
        checks,
        "metaserver_failover_no_unexpected_peer_death",
        isinstance(meta_failover, dict)
        and isinstance(meta_failover.get("diagnostics_unexpected_down_count"), (int, float))
        and meta_failover.get("diagnostics_unexpected_down_count") == 0,
        f"metrics={meta_failover}",
    )
    add_check(
        checks,
        "metaserver_failover_no_fatal_logs",
        isinstance(meta_failover, dict)
        and isinstance(meta_failover.get("diagnostics_fatal_log_line_count"), (int, float))
        and meta_failover.get("diagnostics_fatal_log_line_count") == 0,
        f"metrics={meta_failover}",
    )

    meta_membership = metrics.get("metaserver_membership", {})
    add_check(
        checks,
        "metaserver_membership_add_remove_converged",
        isinstance(meta_membership, dict)
        and meta_membership.get("membership_nodes_after_add") == 3
        and meta_membership.get("membership_nodes_after_remove") == 2
        and (meta_membership.get("node3_applied_index_after_add") or 0) > 0,
        f"metrics={meta_membership}",
    )

    data_failover = metrics.get("data_failover", {})
    write_read_ms = data_failover.get("post_failover_write_read_ms") if isinstance(data_failover, dict) else None
    visibility_errors = data_failover.get("secondary_visibility_errors") if isinstance(data_failover, dict) else None
    visibility_p99 = data_failover.get("secondary_visibility_p99_us") if isinstance(data_failover, dict) else None
    add_check(
        checks,
        "data_failover_bounded",
        isinstance(write_read_ms, (int, float)) and write_read_ms <= max_data_failover_write_read_ms,
        f"value={write_read_ms},max={max_data_failover_write_read_ms}",
    )
    add_check(
        checks,
        "data_failover_background_traffic_survived",
        isinstance(data_failover, dict)
        and data_failover.get("background_failover_enabled") == 1
        and data_failover.get("background_failover_active_at_kill") == 1
        and data_failover.get("background_failover_exit_code") == 0
        and data_failover.get("background_failover_errors") == 0
        and data_failover.get("background_failover_zero_errors") == 1,
        f"metrics={data_failover}",
    )
    add_check(
        checks,
        "secondary_visibility_no_errors",
        isinstance(visibility_errors, (int, float)) and visibility_errors == 0,
        f"value={visibility_errors}",
    )
    add_check(
        checks,
        "secondary_visibility_p99_bounded",
        isinstance(visibility_p99, (int, float)) and visibility_p99 <= max_secondary_visibility_p99_us,
        f"value={visibility_p99},max={max_secondary_visibility_p99_us}",
    )
    post_write_apply_lag = (
        data_failover.get("post_failover_after_write_raft_max_apply_lag")
        if isinstance(data_failover, dict)
        else None
    )
    post_write_fatal_events = (
        data_failover.get("post_failover_after_write_raft_max_fatal_events")
        if isinstance(data_failover, dict)
        else None
    )
    add_check(
        checks,
        "data_failover_post_write_apply_lag_bounded",
        isinstance(post_write_apply_lag, (int, float))
        and post_write_apply_lag <= max_post_failover_apply_lag,
        f"value={post_write_apply_lag},max={max_post_failover_apply_lag}",
    )
    add_check(
        checks,
        "data_failover_no_raft_fatal_events",
        isinstance(post_write_fatal_events, (int, float)) and post_write_fatal_events == 0,
        f"value={post_write_fatal_events}",
    )

    data_membership = metrics.get("data_membership", {})
    add_check(
        checks,
        "data_membership_scale_up_down_converged",
        isinstance(data_membership, dict)
        and data_membership.get("after_scale_up_active_replicas") == 3
        and data_membership.get("after_scale_down_active_replicas") == 2
        and data_membership.get("after_drop_active_replicas") == 2
        and data_membership.get("scale_down_server3_active_partitions") == 0
        and data_membership.get("drop_server3_active_partitions") == 0,
        f"metrics={data_membership}",
    )
    add_check(
        checks,
        "data_membership_server3_raft_healthy",
        isinstance(data_membership, dict)
        and data_membership.get("server3_after_scale_up_running") == 1
        and data_membership.get("server3_after_scale_up_voter_count") == 3
        and data_membership.get("server3_after_scale_up_fatal_event_count") == 0,
        f"metrics={data_membership}",
    )
    membership_lags = []
    membership_fatal_events = []
    if isinstance(data_membership, dict):
        for phase in ("baseline", "after_scale_up", "after_scale_down", "after_drop"):
            membership_lags.append(data_membership.get(f"{phase}_raft_max_apply_lag"))
            membership_fatal_events.append(data_membership.get(f"{phase}_raft_max_fatal_events"))
    add_check(
        checks,
        "data_membership_apply_lag_bounded",
        membership_lags
        and all(
            isinstance(value, (int, float)) and value <= max_post_failover_apply_lag
            for value in membership_lags
        ),
        f"values={membership_lags},max={max_post_failover_apply_lag}",
    )
    add_check(
        checks,
        "data_membership_no_raft_fatal_events",
        membership_fatal_events
        and all(isinstance(value, (int, float)) and value == 0 for value in membership_fatal_events),
        f"values={membership_fatal_events}",
    )

    data_scale = metrics.get("data_2node_scale", {})
    add_check(
        checks,
        "data_2node_scale_has_qps",
        isinstance(data_scale, dict)
        and (data_scale.get("best_set_qps") or 0) > 0
        and (data_scale.get("best_get_qps") or 0) > 0,
        f"metrics={data_scale}",
    )
    add_check(
        checks,
        "data_2node_scale_no_errors",
        isinstance(data_scale, dict)
        and isinstance(data_scale.get("max_errors"), (int, float))
        and data_scale.get("max_errors") == 0
        and isinstance(data_scale.get("max_exit_code"), (int, float))
        and data_scale.get("max_exit_code") == 0,
        f"metrics={data_scale}",
    )
    add_check(
        checks,
        "data_2node_scale_latency_bounded",
        isinstance(data_scale, dict)
        and isinstance(data_scale.get("max_set_p99_us"), (int, float))
        and isinstance(data_scale.get("max_get_p99_us"), (int, float))
        and data_scale.get("max_set_p99_us") <= max_2node_scale_p99_us
        and data_scale.get("max_get_p99_us") <= max_2node_scale_p99_us,
        f"metrics={data_scale},max={max_2node_scale_p99_us}",
    )

    data_mixed = metrics.get("data_mixed_rw", {})
    add_check(
        checks,
        "data_mixed_rw_no_errors",
        isinstance(data_mixed, dict)
        and (data_mixed.get("secondary_phase_count") or 0) >= 2
        and isinstance(data_mixed.get("max_errors"), (int, float))
        and data_mixed.get("max_errors") == 0
        and isinstance(data_mixed.get("max_background_errors"), (int, float))
        and data_mixed.get("max_background_errors") == 0
        and isinstance(data_mixed.get("max_background_exit_code"), (int, float))
        and data_mixed.get("max_background_exit_code") == 0,
        f"metrics={data_mixed}",
    )
    add_check(
        checks,
        "data_mixed_rw_visibility_p99_bounded",
        isinstance(data_mixed, dict)
        and isinstance(data_mixed.get("max_p99_us"), (int, float))
        and data_mixed.get("max_p99_us") <= max_secondary_visibility_p99_us,
        f"value={data_mixed.get('max_p99_us') if isinstance(data_mixed, dict) else None},"
        f"max={max_secondary_visibility_p99_us}",
    )

    snapshot = metrics.get("data_snapshot_restore", {})
    add_check(
        checks,
        "data_snapshot_restore_artifacts_present",
        isinstance(snapshot, dict)
        and (snapshot.get("snapshot_file_count_before_restart") or 0) > 0
        and (snapshot.get("applied_index_file_count") or 0) > 0
        and (snapshot.get("wal_file_count") or 0) > 0,
        f"metrics={snapshot}",
    )

    passed = all(bool(check["passed"]) for check in checks)
    return {"passed": passed, "checks": checks}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--production-assertions", action="store_true")
    parser.add_argument("--max-metaserver-failover-ms", type=float, default=10_000)
    parser.add_argument("--max-data-failover-write-read-ms", type=float, default=10_000)
    parser.add_argument("--max-secondary-visibility-p99-us", type=float, default=50_000)
    parser.add_argument("--max-post-failover-apply-lag", type=float, default=128)
    parser.add_argument("--max-2node-scale-p99-us", type=float, default=150_000)
    args = parser.parse_args()

    result_dir = args.result_dir
    summary = summarize(result_dir)
    assertion_status = 0
    if args.production_assertions:
        assertions = validate_production_assertions(
            summary,
            max_metaserver_failover_ms=args.max_metaserver_failover_ms,
            max_data_failover_write_read_ms=args.max_data_failover_write_read_ms,
            max_secondary_visibility_p99_us=args.max_secondary_visibility_p99_us,
            max_post_failover_apply_lag=args.max_post_failover_apply_lag,
            max_2node_scale_p99_us=args.max_2node_scale_p99_us,
        )
        summary["production_assertions"] = assertions
        assertion_status = 0 if assertions["passed"] else 1
    json_path = result_dir / "metrics.json"
    prom_path = result_dir / "metrics.prom"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_prometheus(summary, prom_path)
    print(f"metrics_json={json_path}")
    print(f"metrics_prom={prom_path}")
    print(f"passed={summary['passed']}")
    print(f"failed={summary['failed']}")
    if args.production_assertions:
        print(f"production_ready={1 if summary['production_assertions']['passed'] else 0}")
        for check in summary["production_assertions"]["checks"]:
            status = "pass" if check["passed"] else "fail"
            print(f"production_check={check['name']} status={status} {check['detail']}")
    return 0 if summary["failed"] == 0 and assertion_status == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
