#!/usr/bin/env python3
"""Validate same-config C++/Rust TemporalStore performance parity evidence.

This validator is deliberately fail-closed. A missing live benchmark row is
allowed only when it is explicitly marked as an active blocker. A row may claim
`production_performance_parity` only when it carries same-config C++ and Rust
metrics, zero errors/timeouts, no fallback flags, selected-ref parity, and QPS /
latency ratios within policy thresholds.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from validate_storage_tuning_parity import EXPECTED_DEFAULTS as REQUIRED_STORAGE_TUNING


ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "compat" / "temporalstore_cpp_rust_performance_parity_matrix.json"

REQUIRED_WORKLOADS = [
    "1K_event_ingestion",
    "10K_event_ingestion",
    "100K_event_ingestion",
    "retrieve_workers_4",
    "retrieve_workers_8",
    "retrieve_workers_16",
    "retrieve_workers_32",
]

REQUIRED_METRICS = [
    "message_qps",
    "retrieve_qps",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "timeout_count",
    "error_count",
    "fallback_flags",
    "selected_ref_parity",
    "scanned_records",
    "cache_hit_rate",
    "append_watermark",
    "compaction_watermark",
]

SAME_CONFIG_KEYS = [
    "dataset",
    "storage_mode",
    "topology",
    "batch_size",
    "token_budget",
    "embedding_model",
    "reader_model",
    "judge_model",
    "storage_tuning",
]

WORKLOAD_RUN_ARGS = {
    "1K_event_ingestion": ["--events", "1000"],
    "10K_event_ingestion": ["--events", "10000"],
    "100K_event_ingestion": ["--events", "100000"],
    "retrieve_workers_4": ["--retrieve-workers", "4"],
    "retrieve_workers_8": ["--retrieve-workers", "8"],
    "retrieve_workers_16": ["--retrieve-workers", "16"],
    "retrieve_workers_32": ["--retrieve-workers", "32"],
}

REQUIRED_MISSING_EVIDENCE_HINT_FIELDS = [
    "artifact_dir",
    "comparison_path",
    "recommended_execution_output",
    "command",
    "import_command",
    "required_same_config_fields",
    "required_result",
]

REQUIRED_SAME_CONFIG_COMMAND_ARGS = {
    "--dataset": "matrixark-scale-synthetic",
    "--messages-per-ingest": "20",
    "--max-context-tokens": "12000",
    "--embedding-model": "matrixark-local-token-hash-v1",
    "--reader-model": "matrixark-deterministic-reader",
    "--judge-model": "matrixark-deterministic-judge",
    "--metaserver": "127.0.0.1:18000",
    "--namespace": "deploy_ns",
    "--table": "deploy_table",
    "--storage-family": "shared_store",
    "--storage-mode": "multi_node",
    "--write-mode": "async",
    "--oplog-mode": "async",
    "--replication-mode": "shared_store",
}

REQUIRED_COMPLETED_SAME_CONFIG_VALUES = {
    "dataset": "matrixark-scale-synthetic",
    "batch_size": 20,
    "token_budget": 12000,
    "embedding_model": "matrixark-local-token-hash-v1",
    "reader_model": "matrixark-deterministic-reader",
    "judge_model": "matrixark-deterministic-judge",
}

VALID_ROW_STATUSES = {
    "missing_live_evidence",
    "performance_candidate",
    "production_performance_parity",
}


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _exceeds_limit(value: Any, limit: float) -> bool:
    number = _as_number(value)
    return number is not None and number > limit


def _require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def _expected_artifact_dir(workload: str) -> str:
    return f"docs/benchmarks/parity_{workload}"


def _validate_missing_evidence_hint(row: dict[str, Any], failures: list[str]) -> None:
    workload = str(row.get("workload") or "")
    hint = row.get("next_run_hint")
    if not isinstance(hint, dict):
        failures.append(f"{workload} missing_live_evidence requires next_run_hint")
        return
    for field in REQUIRED_MISSING_EVIDENCE_HINT_FIELDS:
        if field not in hint:
            failures.append(f"{workload} next_run_hint missing `{field}`")
    artifact_dir = _expected_artifact_dir(workload)
    if hint.get("artifact_dir") != artifact_dir:
        failures.append(f"{workload} next_run_hint.artifact_dir must be {artifact_dir}")
    if hint.get("comparison_path") != f"{artifact_dir}/comparison.json":
        failures.append(f"{workload} next_run_hint.comparison_path must be {artifact_dir}/comparison.json")
    if hint.get("recommended_execution_output") != f"{artifact_dir}/execution.json":
        failures.append(f"{workload} next_run_hint.recommended_execution_output must be {artifact_dir}/execution.json")
    command = hint.get("command")
    if not isinstance(command, list):
        failures.append(f"{workload} next_run_hint.command must be a list")
    else:
        required = ["python", "tools/run_matrixark_cpp_rust_scale_report.py", *WORKLOAD_RUN_ARGS.get(workload, [])]
        for item in required:
            if item not in command:
                failures.append(f"{workload} next_run_hint.command missing `{item}`")
        for item in ["--backends", "cpp", "rust", "--artifact-dir", artifact_dir, "--require-perf-parity"]:
            if item not in command:
                failures.append(f"{workload} next_run_hint.command missing `{item}`")
        if "--require-phase-scale-matrix" not in command:
            failures.append(f"{workload} next_run_hint.command missing `--require-phase-scale-matrix`")
        for flag, expected_value in REQUIRED_SAME_CONFIG_COMMAND_ARGS.items():
            if flag not in command:
                failures.append(f"{workload} next_run_hint.command missing `{flag}`")
                continue
            index = command.index(flag)
            actual_value = command[index + 1] if index + 1 < len(command) else None
            if actual_value != expected_value:
                failures.append(
                    f"{workload} next_run_hint.command {flag} drift: "
                    f"expected {expected_value!r} got {actual_value!r}"
                )
    import_command = hint.get("import_command")
    expected_import = [
        "python",
        "tools/import_temporalstore_cpp_rust_performance_evidence.py",
        "--report",
        f"{artifact_dir}/comparison.json",
        "--validate",
    ]
    if import_command != expected_import:
        failures.append(f"{workload} next_run_hint.import_command drift")
    required_same_config_fields = _as_list(hint.get("required_same_config_fields"))
    for key in SAME_CONFIG_KEYS:
        if key not in required_same_config_fields:
            failures.append(f"{workload} next_run_hint.required_same_config_fields missing `{key}`")
    required_result = _as_list(hint.get("required_result"))
    for expected in [
        "same-config C++ and Rust comparison.json with passed backends",
        "zero timeouts/errors/fallback flags",
        "selected_ref_parity=true",
        "ratios within configured thresholds",
    ]:
        if expected not in required_result:
            failures.append(f"{workload} next_run_hint.required_result missing `{expected}`")


def _validate_metric_block(
    row: dict[str, Any],
    side: str,
    failures: list[str],
    *,
    require_selected_ref_parity: bool = True,
) -> dict[str, Any]:
    metrics = row.get(side)
    if not isinstance(metrics, dict):
        failures.append(f"{row.get('workload')} {side} metrics must be an object")
        return {}
    for metric in REQUIRED_METRICS:
        if metric not in metrics:
            failures.append(f"{row.get('workload')} {side} missing metric `{metric}`")
    for numeric in [
        "message_qps",
        "retrieve_qps",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "timeout_count",
        "error_count",
        "scanned_records",
        "cache_hit_rate",
        "append_watermark",
        "compaction_watermark",
    ]:
        if numeric in metrics:
            value = _as_number(metrics.get(numeric))
            if value is None or value < 0:
                failures.append(f"{row.get('workload')} {side}.{numeric} must be non-negative")
    if "fallback_flags" in metrics and not isinstance(metrics.get("fallback_flags"), list):
        failures.append(f"{row.get('workload')} {side}.fallback_flags must be a list")
    if (
        require_selected_ref_parity
        and "selected_ref_parity" in metrics
        and metrics.get("selected_ref_parity") is not True
    ):
        failures.append(f"{row.get('workload')} {side}.selected_ref_parity must be true")
    return metrics


def _validate_ratios(row: dict[str, Any], thresholds: dict[str, Any], failures: list[str]) -> None:
    ratios = row.get("ratios")
    if not isinstance(ratios, dict):
        failures.append(f"{row.get('workload')} ratios must be an object")
        return
    min_qps = float(thresholds.get("min_rust_cpp_qps_ratio") or 0.8)
    max_latency = float(thresholds.get("max_rust_cpp_latency_ratio") or 2.0)
    for qps_ratio in ["message_qps_ratio", "retrieve_qps_ratio"]:
        value = _as_number(ratios.get(qps_ratio))
        if value is None:
            failures.append(f"{row.get('workload')} {qps_ratio} missing")
        elif value < min_qps:
            failures.append(f"{row.get('workload')} {qps_ratio} below {min_qps}")
    for latency_ratio in ["p50_ratio", "p95_ratio", "p99_ratio"]:
        value = _as_number(ratios.get(latency_ratio))
        if value is None or value > max_latency:
            failures.append(f"{row.get('workload')} {latency_ratio} above {max_latency}")


def _validate_completed_same_config(row: dict[str, Any], failures: list[str]) -> None:
    workload = row.get("workload")
    for key in SAME_CONFIG_KEYS:
        if row.get(key) in (None, "", "required_per_row"):
            failures.append(f"{workload} missing same-config field `{key}`")
    for key, expected in REQUIRED_COMPLETED_SAME_CONFIG_VALUES.items():
        if row.get(key) != expected:
            failures.append(
                f"{workload} same-config field `{key}` drift: "
                f"expected {expected!r} got {row.get(key)!r}"
            )
    storage_tuning = row.get("storage_tuning")
    if not isinstance(storage_tuning, dict):
        failures.append(f"{workload} storage_tuning must be an object")
    else:
        for key, expected in REQUIRED_STORAGE_TUNING.items():
            if key not in storage_tuning:
                failures.append(f"{workload} storage_tuning missing `{key}`")
            elif storage_tuning.get(key) != expected:
                failures.append(
                    f"{workload} storage_tuning `{key}` drift: "
                    f"expected {expected!r} got {storage_tuning.get(key)!r}"
                )


def main() -> int:
    data = json.loads(MATRIX.read_text(encoding="utf-8"))
    failures: list[str] = []

    _require(data.get("schema") == "temporalstore_cpp_rust_performance_parity_matrix_v1", "unexpected schema", failures)

    thresholds = data.get("thresholds")
    if not isinstance(thresholds, dict):
        thresholds = {}
        failures.append("thresholds must be an object")
    for key in [
        "min_rust_cpp_qps_ratio",
        "max_rust_cpp_latency_ratio",
        "max_timeout_count",
        "max_error_count",
        "allow_fallback_flags",
        "require_selected_ref_parity",
    ]:
        if key not in thresholds:
            failures.append(f"thresholds missing `{key}`")

    same_config = data.get("same_config")
    if not isinstance(same_config, dict):
        same_config = {}
        failures.append("same_config must be an object")
    for key in SAME_CONFIG_KEYS:
        if key not in same_config:
            failures.append(f"same_config missing `{key}`")

    declared_workloads = _as_list(data.get("required_workloads"))
    for workload in REQUIRED_WORKLOADS:
        if workload not in declared_workloads:
            failures.append(f"required_workloads missing `{workload}`")
    declared_metrics = _as_list(data.get("required_metrics"))
    for metric in REQUIRED_METRICS:
        if metric not in declared_metrics:
            failures.append(f"required_metrics missing `{metric}`")

    status = data.get("status")
    if not isinstance(status, dict):
        status = {}
        failures.append("status must be an object")
    global_production = status.get("production_performance_parity") is True
    global_candidate = status.get("performance_candidate") is True
    global_blockers = _as_list(status.get("open_blockers"))
    if global_production and global_blockers:
        failures.append("global production_performance_parity cannot be true while open_blockers remain")
    if global_production and not global_candidate:
        failures.append("global production_performance_parity requires performance_candidate=true")

    rows = data.get("rows")
    if not isinstance(rows, list):
        rows = []
        failures.append("rows must be a list")
    by_workload = {row.get("workload"): row for row in rows if isinstance(row, dict)}
    for workload in REQUIRED_WORKLOADS:
        if workload not in by_workload:
            failures.append(f"rows missing workload `{workload}`")

    production_rows = 0
    candidate_rows = 0
    missing_rows = 0
    for workload, row in by_workload.items():
        if workload not in REQUIRED_WORKLOADS:
            failures.append(f"unknown workload `{workload}`")
        row_status = row.get("status")
        if row_status not in VALID_ROW_STATUSES:
            failures.append(f"{workload} invalid status `{row_status}`")
            continue
        blockers = _as_list(row.get("open_blockers"))
        if row_status == "missing_live_evidence":
            missing_rows += 1
            if not blockers:
                failures.append(f"{workload} missing_live_evidence requires open_blockers")
            if row.get("same_config_match") is True:
                failures.append(f"{workload} missing_live_evidence cannot claim same_config_match")
            _validate_missing_evidence_hint(row, failures)
            continue

        if blockers:
            failures.append(f"{workload} cannot be {row_status} while open_blockers remain")
        if row.get("same_config_match") is not True:
            failures.append(f"{workload} requires same_config_match=true")
        _validate_completed_same_config(row, failures)
        require_selected_ref_parity = thresholds.get("require_selected_ref_parity") is not False
        cpp = _validate_metric_block(
            row,
            "cpp",
            failures,
            require_selected_ref_parity=require_selected_ref_parity,
        )
        rust = _validate_metric_block(
            row,
            "rust",
            failures,
            require_selected_ref_parity=require_selected_ref_parity,
        )
        _validate_ratios(row, thresholds, failures)
        max_timeout = float(thresholds.get("max_timeout_count") or 0)
        max_error = float(thresholds.get("max_error_count") or 0)
        for side, metrics in [("cpp", cpp), ("rust", rust)]:
            if _exceeds_limit(metrics.get("timeout_count"), max_timeout):
                failures.append(f"{workload} {side}.timeout_count exceeds {max_timeout}")
            if _exceeds_limit(metrics.get("error_count"), max_error):
                failures.append(f"{workload} {side}.error_count exceeds {max_error}")
            if thresholds.get("allow_fallback_flags") is False and _as_list(metrics.get("fallback_flags")):
                failures.append(f"{workload} {side}.fallback_flags must be empty")
        if row_status == "performance_candidate":
            candidate_rows += 1
        if row_status == "production_performance_parity":
            candidate_rows += 1
            production_rows += 1

    if global_candidate and candidate_rows != len(REQUIRED_WORKLOADS):
        failures.append("global performance_candidate requires every workload to be at least performance_candidate")
    if global_production and production_rows != len(REQUIRED_WORKLOADS):
        failures.append("global production_performance_parity requires every workload to pass production_performance_parity")
    if global_production and missing_rows:
        failures.append("global production_performance_parity cannot have missing_live_evidence rows")

    if failures:
        details = "\n".join(f"- {failure}" for failure in failures)
        raise SystemExit(f"TemporalStore C++/Rust performance parity matrix failed:\n{details}")

    print("TemporalStore C++/Rust performance parity matrix is explicit and fail-closed")
    print(f"- workloads={len(REQUIRED_WORKLOADS)}")
    print(f"- missing_live_evidence_rows={missing_rows}")
    print(f"- performance_candidate={global_candidate}")
    print(f"- production_performance_parity={global_production}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
