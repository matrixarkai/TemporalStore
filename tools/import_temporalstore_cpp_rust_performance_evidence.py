#!/usr/bin/env python3
"""Import one C++/Rust scale report into the performance parity matrix.

The importer is conservative: it updates only the workload rows proven by the
input report, and it never upgrades a row from `missing_live_evidence` unless
the report contains same-config C++ and Rust backend metrics with zero
errors/timeouts, no fallback flags, selected-ref parity, and threshold-compliant
QPS/latency ratios.
"""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from validate_storage_tuning_parity import EXPECTED_DEFAULTS as REQUIRED_STORAGE_TUNING


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = ROOT / "compat" / "temporalstore_cpp_rust_performance_parity_matrix.json"
VALIDATOR = ROOT / "tools" / "validate_temporalstore_cpp_rust_performance_parity.py"

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

REQUIRED_SAME_CONFIG_VALUES = {
    "dataset": "matrixark-scale-synthetic",
    "batch_size": 20,
    "token_budget": 12000,
    "embedding_model": "matrixark-local-token-hash-v1",
    "reader_model": "matrixark-deterministic-reader",
    "judge_model": "matrixark-deterministic-judge",
}


def _dig(data: dict[str, Any], *path: str, default: Any = None) -> Any:
    value: Any = data
    for key in path:
        if not isinstance(value, dict):
            return default
        value = value.get(key)
    return default if value is None else value


def _num(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _int(value: Any, default: int = 0) -> int:
    return int(_num(value, float(default)))


def _list(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [str(key) for key, flag in value.items() if flag]
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _storage_mode(report: dict[str, Any]) -> Any:
    config = report.get("config") if isinstance(report.get("config"), dict) else {}
    options = config.get("storage_options") if isinstance(config.get("storage_options"), dict) else {}
    return (
        options.get("route")
        or options.get("storage_family")
        or options.get("storage_mode")
        or config.get("storage_mode")
        or "default"
    )


def _same_config(report: dict[str, Any]) -> dict[str, Any]:
    config = report.get("config") if isinstance(report.get("config"), dict) else {}
    return {
        "dataset": config.get("dataset") or config.get("phase_name") or "matrixark_scale",
        "storage_mode": _storage_mode(report),
        "topology": config.get("topology"),
        "batch_size": config.get("batch_size"),
        "token_budget": config.get("max_context_tokens"),
        "embedding_model": config.get("embedding_model"),
        "reader_model": config.get("reader_model"),
        "judge_model": config.get("judge_model"),
        "storage_tuning": config.get("effective_storage_tuning"),
    }


def _same_config_blockers(same_config: dict[str, Any]) -> list[str]:
    blockers = [
        "missing_same_config:" + ",".join(
            key for key in SAME_CONFIG_KEYS if same_config.get(key) in (None, "", "required_per_row")
        )
    ]
    blockers = [blocker for blocker in blockers if blocker != "missing_same_config:"]
    for key, expected in REQUIRED_SAME_CONFIG_VALUES.items():
        if same_config.get(key) != expected:
            blockers.append(f"same_config_drift:{key}")
    storage_tuning = same_config.get("storage_tuning")
    if not isinstance(storage_tuning, dict):
        blockers.append("storage_tuning_missing")
    else:
        for key, expected in REQUIRED_STORAGE_TUNING.items():
            if key not in storage_tuning:
                blockers.append(f"storage_tuning_missing:{key}")
            elif storage_tuning.get(key) != expected:
                blockers.append(f"storage_tuning_drift:{key}")
    return blockers


def _backend_storage_tuning_blockers(name: str, backend: dict[str, Any]) -> list[str]:
    tuning = backend.get("effective_storage_tuning")
    if not isinstance(tuning, dict):
        return [f"{name}_storage_tuning_missing"]
    blockers: list[str] = []
    for key, expected in REQUIRED_STORAGE_TUNING.items():
        if key not in tuning:
            blockers.append(f"{name}_storage_tuning_missing:{key}")
        elif tuning.get(key) != expected:
            blockers.append(f"{name}_storage_tuning_drift:{key}")
    return blockers


def _selected_ref_parity(report: dict[str, Any]) -> bool:
    phase0 = _dig(report, "comparison", "phase0_correctness", default={})
    evidence = phase0.get("evidence") if isinstance(phase0, dict) else {}
    return bool(isinstance(evidence, dict) and evidence.get("selected_ref_parity") is True)


def _phase_scale_blockers(report: dict[str, Any]) -> list[str]:
    phase_scale = report.get("phase_scale_matrix")
    if not isinstance(phase_scale, dict):
        return ["phase_scale_matrix_missing"]
    blockers: list[str] = []
    if phase_scale.get("require_gate") is not True:
        blockers.append("phase_scale_matrix_not_required")
    if phase_scale.get("status") != "passed":
        blockers.append(f"phase_scale_matrix_{phase_scale.get('status') or 'unknown'}")
    open_cases = phase_scale.get("open_required_cases")
    if isinstance(open_cases, list) and open_cases:
        blockers.append("phase_scale_matrix_open_required_cases")
    full_pipeline = phase_scale.get("full_contextmemory_pipeline")
    if isinstance(full_pipeline, dict) and full_pipeline.get("status") != "passed":
        blockers.append("phase_scale_contextmemory_pipeline_incomplete")
    return blockers


def _fallback_flags(backend: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    flags.extend(_list(backend.get("fallback_flags")))
    retrieve = backend.get("retrieve") if isinstance(backend.get("retrieve"), dict) else {}
    stage = retrieve.get("stage_metrics") if isinstance(retrieve.get("stage_metrics"), dict) else {}
    flags.extend(_list(retrieve.get("fallback_flags_total")))
    flags.extend(_list(stage.get("fallback_flags_total")))
    if _int(stage.get("broad_scan_used_count")) > 0:
        flags.append("broad_scan_used")
    if _int(stage.get("python_pack_fallback_count")) > 0:
        flags.append("python_pack_fallback")
    if _int(stage.get("native_pack_fallback_count")) > 0:
        flags.append("native_pack_fallback")
    if _int(stage.get("timeout_partial_count")) > 0 or _int(retrieve.get("partial_context_packs")) > 0:
        flags.append("timeout_partial")
    if backend.get("partial_context_pack") is True:
        flags.append("partial_context_pack")
    return sorted(set(flags))


def _metric_block(backend: dict[str, Any], mode: str, selected_ref_parity: bool) -> dict[str, Any]:
    ingest = backend.get("ingest") if isinstance(backend.get("ingest"), dict) else {}
    retrieve = backend.get("retrieve") if isinstance(backend.get("retrieve"), dict) else {}
    stage = retrieve.get("stage_metrics") if isinstance(retrieve.get("stage_metrics"), dict) else {}
    lifecycle = backend.get("storage_lifecycle_metrics") if isinstance(backend.get("storage_lifecycle_metrics"), dict) else {}
    if not lifecycle:
        lifecycle = backend.get("storage_lifecycle") if isinstance(backend.get("storage_lifecycle"), dict) else {}
        lifecycle = lifecycle.get("storage_lifecycle_metrics") if isinstance(lifecycle.get("storage_lifecycle_metrics"), dict) else {}
    primary = ingest if mode == "ingest" else retrieve
    return {
        "message_qps": _num(_dig(backend, "ingest_messages", "message_qps")),
        "retrieve_qps": _num(retrieve.get("qps")),
        "p50_ms": _num(primary.get("p50_ms")),
        "p95_ms": _num(primary.get("p95_ms")),
        "p99_ms": _num(primary.get("p99_ms")),
        "timeout_count": _int(primary.get("timeout_count")) + _int(stage.get("timeout_count")),
        "error_count": len(backend.get("errors") or []),
        "fallback_flags": _fallback_flags(backend),
        "selected_ref_parity": bool(selected_ref_parity),
        "scanned_records": _num(stage.get("scanned_records_avg")),
        "cache_hit_rate": _num(stage.get("cache_hit_rate")),
        "append_watermark": _num(lifecycle.get("append_watermark")),
        "compaction_watermark": _num(lifecycle.get("compaction_watermark")),
    }


def _ratios(cpp: dict[str, Any], rust: dict[str, Any]) -> dict[str, Any]:
    def qps_ratio(name: str) -> float:
        cpp_value = _num(cpp.get(name))
        rust_value = _num(rust.get(name))
        return round(rust_value / cpp_value, 6) if cpp_value else (1.0 if rust_value == 0 else 999999.0)

    def latency_ratio(name: str) -> float:
        cpp_value = _num(cpp.get(name))
        rust_value = _num(rust.get(name))
        return round(rust_value / cpp_value, 6) if cpp_value else (1.0 if rust_value == 0 else 999999.0)

    return {
        "message_qps_ratio": qps_ratio("message_qps"),
        "retrieve_qps_ratio": qps_ratio("retrieve_qps"),
        "p50_ratio": latency_ratio("p50_ms"),
        "p95_ratio": latency_ratio("p95_ms"),
        "p99_ratio": latency_ratio("p99_ms"),
    }


def _row_status(
    cpp: dict[str, Any],
    rust: dict[str, Any],
    ratios: dict[str, Any],
    thresholds: dict[str, Any],
) -> tuple[str, list[str]]:
    blockers: list[str] = []
    min_qps = float(thresholds.get("min_rust_cpp_qps_ratio") or 0.8)
    max_latency = float(thresholds.get("max_rust_cpp_latency_ratio") or 2.0)
    max_timeouts = int(thresholds.get("max_timeout_count") or 0)
    max_errors = int(thresholds.get("max_error_count") or 0)
    allow_fallback_flags = thresholds.get("allow_fallback_flags") is True
    require_selected_ref_parity = thresholds.get("require_selected_ref_parity") is not False
    if _int(cpp.get("timeout_count")) > max_timeouts or _int(rust.get("timeout_count")) > max_timeouts:
        blockers.append(f"timeout_count_above_{max_timeouts}")
    if _int(cpp.get("error_count")) > max_errors or _int(rust.get("error_count")) > max_errors:
        blockers.append(f"error_count_above_{max_errors}")
    if not allow_fallback_flags and (cpp.get("fallback_flags") or rust.get("fallback_flags")):
        blockers.append("fallback_flags_present")
    if require_selected_ref_parity and (
        cpp.get("selected_ref_parity") is not True or rust.get("selected_ref_parity") is not True
    ):
        blockers.append("selected_ref_parity_missing")
    for side, metrics in (("cpp", cpp), ("rust", rust)):
        append_watermark = _num(metrics.get("append_watermark"))
        compaction_watermark = _num(metrics.get("compaction_watermark"))
        if append_watermark <= 0:
            blockers.append(f"{side}_append_watermark_not_advanced")
        if compaction_watermark > append_watermark:
            blockers.append(f"{side}_compaction_watermark_ahead_of_append")
    for key in ("message_qps_ratio", "retrieve_qps_ratio"):
        if _num(ratios.get(key)) < min_qps:
            blockers.append(f"{key}_below_{min_qps}")
    for key in ("p50_ratio", "p95_ratio", "p99_ratio"):
        if _num(ratios.get(key), 999999.0) > max_latency:
            blockers.append(f"{key}_above_{max_latency}")
    if blockers:
        return "performance_candidate", blockers
    return "production_performance_parity", []


def _candidate_workloads(report: dict[str, Any]) -> list[tuple[str, str]]:
    config = report.get("config") if isinstance(report.get("config"), dict) else {}
    workloads: list[tuple[str, str]] = []
    events = _int(config.get("events"))
    if events in (1000, 10000, 100000):
        workloads.append((f"{events // 1000 if events < 100000 else 100}K_event_ingestion", "ingest"))
    workers = _int(config.get("retrieve_workers"))
    if workers in (4, 8, 16, 32):
        workloads.append((f"retrieve_workers_{workers}", "retrieve"))
    return workloads


def import_report(matrix: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(matrix)
    cpp = _dig(report, "backends", "cpp", default={})
    rust = _dig(report, "backends", "rust", default={})
    same_config = _same_config(report)
    same_config_blockers = _same_config_blockers(same_config)
    selected_ref_parity = _selected_ref_parity(report)
    phase_scale_blockers = _phase_scale_blockers(report)
    thresholds = out.get("thresholds") if isinstance(out.get("thresholds"), dict) else {}
    rows = out.get("rows") if isinstance(out.get("rows"), list) else []
    by_workload = {row.get("workload"): row for row in rows if isinstance(row, dict)}
    for workload, mode in _candidate_workloads(report):
        row = by_workload.get(workload)
        if not isinstance(row, dict):
            continue
        blockers: list[str] = []
        if not isinstance(cpp, dict) or cpp.get("status") != "passed":
            blockers.append("cpp_backend_not_passed")
        if not isinstance(rust, dict) or rust.get("status") != "passed":
            blockers.append("rust_backend_not_passed")
        if isinstance(cpp, dict) and cpp.get("status") == "passed":
            blockers.extend(_backend_storage_tuning_blockers("cpp", cpp))
        if isinstance(rust, dict) and rust.get("status") == "passed":
            blockers.extend(_backend_storage_tuning_blockers("rust", rust))
        blockers.extend(same_config_blockers)
        blockers.extend(phase_scale_blockers)
        if blockers:
            row.update(
                {
                    "status": "missing_live_evidence",
                    "same_config_match": False,
                    "open_blockers": blockers,
                }
            )
            continue
        cpp_metrics = _metric_block(cpp, mode, selected_ref_parity)
        rust_metrics = _metric_block(rust, mode, selected_ref_parity)
        ratios = _ratios(cpp_metrics, rust_metrics)
        status, blockers = _row_status(cpp_metrics, rust_metrics, ratios, thresholds)
        if blockers:
            row.update(
                {
                    "status": "missing_live_evidence",
                    "same_config_match": False,
                    **same_config,
                    "cpp": cpp_metrics,
                    "rust": rust_metrics,
                    "ratios": ratios,
                    "source_report": str(report.get("artifact_dir") or report.get("report_path") or "input_report"),
                    "open_blockers": blockers,
                }
            )
            continue
        row.update(
            {
                "status": status,
                "same_config_match": True,
                **same_config,
                "cpp": cpp_metrics,
                "rust": rust_metrics,
                "ratios": ratios,
                "source_report": str(report.get("artifact_dir") or report.get("report_path") or "input_report"),
                "open_blockers": blockers,
            }
        )
    statuses = [row.get("status") for row in rows if isinstance(row, dict)]
    candidate_ready = bool(statuses) and all(status in {"performance_candidate", "production_performance_parity"} for status in statuses)
    production_ready = bool(statuses) and all(status == "production_performance_parity" for status in statuses)
    open_blockers: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for blocker in row.get("open_blockers") or []:
            open_blockers.append(f"{row.get('workload')}:{blocker}")
    out["status"] = {
        "performance_candidate": candidate_ready,
        "production_performance_parity": production_ready,
        "open_blockers": open_blockers,
    }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Import a C++/Rust scale report into the performance parity matrix.")
    parser.add_argument("--report", required=True, type=Path, help="Path to comparison.json from run_matrixark_cpp_rust_scale_report.py")
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--output", type=Path, default=None, help="Write updated matrix here. Defaults to --matrix.")
    parser.add_argument("--validate", action="store_true", help="Run the matrix validator after writing.")
    args = parser.parse_args()

    matrix = json.loads(args.matrix.read_text(encoding="utf-8"))
    report = json.loads(args.report.read_text(encoding="utf-8"))
    updated = import_report(matrix, report)
    output = args.output or args.matrix
    output.write_text(json.dumps(updated, indent=2) + "\n", encoding="utf-8")
    if args.validate:
        subprocess.run([sys.executable, str(VALIDATOR)], cwd=ROOT, check=True)
    print(f"updated {output}")
    print(f"performance_candidate={updated.get('status', {}).get('performance_candidate')}")
    print(f"production_performance_parity={updated.get('status', {}).get('production_performance_parity')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
