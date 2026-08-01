#!/usr/bin/env python3
"""Generate Rust/C++ shared-store append-blob parity evidence.

The Rust side is live runtime evidence from the MatrixObject-backed protobuf
append-blob WAL workflow. The C++ side includes live MatrixObjectStore append
runtime evidence plus source contract checks for the AppendObject/RPC offset
surface that TemporalStore shared-store replication relies on.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIXOBJECT_REPO = Path("/root/src/github-services/MatrixObjectStore")
SCHEMA = "temporalstore_shared_store_blob_append_cpp_rust_parity_v1"


def _run(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: int = 180,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        argv,
        cwd=cwd,
        env=merged_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def _git_rev(repo: Path, ref: str = "HEAD") -> str:
    result = _run(["git", "rev-parse", ref], cwd=repo, timeout=30)
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip()


def _load_rust_runtime_report(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    if args.rust_report:
        data = json.loads(Path(args.rust_report).read_text(encoding="utf-8"))
        return data, {
            "mode": "loaded",
            "path": str(args.rust_report),
            "returncode": 0,
            "stderr_tail": "",
        }

    env = {
        "CARGO_TARGET_DIR": args.cargo_target_dir,
        "TEMPORALSTORE_APPEND_BLOB_PARITY_ENTRIES": str(args.entries),
        "TEMPORALSTORE_APPEND_BLOB_PARITY_VALUE_BYTES": str(args.value_bytes),
    }
    command = [
        "cargo",
        "run",
    ]
    if args.rust_profile == "release":
        command.append("--release")
    command.extend([
        "-p",
        "temporalstore-rust",
        "--features",
        "matrixobject",
        "--example",
        "shared_store_append_blob_parity_report",
    ])
    started = time.time()
    result = _run(command, cwd=ROOT, env=env, timeout=args.timeout_seconds)
    command_report = {
        "mode": "executed",
        "argv": command,
        "returncode": result.returncode,
        "elapsed_ms": round((time.time() - started) * 1000, 3),
        "stderr_tail": result.stderr[-4000:],
    }
    if result.returncode != 0:
        return {"schema": "rust_runtime_failed", "stdout_tail": result.stdout[-4000:]}, command_report
    try:
        return json.loads(result.stdout), command_report
    except json.JSONDecodeError:
        return {
            "schema": "rust_runtime_invalid_json",
            "stdout_tail": result.stdout[-4000:],
        }, command_report


def _source_contains(path: Path, patterns: list[str]) -> dict[str, bool]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {pattern: False for pattern in patterns}
    return {pattern: bool(re.search(pattern, text, re.MULTILINE | re.DOTALL)) for pattern in patterns}



def _load_cpp_runtime_report(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    if args.cpp_runtime_report:
        data = json.loads(Path(args.cpp_runtime_report).read_text(encoding="utf-8"))
        return data, {
            "mode": "loaded",
            "path": str(args.cpp_runtime_report),
            "returncode": 0,
            "stderr_tail": "",
        }

    bin_path = Path(args.cpp_runtime_bin) if args.cpp_runtime_bin else (
        Path(args.matrixobject_repo)
        / "build-mvp/matrixobjectstore/objectstore/objectstore_append_blob_parity_report"
    )
    if not bin_path.exists():
        return {
            "status": "missing",
            "reason": "cpp runtime emitter binary not found",
            "expected_binary": str(bin_path),
        }, {
            "mode": "missing",
            "argv": [str(bin_path)],
            "returncode": 127,
            "stderr_tail": "",
        }

    root = Path(args.output_dir)
    if not root.is_absolute():
        root = ROOT / root
    runtime_root = root / "cpp_matrixobjectstore_runtime_root"
    command = [str(bin_path), str(runtime_root), str(args.entries), str(args.value_bytes)]
    started = time.time()
    result = _run(command, cwd=Path(args.matrixobject_repo), timeout=args.timeout_seconds)
    command_report = {
        "mode": "executed",
        "argv": command,
        "returncode": result.returncode,
        "elapsed_ms": round((time.time() - started) * 1000, 3),
        "stderr_tail": result.stderr[-4000:],
    }
    if result.returncode != 0:
        return {"status": "failed", "stdout_tail": result.stdout[-4000:]}, command_report
    try:
        return json.loads(result.stdout), command_report
    except json.JSONDecodeError:
        return {
            "status": "invalid_json",
            "stdout_tail": result.stdout[-4000:],
        }, command_report

def _cpp_contract(matrixobject_repo: Path) -> dict[str, Any]:
    object_cc = matrixobject_repo / "matrixobjectstore/objectstore/objectstore.cc"
    rpc_cc = matrixobject_repo / "matrixobjectstore/objectstore/objectstore_rpc.cc"
    object_h = matrixobject_repo / "matrixobjectstore/objectstore/objectstore.h"
    rust_lib = matrixobject_repo / "rust/matrixobjectstore-rs/src/lib.rs"

    object_cc_checks = _source_contains(
        object_cc,
        [
            r"Status\s+ObjectStore::AppendObject",
            r"const\s+uint64_t\s+previous_size",
            r"info\.size\s*=\s*appended_size",
            r"info\.offset\s*=\s*extents\.front\(\)\.offset",
            r"info\.extents\s*=\s*extents",
            r"AppendCommittedObjectChangeLocked\(\"append_object\"",
        ],
    )
    rpc_checks = _source_contains(
        rpc_cc,
        [
            r"request\.method\s*==\s*\"AppendObject\"",
            r"\[prefix\s*\+\s*\"offset\"\]\s*=\s*std::to_string\(info\.offset\)",
            r"\[extent_prefix\s*\+\s*\"offset\"\]\s*=\s*std::to_string\(info\.extents\[i\]\.offset\)",
            r"\[extent_prefix\s*\+\s*\"length\"\]\s*=\s*std::to_string\(info\.extents\[i\]\.length\)",
        ],
    )
    header_checks = _source_contains(
        object_h,
        [
            r"struct\s+ObjectInfo",
            r"uint64_t\s+offset\s*=\s*0",
            r"std::vector<ObjectExtent>\s+extents",
            r"Status\s+AppendObject",
        ],
    )
    rust_api_checks = _source_contains(
        rust_lib,
        [
            r"pub\s+fn\s+append_object",
            r"->\s*Result<ObjectMetadata,\s*ObjectError>",
            r"pub\s+struct\s+ObjectMetadata",
            r"pub\s+extents:\s+Vec<ObjectExtent>",
        ],
    )
    all_checks = {
        **{f"object_cc:{key}": value for key, value in object_cc_checks.items()},
        **{f"rpc_cc:{key}": value for key, value in rpc_checks.items()},
        **{f"object_h:{key}": value for key, value in header_checks.items()},
        **{f"rust_api:{key}": value for key, value in rust_api_checks.items()},
    }
    return {
        "backend": "cpp",
        "matrixobject_repo": str(matrixobject_repo),
        "matrixobject_commit": _git_rev(matrixobject_repo),
        "evidence_type": "source_contract",
        "append_object_returns_object_info": all(object_cc_checks.values()) and all(header_checks.values()),
        "rpc_exposes_offset_and_extents": all(rpc_checks.values()),
        "rust_matrixobject_api_exposes_metadata": all(rust_api_checks.values()),
        "checks": all_checks,
        "source_files": {
            "object_cc": str(object_cc),
            "objectstore_rpc_cc": str(rpc_cc),
            "objectstore_h": str(object_h),
            "rust_lib": str(rust_lib),
        },
    }




def _rust_replay_latency_total_us(rust_report: dict[str, Any], field: str) -> int | None:
    if not isinstance(rust_report, dict):
        return None
    total = 0
    found = False
    for phase in ("direct_publish", "sync_writer", "async_writer"):
        value = (
            rust_report.get(phase, {})
            .get("replay", {})
            .get(field)
        )
        if isinstance(value, int):
            total += value
            found = True
    return total if found else None

def _cpp_runtime_summary(cpp_report: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(cpp_report, dict):
        return {"runtime_valid": False, "reason": "missing runtime report"}
    return {
        "runtime_valid": cpp_report.get("status") == "passed"
        and cpp_report.get("engine") == "cpp_matrixobjectstore",
        "offsets_monotonic": bool(cpp_report.get("offsets_monotonic")),
        "offsets_contiguous": bool(cpp_report.get("offsets_contiguous")),
        "reopened_recovered_all_bytes": bool(cpp_report.get("reopened_recovered_all_bytes")),
        "full_read_matches": bool(cpp_report.get("full_read_matches")),
        "tail_read_matches": bool(cpp_report.get("tail_read_matches")),
        "offset_index_matches": bool(cpp_report.get("offset_index_matches")),
        "offset_index_object_size": cpp_report.get("offset_index_object_size"),
        "offset_index_extent_count": cpp_report.get("offset_index_extent_count"),
        "append_latency_avg_us": _avg_latency_us(cpp_report.get("appends")),
        "indexed_append_latency_avg_us": _avg_latency_us(cpp_report.get("appends"), "indexed_latency_us"),
        "reopen_latency_us": cpp_report.get("reopen_latency_us"),
        "read_full_latency_us": cpp_report.get("read_full_latency_us"),
        "read_tail_latency_us": cpp_report.get("read_tail_latency_us"),
        "reopened_extent_count": cpp_report.get("reopened_extent_count"),
        "read_cache_bytes": cpp_report.get("read_cache_bytes"),
        "read_cache_pages": cpp_report.get("read_cache_pages"),
        "read_cache_hits": cpp_report.get("read_cache_hits"),
        "read_cache_misses": cpp_report.get("read_cache_misses"),
        "read_cache_max_bytes": cpp_report.get("read_cache_max_bytes"),
    }


def _avg_latency_us(appends: Any, field: str = "latency_us") -> float | None:
    if not isinstance(appends, list) or not appends:
        return None
    values = [item.get(field) for item in appends if isinstance(item, dict)]
    return _avg_number_list(values)


def _avg_number_list(values: Any) -> float | None:
    if not isinstance(values, list) or not values:
        return None
    numeric = [float(value) for value in values if isinstance(value, (int, float))]
    if not numeric:
        return None
    return round(sum(numeric) / len(numeric), 3)

def _rust_summary(rust_report: dict[str, Any]) -> dict[str, Any]:
    summary = rust_report.get("summary") if isinstance(rust_report, dict) else None
    if not isinstance(summary, dict):
        return {
            "runtime_valid": False,
            "reason": "missing summary",
        }
    snapshot_reopen = rust_report.get("snapshot_reopen", {})
    snapshot_reopen_ok = bool(summary.get("snapshot_reopen_restores_offset_metadata")) and bool(
        summary.get("snapshot_reopen_recovered_all_records")
    )
    cache_metrics_available = bool(summary.get("snapshot_reopen_cache_metrics_available"))
    direct_publish = rust_report.get("direct_publish", {})
    sync_writer = rust_report.get("sync_writer", {})
    async_writer = rust_report.get("async_writer", {})
    return {
        "runtime_valid": rust_report.get("schema") == "temporalstore_shared_store_append_blob_parity_report_v1",
        "matrixobject_mode": rust_report.get("matrixobject_mode"),
        "storage_semantics": "persistent_snapshot_path_reopen_matrixobject_binding",
        "durable_reopen_equivalent": snapshot_reopen_ok,
        "durable_reopen_caveat": "Rust MatrixObjectStore now reloads from a configured persistent snapshot path; C++ still uses an incremental disk-root ObjectStore reopen.",
        "snapshot_reopen_restores_offset_metadata": bool(summary.get("snapshot_reopen_restores_offset_metadata")),
        "snapshot_reopen_recovered_all_records": bool(summary.get("snapshot_reopen_recovered_all_records")),
        "snapshot_reopen_cache_metrics_available": cache_metrics_available,
        "cache_before_replay": snapshot_reopen.get("cache_before_replay"),
        "cache_after_retrieval": snapshot_reopen.get("cache_after_retrieval"),
        "snapshot_export_latency_us": snapshot_reopen.get("snapshot_export_latency_us"),
        "snapshot_disk_write_latency_us": snapshot_reopen.get("snapshot_disk_write_latency_us"),
        "snapshot_disk_read_latency_us": snapshot_reopen.get("snapshot_disk_read_latency_us"),
        "snapshot_import_latency_us": snapshot_reopen.get("snapshot_import_latency_us"),
        "snapshot_bytes": snapshot_reopen.get("snapshot_bytes"),
        "offsets_monotonic": bool(summary.get("direct_offsets_monotonic")),
        "offsets_contiguous": bool(summary.get("direct_offsets_contiguous")),
        "direct_offset_slices_decode_expected_frames": bool(
            summary.get("direct_offset_slices_decode_expected_frames")
        ),
        "direct_offset_index_maps_oplog_to_blob_offsets": bool(
            summary.get("direct_offset_index_maps_oplog_to_blob_offsets")
        ),
        "secondary_replay_recovered_all_records": bool(
            summary.get("secondary_replay_recovered_all_records")
        ),
        "single_node_reload_recovered_all_records": bool(
            summary.get("single_node_reload_recovered_all_records")
        ),
        "sync_reports_include_offsets": bool(summary.get("sync_reports_include_offsets")),
        "async_flush_reports_include_offsets": bool(summary.get("async_flush_reports_include_offsets")),
        "replay_recovered_all_records": bool(summary.get("replay_recovered_all_records")),
        "retrieval_recovered_all_records": bool(summary.get("retrieval_recovered_all_records")),
        "append_latency_avg_us": summary.get("append_latency_avg_us"),
        "append_latency_p95_us": summary.get("append_latency_p95_us"),
        "direct_publish_latency_avg_us": _avg_number_list(direct_publish.get("publish_latencies_us")),
        "sync_writer_latency_avg_us": _avg_number_list(sync_writer.get("write_latencies_us")),
        "async_enqueue_latency_avg_us": _avg_number_list(async_writer.get("enqueue_latencies_us")),
        "async_flush_latency_us": async_writer.get("flush_latency_us"),
        "replay_latency_total_us": summary.get("replay_latency_total_us"),
        "retrieval_latency_avg_us": summary.get("retrieval_latency_avg_us"),
        "secondary_replay_latency_total_us": _rust_replay_latency_total_us(
            rust_report, "secondary_replay_latency_us"
        ),
        "single_node_reload_latency_total_us": _rust_replay_latency_total_us(
            rust_report, "single_node_reload_latency_us"
        ),
    }



def _latency_ratio(numerator: Any, denominator: Any) -> float | None:
    if not isinstance(numerator, (int, float)) or not isinstance(denominator, (int, float)):
        return None
    if denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 3)

def _parity_status(rust: dict[str, Any], cpp_contract: dict[str, Any], cpp_runtime: dict[str, Any]) -> dict[str, Any]:
    rust_summary = _rust_summary(rust)
    cpp_summary = _cpp_runtime_summary(cpp_runtime)
    append_latency_ratio = _latency_ratio(
        rust_summary.get("direct_publish_latency_avg_us"),
        cpp_summary.get("indexed_append_latency_avg_us") or cpp_summary.get("append_latency_avg_us"),
    )
    full_read_latency_ratio = _latency_ratio(
        rust_summary.get("retrieval_latency_avg_us"),
        cpp_summary.get("read_full_latency_us"),
    )
    feature_mismatches = []
    if not rust_summary.get("durable_reopen_equivalent"):
        feature_mismatches.append(
            "Rust MatrixObject binding did not prove disk snapshot reopen with offset metadata and records restored"
        )
    if rust_summary.get("durable_reopen_equivalent"):
        feature_mismatches.append(
            "Durability mechanism differs: Rust evidence is persistent snapshot-path reopen; C++ evidence is incremental disk-root ObjectStore reopen"
        )
    checks = {
        "rust_runtime_valid": bool(rust_summary.get("runtime_valid")),
        "rust_offsets_monotonic": bool(rust_summary.get("offsets_monotonic")),
        "rust_offsets_contiguous": bool(rust_summary.get("offsets_contiguous")),
        "rust_offset_slices_decode_expected_frames": bool(
            rust_summary.get("direct_offset_slices_decode_expected_frames")
        ),
        "rust_offset_index_maps_oplog_to_blob_offsets": bool(
            rust_summary.get("direct_offset_index_maps_oplog_to_blob_offsets")
        ),
        "rust_snapshot_reopen_restores_offset_metadata": bool(
            rust_summary.get("snapshot_reopen_restores_offset_metadata")
        ),
        "rust_snapshot_reopen_recovered_all_records": bool(
            rust_summary.get("snapshot_reopen_recovered_all_records")
        ),
        "rust_snapshot_reopen_cache_metrics_available": bool(
            rust_summary.get("snapshot_reopen_cache_metrics_available")
        ),
        "rust_durable_reopen_equivalent_to_cpp": bool(
            rust_summary.get("durable_reopen_equivalent")
        ),
        "rust_secondary_replay_recovered_all_records": bool(
            rust_summary.get("secondary_replay_recovered_all_records")
        ),
        "rust_single_node_reload_recovered_all_records": bool(
            rust_summary.get("single_node_reload_recovered_all_records")
        ),
        "rust_sync_reports_include_offsets": bool(rust_summary.get("sync_reports_include_offsets")),
        "rust_async_flush_reports_include_offsets": bool(
            rust_summary.get("async_flush_reports_include_offsets")
        ),
        "rust_replay_recovered_all_records": bool(rust_summary.get("replay_recovered_all_records")),
        "rust_retrieval_recovered_all_records": bool(
            rust_summary.get("retrieval_recovered_all_records")
        ),
        "cpp_runtime_valid": bool(cpp_summary.get("runtime_valid")),
        "cpp_offsets_monotonic": bool(cpp_summary.get("offsets_monotonic")),
        "cpp_offsets_contiguous": bool(cpp_summary.get("offsets_contiguous")),
        "cpp_reopen_recovered_all_bytes": bool(cpp_summary.get("reopened_recovered_all_bytes")),
        "cpp_full_read_matches": bool(cpp_summary.get("full_read_matches")),
        "cpp_tail_read_matches": bool(cpp_summary.get("tail_read_matches")),
        "cpp_offset_index_matches": bool(cpp_summary.get("offset_index_matches")),
        "cpp_runtime_cache_metrics_available": isinstance(cpp_summary.get("read_cache_max_bytes"), (int, float)),
        "cpp_append_object_returns_object_info": bool(cpp_contract.get("append_object_returns_object_info")),
        "cpp_rpc_exposes_offset_and_extents": bool(cpp_contract.get("rpc_exposes_offset_and_extents")),
        "matrixobject_rust_api_exposes_metadata": bool(
            cpp_contract.get("rust_matrixobject_api_exposes_metadata")
        ),
        "append_latency_ratio_within_2x_when_comparable": bool(
            rust_summary.get("durable_reopen_equivalent")
        )
        and append_latency_ratio is not None
        and append_latency_ratio <= 2.0,
        "retrieval_vs_cpp_full_read_latency_ratio_within_2x_when_comparable": bool(
            rust_summary.get("durable_reopen_equivalent")
        )
        and full_read_latency_ratio is not None
        and full_read_latency_ratio <= 2.0,
    }
    blockers = [name for name, passed in checks.items() if not passed]
    return {
        "status": "passed" if not blockers else "failed",
        "checks": checks,
        "blockers": blockers,
        "latency_ratios": {
            "rust_direct_publish_avg_us_to_cpp_indexed_append_avg_us": append_latency_ratio,
            "rust_retrieval_avg_us_to_cpp_full_read_us": full_read_latency_ratio,
            "threshold": "<=2.0x for Rust direct publish vs C++ indexed append when durable offset semantics are comparable",
            "comparable": bool(rust_summary.get("durable_reopen_equivalent")),
        },
        "feature_mismatches": feature_mismatches,
        "root_cause": (
            "Rust originally appeared much faster because the MatrixObject binding benchmark used "
            "an in-memory MatrixObjectStore, while the C++ benchmark used a disk-root ObjectStore, "
            "FlushForShutdown, process-style reopen, extent metadata recovery, and range reads. "
            "Rust now persists protobuf oplog-offset metadata and validates MatrixObjectStore reload "
            "from a configured persistent snapshot path. The latency gate compares Rust direct publish "
            "with C++ indexed append, because both include durable offset metadata; sync writer and "
            "async flush are reported separately. Rust durability is still snapshot-file based rather "
            "than C++ incremental disk-root reopen."
        ),
        "note": (
            "Rust TemporalStore MatrixObject append-blob runtime evidence now validates WAL frame "
            "offsets, a protobuf oplog-index offset metadata sidecar, and persistent snapshot-path "
            "reopen/readback. C++ MatrixObjectStore runtime evidence covers incremental disk-root "
            "reopen/readback."
        ),
    }


def _render_html(report: dict[str, Any]) -> str:
    status = report["parity"]["status"]
    rust_summary = report["rust_summary"]
    cpp = report["cpp_contract"]
    cpp_runtime_summary = report["cpp_runtime_summary"]
    rows = [
        ("Status", status),
        ("TemporalStore commit", report["temporalstore_commit"]),
        ("MatrixObject commit", cpp["matrixobject_commit"]),
        ("Rust mixed avg append latency us", rust_summary.get("append_latency_avg_us")),
        ("Rust direct publish avg latency us", rust_summary.get("direct_publish_latency_avg_us")),
        ("Rust sync writer avg latency us", rust_summary.get("sync_writer_latency_avg_us")),
        ("Rust async enqueue avg latency us", rust_summary.get("async_enqueue_latency_avg_us")),
        ("Rust async flush latency us", rust_summary.get("async_flush_latency_us")),
        ("Rust p95 append latency us", rust_summary.get("append_latency_p95_us")),
        ("Rust MatrixObject mode", rust_summary.get("matrixobject_mode")),
        ("Rust durable reopen equivalent", rust_summary.get("durable_reopen_equivalent")),
        ("Rust durable reopen caveat", rust_summary.get("durable_reopen_caveat")),
        ("Rust snapshot bytes", rust_summary.get("snapshot_bytes")),
        ("Rust snapshot export latency us", rust_summary.get("snapshot_export_latency_us")),
        ("Rust snapshot import latency us", rust_summary.get("snapshot_import_latency_us")),
        ("Rust total replay latency us", rust_summary.get("replay_latency_total_us")),
        ("Rust avg retrieval latency us", rust_summary.get("retrieval_latency_avg_us")),
        ("C++ raw append avg latency us", cpp_runtime_summary.get("append_latency_avg_us")),
        ("C++ indexed append avg latency us", cpp_runtime_summary.get("indexed_append_latency_avg_us")),
        ("C++ offset index matches", cpp_runtime_summary.get("offset_index_matches")),
        ("C++ offset index object size", cpp_runtime_summary.get("offset_index_object_size")),
        (
            "Rust direct/C++ indexed append latency ratio",
            report["parity"].get("latency_ratios", {}).get("rust_direct_publish_avg_us_to_cpp_indexed_append_avg_us"),
        ),
        (
            "Rust retrieval/C++ full read latency ratio",
            report["parity"].get("latency_ratios", {}).get("rust_retrieval_avg_us_to_cpp_full_read_us"),
        ),
        ("C++ reopen latency us", cpp_runtime_summary.get("reopen_latency_us")),
        ("C++ full read latency us", cpp_runtime_summary.get("read_full_latency_us")),
        ("C++ tail read latency us", cpp_runtime_summary.get("read_tail_latency_us")),
        ("C++ reopened extent count", cpp_runtime_summary.get("reopened_extent_count")),
        ("Rust cache after retrieval", rust_summary.get("cache_after_retrieval")),
        ("C++ read cache bytes", cpp_runtime_summary.get("read_cache_bytes")),
        ("C++ read cache pages", cpp_runtime_summary.get("read_cache_pages")),
        ("C++ read cache hits", cpp_runtime_summary.get("read_cache_hits")),
        ("C++ read cache misses", cpp_runtime_summary.get("read_cache_misses")),
    ]
    check_rows = "\n".join(
        f"<tr><td>{html.escape(key)}</td><td>{'pass' if value else 'fail'}</td></tr>"
        for key, value in report["parity"]["checks"].items()
    )
    summary_rows = "\n".join(
        f"<tr><td>{html.escape(str(name))}</td><td>{html.escape(str(value))}</td></tr>"
        for name, value in rows
    )
    raw = html.escape(json.dumps(report, indent=2, sort_keys=True))
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>TemporalStore Shared-Store Append Blob Parity</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #202124; }}
    h1, h2 {{ margin-bottom: 8px; }}
    table {{ border-collapse: collapse; margin: 16px 0; width: 100%; }}
    th, td {{ border: 1px solid #d0d7de; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f6f8fa; }}
    .pill {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: {'#dafbe1' if status == 'passed' else '#ffebe9'}; }}
    pre {{ white-space: pre-wrap; background: #f6f8fa; padding: 12px; border: 1px solid #d0d7de; overflow: auto; }}
  </style>
</head>
<body>
  <h1>TemporalStore Shared-Store Append Blob Parity</h1>
  <p><span class="pill">{html.escape(status)}</span></p>
  <h2>Summary</h2>
  <table><tbody>{summary_rows}</tbody></table>
  <h2>Parity Checks</h2>
  <table><thead><tr><th>Check</th><th>Result</th></tr></thead><tbody>{check_rows}</tbody></table>
  <h2>Raw Evidence</h2>
  <pre>{raw}</pre>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="docs/benchmarks/shared_store_blob_append_parity")
    parser.add_argument("--matrixobject-repo", default=str(DEFAULT_MATRIXOBJECT_REPO))
    parser.add_argument("--cargo-target-dir", default="/root/src/github-services/TemporalStore/target")
    parser.add_argument("--entries", type=int, default=8)
    parser.add_argument("--value-bytes", type=int, default=64)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--rust-profile", choices=["release", "dev"], default="release")
    parser.add_argument("--rust-report")
    parser.add_argument("--cpp-runtime-report")
    parser.add_argument("--cpp-runtime-bin")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    rust_runtime, command_report = _load_rust_runtime_report(args)
    cpp_runtime, cpp_command_report = _load_cpp_runtime_report(args)
    cpp_contract = _cpp_contract(Path(args.matrixobject_repo))
    report = {
        "schema": SCHEMA,
        "generated_at_unix": int(time.time()),
        "temporalstore_repo": str(ROOT),
        "temporalstore_commit": _git_rev(ROOT),
        "rust_command": command_report,
        "rust_summary": _rust_summary(rust_runtime),
        "rust_runtime": rust_runtime,
        "cpp_runtime_command": cpp_command_report,
        "cpp_runtime_summary": _cpp_runtime_summary(cpp_runtime),
        "cpp_runtime": cpp_runtime,
        "cpp_contract": cpp_contract,
    }
    report["parity"] = _parity_status(rust_runtime, cpp_contract, cpp_runtime)

    json_path = output_dir / "shared_store_blob_append_parity_report.json"
    html_path = output_dir / "shared_store_blob_append_parity_report.html"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    html_path.write_text(_render_html(report), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "html": str(html_path), "status": report["parity"]["status"]}, indent=2))
    return 0 if report["parity"]["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
