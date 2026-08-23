#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Generate cross-format shared-store append-blob parity evidence.

The Rust side is live runtime evidence from the MatrixObject-backed protobuf
append-blob WAL workflow. The side includes live MatrixObjectStore append
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
DEFAULT_MATRIXOBJECT_REPO = Path("/opt/github-services/MatrixObjectStore")
SCHEMA = "temporalstore_shared_store_blob_append_rust_parity_v2"


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
        "shared_store_append_blob_conformance_report",
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



def _load_runtime_report(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    if args.native_runtime_report:
        data = json.loads(Path(args.native_runtime_report).read_text(encoding="utf-8"))
        return data, {
            "mode": "loaded",
            "path": str(args.native_runtime_report),
            "returncode": 0,
            "stderr_tail": "",
        }

    bin_path = Path(args.native_runtime_bin) if args.native_runtime_bin else (
        Path(args.matrixobject_repo)
        / "build-mvp/matrixobjectstore/objectstore/objectstore_append_blob_parity_report"
    )
    if not bin_path.exists():
        return {
            "status": "missing",
            "reason": "native runtime emitter binary not found",
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
    runtime_root = root / "native_matrixobjectstore_runtime_root"
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

def _contract(matrixobject_repo: Path) -> dict[str, Any]:
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
        "backend": "native",
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

def _runtime_summary(native_report: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(native_report, dict):
        return {"runtime_valid": False, "reason": "missing runtime report"}
    append_latency_avg_us = _avg_latency_us(native_report.get("appends"))
    indexed_append_latency_avg_us = _avg_latency_us(native_report.get("appends"), "indexed_latency_us")
    read_full_latency_us = native_report.get("read_full_latency_us")
    read_tail_latency_us = native_report.get("read_tail_latency_us")
    return {
        "runtime_valid": native_report.get("status") == "passed"
        and native_report.get("engine") == "native_matrixobjectstore",
        "offsets_monotonic": bool(native_report.get("offsets_monotonic")),
        "offsets_contiguous": bool(native_report.get("offsets_contiguous")),
        "reopened_recovered_all_bytes": bool(native_report.get("reopened_recovered_all_bytes")),
        "full_read_matches": bool(native_report.get("full_read_matches")),
        "tail_read_matches": bool(native_report.get("tail_read_matches")),
        "offset_index_matches": bool(native_report.get("offset_index_matches")),
        "offset_index_object_size": native_report.get("offset_index_object_size"),
        "offset_index_extent_count": native_report.get("offset_index_extent_count"),
        "append_latency_avg_us": append_latency_avg_us,
        "indexed_append_latency_avg_us": indexed_append_latency_avg_us,
        "append_throughput_ops_per_sec": _throughput_ops_per_sec(append_latency_avg_us),
        "indexed_append_throughput_ops_per_sec": _throughput_ops_per_sec(indexed_append_latency_avg_us),
        "reopen_latency_us": native_report.get("reopen_latency_us"),
        "read_full_latency_us": read_full_latency_us,
        "read_tail_latency_us": read_tail_latency_us,
        "read_full_throughput_ops_per_sec": _throughput_ops_per_sec(read_full_latency_us),
        "read_tail_throughput_ops_per_sec": _throughput_ops_per_sec(read_tail_latency_us),
        "reopened_extent_count": native_report.get("reopened_extent_count"),
        "read_cache_bytes": native_report.get("read_cache_bytes"),
        "read_cache_pages": native_report.get("read_cache_pages"),
        "read_cache_hits": native_report.get("read_cache_hits"),
        "read_cache_misses": native_report.get("read_cache_misses"),
        "read_cache_max_bytes": native_report.get("read_cache_max_bytes"),
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


def _throughput_ops_per_sec(latency_us: Any) -> float | None:
    if not isinstance(latency_us, (int, float)) or latency_us <= 0:
        return None
    return round(1_000_000.0 / float(latency_us), 3)

def _rust_summary(rust_report: dict[str, Any]) -> dict[str, Any]:
    summary = rust_report.get("summary") if isinstance(rust_report, dict) else None
    if not isinstance(summary, dict):
        return {
            "runtime_valid": False,
            "reason": "missing summary",
        }
    snapshot_reopen = rust_report.get("journal_reopen") or rust_report.get("snapshot_reopen", {})
    snapshot_reopen_ok = bool(summary.get("snapshot_reopen_restores_offset_metadata")) and bool(
        summary.get("snapshot_reopen_recovered_all_records")
    )
    cache_metrics_available = bool(summary.get("snapshot_reopen_cache_metrics_available"))
    direct_publish = rust_report.get("direct_publish", {})
    sync_writer = rust_report.get("sync_writer", {})
    async_writer = rust_report.get("async_writer", {})
    append_latency_avg_us = summary.get("append_latency_avg_us")
    direct_publish_latency_avg_us = _avg_number_list(direct_publish.get("publish_latencies_us"))
    sync_writer_latency_avg_us = _avg_number_list(sync_writer.get("write_latencies_us"))
    async_enqueue_latency_avg_us = _avg_number_list(async_writer.get("enqueue_latencies_us"))
    retrieval_latency_avg_us = summary.get("retrieval_latency_avg_us")
    return {
        "runtime_valid": rust_report.get("schema") == "temporalstore_shared_store_append_blob_conformance_report_v1",
        "matrixobject_mode": rust_report.get("matrixobject_mode"),
        "storage_semantics": "incremental_journal_reopen_matrixobject_binding",
        "durable_reopen_equivalent": snapshot_reopen_ok,
        "durable_reopen_caveat": "Rust MatrixObjectStore now reloads from an incremental checksummed journal path; uses incremental disk-root ObjectStore reopen.",
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
        "authoritative_offset_lookup_reads_all_records": bool(
            summary.get("authoritative_offset_lookup_reads_all_records")
        ),
        "authoritative_offset_lookup_matches_all_records": bool(
            summary.get("authoritative_offset_lookup_matches_all_records")
        ),
        "authoritative_offset_lookup_has_extent_metadata": bool(
            summary.get("authoritative_offset_lookup_has_extent_metadata")
        ),
        "lower_layer_blob_offset_reads_proven": bool(
            summary.get("lower_layer_blob_offset_reads_proven")
        ),
        "lower_layer_range_reads_avoid_full_blob_scans": bool(
            summary.get("lower_layer_range_reads_avoid_full_blob_scans")
        ),
        "direct_authoritative_offset_lookup": direct_publish.get("authoritative_offset_lookup"),
        "sync_authoritative_offset_lookup": sync_writer.get("authoritative_offset_lookup"),
        "async_authoritative_offset_lookup": async_writer.get("authoritative_offset_lookup"),
        "direct_offset_metadata_mappings": direct_publish.get("offset_metadata_mappings"),
        "sync_offset_metadata_mappings": sync_writer.get("offset_metadata_mappings"),
        "async_offset_metadata_mappings": async_writer.get("offset_metadata_mappings"),
        "secondary_replay_recovered_all_records": bool(
            summary.get("secondary_replay_recovered_all_records")
        ),
        "single_node_reload_recovered_all_records": bool(
            summary.get("single_node_reload_recovered_all_records")
        ),
        "replay_uses_offset_index_metadata": bool(
            summary.get("replay_uses_offset_index_metadata")
        ),
        "secondary_replay_uses_offset_index_metadata": bool(
            summary.get("secondary_replay_uses_offset_index_metadata")
        ),
        "single_node_reload_uses_offset_index_metadata": bool(
            summary.get("single_node_reload_uses_offset_index_metadata")
        ),
        "sync_reports_include_offsets": bool(summary.get("sync_reports_include_offsets")),
        "async_flush_reports_include_offsets": bool(summary.get("async_flush_reports_include_offsets")),
        "replay_recovered_all_records": bool(summary.get("replay_recovered_all_records")),
        "retrieval_recovered_all_records": bool(summary.get("retrieval_recovered_all_records")),
        "append_latency_avg_us": append_latency_avg_us,
        "append_latency_p95_us": summary.get("append_latency_p95_us"),
        "direct_publish_latency_avg_us": direct_publish_latency_avg_us,
        "sync_writer_latency_avg_us": sync_writer_latency_avg_us,
        "async_enqueue_latency_avg_us": async_enqueue_latency_avg_us,
        "append_throughput_ops_per_sec": _throughput_ops_per_sec(append_latency_avg_us),
        "direct_publish_throughput_ops_per_sec": _throughput_ops_per_sec(direct_publish_latency_avg_us),
        "sync_writer_throughput_ops_per_sec": _throughput_ops_per_sec(sync_writer_latency_avg_us),
        "async_enqueue_throughput_ops_per_sec": _throughput_ops_per_sec(async_enqueue_latency_avg_us),
        "async_flush_latency_us": async_writer.get("flush_latency_us"),
        "replay_latency_total_us": summary.get("replay_latency_total_us"),
        "retrieval_latency_avg_us": retrieval_latency_avg_us,
        "retrieval_throughput_ops_per_sec": _throughput_ops_per_sec(retrieval_latency_avg_us),
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


def _throughput_ratio(numerator: Any, denominator: Any) -> float | None:
    if not isinstance(numerator, (int, float)) or not isinstance(denominator, (int, float)):
        return None
    if denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 3)

def _parity_status(
    rust: dict[str, Any],
    native_contract: dict[str, Any],
    native_runtime: dict[str, Any],
    *,
    rust_profile: str = "release",
) -> dict[str, Any]:
    rust_summary = _rust_summary(rust)
    native_summary = _runtime_summary(native_runtime)
    release_latency_gate = rust_profile == "release"
    append_latency_ratio = _latency_ratio(
        rust_summary.get("direct_publish_latency_avg_us"),
        native_summary.get("indexed_append_latency_avg_us") or native_summary.get("append_latency_avg_us"),
    )
    full_read_latency_ratio = _latency_ratio(
        rust_summary.get("retrieval_latency_avg_us"),
        native_summary.get("read_full_latency_us"),
    )
    append_throughput_ratio = _throughput_ratio(
        rust_summary.get("direct_publish_throughput_ops_per_sec"),
        native_summary.get("indexed_append_throughput_ops_per_sec"),
    )
    retrieval_throughput_ratio = _throughput_ratio(
        rust_summary.get("retrieval_throughput_ops_per_sec"),
        native_summary.get("read_full_throughput_ops_per_sec"),
    )
    feature_mismatches = []
    if not rust_summary.get("durable_reopen_equivalent"):
        feature_mismatches.append(
            "Rust MatrixObject binding did not prove incremental journal/disk reopen with offset metadata and records restored"
        )
    if not (
        rust_summary.get("replay_uses_offset_index_metadata")
        and rust_summary.get("secondary_replay_uses_offset_index_metadata")
        and rust_summary.get("single_node_reload_uses_offset_index_metadata")
    ):
        feature_mismatches.append(
            "Rust replay/reload did not prove offset-index range reads for primary replay, secondary replay, and single-node reload"
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
        "rust_authoritative_offset_lookup_reads_all_records": bool(
            rust_summary.get("authoritative_offset_lookup_reads_all_records")
        ),
        "rust_authoritative_offset_lookup_matches_all_records": bool(
            rust_summary.get("authoritative_offset_lookup_matches_all_records")
        ),
        "rust_authoritative_offset_lookup_has_extent_metadata": bool(
            rust_summary.get("authoritative_offset_lookup_has_extent_metadata")
        ),
        "rust_lower_layer_blob_offset_reads_proven": bool(
            rust_summary.get("lower_layer_blob_offset_reads_proven")
        ),
        "rust_lower_layer_range_reads_avoid_full_blob_scans": bool(
            rust_summary.get("lower_layer_range_reads_avoid_full_blob_scans")
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
        "rust_durable_reopen_equivalent_to_native": bool(
            rust_summary.get("durable_reopen_equivalent")
        ),
        "rust_secondary_replay_recovered_all_records": bool(
            rust_summary.get("secondary_replay_recovered_all_records")
        ),
        "rust_single_node_reload_recovered_all_records": bool(
            rust_summary.get("single_node_reload_recovered_all_records")
        ),
        "rust_replay_uses_offset_index_metadata": bool(
            rust_summary.get("replay_uses_offset_index_metadata")
        ),
        "rust_secondary_replay_uses_offset_index_metadata": bool(
            rust_summary.get("secondary_replay_uses_offset_index_metadata")
        ),
        "rust_single_node_reload_uses_offset_index_metadata": bool(
            rust_summary.get("single_node_reload_uses_offset_index_metadata")
        ),
        "rust_sync_reports_include_offsets": bool(rust_summary.get("sync_reports_include_offsets")),
        "rust_async_flush_reports_include_offsets": bool(
            rust_summary.get("async_flush_reports_include_offsets")
        ),
        "rust_replay_recovered_all_records": bool(rust_summary.get("replay_recovered_all_records")),
        "rust_retrieval_recovered_all_records": bool(
            rust_summary.get("retrieval_recovered_all_records")
        ),
        "native_runtime_valid": bool(native_summary.get("runtime_valid")),
        "native_offsets_monotonic": bool(native_summary.get("offsets_monotonic")),
        "native_offsets_contiguous": bool(native_summary.get("offsets_contiguous")),
        "native_reopen_recovered_all_bytes": bool(native_summary.get("reopened_recovered_all_bytes")),
        "native_full_read_matches": bool(native_summary.get("full_read_matches")),
        "native_tail_read_matches": bool(native_summary.get("tail_read_matches")),
        "native_offset_index_matches": bool(native_summary.get("offset_index_matches")),
        "native_runtime_cache_metrics_available": isinstance(native_summary.get("read_cache_max_bytes"), (int, float)),
        "native_append_object_returns_object_info": bool(native_contract.get("append_object_returns_object_info")),
        "native_rpc_exposes_offset_and_extents": bool(native_contract.get("rpc_exposes_offset_and_extents")),
        "matrixobject_rust_api_exposes_metadata": bool(
            native_contract.get("rust_matrixobject_api_exposes_metadata")
        ),
        "append_latency_ratio_within_2x_when_comparable": (
            not release_latency_gate
            or (
                bool(rust_summary.get("durable_reopen_equivalent"))
                and append_latency_ratio is not None
                and append_latency_ratio <= 2.0
            )
        ),
        "retrieval_vs_full_read_latency_ratio_within_2x_when_comparable": (
            not release_latency_gate
            or (
                bool(rust_summary.get("durable_reopen_equivalent"))
                and full_read_latency_ratio is not None
                and full_read_latency_ratio <= 2.0
            )
        ),
        "append_throughput_ratio_at_least_0_5x_when_comparable": (
            not release_latency_gate
            or (
                bool(rust_summary.get("durable_reopen_equivalent"))
                and append_throughput_ratio is not None
                and append_throughput_ratio >= 0.5
            )
        ),
        "retrieval_vs_full_read_throughput_ratio_at_least_0_5x_when_comparable": (
            not release_latency_gate
            or (
                bool(rust_summary.get("durable_reopen_equivalent"))
                and retrieval_throughput_ratio is not None
                and retrieval_throughput_ratio >= 0.5
            )
        ),
    }
    blockers = [name for name, passed in checks.items() if not passed]
    performance_ratios = {
        "rust_direct_publish_avg_us_to_indexed_append_avg_us": append_latency_ratio,
        "rust_retrieval_avg_us_to_full_read_us": full_read_latency_ratio,
        "rust_direct_publish_ops_per_sec_to_indexed_append_ops_per_sec": append_throughput_ratio,
        "rust_retrieval_ops_per_sec_to_full_read_ops_per_sec": retrieval_throughput_ratio,
        "threshold": "<=2.0x for Rust direct publish vs indexed append when durable offset semantics are comparable",
        "throughput_threshold": ">=0.5x for Rust direct publish vs indexed append when durable offset semantics are comparable",
        "comparable": bool(rust_summary.get("durable_reopen_equivalent")),
        "gate_profile": rust_profile,
        "release_profile_required_for_latency_gate": release_latency_gate,
    }
    return {
        "status": "passed" if not blockers else "failed",
        "checks": checks,
        "blockers": blockers,
        "performance_ratios": performance_ratios,
        "latency_ratios": performance_ratios,
        "feature_mismatches": feature_mismatches,
        "root_cause": (
            "Rust originally appeared much faster because the MatrixObject binding benchmark used "
            "an in-memory MatrixObjectStore, while the benchmark used a disk-root ObjectStore, "
            "FlushForShutdown, process-style reopen, extent metadata recovery, and range reads. "
            "Rust now persists protobuf oplog-offset metadata and validates MatrixObjectStore reload "
            "from an incremental checksummed journal path. The latency gate compares Rust direct publish "
            "with indexed append, because both include durable offset metadata; sync writer and "
            "async flush are reported separately. Throughput is reported from the same average latency "
            "samples so release parity covers both latency and ops/sec views of the comparable paths."
        ),
        "note": (
            "Rust TemporalStore MatrixObject append-blob runtime evidence now validates WAL frame "
            "offsets, a protobuf oplog-index offset metadata sidecar, authoritative oplog-index "
            "metadata lookup/range reads, and incremental journal reopen/readback. MatrixObjectStore runtime evidence covers incremental disk-root "
            "reopen/readback."
        ),
    }


def _render_html(report: dict[str, Any]) -> str:
    status = report["parity"]["status"]
    rust_summary = report["rust_summary"]
    native = report["native_contract"]
    native_runtime_summary = report["native_runtime_summary"]
    performance_ratios = report["parity"].get("performance_ratios") or report["parity"].get("latency_ratios", {})
    rows = [
        ("Status", status),
        ("TemporalStore commit", report["temporalstore_commit"]),
        ("MatrixObject commit", native["matrixobject_commit"]),
        ("Rust mixed avg append latency us", rust_summary.get("append_latency_avg_us")),
        ("Rust direct publish avg latency us", rust_summary.get("direct_publish_latency_avg_us")),
        ("Rust sync writer avg latency us", rust_summary.get("sync_writer_latency_avg_us")),
        ("Rust async enqueue avg latency us", rust_summary.get("async_enqueue_latency_avg_us")),
        ("Rust async flush latency us", rust_summary.get("async_flush_latency_us")),
        ("Rust p95 append latency us", rust_summary.get("append_latency_p95_us")),
        ("Rust mixed append throughput ops/sec", rust_summary.get("append_throughput_ops_per_sec")),
        ("Rust direct publish throughput ops/sec", rust_summary.get("direct_publish_throughput_ops_per_sec")),
        ("Rust sync writer throughput ops/sec", rust_summary.get("sync_writer_throughput_ops_per_sec")),
        ("Rust async enqueue throughput ops/sec", rust_summary.get("async_enqueue_throughput_ops_per_sec")),
        ("Rust MatrixObject mode", rust_summary.get("matrixobject_mode")),
        ("Rust durable reopen equivalent", rust_summary.get("durable_reopen_equivalent")),
        ("Rust durable reopen caveat", rust_summary.get("durable_reopen_caveat")),
        ("Rust authoritative offset lookup reads all records", rust_summary.get("authoritative_offset_lookup_reads_all_records")),
        ("Rust authoritative offset lookup matches all records", rust_summary.get("authoritative_offset_lookup_matches_all_records")),
        ("Rust authoritative offset lookup has extent metadata", rust_summary.get("authoritative_offset_lookup_has_extent_metadata")),
        ("Rust lower-layer blob offset reads proven", rust_summary.get("lower_layer_blob_offset_reads_proven")),
        ("Rust lower-layer range reads avoid full blob scans", rust_summary.get("lower_layer_range_reads_avoid_full_blob_scans")),
        ("Rust direct authoritative offset lookup", rust_summary.get("direct_authoritative_offset_lookup")),
        ("Rust sync authoritative offset lookup", rust_summary.get("sync_authoritative_offset_lookup")),
        ("Rust async authoritative offset lookup", rust_summary.get("async_authoritative_offset_lookup")),
        ("Rust replay uses offset-index metadata", rust_summary.get("replay_uses_offset_index_metadata")),
        ("Rust secondary replay uses offset-index metadata", rust_summary.get("secondary_replay_uses_offset_index_metadata")),
        ("Rust single-node reload uses offset-index metadata", rust_summary.get("single_node_reload_uses_offset_index_metadata")),
        ("Rust direct offset metadata mappings", rust_summary.get("direct_offset_metadata_mappings")),
        ("Rust sync offset metadata mappings", rust_summary.get("sync_offset_metadata_mappings")),
        ("Rust async offset metadata mappings", rust_summary.get("async_offset_metadata_mappings")),
        ("Rust snapshot bytes", rust_summary.get("snapshot_bytes")),
        ("Rust snapshot export latency us", rust_summary.get("snapshot_export_latency_us")),
        ("Rust snapshot import latency us", rust_summary.get("snapshot_import_latency_us")),
        ("Rust total replay latency us", rust_summary.get("replay_latency_total_us")),
        ("Rust avg retrieval latency us", rust_summary.get("retrieval_latency_avg_us")),
        ("Rust retrieval throughput ops/sec", rust_summary.get("retrieval_throughput_ops_per_sec")),
        ("raw append avg latency us", native_runtime_summary.get("append_latency_avg_us")),
        ("indexed append avg latency us", native_runtime_summary.get("indexed_append_latency_avg_us")),
        ("raw append throughput ops/sec", native_runtime_summary.get("append_throughput_ops_per_sec")),
        ("indexed append throughput ops/sec", native_runtime_summary.get("indexed_append_throughput_ops_per_sec")),
        ("offset index matches", native_runtime_summary.get("offset_index_matches")),
        ("offset index object size", native_runtime_summary.get("offset_index_object_size")),
        (
            "Rust direct/indexed append latency ratio",
            performance_ratios.get("rust_direct_publish_avg_us_to_indexed_append_avg_us"),
        ),
        (
            "Rust retrieval/full read latency ratio",
            performance_ratios.get("rust_retrieval_avg_us_to_full_read_us"),
        ),
        (
            "Rust direct/indexed append throughput ratio",
            performance_ratios.get("rust_direct_publish_ops_per_sec_to_indexed_append_ops_per_sec"),
        ),
        (
            "Rust retrieval/full read throughput ratio",
            performance_ratios.get("rust_retrieval_ops_per_sec_to_full_read_ops_per_sec"),
        ),
        ("reopen latency us", native_runtime_summary.get("reopen_latency_us")),
        ("full read latency us", native_runtime_summary.get("read_full_latency_us")),
        ("tail read latency us", native_runtime_summary.get("read_tail_latency_us")),
        ("full read throughput ops/sec", native_runtime_summary.get("read_full_throughput_ops_per_sec")),
        ("tail read throughput ops/sec", native_runtime_summary.get("read_tail_throughput_ops_per_sec")),
        ("reopened extent count", native_runtime_summary.get("reopened_extent_count")),
        ("Rust cache after retrieval", rust_summary.get("cache_after_retrieval")),
        ("read cache bytes", native_runtime_summary.get("read_cache_bytes")),
        ("read cache pages", native_runtime_summary.get("read_cache_pages")),
        ("read cache hits", native_runtime_summary.get("read_cache_hits")),
        ("read cache misses", native_runtime_summary.get("read_cache_misses")),
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
    parser.add_argument("--cargo-target-dir", default="/opt/github-services/TemporalStore/target")
    parser.add_argument("--entries", type=int, default=8)
    parser.add_argument("--value-bytes", type=int, default=64)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--rust-profile", choices=["release", "dev"], default="release")
    parser.add_argument("--rust-report")
    parser.add_argument("--native-runtime-report")
    parser.add_argument("--native-runtime-bin")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    rust_runtime, command_report = _load_rust_runtime_report(args)
    native_runtime, native_command_report = _load_runtime_report(args)
    native_contract = _contract(Path(args.matrixobject_repo))
    report = {
        "schema": SCHEMA,
        "generated_at_unix": int(time.time()),
        "temporalstore_repo": str(ROOT),
        "temporalstore_commit": _git_rev(ROOT),
        "rust_command": command_report,
        "rust_summary": _rust_summary(rust_runtime),
        "rust_runtime": rust_runtime,
        "native_runtime_command": native_command_report,
        "native_runtime_summary": _runtime_summary(native_runtime),
        "native_runtime": native_runtime,
        "native_contract": native_contract,
    }
    report["parity"] = _parity_status(
        rust_runtime,
        native_contract,
        native_runtime,
        rust_profile=args.rust_profile,
    )

    json_path = output_dir / "shared_store_blob_append_parity_report.json"
    html_path = output_dir / "shared_store_blob_append_parity_report.html"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    html_path.write_text(_render_html(report), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "html": str(html_path), "status": report["parity"]["status"]}, indent=2))
    return 0 if report["parity"]["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
