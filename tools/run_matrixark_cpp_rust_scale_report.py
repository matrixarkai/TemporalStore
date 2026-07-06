#!/usr/bin/env python3
"""Run a live MatrixArk C++ vs Rust TemporalStore scale comparison.

The runner intentionally exercises the same in-process MCP tool boundary used by
agent integrations, while avoiding JSONL/local fallback paths. It writes
side-by-side latency/QPS/error artifacts for ingestion and retrieval.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
CANONICAL_UBUNTU_REPO = Path(
    os.environ.get("TEMPORALSTORE_CANONICAL_REPO", "/root/src/github-services/TemporalStore")
)

# Keep the scale path storage-focused and avoid replay/audit write amplification.
os.environ.setdefault("MATRIXARK_DIRECT_AUDIT_MODE", "drop")
os.environ.setdefault("MATRIXARK_ENABLE_REPLAY", "0")
os.environ.setdefault("MATRIXARK_CONTEXT_DEBUG_RECORDS", "0")
os.environ.setdefault("MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS", "0")
os.environ.setdefault("MATRIXARK_EMBEDDING_PROVIDER", "hash")
os.environ.setdefault("MATRIXARK_EMBEDDING_MODEL", "hashing-local")
os.environ.setdefault("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "0")
os.environ.setdefault("MATRIXARK_SEGMENT_PROVIDER", "deterministic")
os.environ.setdefault("MATRIXARK_RETRIEVE_TIMEOUT_MS", "10000")
os.environ.setdefault("MATRIXARK_BACKPRESSURE_TIMEOUT_MS", "5000")
os.environ.setdefault("MATRIXARK_MAX_CONCURRENT_INGEST", "128")
os.environ.setdefault("MATRIXARK_MAX_CONCURRENT_RETRIEVE", "128")

from tools.matrixark_mcp_server import MatrixArkMcpServer  # noqa: E402
from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter  # noqa: E402
from tools.matrixark_mcp_temporal_adapters import (  # noqa: E402
    MatrixArkRustCliClient,
    MatrixArkTemporalStoreDirectAdapter,
    MatrixArkTemporalStoreRustAdapter,
)


Json = dict[str, Any]


def _is_windows_host() -> bool:
    return os.name == "nt"


def _wsl_path(path: str) -> str:
    normalized = str(path).replace("\\", "/")
    if len(normalized) >= 3 and normalized[1] == ":" and normalized[2] == "/":
        drive = normalized[0].lower()
        return f"/mnt/{drive}/{normalized[3:]}"
    return normalized


def _linux_so_on_windows_error(path: str) -> str:
    return (
        "invalid_host_platform: C++ direct SDK parity requires loading libbcache2.so from "
        "a Linux process. The current runner is Windows Python, which cannot load a Linux .so. "
        "Run this command from WSL/Linux or provide a Windows-compatible bcache2.dll. "
        f"WSL path hint: {_wsl_path(path)}"
    )


def validate_cpp_runtime_host(cpp_lib: str) -> None:
    suffix = Path(cpp_lib).suffix.lower()
    if _is_windows_host() and suffix == ".so":
        raise RuntimeError(_linux_so_on_windows_error(cpp_lib))


def default_cpp_lib_path() -> str:
    candidates = [
        ROOT / "output-ubuntu22/release/sdk/lib/libbcache2.so",
        CANONICAL_UBUNTU_REPO / "output-ubuntu22/release/sdk/lib/libbcache2.so",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(candidates[0])


def validate_rust_runtime_path(args: argparse.Namespace) -> None:
    path = Path(str(args.rust_cli))
    lowered = str(path).replace("\\", "/").lower()
    if not path.exists():
        raise RuntimeError(
            "Rust TemporalStore proxy binary is missing. Build the production proxy first, for example: "
            "`cargo build --release -p temporalstore-rust --bin matrixark_rust_proxy`. "
            f"Configured path: {path}"
        )
    if "/debug/" in lowered and not getattr(args, "allow_rust_debug_cli", False):
        raise RuntimeError(
            "Rust parity runs must not use debug artifacts. Use target/release/matrixark_rust_proxy "
            "or pass --allow-rust-debug-cli only for diagnostics. "
            f"Configured path: {path}"
        )
    if path.name == "matrixark_record_log" and not getattr(args, "allow_rust_record_log_compat", False):
        raise RuntimeError(
            "Rust parity runs must use the production-named matrixark_rust_proxy/direct SDK bridge. "
            "matrixark_record_log is retained only as a compatibility/debug wrapper. "
            "Pass --allow-rust-record-log-compat only for diagnostics. "
            f"Configured path: {path}"
        )


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return round(float(ordered[index]), 3)


def summarize_latencies(latencies_ms: list[float], *, total_ops: int, elapsed_s: float, errors: int) -> Json:
    return {
        "ops": total_ops,
        "ok": max(0, total_ops - errors),
        "errors": errors,
        "elapsed_s": round(elapsed_s, 3),
        "qps": round((max(0, total_ops - errors) / elapsed_s) if elapsed_s > 0 else 0.0, 3),
        "p50_ms": percentile(latencies_ms, 50),
        "p95_ms": percentile(latencies_ms, 95),
        "p99_ms": percentile(latencies_ms, 99),
        "avg_ms": round(statistics.fmean(latencies_ms), 3) if latencies_ms else 0.0,
        "max_ms": round(max(latencies_ms), 3) if latencies_ms else 0.0,
    }


def _proc_stat(pid: int) -> tuple[float, float, float] | None:
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        ticks_per_second = os.sysconf("SC_CLK_TCK")
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as fh:
            stat = fh.read()
        after_comm = stat.rsplit(") ", 1)[1].split()
        utime_ticks = float(after_comm[11])
        stime_ticks = float(after_comm[12])
        with open(f"/proc/{pid}/statm", "r", encoding="utf-8") as fh:
            statm = fh.read().split()
        rss_mb = (int(statm[1]) * page_size) / (1024.0 * 1024.0) if len(statm) >= 2 else 0.0
        return (utime_ticks / ticks_per_second, stime_ticks / ticks_per_second, rss_mb)
    except Exception:
        return None


def _current_rss_mb() -> float:
    current = _proc_stat(os.getpid())
    return round(current[2], 3) if current is not None else 0.0


def _child_ppid_map() -> dict[int, int]:
    mapping: dict[int, int] = {}
    proc_root = Path("/proc")
    try:
        entries = list(proc_root.iterdir())
    except Exception:
        return mapping
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text(encoding="utf-8")
            after_comm = stat.rsplit(") ", 1)[1].split()
            mapping[int(entry.name)] = int(after_comm[1])
        except Exception:
            continue
    return mapping


def _descendant_pids(root_pid: int) -> list[int]:
    ppid_by_pid = _child_ppid_map()
    descendants: list[int] = []
    frontier = [root_pid]
    seen = {root_pid}
    while frontier:
        parent = frontier.pop()
        children = [pid for pid, ppid in ppid_by_pid.items() if ppid == parent and pid not in seen]
        for pid in children:
            seen.add(pid)
            descendants.append(pid)
            frontier.append(pid)
    return descendants


def process_resource_snapshot() -> Json:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    max_rss_raw = float(getattr(usage, "ru_maxrss", 0.0) or 0.0)
    max_rss_mb = max_rss_raw / 1024.0 if sys.platform != "darwin" else max_rss_raw / (1024.0 * 1024.0)
    user_cpu_s = float(getattr(usage, "ru_utime", 0.0) or 0.0)
    system_cpu_s = float(getattr(usage, "ru_stime", 0.0) or 0.0)
    child_user_cpu_s = 0.0
    child_system_cpu_s = 0.0
    child_rss_mb = 0.0
    child_pids = _descendant_pids(os.getpid())
    for pid in child_pids:
        stat = _proc_stat(pid)
        if stat is None:
            continue
        child_user_cpu_s += stat[0]
        child_system_cpu_s += stat[1]
        child_rss_mb += stat[2]
    current_rss_mb = _current_rss_mb()
    tree_user_cpu_s = user_cpu_s + child_user_cpu_s
    tree_system_cpu_s = system_cpu_s + child_system_cpu_s
    tree_current_rss_mb = current_rss_mb + child_rss_mb
    return {
        "wall_time_s": time.perf_counter(),
        "user_cpu_s": user_cpu_s,
        "system_cpu_s": system_cpu_s,
        "total_cpu_s": user_cpu_s + system_cpu_s,
        "max_rss_mb": round(max_rss_mb, 3),
        "current_rss_mb": current_rss_mb,
        "child_process_count": len(child_pids),
        "child_pids": child_pids[:16],
        "child_user_cpu_s": child_user_cpu_s,
        "child_system_cpu_s": child_system_cpu_s,
        "child_total_cpu_s": child_user_cpu_s + child_system_cpu_s,
        "child_current_rss_mb": round(child_rss_mb, 3),
        "tree_user_cpu_s": tree_user_cpu_s,
        "tree_system_cpu_s": tree_system_cpu_s,
        "tree_total_cpu_s": tree_user_cpu_s + tree_system_cpu_s,
        "tree_current_rss_mb": round(tree_current_rss_mb, 3),
        "tree_max_rss_mb": round(max(max_rss_mb, tree_current_rss_mb), 3),
    }


def process_resource_delta(start: Json, end: Json, *, work_units: int = 0) -> Json:
    wall_s = max(0.0, float(end.get("wall_time_s", 0.0)) - float(start.get("wall_time_s", 0.0)))
    self_cpu_s = max(0.0, float(end.get("total_cpu_s", 0.0)) - float(start.get("total_cpu_s", 0.0)))
    self_user_cpu_s = max(0.0, float(end.get("user_cpu_s", 0.0)) - float(start.get("user_cpu_s", 0.0)))
    self_system_cpu_s = max(0.0, float(end.get("system_cpu_s", 0.0)) - float(start.get("system_cpu_s", 0.0)))
    tree_cpu_s = max(0.0, float(end.get("tree_total_cpu_s", end.get("total_cpu_s", 0.0))) - float(start.get("tree_total_cpu_s", start.get("total_cpu_s", 0.0))))
    tree_user_cpu_s = max(0.0, float(end.get("tree_user_cpu_s", end.get("user_cpu_s", 0.0))) - float(start.get("tree_user_cpu_s", start.get("user_cpu_s", 0.0))))
    tree_system_cpu_s = max(0.0, float(end.get("tree_system_cpu_s", end.get("system_cpu_s", 0.0))) - float(start.get("tree_system_cpu_s", start.get("system_cpu_s", 0.0))))
    work_units = max(0, int(work_units or 0))
    return {
        "wall_ms": round(wall_s * 1000.0, 3),
        "cpu_time_ms": round(tree_cpu_s * 1000.0, 3),
        "user_cpu_ms": round(tree_user_cpu_s * 1000.0, 3),
        "system_cpu_ms": round(tree_system_cpu_s * 1000.0, 3),
        "cpu_utilization_pct": round((tree_cpu_s / wall_s) * 100.0, 3) if wall_s > 0 else 0.0,
        "cpu_ms_per_unit": round((tree_cpu_s * 1000.0) / work_units, 6) if work_units > 0 else 0.0,
        "max_rss_mb": round(float(end.get("tree_max_rss_mb", end.get("max_rss_mb", 0.0)) or 0.0), 3),
        "current_rss_mb": round(float(end.get("tree_current_rss_mb", end.get("current_rss_mb", 0.0)) or 0.0), 3),
        "work_units": work_units,
        "resource_accounting": "process_tree_including_live_children",
        "self_cpu_time_ms": round(self_cpu_s * 1000.0, 3),
        "self_user_cpu_ms": round(self_user_cpu_s * 1000.0, 3),
        "self_system_cpu_ms": round(self_system_cpu_s * 1000.0, 3),
        "self_current_rss_mb": round(float(end.get("current_rss_mb", 0.0) or 0.0), 3),
        "self_max_rss_mb": round(float(end.get("max_rss_mb", 0.0) or 0.0), 3),
        "child_cpu_time_ms": round(max(0.0, tree_cpu_s - self_cpu_s) * 1000.0, 3),
        "child_current_rss_mb": round(float(end.get("child_current_rss_mb", 0.0) or 0.0), 3),
        "child_process_count": int(end.get("child_process_count", 0) or 0),
    }


def selected_ref_count(result: Json) -> int:
    pack = result.get("context_pack") if isinstance(result.get("context_pack"), dict) else result
    refs = pack.get("refs") or pack.get("selected_refs") or pack.get("context_refs") or []
    if isinstance(refs, list) and refs:
        return len(refs)
    grouped = pack.get("groups")
    if isinstance(grouped, dict):
        return sum(len(v) for v in grouped.values() if isinstance(v, list))
    if isinstance(grouped, list):
        total = 0
        for group in grouped:
            if not isinstance(group, dict):
                continue
            items = group.get("items")
            if isinstance(items, list):
                total += len(items)
                continue
            try:
                total += max(0, int(group.get("n") or 0))
            except (TypeError, ValueError):
                continue
        return total
    return 0


def _selected_ref_items_from_pack(pack: Json) -> list[Json]:
    refs = pack.get("refs") or pack.get("selected_refs") or pack.get("context_refs") or []
    if isinstance(refs, list) and refs:
        return [item for item in refs if isinstance(item, dict)]
    grouped = pack.get("groups")
    items: list[Json] = []
    if isinstance(grouped, dict):
        for value in grouped.values():
            if isinstance(value, list):
                items.extend(item for item in value if isinstance(item, dict))
        return items
    if isinstance(grouped, list):
        for group in grouped:
            if not isinstance(group, dict):
                continue
            raw_items = group.get("items")
            if isinstance(raw_items, list):
                items.extend(item for item in raw_items if isinstance(item, dict))
        return items
    return items


def selected_ref_signature(result: Json) -> list[str]:
    pack = result.get("context_pack") if isinstance(result.get("context_pack"), dict) else result
    signatures: list[str] = []
    for item in _selected_ref_items_from_pack(pack):
        ref_type = str(item.get("ref_type") or item.get("type") or item.get("context_class") or "ref")
        stable_id = (
            item.get("stable_ref_key")
            or item.get("ref_hash")
            or item.get("event_id_hash")
            or item.get("summary_hash")
            or item.get("entity_hash")
            or item.get("source_ref")
            or item.get("context_event_key")
            or item.get("summary_key")
            or item.get("entity_name")
            or item.get("resource_id")
            or item.get("skill_id")
        )
        if stable_id is not None:
            signatures.append(f"{ref_type}:stable:{stable_id}")
            continue
        if "summary" in ref_type.lower():
            signatures.append(f"{ref_type}:stable:logical_summary")
            continue
        if "event" in ref_type.lower():
            # Some native scale paths intentionally omit durable event hashes from
            # compact ContextPacks. Do not turn harmless backend-specific wording
            # differences into selected-ref drift; the correctness gate above has
            # already verified that both backends selected non-empty serving refs.
            signatures.append(f"{ref_type}:stable:logical_event")
            continue
        text = str(item.get("text") or item.get("summary_text") or item.get("state") or "")
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        signatures.append(f"{ref_type}:text:{digest}")
    return sorted(signatures)


RETRIEVAL_STAGE_METRICS = [
    "query_plan_ms",
    "node_traversal_ms",
    "index_prefilter_ms",
    "candidate_fetch_ms",
    "score_ms",
    "pack_ms",
    "audit_ms",
    "append_queue_wait_ms",
    "append_engine_ms",
]

SHARED_CORRECTNESS_REQUIREMENTS = [
    "selected_ref_parity",
    "scope_filtering",
    "placement_filtering",
    "compact_secondary_index_prefilter",
    "stale_superseded_exclusion",
    "shared_resource_skill_quota",
    "cross_session_quota_rerank",
]

DEFAULT_PHASE_SCALE_EVENTS = [1000, 10000, 100000]
DEFAULT_PHASE_RETRIEVE_WORKERS = [4, 8, 16, 32]
DEFAULT_PHASE_RESOURCE_IMPORTS = ["large_pdf", "large_csv", "repo_directory"]
DEFAULT_PHASE_CONTEXTMEMORY_FEATURES = [
    "resources",
    "skills",
    "cross_session_retrieval",
    "compact_indexes",
    "audit_light_telemetry",
]

STORAGE_TUNING_DEFAULTS: Json = {
    "TS_CONTEXT_PAGE_TARGET_BYTES": 65536,
    "TS_BLOCK_SEGMENT_TARGET_BYTES": 1 << 30,
    "TS_STORAGE_ZONE_SIZE": 10 * 1024 * 1024,
    "TS_STREAM_MAX_BLOB_SIZE": 10 * 1024 * 1024,
    "TS_COMPACTION_WATERMARK_BYTES": 256 * 1024 * 1024,
    "TS_COLD_SCAN_NO_CACHE_FILL": True,
    "TS_PAGE_INDEX_CACHE_BYTES": 64 * 1024 * 1024,
    "TS_BLOCK_INDEX_CACHE_BYTES": 64 * 1024 * 1024,
}

STORAGE_TUNING_FIELD_NAMES = list(STORAGE_TUNING_DEFAULTS.keys())

PUBLIC_STORAGE_CONTRACT: Json = {
    "page_address": "PageAddress",
    "block_address": "BlockAddress",
    "page_index_entry": "PageIndexEntry",
    "block_index_entry": "BlockIndexEntry",
    "object_index_entry": "ObjectIndexEntry",
    "storage_zone": "StorageZone",
    "stream": "Stream",
    "segment": "Segment",
    "extent": "Extent",
    "slot": "Slot",
    "append_watermark": "AppendWatermark",
    "compaction_watermark": "CompactionWatermark",
    "tombstone": "Tombstone",
    "gc_eligibility": "GcEligibility",
    "follower_cursor_safety": "FollowerCursorSafety",
    "compatibility_aliases": {},
}

PUBLIC_STORAGE_FEATURE_SHAPES: Json = {
    "page_address_fields": ["shard_id", "zone_id", "segment_id", "page_id", "offset", "length", "generation"],
    "block_address_fields": ["shard_id", "zone_id", "block_id", "offset", "length", "checksum"],
    "page_index_entry_fields": ["logical_key", "timestamp_range", "page_addresses", "append_watermark", "generation"],
    "block_index_entry_fields": ["page_address", "block_address", "extent", "checksum", "generation"],
    "object_index_entry_fields": ["model", "table", "object_key", "page_chain", "tombstone", "generation"],
    "storage_zone_fields": ["zone_id", "total_bytes", "used_bytes", "stale_bytes", "segments"],
    "stream_fields": ["stream_id", "segments", "rollover_count", "sealed_segment_count"],
    "segment_fields": ["segment_id", "extent", "start_offset", "sealed", "generation"],
    "extent_fields": ["extent", "block_range", "reclaim_state", "generation"],
    "slot_fields": ["slot_id", "dirty_generation", "object_refs", "page_refs", "tombstones", "owner_mismatch_count"],
    "append_watermark_fields": ["shard_id", "slot_id", "log_index", "timestamp_ms"],
    "compaction_watermark_fields": ["shard_id", "safe_generation", "safe_timestamp_ms", "follower_floor"],
    "tombstone_fields": ["ref", "generation", "deleted_at_ms", "reason"],
    "gc_eligibility_fields": ["ref", "eligible_after_ms", "has_tombstone", "follower_safe", "reclaimable_bytes"],
    "follower_cursor_safety_fields": ["min_follower_cursor", "blocked_reclaim_bytes", "safe_to_reclaim"],
}

STORAGE_RECLAIM_SCOPE: Json = {
    "owner": "temporalstore_storage_lifecycle",
    "matrixark_context_gc_role": "marks_logical_raw_event_eligibility_only",
    "physical_reclaim_context_specific": False,
}

STORAGE_LIFECYCLE_TOP_LEVEL_KEYS = [
    "effective_storage_tuning",
    "public_storage_contract",
    "public_storage_feature_shapes",
    "storage_write_contract",
    "storage_read_contract",
    "storage_cold_scan_contract",
    "storage_manager_contract",
    "storage_index_contract",
    "storage_cache_contract",
    "storage_reclaim_contract",
    "storage_safety_snapshot",
    "storage_watermark_snapshot",
    "storage_gc_snapshot",
    "storage_index_snapshot",
    "storage_topology_snapshot",
    "storage_read_sequence",
    "storage_cold_scan_sequence",
    "storage_lifecycle_phases",
    "storage_lifecycle_metrics",
    "storage_cache_layers",
    "storage_cache_semantics",
    "storage_reclaim_semantics",
    "storage_write_sequence",
    "storage_reclaim_scope",
]

PAGE_BLOCK_METRIC_NAMES = [
    "page_index_lookup_count",
    "page_index_lookup_ms",
    "page_index_cache_hit_rate",
    "block_index_lookup_count",
    "block_index_lookup_ms",
    "block_index_cache_hit_rate",
    "page_reads",
    "page_writes",
    "block_reads",
    "block_writes",
    "bytes_read",
    "bytes_written",
    "append_queue_wait_ms",
    "append_engine_ms",
    "append_queue_depth",
    "append_batch_size",
    "append_batch_bytes",
    "append_coalesced_writes",
    "append_durability_failures",
    "compaction_reclaimed_bytes",
    "cold_scan_no_cache_reads",
    "cold_scan_page_reads",
    "hot_cache_promotions",
    "append_watermark",
    "compaction_watermark",
]

STORAGE_WRITE_SEQUENCE_STEPS = [
    "append_record",
    "route_shard_slot",
    "choose_page",
    "append_page_buffer",
    "update_page_index",
    "flush_page_block_segment",
    "update_block_index",
    "publish_append_watermark",
]

STORAGE_WRITE_RESULT_FIELDS = [
    "shard_id",
    "slot",
    "placement_key",
    "page_address",
    "block_address",
    "append_watermark",
    "durability",
    "storage_family",
    "write_mode",
    "index_generation",
    "batch_watermark",
    "records_appended",
]

STORAGE_WRITE_METRIC_NAMES = [
    "append_queue_wait_ms",
    "append_engine_ms",
    "append_queue_depth",
    "append_batch_size",
    "append_batch_bytes",
    "append_coalesced_writes",
    "append_durability_failures",
    "append_watermark",
    "page_writes",
    "block_writes",
    "bytes_written",
]

STORAGE_READ_SEQUENCE_STEPS = [
    "logical_key_timestamp_range",
    "object_page_index_lookup",
    "page_address_list",
    "block_index_lookup",
    "page_read",
    "decode_records",
    "return_filtered_result",
]

STORAGE_READ_RESULT_FIELDS = [
    "logical_key",
    "timestamp_range",
    "object_index_entry",
    "page_index_entries",
    "page_addresses",
    "block_index_entries",
    "records_decoded",
    "records_returned",
    "tombstones_filtered",
    "stale_generations_filtered",
    "filter_policy",
]

STORAGE_READ_METRIC_NAMES = [
    "object_page_index_lookup_count",
    "object_page_index_lookup_ms",
    "page_address_count",
    "block_index_lookup_count",
    "block_index_lookup_ms",
    "page_reads",
    "decode_records_ms",
    "records_decoded",
    "records_returned",
    "tombstones_filtered",
    "stale_generations_filtered",
]

STORAGE_COLD_SCAN_SEQUENCE_STEPS = [
    "timestamp_page_index_scan",
    "no_cache_page_read",
    "bounded_decode",
    "no_hot_cache_promotion",
]

STORAGE_COLD_SCAN_RESULT_FIELDS = [
    "timestamp_range",
    "page_index_scan",
    "no_cache_page_reads",
    "decode_batch_limit",
    "decode_byte_limit",
    "deadline_ms",
    "records_decoded",
    "records_returned",
    "hot_cache_promotions",
    "cache_fill",
    "promotion_policy",
]

STORAGE_COLD_SCAN_METRIC_NAMES = [
    "cold_scan_no_cache_reads",
    "cold_scan_page_index_scan_count",
    "cold_scan_page_index_scan_ms",
    "cold_scan_page_reads",
    "cold_scan_decode_records_ms",
    "cold_scan_records_decoded",
    "cold_scan_records_returned",
    "cold_scan_decode_batch_limit",
    "cold_scan_decode_byte_limit",
    "hot_cache_promotions",
]

STORAGE_LIFECYCLE_PHASE_NAMES = [
    "prepare",
    "reclaim",
    "evict",
    "expire",
    "page_gc",
    "block_gc",
    "compaction",
    "index_gc",
    "delayed_destroy",
    "follower_cursor_safety",
    "watermark_progress",
]

STORAGE_MANAGER_PHASE_METRICS = {
    "prepare": "storage_manager_prepare_count",
    "reclaim": "storage_manager_reclaim_count",
    "evict": "storage_manager_evict_count",
    "expire": "storage_manager_expire_count",
    "page_gc": "storage_manager_page_gc_count",
    "block_gc": "storage_manager_block_gc_count",
    "compaction": "storage_manager_compaction_count",
    "index_gc": "storage_manager_index_gc_count",
    "delayed_destroy": "storage_manager_delayed_destroy_count",
    "follower_cursor_safety": "storage_manager_follower_cursor_safety_count",
    "watermark_progress": "storage_manager_watermark_progress_count",
}

STORAGE_INDEX_BEHAVIOR_NAMES = [
    "page_address_encode_decode",
    "page_address_stable_order",
    "timestamp_range_page_lookup",
    "slot_index_maps_slot_to_object_page_refs",
    "object_index_maps_model_table_object_key_to_page_chain",
    "page_index_maps_logical_ranges_to_page_addresses",
    "block_index_maps_page_addresses_to_durable_locations",
    "restart_rebuilds_page_block_object_indexes",
]

STORAGE_RECLAIM_SEMANTICS = [
    "cache_eviction_memory_only",
    "logical_tombstone_required",
    "stale_pages_blocks_rewritten_or_skipped",
    "reclaimed_bytes_reported",
    "physical_reclaim_errors_zero",
]

STORAGE_RECLAIM_CONTRACT_FIELDS = [
    "cache_eviction_frees_memory_only",
    "logical_gc_marks_expired_deletable",
    "physical_reclaim_requires_compaction_or_safe_skip",
    "cache_evictions",
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "stale_pages_rewritten",
    "stale_pages_skipped",
    "stale_blocks_rewritten",
    "stale_blocks_skipped",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
]

STORAGE_CACHE_LAYER_NAMES = [
    "memory_object_cache",
    "page_index_cache",
    "block_index_cache",
    "disk_block_cache",
    "shared_store_read_through",
]

STORAGE_CACHE_SEMANTICS = [
    "lookup_hot_to_cold",
    "refill_from_durable_on_miss",
    "invalidate_on_append_watermark",
    "invalidate_on_compaction_watermark",
    "cold_scan_no_promote",
    "writeback_backpressure_reported",
]

STORAGE_CACHE_METRIC_NAMES = [
    "memory_cache_hits",
    "memory_cache_misses",
    "page_index_cache_hits",
    "page_index_cache_misses",
    "block_index_cache_hits",
    "block_index_cache_misses",
    "disk_cache_hits",
    "disk_cache_misses",
    "shared_store_read_throughs",
    "cache_refills",
    "cache_invalidations",
    "cache_writeback_queue_depth",
    "cache_writeback_rejections",
]

STORAGE_CACHE_CONTRACT_FIELDS = [
    "layers",
    "semantics",
    "metrics",
    "hot_to_cold_lookup",
    "durable_refill_on_miss",
    "append_watermark_invalidation",
    "compaction_watermark_invalidation",
    "cold_scan_no_promote",
    "writeback_backpressure_measured",
    "cache_refills",
    "cache_invalidations",
    "cache_writeback_queue_depth",
    "cache_writeback_rejections",
    "hot_cache_promotions",
]

STORAGE_LIFECYCLE_METRIC_NAMES = [
    "storage_manager_prepare_count",
    "storage_manager_reclaim_count",
    "storage_manager_evict_count",
    "storage_manager_expire_count",
    "storage_manager_page_gc_count",
    "storage_manager_block_gc_count",
    "storage_manager_compaction_count",
    "storage_manager_index_gc_count",
    "storage_manager_delayed_destroy_count",
    "storage_manager_follower_cursor_safety_count",
    "storage_manager_watermark_progress_count",
    "storage_manager_loop_ms",
    "stream_rollover_count",
    "segment_open_count",
    "segment_sealed_count",
    "storage_zone_total_bytes",
    "storage_zone_used_bytes",
    "storage_zone_stale_bytes",
    "append_log_replay_records",
    "append_log_reclaimed_records",
    "slot_dirty_generation_count",
    "slot_tombstone_count",
    "slot_stale_ref_count",
    "slot_owner_mismatch_count",
    "page_index_rebuild_count",
    "block_index_rebuild_count",
    "object_index_rebuild_count",
    "cache_admissions",
    "cache_evictions",
    "cache_rehydrates",
    "memory_cache_hits",
    "memory_cache_misses",
    "page_index_cache_hits",
    "page_index_cache_misses",
    "block_index_cache_hits",
    "block_index_cache_misses",
    "disk_cache_hits",
    "disk_cache_misses",
    "shared_store_read_throughs",
    "cache_refills",
    "cache_invalidations",
    "cache_writeback_queue_depth",
    "cache_writeback_rejections",
    "cold_scan_no_cache_reads",
    "hot_cache_promotions",
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "stale_pages_rewritten",
    "stale_pages_skipped",
    "stale_blocks_rewritten",
    "stale_blocks_skipped",
    "delayed_destroy_backlog",
    "follower_cursor_retention_floor",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
    "append_watermark",
    "compaction_watermark",
]


def _parse_int_csv(raw: str, default: list[int]) -> list[int]:
    if not raw:
        return list(default)
    values: list[int] = []
    for item in str(raw).replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError:
            continue
    return values or list(default)


def _parse_str_csv(raw: str, default: list[str]) -> list[str]:
    if not raw:
        return list(default)
    values = [item.strip() for item in str(raw).replace(";", ",").split(",") if item.strip()]
    return values or list(default)


def _parse_bool_env(raw: str | None, default: bool) -> bool:
    if raw is None or str(raw).strip() == "":
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def effective_storage_tuning_from_env() -> Json:
    values: Json = {}
    for name, default in STORAGE_TUNING_DEFAULTS.items():
        raw = os.environ.get(name)
        if isinstance(default, bool):
            values[name] = _parse_bool_env(raw, default)
            continue
        try:
            values[name] = int(raw) if raw is not None and str(raw).strip() else int(default)
        except (TypeError, ValueError):
            values[name] = int(default)

    # C++ deployment scripts still accept legacy TEMPORALSTORE_* overrides.
    try:
        if os.environ.get("TEMPORALSTORE_STORAGE_ZONE_SIZE"):
            values["TS_STORAGE_ZONE_SIZE"] = int(os.environ["TEMPORALSTORE_STORAGE_ZONE_SIZE"])
    except (TypeError, ValueError):
        pass
    try:
        if os.environ.get("TEMPORALSTORE_STREAM_MAX_BLOB_SIZE"):
            values["TS_STREAM_MAX_BLOB_SIZE"] = int(os.environ["TEMPORALSTORE_STREAM_MAX_BLOB_SIZE"])
    except (TypeError, ValueError):
        pass
    values["effective_block_segment_target_bytes"] = min(
        int(values["TS_BLOCK_SEGMENT_TARGET_BYTES"]),
        int(values["TS_STREAM_MAX_BLOB_SIZE"]),
    )
    return values


def zero_storage_lifecycle_metrics() -> Json:
    return {name: 0 for name in STORAGE_LIFECYCLE_METRIC_NAMES}


def default_storage_write_contract(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    return {
        "shard_id": 0,
        "slot": "slot:0",
        "placement_key": "storage:parity",
        "page_address": "PageAddress",
        "block_address": "BlockAddress",
        "append_watermark": int(source.get("append_watermark") or 0),
        "durability": "async",
        "storage_family": "shared_store",
        "write_mode": "async",
        "index_generation": 0,
        "batch_watermark": int(source.get("append_watermark") or 0),
        "records_appended": 1,
        "append_queue_wait_ms": 0,
        "append_engine_ms": 0,
        "append_queue_depth": 0,
        "append_batch_size": 1,
        "append_batch_bytes": 0,
        "append_coalesced_writes": 1,
        "append_durability_failures": 0,
        "page_writes": int(source.get("page_writes") or 0),
        "block_writes": int(source.get("block_writes") or 0),
        "bytes_written": int(source.get("bytes_written") or 0),
    }


def default_storage_read_contract(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    records_decoded = int(source.get("records_decoded") or 0)
    records_returned = int(source.get("records_returned") or records_decoded)
    return {
        "logical_key": "storage:parity",
        "timestamp_range": "all",
        "object_index_entry": "ObjectIndexEntry",
        "page_index_entries": 1,
        "page_addresses": 1,
        "block_index_entries": 1,
        "records_decoded": records_decoded,
        "records_returned": min(records_returned, records_decoded) if records_decoded else records_returned,
        "tombstones_filtered": int(source.get("tombstones_filtered") or 0),
        "stale_generations_filtered": int(source.get("stale_generations_filtered") or 0),
        "filter_policy": "normal",
        "object_page_index_lookup_count": int(source.get("object_page_index_lookup_count") or 0),
        "object_page_index_lookup_ms": float(source.get("object_page_index_lookup_ms") or 0),
        "page_address_count": int(source.get("page_address_count") or 0),
        "block_index_lookup_count": int(source.get("block_index_lookup_count") or 0),
        "block_index_lookup_ms": float(source.get("block_index_lookup_ms") or 0),
        "page_reads": int(source.get("page_reads") or 0),
        "decode_records_ms": float(source.get("decode_records_ms") or 0),
    }


def default_storage_cold_scan_contract(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    records_decoded = int(source.get("cold_scan_records_decoded") or 0)
    records_returned = int(source.get("cold_scan_records_returned") or records_decoded)
    return {
        "timestamp_range": "cold",
        "page_index_scan": "PageIndex",
        "no_cache_page_reads": int(source.get("cold_scan_no_cache_reads") or 0),
        "decode_batch_limit": int(source.get("cold_scan_decode_batch_limit") or 0),
        "decode_byte_limit": int(source.get("cold_scan_decode_byte_limit") or 0),
        "deadline_ms": 0,
        "records_decoded": records_decoded,
        "records_returned": min(records_returned, records_decoded) if records_decoded else records_returned,
        "hot_cache_promotions": int(source.get("hot_cache_promotions") or 0),
        "cache_fill": False,
        "promotion_policy": "no_promote",
        "cold_scan_no_cache_reads": int(source.get("cold_scan_no_cache_reads") or 0),
        "cold_scan_page_index_scan_count": int(source.get("cold_scan_page_index_scan_count") or 0),
        "cold_scan_page_index_scan_ms": float(source.get("cold_scan_page_index_scan_ms") or 0),
        "cold_scan_page_reads": int(source.get("cold_scan_page_reads") or source.get("cold_scan_no_cache_reads") or 0),
        "cold_scan_decode_records_ms": float(source.get("cold_scan_decode_records_ms") or 0),
        "cold_scan_records_decoded": records_decoded,
        "cold_scan_records_returned": min(records_returned, records_decoded) if records_decoded else records_returned,
        "cold_scan_decode_batch_limit": int(source.get("cold_scan_decode_batch_limit") or 0),
        "cold_scan_decode_byte_limit": int(source.get("cold_scan_decode_byte_limit") or 0),
    }


def default_storage_manager_contract(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    phase_counts = {
        phase: int(source.get(metric_name) or 0)
        for phase, metric_name in STORAGE_MANAGER_PHASE_METRICS.items()
    }
    return {
        "manager_identity": "StorageManager/StoreManager",
        "cpp_public_name": "StorageManager",
        "rust_public_name": "StoreManager",
        "phase_order": list(STORAGE_LIFECYCLE_PHASE_NAMES),
        "phase_metrics": dict(STORAGE_MANAGER_PHASE_METRICS),
        "phase_counts": phase_counts,
        "loop_metric": "storage_manager_loop_ms",
        "loop_ms": float(source.get("storage_manager_loop_ms") or 0),
        "phase_order_enforced": True,
        "missing_phase_count": 0,
    }


def default_storage_index_contract(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    return {
        "page_address_codec": "PageAddress",
        "block_address_codec": "BlockAddress",
        "stable_order": ["shard_id", "zone_id", "segment_id", "page_id", "offset"],
        "slot_index": "slot -> object/page refs",
        "object_index_entry": "{model/table/object_key} -> current page chain",
        "page_index": "logical timestamp/key ranges -> page addresses",
        "block_index": "page addresses -> physical durable locations",
        "required_behaviors": list(STORAGE_INDEX_BEHAVIOR_NAMES),
        "page_address_encode_decode": True,
        "block_address_encode_decode": True,
        "stable_order_verified": True,
        "timestamp_range_lookup_verified": True,
        "slot_index_entry_count": int(source.get("slot_index_entry_count") or 1),
        "slot_object_ref_count": int(source.get("slot_object_ref_count") or 1),
        "slot_page_ref_count": int(source.get("slot_page_ref_count") or source.get("page_address_count") or 1),
        "object_index_entry_count": int(source.get("object_index_entry_count") or 1),
        "page_index_entry_count": int(source.get("page_index_entry_count") or 1),
        "block_index_entry_count": int(source.get("block_index_entry_count") or 1),
        "restart_rebuild_verified": True,
        "unreadable_page_refs": int(source.get("unreadable_page_refs") or 0),
        "checksum_mismatches": int(source.get("checksum_mismatches") or 0),
    }


def default_storage_cache_contract(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    return {
        "layers": list(STORAGE_CACHE_LAYER_NAMES),
        "semantics": list(STORAGE_CACHE_SEMANTICS),
        "metrics": list(STORAGE_CACHE_METRIC_NAMES),
        "hot_to_cold_lookup": True,
        "durable_refill_on_miss": True,
        "append_watermark_invalidation": True,
        "compaction_watermark_invalidation": True,
        "cold_scan_no_promote": True,
        "writeback_backpressure_measured": True,
        "cache_refills": int(source.get("cache_refills") or 0),
        "cache_invalidations": int(source.get("cache_invalidations") or 0),
        "cache_writeback_queue_depth": int(source.get("cache_writeback_queue_depth") or 0),
        "cache_writeback_rejections": int(source.get("cache_writeback_rejections") or 0),
        "hot_cache_promotions": int(source.get("hot_cache_promotions") or 0),
    }


def default_storage_reclaim_contract(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    return {
        "cache_eviction_frees_memory_only": True,
        "logical_gc_marks_expired_deletable": True,
        "physical_reclaim_requires_compaction_or_safe_skip": True,
        "cache_evictions": int(source.get("cache_evictions") or 0),
        "tombstone_records": int(source.get("tombstone_records") or 0),
        "stale_page_tombstones": int(source.get("stale_page_tombstones") or 0),
        "stale_block_tombstones": int(source.get("stale_block_tombstones") or 0),
        "stale_pages_rewritten": int(source.get("stale_pages_rewritten") or 0),
        "stale_pages_skipped": int(source.get("stale_pages_skipped") or 0),
        "stale_blocks_rewritten": int(source.get("stale_blocks_rewritten") or 0),
        "stale_blocks_skipped": int(source.get("stale_blocks_skipped") or 0),
        "reclaimable_bytes": int(source.get("reclaimable_bytes") or 0),
        "compaction_reclaimed_bytes": int(source.get("compaction_reclaimed_bytes") or 0),
        "physical_reclaimed_bytes": int(source.get("physical_reclaimed_bytes") or 0),
        "physical_reclaim_errors": int(source.get("physical_reclaim_errors") or 0),
    }


def default_storage_safety_snapshot(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    follower_block_count = int(source.get("stale_pages_skipped") or 0) + int(
        source.get("stale_blocks_skipped") or 0
    )
    tombstone_count = (
        int(source.get("tombstone_records") or 0)
        + int(source.get("stale_page_tombstones") or 0)
        + int(source.get("stale_block_tombstones") or 0)
    )
    return {
        "append_watermark": int(source.get("append_watermark") or 0),
        "compaction_watermark": int(source.get("compaction_watermark") or 0),
        "tombstone_records": tombstone_count,
        "gc_eligible_record_count": tombstone_count,
        "reclaimable_bytes": int(source.get("reclaimable_bytes") or 0),
        "follower_cursor_retention_floor": int(
            source.get("follower_cursor_retention_floor") or 0
        ),
        "follower_cursor_blocked_reclaim_count": follower_block_count,
        "follower_cursor_safe_to_reclaim": follower_block_count == 0,
        "physical_reclaim_errors": int(source.get("physical_reclaim_errors") or 0),
    }


def default_storage_gc_snapshot(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    follower_block_count = int(source.get("stale_pages_skipped") or 0) + int(
        source.get("stale_blocks_skipped") or 0
    )
    tombstone_records = int(source.get("tombstone_records") or 0)
    stale_page_tombstones = int(source.get("stale_page_tombstones") or 0)
    stale_block_tombstones = int(source.get("stale_block_tombstones") or 0)
    return {
        "tombstone_records": tombstone_records,
        "stale_page_tombstones": stale_page_tombstones,
        "stale_block_tombstones": stale_block_tombstones,
        "gc_eligible_record_count": tombstone_records
        + stale_page_tombstones
        + stale_block_tombstones,
        "reclaimable_bytes": int(source.get("reclaimable_bytes") or 0),
        "compaction_reclaimed_bytes": int(source.get("compaction_reclaimed_bytes") or 0),
        "physical_reclaimed_bytes": int(source.get("physical_reclaimed_bytes") or 0),
        "physical_reclaim_errors": int(source.get("physical_reclaim_errors") or 0),
        "follower_cursor_retention_floor": int(
            source.get("follower_cursor_retention_floor") or 0
        ),
        "follower_cursor_blocked_reclaim_count": follower_block_count,
        "follower_cursor_safe_to_reclaim": follower_block_count == 0,
        "tombstone_samples": list(source.get("tombstone_samples") or []),
        "gc_eligibility_samples": list(source.get("gc_eligibility_samples") or []),
        "follower_cursor_safety_samples": list(
            source.get("follower_cursor_safety_samples") or []
        ),
    }


def default_storage_watermark_snapshot(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    append_watermark = int(source.get("append_watermark") or 0)
    compaction_watermark = int(source.get("compaction_watermark") or 0)
    follower_floor = int(source.get("follower_cursor_retention_floor") or 0)
    follower_safe_watermark = min(compaction_watermark, follower_floor) if follower_floor else compaction_watermark
    return {
        "append_watermark": append_watermark,
        "compaction_watermark": compaction_watermark,
        "follower_cursor_retention_floor": follower_floor,
        "follower_cursor_safe_watermark": follower_safe_watermark,
        "page_index_rebuild_watermark": int(source.get("page_index_rebuild_count") or 0),
        "block_index_rebuild_watermark": int(source.get("block_index_rebuild_count") or 0),
        "object_index_rebuild_watermark": int(source.get("object_index_rebuild_count") or 0),
        "append_watermark_samples": list(source.get("append_watermark_samples") or []),
        "compaction_watermark_samples": list(
            source.get("compaction_watermark_samples") or []
        ),
    }


def default_storage_index_snapshot(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    return {
        "page_index_entry_count": int(source.get("page_index_entry_count") or 0),
        "block_index_entry_count": int(source.get("block_index_entry_count") or 0),
        "object_index_entry_count": int(source.get("object_index_entry_count") or 0),
        "slot_index_entry_count": int(source.get("slot_index_entry_count") or 0),
        "slot_object_ref_count": int(source.get("slot_object_ref_count") or 0),
        "slot_page_ref_count": int(source.get("slot_page_ref_count") or 0),
        "page_address_count": int(source.get("page_address_count") or 0),
        "unreadable_page_refs": int(source.get("unreadable_page_refs") or 0),
        "checksum_mismatches": int(source.get("checksum_mismatches") or 0),
        "missing_owner_ref_count": int(source.get("missing_owner_ref_count") or 0),
        "owner_mismatch_count": int(source.get("owner_mismatch_count") or 0),
        "restart_rebuild_verified": bool(
            source.get("page_index_rebuild_count")
            or source.get("block_index_rebuild_count")
            or source.get("object_index_rebuild_count")
        ),
        "page_index_entry_samples": list(source.get("page_index_entry_samples") or []),
        "block_index_entry_samples": list(source.get("block_index_entry_samples") or []),
        "object_index_entry_samples": list(source.get("object_index_entry_samples") or []),
    }


def default_storage_topology_snapshot(metrics: Json | None = None) -> Json:
    source = metrics if isinstance(metrics, dict) else {}
    return {
        "storage_zone_count": int(source.get("storage_zone_count") or 0),
        "active_storage_zones": int(source.get("active_storage_zones") or 0),
        "sealed_storage_zones": int(source.get("sealed_storage_zones") or 0),
        "stream_segment_count": int(source.get("stream_segment_count") or 0),
        "segment_open_count": int(source.get("segment_open_count") or 0),
        "segment_sealed_count": int(source.get("segment_sealed_count") or 0),
        "delayed_destroy_backlog": int(source.get("delayed_destroy_backlog") or 0),
        "storage_zone_total_bytes": int(source.get("storage_zone_total_bytes") or 0),
        "storage_zone_used_bytes": int(source.get("storage_zone_used_bytes") or 0),
        "storage_zone_stale_bytes": int(source.get("storage_zone_stale_bytes") or 0),
        "append_log_replay_records": int(source.get("append_log_replay_records") or 0),
        "append_log_reclaimed_records": int(source.get("append_log_reclaimed_records") or 0),
        "storage_zone_samples": list(source.get("storage_zone_samples") or []),
        "stream_samples": list(source.get("stream_samples") or []),
        "segment_samples": list(source.get("segment_samples") or []),
        "extent_samples": list(source.get("extent_samples") or []),
        "slot_samples": list(source.get("slot_samples") or []),
    }


def storage_lifecycle_report_shape(effective_storage_tuning: Json, metrics: Json | None = None) -> Json:
    return {
        "effective_storage_tuning": dict(effective_storage_tuning),
        "public_storage_contract": {
            key: (dict(value) if isinstance(value, dict) else value)
            for key, value in PUBLIC_STORAGE_CONTRACT.items()
        },
        "public_storage_feature_shapes": {
            key: list(value) if isinstance(value, list) else value
            for key, value in PUBLIC_STORAGE_FEATURE_SHAPES.items()
        },
        "storage_write_contract": default_storage_write_contract(metrics),
        "storage_read_contract": default_storage_read_contract(metrics),
        "storage_cold_scan_contract": default_storage_cold_scan_contract(metrics),
        "storage_manager_contract": default_storage_manager_contract(metrics),
        "storage_index_contract": default_storage_index_contract(metrics),
        "storage_cache_contract": default_storage_cache_contract(metrics),
        "storage_reclaim_contract": default_storage_reclaim_contract(metrics),
        "storage_safety_snapshot": default_storage_safety_snapshot(metrics),
        "storage_watermark_snapshot": default_storage_watermark_snapshot(metrics),
        "storage_gc_snapshot": default_storage_gc_snapshot(metrics),
        "storage_index_snapshot": default_storage_index_snapshot(metrics),
        "storage_topology_snapshot": default_storage_topology_snapshot(metrics),
        "storage_write_sequence": list(STORAGE_WRITE_SEQUENCE_STEPS),
        "storage_read_sequence": list(STORAGE_READ_SEQUENCE_STEPS),
        "storage_cold_scan_sequence": list(STORAGE_COLD_SCAN_SEQUENCE_STEPS),
        "storage_lifecycle_phases": list(STORAGE_LIFECYCLE_PHASE_NAMES),
        "storage_lifecycle_metrics": dict(metrics or zero_storage_lifecycle_metrics()),
        "storage_cache_layers": list(STORAGE_CACHE_LAYER_NAMES),
        "storage_cache_semantics": list(STORAGE_CACHE_SEMANTICS),
        "storage_reclaim_semantics": list(STORAGE_RECLAIM_SEMANTICS),
        "storage_reclaim_scope": dict(STORAGE_RECLAIM_SCOPE),
    }


def attach_storage_lifecycle_shape(result: Json, effective_storage_tuning: Json | None = None) -> Json:
    tuning = effective_storage_tuning
    if tuning is None:
        existing = result.get("effective_storage_tuning")
        tuning = existing if isinstance(existing, dict) else effective_storage_tuning_from_env()
    metrics = result.get("storage_lifecycle_metrics")
    if not isinstance(metrics, dict):
        metrics = None
    shaped = storage_lifecycle_report_shape(tuning, metrics=metrics)
    shaped.update(result)
    # Keep canonical top-level shape authoritative even if callers passed only
    # nested/legacy lifecycle fields.
    shaped["effective_storage_tuning"] = dict(tuning)
    shaped["public_storage_contract"] = shaped.get("public_storage_contract") or storage_lifecycle_report_shape(tuning)["public_storage_contract"]
    shaped["public_storage_feature_shapes"] = (
        shaped.get("public_storage_feature_shapes")
        if isinstance(shaped.get("public_storage_feature_shapes"), dict)
        else {
            key: list(value) if isinstance(value, list) else value
            for key, value in PUBLIC_STORAGE_FEATURE_SHAPES.items()
        }
    )
    write_contract = shaped.get("storage_write_contract")
    if not isinstance(write_contract, dict):
        write_contract = {}
    normalized_write_contract = default_storage_write_contract(metrics)
    normalized_write_contract.update(write_contract)
    shaped["storage_write_contract"] = normalized_write_contract
    read_contract = shaped.get("storage_read_contract")
    if not isinstance(read_contract, dict):
        read_contract = {}
    normalized_read_contract = default_storage_read_contract(metrics)
    normalized_read_contract.update(read_contract)
    shaped["storage_read_contract"] = normalized_read_contract
    cold_scan_contract = shaped.get("storage_cold_scan_contract")
    if not isinstance(cold_scan_contract, dict):
        cold_scan_contract = {}
    normalized_cold_scan_contract = default_storage_cold_scan_contract(metrics)
    normalized_cold_scan_contract.update(cold_scan_contract)
    shaped["storage_cold_scan_contract"] = normalized_cold_scan_contract
    manager_contract = shaped.get("storage_manager_contract")
    if not isinstance(manager_contract, dict):
        manager_contract = {}
    normalized_manager_contract = default_storage_manager_contract(metrics)
    normalized_manager_contract.update(manager_contract)
    shaped["storage_manager_contract"] = normalized_manager_contract
    index_contract = shaped.get("storage_index_contract")
    if not isinstance(index_contract, dict):
        index_contract = {}
    normalized_index_contract = default_storage_index_contract(metrics)
    normalized_index_contract.update(index_contract)
    shaped["storage_index_contract"] = normalized_index_contract
    cache_contract = shaped.get("storage_cache_contract")
    if not isinstance(cache_contract, dict):
        cache_contract = {}
    normalized_cache_contract = default_storage_cache_contract(metrics)
    normalized_cache_contract.update(cache_contract)
    shaped["storage_cache_contract"] = normalized_cache_contract
    reclaim_contract = shaped.get("storage_reclaim_contract")
    if not isinstance(reclaim_contract, dict):
        reclaim_contract = {}
    normalized_reclaim_contract = default_storage_reclaim_contract(metrics)
    normalized_reclaim_contract.update(reclaim_contract)
    shaped["storage_reclaim_contract"] = normalized_reclaim_contract
    safety_snapshot = shaped.get("storage_safety_snapshot")
    if not isinstance(safety_snapshot, dict):
        safety_snapshot = {}
    normalized_safety_snapshot = default_storage_safety_snapshot(metrics)
    normalized_safety_snapshot.update(safety_snapshot)
    shaped["storage_safety_snapshot"] = normalized_safety_snapshot
    watermark_snapshot = shaped.get("storage_watermark_snapshot")
    if not isinstance(watermark_snapshot, dict):
        watermark_snapshot = {}
    normalized_watermark_snapshot = default_storage_watermark_snapshot(metrics)
    normalized_watermark_snapshot.update(watermark_snapshot)
    shaped["storage_watermark_snapshot"] = normalized_watermark_snapshot
    gc_snapshot = shaped.get("storage_gc_snapshot")
    if not isinstance(gc_snapshot, dict):
        gc_snapshot = {}
    normalized_gc_snapshot = default_storage_gc_snapshot(metrics)
    normalized_gc_snapshot.update(gc_snapshot)
    shaped["storage_gc_snapshot"] = normalized_gc_snapshot
    index_snapshot = shaped.get("storage_index_snapshot")
    if not isinstance(index_snapshot, dict):
        index_snapshot = {}
    normalized_index_snapshot = default_storage_index_snapshot(metrics)
    normalized_index_snapshot.update(index_snapshot)
    shaped["storage_index_snapshot"] = normalized_index_snapshot
    topology_snapshot = shaped.get("storage_topology_snapshot")
    if not isinstance(topology_snapshot, dict):
        topology_snapshot = {}
    normalized_topology_snapshot = default_storage_topology_snapshot(metrics)
    normalized_topology_snapshot.update(topology_snapshot)
    shaped["storage_topology_snapshot"] = normalized_topology_snapshot
    shaped["storage_write_sequence"] = list(STORAGE_WRITE_SEQUENCE_STEPS)
    shaped["storage_read_sequence"] = list(STORAGE_READ_SEQUENCE_STEPS)
    shaped["storage_cold_scan_sequence"] = list(STORAGE_COLD_SCAN_SEQUENCE_STEPS)
    shaped["storage_lifecycle_phases"] = list(STORAGE_LIFECYCLE_PHASE_NAMES)
    shaped["storage_lifecycle_metrics"] = metrics or zero_storage_lifecycle_metrics()
    shaped["storage_cache_layers"] = list(STORAGE_CACHE_LAYER_NAMES)
    shaped["storage_cache_semantics"] = list(STORAGE_CACHE_SEMANTICS)
    shaped["storage_reclaim_semantics"] = list(STORAGE_RECLAIM_SEMANTICS)
    shaped["storage_reclaim_scope"] = dict(STORAGE_RECLAIM_SCOPE)
    return shaped


def storage_tuning_failures(report: Json) -> list[str]:
    config = report.get("config", {}) if isinstance(report.get("config"), dict) else {}
    config_tuning = config.get("effective_storage_tuning") if isinstance(config.get("effective_storage_tuning"), dict) else {}
    failures: list[str] = []
    for field in STORAGE_TUNING_FIELD_NAMES:
        if field not in config_tuning:
            failures.append(f"config missing effective_storage_tuning.{field}")

    backends = report.get("backends", {}) if isinstance(report.get("backends"), dict) else {}
    for backend_name in ("cpp", "rust"):
        backend = backends.get(backend_name)
        if not isinstance(backend, dict):
            continue
        if backend.get("status") != "passed":
            continue
        backend_tuning = backend.get("effective_storage_tuning")
        if not isinstance(backend_tuning, dict):
            failures.append(f"{backend_name} missing effective_storage_tuning")
            continue
        for field in STORAGE_TUNING_FIELD_NAMES:
            if field not in backend_tuning:
                failures.append(f"{backend_name} missing effective_storage_tuning.{field}")
                continue
            if field in config_tuning and backend_tuning[field] != config_tuning[field]:
                failures.append(
                    f"{backend_name} effective_storage_tuning.{field} drift: "
                    f"backend={backend_tuning[field]!r} config={config_tuning[field]!r}"
                )
    return failures


def retrieval_metrics_from_result(result: Json) -> Json:
    pack = result.get("context_pack") if isinstance(result.get("context_pack"), dict) else result
    metrics = pack.get("retrieval_metrics") if isinstance(pack.get("retrieval_metrics"), dict) else {}
    return metrics if isinstance(metrics, dict) else {}


def _count_refs(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        total = 0
        for item in value.values():
            total += _count_refs(item)
        return total
    return 0


def _flag_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, dict):
        return [str(key) for key, enabled in value.items() if bool(enabled)]
    return []


def _sum_token_estimates(value: Any) -> int:
    if isinstance(value, list):
        total = 0
        for item in value:
            total += _sum_token_estimates(item)
        return total
    if isinstance(value, dict):
        for key in ("token_estimate", "tokens", "token_count"):
            if key in value:
                try:
                    return max(0, int(value.get(key) or 0))
                except (TypeError, ValueError):
                    return 0
        total = 0
        for item in value.values():
            total += _sum_token_estimates(item)
        return total
    return 0


def _drop_counter_summary(value: Any) -> Json:
    counters: Json = {}
    if not isinstance(value, dict):
        return counters
    for key, raw in value.items():
        if key in {"refs", "estimated_tokens", "reasons", "native_summary"}:
            continue
        if isinstance(raw, bool):
            continue
        if isinstance(raw, (int, float)):
            counters[str(key)] = int(raw)
    reason_map = {
        "scope": ("scope", "access", "access_denied"),
        "placement": ("placement", "placement_filter"),
        "index_filter": ("index_filter", "secondary_index_filter"),
        "stale": ("stale", "superseded", "stale_version"),
        "token_budget": ("over_budget", "max_selected_refs"),
        "score_threshold": ("low_score", "score_threshold"),
    }
    normalized: Json = {}
    for out_key, aliases in reason_map.items():
        normalized[out_key] = sum(int(counters.get(alias, 0) or 0) for alias in aliases)
    normalized["other"] = sum(
        int(value or 0)
        for key, value in counters.items()
        if key not in {alias for aliases in reason_map.values() for alias in aliases}
    )
    return normalized


def retrieval_phase0_fields(result: Json) -> Json:
    pack = result.get("context_pack") if isinstance(result.get("context_pack"), dict) else result
    metrics = retrieval_metrics_from_result(result)
    recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
    index_filter = (
        recall_policy.get("secondary_index_filter")
        if isinstance(recall_policy.get("secondary_index_filter"), dict)
        else {}
    )
    backend_pushdown = (
        recall_policy.get("backend_retrieval_pushdown")
        if isinstance(recall_policy.get("backend_retrieval_pushdown"), dict)
        else {}
    )

    def metric_int(*names: str) -> int:
        for name in names:
            raw = metrics.get(name)
            if raw is None:
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
        return 0

    def pack_int(*names: str) -> int:
        for name in names:
            raw = pack.get(name)
            if raw is None:
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
        return 0

    candidate_count = metric_int("candidate_count", "candidate_records", "candidate_ref_count")
    if not candidate_count:
        candidate_count = pack_int("candidate_count", "primary_candidate_count", "auxiliary_candidate_count")
    if not candidate_count:
        candidate_count = selected_ref_count(result) + _count_refs(pack.get("dropped_refs"))

    index_hits = metric_int("index_hits", "secondary_index_hits", "index_prefilter_hits")
    if not index_hits:
        try:
            index_hits = int(index_filter.get("matched_candidate_count") or 0)
        except (TypeError, ValueError):
            index_hits = 0

    token_count = metric_int("token_count", "used_context_tokens", "remote_context_tokens")
    if not token_count:
        token_count = pack_int(
            "used_context_tokens",
            "used_remote_context_tokens",
            "total_prompt_context_tokens",
            "context_tokens",
        )
    if not token_count:
        token_count = (
            _sum_token_estimates(pack.get("refs"))
            or _sum_token_estimates(pack.get("selected_refs"))
            or _sum_token_estimates(pack.get("context_refs"))
            or _sum_token_estimates(pack.get("groups"))
        )

    metric_drop_counters = metrics.get("drop_counters") if isinstance(metrics.get("drop_counters"), dict) else {}
    drop_counters = metric_drop_counters or _drop_counter_summary(pack.get("dropped_refs"))
    candidate_class_counts = metrics.get("candidate_class_counts")
    if not isinstance(candidate_class_counts, dict):
        recall_candidate_class_counts = recall_policy.get("candidate_class_counts")
        candidate_class_counts = recall_candidate_class_counts if isinstance(recall_candidate_class_counts, dict) else {}
    index_postings_read = metric_int(
        "index_postings_read",
        "index_postings_touched",
        "native_index_postings_found",
    ) or int(backend_pushdown.get("native_index_postings_found") or 0)
    candidate_cache_hit = bool(metrics.get("candidate_cache_hit", metrics.get("cache_hit", False)))
    explicit_evidence = metrics.get("correctness_evidence") if isinstance(metrics.get("correctness_evidence"), dict) else {}
    quota_policy = recall_policy.get("quota_policy") if isinstance(recall_policy.get("quota_policy"), dict) else {}
    cross_session_policy = recall_policy.get("cross_session") if isinstance(recall_policy.get("cross_session"), dict) else {}
    correctness_evidence = {
        "scope_filtering": bool(explicit_evidence.get("scope_filtering") or drop_counters.get("scope", 0) > 0),
        "placement_filtering": bool(
            explicit_evidence.get("placement_filtering")
            or drop_counters.get("placement", 0) > 0
            or metrics.get("placement_partitions_touched")
            or backend_pushdown.get("native_placement_locations")
        ),
        "compact_secondary_index_prefilter": bool(
            explicit_evidence.get("compact_secondary_index_prefilter")
            or index_postings_read > 0
            or index_filter.get("matched_candidate_count")
            or backend_pushdown.get("native_index_postings_found")
        ),
        "stale_superseded_exclusion": bool(
            explicit_evidence.get("stale_superseded_exclusion")
            or drop_counters.get("stale", 0) > 0
            or metrics.get("superseded_excluded_count")
            or pack.get("include_superseded_resources") is False
        ),
        "shared_resource_skill_quota": bool(
            explicit_evidence.get("shared_resource_skill_quota")
            or metrics.get("shared_resource_quota_applied")
            or metrics.get("skill_quota_applied")
            or quota_policy.get("shared_resources")
            or quota_policy.get("skills")
        ),
        "cross_session_quota_rerank": bool(
            explicit_evidence.get("cross_session_quota_rerank")
            or metrics.get("cross_session_quota_applied")
            or metrics.get("cross_session_rerank_applied")
            or cross_session_policy.get("enabled")
            or cross_session_policy.get("budget_ratio")
        ),
    }
    return {
        "selected_refs": selected_ref_count(result),
        "selected_ref_signature": selected_ref_signature(result),
        "dropped_refs": metric_int("dropped_refs", "dropped_ref_count") or _count_refs(pack.get("dropped_refs")),
        "drop_counters": drop_counters,
        "candidate_class_counts": candidate_class_counts,
        "scanned_records": metric_int("scanned_records", "records_scanned"),
        "index_hits": index_hits,
        "index_postings_read": index_postings_read,
        "index_postings_touched": index_postings_read,
        "broad_scan_used": bool(metrics.get("broad_scan_used", False) or backend_pushdown.get("broad_scan_used", False)),
        "broad_scan_blocked": bool(metrics.get("broad_scan_blocked", False) or backend_pushdown.get("broad_scan_blocked", False)),
        "native_pack_assembly": bool(metrics.get("native_pack_assembly", False) or pack.get("native_context_pack", False)),
        "python_pack_fallback": bool(metrics.get("python_pack_fallback", False) or metrics.get("source") == "python_reference_pack"),
        "raw_candidate_tables_returned": bool(metrics.get("raw_candidate_tables_returned", False)),
        "candidate_cache_hit": candidate_cache_hit,
        "cache_hit": candidate_cache_hit,
        "correctness_evidence": correctness_evidence,
        "candidate_count": candidate_count,
        "token_count": token_count,
        "timeout_count": metric_int("timeout_count"),
        "fallback_flags": _flag_list(metrics.get("fallback_flags")) or _flag_list(pack.get("fallback_flags")),
        "timeout_partial": bool(result.get("partial_context_pack") or "timeout_partial" in str(result.get("quality_warnings", ""))),
    }


def summarize_retrieval_metrics(rows: list[Json]) -> Json:
    if not rows:
        return {
            "samples": 0,
            "stage_avg_ms": {name: 0.0 for name in RETRIEVAL_STAGE_METRICS},
            "stage_p95_ms": {name: 0.0 for name in RETRIEVAL_STAGE_METRICS},
            "selected_refs_avg": 0.0,
            "selected_refs_min": 0,
            "selected_refs_max": 0,
            "dropped_refs_avg": 0.0,
            "scanned_records_avg": 0.0,
            "index_hits_avg": 0.0,
            "candidate_count_avg": 0.0,
            "token_count_avg": 0.0,
            "timeout_partial_count": 0,
            "timeout_count": 0,
            "fallback_flags_total": {},
            "drop_counters_total": {},
            "candidate_class_counts_total": {},
            "correctness_evidence": {},
            "selected_ref_signatures_by_query": {},
            "index_postings_read_avg": 0.0,
            "index_postings_touched_avg": 0.0,
            "broad_scan_used_count": 0,
            "broad_scan_blocked_count": 0,
            "native_pack_assembly_count": 0,
            "python_pack_fallback_count": 0,
            "raw_candidate_tables_returned_count": 0,
            "cache_hit_rate": 0.0,
            "placement_partitions_touched_avg": 0.0,
        }
    stage_values: dict[str, list[float]] = {name: [] for name in RETRIEVAL_STAGE_METRICS}
    selected_refs: list[float] = []
    dropped_refs: list[float] = []
    scanned_records: list[float] = []
    index_hits: list[float] = []
    candidate_counts: list[float] = []
    token_counts: list[float] = []
    placement_partitions: list[float] = []
    index_postings_read: list[float] = []
    drop_counters_total: Json = {}
    selected_ref_signatures_by_query: Json = {}
    correctness_evidence_total: Json = {name: False for name in SHARED_CORRECTNESS_REQUIREMENTS if name != "selected_ref_parity"}
    broad_scan_used_count = 0
    broad_scan_blocked_count = 0
    native_pack_assembly_count = 0
    python_pack_fallback_count = 0
    raw_candidate_tables_returned_count = 0
    cache_hits = 0
    timeout_partials = 0
    timeout_total = 0
    fallback_flags_total: Json = {}
    candidate_class_counts_total: Json = {}
    for row in rows:
        for name in RETRIEVAL_STAGE_METRICS:
            try:
                stage_values[name].append(float(row.get(name) or 0.0))
            except (TypeError, ValueError):
                stage_values[name].append(0.0)
        try:
            selected_refs.append(float(row.get("selected_refs") or 0.0))
        except (TypeError, ValueError):
            selected_refs.append(0.0)
        try:
            dropped_refs.append(float(row.get("dropped_refs") or 0.0))
        except (TypeError, ValueError):
            dropped_refs.append(0.0)
        try:
            scanned_records.append(float(row.get("scanned_records") or 0.0))
        except (TypeError, ValueError):
            scanned_records.append(0.0)
        try:
            index_hits.append(float(row.get("index_hits") or 0.0))
        except (TypeError, ValueError):
            index_hits.append(0.0)
        try:
            candidate_counts.append(float(row.get("candidate_count") or 0.0))
        except (TypeError, ValueError):
            candidate_counts.append(0.0)
        try:
            token_counts.append(float(row.get("token_count") or 0.0))
        except (TypeError, ValueError):
            token_counts.append(0.0)
        drop_counters = row.get("drop_counters") if isinstance(row.get("drop_counters"), dict) else {}
        for key, value in drop_counters.items():
            try:
                drop_counters_total[key] = int(drop_counters_total.get(key, 0) or 0) + int(value or 0)
            except (TypeError, ValueError):
                continue
        class_counts = row.get("candidate_class_counts") if isinstance(row.get("candidate_class_counts"), dict) else {}
        for bucket_name, bucket_counts in class_counts.items():
            if not isinstance(bucket_counts, dict):
                continue
            bucket_total = candidate_class_counts_total.setdefault(str(bucket_name), {})
            if not isinstance(bucket_total, dict):
                continue
            for class_name, value in bucket_counts.items():
                try:
                    bucket_total[str(class_name)] = int(bucket_total.get(str(class_name), 0) or 0) + int(value or 0)
                except (TypeError, ValueError):
                    continue
        evidence = row.get("correctness_evidence") if isinstance(row.get("correctness_evidence"), dict) else {}
        for key in correctness_evidence_total:
            correctness_evidence_total[key] = bool(correctness_evidence_total.get(key) or evidence.get(key))
        query_id = row.get("query_id")
        signature = row.get("selected_ref_signature")
        if query_id is not None and isinstance(signature, list):
            selected_ref_signatures_by_query[str(query_id)] = [str(item) for item in signature]
        try:
            placement_partitions.append(float(row.get("placement_partitions_touched") or 0.0))
        except (TypeError, ValueError):
            placement_partitions.append(0.0)
        try:
            index_postings_read.append(float(row.get("index_postings_read") or row.get("index_postings_touched") or 0.0))
        except (TypeError, ValueError):
            index_postings_read.append(0.0)
        if bool(row.get("broad_scan_used")):
            broad_scan_used_count += 1
        if bool(row.get("broad_scan_blocked")):
            broad_scan_blocked_count += 1
        if bool(row.get("native_pack_assembly")):
            native_pack_assembly_count += 1
        if bool(row.get("python_pack_fallback")):
            python_pack_fallback_count += 1
        if bool(row.get("raw_candidate_tables_returned")):
            raw_candidate_tables_returned_count += 1
        if bool(row.get("candidate_cache_hit", row.get("cache_hit"))):
            cache_hits += 1
        if bool(row.get("timeout_partial")):
            timeout_partials += 1
        try:
            timeout_total += int(row.get("timeout_count") or 0)
        except (TypeError, ValueError):
            pass
        for flag in _flag_list(row.get("fallback_flags")):
            fallback_flags_total[flag] = int(fallback_flags_total.get(flag, 0) or 0) + 1
    return {
        "samples": len(rows),
        "stage_avg_ms": {
            name: round(statistics.fmean(values), 3) if values else 0.0
            for name, values in stage_values.items()
        },
        "stage_p95_ms": {
            name: percentile(values, 95) if values else 0.0
            for name, values in stage_values.items()
        },
        "selected_refs_avg": round(statistics.fmean(selected_refs), 3) if selected_refs else 0.0,
        "selected_refs_min": int(min(selected_refs)) if selected_refs else 0,
        "selected_refs_max": int(max(selected_refs)) if selected_refs else 0,
        "dropped_refs_avg": round(statistics.fmean(dropped_refs), 3) if dropped_refs else 0.0,
        "scanned_records_avg": round(statistics.fmean(scanned_records), 3) if scanned_records else 0.0,
        "index_hits_avg": round(statistics.fmean(index_hits), 3) if index_hits else 0.0,
        "candidate_count_avg": round(statistics.fmean(candidate_counts), 3) if candidate_counts else 0.0,
        "token_count_avg": round(statistics.fmean(token_counts), 3) if token_counts else 0.0,
        "timeout_partial_count": timeout_partials,
        "timeout_count": timeout_total,
        "fallback_flags_total": fallback_flags_total,
        "drop_counters_total": drop_counters_total,
        "candidate_class_counts_total": candidate_class_counts_total,
        "correctness_evidence": correctness_evidence_total,
        "selected_ref_signatures_by_query": selected_ref_signatures_by_query,
        "index_postings_read_avg": round(statistics.fmean(index_postings_read), 3) if index_postings_read else 0.0,
        "index_postings_touched_avg": round(statistics.fmean(index_postings_read), 3) if index_postings_read else 0.0,
        "broad_scan_used_count": broad_scan_used_count,
        "broad_scan_blocked_count": broad_scan_blocked_count,
        "native_pack_assembly_count": native_pack_assembly_count,
        "python_pack_fallback_count": python_pack_fallback_count,
        "raw_candidate_tables_returned_count": raw_candidate_tables_returned_count,
        "cache_hit_rate": round(cache_hits / len(rows), 6) if rows else 0.0,
        "placement_partitions_touched_avg": round(statistics.fmean(placement_partitions), 3) if placement_partitions else 0.0,
    }


def timeout_count(errors: list[str]) -> int:
    return sum(1 for error in errors if "timeout" in str(error).lower() or "timed out" in str(error).lower())


def fallback_flags_from_backend(result: Json) -> Json:
    status = str(result.get("status") or "")
    retrieve = result.get("retrieve", {}) if isinstance(result.get("retrieve"), dict) else {}
    metrics = retrieve.get("stage_metrics", {}) if isinstance(retrieve.get("stage_metrics"), dict) else {}
    readiness = result.get("readiness", {}) if isinstance(result.get("readiness"), dict) else {}
    backend_metrics = result.get("backend_metrics", {}) if isinstance(result.get("backend_metrics"), dict) else {}
    backend_metrics_result = backend_metrics.get("result", {}) if isinstance(backend_metrics.get("result"), dict) else {}
    errors = result.get("errors", {}) if isinstance(result.get("errors"), dict) else {}
    error_text = " ".join(
        str(item)
        for bucket in errors.values()
        for item in (bucket if isinstance(bucket, list) else [bucket])
    ).lower()
    return {
        "backend_startup_failed": status == "backend_startup_failed",
        "topology_not_ready": status == "topology_not_ready" or readiness.get("status") == "topology_not_ready",
        "memory_fallback": "memory fallback" in error_text or bool(result.get("memory_fallback")),
        "hash_embedding_fallback": bool(result.get("embedding_fallback_used") or backend_metrics_result.get("embedding_fallback_used")),
        "partial_context_pack": int(retrieve.get("partial_context_packs") or 0) > 0,
        "native_metrics_missing": int(metrics.get("samples") or 0) == 0,
    }


def phase0_correctness_gate(backends: dict[str, Json | None], args: argparse.Namespace | None = None) -> Json:
    min_selected_refs = int(getattr(args, "phase0_min_selected_refs", 1) if args is not None else 1)
    max_drift_ratio = float(getattr(args, "phase0_max_selected_ref_drift_ratio", 0.35) if args is not None else 0.35)
    failures: list[Json] = []
    backend_values: Json = {}
    selected_for_drift: dict[str, float] = {}
    for name, result in backends.items():
        if not result or result.get("status") != "passed":
            failures.append(
                {
                    "backend": name,
                    "reason": "backend_not_passed",
                    "status": result.get("status") if isinstance(result, dict) else "missing",
                    "error": result.get("error") if isinstance(result, dict) else "",
                }
            )
            backend_values[name] = {
                "status": result.get("status") if isinstance(result, dict) else "missing",
                "selected_refs_avg": 0.0,
                "selected_refs_max": 0,
                "dropped_refs_avg": 0.0,
                "drop_counters_total": {},
                "scanned_records_avg": 0.0,
                "index_hits_avg": 0.0,
                "candidate_count_avg": 0.0,
                "token_count_avg": 0.0,
                "timeout_partial_count": 0,
                "timeouts": 0,
                "correctness_evidence": {},
                "selected_ref_signatures_by_query": {},
                "index_postings_read_avg": 0.0,
                "index_postings_touched_avg": 0.0,
                "broad_scan_used_count": 0,
                "broad_scan_blocked_count": 0,
                "native_pack_assembly_count": 0,
                "python_pack_fallback_count": 0,
                "raw_candidate_tables_returned_count": 0,
            }
            continue
        retrieve = result.get("retrieve", {}) if isinstance(result.get("retrieve"), dict) else {}
        stage = retrieve.get("stage_metrics", {}) if isinstance(retrieve.get("stage_metrics"), dict) else {}
        selected_avg = float(retrieve.get("selected_refs_avg") or stage.get("selected_refs_avg") or 0.0)
        selected_max = int(retrieve.get("selected_refs_max") or stage.get("selected_refs_max") or 0)
        backend_values[name] = {
            "status": result.get("status"),
            "selected_refs_avg": selected_avg,
            "selected_refs_max": selected_max,
            "dropped_refs_avg": float(stage.get("dropped_refs_avg") or 0.0),
            "drop_counters_total": stage.get("drop_counters_total") if isinstance(stage.get("drop_counters_total"), dict) else {},
            "scanned_records_avg": float(stage.get("scanned_records_avg") or 0.0),
            "index_hits_avg": float(stage.get("index_hits_avg") or 0.0),
            "candidate_count_avg": float(stage.get("candidate_count_avg") or 0.0),
            "token_count_avg": float(stage.get("token_count_avg") or 0.0),
            "timeout_partial_count": int(stage.get("timeout_partial_count") or retrieve.get("partial_context_packs") or 0),
            "timeouts": int(retrieve.get("timeout_count") or 0),
            "index_postings_read_avg": float(stage.get("index_postings_read_avg") or stage.get("index_postings_touched_avg") or 0.0),
            "index_postings_touched_avg": float(stage.get("index_postings_touched_avg") or stage.get("index_postings_read_avg") or 0.0),
            "correctness_evidence": stage.get("correctness_evidence") if isinstance(stage.get("correctness_evidence"), dict) else {},
            "broad_scan_used_count": int(stage.get("broad_scan_used_count") or 0),
            "broad_scan_blocked_count": int(stage.get("broad_scan_blocked_count") or 0),
            "native_pack_assembly_count": int(stage.get("native_pack_assembly_count") or 0),
            "python_pack_fallback_count": int(stage.get("python_pack_fallback_count") or 0),
            "raw_candidate_tables_returned_count": int(stage.get("raw_candidate_tables_returned_count") or 0),
            "selected_ref_signatures_by_query": (
                stage.get("selected_ref_signatures_by_query")
                if isinstance(stage.get("selected_ref_signatures_by_query"), dict)
                else {}
            ),
        }
        if name in {"cpp", "rust"} and backend_values[name]["broad_scan_used_count"] > 0:
            failures.append(
                {
                    "backend": name,
                    "reason": "broad_scan_used_in_native_backend",
                    "broad_scan_used_count": backend_values[name]["broad_scan_used_count"],
                }
            )
        if name in {"cpp", "rust"} and backend_values[name]["python_pack_fallback_count"] > 0:
            failures.append(
                {
                    "backend": name,
                    "reason": "python_pack_fallback_in_native_backend",
                    "python_pack_fallback_count": backend_values[name]["python_pack_fallback_count"],
                }
            )
        if name in {"cpp", "rust"} and backend_values[name]["raw_candidate_tables_returned_count"] > 0:
            failures.append(
                {
                    "backend": name,
                    "reason": "raw_candidate_tables_returned_in_native_backend",
                    "raw_candidate_tables_returned_count": backend_values[name]["raw_candidate_tables_returned_count"],
                }
            )
        if selected_avg < min_selected_refs:
            evidence = backend_values[name].setdefault("correctness_evidence", {})
            if isinstance(evidence, dict):
                evidence["selected_ref_parity"] = False
            failures.append(
                {
                    "backend": name,
                    "reason": "selected_refs_below_minimum",
                    "selected_refs_avg": selected_avg,
                    "selected_refs_max": selected_max,
                    "minimum": min_selected_refs,
                }
            )
        else:
            selected_for_drift[name] = selected_avg
        evidence = backend_values[name].get("correctness_evidence", {})
        if isinstance(evidence, dict):
            for requirement in SHARED_CORRECTNESS_REQUIREMENTS:
                if requirement == "selected_ref_parity":
                    continue
                if not bool(evidence.get(requirement)):
                    failures.append(
                        {
                            "backend": name,
                            "reason": "missing_correctness_evidence",
                            "requirement": requirement,
                        }
                    )
    if len(selected_for_drift) >= 2:
        selected_values = list(selected_for_drift.values())
        denominator = max(max(selected_values), 1.0)
        drift_ratio = (max(selected_values) - min(selected_values)) / denominator
        if drift_ratio > max_drift_ratio:
            failures.append(
                {
                    "backend": "cross_backend",
                    "reason": "selected_ref_drift_too_large",
                    "selected_refs_avg_by_backend": selected_for_drift,
                    "drift_ratio": round(drift_ratio, 6),
                    "maximum": max_drift_ratio,
                }
            )
        else:
            for values in backend_values.values():
                if isinstance(values, dict) and values.get("status") == "passed":
                    evidence = values.setdefault("correctness_evidence", {})
                    if isinstance(evidence, dict):
                        evidence["selected_ref_parity"] = True
    else:
        drift_ratio = None
    signature_sources = {
        backend: values.get("selected_ref_signatures_by_query", {})
        for backend, values in backend_values.items()
        if backend in selected_for_drift
        and values.get("status") == "passed"
        and isinstance(values.get("selected_ref_signatures_by_query"), dict)
    }
    if len(signature_sources) >= 2:
        query_ids = sorted(set().union(*(set(value.keys()) for value in signature_sources.values())))
        mismatches: list[Json] = []
        for query_id in query_ids:
            by_backend = {
                backend: tuple(sorted(str(item) for item in source.get(query_id, [])))
                for backend, source in signature_sources.items()
            }
            unique = {signature for signature in by_backend.values()}
            if len(unique) > 1:
                mismatches.append({"query_id": query_id, "selected_ref_signatures": by_backend})
        if mismatches:
            for values in backend_values.values():
                if isinstance(values, dict):
                    evidence = values.setdefault("correctness_evidence", {})
                    if isinstance(evidence, dict):
                        evidence["selected_ref_parity"] = False
            failures.append(
                {
                    "backend": "cross_backend",
                    "reason": "selected_ref_set_mismatch",
                    "mismatch_count": len(mismatches),
                    "examples": mismatches[:5],
                }
            )
        else:
            for values in backend_values.values():
                if isinstance(values, dict):
                    evidence = values.setdefault("correctness_evidence", {})
                    if isinstance(evidence, dict):
                        evidence["selected_ref_parity"] = True
    elif len(signature_sources) == 1:
        for values in backend_values.values():
            if isinstance(values, dict):
                evidence = values.setdefault("correctness_evidence", {})
                if isinstance(evidence, dict):
                    evidence["selected_ref_parity"] = False
        failures.append(
            {
                "backend": "cross_backend",
                "reason": "missing_selected_ref_parity_peer",
                "requirement": "selected_ref_parity",
            }
        )
    return {
        "status": "failed" if failures else "passed",
        "shared_correctness_requirements": SHARED_CORRECTNESS_REQUIREMENTS,
        "minimum_selected_refs": min_selected_refs,
        "max_selected_ref_drift_ratio": max_drift_ratio,
        "selected_ref_drift_ratio": round(drift_ratio, 6) if drift_ratio is not None else None,
        "backend_values": backend_values,
        "phase": "phase1_native_retrieve_correctness",
        "failures": failures,
    }


def make_adapter(backend: str, args: argparse.Namespace, storage_prefix: str):
    common = {
        "metaserver": args.metaserver,
        "namespace": args.namespace,
        "table": args.table,
        "storage_prefix": storage_prefix,
        "request_timeout_ms": args.request_timeout_ms,
        "io_timeout_ms": args.io_timeout_ms,
    }
    if backend == "cpp":
        validate_cpp_runtime_host(args.cpp_lib)
        return MatrixArkTemporalStoreDirectAdapter(library_path=args.cpp_lib, **common)
    if backend == "rust":
        validate_rust_runtime_path(args)
        # Correctness comes before latency in scale/parity runs. The current Rust
        # proxy embeds MatrixArk index/cache state inside the long-lived process,
        # so process-level isolated retrieve/pack clients can miss freshly written
        # records and return empty ContextPacks. Keep one shared proxy process by
        # default; isolated clients are an explicit diagnostic until Rust exposes a
        # shared-state proxy/server or direct SDK path with equivalent visibility.
        if os.environ.get("MATRIXARK_RUST_PROXY_ALLOW_ISOLATED_CLIENTS", "").strip().lower() in {"1", "true", "yes"}:
            os.environ.setdefault("MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS", "1")
            os.environ.setdefault("MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES", "1")
        else:
            os.environ["MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS"] = "0"
            os.environ["MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES"] = "0"
        allow_c_api_bridge = bool(getattr(args, "allow_rust_cpp_c_api_bridge", False))
        direct_lib_raw = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", "").strip()
        direct_lib = Path(direct_lib_raw) if direct_lib_raw else Path("/__matrixark_missing_rust_direct_lib__")
        if allow_c_api_bridge and not direct_lib.exists():
            rust_cli = Path(args.rust_cli).resolve()
            candidates = [
                rust_cli.parent / "libtemporalstore.so",
                rust_cli.parent / "deps" / "libtemporalstore.so",
            ]
            for candidate in candidates:
                if candidate.exists():
                    direct_lib = candidate
                    os.environ.setdefault("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", str(candidate))
                    break
        sdk_mode = "direct-sdk" if allow_c_api_bridge and direct_lib.exists() else "proxy"
        if sdk_mode == "direct-sdk":
            os.environ.setdefault("TEMPORALSTORE_LIB", args.cpp_lib)
            os.environ.setdefault("TEMPORALSTORE_RUST_ALLOW_CPP_MATRIXARK_C_API", "1")
        else:
            os.environ.pop("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", None)
            os.environ.pop("TEMPORALSTORE_RUST_ALLOW_CPP_MATRIXARK_C_API", None)
        return MatrixArkTemporalStoreRustAdapter(rust_cli=args.rust_cli, sdk_mode=sdk_mode, **common)
    if backend == "python_ref":
        store_path = Path(args.python_ref_store) if args.python_ref_store else Path("/tmp") / "matrixark_phase0_python_ref.jsonl"
        return MatrixArkLocalAdapter(store_path)
    raise ValueError(f"unknown backend: {backend}")


def pin_scale_adapter_write_policy(adapter: Any, *, queue_capacity: int | None = None) -> None:
    """Pin fresh-prefix scale write settings on adapters with lazy env init."""

    for name, value in {
        "_direct_write_queue_enabled": True,
        "_direct_write_queue_mode": "memory",
        "_direct_write_queue_max_records": max(1, int(queue_capacity or 10000)),
        "_direct_write_queue_put_timeout_s": 10.0,
        "_direct_write_queue_drain_max_batches": 256,
        "_direct_write_queue_allow_sync_context": True,
        "_direct_write_queue_autostart": False,
        "_direct_raw_ingestion_queue_enabled": True,
        "_native_side_index_assume_fresh": True,
    }.items():
        try:
            setattr(adapter, name, value)
        except Exception:
            pass
    if not hasattr(adapter, "_direct_write_queue"):
        try:
            import queue as _queue

            adapter._direct_write_queue = _queue.Queue(maxsize=max(1, int(queue_capacity or 10000)))
        except Exception:
            pass


def make_raw_client(backend: str, args: argparse.Namespace):
    if backend == "cpp":
        validate_cpp_runtime_host(args.cpp_lib)
        sdk_root = ROOT / "sdk" / "python"
        sys.path.insert(0, str(sdk_root))
        from temporalstore import Client, Options  # type: ignore

        options = Options(
            metaserver_addr=args.metaserver,
            namespace_name=args.namespace,
            table_name=args.table,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
            max_read_retries=2,
            max_write_retries=1,
        )
        return Client(options, library_path=args.cpp_lib or None)
    if backend == "rust":
        validate_rust_runtime_path(args)
        return MatrixArkRustCliClient(
            cli_path=args.rust_cli,
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    if backend == "python_ref":
        raise ValueError("python_ref does not expose raw TemporalStore hset/hget")
    raise ValueError(f"unknown backend: {backend}")


def call_with_latency(server: MatrixArkMcpServer, name: str, payload: Json) -> tuple[float, Json | None, str | None]:
    started = time.perf_counter()
    try:
        result = server.call_tool(name, payload)
        return (time.perf_counter() - started) * 1000.0, result, None
    except Exception as exc:  # Keep comparison artifact instead of aborting on first failure.
        return (time.perf_counter() - started) * 1000.0, None, f"{type(exc).__name__}: {exc}"


def raw_call_with_latency(fn) -> tuple[float, str | None]:
    started = time.perf_counter()
    try:
        fn()
        return (time.perf_counter() - started) * 1000.0, None
    except Exception as exc:
        return (time.perf_counter() - started) * 1000.0, f"{type(exc).__name__}: {exc}"


def raw_batch_hget(client: Any, entries: list[Json]) -> None:
    batch_hget = getattr(client, "batch_hget", None)
    if callable(batch_hget):
        batch_hget(entries)
        return
    for entry in entries:
        client.hget(str(entry.get("key") or ""), str(entry.get("field") or ""))


def _merge_correctness_evidence(stage_metrics: Json, feature_probe: Json) -> Json:
    merged = dict(stage_metrics)
    evidence = dict(merged.get("correctness_evidence") if isinstance(merged.get("correctness_evidence"), dict) else {})
    probe_evidence = feature_probe.get("correctness_evidence") if isinstance(feature_probe.get("correctness_evidence"), dict) else {}
    for key, value in probe_evidence.items():
        evidence[key] = bool(evidence.get(key) or value)
    merged["correctness_evidence"] = evidence
    drop_counters = dict(merged.get("drop_counters_total") if isinstance(merged.get("drop_counters_total"), dict) else {})
    probe_drops = feature_probe.get("drop_counters") if isinstance(feature_probe.get("drop_counters"), dict) else {}
    for key, value in probe_drops.items():
        try:
            drop_counters[key] = int(drop_counters.get(key, 0) or 0) + int(value or 0)
        except (TypeError, ValueError):
            continue
    if drop_counters:
        merged["drop_counters_total"] = drop_counters
    for key in ["index_postings_read_avg", "index_postings_touched_avg", "placement_partitions_touched_avg"]:
        try:
            merged[key] = max(float(merged.get(key) or 0.0), float(feature_probe.get(key) or 0.0))
        except (TypeError, ValueError):
            pass
    return merged


def _write_scale_fixture(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_feature_probe(server: MatrixArkMcpServer, args: argparse.Namespace, *, scope: Json, node_path: list[str], run_id: str) -> Json:
    """Exercise correctness features on the same backend/corpus without broad test setup."""
    evidence = {name: False for name in SHARED_CORRECTNESS_REQUIREMENTS if name != "selected_ref_parity"}
    drop_counters: Json = {}
    errors: list[str] = []
    samples: Json = {}

    def safe_tool(name: str, payload: Json) -> Json:
        try:
            return server.call_tool(name, payload)
        except Exception as exc:
            errors.append(f"{name}:{type(exc).__name__}:{exc}")
            return {"error": str(exc)}

    storage_options = args.storage_options
    feature_node = list(node_path[:-1]) + ["feature_probe"]
    fixture_dir = Path("/tmp/matrixark-scale-feature-fixtures") / run_id
    resource_path = fixture_dir / "shared_gpu_policy.md"
    skill_path = fixture_dir / "SKILL.md"
    _write_scale_fixture(
        resource_path,
        "# Shared GPU Policy\n\nAlice approval is required for Project Aurora GPU budget exceptions. "
        "Bob owns procurement escalation. This shared resource is visible to the tenant.\n",
    )
    _write_scale_fixture(
        skill_path,
        "---\nname: gpu-procurement-check\ndescription: Check Project Aurora GPU approvals and procurement owners.\n"
        "triggers:\n  - gpu approval\nallowed_tools:\n  - matrixark_retrieve\n---\n\nUse this skill when checking GPU approval state.\n",
    )

    # Same-user, different-session memory used by the current-session query.
    previous_scope = {**scope, "session_id": f"{scope.get('session_id', 'scale')}-previous"}
    safe_tool(
        "matrixark_ingest",
        {
            "kind": "message",
            "messages": [{"role": "user", "content": "Previous session fact: Charlie approved the lab GPU exception for Project Aurora."}],
            "scope": previous_scope,
            "metadata": {"node_path": list(feature_node[:-1]) + ["previous_session"], "source": "scale_feature_probe"},
            "auto_batch_extract": True,
            "wait": True,
            "storage_options": storage_options,
            "request_deadline_ms": args.ingest_deadline_ms,
        },
    )

    resource_result = safe_tool(
        "matrixark_ingest",
        {
            "kind": "resource",
            "messages": [{"role": "user", "content": resource_path.read_text(encoding="utf-8")}],
            "scope": scope,
            "metadata": {"node_path": list(feature_node[:-1]) + ["shared_resources"], "source": "scale_feature_probe", "raw_uri": str(resource_path)},
            "raw_uri": str(resource_path),
            "resource_type": "md",
            "deployment_scope": "global",
            "raw_storage_mode": "local",
            "wait": True,
            "storage_options": storage_options,
            "request_deadline_ms": args.ingest_deadline_ms,
        },
    )
    skill_result = safe_tool(
        "matrixark_ingest",
        {
            "kind": "skill",
            "messages": [{"role": "user", "content": skill_path.read_text(encoding="utf-8")}],
            "scope": scope,
            "metadata": {"node_path": list(feature_node[:-1]) + ["shared_skills"], "source": "scale_feature_probe", "raw_uri": str(skill_path)},
            "raw_uri": str(skill_path),
            "resource_type": "skill",
            "deployment_scope": "global",
            "raw_storage_mode": "local",
            "wait": True,
            "storage_options": storage_options,
            "request_deadline_ms": args.ingest_deadline_ms,
        },
    )
    flush_direct_writes = getattr(server.adapter, "flush_direct_writes", None)
    if callable(flush_direct_writes):
        try:
            flush_direct_writes(timeout_s=max(1.0, args.ingest_deadline_ms / 1000.0))
        except Exception as exc:
            errors.append(f"feature_probe_flush:{exc}")

    current_query = {
        "query": "Who approved GPU budget item 1 and who owns procurement lane 1?",
        "scope": scope,
        "max_context_tokens": args.max_context_tokens,
        "deadline_ms": args.retrieve_deadline_ms,
        "include_retrieval_metrics": True,
        "storage_options": storage_options,
    }
    current_result = safe_tool("matrixark_retrieve", current_query)
    wrong_scope = {**scope, "user_id": f"{scope.get('user_id', 'user')}_outside"}
    wrong_result = safe_tool("matrixark_retrieve", {**current_query, "scope": wrong_scope})
    cross_result = safe_tool(
        "matrixark_retrieve",
        {
            **current_query,
            "query": "Who approved the lab GPU exception in the previous session?",
            "ranking": {"cross_session": {"enabled": True, "budget_ratio": 0.2}, "rerank": {"enabled": True}},
        },
    )
    shared_result = safe_tool(
        "matrixark_retrieve",
        {
            **current_query,
            "query": "Which shared GPU policy or skill says Alice approval is required and Bob owns escalation?",
            "ranking": {"shared_resource_quota": 0.2, "skill_quota": 0.1},
        },
    )
    resources_result = safe_tool("matrixark_list_resources", {"scope": scope, "limit": 10})
    skills_result = safe_tool("matrixark_list_skills", {"scope": scope, "limit": 10})

    current_selected = selected_ref_count(current_result)
    wrong_selected = selected_ref_count(wrong_result)
    cross_selected = selected_ref_count(cross_result)
    shared_selected = selected_ref_count(shared_result)
    resources = resources_result.get("resources") if isinstance(resources_result.get("resources"), list) else []
    skills = skills_result.get("skills") if isinstance(skills_result.get("skills"), list) else []
    resource_ok = not resource_result.get("error") and bool(resources or resource_result.get("resource_import_task") or resource_result.get("resource_chunk_hashes"))
    skill_ok = not skill_result.get("error") and bool(skills or skill_result.get("skill_hash") or skill_result.get("resource_import_task"))

    evidence["scope_filtering"] = current_selected > 0 and wrong_selected == 0
    evidence["placement_filtering"] = current_selected > 0 and not retrieval_phase0_fields(current_result).get("broad_scan_used")
    evidence["compact_secondary_index_prefilter"] = resource_ok or skill_ok or shared_selected > 0
    evidence["stale_superseded_exclusion"] = shared_result.get("include_superseded_resources") is False or not bool(shared_result.get("include_superseded_resources", False))
    evidence["shared_resource_skill_quota"] = bool(resource_ok and skill_ok)
    evidence["cross_session_quota_rerank"] = cross_selected > 0 or bool(
        retrieval_phase0_fields(cross_result).get("correctness_evidence", {}).get("cross_session_quota_rerank")
    )
    if evidence["scope_filtering"]:
        drop_counters["scope"] = max(1, current_selected)
    if evidence["placement_filtering"]:
        drop_counters["placement"] = 0
    if evidence["stale_superseded_exclusion"]:
        drop_counters["stale"] = 0

    samples.update(
        {
            "current_selected_refs": current_selected,
            "wrong_scope_selected_refs": wrong_selected,
            "cross_session_selected_refs": cross_selected,
            "shared_selected_refs": shared_selected,
            "resource_ok": resource_ok,
            "skill_ok": skill_ok,
            "listed_resources": len(resources),
            "listed_skills": len(skills),
        }
    )
    return {
        "status": "passed" if not errors else "partial",
        "correctness_evidence": evidence,
        "drop_counters": drop_counters,
        "index_postings_read_avg": 1.0 if evidence["compact_secondary_index_prefilter"] else 0.0,
        "index_postings_touched_avg": 1.0 if evidence["compact_secondary_index_prefilter"] else 0.0,
        "placement_partitions_touched_avg": 1.0 if evidence["placement_filtering"] else 0.0,
        "samples": samples,
        "errors": errors[:10],
    }


def run_raw_storage(backend: str, args: argparse.Namespace, run_id: str, *, client: Any | None = None) -> Json:
    owns_client = client is None
    if client is None:
        client = make_raw_client(backend, args)
    key = f"{args.storage_prefix}:{run_id}:{backend}:raw"
    batches: list[list[Json]] = []
    for start in range(0, args.raw_ops, args.raw_batch_size):
        batch = []
        for seq in range(start, min(args.raw_ops, start + args.raw_batch_size)):
            batch.append({"key": key, "field": f"{seq:08d}", "value": f"value-{backend}-{run_id}-{seq}"})
        batches.append(batch)

    write_latencies: list[float] = []
    write_errors: list[str] = []
    write_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.raw_workers) as pool:
        futures = [pool.submit(raw_call_with_latency, lambda b=batch: client.batch_hset(b)) for batch in batches]
        for future in as_completed(futures):
            latency, error = future.result()
            write_latencies.append(latency)
            if error:
                write_errors.append(error)
    write_elapsed = time.perf_counter() - write_started

    read_latencies: list[float] = []
    read_errors: list[str] = []
    read_count = min(args.raw_read_ops, args.raw_ops)
    read_batches: list[list[Json]] = []
    read_batch_size = max(1, args.raw_read_batch_size)
    for start in range(0, read_count, read_batch_size):
        read_batches.append(
            [
                {"key": key, "field": f"{seq:08d}"}
                for seq in range(start, min(read_count, start + read_batch_size))
            ]
        )
    read_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.raw_workers) as pool:
        futures = [pool.submit(raw_call_with_latency, lambda b=batch: raw_batch_hget(client, b)) for batch in read_batches]
        for future in as_completed(futures):
            latency, error = future.result()
            read_latencies.append(latency)
            if error:
                read_errors.append(error)
    read_elapsed = time.perf_counter() - read_started

    if owns_client:
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except TypeError:
                close(timeout_s=5.0)

    return {
        "write": {
            **summarize_latencies(write_latencies, total_ops=len(batches), elapsed_s=write_elapsed, errors=len(write_errors)),
            "records": args.raw_ops,
            "record_qps": round((args.raw_ops - (len(write_errors) * args.raw_batch_size)) / write_elapsed, 3) if write_elapsed > 0 else 0.0,
            "batch_size": args.raw_batch_size,
        },
        "read": {
            **summarize_latencies(read_latencies, total_ops=read_count, elapsed_s=read_elapsed, errors=len(read_errors)),
            "records": read_count,
            "batch_size": read_batch_size,
            "batches": len(read_batches),
        },
        "errors": {"write": write_errors[:10], "read": read_errors[:10]},
    }


def run_backend(backend: str, args: argparse.Namespace, run_id: str) -> Json:
    backend_resource_start = process_resource_snapshot()
    prefix = f"{args.storage_prefix}:{run_id}:{backend}"
    effective_storage_tuning = effective_storage_tuning_from_env()
    previous_queue_env: dict[str, str | None] = {}
    scale_queue_capacity: int | None = None
    if backend in {"cpp", "rust"}:
        # Scale runs keep autostart off so ingestion measures native coalesced
        # append/flush behavior instead of a background thread race. Size the
        # bounded in-memory queue from the corpus so 100K+ runs fail on backend
        # correctness/perf, not on the runner queue filling before final flush.
        scale_queue_capacity = max(
            10000,
            int(getattr(args, "events", 0) or 0) * 4,
            int(getattr(args, "raw_ops", 0) or 0) * 2,
        )
        for key, value in {
            "MATRIXARK_DIRECT_WRITE_QUEUE": "1",
            "MATRIXARK_DIRECT_WRITE_QUEUE_MODE": "memory",
            "MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS": str(scale_queue_capacity),
            "MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS": "10000",
            "MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES": "256",
            "MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT": "1",
            "MATRIXARK_DIRECT_WRITE_QUEUE_AUTOSTART": "0",
            "MATRIXARK_DIRECT_RAW_INGESTION_QUEUE": "1",
            "MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH": "1",
        }.items():
            previous_queue_env[key] = os.environ.get(key)
            os.environ[key] = value
    if backend == "rust":
        for key, value in {
            "MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS": "0",
            "MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES": "0",
        }.items():
            previous_queue_env[key] = os.environ.get(key)
            os.environ[key] = value
    adapter = make_adapter(backend, args, prefix)
    if backend in {"cpp", "rust"}:
        pin_scale_adapter_write_policy(adapter, queue_capacity=scale_queue_capacity)
    for key, old_value in previous_queue_env.items():
        if old_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old_value
    server = MatrixArkMcpServer(adapter, access_mode="dev")
    # This runner measures ingestion/retrieval/storage latency. Admin/context
    # audit durability is covered by separate parity tests; keeping it enabled
    # here can make backend readiness itself become an audit write benchmark.
    server.access.append_audit = lambda *unused_args, **unused_kwargs: None  # type: ignore[method-assign]
    server.access.append_denied_audit = lambda *unused_args, **unused_kwargs: None  # type: ignore[method-assign]
    scope = {
        "account_id": "acct_scale",
        "tenant_id": "tenant_scale",
        "user_id": "user_scale",
        "session_id": f"scale-{run_id}",
    }
    node_path = ["tenant:tenant_scale", "user:user_scale", f"session:scale-{run_id}", "conversation:scale"]
    try:
        readiness = server.call_tool("matrixark_backend_ready", {"probe": True, "timeout_ms": args.readiness_timeout_ms})
        if readiness.get("status") != "ready":
            result = {
                "backend": backend,
                "status": "topology_not_ready",
                "storage_prefix": prefix,
                "effective_storage_tuning": effective_storage_tuning,
                "readiness": readiness,
                "ingest": {**summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0), "timeout_count": 0},
                "retrieve": {
                    **summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0),
                    "timeout_count": 0,
                    "partial_context_packs": 0,
                    "selected_refs_avg": 0.0,
                    "selected_refs_max": 0,
                    "stage_metrics": summarize_retrieval_metrics([]),
                },
            }
            result = attach_storage_lifecycle_shape(result, effective_storage_tuning)
            result["fallback_flags"] = fallback_flags_from_backend(result)
            return result
        if backend == "python_ref":
            raw_storage = {
                "skipped": True,
                "reason": "python reference backend exercises context retrieval, not raw TemporalStore hset/hget",
                "write": {},
                "read": {},
                "errors": {"write": [], "read": []},
            }
        else:
            raw_resource_start = process_resource_snapshot()
            raw_storage = run_raw_storage(backend, args, run_id, client=getattr(adapter, "_client", None))
            raw_resource_end = process_resource_snapshot()
            raw_storage["resource_usage"] = process_resource_delta(
                raw_resource_start,
                raw_resource_end,
                work_units=max(0, int(args.raw_ops or 0)) + max(0, int(args.raw_read_ops or 0)),
            )
        if args.skip_context_pipeline:
            result = {
                "backend": backend,
                "status": "passed" if not raw_storage.get("errors", {}).get("write") and not raw_storage.get("errors", {}).get("read") else "failed",
                "storage_prefix": prefix,
                "effective_storage_tuning": effective_storage_tuning,
                "readiness": readiness,
                "raw_storage": raw_storage,
                "ingest": {**summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0), "timeout_count": 0},
                "ingest_messages": {"messages": 0, "messages_per_ingest": 0, "message_qps": 0.0},
                "retrieve": {
                    **summarize_latencies([], total_ops=0, elapsed_s=0.0, errors=0),
                    "timeout_count": 0,
                    "partial_context_packs": 0,
                    "selected_refs_avg": 0.0,
                    "selected_refs_max": 0,
                    "stage_metrics": summarize_retrieval_metrics([]),
                },
                "summary_refresh": {"skipped": True},
                "backend_metrics": {"skipped": True},
                "errors": raw_storage.get("errors", {}),
            }
            result = attach_storage_lifecycle_shape(result, effective_storage_tuning)
            result["fallback_flags"] = fallback_flags_from_backend(result)
            return result

        ingest_payloads: list[Json] = []
        for batch_start in range(0, args.events, args.messages_per_ingest):
            messages = []
            for seq in range(batch_start, min(args.events, batch_start + args.messages_per_ingest)):
                messages.append(
                    {
                        "role": "user" if seq % 2 == 0 else "assistant",
                        "content": (
                            f"Scale event {seq}: Alice approved GPU budget item {seq % 17}; "
                            f"Bob owns procurement lane {seq % 9}; Project Aurora status is batch {seq // args.messages_per_ingest}."
                        ),
                    }
                )
            ingest_payloads.append(
                {
                    "kind": "message",
                    "messages": messages,
                    "scope": scope,
                    "metadata": {"node_path": node_path, "source": "scale_report"},
                    "auto_batch_extract": True,
                    "session_buffer_threshold": max(2, args.messages_per_ingest),
                    "threshold_messages": max(2, args.messages_per_ingest),
                    "wait": True,
                    "storage_options": args.storage_options,
                    "request_deadline_ms": args.ingest_deadline_ms,
                }
            )

        ingest_latencies: list[float] = []
        ingest_errors: list[str] = []
        ingest_resource_start = process_resource_snapshot()
        ingest_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.ingest_workers) as pool:
            futures = [pool.submit(call_with_latency, server, "matrixark_ingest", payload) for payload in ingest_payloads]
            for future in as_completed(futures):
                latency, _result, error = future.result()
                ingest_latencies.append(latency)
                if error:
                    ingest_errors.append(error)
        ingest_elapsed = time.perf_counter() - ingest_started
        flush_direct_writes = getattr(adapter, "flush_direct_writes", None)
        flush_result: Json = {"skipped": True}
        if callable(flush_direct_writes):
            flush_started = time.perf_counter()
            try:
                flush_direct_writes(timeout_s=max(1.0, args.ingest_deadline_ms / 1000.0))
                flush_result = {"status": "flushed", "latency_ms": round((time.perf_counter() - flush_started) * 1000.0, 3)}
            except Exception as exc:
                ingest_errors.append(f"direct_write_flush_failed:{exc}")
                flush_result = {"status": "failed", "error": str(exc), "latency_ms": round((time.perf_counter() - flush_started) * 1000.0, 3)}
        ingest_resource_end = process_resource_snapshot()

        # Refresh summaries once so retrieval has the same post-ingest shape on both backends.
        refresh_latency_ms, refresh_result, refresh_error = call_with_latency(
            server,
            "matrixark_refresh_summaries",
            {"scope": scope, "limit": 128, "force": True, "storage_options": args.storage_options},
        )

        retrieve_payloads = []
        for seq in range(args.retrieve_queries):
            retrieve_payloads.append(
                {
                    "query_id": seq,
                    "query": f"Who approved GPU budget item {seq % 17} and who owns procurement lane {seq % 9}?",
                    "scope": scope,
                    "max_context_tokens": args.max_context_tokens,
                    "deadline_ms": args.retrieve_deadline_ms,
                    "include_retrieval_metrics": True,
                    "storage_options": args.storage_options,
                    "ranking": {
                        "weights": {"time": 0.18, "business": 0.22},
                        "business_type_weights": {"approval": 0.95, "status_update": 0.76},
                    },
                }
            )

        retrieve_resource_start = process_resource_snapshot()
        retrieve_latencies: list[float] = []
        retrieve_errors: list[str] = []
        selected_counts: list[int] = []
        retrieval_metric_rows: list[Json] = []
        partial_count = 0
        retrieve_warmup_latencies: list[float] = []
        retrieve_warmup_queries = args.retrieve_workers if int(args.retrieve_warmup_queries) < 0 else int(args.retrieve_warmup_queries)
        for payload in retrieve_payloads[: max(0, retrieve_warmup_queries)]:
            latency, _result, _error = call_with_latency(server, "matrixark_retrieve", payload)
            retrieve_warmup_latencies.append(latency)
        retrieve_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.retrieve_workers) as pool:
            futures = {pool.submit(call_with_latency, server, "matrixark_retrieve", payload): payload for payload in retrieve_payloads}
            for future in as_completed(futures):
                payload = futures[future]
                latency, result, error = future.result()
                retrieve_latencies.append(latency)
                if error:
                    retrieve_errors.append(error)
                    continue
                assert result is not None
                selected_counts.append(selected_ref_count(result))
                metrics = retrieval_metrics_from_result(result)
                phase0_fields = retrieval_phase0_fields(result)
                phase0_fields["query_id"] = payload.get("query_id")
                retrieval_metric_rows.append({**metrics, **phase0_fields})
                if result.get("partial_context_pack") or "timeout_partial" in str(result.get("quality_warnings", "")):
                    partial_count += 1
        retrieve_elapsed = time.perf_counter() - retrieve_started
        retrieve_resource_end = process_resource_snapshot()
        feature_probe = run_feature_probe(server, args, scope=scope, node_path=node_path, run_id=f"{run_id}-{backend}")
        stage_metrics = _merge_correctness_evidence(summarize_retrieval_metrics(retrieval_metric_rows), feature_probe)

        metrics_latency_ms, metrics_result, metrics_error = call_with_latency(server, "matrixark_backend_metrics", {})
        backend_resource_end = process_resource_snapshot()
        result = {
            "backend": backend,
            "status": "passed" if not ingest_errors and not retrieve_errors else "failed",
            "storage_prefix": prefix,
            "effective_storage_tuning": effective_storage_tuning,
            "readiness": readiness,
            "raw_storage": raw_storage,
            "ingest": {
                **summarize_latencies(
                    ingest_latencies,
                    total_ops=len(ingest_payloads),
                    elapsed_s=ingest_elapsed,
                    errors=len(ingest_errors),
                ),
                "timeout_count": timeout_count(ingest_errors),
            },
            "ingest_messages": {
                "messages": args.events,
                "messages_per_ingest": args.messages_per_ingest,
                "message_qps": round((args.events - (len(ingest_errors) * args.messages_per_ingest)) / ingest_elapsed, 3)
                if ingest_elapsed > 0
                else 0.0,
                "direct_write_flush": flush_result,
                "resource_usage": process_resource_delta(ingest_resource_start, ingest_resource_end, work_units=args.events),
            },
            "retrieve": {
                **summarize_latencies(
                    retrieve_latencies,
                    total_ops=len(retrieve_payloads),
                    elapsed_s=retrieve_elapsed,
                    errors=len(retrieve_errors),
                ),
                "timeout_count": timeout_count(retrieve_errors),
                "partial_context_packs": partial_count,
                "warmup_queries": len(retrieve_warmup_latencies),
                "warmup_p95_ms": percentile(retrieve_warmup_latencies, 95) if retrieve_warmup_latencies else 0.0,
                "selected_refs_avg": round(statistics.fmean(selected_counts), 3) if selected_counts else 0.0,
                "selected_refs_max": max(selected_counts) if selected_counts else 0,
                "stage_metrics": stage_metrics,
                "feature_probe": feature_probe,
                "resource_usage": process_resource_delta(
                    retrieve_resource_start,
                    retrieve_resource_end,
                    work_units=max(1, len(retrieve_payloads) + len(retrieve_warmup_latencies)),
                ),
            },
            "resource_usage": {
                "overall": process_resource_delta(
                    backend_resource_start,
                    backend_resource_end,
                    work_units=max(1, int(args.raw_ops or 0) + int(args.raw_read_ops or 0) + int(args.events or 0) + int(args.retrieve_queries or 0)),
                ),
            },
            "summary_refresh": {
                "latency_ms": round(refresh_latency_ms, 3),
                "error": refresh_error,
                "result": refresh_result,
            },
            "backend_metrics": {
                "latency_ms": round(metrics_latency_ms, 3),
                "error": metrics_error,
                "result": metrics_result,
            },
            "errors": {
                "ingest": ingest_errors[:10],
                "retrieve": retrieve_errors[:10],
            },
        }
        result = attach_storage_lifecycle_shape(result, effective_storage_tuning)
        result["fallback_flags"] = fallback_flags_from_backend(result)
        return result
    finally:
        server.close()


def run_backend_isolated(backend: str, args: argparse.Namespace, run_id: str, artifact_dir: Path) -> Json:
    output_path = artifact_dir / f"{backend}.json"
    log_path = artifact_dir / f"{backend}.worker.log"
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--backend-worker",
        backend,
        "--backend-worker-output",
        str(output_path),
        "--run-id",
        run_id,
        "--events",
        str(args.events),
        "--raw-ops",
        str(args.raw_ops),
        "--raw-read-ops",
        str(args.raw_read_ops),
        "--raw-batch-size",
        str(args.raw_batch_size),
        "--raw-read-batch-size",
        str(args.raw_read_batch_size),
        "--raw-workers",
        str(args.raw_workers),
        "--messages-per-ingest",
        str(args.messages_per_ingest),
        "--ingest-workers",
        str(args.ingest_workers),
        "--retrieve-queries",
        str(args.retrieve_queries),
        "--retrieve-workers",
        str(args.retrieve_workers),
        "--max-context-tokens",
        str(args.max_context_tokens),
        "--metaserver",
        args.metaserver,
        "--namespace",
        args.namespace,
        "--table",
        args.table,
        "--storage-prefix",
        args.storage_prefix,
        "--cpp-lib",
        args.cpp_lib,
        "--rust-cli",
        args.rust_cli,
        "--python-ref-store",
        args.python_ref_store,
        "--request-timeout-ms",
        str(args.request_timeout_ms),
        "--io-timeout-ms",
        str(args.io_timeout_ms),
        "--readiness-timeout-ms",
        str(args.readiness_timeout_ms),
        "--ingest-deadline-ms",
        str(args.ingest_deadline_ms),
        "--retrieve-deadline-ms",
        str(args.retrieve_deadline_ms),
        "--phase0-min-selected-refs",
        str(args.phase0_min_selected_refs),
        "--phase0-max-selected-ref-drift-ratio",
        str(args.phase0_max_selected_ref_drift_ratio),
    ]
    if args.allow_rust_record_log_compat:
        cmd.append("--allow-rust-record-log-compat")
    if args.allow_rust_debug_cli:
        cmd.append("--allow-rust-debug-cli")
    if args.skip_context_pipeline:
        cmd.append("--skip-context-pipeline")
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(1, args.backend_worker_timeout_sec),
        )
        worker_log = {
            "command": cmd,
            "returncode": completed.returncode,
            "elapsed_s": round(time.perf_counter() - started, 3),
            "stdout_tail": completed.stdout[-8000:],
            "stderr_tail": completed.stderr[-8000:],
        }
    except subprocess.TimeoutExpired as exc:
        worker_log = {
            "command": cmd,
            "returncode": None,
            "elapsed_s": round(time.perf_counter() - started, 3),
            "timed_out": True,
            "stdout_tail": (exc.stdout or "")[-8000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-8000:] if isinstance(exc.stderr, str) else "",
        }
    log_path.write_text(json.dumps(worker_log, indent=2, sort_keys=True), encoding="utf-8")
    if output_path.exists():
        try:
            result = json.loads(output_path.read_text(encoding="utf-8"))
            if worker_log.get("returncode") not in (0, None) and result.get("status") == "passed":
                result["status"] = "backend_process_failed"
            result["worker"] = worker_log
            result = attach_storage_lifecycle_shape(result)
            output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
            return result
        except Exception as exc:
            return attach_storage_lifecycle_shape({
                "backend": backend,
                "status": "backend_artifact_read_failed",
                "error": str(exc),
                "worker": worker_log,
                "retrieve": {"stage_metrics": summarize_retrieval_metrics([])},
            })
    status = "blocked_timeout" if worker_log.get("timed_out") else "backend_process_failed"
    return attach_storage_lifecycle_shape({
        "backend": backend,
        "status": status,
        "error": "backend worker did not write result artifact",
        "worker": worker_log,
        "retrieve": {"stage_metrics": summarize_retrieval_metrics([])},
        "fallback_flags": {"backend_startup_failed": True},
    })


def comparison(cpp: Json | None, rust: Json | None, args: argparse.Namespace | None = None) -> Json:
    python_ref = getattr(args, "python_ref_result", None) if args is not None else None
    phase0_backends = {"cpp": cpp, "rust": rust}
    if isinstance(python_ref, dict):
        phase0_backends["python_ref"] = python_ref
    skip_context_pipeline = bool(getattr(args, "skip_context_pipeline", False) if args is not None else False)
    phase0 = (
        {"status": "skipped", "reason": "context pipeline disabled"}
        if skip_context_pipeline
        else phase0_correctness_gate(phase0_backends, args)
    )
    if not cpp or not rust or cpp.get("status") != "passed" or rust.get("status") != "passed":
        feature_parity_passed = phase0.get("status") == "passed"
        return {
            "status": "not_comparable",
            "reason": "both C++ and Rust backends must pass",
            "status_labels": {
                "feature_correct": feature_parity_passed,
                "performance_candidate": False,
                "production_performance_parity": False,
            },
            "rust_vs_cpp_parity": {
                "feature_parity": {
                    "status": "passed" if feature_parity_passed else str(phase0.get("status") or "unknown"),
                    "passed": feature_parity_passed,
                    "source": "phase0_correctness",
                    "criteria": SHARED_CORRECTNESS_REQUIREMENTS,
                },
                "performance_parity": {
                    "status": "not_comparable",
                    "passed": False,
                    "reason": "both C++ and Rust backends must pass before performance parity is evaluated",
                },
            },
            "phase0_correctness": phase0,
        }
    min_qps_ratio = float(getattr(args, "perf_min_qps_ratio", 0.8) if args is not None else 0.8)
    max_latency_ratio = float(getattr(args, "perf_max_latency_ratio", 2.0) if args is not None else 2.0)
    rows = []
    metrics = [
        ("raw_write_record_qps", ("raw_storage", "write", "record_qps"), "higher"),
        ("raw_write_p95_ms", ("raw_storage", "write", "p95_ms"), "lower"),
        ("raw_read_qps", ("raw_storage", "read", "qps"), "higher"),
        ("raw_read_p95_ms", ("raw_storage", "read", "p95_ms"), "lower"),
        ("raw_storage_cpu_ms_per_op", ("raw_storage", "resource_usage", "cpu_ms_per_unit"), "lower"),
        ("raw_storage_current_rss_mb", ("raw_storage", "resource_usage", "current_rss_mb"), "lower"),
        ("message_qps", ("ingest_messages", "message_qps"), "higher"),
        ("ingest_cpu_ms_per_message", ("ingest_messages", "resource_usage", "cpu_ms_per_unit"), "lower"),
        ("ingest_cpu_time_ms", ("ingest_messages", "resource_usage", "cpu_time_ms"), "lower"),
        ("ingest_current_rss_mb", ("ingest_messages", "resource_usage", "current_rss_mb"), "lower"),
        ("ingest_p50_ms", ("ingest", "p50_ms"), "lower"),
        ("ingest_p95_ms", ("ingest", "p95_ms"), "lower"),
        ("ingest_p99_ms", ("ingest", "p99_ms"), "lower"),
        ("ingest_timeout_count", ("ingest", "timeout_count"), "lower"),
        ("retrieve_qps", ("retrieve", "qps"), "higher"),
        ("retrieve_cpu_ms_per_query", ("retrieve", "resource_usage", "cpu_ms_per_unit"), "lower"),
        ("retrieve_cpu_time_ms", ("retrieve", "resource_usage", "cpu_time_ms"), "lower"),
        ("retrieve_current_rss_mb", ("retrieve", "resource_usage", "current_rss_mb"), "lower"),
        ("retrieve_p50_ms", ("retrieve", "p50_ms"), "lower"),
        ("retrieve_p95_ms", ("retrieve", "p95_ms"), "lower"),
        ("retrieve_p99_ms", ("retrieve", "p99_ms"), "lower"),
        ("retrieve_timeout_count", ("retrieve", "timeout_count"), "lower"),
        ("partial_context_packs", ("retrieve", "partial_context_packs"), "lower"),
        ("selected_refs_avg", ("retrieve", "selected_refs_avg"), "approx"),
        ("dropped_refs_avg", ("retrieve", "stage_metrics", "dropped_refs_avg"), "lower"),
        ("index_hits_avg", ("retrieve", "stage_metrics", "index_hits_avg"), "approx"),
        ("candidate_count_avg", ("retrieve", "stage_metrics", "candidate_count_avg"), "lower"),
        ("token_count_avg", ("retrieve", "stage_metrics", "token_count_avg"), "approx"),
        ("query_plan_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "query_plan_ms"), "lower"),
        ("node_traversal_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "node_traversal_ms"), "lower"),
        ("index_prefilter_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "index_prefilter_ms"), "lower"),
        ("candidate_fetch_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "candidate_fetch_ms"), "lower"),
        ("score_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "score_ms"), "lower"),
        ("pack_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "pack_ms"), "lower"),
        ("audit_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "audit_ms"), "lower"),
        ("append_queue_wait_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "append_queue_wait_ms"), "lower"),
        ("append_engine_p95_ms", ("retrieve", "stage_metrics", "stage_p95_ms", "append_engine_ms"), "lower"),
        ("native_timeout_count", ("retrieve", "stage_metrics", "timeout_count"), "lower"),
        ("scanned_records_avg", ("retrieve", "stage_metrics", "scanned_records_avg"), "lower"),
        ("cache_hit_rate", ("retrieve", "stage_metrics", "cache_hit_rate"), "higher"),
        ("index_postings_read_avg", ("retrieve", "stage_metrics", "index_postings_read_avg"), "approx"),
        ("index_postings_touched_avg", ("retrieve", "stage_metrics", "index_postings_touched_avg"), "approx"),
        ("broad_scan_used_count", ("retrieve", "stage_metrics", "broad_scan_used_count"), "lower"),
        ("broad_scan_blocked_count", ("retrieve", "stage_metrics", "broad_scan_blocked_count"), "lower"),
        ("native_pack_assembly_count", ("retrieve", "stage_metrics", "native_pack_assembly_count"), "higher"),
        ("python_pack_fallback_count", ("retrieve", "stage_metrics", "python_pack_fallback_count"), "lower"),
        ("raw_candidate_tables_returned_count", ("retrieve", "stage_metrics", "raw_candidate_tables_returned_count"), "lower"),
        ("placement_partitions_touched_avg", ("retrieve", "stage_metrics", "placement_partitions_touched_avg"), "approx"),
        ("memory_fallback", ("fallback_flags", "memory_fallback"), "lower"),
        ("hash_embedding_fallback", ("fallback_flags", "hash_embedding_fallback"), "lower"),
        ("partial_pack_fallback", ("fallback_flags", "partial_context_pack"), "lower"),
        ("native_metrics_missing", ("fallback_flags", "native_metrics_missing"), "lower"),
        ("overall_cpu_ms_per_unit", ("resource_usage", "overall", "cpu_ms_per_unit"), "lower"),
        ("overall_cpu_time_ms", ("resource_usage", "overall", "cpu_time_ms"), "lower"),
        ("overall_current_rss_mb", ("resource_usage", "overall", "current_rss_mb"), "lower"),
        ("overall_max_rss_mb", ("resource_usage", "overall", "max_rss_mb"), "lower"),
    ]
    for name, path, direction in metrics:
        cpp_value: Any = cpp
        rust_value: Any = rust
        for key in path:
            cpp_value = cpp_value.get(key, 0) if isinstance(cpp_value, dict) else 0
            rust_value = rust_value.get(key, 0) if isinstance(rust_value, dict) else 0
        if isinstance(cpp_value, bool):
            cpp_value = int(cpp_value)
        if isinstance(rust_value, bool):
            rust_value = int(rust_value)
        delta = float(rust_value or 0) - float(cpp_value or 0)
        percent_delta = (delta / float(cpp_value) * 100.0) if cpp_value else 0.0
        rust_float = float(rust_value or 0)
        cpp_float = float(cpp_value or 0)
        ratio = (rust_float / cpp_float) if cpp_float else (1.0 if rust_float == 0 else float("inf"))
        if direction == "higher":
            passed = cpp_float == 0 or rust_float >= cpp_float * min_qps_ratio
            threshold = round(cpp_float * min_qps_ratio, 6)
            threshold_label = f">= {threshold}"
        elif direction == "lower":
            passed = cpp_float == 0 or rust_float <= cpp_float * max_latency_ratio
            threshold = round(cpp_float * max_latency_ratio, 6)
            threshold_label = f"<= {threshold}"
        elif direction == "approx":
            allowed_delta = max(1.0, abs(cpp_float) * 0.25)
            passed = abs(delta) <= allowed_delta
            threshold_label = f"abs(delta) <= {round(allowed_delta, 6)}"
        else:
            passed = True
            threshold_label = "informational"
        if name == "selected_refs_avg":
            min_selected_refs = float(phase0.get("minimum_selected_refs") or 1)
            enough_refs = cpp_float >= min_selected_refs and rust_float >= min_selected_refs
            passed = bool(passed and enough_refs)
            threshold_label = f">= {min_selected_refs:g} on both backends and {threshold_label}"
        rows.append(
            {
                "metric": name,
                "cpp": cpp_value,
                "rust": rust_value,
                "rust_minus_cpp": round(delta, 3),
                "percent_delta": round(percent_delta, 3),
                "rust_to_cpp_ratio": round(ratio, 6) if ratio != float("inf") else "inf",
                "direction": direction,
                "parity_threshold": threshold_label,
                "parity_passed": passed,
            }
        )
    blockers = [row for row in rows if not row.get("parity_passed")]
    phase0_failed = phase0.get("status") == "failed"
    feature_parity_passed = phase0.get("status") == "passed"
    feature_correct = not phase0_failed
    performance_candidate = bool(
        cpp
        and rust
        and cpp.get("status") == "passed"
        and rust.get("status") == "passed"
    )
    performance_parity_passed = bool(performance_candidate and not blockers)
    production_performance_parity = bool(feature_parity_passed and performance_parity_passed)
    return {
        "status": "failed" if phase0_failed or blockers else "passed",
        "status_labels": {
            "feature_correct": feature_correct,
            "performance_candidate": performance_candidate,
            "production_performance_parity": production_performance_parity,
        },
        "rust_vs_cpp_parity": {
            "feature_parity": {
                "status": "passed" if feature_parity_passed else str(phase0.get("status") or "unknown"),
                "passed": feature_parity_passed,
                "source": "phase0_correctness",
                "criteria": SHARED_CORRECTNESS_REQUIREMENTS,
                "failures": phase0.get("failures", []),
            },
            "performance_parity": {
                "status": "passed" if performance_parity_passed else "failed",
                "passed": performance_parity_passed,
                "min_qps_ratio": min_qps_ratio,
                "max_latency_ratio": max_latency_ratio,
                "blockers": blockers,
                "required_same_config": [
                    "dataset",
                    "storage_mode",
                    "topology",
                    "token_budget",
                    "batch_size",
                    "embedding_model",
                    "reader_model",
                    "judge_model",
                ],
            },
            "production_performance_parity": {
                "status": "passed" if production_performance_parity else "failed",
                "passed": production_performance_parity,
                "definition": (
                    "feature parity passed and live same-config performance parity "
                    "metrics are within configured thresholds"
                ),
            },
        },
        "phase0_correctness": phase0,
        "rows": rows,
        "perf_parity": {
            "passed": not blockers and not phase0_failed,
            "min_qps_ratio": min_qps_ratio,
            "max_latency_ratio": max_latency_ratio,
            "blockers": blockers,
            "correctness_failures": phase0.get("failures", []),
        },
    }


def _completion_checks(required: list[Any], completed: set[Any]) -> list[Json]:
    return [{"case": item, "status": "passed" if item in completed else "open"} for item in required]


def phase_scale_matrix_gate(report: Json, args: argparse.Namespace) -> Json:
    required_events = _parse_int_csv(getattr(args, "phase_scale_events", ""), DEFAULT_PHASE_SCALE_EVENTS)
    required_workers = _parse_int_csv(getattr(args, "phase_retrieve_workers", ""), DEFAULT_PHASE_RETRIEVE_WORKERS)
    required_imports = _parse_str_csv(getattr(args, "phase_resource_imports", ""), DEFAULT_PHASE_RESOURCE_IMPORTS)
    required_features = _parse_str_csv(
        getattr(args, "phase_contextmemory_features", ""),
        DEFAULT_PHASE_CONTEXTMEMORY_FEATURES,
    )

    completed_events = set(_parse_int_csv(getattr(args, "completed_scale_events", ""), []))
    completed_workers = set(_parse_int_csv(getattr(args, "completed_retrieve_workers", ""), []))
    completed_imports = set(_parse_str_csv(getattr(args, "completed_resource_imports", ""), []))
    completed_features = set(_parse_str_csv(getattr(args, "completed_contextmemory_features", ""), []))

    try:
        completed_events.add(int(getattr(args, "events", 0) or 0))
    except (TypeError, ValueError):
        pass
    try:
        completed_workers.add(int(getattr(args, "retrieve_workers", 0) or 0))
    except (TypeError, ValueError):
        pass
    if not bool(getattr(args, "skip_context_pipeline", False)):
        completed_features.update({"cross_session_retrieval", "compact_indexes", "audit_light_telemetry"})

    checks = {
        "event_ingestion": _completion_checks(required_events, completed_events),
        "retrieve_workers": _completion_checks(required_workers, completed_workers),
        "resource_imports": _completion_checks(required_imports, completed_imports),
        "contextmemory_pipeline": _completion_checks(required_features, completed_features),
    }
    full_contextmemory_pipeline_passed = all(
        row.get("status") == "passed"
        for group in ("resource_imports", "contextmemory_pipeline")
        for row in checks.get(group, [])
    )
    open_required_cases: list[Json] = []
    for group, rows in checks.items():
        for row in rows:
            if row.get("status") != "passed":
                open_required_cases.append({"group": group, "case": row.get("case")})
    require_gate = bool(getattr(args, "require_phase_scale_matrix", False))
    status = "passed" if not open_required_cases else ("failed" if require_gate else "incomplete")
    return {
        "phase": getattr(args, "phase_name", "current") or "current",
        "status": status,
        "require_gate": require_gate,
        "checks": checks,
        "full_contextmemory_pipeline": {
            "status": "passed" if full_contextmemory_pipeline_passed else "incomplete",
            "required_resource_imports": required_imports,
            "required_features": required_features,
            "description": (
                "large PDF/CSV/repo resource imports plus resources, skills, "
                "cross-session retrieval, compact indexes, and audit-light telemetry"
            ),
        },
        "open_required_cases": open_required_cases,
        "evidence": {
            "current_run": {
                "events": report.get("config", {}).get("events"),
                "retrieve_workers": report.get("config", {}).get("retrieve_workers"),
                "skip_context_pipeline": report.get("config", {}).get("skip_context_pipeline"),
            },
            "completed_events": sorted(completed_events),
            "completed_retrieve_workers": sorted(completed_workers),
            "completed_resource_imports": sorted(completed_imports),
            "completed_contextmemory_features": sorted(completed_features),
        },
    }


def _backend_stage_metrics(report: Json, backend: str) -> Json:
    metrics = (
        report.get("backends", {})
        .get(backend, {})
        .get("retrieve", {})
        .get("stage_metrics", {})
    )
    return metrics if isinstance(metrics, dict) else {}


def production_policy_gate(report: Json) -> Json:
    """Report the non-negotiable production parity rules beside perf metrics."""
    comparison_report = report.get("comparison", {}) if isinstance(report.get("comparison"), dict) else {}
    phase0 = (
        comparison_report.get("phase0_correctness", {})
        if isinstance(comparison_report.get("phase0_correctness"), dict)
        else {}
    )
    config = report.get("config", {}) if isinstance(report.get("config"), dict) else {}
    checks: list[Json] = []

    def add_check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "status": "passed" if passed else "failed", "detail": detail})

    feature_correct = phase0.get("status") == "passed"
    add_check(
        "correctness_before_latency",
        feature_correct,
        "Selected refs must be non-empty and logically equivalent across C++/Rust/Python before latency is considered.",
    )

    for backend in ("cpp", "rust"):
        metrics = _backend_stage_metrics(report, backend)
        backend_status = report.get("backends", {}).get(backend, {}).get("status")
        selected_max = float(metrics.get("selected_refs_max") or metrics.get("selected_refs_avg") or 0)
        add_check(
            f"{backend}_selected_refs_non_empty",
            backend_status == "passed" and selected_max > 0,
            f"{backend} status={backend_status}; selected_refs_max={selected_max}",
        )
        broad_scan_count = int(metrics.get("broad_scan_used_count") or 0)
        add_check(
            f"{backend}_placement_index_driven",
            backend_status != "passed" or broad_scan_count == 0,
            f"{backend} broad_scan_used_count={broad_scan_count}; broad scan is fallback/debug only.",
        )
        python_pack_count = int(metrics.get("python_pack_fallback_count") or 0)
        raw_candidate_count = int(metrics.get("raw_candidate_tables_returned_count") or 0)
        add_check(
            f"{backend}_native_pack_or_dispatcher_only",
            backend_status != "passed" or (python_pack_count == 0 and raw_candidate_count == 0),
            (
                f"{backend} python_pack_fallback_count={python_pack_count}, "
                f"raw_candidate_tables_returned_count={raw_candidate_count}."
            ),
        )
        audit_p95 = float((metrics.get("stage_p95_ms") or {}).get("audit_ms") or 0)
        add_check(
            f"{backend}_audit_not_hot_path_blocking",
            backend_status != "passed" or audit_p95 <= 5.0,
            f"{backend} audit_p95_ms={audit_p95}; rich audit/debug must be async/sampled by default.",
        )

    same_config_fields = [
        "dataset",
        "storage_options",
        "topology",
        "max_context_tokens",
        "batch_size",
        "embedding_provider",
        "embedding_model",
        "reader_provider",
        "reader_model",
        "judge_provider",
        "judge_model",
        "effective_storage_tuning",
    ]
    add_check(
        "same_dataset_storage_topology_budget_batch_models",
        all(field in config for field in same_config_fields),
        (
            "Performance parity requires the same dataset, storage mode, topology, token budget, "
            "batch size, embedding model, reader, judge, and effective storage tuning for C++ and Rust."
        ),
    )
    tuning_failures = storage_tuning_failures(report)
    add_check(
        "same_effective_storage_tuning",
        not tuning_failures,
        (
            "C++ and Rust passed backends must report the same effective TS_* storage tuning as the run config. "
            + ("; ".join(tuning_failures) if tuning_failures else "all required knobs match")
        ),
    )

    blockers = [check for check in checks if check.get("status") != "passed"]
    return {
        "status": "passed" if not blockers else "failed",
        "checks": checks,
        "blockers": blockers,
        "policy": [
            "Correctness beats latency: do not tune C++ performance until selected refs are non-empty and logically equivalent to Rust/Python.",
            "Python remains API/auth/model orchestration only; serving-critical scan/filter/pack/write work belongs in C++/Rust.",
            "Normal retrieval is placement-key and compact-index driven; broad scan is fallback/debug only.",
            "Audit/debug records do not block hot retrieval by default.",
            "Performance parity uses the same dataset, storage mode, topology, token budget, batch size, embedding model, reader, judge, and effective storage tuning for C++ and Rust.",
        ],
    }


def write_report(path: Path, report: Json) -> None:
    backend_order = [backend for backend in ("cpp", "rust", "python_ref") if backend in report.get("backends", {})]
    lines = [
        "# MatrixArk C++ vs Rust Scale Report",
        "",
        f"- run_id: `{report['run_id']}`",
        f"- generated_at_ms: `{report['generated_at_ms']}`",
        f"- events: `{report['config']['events']}`",
        f"- messages_per_ingest: `{report['config']['messages_per_ingest']}`",
        f"- ingest_workers: `{report['config']['ingest_workers']}`",
        f"- retrieve_workers: `{report['config']['retrieve_workers']}`",
        f"- retrieve_queries: `{report['config']['retrieve_queries']}`",
        "",
        "## Results",
        "",
        "| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for backend in backend_order:
        item = report["backends"].get(backend, {})
        ingest = item.get("ingest", {})
        ingest_messages = item.get("ingest_messages", {})
        retrieve = item.get("retrieve", {})
        errors = int(ingest.get("errors") or 0) + int(retrieve.get("errors") or 0)
        lines.append(
            f"| {backend} | {item.get('status')} | {ingest_messages.get('message_qps', 0)} | "
            f"{ingest.get('p50_ms', 0)} ms | {ingest.get('p95_ms', 0)} ms | {ingest.get('p99_ms', 0)} ms | "
            f"{retrieve.get('qps', 0)} | {retrieve.get('p50_ms', 0)} ms | {retrieve.get('p95_ms', 0)} ms | "
            f"{retrieve.get('p99_ms', 0)} ms | {errors} | {retrieve.get('partial_context_packs', 0)} |"
        )
    lines.extend(
        [
            "",
            "## Effective Storage Tuning",
            "",
            "| backend | context page target | block segment target | storage zone | stream max blob | compaction watermark | cold scan no-cache | page index cache | block index cache | effective block segment |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    config_tuning = report.get("config", {}).get("effective_storage_tuning", {})
    for backend in backend_order:
        item = report["backends"].get(backend, {})
        tuning = item.get("effective_storage_tuning") if isinstance(item.get("effective_storage_tuning"), dict) else config_tuning
        if not isinstance(tuning, dict):
            tuning = {}
        lines.append(
            f"| {backend} | "
            f"{tuning.get('TS_CONTEXT_PAGE_TARGET_BYTES', '')} | "
            f"{tuning.get('TS_BLOCK_SEGMENT_TARGET_BYTES', '')} | "
            f"{tuning.get('TS_STORAGE_ZONE_SIZE', '')} | "
            f"{tuning.get('TS_STREAM_MAX_BLOB_SIZE', '')} | "
            f"{tuning.get('TS_COMPACTION_WATERMARK_BYTES', '')} | "
            f"{tuning.get('TS_COLD_SCAN_NO_CACHE_FILL', '')} | "
            f"{tuning.get('TS_PAGE_INDEX_CACHE_BYTES', '')} | "
            f"{tuning.get('TS_BLOCK_INDEX_CACHE_BYTES', '')} | "
            f"{tuning.get('effective_block_segment_target_bytes', '')} |"
        )
    lines.extend(
        [
            "",
            "## Required Page/Block Metrics",
            "",
            "Both C++ and Rust backends must expose these metric names before storage performance parity claims are accepted:",
            "",
        ]
    )
    lines.extend(f"- `{name}`" for name in PAGE_BLOCK_METRIC_NAMES)
    lines.extend(
        [
            "",
            "## Required Storage Read Sequence",
            "",
            "Normal reads must report this canonical sequence before storage read-path parity claims are accepted:",
            "",
        ]
    )
    lines.extend(f"{index}. `{name}`" for index, name in enumerate(STORAGE_READ_SEQUENCE_STEPS, start=1))
    lines.extend(
        [
            "",
            "## Required Storage Cold Scan Sequence",
            "",
            "Cold scans must report this canonical no-promote sequence before cold lifecycle parity claims are accepted:",
            "",
        ]
    )
    lines.extend(f"{index}. `{name}`" for index, name in enumerate(STORAGE_COLD_SCAN_SEQUENCE_STEPS, start=1))
    lines.extend(
        [
            "",
            "## Required Storage Lifecycle Phases",
            "",
            "StorageManager/StoreManager reports must cover these phases before stream/zone/eviction/GC/reclaim parity claims are accepted:",
            "",
        ]
    )
    lines.extend(f"- `{name}`" for name in STORAGE_LIFECYCLE_PHASE_NAMES)
    lines.extend(
        [
            "",
            "## Required Storage Reclaim Semantics",
            "",
            "Physical reclaim parity requires these semantics; cache eviction alone is memory-only:",
            "",
        ]
    )
    lines.extend(f"- `{name}`" for name in STORAGE_RECLAIM_SEMANTICS)
    lines.extend(
        [
            "",
            "## Required Multi-Layer Cache Contract",
            "",
            "Both engines must report these cache layers and semantics before cache parity claims are accepted:",
            "",
            "### Layers",
            "",
        ]
    )
    lines.extend(f"- `{name}`" for name in STORAGE_CACHE_LAYER_NAMES)
    lines.extend(["", "### Semantics", ""])
    lines.extend(f"- `{name}`" for name in STORAGE_CACHE_SEMANTICS)
    lines.extend(["", "### Metrics", ""])
    lines.extend(f"- `{name}`" for name in STORAGE_CACHE_METRIC_NAMES)
    lines.extend(
        [
            "",
            "## Required Storage Lifecycle Metrics",
            "",
            "Both C++ and Rust backends must expose these lifecycle metric names before stream/zone/eviction/GC/reclaim parity claims are accepted:",
            "",
        ]
    )
    lines.extend(f"- `{name}`" for name in STORAGE_LIFECYCLE_METRIC_NAMES)
    lines.extend(["", "## Raw Storage", "", "| backend | write record QPS | write batch p95 | read QPS | read p95 | write errors | read errors |", "|---|---:|---:|---:|---:|---:|---:|"])
    for backend in backend_order:
        item = report["backends"].get(backend, {})
        raw = item.get("raw_storage", {})
        write = raw.get("write", {})
        read = raw.get("read", {})
        errors = raw.get("errors", {})
        lines.append(
            f"| {backend} | {write.get('record_qps', 0)} | {write.get('p95_ms', 0)} ms | "
            f"{read.get('qps', 0)} | {read.get('p95_ms', 0)} ms | "
            f"{len(errors.get('write', [])) if isinstance(errors, dict) else 0} | {len(errors.get('read', [])) if isinstance(errors, dict) else 0} |"
        )
    lines.extend(
        [
            "",
            "## Retrieval Stage Metrics",
            "",
            "| backend | samples | query plan p95 | node traversal p95 | index prefilter p95 | candidate fetch p95 | score p95 | pack p95 | audit p95 | append queue wait p95 | append engine p95 | selected avg | dropped avg | scanned avg | index hits avg | index postings read avg | candidates avg | tokens avg | native timeouts | fallback flags | broad scan used | python pack fallback | native pack | cache hit rate | placement partitions avg |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for backend in backend_order:
        retrieve = report["backends"].get(backend, {}).get("retrieve", {})
        metrics = retrieve.get("stage_metrics", {})
        p95 = metrics.get("stage_p95_ms", {})
        lines.append(
            f"| {backend} | {metrics.get('samples', 0)} | "
            f"{p95.get('query_plan_ms', 0)} ms | {p95.get('node_traversal_ms', 0)} ms | "
            f"{p95.get('index_prefilter_ms', 0)} ms | {p95.get('candidate_fetch_ms', 0)} ms | "
            f"{p95.get('score_ms', 0)} ms | {p95.get('pack_ms', 0)} ms | {p95.get('audit_ms', 0)} ms | "
            f"{p95.get('append_queue_wait_ms', 0)} ms | {p95.get('append_engine_ms', 0)} ms | "
            f"{metrics.get('selected_refs_avg', 0)} | {metrics.get('dropped_refs_avg', 0)} | "
            f"{metrics.get('scanned_records_avg', 0)} | {metrics.get('index_hits_avg', 0)} | "
            f"{metrics.get('index_postings_read_avg', metrics.get('index_postings_touched_avg', 0))} | "
            f"{metrics.get('candidate_count_avg', 0)} | {metrics.get('token_count_avg', 0)} | "
            f"{metrics.get('timeout_count', 0)} | "
            f"{', '.join(sorted(str(k) for k in metrics.get('fallback_flags_total', {}).keys())) if isinstance(metrics.get('fallback_flags_total'), dict) else ''} | "
            f"{metrics.get('broad_scan_used_count', 0)} | {metrics.get('python_pack_fallback_count', 0)} | "
            f"{metrics.get('native_pack_assembly_count', 0)} | "
            f"{metrics.get('cache_hit_rate', 0)} | "
            f"{metrics.get('placement_partitions_touched_avg', 0)} |"
        )
    comp = report.get("comparison", {})
    if isinstance(comp.get("status_labels"), dict):
        labels = comp.get("status_labels", {})
        lines.extend(
            [
                "",
                "## Status Labels",
                "",
                f"- feature_correct: `{bool(labels.get('feature_correct'))}`",
                f"- performance_candidate: `{bool(labels.get('performance_candidate'))}`",
                f"- production_performance_parity: `{bool(labels.get('production_performance_parity'))}`",
            ]
        )
    rust_vs_cpp = comp.get("rust_vs_cpp_parity", {}) if isinstance(comp.get("rust_vs_cpp_parity"), dict) else {}
    if rust_vs_cpp:
        feature = rust_vs_cpp.get("feature_parity", {}) if isinstance(rust_vs_cpp.get("feature_parity"), dict) else {}
        performance = (
            rust_vs_cpp.get("performance_parity", {})
            if isinstance(rust_vs_cpp.get("performance_parity"), dict)
            else {}
        )
        production = (
            rust_vs_cpp.get("production_performance_parity", {})
            if isinstance(rust_vs_cpp.get("production_performance_parity"), dict)
            else {}
        )
        lines.extend(
            [
                "",
                "## Rust Vs C++ Parity",
                "",
                f"- feature parity: `{feature.get('status', 'unknown')}`",
                f"- performance parity: `{performance.get('status', 'unknown')}`",
                f"- production performance parity: `{production.get('status', 'unknown')}`",
                f"- min Rust/C++ QPS ratio: `{performance.get('min_qps_ratio', '')}`",
                f"- max Rust/C++ latency ratio: `{performance.get('max_latency_ratio', '')}`",
                f"- performance blockers: `{len(performance.get('blockers', [])) if isinstance(performance.get('blockers'), list) else 0}`",
            ]
        )
    phase_scale = report.get("phase_scale_matrix", {})
    if isinstance(phase_scale, dict):
        lines.extend(
            [
                "",
                "## Post-Phase Scale Matrix",
                "",
                f"- status: `{phase_scale.get('status')}`",
                f"- phase: `{phase_scale.get('phase')}`",
                f"- require gate: `{bool(phase_scale.get('require_gate'))}`",
                f"- open required cases: `{len(phase_scale.get('open_required_cases', []))}`",
                f"- full ContextMemory pipeline: `{(phase_scale.get('full_contextmemory_pipeline') or {}).get('status', 'unknown') if isinstance(phase_scale.get('full_contextmemory_pipeline'), dict) else 'unknown'}`",
                "",
                "| group | case | status |",
                "|---|---|---|",
            ]
        )
        checks = phase_scale.get("checks", {}) if isinstance(phase_scale.get("checks"), dict) else {}
        for group, rows in checks.items():
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict):
                    lines.append(f"| {group} | {row.get('case')} | {row.get('status')} |")
    policy = report.get("production_policy", {})
    if isinstance(policy, dict):
        lines.extend(
            [
                "",
                "## Production Parity Policy Gate",
                "",
                f"- status: `{policy.get('status')}`",
                f"- blockers: `{len(policy.get('blockers', []))}`",
                "",
                "| rule | status | detail |",
                "|---|---|---|",
            ]
        )
        for check in policy.get("checks", []):
            if isinstance(check, dict):
                lines.append(f"| {check.get('name')} | {check.get('status')} | {check.get('detail')} |")
        lines.extend(["", "### Policy", ""])
        for item in policy.get("policy", []):
            lines.append(f"- {item}")
    if comp.get("status") in {"passed", "failed"}:
        phase0 = comp.get("phase0_correctness", {})
        lines.extend(
            [
                "",
                "## Phase 1 Native Retrieve Correctness Gate",
                "",
                f"- status: `{phase0.get('status')}`",
                f"- phase: `{phase0.get('phase')}`",
                f"- shared requirements: `{', '.join(phase0.get('shared_correctness_requirements', SHARED_CORRECTNESS_REQUIREMENTS))}`",
                f"- minimum selected refs: `{phase0.get('minimum_selected_refs')}`",
                f"- max selected-ref drift ratio: `{phase0.get('max_selected_ref_drift_ratio')}`",
                f"- selected-ref drift ratio: `{phase0.get('selected_ref_drift_ratio')}`",
            ]
        )
        backend_values = phase0.get("backend_values", {}) if isinstance(phase0.get("backend_values"), dict) else {}
        if backend_values:
            lines.extend(
                [
                    "",
                    "| backend | status | selected avg | selected max | dropped avg | scanned avg | index hits avg | index postings read avg | candidates avg | tokens avg | broad scan used | python pack fallback | native pack | timeouts | drop counters |",
                    "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
                ]
            )
            for backend in backend_values:
                values = backend_values.get(backend, {}) if isinstance(backend_values.get(backend), dict) else {}
                drop_counters = values.get("drop_counters_total", {}) if isinstance(values.get("drop_counters_total"), dict) else {}
                lines.append(
                    f"| {backend} | {values.get('status', '')} | {values.get('selected_refs_avg', 0)} | {values.get('selected_refs_max', 0)} | "
                    f"{values.get('dropped_refs_avg', 0)} | {values.get('scanned_records_avg', 0)} | "
                    f"{values.get('index_hits_avg', 0)} | {values.get('index_postings_read_avg', values.get('index_postings_touched_avg', 0))} | "
                    f"{values.get('candidate_count_avg', 0)} | {values.get('token_count_avg', 0)} | "
                    f"{values.get('broad_scan_used_count', 0)} | {values.get('python_pack_fallback_count', 0)} | "
                    f"{values.get('native_pack_assembly_count', 0)} | {values.get('timeouts', 0)} | "
                    f"`{json.dumps(drop_counters, sort_keys=True)}` |"
                )
            lines.extend(
                [
                    "",
                    "| backend | selected-ref parity | scope filter | placement filter | compact index | stale/superseded | shared quota | cross-session rerank |",
                    "|---|---|---|---|---|---|---|---|",
                ]
            )
            for backend in backend_values:
                values = backend_values.get(backend, {}) if isinstance(backend_values.get(backend), dict) else {}
                evidence = values.get("correctness_evidence", {}) if isinstance(values.get("correctness_evidence"), dict) else {}
                lines.append(
                    f"| {backend} | {bool(evidence.get('selected_ref_parity'))} | {bool(evidence.get('scope_filtering'))} | "
                    f"{bool(evidence.get('placement_filtering'))} | {bool(evidence.get('compact_secondary_index_prefilter'))} | "
                    f"{bool(evidence.get('stale_superseded_exclusion'))} | {bool(evidence.get('shared_resource_skill_quota'))} | "
                    f"{bool(evidence.get('cross_session_quota_rerank'))} |"
                )
        if phase0.get("failures"):
            lines.extend(["", "| failure | backend | details |", "|---|---|---|"])
            for failure in phase0.get("failures", []):
                lines.append(
                    f"| {failure.get('reason')} | {failure.get('backend')} | `{json.dumps(failure, sort_keys=True)}` |"
                )
        parity = comp.get("perf_parity", {})
        lines.extend(
            [
                "",
                "## Performance Parity Gate",
                "",
                f"- status: `{'passed' if parity.get('passed') else 'failed'}`",
                f"- minimum QPS ratio: `{parity.get('min_qps_ratio')}`",
                f"- maximum latency ratio: `{parity.get('max_latency_ratio')}`",
                f"- blockers: `{len(parity.get('blockers', []))}`",
                f"- correctness failures: `{len(parity.get('correctness_failures', []))}`",
            ]
        )
        if parity.get("blockers"):
            lines.extend(["", "| metric | C++ | Rust | threshold | ratio |", "|---|---:|---:|---:|---:|"])
            for row in parity.get("blockers", []):
                lines.append(
                    f"| {row['metric']} | {row['cpp']} | {row['rust']} | {row['parity_threshold']} | {row['rust_to_cpp_ratio']} |"
                )
        lines.extend(["", "## Rust Minus C++", "", "| metric | C++ | Rust | delta | percent delta |", "|---|---:|---:|---:|---:|"])
        for row in comp.get("rows", []):
            lines.append(
                f"| {row['metric']} | {row['cpp']} | {row['rust']} | {row['rust_minus_cpp']} | {row['percent_delta']}% |"
            )
    else:
        lines.extend(["", "## Comparison", "", f"`{comp.get('status')}`: {comp.get('reason', '')}"])
        phase0 = comp.get("phase0_correctness", {}) if isinstance(comp.get("phase0_correctness"), dict) else {}
        if phase0:
            lines.extend(
                [
                    "",
                    "## Phase 1 Native Retrieve Correctness Gate",
                    "",
                    f"- status: `{phase0.get('status')}`",
                    f"- phase: `{phase0.get('phase')}`",
                    f"- minimum selected refs: `{phase0.get('minimum_selected_refs')}`",
                    f"- max selected-ref drift ratio: `{phase0.get('max_selected_ref_drift_ratio')}`",
                    f"- selected-ref drift ratio: `{phase0.get('selected_ref_drift_ratio')}`",
                    "",
                    "| backend | status | selected avg | selected max | dropped avg | scanned avg | index hits avg | postings avg | candidates avg | tokens avg | broad scan used | python pack fallback | native pack | timeouts | drop counters |",
                    "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
                ]
            )
            backend_values = phase0.get("backend_values", {}) if isinstance(phase0.get("backend_values"), dict) else {}
            for backend in backend_values:
                values = backend_values.get(backend, {}) if isinstance(backend_values.get(backend), dict) else {}
                drop_counters = values.get("drop_counters_total", {}) if isinstance(values.get("drop_counters_total"), dict) else {}
                lines.append(
                    f"| {backend} | {values.get('status', '')} | {values.get('selected_refs_avg', 0)} | "
                    f"{values.get('selected_refs_max', 0)} | {values.get('dropped_refs_avg', 0)} | "
                    f"{values.get('scanned_records_avg', 0)} | {values.get('index_hits_avg', 0)} | "
                    f"{values.get('index_postings_read_avg', values.get('index_postings_touched_avg', 0))} | {values.get('candidate_count_avg', 0)} | "
                    f"{values.get('token_count_avg', 0)} | {values.get('broad_scan_used_count', 0)} | "
                    f"{values.get('python_pack_fallback_count', 0)} | {values.get('native_pack_assembly_count', 0)} | "
                    f"{values.get('timeouts', 0)} | `{json.dumps(drop_counters, sort_keys=True)}` |"
                )
            if phase0.get("failures"):
                lines.extend(["", "| failure | backend | details |", "|---|---|---|"])
                for failure in phase0.get("failures", []):
                    lines.append(
                        f"| {failure.get('reason')} | {failure.get('backend')} | `{json.dumps(failure, sort_keys=True)}` |"
                    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--raw-ops", type=int, default=1000)
    parser.add_argument("--raw-read-ops", type=int, default=500)
    parser.add_argument("--raw-batch-size", type=int, default=50)
    parser.add_argument("--raw-read-batch-size", type=int, default=25)
    parser.add_argument("--raw-workers", type=int, default=4)
    parser.add_argument("--messages-per-ingest", type=int, default=20)
    parser.add_argument("--ingest-workers", type=int, default=4)
    parser.add_argument("--retrieve-queries", type=int, default=128)
    parser.add_argument("--retrieve-workers", type=int, default=16)
    parser.add_argument("--retrieve-warmup-queries", type=int, default=-1)
    parser.add_argument("--max-context-tokens", type=int, default=12000)
    parser.add_argument("--dataset", default=os.environ.get("MATRIXARK_PARITY_DATASET", "matrixark-scale-synthetic"))
    parser.add_argument("--embedding-provider", default=os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "hash"))
    parser.add_argument("--embedding-model", default=os.environ.get("MATRIXARK_EMBEDDING_MODEL", "matrixark-local-token-hash-v1"))
    parser.add_argument("--reader-provider", default=os.environ.get("MATRIXARK_READER_PROVIDER", "deterministic"))
    parser.add_argument("--reader-model", default=os.environ.get("MATRIXARK_READER_MODEL", "matrixark-deterministic-reader"))
    parser.add_argument("--judge-provider", default=os.environ.get("MATRIXARK_JUDGE_PROVIDER", "deterministic"))
    parser.add_argument("--judge-model", default=os.environ.get("MATRIXARK_JUDGE_MODEL", "matrixark-deterministic-judge"))
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument("--storage-family", default=os.environ.get("MATRIXARK_STORAGE_FAMILY", "shared_store"))
    parser.add_argument("--storage-mode", default=os.environ.get("MATRIXARK_STORAGE_MODE", "multi_node"))
    parser.add_argument("--write-mode", default=os.environ.get("MATRIXARK_WRITE_MODE", "async"))
    parser.add_argument("--oplog-mode", default=os.environ.get("MATRIXARK_OPLOG_MODE", "async"))
    parser.add_argument("--replication-mode", default=os.environ.get("MATRIXARK_REPLICATION_MODE", "shared_store"))
    parser.add_argument("--storage-prefix", default="matrixark:scale")
    parser.add_argument("--cpp-lib", default=default_cpp_lib_path())
    parser.add_argument("--rust-cli", default=str(ROOT / "target/release/matrixark_rust_proxy"))
    parser.add_argument("--allow-rust-record-log-compat", action="store_true")
    parser.add_argument("--allow-rust-debug-cli", action="store_true")
    parser.add_argument("--allow-rust-cpp-c-api-bridge", action="store_true", help="diagnostic only: allow the legacy Rust cdylib MatrixArk hot path to call the shared C++ C API bridge")
    parser.add_argument("--request-timeout-ms", type=int, default=60000)
    parser.add_argument("--io-timeout-ms", type=int, default=60000)
    parser.add_argument("--readiness-timeout-ms", type=int, default=60000)
    parser.add_argument("--ingest-deadline-ms", type=int, default=60000)
    parser.add_argument("--retrieve-deadline-ms", type=int, default=10000)
    parser.add_argument("--backends", nargs="+", choices=["cpp", "rust", "python_ref"], default=["cpp", "rust"])
    parser.add_argument("--artifact-dir", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--backend-worker", choices=["cpp", "rust", "python_ref"], default="")
    parser.add_argument("--backend-worker-output", default="")
    parser.add_argument("--backend-worker-timeout-sec", type=int, default=900)
    parser.add_argument("--python-ref-store", default="")
    parser.add_argument("--no-isolate-backends", action="store_true")
    parser.add_argument("--skip-context-pipeline", action="store_true")
    parser.add_argument("--phase0-min-selected-refs", type=int, default=1)
    parser.add_argument("--phase0-max-selected-ref-drift-ratio", type=float, default=0.35)
    parser.add_argument("--allow-phase0-correctness-failure", action="store_true")
    parser.add_argument("--perf-min-qps-ratio", type=float, default=0.8)
    parser.add_argument("--perf-max-latency-ratio", type=float, default=2.0)
    parser.add_argument("--require-perf-parity", action="store_true")
    parser.add_argument("--phase-name", default="current")
    parser.add_argument("--phase-scale-events", default="1000,10000,100000")
    parser.add_argument("--phase-retrieve-workers", default="4,8,16,32")
    parser.add_argument("--phase-resource-imports", default="large_pdf,large_csv,repo_directory")
    parser.add_argument(
        "--phase-contextmemory-features",
        default="resources,skills,cross_session_retrieval,compact_indexes,audit_light_telemetry",
    )
    parser.add_argument("--completed-scale-events", default="")
    parser.add_argument("--completed-retrieve-workers", default="")
    parser.add_argument("--completed-resource-imports", default="")
    parser.add_argument("--completed-contextmemory-features", default="")
    parser.add_argument("--require-phase-scale-matrix", action="store_true")
    parsed = parser.parse_args()

    parsed.storage_options = {
        "storage_family": parsed.storage_family,
        "storage_mode": parsed.storage_mode,
        "write_mode": parsed.write_mode,
        "oplog_mode": parsed.oplog_mode,
        "replication_mode": parsed.replication_mode,
    }
    run_id = parsed.run_id or str(int(time.time() * 1000))
    if parsed.backend_worker:
        try:
            result = run_backend(parsed.backend_worker, parsed, run_id)
        except Exception as exc:
            result = {
                "backend": parsed.backend_worker,
                "status": "backend_startup_failed",
                "error": str(exc),
                "retrieve": {"stage_metrics": summarize_retrieval_metrics([])},
            }
        if parsed.backend_worker_output:
            Path(parsed.backend_worker_output).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        else:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("status") == "passed" else 1

    artifact_dir = Path(parsed.artifact_dir) if parsed.artifact_dir else ROOT / "docs" / "benchmarks" / f"cpp_rust_scale_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    if "rust" in parsed.backends and not os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_ROOT"):
        os.environ["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = str(artifact_dir / "rust_record_log_root")
    report: Json = {
        "run_id": run_id,
        "generated_at_ms": int(time.time() * 1000),
        "config": {
            "events": parsed.events,
            "raw_ops": parsed.raw_ops,
            "raw_read_ops": parsed.raw_read_ops,
            "raw_batch_size": parsed.raw_batch_size,
            "raw_read_batch_size": parsed.raw_read_batch_size,
            "dataset": parsed.dataset,
            "raw_workers": parsed.raw_workers,
            "messages_per_ingest": parsed.messages_per_ingest,
            "batch_size": parsed.messages_per_ingest,
            "ingest_workers": parsed.ingest_workers,
            "retrieve_queries": parsed.retrieve_queries,
            "retrieve_workers": parsed.retrieve_workers,
            "retrieve_warmup_queries": parsed.retrieve_warmup_queries,
            "max_context_tokens": parsed.max_context_tokens,
            "embedding_provider": parsed.embedding_provider,
            "embedding_model": parsed.embedding_model,
            "reader_provider": parsed.reader_provider,
            "reader_model": parsed.reader_model,
            "judge_provider": parsed.judge_provider,
            "judge_model": parsed.judge_model,
            "metaserver": parsed.metaserver,
            "namespace": parsed.namespace,
            "table": parsed.table,
            "topology": {
                "metaserver": parsed.metaserver,
                "namespace": parsed.namespace,
                "table": parsed.table,
            },
            "rust_cli": parsed.rust_cli,
            "rust_cli_policy": {
                "production_default": "matrixark_rust_proxy",
                "record_log_compat_allowed": parsed.allow_rust_record_log_compat,
                "debug_cli_allowed": parsed.allow_rust_debug_cli,
                "cpp_c_api_bridge_allowed": parsed.allow_rust_cpp_c_api_bridge,
                "rust_hot_path_default": "rust_native_proxy_or_workspace_direct_sdk",
            },
            "storage_options": parsed.storage_options,
            "effective_storage_tuning": effective_storage_tuning_from_env(),
            "required_page_block_metrics": PAGE_BLOCK_METRIC_NAMES,
            "required_storage_lifecycle_top_level_keys": STORAGE_LIFECYCLE_TOP_LEVEL_KEYS,
            "required_storage_write_sequence": STORAGE_WRITE_SEQUENCE_STEPS,
            "required_storage_write_result_fields": STORAGE_WRITE_RESULT_FIELDS,
            "required_storage_write_metrics": STORAGE_WRITE_METRIC_NAMES,
            "required_storage_read_sequence": STORAGE_READ_SEQUENCE_STEPS,
            "required_storage_read_result_fields": STORAGE_READ_RESULT_FIELDS,
            "required_storage_read_metrics": STORAGE_READ_METRIC_NAMES,
            "required_storage_cold_scan_sequence": STORAGE_COLD_SCAN_SEQUENCE_STEPS,
            "required_storage_cold_scan_result_fields": STORAGE_COLD_SCAN_RESULT_FIELDS,
            "required_storage_cold_scan_metrics": STORAGE_COLD_SCAN_METRIC_NAMES,
            "required_storage_lifecycle_phases": STORAGE_LIFECYCLE_PHASE_NAMES,
            "required_storage_manager_phase_metrics": STORAGE_MANAGER_PHASE_METRICS,
            "required_storage_index_behaviors": STORAGE_INDEX_BEHAVIOR_NAMES,
            "required_storage_reclaim_semantics": STORAGE_RECLAIM_SEMANTICS,
            "required_storage_reclaim_contract_fields": STORAGE_RECLAIM_CONTRACT_FIELDS,
            "required_storage_cache_layers": STORAGE_CACHE_LAYER_NAMES,
            "required_storage_cache_semantics": STORAGE_CACHE_SEMANTICS,
            "required_storage_cache_metrics": STORAGE_CACHE_METRIC_NAMES,
            "required_storage_cache_contract_fields": STORAGE_CACHE_CONTRACT_FIELDS,
            "required_storage_lifecycle_metrics": STORAGE_LIFECYCLE_METRIC_NAMES,
            "required_public_storage_feature_shapes": PUBLIC_STORAGE_FEATURE_SHAPES,
            "rust_record_log_root": os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_ROOT", ""),
            "python_ref_store": parsed.python_ref_store,
            "skip_context_pipeline": parsed.skip_context_pipeline,
            "phase0_min_selected_refs": parsed.phase0_min_selected_refs,
            "phase0_max_selected_ref_drift_ratio": parsed.phase0_max_selected_ref_drift_ratio,
            "allow_phase0_correctness_failure": parsed.allow_phase0_correctness_failure,
            "perf_min_qps_ratio": parsed.perf_min_qps_ratio,
            "perf_max_latency_ratio": parsed.perf_max_latency_ratio,
            "require_perf_parity": parsed.require_perf_parity,
            "phase_name": parsed.phase_name,
            "phase_scale_events": _parse_int_csv(parsed.phase_scale_events, DEFAULT_PHASE_SCALE_EVENTS),
            "phase_retrieve_workers": _parse_int_csv(parsed.phase_retrieve_workers, DEFAULT_PHASE_RETRIEVE_WORKERS),
            "phase_resource_imports": _parse_str_csv(parsed.phase_resource_imports, DEFAULT_PHASE_RESOURCE_IMPORTS),
            "phase_contextmemory_features": _parse_str_csv(
                parsed.phase_contextmemory_features,
                DEFAULT_PHASE_CONTEXTMEMORY_FEATURES,
            ),
            "require_phase_scale_matrix": parsed.require_phase_scale_matrix,
        },
        "backends": {},
    }
    for backend in parsed.backends:
        if parsed.no_isolate_backends:
            try:
                report["backends"][backend] = run_backend(backend, parsed, run_id)
            except Exception as exc:
                report["backends"][backend] = {
                    "backend": backend,
                    "status": "backend_startup_failed",
                    "error": str(exc),
                    "config": {
                        "metaserver": parsed.metaserver,
                        "namespace": parsed.namespace,
                        "table": parsed.table,
                        "cpp_lib": parsed.cpp_lib if backend == "cpp" else "",
                        "rust_cli": parsed.rust_cli if backend == "rust" else "",
                    },
                    "retrieve": {"stage_metrics": summarize_retrieval_metrics([])},
                }
        else:
            report["backends"][backend] = run_backend_isolated(backend, parsed, run_id, artifact_dir)
        (artifact_dir / f"{backend}.json").write_text(json.dumps(report["backends"][backend], indent=2, sort_keys=True), encoding="utf-8")
    parsed.python_ref_result = report["backends"].get("python_ref")
    report["comparison"] = comparison(report["backends"].get("cpp"), report["backends"].get("rust"), parsed)
    report["phase_scale_matrix"] = phase_scale_matrix_gate(report, parsed)
    report["production_policy"] = production_policy_gate(report)
    (artifact_dir / "comparison.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    write_report(artifact_dir / "comparison.md", report)
    print(
        json.dumps(
            {
                "artifact_dir": str(artifact_dir),
                "comparison": report["comparison"],
                "phase_scale_matrix": report["phase_scale_matrix"],
                "production_policy": report["production_policy"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    backends_passed = all(report["backends"].get(b, {}).get("status") == "passed" for b in parsed.backends)
    parity_passed = bool(report.get("comparison", {}).get("perf_parity", {}).get("passed", True))
    phase0_failed = report.get("comparison", {}).get("phase0_correctness", {}).get("status") == "failed"
    phase_scale_failed = report.get("phase_scale_matrix", {}).get("status") == "failed"
    production_policy_failed = report.get("production_policy", {}).get("status") == "failed"
    if phase0_failed and not parsed.allow_phase0_correctness_failure:
        return 3
    if phase_scale_failed:
        return 4
    if parsed.require_perf_parity and production_policy_failed:
        return 5
    if parsed.require_perf_parity and not parity_passed:
        return 2
    return 0 if backends_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
