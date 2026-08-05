"""Split out of validate_storage_lifecycle_parity.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import ast
import json
import pathlib
from typing import Any

try:  # package path (tools.validate_storage_lifecycle_parity)
    from .validate_storage_lifecycle_parity import (
        ALLOWED_ALIAS_CONTAINERS,
        CANONICAL_JSON_FIELDS,
        LEGACY_ALIAS_MAP,
        SCALE_REPORT,
    )
except ImportError:  # top-level path (validate_storage_lifecycle_parity)
    from validate_storage_lifecycle_parity import (
        ALLOWED_ALIAS_CONTAINERS,
        CANONICAL_JSON_FIELDS,
        LEGACY_ALIAS_MAP,
        SCALE_REPORT,
    )

__all__ = ['_extract_runner_list', '_extract_runner_dict', '_load_json', '_dig_metrics', '_dig_config', '_dig_sequence', '_dig_write_contract', '_dig_read_contract', '_dig_cold_scan_contract', '_dig_manager_contract', '_dig_index_contract', '_dig_cache_contract', '_dig_lifecycle_phases', '_dig_reclaim_semantics', '_dig_reclaim_scope', '_dig_reclaim_contract', '_dig_safety_snapshot', '_dig_watermark_snapshot', '_dig_gc_snapshot', '_dig_index_snapshot', '_dig_topology_snapshot', '_dig_cache_layers', '_dig_cache_semantics', '_as_number', '_walk_public_keys', '_normalize_public_storage_shape', '_dig_public_storage_feature_shapes', '_metric_number', '_validate_physical_reclaim_evidence', '_validate_stream_slot_index_evidence']


def _extract_runner_list(name: str) -> list[str]:
    tree = ast.parse(SCALE_REPORT.read_text(encoding="utf-8"), filename=str(SCALE_REPORT))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                value = ast.literal_eval(node.value)
                if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                    raise AssertionError(f"{name} must be a list[str]")
                return value
    raise AssertionError(f"{name} not found in scale report runner")


def _extract_runner_dict(name: str) -> dict[str, str]:
    tree = ast.parse(SCALE_REPORT.read_text(encoding="utf-8"), filename=str(SCALE_REPORT))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                value = ast.literal_eval(node.value)
                if not isinstance(value, dict) or not all(isinstance(key, str) and isinstance(item, str) for key, item in value.items()):
                    raise AssertionError(f"{name} must be a dict[str, str]")
                return value
    raise AssertionError(f"{name} not found in scale report runner")


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _dig_metrics(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_lifecycle_metrics"),
        report.get("storage_lifecycle", {}).get("metrics") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("metrics", {}).get("storage_lifecycle") if isinstance(report.get("metrics"), dict) else None,
        report.get("metrics"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_config(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("effective_storage_tuning"),
        report.get("config", {}).get("effective_storage_tuning") if isinstance(report.get("config"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_sequence(report: dict[str, Any], key: str) -> list[str]:
    candidates = [
        report.get(key),
        report.get("storage_lifecycle", {}).get(key) if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_sequences", {}).get(key) if isinstance(report.get("storage_sequences"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_write_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_write_contract"),
        report.get("storage_lifecycle", {}).get("write_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("write_path", {}).get("contract") if isinstance(report.get("write_path"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_read_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_read_contract"),
        report.get("storage_lifecycle", {}).get("read_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("read_path", {}).get("contract") if isinstance(report.get("read_path"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_cold_scan_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_cold_scan_contract"),
        report.get("storage_lifecycle", {}).get("cold_scan_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("cold_scan_path", {}).get("contract") if isinstance(report.get("cold_scan_path"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_manager_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_manager_contract"),
        report.get("storage_lifecycle", {}).get("manager_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_manager", {}).get("contract") if isinstance(report.get("storage_manager"), dict) else None,
        report.get("store_manager", {}).get("contract") if isinstance(report.get("store_manager"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_index_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_index_contract"),
        report.get("storage_lifecycle", {}).get("index_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_index", {}).get("contract") if isinstance(report.get("storage_index"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_cache_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_cache_contract"),
        report.get("storage_lifecycle", {}).get("cache_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_cache", {}).get("contract") if isinstance(report.get("storage_cache"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_lifecycle_phases(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_lifecycle_phases"),
        report.get("storage_lifecycle", {}).get("phases") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_reclaim_semantics(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_reclaim_semantics"),
        report.get("storage_lifecycle", {}).get("reclaim_semantics") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_reclaim_scope(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_reclaim_scope"),
        report.get("storage_lifecycle", {}).get("reclaim_scope") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_reclaim_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_reclaim_contract"),
        report.get("storage_lifecycle", {}).get("reclaim_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_reclaim", {}).get("contract") if isinstance(report.get("storage_reclaim"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_safety_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_safety_snapshot"),
        report.get("storage_lifecycle", {}).get("safety_snapshot") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_safety", {}).get("snapshot") if isinstance(report.get("storage_safety"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_watermark_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_watermark_snapshot"),
        report.get("storage_lifecycle", {}).get("watermark_snapshot") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_watermarks") if isinstance(report.get("storage_watermarks"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_gc_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_gc_snapshot"),
        report.get("storage_lifecycle", {}).get("gc_snapshot") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_gc", {}).get("snapshot") if isinstance(report.get("storage_gc"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_index_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_index_snapshot"),
        report.get("storage_lifecycle", {}).get("index_snapshot") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_index", {}).get("snapshot") if isinstance(report.get("storage_index"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_topology_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_topology_snapshot"),
        report.get("storage_lifecycle", {}).get("topology_snapshot") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_topology", {}).get("snapshot") if isinstance(report.get("storage_topology"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_cache_layers(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_cache_layers"),
        report.get("storage_lifecycle", {}).get("cache_layers") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_cache", {}).get("layers") if isinstance(report.get("storage_cache"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_cache_semantics(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_cache_semantics"),
        report.get("storage_lifecycle", {}).get("cache_semantics") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_cache", {}).get("semantics") if isinstance(report.get("storage_cache"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _walk_public_keys(value: Any, *, in_alias_container: bool = False, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    violations: list[tuple[tuple[str, ...], str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            child_in_alias = in_alias_container or str(key) in ALLOWED_ALIAS_CONTAINERS
            if not child_in_alias and str(key) in LEGACY_ALIAS_MAP:
                violations.append((child_path, str(key)))
            violations.extend(_walk_public_keys(child, in_alias_container=child_in_alias, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            violations.extend(_walk_public_keys(child, in_alias_container=in_alias_container, path=(*path, str(index))))
    return violations


def _normalize_public_storage_shape(report: dict[str, Any]) -> dict[str, Any]:
    """Return canonical public storage fields from a backend report.

    Canonical fields win. Legacy aliases are accepted only from compatibility
    alias containers and normalized for comparison.
    """
    source_candidates = [
        report.get("public_storage_contract"),
        report.get("storage_public_contract"),
        report.get("storage_lifecycle", {}).get("public_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    source = next((candidate for candidate in source_candidates if isinstance(candidate, dict)), {})
    aliases = source.get("compatibility_aliases") if isinstance(source.get("compatibility_aliases"), dict) else {}

    normalized: dict[str, Any] = {}
    for key in CANONICAL_JSON_FIELDS:
        if key in source:
            normalized[key] = source[key]
    for alias, canonical in LEGACY_ALIAS_MAP.items():
        if alias in aliases and canonical not in normalized:
            normalized[canonical] = aliases[alias]
    return normalized


def _dig_public_storage_feature_shapes(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("public_storage_feature_shapes"),
        report.get("storage_lifecycle", {}).get("public_feature_shapes") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_public_contract", {}).get("feature_shapes") if isinstance(report.get("storage_public_contract"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _metric_number(metrics: dict[str, Any], name: str) -> float:
    value = _as_number(metrics.get(name))
    return 0.0 if value is None else value


def _validate_physical_reclaim_evidence(backend: str, metrics: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    cache_evictions = _metric_number(metrics, "cache_evictions")
    physical_reclaimed = _metric_number(metrics, "physical_reclaimed_bytes")
    compaction_reclaimed = _metric_number(metrics, "compaction_reclaimed_bytes")
    physical_errors = _metric_number(metrics, "physical_reclaim_errors")
    tombstone_evidence = (
        _metric_number(metrics, "tombstone_records")
        + _metric_number(metrics, "stale_page_tombstones")
        + _metric_number(metrics, "stale_block_tombstones")
    )
    rewrite_or_skip_evidence = (
        _metric_number(metrics, "stale_pages_rewritten")
        + _metric_number(metrics, "stale_pages_skipped")
        + _metric_number(metrics, "stale_blocks_rewritten")
        + _metric_number(metrics, "stale_blocks_skipped")
    )

    if cache_evictions > 0 and physical_reclaimed == 0 and compaction_reclaimed == 0:
        # This is valid and intentional: cache eviction frees memory only.
        return failures

    if physical_reclaimed > 0:
        if physical_errors != 0:
            failures.append(f"{backend} physical reclaim reported bytes with errors={physical_errors}")
        if tombstone_evidence <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without tombstone evidence")
        if rewrite_or_skip_evidence <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without stale page/block rewrite-or-skip evidence")
        if compaction_reclaimed <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without compaction_reclaimed_bytes")
    return failures


def _validate_stream_slot_index_evidence(backend: str, metrics: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    segment_open = _metric_number(metrics, "segment_open_count")
    segment_sealed = _metric_number(metrics, "segment_sealed_count")
    stream_rollover = _metric_number(metrics, "stream_rollover_count")
    log_replay = _metric_number(metrics, "append_log_replay_records")
    log_reclaimed = _metric_number(metrics, "append_log_reclaimed_records")
    owner_mismatch = _metric_number(metrics, "slot_owner_mismatch_count")

    if segment_sealed > segment_open:
        failures.append(
            f"{backend} segment_sealed_count cannot exceed segment_open_count: "
            f"sealed={segment_sealed} open={segment_open}"
        )
    if stream_rollover > 0 and segment_open <= 0:
        failures.append(f"{backend} stream rollover reported without segment open evidence")
    if log_reclaimed > log_replay:
        failures.append(
            f"{backend} append_log_reclaimed_records cannot exceed append_log_replay_records: "
            f"reclaimed={log_reclaimed} replay={log_replay}"
        )
    if owner_mismatch != 0:
        failures.append(f"{backend} slot_owner_mismatch_count must be zero, got {owner_mismatch}")
    for metric in [
        "slot_dirty_generation_count",
        "slot_tombstone_count",
        "slot_stale_ref_count",
        "page_index_rebuild_count",
        "block_index_rebuild_count",
        "object_index_rebuild_count",
    ]:
        if _metric_number(metrics, metric) < 0:
            failures.append(f"{backend} {metric} must be non-negative")
    return failures
