#!/usr/bin/env python3
"""Validate Rust TemporalStore resource/skill/conversation scale evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REQUIRED_LAYERS = {"agent", "user", "workspace", "global"}
REQUIRED_GROUPS = {"global", "user:user", "workspace:context"}
REQUIRED_SCOPE_KEYS = {"agent:codex", "agent:claude", "global", "user:user", "workspace:context"}
REQUIRED_SCAN_SCOPE_KEYS = {"agent:codex", "global", "user:user", "workspace:context"}
REQUIRED_SELECTED_SCOPE_KEYS = {"agent:codex", "global", "user:user", "workspace:context"}
REQUIRED_SELECTED_SKILLS = {"benchmark-reader", "context-debug", "payments-incident"}
REQUIRED_SKILL_OWNER_SCOPES = {"team:benchmarks", "team:context", "team:payments"}
REQUIRED_SKILL_TRIGGER_TERMS = {"checkout", "context", "injection", "latency", "rollback", "summary"}
REQUIRED_RESOURCE_IMPORT_KINDS = {"git_repo", "markdown", "pdf", "url"}
REQUIRED_RESOURCE_OWNER_SCOPES = {"team:benchmarks", "team:context", "team:payments", "team:platform"}
REQUIRED_RESOURCE_PARSERS = {"context-scale-harness"}


def fail(message: str) -> int:
    print(f"context resource/skill scale validation failed: {message}", file=sys.stderr)
    return 1


def load_report(path: Path) -> dict[str, Any]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ValueError(f"report not found: {path}")
    except json.JSONDecodeError as exc:
        raise ValueError(f"report is not valid JSON: {exc}") from exc
    if not isinstance(report, dict):
        raise ValueError("report root must be a JSON object")
    return report


def require_bool(report: dict[str, Any], field: str) -> None:
    if report.get(field) is not True:
        raise ValueError(f"{field} must be true, got {report.get(field)!r}")


def require_int_at_least(report: dict[str, Any], field: str, minimum: int) -> int:
    value = report.get(field)
    if not isinstance(value, int):
        raise ValueError(f"{field} must be an integer, got {value!r}")
    if value < minimum:
        raise ValueError(f"{field} must be >= {minimum}, got {value}")
    return value


def require_int_equal(report: dict[str, Any], field: str, expected: int) -> None:
    value = report.get(field)
    if value != expected:
        raise ValueError(f"{field} must be {expected}, got {value!r}")


def require_int_between(report: dict[str, Any], field: str, minimum: int, maximum: int) -> int:
    value = report.get(field)
    if not isinstance(value, int):
        raise ValueError(f"{field} must be an integer, got {value!r}")
    if value < minimum or value > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}, got {value}")
    return value


def require_string_set(report: dict[str, Any], field: str, required: set[str]) -> set[str]:
    value = report.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field} must be a string array")
    observed = set(value)
    missing = sorted(required - observed)
    if missing:
        raise ValueError(f"{field} missing required entries: {missing}")
    return observed


def require_map_keys_at_least(
    report: dict[str, Any], field: str, required: set[str], minimum: int
) -> dict[str, Any]:
    value = report.get(field)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"{field} missing required keys: {missing}")
    too_small = {
        key: value.get(key)
        for key in sorted(required)
        if not isinstance(value.get(key), int) or value.get(key) < minimum
    }
    if too_small:
        raise ValueError(f"{field} entries must be >= {minimum}: {too_small}")
    return value


def validate_report(report: dict[str, Any], min_sources: int, max_expanded: int) -> None:
    require_bool(report, "ready")
    require_bool(report, "multi_agent_scan_ready")
    require_bool(report, "fanout_ready")
    require_bool(report, "secondary_index_ready")
    require_bool(report, "fanout_namespace_replication_avoided")
    total = require_int_at_least(report, "total_source_count", min_sources)
    accepted = require_int_at_least(report, "accepted_sources", min_sources)
    if accepted != total:
        raise ValueError(f"accepted_sources must equal total_source_count, got {accepted}/{total}")
    require_int_equal(report, "failed_sources", 0)
    candidates = require_int_at_least(report, "fanout_namespace_node_candidates", min_sources)
    summary_query_nodes = require_int_at_least(report, "fanout_summary_embedding_query_nodes", 1)
    pruned_peers = require_int_at_least(report, "fanout_summary_pruned_peer_agent_nodes", 1)
    if summary_query_nodes + pruned_peers != candidates:
        raise ValueError(
            "fanout_summary_embedding_query_nodes + fanout_summary_pruned_peer_agent_nodes "
            f"must equal fanout_namespace_node_candidates, got {summary_query_nodes}+{pruned_peers}!={candidates}"
        )
    expanded = require_int_at_least(report, "fanout_event_expanded_nodes", 1)
    if expanded > max_expanded:
        raise ValueError(f"fanout_event_expanded_nodes must be <= {max_expanded}, got {expanded}")
    if expanded >= candidates:
        raise ValueError(f"fanout did not reduce candidates: {expanded}/{candidates}")
    avoided = require_int_at_least(report, "fanout_avoided_namespace_replication_nodes", 1)
    if avoided != candidates - expanded:
        raise ValueError(
            "fanout_avoided_namespace_replication_nodes must equal candidates-expanded, "
            f"got {avoided} vs {candidates - expanded}"
        )
    require_int_at_least(report, "fanout_reduction_percent", 40)
    require_int_at_least(report, "fanout_selected_colocation_group_count", 4)
    require_string_set(report, "fanout_selected_colocation_scope_keys", REQUIRED_SELECTED_SCOPE_KEYS)
    require_string_set(report, "fanout_selected_colocation_groups", REQUIRED_GROUPS)
    require_int_at_least(report, "fanout_selected_current_agent_nodes", 1)
    require_bool(report, "fanout_current_agent_first_selected")
    selected_order = report.get("fanout_selected_colocation_scope_order")
    if not isinstance(selected_order, list) or not selected_order:
        raise ValueError("fanout_selected_colocation_scope_order must be a non-empty string array")
    if selected_order[0] != "agent:codex":
        raise ValueError(
            "fanout_selected_colocation_scope_order must start with current agent agent:codex, "
            f"got {selected_order[:3]!r}"
        )
    require_int_at_least(report, "fanout_peer_agent_nodes", 1)
    require_int_equal(report, "fanout_selected_peer_agent_nodes", 0)
    require_int_at_least(report, "fanout_skipped_peer_agent_nodes", 1)
    require_bool(report, "fanout_peer_agent_limit_applied")
    require_int_at_least(report, "fanout_selected_user_shared_nodes", 1)
    require_int_at_least(report, "fanout_selected_workspace_shared_nodes", 1)
    require_int_at_least(report, "fanout_selected_global_shared_nodes", 1)
    require_int_equal(report, "fanout_shared_scope_coverage_count", 3)
    require_bool(report, "fanout_shared_scope_coverage_ready")
    shared_quota = require_int_at_least(report, "fanout_shared_layer_quota_nodes", 4)
    shared_selected = require_int_at_least(report, "fanout_shared_selected_node_count", shared_quota)
    current_selected = require_int_at_least(report, "fanout_selected_current_agent_nodes", 1)
    if current_selected <= shared_selected:
        raise ValueError(
            "fanout_selected_current_agent_nodes must remain boosted above shared selected nodes, "
            f"got current={current_selected} shared={shared_selected}"
        )
    require_int_between(report, "fanout_current_agent_boost_percent", 40, 75)
    require_bool(report, "fanout_current_agent_boost_bounded")
    require_bool(report, "fanout_layer_quota_applied")
    require_int_at_least(report, "retrieved_block_count", 8)
    require_int_at_least(report, "retrieved_current_agent_block_count", 1)
    require_int_at_least(report, "retrieved_user_shared_block_count", 1)
    require_int_at_least(report, "retrieved_workspace_shared_block_count", 1)
    require_int_at_least(report, "retrieved_global_shared_block_count", 1)
    selected_skill_count = require_int_at_least(report, "selected_skill_count", 3)
    require_string_set(report, "selected_skill_names", REQUIRED_SELECTED_SKILLS)
    require_string_set(report, "selected_skill_owner_scopes", REQUIRED_SKILL_OWNER_SCOPES)
    require_string_set(report, "selected_skill_trigger_terms", REQUIRED_SKILL_TRIGGER_TERMS)
    allowed_tool_matches = require_int_at_least(
        report, "selected_skill_allowed_tool_matches", selected_skill_count
    )
    if allowed_tool_matches != selected_skill_count:
        raise ValueError(
            "selected_skill_allowed_tool_matches must equal selected_skill_count, "
            f"got {allowed_tool_matches}/{selected_skill_count}"
        )
    require_map_keys_at_least(report, "resource_import_kinds", REQUIRED_RESOURCE_IMPORT_KINDS, 1)
    require_string_set(report, "resource_owner_scopes", REQUIRED_RESOURCE_OWNER_SCOPES)
    require_string_set(report, "resource_parser_names", REQUIRED_RESOURCE_PARSERS)
    require_int_at_least(report, "selected_ref_count", 8)
    require_bool(report, "fanout_selected_ref_current_agent_first")
    require_bool(report, "fanout_injection_current_agent_first")
    require_int_equal(report, "fanout_selected_peer_agent_ref_count", 0)
    selected_ref_scopes = require_string_set(
        report, "fanout_selected_ref_scope_keys", REQUIRED_SELECTED_SCOPE_KEYS
    )
    selected_ref_order = report.get("fanout_selected_ref_scope_order")
    if not isinstance(selected_ref_order, list) or not selected_ref_order:
        raise ValueError("fanout_selected_ref_scope_order must be a non-empty string array")
    if selected_ref_order[0] != "agent:codex":
        raise ValueError(
            "fanout_selected_ref_scope_order must start with current agent agent:codex, "
            f"got {selected_ref_order[:3]!r}"
        )
    injection_order = report.get("fanout_injection_scope_order")
    if not isinstance(injection_order, list) or not injection_order:
        raise ValueError("fanout_injection_scope_order must be a non-empty string array")
    if injection_order[0] != "agent:codex":
        raise ValueError(
            "fanout_injection_scope_order must start with current agent agent:codex, "
            f"got {injection_order[:3]!r}"
        )
    require_string_set(report, "fanout_scan_layers", REQUIRED_LAYERS)
    require_string_set(report, "fanout_colocation_groups", REQUIRED_GROUPS)
    require_string_set(report, "fanout_colocation_scope_keys", REQUIRED_SCOPE_KEYS)
    require_string_set(report, "fanout_required_scan_scope_keys", REQUIRED_SCAN_SCOPE_KEYS)
    require_int_equal(report, "fanout_locality_key_count", expanded)
    require_int_equal(report, "fanout_peer_locality_key_count", 0)
    locality_scopes = require_string_set(
        report, "fanout_locality_scope_keys", REQUIRED_SELECTED_SCOPE_KEYS
    )
    if locality_scopes != set(report["fanout_selected_colocation_scope_keys"]):
        raise ValueError(
            "fanout_locality_scope_keys must exactly match fanout_selected_colocation_scope_keys, "
            f"got {sorted(locality_scopes)} vs {sorted(report['fanout_selected_colocation_scope_keys'])}"
        )
    if selected_ref_scopes != locality_scopes:
        raise ValueError(
            "fanout_selected_ref_scope_keys must exactly match fanout_locality_scope_keys, "
            f"got {sorted(selected_ref_scopes)} vs {sorted(locality_scopes)}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "report",
        nargs="?",
        default="docs/benchmark_archives/context_resource_skill_scale_20260706_summary.json",
        help="Path to the resource/skill/conversation scale JSON report.",
    )
    parser.add_argument("--min-sources", type=int, default=40)
    parser.add_argument("--max-expanded", type=int, default=16)
    args = parser.parse_args()
    try:
        report = load_report(Path(args.report))
        validate_report(report, args.min_sources, args.max_expanded)
    except ValueError as exc:
        return fail(str(exc))
    print(
        "validated context resource/skill scale evidence "
        f"report={args.report} sources={report['total_source_count']} "
        f"expanded={report['fanout_event_expanded_nodes']} "
        f"retrieved_blocks={report['retrieved_block_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
