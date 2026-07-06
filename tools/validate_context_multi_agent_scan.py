#!/usr/bin/env python3
"""Validate Rust TemporalStore multi-agent context scan evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REQUIRED_LAYERS = {"agent", "user", "workspace", "global"}
REQUIRED_COLOCATION_GROUPS = {"user:user", "workspace:context", "global"}
REQUIRED_SELECTED_SCOPE_KEYS = {"agent:codex", "user:user", "workspace:context", "global"}


def fail(message: str) -> int:
    print(f"context multi-agent scan validation failed: {message}", file=sys.stderr)
    return 1


def load_report(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except FileNotFoundError:
        raise ValueError(f"report not found: {path}")
    except json.JSONDecodeError as exc:
        raise ValueError(f"report is not valid JSON: {exc}") from exc
    if not isinstance(report, dict):
        raise ValueError("report root must be a JSON object")
    return report


def require_bool(report: dict[str, Any], field: str) -> bool:
    value = report.get(field)
    if value is not True:
        raise ValueError(f"{field} must be true, got {value!r}")
    return True


def require_int_at_least(report: dict[str, Any], field: str, minimum: int) -> int:
    value = report.get(field)
    if not isinstance(value, int):
        raise ValueError(f"{field} must be an integer, got {value!r}")
    if value < minimum:
        raise ValueError(f"{field} must be >= {minimum}, got {value}")
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


def require_int_equal(report: dict[str, Any], field: str, expected: int) -> None:
    value = report.get(field)
    if value != expected:
        raise ValueError(f"{field} must be {expected}, got {value!r}")


def require_distribution_count(report: dict[str, Any], field: str, key: str, expected: int) -> None:
    value = report.get(field)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    observed = value.get(key, 0)
    if observed != expected:
        raise ValueError(f"{field}[{key!r}] must be {expected}, got {observed!r}")


def require_int_map(report: dict[str, Any], field: str) -> dict[str, int]:
    value = report.get(field)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    bad_items = {
        key: item
        for key, item in value.items()
        if not isinstance(key, str) or not isinstance(item, int)
    }
    if bad_items:
        raise ValueError(f"{field} must map strings to integers, got {bad_items!r}")
    return value


def validate_report(report: dict[str, Any], min_candidates: int, max_expanded: int) -> None:
    require_bool(report, "ready")
    require_bool(report, "fanout_reduced")
    require_bool(report, "namespace_replication_avoided")
    require_bool(report, "layer_quota_applied")
    candidates = require_int_at_least(report, "namespace_node_candidates", min_candidates)
    summary_query_nodes = require_int_at_least(report, "summary_embedding_query_nodes", 1)
    pruned_peers = require_int_at_least(report, "summary_pruned_peer_agent_nodes", 1)
    if summary_query_nodes + pruned_peers != candidates:
        raise ValueError(
            "summary_embedding_query_nodes + summary_pruned_peer_agent_nodes "
            f"must equal namespace_node_candidates, got {summary_query_nodes}+{pruned_peers}!={candidates}"
        )
    require_int_equal(report, "configured_summary_node_limit", 11)
    require_int_equal(report, "effective_summary_node_limit", 11)
    require_int_equal(report, "configured_event_node_limit", 4)
    require_int_equal(report, "effective_event_node_limit", 4)
    require_int_equal(report, "configured_peer_agent_node_limit", 0)
    expanded = require_int_at_least(report, "event_expanded_nodes", 1)
    if expanded > max_expanded:
        raise ValueError(f"event_expanded_nodes must be <= {max_expanded}, got {expanded}")
    if expanded >= candidates:
        raise ValueError(
            f"fanout did not reduce candidates: candidates={candidates} expanded={expanded}"
        )
    avoided = require_int_at_least(report, "avoided_namespace_replication_nodes", 1)
    if avoided != candidates - expanded:
        raise ValueError(
            "avoided_namespace_replication_nodes must equal candidates-expanded, "
            f"got {avoided} vs {candidates - expanded}"
        )
    require_int_at_least(report, "fanout_reduction_percent", 40)
    require_int_equal(report, "selected_colocation_group_count", 3)
    require_int_equal(report, "selected_colocation_scope_count", 4)
    require_string_set(report, "selected_colocation_scope_keys", REQUIRED_SELECTED_SCOPE_KEYS)
    require_string_set(report, "selected_colocation_groups", REQUIRED_COLOCATION_GROUPS)
    candidate_groups = require_int_map(report, "colocation_group_candidate_counts")
    selected_groups = require_int_map(report, "selected_colocation_group_counts")
    pruned_groups = require_int_map(report, "summary_pruned_colocation_group_counts")
    pruned_scopes = require_int_map(report, "summary_pruned_colocation_scope_counts")
    skipped_groups = require_int_map(report, "skipped_colocation_group_counts")
    skipped_scopes = require_int_map(report, "skipped_colocation_scope_counts")
    if set(selected_groups) != REQUIRED_COLOCATION_GROUPS:
        raise ValueError(
            "selected_colocation_group_counts must contain exactly the selected shared groups, "
            f"got {sorted(selected_groups)}"
        )
    for group in REQUIRED_COLOCATION_GROUPS:
        if candidate_groups.get(group, 0) < selected_groups.get(group, 0):
            raise ValueError(
                f"selected colocation group {group} exceeds candidates: "
                f"{selected_groups.get(group, 0)}>{candidate_groups.get(group, 0)}"
            )
        if (
            selected_groups.get(group, 0)
            + skipped_groups.get(group, 0)
            + pruned_groups.get(group, 0)
            != candidate_groups.get(group, 0)
        ):
            raise ValueError(
                f"selected+skipped+pruned colocation group {group} must equal candidates: "
                f"{selected_groups.get(group, 0)}+{skipped_groups.get(group, 0)}+"
                f"{pruned_groups.get(group, 0)}!={candidate_groups.get(group, 0)}"
            )
    if sum(candidate_groups.values()) != candidates:
        raise ValueError(
            "colocation_group_candidate_counts must account for all namespace candidates, "
            f"got {sum(candidate_groups.values())} vs {candidates}"
        )
    if sum(selected_groups.values()) != expanded:
        raise ValueError(
            "selected_colocation_group_counts must account for all expanded nodes, "
            f"got {sum(selected_groups.values())} vs {expanded}"
        )
    skipped_budget_nodes = require_int_at_least(report, "skipped_summary_budget_node_count", 1)
    if skipped_budget_nodes + pruned_peers != avoided:
        raise ValueError(
            "skipped_summary_budget_node_count + summary_pruned_peer_agent_nodes "
            f"must equal avoided namespace nodes, got {skipped_budget_nodes}+{pruned_peers}!={avoided}"
        )
    if sum(skipped_groups.values()) != skipped_budget_nodes:
        raise ValueError(
            "skipped_colocation_group_counts must account for skipped nodes, "
            f"got {sum(skipped_groups.values())} vs {skipped_budget_nodes}"
        )
    if sum(skipped_scopes.values()) != skipped_budget_nodes:
        raise ValueError(
            "skipped_colocation_scope_counts must account for skipped nodes, "
            f"got {sum(skipped_scopes.values())} vs {skipped_budget_nodes}"
        )
    if skipped_scopes.get("agent:codex", 0) < 1:
        raise ValueError("skipped_colocation_scope_counts must prove bounded current-agent overflow")
    if sum(pruned_groups.values()) != pruned_peers:
        raise ValueError(
            "summary_pruned_colocation_group_counts must account for pruned peer agents, "
            f"got {sum(pruned_groups.values())} vs {pruned_peers}"
        )
    if pruned_scopes.get("agent:claude", 0) < 1:
        raise ValueError("summary_pruned_colocation_scope_counts must prove peer-agent pruning")
    require_int_at_least(report, "max_selected_colocation_group_nodes", 1)
    require_bool(report, "colocation_group_fanout_reduced")
    require_int_at_least(report, "shared_layer_quota_nodes", 4)
    require_int_at_least(report, "candidate_current_agent_nodes", 1)
    require_int_at_least(report, "candidate_peer_agent_nodes", 1)
    require_int_at_least(report, "candidate_user_shared_nodes", 1)
    require_int_at_least(report, "candidate_workspace_shared_nodes", 1)
    require_int_at_least(report, "candidate_global_shared_nodes", 1)
    require_int_equal(report, "candidate_shared_scope_coverage_count", 3)
    require_int_at_least(report, "candidate_shared_node_count", 3)
    require_bool(report, "candidate_scope_pressure_ready")
    require_int_at_least(report, "selected_current_agent_nodes", 1)
    require_distribution_count(
        report,
        "selected_colocation_scope_distribution",
        "agent:codex",
        report["selected_current_agent_nodes"],
    )
    require_distribution_count(report, "selected_colocation_scope_distribution", "agent:claude", 0)
    require_distribution_count(
        report,
        "selected_colocation_scope_distribution",
        "user:user",
        report["selected_user_shared_nodes"],
    )
    require_distribution_count(
        report,
        "selected_colocation_scope_distribution",
        "workspace:context",
        report["selected_workspace_shared_nodes"],
    )
    require_distribution_count(
        report,
        "selected_colocation_scope_distribution",
        "global",
        report["selected_global_shared_nodes"],
    )
    require_bool(report, "current_agent_first_selected")
    selected_order = report.get("selected_colocation_scope_order")
    if not isinstance(selected_order, list) or not selected_order:
        raise ValueError("selected_colocation_scope_order must be a non-empty string array")
    if selected_order[0] != "agent:codex":
        raise ValueError(
            "selected_colocation_scope_order must start with current agent agent:codex, "
            f"got {selected_order[:3]!r}"
        )
    require_int_at_least(report, "peer_agent_nodes", 1)
    selected_peer = report.get("selected_peer_agent_nodes")
    if not isinstance(selected_peer, int):
        raise ValueError(f"selected_peer_agent_nodes must be an integer, got {selected_peer!r}")
    if selected_peer != 0:
        raise ValueError(
            "selected_peer_agent_nodes must be 0 under tight shared quota, "
            f"got {selected_peer}"
        )
    require_int_at_least(report, "skipped_peer_agent_nodes", 1)
    require_bool(report, "peer_agent_limit_applied")
    require_int_at_least(report, "selected_user_shared_nodes", 1)
    require_int_at_least(report, "selected_workspace_shared_nodes", 1)
    require_int_at_least(report, "selected_global_shared_nodes", 1)
    shared_selected = require_int_at_least(report, "selected_shared_layer_nodes", 3)
    current_percent = require_int_at_least(report, "selected_current_agent_percent", 1)
    shared_percent = require_int_at_least(report, "selected_shared_layer_percent", 1)
    require_int_equal(report, "selected_peer_agent_percent", 0)
    if current_percent + shared_percent > 100:
        raise ValueError(
            "selected current-agent and shared-layer percentages cannot exceed 100, "
            f"got {current_percent}+{shared_percent}"
        )
    if shared_selected != (
        report["selected_user_shared_nodes"]
        + report["selected_workspace_shared_nodes"]
        + report["selected_global_shared_nodes"]
    ):
        raise ValueError(
            "selected_shared_layer_nodes must equal selected user+workspace+global nodes, "
            f"got {shared_selected}"
        )
    require_int_equal(report, "required_shared_scope_count", 3)
    require_int_equal(report, "selected_shared_scope_coverage_count", 3)
    require_bool(report, "shared_scope_coverage_ready")
    require_int_at_least(report, "retrieved_block_count", 4)
    require_int_at_least(report, "retrieved_current_agent_block_count", 1)
    require_int_at_least(report, "retrieved_user_shared_block_count", 1)
    require_int_at_least(report, "retrieved_workspace_shared_block_count", 1)
    require_int_at_least(report, "retrieved_global_shared_block_count", 1)
    require_int_at_least(report, "selected_ref_count", 4)
    require_bool(report, "selected_ref_current_agent_first")
    require_bool(report, "injection_current_agent_first")
    require_int_equal(report, "selected_peer_agent_ref_count", 0)
    selected_scopes = require_string_set(
        report, "selected_ref_scope_keys", REQUIRED_SELECTED_SCOPE_KEYS
    )
    if selected_scopes != set(report["locality_scope_keys"]):
        raise ValueError(
            "selected_ref_scope_keys must exactly match locality_scope_keys, "
            f"got {sorted(selected_scopes)} vs {sorted(report['locality_scope_keys'])}"
        )
    selected_order = report.get("selected_ref_scope_order")
    if not isinstance(selected_order, list) or not selected_order:
        raise ValueError("selected_ref_scope_order must be a non-empty string array")
    if selected_order[0] != "agent:codex":
        raise ValueError(
            "selected_ref_scope_order must start with current agent agent:codex, "
            f"got {selected_order[:3]!r}"
        )
    injection_order = report.get("injection_scope_order")
    if not isinstance(injection_order, list) or not injection_order:
        raise ValueError("injection_scope_order must be a non-empty string array")
    if injection_order[0] != "agent:codex":
        raise ValueError(
            "injection_scope_order must start with current agent agent:codex, "
            f"got {injection_order[:3]!r}"
        )
    require_string_set(report, "scan_layers", REQUIRED_LAYERS)
    require_string_set(report, "colocation_groups", REQUIRED_COLOCATION_GROUPS)
    require_string_set(
        report,
        "colocation_scope_keys",
        {"agent:codex", "agent:claude", "user:user", "workspace:context", "global"},
    )
    require_string_set(
        report,
        "required_scan_scope_keys",
        {"agent:codex", "user:user", "workspace:context", "global"},
    )
    if report.get("scan_policy_current_agent_scope_key") != "agent:codex":
        raise ValueError(
            "scan_policy_current_agent_scope_key must be agent:codex, "
            f"got {report.get('scan_policy_current_agent_scope_key')!r}"
        )
    if report.get("scan_policy_owner_scope_key") != "workspace:context":
        raise ValueError(
            "scan_policy_owner_scope_key must be workspace:context, "
            f"got {report.get('scan_policy_owner_scope_key')!r}"
        )
    require_string_set(report, "scan_policy_shared_scope_keys", {"global", "user:user"})
    require_bool(report, "scan_policy_implicit_current_agent_scope_added")
    require_bool(report, "scan_policy_owner_scope_included")
    require_bool(report, "scan_policy_shared_scopes_included")
    require_bool(report, "scan_policy_ready")
    locality_keys = report.get("locality_keys")
    if not isinstance(locality_keys, list) or not locality_keys:
        raise ValueError("locality_keys must be a non-empty string array")
    bad_keys = [key for key in locality_keys if not isinstance(key, str) or ":scope:" not in key]
    if bad_keys:
        raise ValueError(f"locality_keys must all include scoped colocation markers: {bad_keys}")
    if not any(":scope:agent:codex:" in key for key in locality_keys):
        raise ValueError("locality_keys must include current-agent scoped colocation")
    require_int_equal(report, "locality_key_count", expanded)
    require_int_equal(report, "peer_locality_key_count", 0)
    locality_scopes = require_string_set(
        report, "locality_scope_keys", REQUIRED_SELECTED_SCOPE_KEYS
    )
    if locality_scopes != set(report["selected_colocation_scope_keys"]):
        raise ValueError(
            "locality_scope_keys must exactly match selected_colocation_scope_keys, "
            f"got {sorted(locality_scopes)} vs {sorted(report['selected_colocation_scope_keys'])}"
        )
    if report.get("current_agent_id") != "codex":
        raise ValueError(f"current_agent_id must be 'codex', got {report.get('current_agent_id')!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "report",
        nargs="?",
        default="docs/benchmark_archives/context_multiagent_scan_20260706_summary.json",
        help="Path to the multi-agent scan JSON report.",
    )
    parser.add_argument("--min-candidates", type=int, default=8)
    parser.add_argument("--max-expanded", type=int, default=4)
    args = parser.parse_args()
    try:
        report = load_report(Path(args.report))
        validate_report(report, args.min_candidates, args.max_expanded)
    except ValueError as exc:
        return fail(str(exc))
    print(
        "validated context multi-agent scan evidence "
        f"report={args.report} candidates={report['namespace_node_candidates']} "
        f"expanded={report['event_expanded_nodes']} layers={','.join(report['scan_layers'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
