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


def validate_report(report: dict[str, Any], min_candidates: int, max_expanded: int) -> None:
    require_bool(report, "ready")
    require_bool(report, "fanout_reduced")
    require_bool(report, "layer_quota_applied")
    candidates = require_int_at_least(report, "namespace_node_candidates", min_candidates)
    expanded = require_int_at_least(report, "event_expanded_nodes", 1)
    if expanded > max_expanded:
        raise ValueError(f"event_expanded_nodes must be <= {max_expanded}, got {expanded}")
    if expanded >= candidates:
        raise ValueError(
            f"fanout did not reduce candidates: candidates={candidates} expanded={expanded}"
        )
    require_int_at_least(report, "shared_layer_quota_nodes", 4)
    require_int_at_least(report, "selected_current_agent_nodes", 1)
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
    require_int_at_least(report, "retrieved_block_count", 4)
    require_int_at_least(report, "selected_ref_count", 4)
    require_string_set(report, "scan_layers", REQUIRED_LAYERS)
    require_string_set(report, "colocation_groups", REQUIRED_COLOCATION_GROUPS)
    require_string_set(
        report,
        "colocation_scope_keys",
        {"agent:codex", "agent:claude", "user:user", "workspace:context", "global"},
    )
    locality_keys = report.get("locality_keys")
    if not isinstance(locality_keys, list) or not locality_keys:
        raise ValueError("locality_keys must be a non-empty string array")
    bad_keys = [key for key in locality_keys if not isinstance(key, str) or ":scope:" not in key]
    if bad_keys:
        raise ValueError(f"locality_keys must all include scoped colocation markers: {bad_keys}")
    if not any(":scope:agent:codex:" in key for key in locality_keys):
        raise ValueError("locality_keys must include current-agent scoped colocation")
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
