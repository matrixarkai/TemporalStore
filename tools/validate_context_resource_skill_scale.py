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


def require_string_set(report: dict[str, Any], field: str, required: set[str]) -> None:
    value = report.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field} must be a string array")
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"{field} missing required entries: {missing}")


def validate_report(report: dict[str, Any], min_sources: int, max_expanded: int) -> None:
    require_bool(report, "ready")
    require_bool(report, "multi_agent_scan_ready")
    require_bool(report, "fanout_ready")
    require_bool(report, "secondary_index_ready")
    total = require_int_at_least(report, "total_source_count", min_sources)
    accepted = require_int_at_least(report, "accepted_sources", min_sources)
    if accepted != total:
        raise ValueError(f"accepted_sources must equal total_source_count, got {accepted}/{total}")
    require_int_equal(report, "failed_sources", 0)
    candidates = require_int_at_least(report, "fanout_namespace_node_candidates", min_sources)
    expanded = require_int_at_least(report, "fanout_event_expanded_nodes", 1)
    if expanded > max_expanded:
        raise ValueError(f"fanout_event_expanded_nodes must be <= {max_expanded}, got {expanded}")
    if expanded >= candidates:
        raise ValueError(f"fanout did not reduce candidates: {expanded}/{candidates}")
    require_int_at_least(report, "fanout_selected_current_agent_nodes", 1)
    require_int_at_least(report, "fanout_peer_agent_nodes", 1)
    require_int_equal(report, "fanout_selected_peer_agent_nodes", 0)
    require_int_at_least(report, "fanout_skipped_peer_agent_nodes", 1)
    require_bool(report, "fanout_peer_agent_limit_applied")
    require_int_at_least(report, "fanout_selected_user_shared_nodes", 1)
    require_int_at_least(report, "fanout_selected_workspace_shared_nodes", 1)
    require_int_at_least(report, "fanout_selected_global_shared_nodes", 1)
    require_int_at_least(report, "fanout_shared_layer_quota_nodes", 4)
    require_bool(report, "fanout_layer_quota_applied")
    require_int_at_least(report, "retrieved_block_count", 8)
    require_int_at_least(report, "selected_ref_count", 8)
    require_string_set(report, "fanout_scan_layers", REQUIRED_LAYERS)
    require_string_set(report, "fanout_colocation_groups", REQUIRED_GROUPS)
    require_string_set(report, "fanout_colocation_scope_keys", REQUIRED_SCOPE_KEYS)


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
