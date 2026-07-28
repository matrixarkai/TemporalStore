#!/usr/bin/env python3
"""Pre-retrieval summary refresh and recall budget helpers."""

from __future__ import annotations

import os
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, access_scope_matches_before_scoring, now_ms, optional_object
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, access_scope_matches_before_scoring, now_ms, optional_object


def _positive_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


PRE_RETRIEVAL_SUMMARY_REFRESH = os.environ.get(
    "MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH", "0"
).strip().lower() in {"1", "true", "yes"}
PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT = _positive_int_env(
    "MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT", 2
)


def auto_source_role_budget_tokens(args: Json, ranking: Json, *, remote_budget_tokens: int) -> tuple[Json, str]:
    mode = str(args.get("source_role_budget_mode") or ranking.get("source_role_budget_mode") or "").strip().lower()
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "source_role_budget_fractions") or optional_object(ranking, "source_role_budget_fractions")
    defaults = {"assistant": 0.45, "tool": 0.35, "user": 0.60}
    budgets: Json = {}
    for role, default_fraction in defaults.items():
        raw_fraction = fractions.get(role, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        budgets[role] = max(1, int(remote_budget * fraction))
    return budgets, mode


def auto_memory_layer_budget_tokens(args: Json, ranking: Json, *, remote_budget_tokens: int) -> tuple[Json, str]:
    mode = str(args.get("memory_layer_budget_mode") or ranking.get("memory_layer_budget_mode") or "").strip().lower()
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "memory_layer_budget_fractions") or optional_object(ranking, "memory_layer_budget_fractions")
    defaults = {
        "summary": 0.30,
        "compression": 0.25,
        "same_session_event": 0.45,
        "cross_session_event": 0.25,
        "same_session_segment": 0.35,
        "cross_session_segment": 0.25,
        "profile_entity": 0.40,
    }
    budgets: Json = {}
    for layer, default_fraction in defaults.items():
        raw_fraction = fractions.get(layer, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        budgets[layer] = max(1, int(remote_budget * fraction))
    return budgets, mode


def pre_retrieval_summary_refresh_memory_layer_budget_tokens(*, remote_budget_tokens: int) -> tuple[Json, str]:
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, "pre_retrieval_summary_refresh_balanced"
    fractions = {
        "summary": 0.25,
        "compression": 0.20,
        "same_session_event": 0.45,
        "cross_session_event": 0.25,
        "same_session_segment": 0.30,
        "cross_session_segment": 0.25,
        "profile_entity": 0.45,
    }
    return {layer: max(1, int(remote_budget * fraction)) for layer, fraction in fractions.items()}, "pre_retrieval_summary_refresh_balanced"


def pre_retrieval_summary_refresh_enabled(args: Json, ranking: Json) -> bool:
    value = (
        args.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in args
        else ranking.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in ranking
        else PRE_RETRIEVAL_SUMMARY_REFRESH
    )
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "auto", "bounded"}
    return bool(value)


def pre_retrieval_summary_refresh_limit(args: Json, ranking: Json) -> int:
    raw_limit = args.get("pre_retrieval_summary_refresh_limit") or ranking.get("pre_retrieval_summary_refresh_limit") or PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT
    try:
        return max(1, int(raw_limit))
    except (TypeError, ValueError):
        return PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT


def run_pre_retrieval_summary_refresh(target: Any, args: Json, ranking: Json, *, scope: Json) -> tuple[Json, list[Json]]:
    refresh: Json = {
        "enabled": pre_retrieval_summary_refresh_enabled(args, ranking),
        "requested_limit": pre_retrieval_summary_refresh_limit(args, ranking),
        "refreshed_count": 0,
        "status": "disabled",
    }
    refreshed_records: list[Json] = []
    if not refresh["enabled"]:
        return refresh, refreshed_records
    started = time.perf_counter()
    try:
        result = target.refresh_summaries(
            {
                "scope": scope,
                "limit": int(refresh["requested_limit"]),
                "refreshed_at_ms": now_ms(),
                **(
                    {"skip_dirty_reasons": args.get("pre_retrieval_summary_refresh_skip_dirty_reasons")}
                    if isinstance(args.get("pre_retrieval_summary_refresh_skip_dirty_reasons"), list)
                    else {}
                ),
            }
        )
        refreshed_count = int(result.get("refreshed_count") or 0)
        refreshed_records = [record for record in result.get("refreshed", []) if isinstance(record, dict)]
        refresh.update(
            {
                "status": "refreshed" if refreshed_count else "no_dirty_nodes",
                "refreshed_count": refreshed_count,
                "compression_created_count": int(result.get("compression_created_count") or 0),
                "skipped_dirty_count": int(result.get("skipped_dirty_count") or 0),
                "skipped_dirty_reasons": result.get("skipped_dirty_reasons") if isinstance(result.get("skipped_dirty_reasons"), dict) else {},
                "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
            }
        )
    except Exception as exc:
        refresh.update({"status": "error", "error": str(exc)[:240], "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3)})
    return refresh, refreshed_records


def merge_refreshed_summary_records(target: Any, records: list[Json], *, retrieval_scope: Json, refreshed_records: list[Json], refresh: Json) -> list[Json]:
    if not refreshed_records and int(refresh.get("refreshed_count") or 0) <= 0:
        return records
    same_user_summary_records = list(refreshed_records)
    try:
        same_user_summary_records.extend(
            record
            for record in target.read_all()
            if isinstance(record, dict)
            and record.get("record_type") == "context_summary"
            and access_scope_matches_before_scoring(record, retrieval_scope)
        )
    except Exception:
        pass
    seen = {
        (record.get("record_type"), record.get("summary_hash") or record.get("node_hash"), tuple(record.get("node_path", [])))
        for record in records
        if isinstance(record, dict)
    }
    for record in same_user_summary_records:
        if not isinstance(record, dict) or record.get("record_type") != "context_summary":
            continue
        identity = (record.get("record_type"), record.get("summary_hash") or record.get("node_hash"), tuple(record.get("node_path", [])))
        if identity in seen:
            continue
        records.append(record)
        seen.add(identity)
    return records
