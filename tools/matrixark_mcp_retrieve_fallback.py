#!/usr/bin/env python3
"""Deadline fallback helpers for MatrixArk retrieval."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


def deadline_fallback_pack(
    target: Any,
    *,
    query: str,
    scope: Json,
    question_type: str,
    max_context_tokens: int,
    local_budget: Json,
    deadline_ms: int,
    started_perf: float,
    records: list[Json],
    reason: str,
    budget_source: str,
    source_role_budget_tokens: Json | None = None,
    source_role_budget_mode: str = "",
    memory_layer_budget_tokens: Json | None = None,
    memory_layer_budget_mode: str = "",
    memory_selection_policy_budget_tokens: Json | None = None,
    memory_selection_policy_budget_mode: str = "",
) -> Json:
    return target.deadline_fallback_pack(
        query=query,
        scope=scope,
        question_type=question_type,
        max_context_tokens=max_context_tokens,
        local_budget=local_budget,
        deadline_ms=deadline_ms,
        elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
        records=records,
        reason=reason,
        budget_source=budget_source,
        source_role_budget_tokens=source_role_budget_tokens,
        source_role_budget_mode=source_role_budget_mode,
        memory_layer_budget_tokens=memory_layer_budget_tokens,
        memory_layer_budget_mode=memory_layer_budget_mode,
        memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
        memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
    )
