#!/usr/bin/env python3
"""Retrieval planning helpers shared by MatrixArk adapters."""

from __future__ import annotations

import os

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError, optional_object
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError, optional_object


RETRIEVAL_STAGE_NAMES = ["query_understanding", "candidate_fetch", "node_traversal", "rerank_score", "pack", "audit"]


def retrieval_deadline_ms(args: Json, ranking: Json) -> int:
    raw_deadline_ms = args.get(
        "deadline_ms",
        ranking.get("deadline_ms", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", 0)),
    )
    try:
        return int(raw_deadline_ms or 0)
    except (TypeError, ValueError):
        raise MatrixArkError("deadline_ms must be an integer")


def default_stage_budgets(deadline_ms: int) -> dict[str, int]:
    if deadline_ms > 0:
        return {
            "query_understanding": max(25, int(deadline_ms * 0.15)),
            "candidate_fetch": max(25, int(deadline_ms * 0.20)),
            "node_traversal": max(25, int(deadline_ms * 0.15)),
            "rerank_score": max(25, int(deadline_ms * 0.30)),
            "pack": max(25, int(deadline_ms * 0.15)),
            "audit": max(10, int(deadline_ms * 0.05)),
        }
    return {
        "query_understanding": 500,
        "candidate_fetch": 750,
        "node_traversal": 500,
        "rerank_score": 1000,
        "pack": 500,
        "audit": 250,
    }


def retrieval_stage_budgets(args: Json, ranking: Json, *, deadline_ms: int) -> tuple[dict[str, int], Json]:
    explicit_stage_budgets = optional_object(args, "stage_budgets_ms") or optional_object(ranking, "stage_budgets_ms")
    defaults = default_stage_budgets(deadline_ms)
    stage_budgets_ms: dict[str, int] = {}
    for stage in RETRIEVAL_STAGE_NAMES:
        value = explicit_stage_budgets.get(stage, ranking.get(f"{stage}_budget_ms", defaults[stage]))
        if not isinstance(value, int) or value < 0:
            raise MatrixArkError(f"stage budget for {stage} must be a non-negative integer")
        stage_budgets_ms[stage] = value
    return stage_budgets_ms, explicit_stage_budgets


def stage_budget_snapshot(
    *,
    stage_budgets_ms: dict[str, int],
    stage_latencies_ms: dict[str, float],
    explicit_stage_budgets: Json,
    deadline_ms: int,
) -> Json:
    stages = {
        stage: {
            "budget_ms": stage_budgets_ms[stage],
            "elapsed_ms": round(float(stage_latencies_ms.get(stage, 0.0)), 3),
            "over_budget": bool(
                stage_budgets_ms[stage] > 0
                and float(stage_latencies_ms.get(stage, 0.0)) > stage_budgets_ms[stage]
            ),
        }
        for stage in RETRIEVAL_STAGE_NAMES
    }
    return {
        "enabled": True,
        "source": "explicit" if explicit_stage_budgets else ("deadline_derived" if deadline_ms > 0 else "defaults"),
        "stages": stages,
        "over_budget_stages": [stage for stage, row in stages.items() if row["over_budget"]],
    }
