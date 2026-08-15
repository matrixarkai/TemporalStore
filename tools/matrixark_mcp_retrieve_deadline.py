#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Deadline and stage-latency tracking for MatrixArk retrieval."""

from __future__ import annotations

from collections.abc import Callable
import time

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json

try:
    from tools import matrixark_mcp_retrieve_planning as retrieve_planning_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_planning as retrieve_planning_helpers


class RetrievalDeadlineTracker:
    def __init__(
        self,
        *,
        started_perf: float,
        deadline_ms: int,
        stage_budgets_ms: dict[str, float],
        explicit_stage_budgets: set[str],
        observe_latency: Callable[[str, float], None],
    ) -> None:
        self.started_perf = started_perf
        self.deadline_ms = deadline_ms
        self.stage_budgets_ms = stage_budgets_ms
        self.explicit_stage_budgets = explicit_stage_budgets
        self.observe_latency = observe_latency
        self.stage_latencies_ms: dict[str, float] = {}

    def elapsed_ms(self) -> float:
        return round((time.perf_counter() - self.started_perf) * 1000.0, 3)

    def deadline_exceeded(self) -> bool:
        return self.deadline_ms > 0 and self.elapsed_ms() >= self.deadline_ms

    def finish_stage(self, stage: str, started: float) -> float:
        elapsed = round((time.perf_counter() - started) * 1000.0, 3)
        self.stage_latencies_ms[stage] = elapsed
        self.observe_latency(f"retrieval_{stage}", elapsed)
        return elapsed

    def stage_budget_snapshot(self) -> Json:
        return retrieve_planning_helpers.stage_budget_snapshot(
            stage_budgets_ms=self.stage_budgets_ms,
            stage_latencies_ms=self.stage_latencies_ms,
            explicit_stage_budgets=self.explicit_stage_budgets,
            deadline_ms=self.deadline_ms,
        )
