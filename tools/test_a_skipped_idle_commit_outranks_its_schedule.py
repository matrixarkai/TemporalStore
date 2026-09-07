#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every terminal idle-commit outcome supersedes the schedule it completes.

`latest_async_pipeline_rows` folds a task's rows to one, ranking by status and breaking ties on
time. An unranked status does not sort low -- it sorts at -1, BELOW `idle_commit_scheduled`, so a
completion written after its schedule loses to it and the task reads as scheduled forever.

That is not hypothetical: `idle_commit_skipped` was missing from the map while every sibling
outcome was present, and it is the outcome the drain emits whenever `session_commit` declines --
which is most of them.

Asserted per status rather than by comparing the map to a list, so a new outcome has to behave
rather than be remembered.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

from matrixark_mcp_async_readiness import latest_async_pipeline_rows  # noqa: E402

# Every status the drain can write as the END of an idle-commit attempt.
TERMINAL_OUTCOMES = ("idle_commit_committed", "idle_commit_skipped", "idle_commit_failed",
                     "idle_commit_attempted")


def fold(*statuses: str) -> str:
    """The status `latest_async_pipeline_rows` keeps, given rows in the order written."""
    rows = [{"task_hash": 1, "status": status, "updated_at_ms": 100 * (index + 1)}
            for index, status in enumerate(statuses)]
    return str(latest_async_pipeline_rows(rows)[0]["status"])


class ASkippedIdleCommitOutranksItsScheduleTest(unittest.TestCase):
    def test_every_terminal_outcome_supersedes_the_schedule(self) -> None:
        for outcome in TERMINAL_OUTCOMES:
            self.assertEqual(
                outcome, fold("idle_commit_scheduled", outcome),
                f"{outcome} written after its schedule must win; if it is missing from the rank "
                f"map it sorts at -1, below the schedule, and the task reads as still scheduled")

    def test_a_schedule_does_not_supersede_an_outcome(self) -> None:
        """The other direction, so the fix cannot be "rank everything equally"."""
        for outcome in TERMINAL_OUTCOMES:
            self.assertEqual(outcome, fold(outcome, "idle_commit_scheduled"),
                             f"a re-schedule row must not undo {outcome}")

    def test_the_fold_still_discriminates(self) -> None:
        """The floor. If the fold returned its input unchanged both assertions above would pass."""
        self.assertEqual("summary_completed", fold("pending", "summary_completed"))
        self.assertEqual(1, len(latest_async_pipeline_rows(
            [{"task_hash": 9, "status": "pending", "updated_at_ms": 1},
             {"task_hash": 9, "status": "extraction_committed", "updated_at_ms": 2}])))


if __name__ == "__main__":
    unittest.main()
