#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The two copies of `latest_async_pipeline_rows` pick the same row.

`matrixark_mcp_async_readiness` and `matrixark_mcp_dashboard` each define a function of this name,
and the tree tracks that on purpose -- `test_matrixark_a_docstring_is_not_an_import` records the
dashboard as defining its own. Two copies are allowed. Two copies that disagree about which record
is current are not.

They did disagree. The readiness copy ranked eight statuses; the dashboard copy ranked three, so
every `idle_commit_*` was unknown to it and ranked -1. Terminal outcomes therefore tied with the
`idle_commit_scheduled` they complete, the timestamp decided alone, and a task whose terminal
record carried the earlier stamp read as STILL SCHEDULED through the dashboard while the readiness
module reported it finished:

    rows                                dashboard said          readiness said
    scheduled t=200, skipped t=100      idle_commit_scheduled   idle_commit_skipped
    scheduled t=200, committed t=100    idle_commit_scheduled   idle_commit_committed

The readiness copy already carried a comment describing this exact defect being fixed THERE. The
fix reached one copy and not the other, which is the shape
`a-guard-covering-one-copy-lets-the-other-copy-keep-the-bug` is about -- so this compares BEHAVIOUR
rather than the two maps, and would catch a divergence introduced anywhere in either function.

OUT OF ORDER IS THE POINT. Both copies agree on every in-order pair, because the later timestamp
wins whatever the ranks are. An in-order fixture makes the break invisible; each pair below is run
in both timestamp directions.
"""
from __future__ import annotations

import itertools
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from matrixark_mcp_async_readiness import latest_async_pipeline_rows as readiness_latest
from matrixark_mcp_dashboard import latest_async_pipeline_rows as dashboard_latest

#: Every status either copy ranks, plus one neither does -- an unknown must be handled the same way
#: by both, and leaving it out would let them differ exactly where the original defect lived.
STATUSES = (
    "pending",
    "idle_commit_scheduled",
    "idle_commit_failed",
    "idle_commit_attempted",
    "idle_commit_committed",
    "idle_commit_skipped",
    "extraction_committed",
    "summary_completed",
    "a_status_neither_copy_knows",
)


def _row(status: str, updated: int) -> dict:
    return {"task_hash": 7, "event_id_hash": 7, "status": status, "updated_at_ms": updated}


def _picked(fn, rows) -> str:
    out = fn(list(rows))
    return str(out[0].get("status")) if out else "(none)"


class TheTwoPipelineRowRankingsAgreeTest(unittest.TestCase):

    def _pairs(self):
        """Every ordered pair of statuses, in BOTH timestamp directions."""
        for first, second in itertools.permutations(STATUSES, 2):
            for stamps in ((100, 200), (200, 100), (100, 100)):
                yield first, second, stamps

    def test_both_copies_pick_the_same_row(self) -> None:
        compared, disagreed = 0, []
        for first, second, (t1, t2) in self._pairs():
            rows = [_row(first, t1), _row(second, t2)]
            compared += 1
            a, b = _picked(readiness_latest, rows), _picked(dashboard_latest, rows)
            if a != b:
                disagreed.append("%s t=%d then %s t=%d -> readiness %s, dashboard %s"
                                 % (first, t1, second, t2, a, b))
        self.assertEqual(
            [], disagreed[:12],
            "the two copies of latest_async_pipeline_rows disagree about which record is current. "
            "A task reads one way on the dashboard and another through readiness, and the one that "
            "is wrong is whichever copy a fix has not reached yet. %d of %d pairs differ."
            % (len(disagreed), compared))
        self.assertGreater(
            compared, 100,
            "only %d pairs were compared, so this proves very little. STATUSES is the subject set; "
            "if it shrinks to nothing this passes while comparing nothing." % compared)

    def test_the_out_of_order_case_is_actually_exercised(self) -> None:
        """Vacuity control on the DIRECTION, not the count.

        Both copies agree on every in-order pair whatever their maps say, because the later stamp
        wins regardless. A suite that only ever built in-order rows would report this file green
        against the original defect, which is exactly what happened to the first probe of it."""
        terminal_first = [_row("idle_commit_committed", 100), _row("idle_commit_scheduled", 200)]
        self.assertEqual(
            "idle_commit_committed", _picked(readiness_latest, terminal_first),
            "a terminal record with the EARLIER stamp must still win on rank; if this reports "
            "idle_commit_scheduled then the ranking is not being consulted at all and the "
            "comparison above is between two copies that are both wrong.")
        self.assertEqual("idle_commit_committed", _picked(dashboard_latest, terminal_first))


if __name__ == "__main__":
    unittest.main()
