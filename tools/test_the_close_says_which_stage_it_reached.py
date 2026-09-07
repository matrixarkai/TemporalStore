#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A close that runs out of budget has to say how far it got.

`close_server_best_effort` abandons the close thread when the budget runs out, so its return value
never arrives and the caller can only report that the budget ran out. That line has been written
10,105 times in the live debug log, every one of them at 750ms, and not one of them said which
stage was holding it.

It matters because the budget is spent IN ORDER -- thread joins, then the adapter close, then the
audit drain -- and the drain is last. `CLOSE_JOIN_BUDGET_SHARE` caps the joins for exactly that
reason: "stops a poller that will not stop from consuming everything and leaving the flushes with
nothing". The stage after it is handed `remaining()` uncapped.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_shutdown as shutdown  # noqa: E402


class _Queue:
    def __init__(self) -> None:
        self.drained_with: float | None = None

    def drain(self, budget_s: float) -> None:
        self.drained_with = budget_s


class _Adapter:
    def __init__(self, blocks_for: float) -> None:
        self.blocks_for = blocks_for
        self.closed_with: float | None = None

    def close(self, timeout_s: float | None = None) -> None:
        self.closed_with = timeout_s
        time.sleep(self.blocks_for)


class _Server:
    """The attributes close_server_within_budget touches, and nothing else."""

    def __init__(self, adapter_blocks_for: float = 0.0) -> None:
        self._summary_stop = threading.Event()
        self._stream_materialize_stop = threading.Event()
        self._summary_thread = None
        self._stream_materialize_thread = None
        self.adapter = _Adapter(adapter_blocks_for)
        self._audit_queue = _Queue()


class TheCloseSaysWhichStageItReachedTest(unittest.TestCase):

    def test_a_close_that_finishes_records_the_last_stage(self) -> None:
        server = _Server()
        shutdown.close_server_within_budget(server, 0.75)
        progress = getattr(server, "_close_progress", None)
        self.assertIsNotNone(progress, "the close recorded no progress at all")
        self.assertEqual(
            "audit_drained", progress["stage"],
            "a close with nothing to wait for must reach the last stage: %r" % (progress,))
        self.assertEqual(750.0, progress["budget_ms"])

    def test_the_stage_is_recorded_as_it_goes_not_at_the_end(self) -> None:
        """The point of the record is to survive a caller that gives up, so it cannot be written
        once at the end -- that is exactly the case where it never gets written."""
        seen = []
        server = _Server()

        # Observed from inside the LAST stage: the drain runs after everything else, so if the
        # progress were written once at the end, the stage visible here would still be missing.
        def record(budget_s: float) -> None:
            seen.append(getattr(server, "_close_progress", {}).get("stage"))

        server._audit_queue.drain = record
        shutdown.close_server_within_budget(server, 0.75)
        self.assertEqual(
            ["adapter_closed"], seen,
            "at the drain the close should already have recorded reaching adapter_closed: %r"
            % (seen,))

    def test_an_overrunning_adapter_leaves_the_audit_drain_nothing(self) -> None:
        """CHARACTERISATION, not an endorsement.

        The joins are capped so they cannot starve what follows. The adapter close is not: it is
        handed `remaining()`, so one that overruns leaves the drain with zero budget -- the same
        starvation, one stage further down. This pins the behaviour so a fix is visible rather
        than silent.

        If this assertion starts failing because the drain now gets a floor, that is the fix
        landing: update this test, do not restore the zero.
        """
        server = _Server(adapter_blocks_for=0.30)
        shutdown.close_server_within_budget(server, 0.20)
        self.assertEqual(
            0.0, server._audit_queue.drained_with,
            "an adapter that outruns the whole budget currently leaves the drain nothing")
        self.assertGreater(
            server._close_progress["elapsed_ms"], server._close_progress["budget_ms"],
            "the close overran its own budget, which is what the caller reports as a timeout")


if __name__ == "__main__":
    unittest.main()
