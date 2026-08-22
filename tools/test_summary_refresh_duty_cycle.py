#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The background summary refresher must never occupy the shared proxy lane.

A refresh pass is O(store): it reads the whole record log and writes the refreshed summaries
back through the SAME proxy lane the request path uses. In `shared_process_mode` that lane is a
single process behind a single permit shared by every write, read and control op, so a loop that
runs a pass on a fixed interval starves the request path as soon as one pass costs longer than
the interval -- and it never recovers, because the store only grows.

Measured on a single-worker gateway over the record-log backend, 16 ingests then 8 retrieves:

    fixed interval (before)   ingest 391 -> 22392 ms, retrieve: 3 ok then a 120s hang and
                              every later retrieve rejected on lane backpressure at 40s
    cost-proportional (after) ingest 699 ->  2808 ms, retrieve: 8/8 ok, 229-760 ms

Both arms ran the refresher at the shipping default interval of 1000 ms; the only difference is
the delay between passes.
"""
from __future__ import annotations

import sys
import tempfile
import time
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

# The tools modules are importable both as `tools.<name>` and bare `<name>`, and the two are
# DISTINCT module objects with separate globals. Patching the knobs on the wrong one silently
# does nothing -- the test then asserts against the shipping defaults and "passes" for the wrong
# reason. Resolve the module that actually supplied the function under test.
summary_runtime = sys.modules[mcp.next_summary_refresh_delay_s.__module__]


class _RecordingStop:
    """Stands in for the loop's stop Event, recording the delay it is asked to wait each time."""

    def __init__(self, stop_after: int) -> None:
        self.delays: list[float] = []
        self._stop_after = stop_after

    def wait(self, timeout: float | None = None) -> bool:
        self.delays.append(float(timeout or 0.0))
        # Returning True means "stopped", which ends the loop.
        return len(self.delays) > self._stop_after

    def set(self) -> None:  # pragma: no cover - only to complete the Event interface
        pass

    def clear(self) -> None:  # pragma: no cover - only to complete the Event interface
        pass


class SummaryRefreshDutyCycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self._old_duty = summary_runtime.SUMMARY_REFRESH_MAX_DUTY
        self._old_cap = summary_runtime.SUMMARY_REFRESH_MAX_BACKOFF_MS

    def tearDown(self) -> None:
        summary_runtime.SUMMARY_REFRESH_MAX_DUTY = self._old_duty
        summary_runtime.SUMMARY_REFRESH_MAX_BACKOFF_MS = self._old_cap

    def _server(self, tmpdir: str) -> mcp.MatrixArkMcpServer:
        adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        self.addCleanup(server.close, timeout_s=1.0)
        return server

    # ---- the delay itself ----------------------------------------------------------------

    def test_cheap_pass_keeps_the_configured_interval(self) -> None:
        """The common case must be unchanged: a fast pass still runs every interval."""
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self._server(tmpdir)
            server._summary_refresh_interval_s = 1.0
            summary_runtime.SUMMARY_REFRESH_MAX_DUTY = 0.2
            # 0.05s of work at 20% duty needs only 0.2s idle, which is under the interval.
            self.assertEqual(1.0, server._next_summary_refresh_delay_s(0.05))

    def test_expensive_pass_backs_off_in_proportion_to_its_cost(self) -> None:
        """This is the regression: a slow pass must buy a proportionally long yield."""
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self._server(tmpdir)
            server._summary_refresh_interval_s = 1.0
            summary_runtime.SUMMARY_REFRESH_MAX_DUTY = 0.2
            # 10s of work at 20% duty implies 40s idle after it (10 / 50 == 20%).
            self.assertAlmostEqual(40.0, server._next_summary_refresh_delay_s(10.0), places=6)

    def test_backoff_is_capped(self) -> None:
        """A pathologically slow pass must not park the refresher effectively forever."""
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self._server(tmpdir)
            server._summary_refresh_interval_s = 1.0
            summary_runtime.SUMMARY_REFRESH_MAX_DUTY = 0.2
            summary_runtime.SUMMARY_REFRESH_MAX_BACKOFF_MS = 30_000
            self.assertEqual(30.0, server._next_summary_refresh_delay_s(10_000.0))

    def test_disabled_refresher_stays_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self._server(tmpdir)
            server._summary_refresh_interval_s = 0.0
            self.assertEqual(0.0, server._next_summary_refresh_delay_s(10.0))

    def test_duty_outside_zero_to_one_falls_back_to_the_interval(self) -> None:
        """A misconfigured duty must degrade to the old behaviour, not to a divide-by-zero."""
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self._server(tmpdir)
            server._summary_refresh_interval_s = 2.0
            for bad in (0.0, 1.0, -0.5, 4.0):
                summary_runtime.SUMMARY_REFRESH_MAX_DUTY = bad
                self.assertEqual(2.0, server._next_summary_refresh_delay_s(10.0), f"duty={bad}")

    # ---- the loop uses it ----------------------------------------------------------------

    def test_loop_reschedules_from_the_cost_of_the_last_pass(self) -> None:
        """The loop must feed the observed pass cost back into its own delay.

        Without this the loop asks for the fixed interval every time, which is exactly what let
        a slow pass run back-to-back and hold the lane.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            server = self._server(tmpdir)
            # Stop the real background thread; drive the loop synchronously instead.
            server._summary_stop.set()
            server._summary_refresh_interval_s = 0.01
            summary_runtime.SUMMARY_REFRESH_MAX_DUTY = 0.2

            pass_s = 0.2

            def slow_refresh(_args):
                time.sleep(pass_s)
                return {"refreshed_count": 0}

            server.adapter.refresh_summaries = slow_refresh  # type: ignore[assignment]
            stop = _RecordingStop(stop_after=2)
            server._summary_stop = stop  # type: ignore[assignment]

            server._summary_refresh_loop()

            # First wait is the configured interval (nothing has run yet); every later wait is
            # derived from the pass that just finished: 0.2s at 20% duty implies ~0.8s idle.
            self.assertEqual(0.01, stop.delays[0])
            self.assertGreaterEqual(len(stop.delays), 2)
            for delay in stop.delays[1:]:
                self.assertGreater(
                    delay, pass_s,
                    "a pass costing %.2fs must yield for longer than it ran, got %.3fs"
                    % (pass_s, delay),
                )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
