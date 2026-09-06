# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A close budget is a total, and a background join may not spend all of it.

`MatrixArkMcpServer.close(timeout_s)` runs four sequential waits. Each used to receive the caller's
FULL budget, so a close could need four times what its caller was willing to wait -- and the caller
waits once, then abandons a daemon thread.

That is not hypothetical. On the live box every one of 2,811 hook closes hit its 750 ms budget
exactly (min = p50 = p90 = max = 750): the first wait, a join against a 1000 ms-interval summary
poller, spent the whole budget, and the two steps that FLUSH -- the adapter close and the audit
drain -- were reached only after the caller had given up.

So the two things worth pinning are:

  * a stuck background thread costs its bounded share, not the whole budget;
  * the flushing steps are still reached, and close still returns inside the budget.

Each is asserted with a positive control, because "close returned quickly" is equally true of a
close that did nothing at all.
"""
import tempfile
import threading
import time
import unittest
from pathlib import Path

from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
from tools.matrixark_mcp_server import MatrixArkMcpServer


class CloseBudgetIsATotalTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.log = Path(self._dir.name) / "events.jsonl"
        self.adapter = MatrixArkLocalAdapter(self.log)
        self.adapter.append({"record_type": "context_event", "event_id": "e-1", "content": "seed"})
        self.server = MatrixArkMcpServer(self.adapter, access_mode="dev")
        self.addCleanup(self._stop_everything)

        self.calls: list[str] = []
        self.budgets: dict[str, float] = {}

        real_adapter_close = self.adapter.close

        def recording_adapter_close(*, timeout_s: float = 5.0) -> None:
            self.calls.append("adapter_close")
            self.budgets["adapter_close"] = timeout_s
            real_adapter_close(timeout_s=timeout_s)

        self.adapter.close = recording_adapter_close        # type: ignore[method-assign]

        real_drain = self.server._audit_queue.drain

        def recording_drain(timeout_s: float) -> None:
            self.calls.append("audit_drain")
            self.budgets["audit_drain"] = timeout_s
            real_drain(timeout_s)

        self.server._audit_queue.drain = recording_drain     # type: ignore[method-assign]

    def _stop_everything(self) -> None:
        self._release.set() if hasattr(self, "_release") else None

    def _wedge_the_summary_thread(self, hold_s: float = 5.0) -> None:
        """Stand in a thread that ignores the stop signal, the way a poller mid-request does."""
        self._release = threading.Event()

        def stubborn() -> None:
            self._release.wait(hold_s)

        thread = threading.Thread(target=stubborn, name="stubborn-poller", daemon=True)
        thread.start()
        self.server._summary_thread = thread
        self.addCleanup(self._release.set)

    def test_a_stuck_background_thread_does_not_spend_the_whole_budget(self):
        self._wedge_the_summary_thread()
        budget = 0.75

        started = time.monotonic()
        self.server.close(timeout_s=budget)
        elapsed = time.monotonic() - started

        self.assertLessEqual(
            elapsed, budget * 1.5,
            "close took %.2f s against a %.2f s budget; a stuck join is still spending it all"
            % (elapsed, budget))
        # Positive control: the thread really was stuck for the whole close.
        self.assertTrue(self.server._summary_thread.is_alive(),
                        "the stand-in thread finished on its own, so nothing was being wedged")

    def test_the_flushing_steps_are_still_reached(self):
        self._wedge_the_summary_thread()
        self.server.close(timeout_s=0.75)

        self.assertIn("adapter_close", self.calls,
                      "the adapter was never flushed: the join consumed the budget")
        self.assertIn("audit_drain", self.calls,
                      "audit writes were never drained: the join consumed the budget")
        self.assertEqual(["adapter_close", "audit_drain"], self.calls)

    def test_the_flushes_get_a_real_share_not_a_leftover_of_zero(self):
        """Reaching them is not enough -- they have to be given time to do anything."""
        self._wedge_the_summary_thread()
        budget = 0.75
        self.server.close(timeout_s=budget)

        self.assertGreater(
            self.budgets.get("adapter_close", 0.0), 0.0,
            "the adapter close was called with no time left, which is the same as skipping it")
        self.assertLessEqual(self.budgets["adapter_close"], budget)

    def test_a_slow_flush_is_bounded_by_what_is_LEFT_of_the_budget(self):
        """The step that spends its budget is what distinguishes a total from a per-step allowance.

        With a fast adapter the two are indistinguishable: each step returns immediately, so handing
        every one of them the whole budget costs nothing and the close still looks quick. It is only
        when a step actually USES its allowance that a per-step budget overruns the caller -- which
        is exactly the live case, where the flush talks to a daemon over a socket.
        """
        self._wedge_the_summary_thread()
        budget = 0.75

        def slow_adapter_close(*, timeout_s: float = 5.0) -> None:
            self.calls.append("adapter_close")
            self.budgets["adapter_close"] = timeout_s
            time.sleep(min(timeout_s, 2.0))          # spends whatever it was given

        self.adapter.close = slow_adapter_close      # type: ignore[method-assign]

        self.server.close(timeout_s=budget)

        self.assertIn("adapter_close", self.calls)
        # Asserted on the BUDGET HANDED OVER, not on the clock. The elapsed difference between a
        # total and a per-step allowance is only the slice the join took, so a wall-clock bound
        # tight enough to catch it would be tight enough to flake. What cannot be fudged is the
        # number: after the join has spent part of the budget, a flush that still receives the
        # WHOLE of it was given a per-step allowance.
        self.assertLess(
            self.budgets["adapter_close"], budget,
            "the flush was handed the whole %.2f s budget after the join had already spent part "
            "of it, so the budget is being treated as per-step rather than as a total" % budget)

    def test_a_clean_close_still_runs_every_step(self):
        """With nothing wedged, the ordinary path is unchanged."""
        self.server.close(timeout_s=0.75)
        self.assertEqual(["adapter_close", "audit_drain"], self.calls)


if __name__ == "__main__":
    unittest.main()
