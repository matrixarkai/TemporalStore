# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The caller's deadline reaches the socket, and cannot lengthen the wait.

Measured on the live one-box: 4 of 101 traced retrievals ended

    native_live exit1_retriever_raised=Rust TemporalStore matrixark_retrieve_context_pack daemon
    timed out waiting for response from /tmp/matrixark-rust-proxy-shared-live.sock after 62.0s

62.0 is `max(2.0, request_timeout_ms / 1000 + 2.0)` with the 60s transport default. It is the
TRANSPORT ceiling -- how long any single call to the store may take -- and it was being used as the
budget for a call whose caller had named a much smaller number. The Claude hook budgets 5s for a
retrieve, so a call with 5s waited 62: twelve times its own budget, and the turn showed it.

The retrieve request already carried `deadline_ms`; `matrixark_local_adapter_retrieve` puts it
there. It simply was not read at this layer.

WHAT THE FIX MUST NOT DO, and why the floor exists. The read timeout was once a constant
`min(2.0, ...)` that no `--request-timeout-ms` could raise, so any response slower than two seconds
became a timeout and hook retrieval came back empty with nothing in the logs but
"TimeoutError: timed out". A cap that can drive the budget to zero would be that bug again, so the
budget never goes below `SOCKET_MINIMUM_BUDGET_S` and always carries `SOCKET_OVERHEAD_S` for
connect and framing.

And the cap only ever REDUCES. A caller asking for longer than the transport allows still gets the
transport ceiling, because that is the promise the transport makes; a deadline is permission to
stop early, not permission to wait longer.
"""
from __future__ import annotations

import importlib
import unittest


def _adapters():
    try:
        return importlib.import_module("tools.matrixark_mcp_temporal_adapters")
    except ImportError:
        return importlib.import_module("matrixark_mcp_temporal_adapters")


class TheCallersDeadlineReachesTheSocketTest(unittest.TestCase):

    def setUp(self) -> None:
        self.module = _adapters()
        self.budget = self.module._socket_budget_seconds
        self.deadline_of = self.module._caller_deadline_ms

    def test_the_transport_ceiling_is_unchanged_when_no_deadline_is_given(self) -> None:
        """Control. A caller that names no deadline must keep exactly what it had."""
        self.assertAlmostEqual(62.0, self.budget(60000, 0), places=6)
        self.assertAlmostEqual(
            62.0, self.budget(60000, -1), places=6,
            msg="a negative deadline is not a deadline and must not shorten the wait")

    def test_a_caller_deadline_shortens_the_wait(self) -> None:
        """The measured case: a 5s hook retrieve waited 62s."""
        self.assertAlmostEqual(
            7.0, self.budget(60000, 5000), places=6,
            msg="a 5s retrieve still waits the transport ceiling")
        self.assertLess(
            self.budget(60000, 5000), 62.0,
            "the caller's deadline does not reach the socket")

    def test_the_cap_only_reduces(self) -> None:
        """A deadline is permission to stop early, not permission to wait longer."""
        self.assertAlmostEqual(
            4.0, self.budget(2000, 60000), places=6,
            msg="a caller asking for 60s got more than the 2s transport ceiling allows")

    def test_no_deadline_however_small_can_starve_the_call(self) -> None:
        """The failure this must not recreate.

        The read timeout was once a constant `min(2.0, ...)` that no `--request-timeout-ms` could
        raise, so any response slower than two seconds became a timeout and hook retrieval came
        back empty. A cap that subtracted rather than added -- `min(ceiling, deadline/1000)` --
        would be that bug again, reached by any caller with a deadline under two seconds.
        """
        for deadline_ms in (1, 10, 100, 250, 999, 1000, 1999):
            self.assertGreaterEqual(
                self.budget(60000, deadline_ms), self.module.SOCKET_OVERHEAD_S,
                "a %dms deadline left less than the connect-and-framing allowance, so the call "
                "cannot complete a handshake and every such caller times out" % deadline_ms)

    def test_the_ceiling_itself_never_goes_below_the_minimum(self) -> None:
        """The transport side of the same property, which a 0 timeout would otherwise break."""
        for request_timeout_ms in (0, 1, 500):
            self.assertGreaterEqual(
                self.budget(request_timeout_ms, 0), self.module.SOCKET_MINIMUM_BUDGET_S,
                "a %dms transport timeout produced a budget below the minimum" % request_timeout_ms)

    def test_the_deadline_is_found_where_callers_put_it(self) -> None:
        """Three shapes, because three callers build the call differently."""
        self.assertEqual(5000, self.deadline_of({"deadline_ms": 5000}))
        self.assertEqual(
            5000, self.deadline_of({"request": {"deadline_ms": 5000}}),
            "the retrieve path puts it in the request body, which is the one that mattered")
        self.assertEqual(250, self.deadline_of({"ranking": {"deadline_ms": 250}}))

    def test_no_deadline_reads_as_no_deadline(self) -> None:
        """0 is the documented "no deadline" value and must not read as "expire immediately"."""
        for shape in ({"deadline_ms": 0}, {"request": {}}, {}, {"deadline_ms": None},
                      {"deadline_ms": "soon"}, {"request": None}):
            self.assertEqual(
                0, self.deadline_of(shape),
                "%r should read as no deadline; anything else caps the socket by accident" % shape)

    def test_the_socket_call_site_passes_the_deadline(self) -> None:
        """The wiring, not just the arithmetic.

        A parameter nothing passes changes nothing, and a signature check cannot tell the
        difference -- so assert the CALL SITE, which is what the live path actually executes.
        """
        import ast
        import inspect

        calls = [
            node for node in ast.walk(ast.parse(inspect.getsource(self.module)))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_call_socket_json"
        ]
        self.assertTrue(calls, "no call to the socket reader; this guard is testing nothing")
        for call in calls:
            keywords = {kw.arg: kw.value for kw in call.keywords}
            self.assertIn(
                "caller_deadline_ms", keywords,
                "the call to _call_socket_json at line %d does not pass the caller's deadline, "
                "so that call still waits the full transport ceiling" % call.lineno)
            self.assertNotEqual(
                "0", ast.unparse(keywords["caller_deadline_ms"]),
                "the deadline is passed as a literal 0, which is the pre-fix behaviour spelled "
                "with an extra argument")


if __name__ == "__main__":
    unittest.main()
