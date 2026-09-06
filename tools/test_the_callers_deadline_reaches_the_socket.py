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
import os
import socket
import tempfile
import threading
import time
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
        """Four spellings, because callers build the call differently.

        `record` is the production one: `matrixark_retrieve_context_pack` passes the retrieve
        request as `record=request`. These assertions are documentation -- the test that actually
        protects the behaviour is the end-to-end one below, which does not name any of them.
        """
        self.assertEqual(5000, self.deadline_of({"deadline_ms": 5000}))
        self.assertEqual(
            5000, self.deadline_of({"record": {"deadline_ms": 5000}}),
            "the live retrieve carries its request under `record`")
        self.assertEqual(5000, self.deadline_of({"request": {"deadline_ms": 5000}}))
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


class TheLiveRetrieveDeadlineReachesTheSocketTest(unittest.TestCase):
    """Drive the real entry point, not a dict I invented.

    This is the test that was missing. The first version of this fix read the deadline from
    `kwargs`, `kwargs["request"]` and `kwargs["ranking"]` -- all three plausible, none of them the
    one `matrixark_retrieve_context_pack` actually uses, which is `record=request`. Every
    shape-based assertion passed and the fix was a no-op on the only path it was written for.

    So this test never names a carrier. It calls the retrieve entry point the hook calls, against a
    socket that never answers, and times how long it waits. If the deadline stops reaching the
    socket -- renamed key, changed call site, reverted arithmetic -- this fails.
    """

    def _hang(self, socket_path: str) -> None:
        """A daemon that accepts and then never answers."""
        ready = threading.Event()
        stop = threading.Event()
        self.addCleanup(stop.set)

        def run() -> None:
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(socket_path)
            server.listen(1)
            server.settimeout(1.0)
            ready.set()
            conn = None
            try:
                while not stop.is_set():
                    try:
                        conn, _ = server.accept()
                        break
                    except socket.timeout:
                        continue
                if conn is not None:
                    with conn:
                        conn.recv(65536)
                        stop.wait(120.0)
            except OSError:
                pass
            finally:
                server.close()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(10.0), "socket server did not come up")
        self.addCleanup(thread.join, 5.0)

    def _client(self, socket_path: str, request_timeout_ms: int):
        module = _adapters()
        previous = os.environ.get("MATRIXARK_RUST_PROXY_SOCKET")
        os.environ["MATRIXARK_RUST_PROXY_SOCKET"] = socket_path

        def restore() -> None:
            if previous is None:
                os.environ.pop("MATRIXARK_RUST_PROXY_SOCKET", None)
            else:
                os.environ["MATRIXARK_RUST_PROXY_SOCKET"] = previous

        self.addCleanup(restore)
        client = module.MatrixArkRustProxyClient(
            proxy_path="matrixark_rust_proxy",
            metaserver="local",
            namespace="ns",
            table="table",
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=request_timeout_ms,
        )
        self.assertEqual(client._proxy_socket, socket_path)
        return client

    def test_a_retrieve_gives_up_at_its_own_deadline_not_the_transport_ceiling(self) -> None:
        """The measured failure, end to end.

        The live box runs `--request-timeout-ms 300000`, so the transport ceiling there is 302s.
        A retrieve carrying a 3s deadline must not wait anywhere near it.
        """
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = os.path.join(tmp, "hung.sock")
            self._hang(socket_path)
            client = self._client(socket_path, request_timeout_ms=300000)

            started = time.monotonic()
            with self.assertRaises(Exception):
                client.matrixark_retrieve_context_pack(
                    count_key="c",
                    record_hash_key="h",
                    shard_size=1,
                    request={"deadline_ms": 3000, "scope": {}, "secondary_index_groups": []},
                )
            elapsed_s = time.monotonic() - started

        # Below: the 3s deadline plus the connect-and-framing allowance, with room for a slow
        # machine. Above: it really did wait for the deadline rather than failing to connect,
        # so this cannot pass because the socket was broken.
        self.assertGreaterEqual(
            elapsed_s, 2.5,
            "gave up before the deadline -- this passed for the wrong reason")
        self.assertLess(
            elapsed_s, 30.0,
            "a retrieve with a 3s deadline waited %.1fs; the caller's deadline is not reaching "
            "the socket, and on the live box that means 302s" % elapsed_s)



if __name__ == "__main__":
    unittest.main()
