#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The warm-proxy socket read is bounded by the call's deadline, not by a constant.

`_call_socket_json` once armed the socket with `min(2.0, request_timeout_ms / 1000)`
and never re-armed it, so a response slower than two seconds raised `TimeoutError` no
matter what `--request-timeout-ms` said. `_call_json` re-raises rather than falling back
to the process-lane path, so the call failed outright -- and a ContextPack over a large
store is always slower than two seconds, which is how hook retrieval came to fail open
with no context and nothing in the logs but "TimeoutError: timed out".

These tests drive a real Unix socket that answers late, so they exercise the timeout the
socket is actually armed with rather than asserting on the source text.
"""
from __future__ import annotations

import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matrixark_mcp_temporal_adapters as adapters  # noqa: E402
import matrixark_rust_proxy_daemon as proxy_daemon  # noqa: E402


class RustProxySocketDeadlineTest(unittest.TestCase):
    def _serve_late(self, socket_path: str, delay_s: float, response: str) -> None:
        """Accept one connection, wait, then answer -- a slow but healthy daemon."""
        ready = threading.Event()

        def run() -> None:
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(socket_path)
            server.listen(1)
            server.settimeout(60.0)
            ready.set()
            try:
                conn, _ = server.accept()
            except OSError:
                server.close()
                return
            with conn:
                try:
                    conn.recv(65536)
                    time.sleep(delay_s)
                    conn.sendall(response.encode("utf-8"))
                except OSError:
                    pass
            server.close()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(10.0), "socket server did not come up")
        self.addCleanup(thread.join, 5.0)

    def _client(self, socket_path: str, request_timeout_ms: int) -> adapters.MatrixArkRustProxyClient:
        previous = os.environ.get("MATRIXARK_RUST_PROXY_SOCKET")
        os.environ["MATRIXARK_RUST_PROXY_SOCKET"] = socket_path

        def restore() -> None:
            if previous is None:
                os.environ.pop("MATRIXARK_RUST_PROXY_SOCKET", None)
            else:
                os.environ["MATRIXARK_RUST_PROXY_SOCKET"] = previous

        self.addCleanup(restore)
        client = adapters.MatrixArkRustProxyClient(
            proxy_path="matrixark_rust_proxy",
            metaserver="local",
            namespace="ns",
            table="table",
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=request_timeout_ms,
        )
        self.assertEqual(client._proxy_socket, socket_path)
        return client

    def test_read_waits_past_two_seconds_when_the_request_timeout_allows_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = os.path.join(tmp, "slow.sock")
            self._serve_late(socket_path, delay_s=3.0, response='{"ok": true, "value": "slow-answer"}\n')
            client = self._client(socket_path, request_timeout_ms=20000)

            started = time.monotonic()
            response = client._call_json("get_string", key="k")
            elapsed_s = time.monotonic() - started

        self.assertEqual(response.get("value"), "slow-answer")
        # The answer only exists after the old fixed cap would have fired: without the
        # wait, this is the assertion that fails, and it cannot pass vacuously.
        self.assertGreaterEqual(elapsed_s, 3.0)

    def test_read_still_gives_up_at_the_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = os.path.join(tmp, "hung.sock")
            self._serve_late(socket_path, delay_s=30.0, response='{"ok": true}\n')
            client = self._client(socket_path, request_timeout_ms=100)

            started = time.monotonic()
            with self.assertRaises(adapters.MatrixArkError) as caught:
                client._call_json("get_string", key="k")
            elapsed_s = time.monotonic() - started

        self.assertIn("timed out", str(caught.exception))
        self.assertIn("get_string", str(caught.exception))
        # Bounded above by the budget -- max(2s, timeout + 2s) -- and below by it too,
        # so a connect that failed outright cannot pass as a well-behaved deadline.
        self.assertGreaterEqual(elapsed_s, 1.5)
        self.assertLess(elapsed_s, 15.0)

    def test_connect_keeps_its_short_ceiling(self) -> None:
        """A large request timeout must not make a stale socket file hang the call."""
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = os.path.join(tmp, "absent.sock")
            client = self._client(socket_path, request_timeout_ms=600000)

            started = time.monotonic()
            with self.assertRaises(OSError):
                client._call_json("get_string", key="k")
            elapsed_s = time.monotonic() - started

        self.assertLess(elapsed_s, 5.0)


class DaemonPingTimeoutTest(unittest.TestCase):
    """A busy daemon must not be mistaken for a dead one.

    Callers treat a failed ping as "no daemon" and start their own, and a second daemon
    unlinks the first one's socket on bind -- so a ping that gives up too early costs a
    cold-spawned proxy per hook invocation, not just a retry.
    """

    def _serve_health_late(self, socket_path: str, delay_s: float) -> None:
        ready = threading.Event()

        def run() -> None:
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(socket_path)
            server.listen(1)
            server.settimeout(60.0)
            ready.set()
            try:
                conn, _ = server.accept()
            except OSError:
                server.close()
                return
            with conn:
                try:
                    conn.recv(65536)
                    time.sleep(delay_s)
                    conn.sendall(b'{"ok":true,"mode":"rust_proxy_daemon"}\n')
                except OSError:
                    pass
            server.close()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(10.0), "socket server did not come up")
        self.addCleanup(thread.join, 5.0)

    def test_ping_waits_for_a_busy_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = os.path.join(tmp, "busy.sock")
            self._serve_health_late(socket_path, delay_s=3.0)

            started = time.monotonic()
            answer = proxy_daemon.ping(Path(socket_path))
            elapsed_s = time.monotonic() - started

        self.assertTrue(answer.get("ok"))
        self.assertGreaterEqual(elapsed_s, 3.0)

    def test_health_survives_an_engine_recycle(self) -> None:
        """A daemon whose engine is momentarily gone is still able to serve.

        `_ensure_proxy` starts one on the next request, so reporting "dead" here is what
        makes every caller start its own proxy -- and a second daemon unlinks this one's
        socket on bind, leaving the losers reloading the store per call.
        """
        with tempfile.TemporaryDirectory() as tmp:
            daemon = proxy_daemon.RustProxyDaemon(
                proxy_path=Path("/bin/cat"),  # executable, never actually driven here
                socket_path=Path(os.path.join(tmp, "health.sock")),
                log_path=Path(os.path.join(tmp, "daemon.log")),
            )
            daemon._proc = None
            self.assertTrue(daemon._health_ok(), "no engine yet is still a serving socket")

            # Held lock = a request in flight. Health must not queue behind it: a cold load
            # takes tens of seconds, and a ping that waits that long reads as a dead daemon.
            daemon._lock.acquire()
            try:
                started = time.monotonic()
                self.assertTrue(daemon._health_ok())
                self.assertLess(time.monotonic() - started, 1.0)
            finally:
                daemon._lock.release()

            daemon.proxy_path = Path(os.path.join(tmp, "not-a-binary"))
            self.assertFalse(daemon._health_ok(), "an engine that cannot start is not healthy")

    def test_ping_timeout_is_configurable_and_bounded(self) -> None:
        previous = os.environ.get("MATRIXARK_RUST_PROXY_PING_TIMEOUT_MS")
        self.addCleanup(
            lambda: os.environ.__setitem__("MATRIXARK_RUST_PROXY_PING_TIMEOUT_MS", previous)
            if previous is not None
            else os.environ.pop("MATRIXARK_RUST_PROXY_PING_TIMEOUT_MS", None)
        )

        os.environ.pop("MATRIXARK_RUST_PROXY_PING_TIMEOUT_MS", None)
        self.assertGreaterEqual(proxy_daemon.ping_timeout_seconds(), 5.0)

        os.environ["MATRIXARK_RUST_PROXY_PING_TIMEOUT_MS"] = "25000"
        self.assertEqual(proxy_daemon.ping_timeout_seconds(), 25.0)

        os.environ["MATRIXARK_RUST_PROXY_PING_TIMEOUT_MS"] = "not-a-number"
        self.assertGreaterEqual(proxy_daemon.ping_timeout_seconds(), 5.0)


if __name__ == "__main__":
    unittest.main()
