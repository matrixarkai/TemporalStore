#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The daemon can bridge over HTTP, and then it stops serializing its callers.

The daemon exists because `--serve` speaks stdio: a pipe has one reader, so one process must own
it and relay for everyone else, behind one lock. On the live one-box deployment that lock, not the
work, is what a request costs:

    queue wait   n=44,200   p50 6,352 ms   p90 434,871 ms   p99 1,167,374 ms
    actual work  n=34,290   p50    44 ms   p90   2,554 ms   p99    24,280 ms

9,910 requests spent their entire budget queueing and were abandoned without being started.

`MATRIXARK_PROXY_DAEMON_HTTP=1` starts the proxy in `--serve-http` and bridges over HTTP, which
needs no lock. The Unix socket is unchanged, so no client moves.

What these tests are FOR is the pair of claims that make that safe: the two transports return the
same answers, and the HTTP one does not queue. A test that only proved HTTP works would pass just
as happily if the two had quietly diverged.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

PROXY = os.environ.get(
    "PROXY_BIN", "/root/wt-rank/target/release/matrixark_rust_proxy"
)


def _ask(socket_path: Path, request: dict, timeout: float = 120.0) -> dict:
    """One request down the daemon's Unix socket -- the path every client uses."""
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.settimeout(timeout)
    try:
        conn.connect(str(socket_path))
        conn.sendall((json.dumps(request) + "\n").encode("utf-8"))
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = conn.recv(65536)
            if not chunk:
                break
            buf += chunk
        return json.loads(buf.decode("utf-8"))
    finally:
        conn.close()


class _Daemon:
    """A daemon in one transport, torn down on exit."""

    def __init__(self, http: bool):
        self.http = http
        self.root = tempfile.mkdtemp(prefix="daemonhttp" if http else "daemonpipe")
        self.socket_path = Path(self.root) / "d.sock"
        self.log_path = Path(self.root) / "d.log"
        self.thread = None
        self.daemon = None

    def __enter__(self):
        os.environ["TS_STANDALONE"] = "1"
        if self.http:
            os.environ["MATRIXARK_PROXY_DAEMON_HTTP"] = "1"
        else:
            os.environ.pop("MATRIXARK_PROXY_DAEMON_HTTP", None)
        import matrixark_rust_proxy_daemon as mod

        self.daemon = mod.RustProxyDaemon(
            proxy_path=Path(PROXY), socket_path=self.socket_path, log_path=self.log_path
        )
        self.thread = threading.Thread(target=self.daemon.start, daemon=True)
        self.thread.start()
        deadline = time.time() + 60
        while time.time() < deadline:
            if self.socket_path.exists():
                try:
                    _ask(self.socket_path, {"op": "metrics_prometheus"}, timeout=10)
                    return self
                except Exception:
                    pass
            time.sleep(0.1)
        raise AssertionError("daemon never answered on %s" % self.socket_path)

    def __exit__(self, *exc):
        if self.daemon is not None:
            self.daemon.stop()
        if self.thread is not None:
            self.thread.join(timeout=15)
        os.environ.pop("MATRIXARK_PROXY_DAEMON_HTTP", None)


def _volatile(response: dict) -> dict:
    """Drop only what MUST differ between two runs against two stores."""
    drop = {
        "elapsed_ms", "rust_engine_time_ms", "serialization_time_ms", "daemon_elapsed_ms",
        "daemon_work_ms", "daemon_queue_wait_ms", "daemon_transport", "root", "append_path",
        "prometheus", "cached_clients",
    }
    return {k: v for k, v in response.items() if k not in drop}


@unittest.skipUnless(os.path.exists(PROXY), "no proxy binary at %s" % PROXY)
class TheDaemonCanBridgeOverHttp(unittest.TestCase):
    def test_the_transport_is_off_by_default(self) -> None:
        """Flipping how a live deployment is reached is a deployment decision, not a code one."""
        import matrixark_rust_proxy_daemon as mod

        os.environ.pop("MATRIXARK_PROXY_DAEMON_HTTP", None)
        self.assertIsNone(mod.RustProxyDaemon._http_listen_addr())

    def test_the_flag_picks_a_bindable_port(self) -> None:
        import matrixark_rust_proxy_daemon as mod

        os.environ["MATRIXARK_PROXY_DAEMON_HTTP"] = "1"
        try:
            addr = mod.RustProxyDaemon._http_listen_addr()
        finally:
            os.environ.pop("MATRIXARK_PROXY_DAEMON_HTTP", None)
        self.assertIsNotNone(addr)
        host, port = addr
        self.assertEqual("127.0.0.1", host)
        self.assertGreater(port, 0, "port 0 would make every caller guess where to connect")

    def test_both_transports_answer_the_same(self) -> None:
        with _Daemon(http=False) as pipe:
            over_pipe = _ask(pipe.socket_path, {"op": "metrics_prometheus"})
        with _Daemon(http=True) as http:
            over_http = _ask(http.socket_path, {"op": "metrics_prometheus"})
        self.assertTrue(over_pipe.get("ok"), over_pipe)
        self.assertTrue(over_http.get("ok"), over_http)
        self.assertEqual(_volatile(over_pipe), _volatile(over_http))

    def test_the_http_bridge_says_which_transport_answered(self) -> None:
        """Without this a reader cannot tell which transport produced a number."""
        with _Daemon(http=True) as http:
            response = _ask(http.socket_path, {"op": "metrics_prometheus"})
        self.assertEqual("http", response.get("daemon_transport"))

    def test_the_http_bridge_reports_no_queue_wait(self) -> None:
        """Zero, not absent: a series charted across the change must keep its shape."""
        with _Daemon(http=True) as http:
            response = _ask(http.socket_path, {"op": "metrics_prometheus"})
        self.assertIn("daemon_queue_wait_ms", response)
        self.assertEqual(0, response["daemon_queue_wait_ms"])

    def test_concurrent_callers_do_not_queue_behind_each_other(self) -> None:
        """The whole point. Every concurrent caller must report a zero queue wait.

        Asserted on the REPORTED wait rather than on wall-clock, because this box runs other
        people's builds and a timing threshold would be measuring their load. The stdio path
        cannot produce this result: one lock, so all but the first caller wait for it.
        """
        results: list = []
        with _Daemon(http=True) as http:
            def call() -> None:
                try:
                    results.append(_ask(http.socket_path, {"op": "metrics_prometheus"}))
                except Exception as exc:  # noqa: BLE001
                    results.append({"ok": False, "error": str(exc)})

            threads = [threading.Thread(target=call) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=120)

        self.assertEqual(8, len(results), "not every caller returned")
        failed = [r for r in results if not r.get("ok")]
        self.assertEqual([], failed, "some concurrent callers failed: %s" % failed[:2])
        waits = [r.get("daemon_queue_wait_ms") for r in results]
        self.assertEqual([0] * 8, waits,
                         "a caller queued on the HTTP transport, which has no lock: %s" % waits)


    def test_the_http_path_does_not_take_the_daemon_lock(self) -> None:
        """The control, and it is deterministic rather than timed.

        Every other test here asserts that HTTP reports a zero queue wait. On a fast op the stdio
        path reports zero too -- nobody waited because nobody was slow -- so those assertions on
        their own do not distinguish the transports and would pass if the lock were still taken.

        So hold the lock and ask anyway. On HTTP the request must still complete, because
        `_call_proxy` returns before the `with self._lock:` block. On stdio the same request cannot
        complete, and that half is asserted too -- a control that only demonstrates the good case
        shows the mechanism works, not that it is the mechanism.
        """
        with _Daemon(http=True) as http:
            self.assertIsNotNone(http.daemon._http_addr, "this daemon is not on the HTTP transport")
            with http.daemon._lock:
                answered = _ask(http.socket_path, {"op": "metrics_prometheus"}, timeout=30)
            self.assertTrue(
                answered.get("ok"),
                "an HTTP request did not complete while the daemon lock was held, so the "
                "transport is still serializing through it: %s" % answered,
            )
            self.assertEqual("http", answered.get("daemon_transport"))

        with _Daemon(http=False) as pipe:
            self.assertIsNone(pipe.daemon._http_addr, "this daemon should be on the pipe")
            blocked = []

            def call() -> None:
                try:
                    blocked.append(_ask(pipe.socket_path, {"op": "metrics_prometheus"}, timeout=6))
                except Exception as exc:  # noqa: BLE001 - a timeout IS the expected result
                    blocked.append({"blocked": type(exc).__name__})

            with pipe.daemon._lock:
                worker = threading.Thread(target=call, daemon=True)
                worker.start()
                worker.join(timeout=8)
            self.assertTrue(blocked, "the stdio caller neither answered nor failed")
            self.assertNotIn(
                "ok", blocked[0],
                "a stdio request completed while the daemon lock was held, so this control is "
                "not testing what it claims: %s" % blocked[0],
            )


if __name__ == "__main__":
    unittest.main()
