#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The transport keeps its connection instead of opening one per call.

Every call built a fresh TCP connection and closed it in a ``finally``, so a mem0 ``batch_update``
of 50 memories cost 50 connections, and 20 ``get`` calls cost 20 more. Measured against a server
that was willing to hold the connection open the whole time: 1.00 connections per request.

Reuse is only safe if the failure it introduces is handled, and it does introduce one. A pooled
socket can be closed by the server at any moment -- an idle timeout, a restart -- and the client
cannot know until it writes and the write fails. So a REUSED connection that fails is tried once
more on a fresh socket, while a FRESH one that fails is raised immediately: that failure is real,
and retrying it only doubles the wait before the caller hears about it.

The tests below pin both halves, plus the case that makes pooling unsafe if it is missed: a server
that answers with keep-alive headers and then closes anyway.

One consequence is worth stating rather than discovering: the pool is per thread, so a thread that
exits leaves its connection to be closed when the object is collected instead of at exit. Long-lived
worker threads -- what a server actually runs -- never reach that, and a one-shot thread's socket
still closes, just not deterministically.
"""
from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:  # top-level (run from tools/) ...
    import matrixark_ingest_client as client
except ImportError:  # ... or package path.
    from tools import matrixark_ingest_client as client  # type: ignore


class _Server:
    """A counting HTTP/1.1 server. ``drop_after_reply`` closes without saying so in the response."""

    def __init__(self, *, status=200, close_header=False, drop_after_reply=False):
        self.connections = 0
        self.requests = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _reply(self):
                outer.requests += 1
                body = json.dumps({"ok": True, "found": status != 404}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                if close_header:
                    self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                if close_header:
                    self.close_connection = True
                elif drop_after_reply:
                    # The response says keep-alive; the server closes anyway. This is what an idle
                    # timeout looks like from the client, and the client cannot see it coming.
                    self.close_connection = True

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                if n:
                    self.rfile.read(n)
                self._reply()

            do_GET = do_POST

            def log_message(self, *a):
                pass

        class Counting(ThreadingHTTPServer):
            daemon_threads = True

            def get_request(self):
                pair = super().get_request()
                outer.connections += 1
                return pair

        self._srv = Counting(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self._srv.server_address[1]
        threading.Thread(target=self._srv.serve_forever, daemon=True).start()

    def close(self):
        self._srv.shutdown()
        self._srv.server_close()


def _clear_pool():
    """Each test starts with an empty pool, or it inherits another test's socket."""
    cache = getattr(client._POOL, "connections", None)
    if cache:
        for conn in list(cache.values()):
            try:
                conn.close()
            except Exception:
                pass
        cache.clear()


class TransportReusesItsConnection(unittest.TestCase):
    def setUp(self):
        _clear_pool()
        self.addCleanup(_clear_pool)

    def test_many_calls_cost_one_connection(self):
        srv = _Server()
        self.addCleanup(srv.close)
        for i in range(30):
            client._post_json(srv.url, None, "/v1/update", {"memory_id": "m%d" % i}, 10.0)
        self.assertEqual(srv.requests, 30)
        self.assertEqual(srv.connections, 1, "30 requests should share one connection")

    def test_a_server_that_says_close_is_not_pooled(self):
        srv = _Server(close_header=True)
        self.addCleanup(srv.close)
        for i in range(5):
            out = client._post_json(srv.url, None, "/v1/update", {"i": i}, 10.0)
            self.assertTrue(out.get("ok"))
        self.assertEqual(srv.requests, 5)
        self.assertEqual(srv.connections, 5, "a Connection: close socket must not be pooled")

    def test_a_dropped_pooled_connection_is_retried(self):
        # The server answers keep-alive and closes anyway, so the pooled socket is dead before the
        # next call touches it. Every call must still return an answer.
        srv = _Server(drop_after_reply=True)
        self.addCleanup(srv.close)
        for i in range(4):
            out = client._post_json(srv.url, None, "/v1/update", {"i": i}, 10.0)
            self.assertTrue(out.get("ok"), "call %d lost its answer to a dead pooled socket" % i)
        self.assertEqual(srv.requests, 4)

    def test_a_fresh_connection_failure_is_raised_not_retried(self):
        # Nothing is listening. The first connect fails, and that is a real fault: it must be
        # raised, not tried again on another fresh socket.
        attempts = []
        real_connect = client._connect

        def counting_connect(base_url, timeout):
            attempts.append(base_url)
            return real_connect(base_url, timeout)

        client._connect = counting_connect
        self.addCleanup(lambda: setattr(client, "_connect", real_connect))
        with self.assertRaises(Exception):
            client._post_json("http://127.0.0.1:1", None, "/v1/update", {"a": 1}, 2.0)
        self.assertEqual(len(attempts), 1, "a fresh connection failure must not be retried")

    def test_error_statuses_still_reach_the_caller(self):
        srv = _Server(status=500)
        self.addCleanup(srv.close)
        with self.assertRaises(RuntimeError):
            client._post_json(srv.url, None, "/v1/update", {"a": 1}, 10.0)

    def test_a_get_404_still_returns_its_body(self):
        srv = _Server(status=404)
        self.addCleanup(srv.close)
        out = client._get_json(srv.url, None, "/v1/memory/nope", 10.0)
        self.assertEqual(out.get("found"), False)

    def test_two_threads_do_not_share_one_connection(self):
        srv = _Server()
        self.addCleanup(srv.close)
        seen = []

        def work():
            _clear_pool()
            for _ in range(3):
                client._post_json(srv.url, None, "/v1/update", {"a": 1}, 10.0)
            cache = getattr(client._POOL, "connections", {}) or {}
            seen.append(len(cache))
            # A thread that exits leaves its pooled socket to be closed when the connection object
            # is collected rather than at exit. Long-lived worker threads never reach this, but a
            # one-shot thread does, so close it here rather than let a ResourceWarning stand in for
            # a fact worth stating.
            _clear_pool()

        threads = [threading.Thread(target=work) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(seen, [1, 1], "each thread keeps its own connection")
        self.assertGreaterEqual(srv.connections, 2, "two threads must not share one socket")


if __name__ == "__main__":
    unittest.main()
