#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""ASGI front: routing, api-key injection, ingest async-default, MCP dispatch, 429, healthz."""
import asyncio
import json
import unittest

import matrixark_asgi as m


class MatrixArkBackpressureError(Exception):  # name must match what the app checks
    pass


class _FakeServer:
    def __init__(self, *, boom=False):
        self.calls = []
        self.handled = []
        self.boom = boom

    def call_tool(self, name, args):
        if self.boom:
            raise MatrixArkBackpressureError("overloaded")
        self.calls.append((name, dict(args)))
        return {"ok": name}

    def handle(self, body):
        self.handled.append(body)
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {"m": body.get("method")}}


def drive(app, *, method="POST", path="/api/ingest", body=None, headers=None):
    """Run one request through the ASGI app; return (status, json_body)."""
    payload = json.dumps(body or {}).encode()
    hdrs = [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {"type": "http", "method": method, "path": path, "headers": hdrs}
    sent = []

    async def receive():
        return {"type": "http.request", "body": payload, "more_body": False}

    async def send(msg):
        sent.append(msg)

    asyncio.run(app(scope, receive, send))
    status = next(x["status"] for x in sent if x["type"] == "http.response.start")
    data = b"".join(x.get("body", b"") for x in sent if x["type"] == "http.response.body")
    return status, json.loads(data or b"{}")


class AsgiTest(unittest.TestCase):
    def setUp(self):
        self.s = _FakeServer()
        self.app = m.make_asgi_app(self.s)

    def test_ingest_async_default_and_api_key(self):
        st, body = drive(self.app, path="/api/ingest",
                         body={"messages": [{"role": "user", "content": "hi"}]},
                         headers={"Authorization": "Bearer k-9"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_ingest", self.s.calls[0][0])
        self.assertTrue(self.s.calls[0][1]["async_processing"])   # fast-ack default
        self.assertEqual("k-9", self.s.calls[0][1]["api_key"])     # tenant key injected

    def test_retrieve_no_async_default(self):
        st, _ = drive(self.app, path="/api/retrieve", body={"query": "x"}, headers={"X-API-Key": "k"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_retrieve", self.s.calls[0][0])
        self.assertNotIn("async_processing", self.s.calls[0][1])

    def test_mcp_dispatch(self):
        st, body = drive(self.app, path="/mcp",
                         body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        self.assertEqual(200, st)
        self.assertEqual({"m": "tools/list"}, body["result"])

    def test_backpressure_returns_429(self):
        app = m.make_asgi_app(_FakeServer(boom=True))
        st, body = drive(app, path="/api/ingest", body={"messages": [{"role": "user", "content": "x"}]})
        self.assertEqual(429, st)
        self.assertEqual("overloaded", body["status"])

    def test_healthz(self):
        st, body = drive(self.app, method="GET", path="/healthz")
        self.assertEqual(200, st)
        self.assertEqual("ok", body["status"])

    def test_unknown_path_404(self):
        st, _ = drive(self.app, path="/api/nope", body={})
        self.assertEqual(404, st)

    def test_bad_json_400(self):
        # craft an invalid body directly
        scope = {"type": "http", "method": "POST", "path": "/api/ingest", "headers": []}
        sent = []

        async def receive():
            return {"type": "http.request", "body": b"{not json", "more_body": False}

        async def send(msg):
            sent.append(msg)

        asyncio.run(self.app(scope, receive, send))
        st = next(x["status"] for x in sent if x["type"] == "http.response.start")
        self.assertEqual(400, st)


if __name__ == "__main__":
    unittest.main()
