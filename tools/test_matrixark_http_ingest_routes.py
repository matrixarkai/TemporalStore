#!/usr/bin/env python3
"""Enterprise HTTP ingestion surface: route registration + async-default behavior."""
import unittest

import matrixark_http as h


class EnterpriseIngestRoutesTest(unittest.TestCase):
    def test_routes_registered(self):
        r = h.HTTP_TOOL_ROUTES
        self.assertEqual("matrixark_ingest", r.get("/api/ingest"))
        self.assertEqual("matrixark_retrieve", r.get("/api/retrieve"))
        self.assertEqual("matrixark_session_commit", r.get("/api/session_commit"))

    def test_ingest_defaults_to_async(self):
        args = {"messages": [{"role": "user", "content": "hi"}]}
        h.apply_ingest_route_defaults("/api/ingest", args)
        self.assertTrue(args["async_processing"])  # fast ack under scale

    def test_caller_can_force_sync(self):
        args = {"messages": [{"role": "user", "content": "hi"}], "async_processing": False}
        h.apply_ingest_route_defaults("/api/ingest", args)
        self.assertFalse(args["async_processing"])  # explicit value preserved

    def test_retrieve_route_untouched(self):
        args = {"query": "x"}
        h.apply_ingest_route_defaults("/api/retrieve", args)
        self.assertNotIn("async_processing", args)  # only /api/ingest gets the default


class _FakeServer:
    def __init__(self):
        self.seen = []
    def handle(self, body):
        self.seen.append(body)
        if body.get("method") == "notifications/initialized":
            return None
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {"echo": body.get("method")}}


class McpOverHttpTest(unittest.TestCase):
    def test_tools_call_pipes_through_handle_and_injects_api_key(self):
        s = _FakeServer()
        body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": "matrixark_retrieve", "arguments": {"query": "x"}}}
        out = h.mcp_http_dispatch(s, body, api_key="k-123")
        self.assertEqual({"echo": "tools/call"}, out["result"])
        self.assertEqual("k-123", s.seen[0]["params"]["arguments"]["api_key"])  # tenant key injected

    def test_ingest_via_mcp_gets_async_default(self):
        s = _FakeServer()
        body = {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "matrixark_ingest", "arguments": {"messages": [{"role": "user", "content": "hi"}]}}}
        h.mcp_http_dispatch(s, body, api_key="k")
        self.assertTrue(s.seen[0]["params"]["arguments"]["async_processing"])

    def test_initialize_and_list_pass_through(self):
        s = _FakeServer()
        self.assertEqual("initialize", h.mcp_http_dispatch(s, {"id": 1, "method": "initialize"})["result"]["echo"])
        self.assertEqual("tools/list", h.mcp_http_dispatch(s, {"id": 2, "method": "tools/list"})["result"]["echo"])

    def test_notification_returns_jsonrpc_envelope(self):
        s = _FakeServer()
        self.assertEqual({"jsonrpc": "2.0"}, h.mcp_http_dispatch(s, {"method": "notifications/initialized"}))


if __name__ == "__main__":
    unittest.main()
