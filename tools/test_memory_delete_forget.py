#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Memory management (mem0 parity): forget / delete / get_all / reset.

Three layers are exercised:

  * Backend (local adapter via a real MatrixArkMcpServer): forget wipes a subject's memory and is
    scope-isolated (a sibling user is untouched), delete removes a single memory, get_all lists a
    subject's active memories, reset wipes the tenant, a wrong confirm never deletes, and the removal
    survives an adapter reload (durable tombstones).
  * mem0 shim (matrixark_mem0_compat.Memory) against a tiny in-process stdlib gateway: delete_all /
    get_all / delete / reset post the correctly-mapped body to /v1/forget /v1/memories /v1/delete
    /v1/reset.
  * Gateway REST routes (matrixark_v1_gateway) against a stub backend: the routes dispatch to the
    right tools and are gated on context:forget (forget/delete/reset) / context:retrieve (get_all).
"""
from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import matrixark_mcp_server as mcp
import matrixark_mem0_compat as mem0
import matrixark_v1_gateway as gw


def _scope_for(user: str, *, tenant: str = "tenant_mem", session: str = "s1") -> dict:
    return {
        "account_id": "acct_local",
        "tenant_id": tenant,
        "user_id": user,
        "session_id": session,
        "agent_name": "t",
    }


# --------------------------------------------------------------------------- #
# Layer 1: backend (local adapter through a real server)
# --------------------------------------------------------------------------- #
class ForgetDeleteBackendCase(unittest.TestCase):
    def _ingest(self, server, user, n):
        ids = []
        for i in range(n):
            result = server.call_tool(
                "matrixark_ingest",
                {"messages": [{"role": "user", "content": f"{user} note {i}: enjoys espresso number {i}"}],
                 "scope": _scope_for(user)},
            )
            ids.append(result.get("event_id_hash"))
        return ids

    def test_forget_wipes_subject_and_isolates_sibling(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            self._ingest(server, "alice", 3)
            self._ingest(server, "bob", 2)

            self.assertEqual(3, server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])
            self.assertEqual(2, server.call_tool("matrixark_get_all", {"scope": _scope_for("bob")})["count"])

            forgotten = server.call_tool("matrixark_forget", {"scope": _scope_for("alice"), "confirm": "alice"})
            self.assertTrue(forgotten["forgotten"])
            self.assertGreaterEqual(forgotten["removed_count"], 3)
            # Hashed-subject audit only (never the raw user_id) in the forget-specific fields; the
            # `access` echo carries the request identity as every tool response does.
            self.assertEqual(64, len(forgotten["subject_sha256"]))
            forget_fields = {k: v for k, v in forgotten.items() if k != "access"}
            self.assertNotIn("alice", json.dumps(forget_fields))

            # alice's memory is gone from both get_all and retrieve; bob is untouched.
            self.assertEqual(0, server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])
            self.assertEqual(2, server.call_tool("matrixark_get_all", {"scope": _scope_for("bob")})["count"])
            retrieved = server.call_tool("matrixark_retrieve", {"query": "espresso", "scope": _scope_for("alice")})
            self.assertTrue(retrieved.get("insufficient_context", False) or not retrieved.get("groups"))

    def test_forget_audit_record_is_hashed_subject_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            self._ingest(server, "alice", 2)
            server.call_tool("matrixark_forget", {"scope": _scope_for("alice"), "confirm": "alice"})
            audits = [r for r in adapter.read_all() if r.get("record_type") == "matrixark_memory_forget_audit"]
            self.assertEqual(1, len(audits))
            self.assertEqual(64, len(audits[0]["subject_sha256"]))
            self.assertNotIn("alice", json.dumps(audits[0]))

    def test_delete_single_memory_leaves_siblings(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            ids = self._ingest(server, "alice", 3)
            before = server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})
            self.assertEqual(3, before["count"])
            target = str(ids[0])

            deleted = server.call_tool("matrixark_delete", {"scope": _scope_for("alice"), "memory_id": target})
            self.assertTrue(deleted["deleted"])
            after = server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})
            self.assertEqual(2, after["count"])
            self.assertNotIn(target, [str(m["id"]) for m in after["memories"]])

    def test_get_all_lists_subject_not_sibling(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            self._ingest(server, "alice", 2)
            self._ingest(server, "bob", 3)
            alice = server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})
            self.assertEqual(2, alice["count"])
            self.assertTrue(all(m["user_id"] == "alice" for m in alice["memories"]))

    def test_reset_wipes_tenant(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            self._ingest(server, "alice", 2)
            self._ingest(server, "bob", 2)
            reset = server.call_tool("matrixark_reset", {"scope": _scope_for("alice"), "confirm": "RESET"})
            self.assertTrue(reset["reset"])
            self.assertEqual(0, server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])
            self.assertEqual(0, server.call_tool("matrixark_get_all", {"scope": _scope_for("bob")})["count"])

    def test_wrong_confirm_never_deletes(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            self._ingest(server, "alice", 2)
            with self.assertRaises(Exception):
                server.call_tool("matrixark_forget", {"scope": _scope_for("alice"), "confirm": "not-alice"})
            with self.assertRaises(Exception):
                server.call_tool("matrixark_reset", {"scope": _scope_for("alice"), "confirm": "wrong"})
            # Nothing was removed.
            self.assertEqual(2, server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])

    def test_forget_survives_adapter_reload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(path)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self._ingest(server, "alice", 3)
            server.call_tool("matrixark_forget", {"scope": _scope_for("alice"), "confirm": "alice"})
            server.close(timeout_s=1.0)
            # Fresh adapter/server over the SAME event log: the forget tombstone is durable.
            adapter2 = mcp.MatrixArkLocalAdapter(path)
            server2 = mcp.MatrixArkMcpServer(adapter2, access_mode="dev")
            self.addCleanup(server2.close, timeout_s=1.0)
            self.assertEqual(0, server2.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])

    def test_reingest_after_forget_is_live_again(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            self._ingest(server, "alice", 2)
            server.call_tool("matrixark_forget", {"scope": _scope_for("alice"), "confirm": "alice"})
            self.assertEqual(0, server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])
            # A forget only removes memories that existed before it; new ones are live.
            self._ingest(server, "alice", 1)
            self.assertEqual(1, server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])


# --------------------------------------------------------------------------- #
# Layer 2: mem0 shim against an in-process mock gateway
# --------------------------------------------------------------------------- #
_POSTS: list[dict] = []


class _GatewayHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def _read(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def do_POST(self):
        body = json.loads(self._read() or b"{}")
        _POSTS.append({"path": self.path, "body": body,
                       "auth": self.headers.get("Authorization") or ""})
        out = json.dumps({"ok": True, "path": self.path, "echo": body}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


class _MockGateway:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _GatewayHandler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown(); self.httpd.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


class MemoryShimCase(unittest.TestCase):
    def setUp(self):
        _POSTS.clear()

    def test_delete_all_maps_to_forget_with_confirm(self):
        with _MockGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            m.delete_all(user_id="alice", agent_id="assistant", run_id="sess-1")
        post = _POSTS[0]
        self.assertEqual("/v1/forget", post["path"])
        self.assertEqual({"user_id": "alice", "agent_id": "assistant", "session_id": "sess-1"},
                         post["body"]["scope"])
        self.assertEqual("alice", post["body"]["confirm"])
        self.assertEqual("Bearer k", post["auth"])

    def test_delete_all_requires_user_id(self):
        with _MockGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            with self.assertRaises(ValueError):
                m.delete_all(agent_id="assistant")
        self.assertEqual([], _POSTS)

    def test_delete_maps_to_delete_endpoint(self):
        with _MockGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            m.delete("12345")
        post = _POSTS[0]
        self.assertEqual("/v1/delete", post["path"])
        self.assertEqual("12345", post["body"]["memory_id"])

    def test_get_all_maps_to_memories_endpoint(self):
        with _MockGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            m.get_all(user_id="alice", limit=10)
        post = _POSTS[0]
        self.assertEqual("/v1/memories", post["path"])
        self.assertEqual({"user_id": "alice"}, post["body"]["scope"])
        self.assertEqual(10, post["body"]["limit"])

    def test_reset_maps_to_reset_endpoint_with_confirm(self):
        with _MockGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            m.reset()
        post = _POSTS[0]
        self.assertEqual("/v1/reset", post["path"])
        self.assertEqual("RESET", post["body"]["confirm"])


# --------------------------------------------------------------------------- #
# Layer 3: gateway REST routes against a stub backend
# --------------------------------------------------------------------------- #
class _StubServer:
    def __init__(self):
        self.calls = []

    def call_tool(self, name, args):
        self.calls.append((name, dict(args)))
        if name == "matrixark_get_all":
            return {"memories": [{"id": 1, "memory": "x"}], "count": 1}
        return {"ok": name, "removed_count": 2}

    def handle(self, body):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {}}


def _drive(app, *, method="POST", path="/v1/forget", body=None, headers=None, query=""):
    payload = json.dumps(body if body is not None else {}).encode()
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {"type": "http", "method": method, "path": path, "headers": hdrs,
             "query_string": query.encode()}

    async def receive():
        return {"type": "http.request", "body": payload, "more_body": False}

    sent = []

    async def send(msg):
        sent.append(msg)

    asyncio.run(app(scope, receive, send))
    start = next(x for x in sent if x["type"] == "http.response.start")
    data = b"".join(x.get("body", b"") for x in sent if x["type"] == "http.response.body")
    return start["status"], data


class GatewayMemoryRoutesCase(unittest.TestCase):
    def setUp(self):
        self.server = _StubServer()
        self.cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}, "require_auth": True})
        self.app = gw.make_v1_app(self.server, self.cfg)

    def test_forget_route_dispatches_forget_tool(self):
        st, body = _drive(self.app, path="/v1/forget",
                          body={"scope": {"user_id": "alice"}, "confirm": "alice"},
                          headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_forget", self.server.calls[0][0])
        # tenant is pinned from the authenticated key.
        self.assertEqual("acme", self.server.calls[0][1]["scope"]["tenant_id"])
        self.assertEqual("alice", self.server.calls[0][1]["confirm"])

    def test_delete_route_dispatches_delete_tool(self):
        st, _ = _drive(self.app, path="/v1/delete", body={"memory_id": "77"},
                       headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_delete", self.server.calls[0][0])
        self.assertEqual("77", self.server.calls[0][1]["memory_id"])

    def test_reset_route_dispatches_reset_tool(self):
        st, _ = _drive(self.app, path="/v1/reset", body={"confirm": "RESET"},
                       headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_reset", self.server.calls[0][0])

    def test_get_memories_route_dispatches_get_all(self):
        st, body = _drive(self.app, method="GET", path="/v1/memories",
                          query="user_id=alice&limit=5",
                          headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_get_all", self.server.calls[0][0])
        self.assertEqual("alice", self.server.calls[0][1]["scope"]["user_id"])
        self.assertEqual(5, self.server.calls[0][1]["limit"])
        self.assertEqual(1, json.loads(body)["count"])

    def test_post_memories_route_also_dispatches_get_all(self):
        st, _ = _drive(self.app, path="/v1/memories", body={"scope": {"user_id": "bob"}},
                       headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_get_all", self.server.calls[0][0])

    def test_forget_requires_auth(self):
        st, _ = _drive(self.app, path="/v1/forget", body={"confirm": "alice"})
        self.assertEqual(401, st)
        self.assertEqual([], self.server.calls)

    def test_forget_route_requires_context_forget_scope(self):
        # Enforced-mode key WITHOUT context:forget -> 403 (edge per-route scope gate).
        cfg = gw.GatewayConfig.from_env({
            "enforced": True, "require_auth": True,
            "hashed_api_keys": {gw._secret_hash("data-key"): {
                "tenant_id": "acme", "account_id": "acct", "scopes": ["context:ingest", "context:retrieve"]}},
        })
        app = gw.make_v1_app(self.server, cfg)
        st, body = _drive(app, path="/v1/forget", body={"scope": {"user_id": "a"}, "confirm": "a"},
                          headers={"Authorization": "Bearer data-key"})
        self.assertEqual(403, st)
        self.assertEqual("insufficient_scope", json.loads(body)["error"])
        self.assertEqual([], self.server.calls)

    def test_forget_route_allows_key_with_context_forget(self):
        cfg = gw.GatewayConfig.from_env({
            "enforced": True, "require_auth": True,
            "hashed_api_keys": {gw._secret_hash("forget-key"): {
                "tenant_id": "acme", "account_id": "acct", "scopes": ["context:forget"]}},
        })
        app = gw.make_v1_app(self.server, cfg)
        st, _ = _drive(app, path="/v1/forget", body={"scope": {"user_id": "a"}, "confirm": "a"},
                       headers={"Authorization": "Bearer forget-key"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_forget", self.server.calls[0][0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
