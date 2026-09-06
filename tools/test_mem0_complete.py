#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0 API completion + delete optimization.

Covers the pieces added on top of the delete/forget/get_all/reset surface:

  * Backend (local adapter through a real MatrixArkMcpServer): get(memory_id) returns one memory,
    update(memory_id, data) supersedes (retrieve/get_all return the new version, the old never
    resurfaces), history(memory_id) lists the ordered ingest -> supersede/delete events, closure
    delete of a source event cascades to single-source derivatives while demoting multi-source ones,
    and the physical purge compacts tombstones out of the JSONL log (log shrinks; logical state is
    preserved across reload).
  * mem0 shim (matrixark_mem0_compat.Memory): search() returns mem0's {"results":[{id, memory,
    score, metadata}]} shape (raw=True keeps the ContextPack), and get/update/history map to the
    right endpoints.
  * Gateway REST routes (matrixark_v1_gateway): POST /v1/update and GET /v1/memory/<id>[/history]
    dispatch to the right tools with the tenant pinned from the key.
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
from matrixark_mem0_compat import _reshape_search_results


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
class GetUpdateHistoryBackendCase(unittest.TestCase):
    def test_a_limited_get_all_returns_the_newest_memories_not_the_oldest(self) -> None:
        """`get_all(limit=N)` must answer with the N most recent memories.

        It sorted ascending and took the head, so a subject's FIRST memories came back and
        everything recent was invisible -- `get_all(limit=10)` against a thousand memories answered
        with the ten oldest. The listing itself stays in chronological order, which is what an
        unlimited call already returned.
        """
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            anchor = 1_780_000_000_000
            for i in range(5):
                server.call_tool("matrixark_ingest", {
                    "messages": [{"role": "user", "content": f"FACT{i} recorded"}],
                    "scope": _scope_for("limituser"),
                    "ingestion_time_ms": anchor + i * 10_000,
                })
            everything = server.call_tool("matrixark_get_all", {"scope": _scope_for("limituser")})
            self.assertEqual(5, everything["count"], "the fill must produce five memories")

            limited = server.call_tool(
                "matrixark_get_all", {"scope": _scope_for("limituser"), "limit": 2})
            self.assertEqual(2, limited["count"])
            texts = json.dumps(limited)
            self.assertIn("FACT4", texts, "the newest memory must be in a limited listing")
            self.assertIn("FACT3", texts)
            self.assertNotIn("FACT0", texts, "the oldest must not crowd out the newest")

            # Still chronological within the page, and an unlimited call is untouched.
            stamps = [int(m["created_at_ms"]) for m in limited["memories"]]
            self.assertEqual(sorted(stamps), stamps, "a page stays in chronological order")
            self.assertEqual(5, len(everything["memories"]))

    def _ingest(self, server, user, text):
        return server.call_tool(
            "matrixark_ingest",
            {"messages": [{"role": "user", "content": text}], "scope": _scope_for(user), "finalize": True},
        )["event_id_hash"]

    def test_get_returns_memory_and_metadata(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            mid = self._ingest(server, "alice", "Alice loves espresso")
            got = server.call_tool("matrixark_get_memory", {"memory_id": str(mid), "scope": _scope_for("alice")})
            self.assertTrue(got["found"])
            self.assertIn("espresso", got["memory"])
            self.assertIsInstance(got["metadata"], dict)
            self.assertEqual(str(mid), got["memory_id"])
            # A missing / never-ingested id reports found=False (no exception).
            missing = server.call_tool("matrixark_get_memory", {"memory_id": "404404404", "scope": _scope_for("alice")})
            self.assertFalse(missing["found"])

    def test_update_supersede_retrieve_returns_new(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            mid = self._ingest(server, "alice", "Alice loves espresso and lives in Rome")
            server.call_tool("matrixark_session_commit", {"scope": _scope_for("alice")})
            updated = server.call_tool(
                "matrixark_update_memory",
                {"memory_id": str(mid), "data": "Alice now prefers matcha green tea in Kyoto",
                 "scope": _scope_for("alice")},
            )
            self.assertTrue(updated["updated"])
            self.assertTrue(updated["superseded"])
            new_id = updated["new_memory_id"]
            self.assertNotEqual(str(new_id), str(mid))
            server.call_tool("matrixark_session_commit", {"scope": _scope_for("alice")})
            # get_all: exactly the new version, the old id gone.
            listed = server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})
            self.assertEqual(1, listed["count"])
            self.assertIn("matcha", listed["memories"][0]["memory"])
            self.assertFalse(
                server.call_tool("matrixark_get_memory", {"memory_id": str(mid), "scope": _scope_for("alice")})["found"])
            # retrieve surfaces the NEW content and never the superseded (tombstoned) content.
            pack = server.call_tool("matrixark_retrieve", {"query": "what does alice drink", "scope": _scope_for("alice")})
            blob = json.dumps(pack, default=str)
            self.assertIn("matcha", blob)
            self.assertNotIn("espresso", blob)

    def test_history_orders_ingest_supersede_delete(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            mid = self._ingest(server, "alice", "Alice loves espresso")
            updated = server.call_tool(
                "matrixark_update_memory",
                {"memory_id": str(mid), "data": "Alice prefers matcha", "scope": _scope_for("alice")})
            new_id = updated["new_memory_id"]
            # History of the OLD id: created (ingested) then superseded (by new id).
            old_hist = server.call_tool("matrixark_memory_history", {"memory_id": str(mid), "scope": _scope_for("alice")})
            old_events = [e["event"] for e in old_hist["history"]]
            self.assertEqual("ingested", old_events[0])
            self.assertIn("superseded", old_events)
            superseded = next(e for e in old_hist["history"] if e["event"] == "superseded")
            self.assertEqual(str(new_id), str(superseded["superseded_by"]))
            # History of the NEW id: ingested + a "created" link back to the memory it supersedes.
            new_hist = server.call_tool("matrixark_memory_history", {"memory_id": str(new_id), "scope": _scope_for("alice")})
            new_events = [e["event"] for e in new_hist["history"]]
            self.assertIn("ingested", new_events)
            created = next(e for e in new_hist["history"] if e["event"] == "created")
            self.assertEqual(str(mid), str(created["supersedes_memory_id"]))
            # Now delete the new id -> its history gains a "deleted" event.
            server.call_tool("matrixark_delete", {"memory_id": str(new_id), "scope": _scope_for("alice")})
            after = server.call_tool("matrixark_memory_history", {"memory_id": str(new_id), "scope": _scope_for("alice")})
            self.assertIn("deleted", [e["event"] for e in after["history"]])


class ClosureDeleteBackendCase(unittest.TestCase):
    def test_delete_source_event_cascades_single_source_derivatives(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            mid = server.call_tool(
                "matrixark_ingest",
                {"messages": [{"role": "user", "content": "Alice loves espresso and lives in Rome"}],
                 "scope": _scope_for("alice"), "finalize": True})["event_id_hash"]
            server.call_tool("matrixark_session_commit", {"scope": _scope_for("alice")})
            # Count derived records built solely from this event before delete.
            import matrixark_mcp_local_adapter as la
            before_single = sum(
                1 for r in adapter.read_all()
                if la._record_provenance_source_ids(r) == {int(mid)})
            self.assertGreater(before_single, 0, "expected single-source derivatives to exist")
            result = server.call_tool("matrixark_delete", {"memory_id": str(mid), "scope": _scope_for("alice")})
            self.assertTrue(result["closure"])
            self.assertTrue(result["deleted"])
            # After the closure delete: the event AND every single-source derivative are gone.
            after_single = sum(
                1 for r in adapter.read_all()
                if la._record_provenance_source_ids(r) == {int(mid)})
            self.assertEqual(0, after_single)
            self.assertFalse(
                server.call_tool("matrixark_get_memory", {"memory_id": str(mid), "scope": _scope_for("alice")})["found"])

    def test_multi_source_derivative_kept_with_trimmed_evidence(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            import matrixark_mcp_local_adapter as la
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            mid = server.call_tool(
                "matrixark_ingest",
                {"messages": [{"role": "user", "content": "Alice enjoys espresso"}],
                 "scope": _scope_for("alice"), "finalize": True})["event_id_hash"]
            server.call_tool("matrixark_session_commit", {"scope": _scope_for("alice")})
            # Synthesize a MULTI-source derivative (an entity whose evidence lists the target event
            # plus a second, unrelated source). Deleting the target must KEEP it, with the target
            # trimmed from its provenance -- not hard-delete it.
            other_source = 99999999999
            adapter.append({
                "record_type": "context_entity",
                "entity_hash": 4242424242,
                "entity_name": "alice_multi",
                "scope_key": next(r.get("scope_key") for r in adapter.read_all()
                                  if r.get("record_type") == "context_event" and str(r.get("event_id_hash")) == str(mid)),
                "source_event_ids": [int(mid), other_source],
                "source_refs": [str(mid), str(other_source)],
                "updated_at_ms": 1,
            })
            result = server.call_tool("matrixark_delete", {"memory_id": str(mid), "scope": _scope_for("alice")})
            self.assertGreaterEqual(result["superseded_count"], 1)
            survivors = [r for r in adapter.read_all()
                         if r.get("record_type") == "context_entity" and r.get("entity_hash") == 4242424242]
            self.assertEqual(1, len(survivors), "multi-source derivative must survive")
            kept = survivors[0]
            self.assertNotIn(int(mid), kept.get("source_event_ids", []))
            self.assertIn(other_source, kept.get("source_event_ids", []))
            self.assertNotIn(str(mid), kept.get("source_refs", []))


class PhysicalPurgeBackendCase(unittest.TestCase):
    def _ingest(self, server, user, i):
        return server.call_tool(
            "matrixark_ingest",
            {"messages": [{"role": "user", "content": f"{user} note {i}"}], "scope": _scope_for(user), "finalize": True},
        )["event_id_hash"]

    def test_purge_compacts_tombstones_and_preserves_state(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(path)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            ids = [self._ingest(server, "alice", i) for i in range(5)]
            for mid in ids[:3]:
                server.call_tool("matrixark_delete", {"memory_id": str(mid), "scope": _scope_for("alice")})
            before_count = server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"]
            self.assertEqual(2, before_count)
            self.assertEqual(3, adapter._count_raw_tombstones())
            bytes_before = path.stat().st_size

            purge = adapter.purge_tombstones(force=True)
            self.assertTrue(purge["purged"])
            self.assertEqual(3, purge["removed_tombstones"])
            self.assertLess(purge["records_after"], purge["records_before"])
            self.assertEqual(0, adapter._count_raw_tombstones())
            self.assertLess(path.stat().st_size, bytes_before)
            server.close(timeout_s=1.0)

            # Reload over the purged log: same logical state; the dropped records stay dropped.
            adapter2 = mcp.MatrixArkLocalAdapter(path)
            server2 = mcp.MatrixArkMcpServer(adapter2, access_mode="dev")
            self.addCleanup(server2.close, timeout_s=1.0)
            self.assertEqual(before_count, server2.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])
            self.assertFalse(adapter2.get_memory({"memory_id": str(ids[0])})["found"])
            self.assertTrue(adapter2.get_memory({"memory_id": str(ids[4])})["found"])

    def test_reset_triggers_purge(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            self._ingest(server, "alice", 0)
            self._ingest(server, "alice", 1)
            reset = server.call_tool("matrixark_reset", {"scope": _scope_for("alice"), "confirm": "RESET"})
            self.assertTrue(reset["reset"])
            # reset force-purges: the reset tombstone (and everything it killed) is compacted away.
            self.assertTrue(reset["purge"]["purged"])
            self.assertEqual(0, adapter._count_raw_tombstones())
            self.assertEqual(0, server.call_tool("matrixark_get_all", {"scope": _scope_for("alice")})["count"])


# --------------------------------------------------------------------------- #
# search reshape (pure function) + mem0 shim
# --------------------------------------------------------------------------- #
class SearchReshapeCase(unittest.TestCase):
    def test_reshape_maps_selected_refs_to_results(self):
        pack = {"selected_refs": [
            {"ref_type": "event", "text": "Alice loves espresso", "source_ref": "e1", "score": 0.91,
             "entity_name": "alice"},
            {"ref_type": "summary", "citation": "Alice lives in Rome", "ref_hash": 12345},
        ]}
        out = _reshape_search_results(pack)
        self.assertIn("results", out)
        self.assertEqual(2, len(out["results"]))
        first = out["results"][0]
        self.assertEqual("Alice loves espresso", first["memory"])
        self.assertEqual("e1", first["id"])
        self.assertEqual(0.91, first["score"])
        self.assertIn("entity_name", first["metadata"])
        # A ref lacking text falls back to citation; id falls back to ref_hash.
        second = out["results"][1]
        self.assertEqual("Alice lives in Rome", second["memory"])
        self.assertEqual(12345, second["id"])

    def test_reshape_empty_on_unknown_shape(self):
        self.assertEqual({"results": []}, _reshape_search_results({"nope": 1}))


# ---- mem0 shim against an in-process mock gateway ----
_REQUESTS: list[dict] = []


class _ShimHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def _read(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _respond(self, payload: dict) -> None:
        out = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def do_GET(self):
        _REQUESTS.append({"method": "GET", "path": self.path})
        if self.path.endswith("/history"):
            self._respond({"memory_id": "77", "history": [{"event": "ingested"}], "count": 1})
        else:
            self._respond({"found": True, "memory": "hello", "memory_id": "77"})

    def do_POST(self):
        body = json.loads(self._read() or b"{}")
        _REQUESTS.append({"method": "POST", "path": self.path, "body": body})
        if self.path == "/v1/retrieve":
            self._respond({"selected_refs": [
                {"ref_type": "event", "text": "Alice loves espresso", "source_ref": "e1", "score": 0.5}]})
        else:
            self._respond({"ok": True, "path": self.path, "echo": body})


class _ShimGateway:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _ShimHandler)
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


class MemoryShimCompletionCase(unittest.TestCase):
    def setUp(self):
        _REQUESTS.clear()

    def test_search_returns_mem0_results_shape(self):
        with _ShimGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            out = m.search("espresso?", user_id="alice", limit=5)
        self.assertIn("results", out)
        self.assertEqual("Alice loves espresso", out["results"][0]["memory"])
        self.assertEqual("e1", out["results"][0]["id"])
        self.assertEqual(0.5, out["results"][0]["score"])

    def test_search_raw_keeps_context_pack(self):
        with _ShimGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            out = m.search("espresso?", user_id="alice", raw=True)
        self.assertIn("selected_refs", out)
        self.assertNotIn("results", out)

    def test_get_maps_to_memory_endpoint(self):
        with _ShimGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            out = m.get("77")
        self.assertEqual("GET", _REQUESTS[0]["method"])
        self.assertEqual("/v1/memory/77", _REQUESTS[0]["path"])
        self.assertTrue(out["found"])

    def test_update_maps_to_update_endpoint(self):
        with _ShimGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            m.update("77", "new content")
        self.assertEqual("POST", _REQUESTS[0]["method"])
        self.assertEqual("/v1/update", _REQUESTS[0]["path"])
        self.assertEqual("77", _REQUESTS[0]["body"]["memory_id"])
        self.assertEqual("new content", _REQUESTS[0]["body"]["data"])

    def test_history_maps_to_history_endpoint(self):
        with _ShimGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            out = m.history("77")
        self.assertEqual("GET", _REQUESTS[0]["method"])
        self.assertEqual("/v1/memory/77/history", _REQUESTS[0]["path"])
        self.assertEqual(1, out["count"])


# --------------------------------------------------------------------------- #
# Gateway REST routes against a stub backend
# --------------------------------------------------------------------------- #
class _StubServer:
    def __init__(self, *, found: bool = True):
        self.calls = []
        self._found = found

    def call_tool(self, name, args):
        self.calls.append((name, dict(args)))
        if name == "matrixark_get_memory":
            return {"found": self._found, "memory": "hello", "memory_id": args.get("memory_id")}
        if name == "matrixark_memory_history":
            return {"memory_id": args.get("memory_id"), "history": [{"event": "ingested"}], "count": 1}
        if name == "matrixark_update_memory":
            return {"updated": True, "memory_id": args.get("memory_id"), "new_memory_id": "88"}
        return {"ok": name}

    def handle(self, body):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {}}

    def _finalize_write_response(self, name, args, identity, hook, response):
        return response


def _drive(app, *, method="POST", path="/v1/update", body=None, headers=None, query=""):
    payload = json.dumps(body if body is not None else {}).encode()
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {"type": "http", "method": method, "path": path, "headers": hdrs, "query_string": query.encode()}

    async def receive():
        return {"type": "http.request", "body": payload, "more_body": False}

    sent = []

    async def send(msg):
        sent.append(msg)

    asyncio.run(app(scope, receive, send))
    start = next(x for x in sent if x["type"] == "http.response.start")
    data = b"".join(x.get("body", b"") for x in sent if x["type"] == "http.response.body")
    return start["status"], data


class GatewayCompletionRoutesCase(unittest.TestCase):
    def setUp(self):
        self.server = _StubServer()
        self.cfg = gw.GatewayConfig.from_env({"api_keys": {"k-acme": "acme"}, "require_auth": True})
        self.app = gw.make_v1_app(self.server, self.cfg)

    def test_update_route_dispatches_update_tool(self):
        st, _ = _drive(self.app, path="/v1/update", body={"memory_id": "77", "data": "new"},
                       headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_update_memory", self.server.calls[0][0])
        self.assertEqual("77", self.server.calls[0][1]["memory_id"])
        self.assertEqual("acme", self.server.calls[0][1]["scope"]["tenant_id"])

    def test_get_memory_route_dispatches_get_tool(self):
        st, body = _drive(self.app, method="GET", path="/v1/memory/77",
                          headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_get_memory", self.server.calls[0][0])
        self.assertEqual("77", self.server.calls[0][1]["memory_id"])
        self.assertEqual("acme", self.server.calls[0][1]["scope"]["tenant_id"])
        self.assertTrue(json.loads(body)["found"])

    def test_history_route_dispatches_history_tool(self):
        st, body = _drive(self.app, method="GET", path="/v1/memory/77/history",
                          headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, st)
        self.assertEqual("matrixark_memory_history", self.server.calls[0][0])
        self.assertEqual("77", self.server.calls[0][1]["memory_id"])
        self.assertEqual(1, json.loads(body)["count"])

    def test_get_memory_route_404_when_not_found(self):
        server = _StubServer(found=False)
        app = gw.make_v1_app(server, self.cfg)
        st, body = _drive(app, method="GET", path="/v1/memory/404",
                          headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(404, st)
        self.assertFalse(json.loads(body)["found"])

    def test_update_route_requires_auth(self):
        st, _ = _drive(self.app, path="/v1/update", body={"memory_id": "77", "data": "x"})
        self.assertEqual(401, st)
        self.assertEqual([], self.server.calls)

    def test_get_memory_route_requires_context_retrieve_scope(self):
        cfg = gw.GatewayConfig.from_env({
            "enforced": True, "require_auth": True,
            "hashed_api_keys": {gw._secret_hash("forget-only"): {
                "tenant_id": "acme", "account_id": "acct", "scopes": ["context:forget"]}},
        })
        app = gw.make_v1_app(self.server, cfg)
        st, body = _drive(app, method="GET", path="/v1/memory/77",
                          headers={"Authorization": "Bearer forget-only"})
        self.assertEqual(403, st)
        self.assertEqual("insufficient_scope", json.loads(body)["error"])
        self.assertEqual([], self.server.calls)

    def test_update_route_requires_context_ingest_scope(self):
        cfg = gw.GatewayConfig.from_env({
            "enforced": True, "require_auth": True,
            "hashed_api_keys": {gw._secret_hash("retrieve-only"): {
                "tenant_id": "acme", "account_id": "acct", "scopes": ["context:retrieve"]}},
        })
        app = gw.make_v1_app(self.server, cfg)
        st, body = _drive(app, path="/v1/update", body={"memory_id": "77", "data": "x"},
                          headers={"Authorization": "Bearer retrieve-only"})
        self.assertEqual(403, st)
        self.assertEqual("insufficient_scope", json.loads(body)["error"])


class Mem0SearchPackShapeCase(unittest.TestCase):
    """search() must return the pack's refs, and they must be addressable.

    Both of these shipped broken: the shim was written against a flat ``selected_refs`` pack, but a
    ContextPack now serves refs GROUPED and emits no ``selected_refs`` key at all, so every
    ``search()`` returned ``{"results": []}`` while ``search_raw`` showed content."""

    def test_grouped_pack_is_reshaped_into_mem0_results(self):
        pack = {
            "context_pack_id": "1",
            "groups": [
                {"type": "event", "n": 1, "items": [
                    {"text": "user: I live in Kyoto.", "memory_layer": "session"}]},
                {"type": "entity", "n": 1, "items": [
                    {"text": "location: Kyoto", "entity_type": "location"}]},
            ],
        }
        results = _reshape_search_results(pack)["results"]
        self.assertEqual(2, len(results), "grouped pack must not reshape to an empty result set")
        self.assertEqual("user: I live in Kyoto.", results[0]["memory"])
        self.assertEqual("location: Kyoto", results[1]["memory"])
        # The group's type survives as metadata so callers keep the event/entity distinction the
        # flat selected_refs shape gave them.
        self.assertEqual("event", results[0]["metadata"].get("ref_type"))
        self.assertEqual("entity", results[1]["metadata"].get("ref_type"))

    def test_flat_selected_refs_pack_still_works(self):
        pack = {"selected_refs": [{"text": "flat ref", "source_ref": "e1", "score": 0.5}]}
        results = _reshape_search_results(pack)["results"]
        self.assertEqual(1, len(results))
        self.assertEqual("flat ref", results[0]["memory"])
        self.assertEqual("e1", results[0]["id"])

    def test_search_results_carry_addressable_ids(self):
        """A synthetic ``ref-N-...`` id is useless: get/update/delete all reject it."""

        class _Client(mem0.Memory):
            def __init__(self):
                pass  # no transport: the two hops are stubbed below

            def search_raw(self, query, **kw):
                return {"groups": [{"type": "event", "n": 1, "items": [
                    {"text": "user: I live in Kyoto."}]}]}

            def get_all(self, **kw):
                return {"memories": [{"id": 12345, "memory": "user: I live in Kyoto."}]}

        results = _Client().search("where do I live", user_id="u1")["results"]
        self.assertEqual(1, len(results))
        self.assertEqual("12345", results[0]["id"], "id must be the real memory id, not ref-N-...")

    def test_unmatched_items_keep_their_synthetic_id(self):
        """A derived entity ref is a projection of a memory, not an addressable memory."""

        class _Client(mem0.Memory):
            def __init__(self):
                pass

            def search_raw(self, query, **kw):
                return {"groups": [{"type": "entity", "n": 1, "items": [
                    {"text": "location: Kyoto"}]}]}

            def get_all(self, **kw):
                return {"memories": [{"id": 12345, "memory": "user: I live in Kyoto."}]}

        results = _Client().search("where", user_id="u1")["results"]
        self.assertTrue(results[0]["id"].startswith("ref-"))

    def test_search_survives_a_failing_id_lookup(self):
        class _Client(mem0.Memory):
            def __init__(self):
                pass

            def search_raw(self, query, **kw):
                return {"groups": [{"type": "event", "n": 1, "items": [{"text": "hello"}]}]}

            def get_all(self, **kw):
                raise RuntimeError("gateway down")

        results = _Client().search("q", user_id="u1")["results"]
        self.assertEqual("hello", results[0]["memory"])


if __name__ == "__main__":
    unittest.main()
