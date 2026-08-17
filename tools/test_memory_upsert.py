#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""PurchaseMemory Phase 2: keyed-upsert (identity_key) + truth-rank guard.

Backend (local adapter through a real server):
  * first ingest with an identity_key creates the keyed fact;
  * a second ingest for the same key with >= rank SUPERSEDES the old (old tombstoned with
    superseded_by set, retrieve / get_all return the new value);
  * a second ingest with a LOWER rank is RANK-GUARDED (old survives untouched, new rejected);
  * recall by identity_key returns the single current live value;
  * the supersede's closure tombstone removes the old event and its derivatives;
  * a keyed fact can also carry expires_at (Phase 1 composes with Phase 2).

mem0 shim (matrixark_mem0_compat.Memory against an in-process stub gateway):
  * add() returns the mem0 results envelope with event ADD / UPDATE / NONE mapped from the
    keyed-upsert outcome, while keeping event_id_hash for backward compatibility.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import matrixark_mcp_server as mcp
import matrixark_mcp_local_adapter as la
import matrixark_mem0_compat as mem0


def _scope(user: str, *, tenant: str = "tenant_upsert", session: str = "s1") -> dict:
    return {
        "account_id": "acct_local",
        "tenant_id": tenant,
        "user_id": user,
        "session_id": session,
        "agent_name": "t",
    }


class KeyedUpsertBackendCase(unittest.TestCase):
    def _server(self, tmp: str):
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        self.addCleanup(server.close, timeout_s=1.0)
        return adapter, server

    def _ingest_key(self, server, user, text, *, key="user.email", truth_class="reported", **extra):
        return server.call_tool("matrixark_ingest", {
            "messages": [{"role": "user", "content": text}],
            "scope": _scope(user), "identity_key": key, "truth_class": truth_class, **extra,
        })

    def test_first_keyed_ingest_creates(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            created = self._ingest_key(server, "alice", "email is a@x.com")
            self.assertEqual("created", created.get("identity_upsert"))
            self.assertEqual("add", created.get("upsert_outcome"))
            recalled = server.call_tool("matrixark_get_memory_by_key", {"scope": _scope("alice"), "identity_key": "user.email"})
            self.assertTrue(recalled["found"])
            self.assertIn("a@x.com", recalled["text"])

    def test_equal_or_higher_rank_supersedes(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            first = self._ingest_key(server, "bob", "email is old@x.com", truth_class="reported")
            old_id = str(first["event_id_hash"])
            second = self._ingest_key(server, "bob", "email is new@x.com", truth_class="asserted")
            new_id = str(second["event_id_hash"])
            self.assertEqual("superseded", second.get("identity_upsert"))
            self.assertEqual("update", second.get("upsert_outcome"))
            self.assertIn(old_id, [str(x) for x in second.get("superseded_memory_ids", [])])
            # Old is gone; new is the live keyed value.
            self.assertFalse(server.call_tool("matrixark_get_memory", {"scope": _scope("bob"), "memory_id": old_id})["found"])
            recalled = server.call_tool("matrixark_get_memory_by_key", {"scope": _scope("bob"), "identity_key": "user.email"})
            self.assertEqual(new_id, str(recalled["id"]))
            self.assertIn("new@x.com", recalled["text"])
            # history of the old id records the supersede link to the new id.
            hist = server.call_tool("matrixark_memory_history", {"scope": _scope("bob"), "memory_id": old_id})
            supersede_events = [e for e in hist["history"] if e.get("event") == "superseded"]
            self.assertTrue(supersede_events)
            self.assertEqual(new_id, str(supersede_events[0].get("superseded_by")))

    def test_same_rank_supersedes(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._ingest_key(server, "cara", "plan is basic", key="subscription.plan", truth_class="reported")
            second = self._ingest_key(server, "cara", "plan is pro", key="subscription.plan", truth_class="reported")
            self.assertEqual("update", second.get("upsert_outcome"))
            recalled = server.call_tool("matrixark_get_memory_by_key", {"scope": _scope("cara"), "identity_key": "subscription.plan"})
            self.assertIn("pro", recalled["text"])

    def test_lower_rank_is_rank_guarded(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            first = self._ingest_key(server, "dan", "email is confirmed@x.com", truth_class="asserted")
            keep_id = str(first["event_id_hash"])
            guarded = self._ingest_key(server, "dan", "email might be guess@x.com", truth_class="inferred")
            self.assertTrue(guarded.get("rank_guarded"))
            self.assertEqual("rank_guarded", guarded.get("upsert_outcome"))
            self.assertEqual(keep_id, str(guarded["event_id_hash"]))
            # The high-confidence fact survives untouched; the low-confidence write never surfaces.
            recalled = server.call_tool("matrixark_get_memory_by_key", {"scope": _scope("dan"), "identity_key": "user.email"})
            self.assertEqual(keep_id, str(recalled["id"]))
            self.assertIn("confirmed@x.com", recalled["text"])
            listing = server.call_tool("matrixark_get_all", {"scope": _scope("dan")})
            self.assertNotIn("guess@x.com", json.dumps(listing))

    def test_recall_by_key_scoped_to_subject(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._ingest_key(server, "erin", "email is erin@x.com", truth_class="asserted")
            self._ingest_key(server, "finn", "email is finn@x.com", truth_class="asserted")
            erin = server.call_tool("matrixark_get_memory_by_key", {"scope": _scope("erin"), "identity_key": "user.email"})
            finn = server.call_tool("matrixark_get_memory_by_key", {"scope": _scope("finn"), "identity_key": "user.email"})
            self.assertIn("erin@x.com", erin["text"])
            self.assertIn("finn@x.com", finn["text"])
            self.assertNotEqual(str(erin["id"]), str(finn["id"]))

    def test_supersede_closure_removes_old_event_and_derivatives(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            first = self._ingest_key(server, "gwen", "home city is Paris", key="profile.city",
                                     truth_class="reported", finalize=True)
            old_id = int(first["event_id_hash"])
            second = self._ingest_key(server, "gwen", "home city is Berlin", key="profile.city",
                                      truth_class="asserted", finalize=True)
            self.assertEqual("update", second.get("upsert_outcome"))
            # No live record still references the old source event (event or any single-source derivative).
            for record in adapter.read_all():
                self.assertNotEqual(str(record.get("event_id_hash")), str(old_id))
                provenance = la._record_provenance_source_ids(record)
                if provenance is not None:
                    self.assertNotEqual(provenance, {old_id})

    def test_keyed_upsert_composes_with_expires_at(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = 1_780_000_000_000
            os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(anchor)  # keep the keyed ephemeral fact live
            self.addCleanup(lambda: os.environ.pop("MATRIXARK_MEMORY_NOW_MS", None))
            self._ingest_key(server, "hank", "temp token abc", key="session.token",
                             truth_class="asserted", ingestion_time_ms=anchor, expires_at=(anchor + 5_000) / 1000.0)
            events = [r for r in adapter.read_all() if r.get("record_type") == "context_event"
                      and r.get("identity_key") == "session.token"]
            self.assertEqual(1, len(events))
            self.assertTrue(events[0].get("ephemeral"))
            self.assertEqual(3, int(events[0]["truth_rank"]))
            self.assertEqual(anchor + 5_000, int(events[0]["expires_at_ms"]))


# --------------------------------------------------------------------------- #
# mem0 shim: add() returns the mem0 results envelope (ADD / UPDATE / NONE).
# --------------------------------------------------------------------------- #
_NEXT_RESPONSE: dict = {}


class _IngestGatewayHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        _ = self.rfile.read(length) if length else b""
        out = json.dumps(_NEXT_RESPONSE).encode()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


class _MockIngestGateway:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _IngestGatewayHandler)
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


class Mem0AddResultsEnvelopeCase(unittest.TestCase):
    def _add(self, response: dict, **kw):
        global _NEXT_RESPONSE
        _NEXT_RESPONSE = response
        with _MockIngestGateway() as g:
            m = mem0.Memory(base_url=g.url, api_key="k")
            return m.add("hello there", user_id="u1", **kw)

    def test_add_returns_results_envelope_add(self):
        # Gateway-wrapped ingest response (accepted/scope/result), normal ingest -> ADD.
        out = self._add({"accepted": 1, "scope": {"user_id": "u1"}, "result": {"event_id_hash": 12345, "status": "accepted"}})
        self.assertEqual("12345", str(out["event_id_hash"]))  # backward-compat accessor
        self.assertIn("results", out)
        self.assertEqual("12345", out["results"][0]["id"])
        self.assertEqual("ADD", out["results"][0]["event"])
        self.assertEqual("hello there", out["results"][0]["memory"])

    def test_add_maps_supersede_to_update(self):
        out = self._add({"result": {"event_id_hash": 999, "upsert_outcome": "update",
                                    "superseded_memory_ids": ["111"]}},
                        identity_key="user.email", truth_class="asserted")
        self.assertEqual("UPDATE", out["results"][0]["event"])
        self.assertEqual("999", out["results"][0]["id"])

    def test_add_maps_rank_guard_to_none_with_surviving_id(self):
        # Rank-guarded: event_id_hash carries the SURVIVING existing record's id.
        out = self._add({"result": {"ingested": False, "rank_guarded": True, "upsert_outcome": "rank_guarded",
                                    "event_id_hash": 777, "rejected_memory_id": "888"}},
                        identity_key="user.email", truth_class="inferred")
        self.assertEqual("NONE", out["results"][0]["event"])
        self.assertEqual("777", out["results"][0]["id"])


if __name__ == "__main__":
    unittest.main()
