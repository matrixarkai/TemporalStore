#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Delete-before-extract resurrection guard (correctness regression).

MatrixArk ingest is two-phase: an event is written immediately (hot_path) and queued as a pending
``session_buffer_event``; async batch extraction later runs at commit and materializes derivatives
(entities / summaries / segments + embeddings + index postings). If a memory is deleted (or its
subject forgotten) while the event is still PENDING, the delete/forget tombstone is written, but a
later commit used to RE-materialize the derivatives with fresh ids appended AFTER the tombstone --
and the order-aware ``apply_memory_tombstones`` (which only removes records that precede a tombstone)
missed them, so the deleted content came back.

The forward guard in ``session_commit`` consults the durable, order-aware tombstone sweep and skips
any pending event whose source no longer survives, so no derivative is ever materialized for a
deleted source. Because every commit trigger (manual/force, threshold, idle-timeout,
session-boundary/finalize, auto_batch_extract) funnels through ``session_commit``, one guard covers
them all; because the signal is read fresh from the durable JSONL log, it is honored across a process
reload.

These cases assert the repro goes from LEAK to SAFE, that each commit trigger is covered, that the
deleted-source signal is cross-process durable, and that the guard does NOT over-suppress (a normal
delete-after-extract still fully deletes, a sibling pending event still materializes, and re-ingesting
the same content later produces live memory again).
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

try:
    from tools.matrixark_mcp_local_adapter import surviving_source_event_ids
except ImportError:
    from matrixark_mcp_local_adapter import surviving_source_event_ids


def _scope(user: str = "alice", *, tenant: str = "tenant_dbe", session: str = "s1") -> dict:
    return {
        "account_id": "acct_local",
        "tenant_id": tenant,
        "user_id": user,
        "session_id": session,
        "agent_name": "d",
    }


class DeleteBeforeExtractCase(unittest.TestCase):
    # ------------------------------------------------------------------ helpers
    def _server(self, tmp: str):
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        self.addCleanup(server.close, timeout_s=1.0)
        return server

    def _ingest_pending(self, server, content: str, scope: dict) -> str:
        """Ingest a single message WITHOUT finalize -> the event stays pending (buffered)."""
        result = server.call_tool("matrixark_ingest", {"messages": [{"role": "user", "content": content}], "scope": scope})
        return str(result["event_id_hash"])

    def _count(self, server, scope: dict) -> int:
        return int(server.call_tool("matrixark_get_all", {"scope": scope})["count"])

    def _retrieve_blob(self, server, query: str, scope: dict) -> str:
        pack = server.call_tool("matrixark_retrieve", {"query": query, "scope": scope})
        return json.dumps(pack, default=str)

    def _assert_no_leak(self, server, scope: dict, *, needle: str = "espresso", query: str = "what does alice drink"):
        self.assertEqual(0, self._count(server, scope), "get_all must be 0 after delete-before-extract commit")
        self.assertNotIn(needle, self._retrieve_blob(server, query, scope), f"retrieve must not leak {needle!r}")

    # ------------------------------------------------------------------ core repro
    def test_repro_delete_before_extract_explicit_commit(self):
        """The exact repro: ingest (no finalize) -> delete while pending -> session_commit (force /
        finalize) -> get_all == 0 AND retrieve has no leaked content."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            mid = self._ingest_pending(server, "Alice loves espresso", _scope())
            deleted = server.call_tool("matrixark_delete", {"memory_id": mid, "scope": _scope()})
            self.assertTrue(deleted["deleted"])
            self.assertEqual(0, self._count(server, _scope()), "delete while pending must drop the memory immediately")
            # Force commit -> async extraction fires (extraction_phase == final, session-boundary).
            server.call_tool("matrixark_session_commit", {"scope": _scope()})
            self._assert_no_leak(server, _scope())

    def test_forget_before_extract(self):
        """forget(subject) while pending -> session_commit -> subject wiped, no resurrection."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            self._ingest_pending(server, "Alice loves espresso", _scope())
            forgotten = server.call_tool("matrixark_forget", {"scope": _scope(), "confirm": "alice"})
            self.assertTrue(forgotten["forgotten"])
            self.assertEqual(0, self._count(server, _scope()))
            server.call_tool("matrixark_session_commit", {"scope": _scope()})
            self._assert_no_leak(server, _scope())

    # ------------------------------------------------------------------ every commit trigger
    def test_trigger_idle_timeout_commit(self):
        """Idle-timeout commit path (force=False, idle_timeout_ms=0 -> idle_ready) -- the arg shape the
        ingest auto-path / drain_due_idle_session_commits use for an idle commit."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            mid = self._ingest_pending(server, "Alice loves espresso", _scope())
            server.call_tool("matrixark_delete", {"memory_id": mid, "scope": _scope()})
            result = server.call_tool(
                "matrixark_session_commit",
                {"scope": _scope(), "force": False, "idle_timeout_ms": 0, "commit_reason": "idle_timeout"},
            )
            # Nothing survives to extract -> the commit does not materialize derivatives.
            self.assertIn(result.get("status"), {"deferred", "empty", "committed"})
            self._assert_no_leak(server, _scope())

    def test_trigger_threshold_commit(self):
        """Pending-threshold commit path (force=False, threshold_messages=1 -> threshold_ready)."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            mid = self._ingest_pending(server, "Alice loves espresso", _scope())
            server.call_tool("matrixark_delete", {"memory_id": mid, "scope": _scope()})
            server.call_tool(
                "matrixark_session_commit",
                {"scope": _scope(), "force": False, "threshold_messages": 1, "commit_reason": "threshold"},
            )
            self._assert_no_leak(server, _scope())

    def test_trigger_finalize_via_ingest_session_boundary(self):
        """End-to-end ingest -> auto session-boundary commit wiring: ingest X (pending), delete X, then
        a second ingest carrying conversation_done=True drives the auto session_commit (force). X must
        stay gone; the boundary-driven commit must not resurrect it. (Also exercises guardrail (b): the
        second event Y materializes.)"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            mid = self._ingest_pending(server, "Alice loves espresso", _scope())
            server.call_tool("matrixark_delete", {"memory_id": mid, "scope": _scope()})
            # Second ingest with a session-boundary flag -> auto batch commit fires (force).
            server.call_tool(
                "matrixark_ingest",
                {"messages": [{"role": "user", "content": "Alice enjoys hiking on weekends"}],
                 "scope": _scope(), "conversation_done": True},
            )
            # espresso is gone; the live memory reflects only the surviving second event.
            self.assertNotIn("espresso", self._retrieve_blob(server, "what does alice drink", _scope()))
            self.assertIn("hiking", self._retrieve_blob(server, "what does alice do on weekends", _scope()))

    # ------------------------------------------------------------------ cross-process durability
    def test_cross_process_durability_fresh_adapter_commit(self):
        """Delete while pending in adapter #1, then a FRESH adapter (reload) runs the commit. The
        deleted-source signal must be durable (read from the JSONL), not in-memory-only."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "events.jsonl"
            adapter1 = mcp.MatrixArkLocalAdapter(path)
            server1 = mcp.MatrixArkMcpServer(adapter1, access_mode="dev")
            mid = self._ingest_pending(server1, "Alice loves espresso", _scope())
            server1.call_tool("matrixark_delete", {"memory_id": mid, "scope": _scope()})
            server1.close(timeout_s=1.0)

            # Fresh process view: brand-new adapter + server over the same durable log.
            adapter2 = mcp.MatrixArkLocalAdapter(path)
            server2 = mcp.MatrixArkMcpServer(adapter2, access_mode="dev")
            self.addCleanup(server2.close, timeout_s=1.0)
            server2.call_tool("matrixark_session_commit", {"scope": _scope()})
            self._assert_no_leak(server2, _scope())

    # ------------------------------------------------------------------ guardrails: no over-suppression
    def test_guardrail_delete_after_extract_still_deletes(self):
        """A normal delete AFTER extraction still fully deletes (the pre-existing path must keep
        working -- the guard must not weaken it)."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            mid = self._ingest_pending(server, "Alice loves espresso", _scope())
            # Extract FIRST (materialize derivatives), then delete.
            server.call_tool("matrixark_session_commit", {"scope": _scope()})
            self.assertEqual(1, self._count(server, _scope()))
            deleted = server.call_tool("matrixark_delete", {"memory_id": mid, "scope": _scope()})
            self.assertTrue(deleted["deleted"])
            self._assert_no_leak(server, _scope())

    def test_guardrail_sibling_pending_event_still_materializes(self):
        """Deleting event X must NOT suppress a DIFFERENT pending event Y in the same session: Y's
        derivatives still materialize normally on commit."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            x = self._ingest_pending(server, "Alice loves espresso", _scope())
            self._ingest_pending(server, "Alice drives a red bicycle", _scope())
            server.call_tool("matrixark_delete", {"memory_id": x, "scope": _scope()})
            server.call_tool("matrixark_session_commit", {"scope": _scope()})
            # X gone, Y alive.
            self.assertNotIn("espresso", self._retrieve_blob(server, "what does alice drink", _scope()))
            blob_y = self._retrieve_blob(server, "what does alice ride", _scope())
            self.assertIn("bicycle", blob_y, "the sibling pending event Y must still materialize")
            self.assertGreaterEqual(self._count(server, _scope()), 1)

    def test_guardrail_reingest_same_content_produces_live_memory(self):
        """After a delete-before-extract, RE-INGESTING the same content later DOES produce live memory
        again -- the suppression is per event / tombstone (order-aware), not a permanent content block."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            server = self._server(tmp)
            mid = self._ingest_pending(server, "Alice loves espresso", _scope())
            server.call_tool("matrixark_delete", {"memory_id": mid, "scope": _scope()})
            server.call_tool("matrixark_session_commit", {"scope": _scope()})
            self._assert_no_leak(server, _scope())
            # Re-ingest the SAME content (new event_id_hash, appended AFTER the tombstone) and commit.
            new_id = self._ingest_pending(server, "Alice loves espresso", _scope())
            self.assertNotEqual(mid, new_id, "re-ingest must mint a fresh event id (time-salted hash)")
            server.call_tool("matrixark_session_commit", {"scope": _scope()})
            self.assertGreaterEqual(self._count(server, _scope()), 1, "re-ingested content must be live again")
            self.assertIn("espresso", self._retrieve_blob(server, "what does alice drink", _scope()))

    # ------------------------------------------------------------------ unit: the skip predicate itself
    def test_surviving_source_event_ids_predicate(self):
        """Directly unit-test the order-aware skip predicate that drives the forward guard."""
        # No tombstone -> fast path returns None (caller keeps every pending event).
        self.assertIsNone(surviving_source_event_ids([{"record_type": "context_event", "event_id_hash": 1}]))
        # A delete tombstone AFTER an event removes it; an event appended AFTER the tombstone survives
        # (order-aware): this is exactly why a re-ingest of deleted content comes back to life.
        records = [
            {"record_type": "context_event", "event_id_hash": 1},
            {"record_type": "context_event", "event_id_hash": 2},
            {"record_type": "matrixark_memory_tombstone", "tombstone_kind": "delete", "target_memory_id": "1", "closure": True},
            {"record_type": "context_event", "event_id_hash": 1},  # re-ingest (same id here) after tombstone
            {"record_type": "context_event", "event_id_hash": 3},
        ]
        surviving = surviving_source_event_ids(records)
        self.assertEqual({"1", "2", "3"}, surviving)  # id 1 re-appears after the tombstone -> survives


if __name__ == "__main__":
    unittest.main()
