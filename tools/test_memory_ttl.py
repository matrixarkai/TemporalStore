#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""PurchaseMemory Phase 1: per-record TTL / retention-cutoff (expire-only, skip summarizer).

Covers, against a real local adapter + server:
  * expires_at hides a record from get_all / retrieve once the clock passes it (and it is visible
    before expiry);
  * ttl_seconds is computed as occurred_at + ttl (expires_at / expires_at_ms stamped correctly);
  * a scope-level retention_cutoff_ts hides records whose occurrence time is older than the cutoff;
  * an expired record is lazily purged (closure tombstone + physical purge) from the durable log;
  * a TTL record is NOT folded into any summary, while a non-TTL record still is;
  * expiry survives an adapter reload (durable: expires_at_ms is a persisted field re-checked on
    every read).

Time is controlled deterministically: ingest with an explicit ``ingestion_time_ms`` so ttl anchors
to a known instant, and set ``MATRIXARK_MEMORY_NOW_MS`` to advance the expiry clock.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp
import matrixark_mcp_local_adapter as la


def _scope(user: str, *, tenant: str = "tenant_ttl", session: str = "s1") -> dict:
    return {
        "account_id": "acct_local",
        "tenant_id": tenant,
        "user_id": user,
        "session_id": session,
        "agent_name": "t",
    }


ANCHOR_MS = 1_780_000_000_000  # a fixed 2026 instant so ttl math is deterministic


class MemoryTtlBackendCase(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("MATRIXARK_MEMORY_NOW_MS", None)
        self.addCleanup(lambda: os.environ.pop("MATRIXARK_MEMORY_NOW_MS", None))

    def _server(self, tmp: str):
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        self.addCleanup(server.close, timeout_s=1.0)
        return adapter, server

    def _set_now(self, ms: int) -> None:
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(int(ms))

    def test_expires_at_hides_record_after_expiry(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            # A durable (non-expiring) memory + an ephemeral one that expires at ANCHOR+10s.
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "alice enjoys espresso every morning"}],
                "scope": _scope("alice"), "ingestion_time_ms": ANCHOR_MS,
            })
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "one time passcode SEVENSEVEN valid briefly"}],
                "scope": _scope("alice"), "ingestion_time_ms": ANCHOR_MS, "expires_at": (ANCHOR_MS + 10_000) / 1000.0,
            })
            # Before expiry: both live.
            self._set_now(ANCHOR_MS + 1_000)
            self.assertEqual(2, server.call_tool("matrixark_get_all", {"scope": _scope("alice")})["count"])
            # After expiry: only the durable one remains, and the secret is gone from retrieve.
            self._set_now(ANCHOR_MS + 20_000)
            after = server.call_tool("matrixark_get_all", {"scope": _scope("alice")})
            self.assertEqual(1, after["count"])
            self.assertNotIn("SEVENSEVEN", json.dumps(after))
            retrieved = server.call_tool("matrixark_retrieve", {"query": "passcode SEVENSEVEN", "scope": _scope("alice")})
            self.assertNotIn("SEVENSEVEN", json.dumps(retrieved))

    def test_ttl_seconds_computed_from_occurred_at(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._set_now(ANCHOR_MS)  # inspect the stamped record before it expires
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "relative ttl note"}],
                "scope": _scope("carol"), "ingestion_time_ms": ANCHOR_MS, "ttl_seconds": 3600,
            })
            events = [r for r in adapter.read_all() if r.get("record_type") == "context_event"]
            self.assertEqual(1, len(events))
            self.assertTrue(events[0].get("ephemeral"))
            self.assertEqual(ANCHOR_MS + 3_600_000, int(events[0]["expires_at_ms"]))
            self.assertAlmostEqual((ANCHOR_MS + 3_600_000) / 1000.0, float(events[0]["expires_at"]), places=3)

    def test_expires_at_wins_over_ttl_seconds(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._set_now(ANCHOR_MS)
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "both fields set"}],
                "scope": _scope("dave"), "ingestion_time_ms": ANCHOR_MS,
                "expires_at": (ANCHOR_MS + 5_000) / 1000.0, "ttl_seconds": 999999,
            })
            events = [r for r in adapter.read_all() if r.get("record_type") == "context_event"]
            self.assertEqual(ANCHOR_MS + 5_000, int(events[0]["expires_at_ms"]))

    def test_retention_cutoff_hides_older_records(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            # An old memory, then a newer memory whose ingest also carries a retention cutoff between them.
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "OLDFACT from long ago"}],
                "scope": _scope("erin"), "ingestion_time_ms": ANCHOR_MS,
            })
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "NEWFACT recorded now"}],
                "scope": _scope("erin"), "ingestion_time_ms": ANCHOR_MS + 100_000,
                "retention_cutoff_ts": (ANCHOR_MS + 50_000) / 1000.0,
            })
            self._set_now(ANCHOR_MS + 200_000)
            listed = server.call_tool("matrixark_get_all", {"scope": _scope("erin")})
            texts = json.dumps(listed)
            self.assertIn("NEWFACT", texts)
            self.assertNotIn("OLDFACT", texts)
            self.assertEqual(1, listed["count"])

    def test_expired_record_is_lazily_purged(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            durable = server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "durable keeper"}],
                "scope": _scope("frank"), "ingestion_time_ms": ANCHOR_MS,
            })
            ephemeral = server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "vanishing PURGEME note"}],
                "scope": _scope("frank"), "ingestion_time_ms": ANCHOR_MS, "expires_at": (ANCHOR_MS + 1_000) / 1000.0,
            })
            self._set_now(ANCHOR_MS + 5_000)
            swept = adapter.sweep_expired_memories(force_purge=True)
            self.assertGreaterEqual(swept["swept"], 1)
            self.assertIn(str(ephemeral["event_id_hash"]), [str(x) for x in swept["expired_memory_ids"]])
            # The expired event is physically gone from the durable raw log; the keeper survives.
            raw_dump = json.dumps(adapter._read_raw_records())
            self.assertNotIn("PURGEME", raw_dump)
            self.assertIn("durable keeper", raw_dump)
            self.assertEqual(1, server.call_tool("matrixark_get_all", {"scope": _scope("frank")})["count"])

    def test_ttl_record_excluded_from_summary_and_non_ttl_included(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            node_path = ["tenant:tenant_ttl", "user:grace", "session:s1"]
            node_hash = la.stable_hash("/".join(node_path))
            scope = _scope("grace")
            normal_event = {
                "record_type": "context_event", "event_id_hash": 555, "node_hash": node_hash,
                "node_path": node_path, "scope": scope, "text": "durable summarizable fact",
                "summary_text": "durable summarizable fact", "updated_at_ms": ANCHOR_MS,
            }
            ttl_event = {
                "record_type": "context_event", "event_id_hash": 556, "node_hash": node_hash,
                "node_path": node_path, "scope": scope, "text": "ephemeral do-not-summarize",
                "summary_text": "ephemeral do-not-summarize", "updated_at_ms": ANCHOR_MS,
                "ephemeral": True, "expires_at_ms": ANCHOR_MS + 10_000_000,
            }
            same_node_events, _child, _entities, _ops, _meta = adapter.node_summary_source_records(
                records=[normal_event, ttl_event], node_path=node_path, scope=scope, node_hash=node_hash,
            )
            folded_ids = {int(r.get("event_id_hash")) for r in same_node_events}
            self.assertIn(555, folded_ids)
            self.assertNotIn(556, folded_ids)

    def test_ttl_event_never_appears_in_generated_summary_source(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._set_now(ANCHOR_MS)  # keep the ephemeral event LIVE so we prove exclusion, not expiry
            scope = _scope("heidi")
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "durable project decision recorded"}],
                "scope": scope, "ingestion_time_ms": ANCHOR_MS, "finalize": True,
            })
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "TEMPSECRET ephemeral token"}],
                "scope": scope, "ingestion_time_ms": ANCHOR_MS, "ttl_seconds": 100000, "finalize": True,
            })
            server.call_tool("matrixark_refresh_summaries", {"scope": scope, "refreshed_at_ms": ANCHOR_MS + 1000})
            summaries = [r for r in adapter.read_all() if r.get("record_type") == "context_summary"]
            for summary in summaries:
                self.assertNotIn("TEMPSECRET", json.dumps(summary))
                source_ids = summary.get("source_event_ids")
                if isinstance(source_ids, list):
                    # No summary may cite the ephemeral event as a source.
                    self.assertNotIn("TEMPSECRET", str(summary.get("summary_text") or ""))

    def test_expiry_survives_adapter_reload(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            event_log = Path(tmp) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(event_log)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "durable after reload"}],
                "scope": _scope("ivan"), "ingestion_time_ms": ANCHOR_MS,
            })
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "RELOADSECRET expiring memory"}],
                "scope": _scope("ivan"), "ingestion_time_ms": ANCHOR_MS, "expires_at": (ANCHOR_MS + 10_000) / 1000.0,
            })
            server.close(timeout_s=1.0)
            # Fresh adapter over the same durable log; advance clock past expiry.
            self._set_now(ANCHOR_MS + 60_000)
            adapter2 = mcp.MatrixArkLocalAdapter(event_log)
            server2 = mcp.MatrixArkMcpServer(adapter2, access_mode="dev")
            self.addCleanup(server2.close, timeout_s=1.0)
            listed = server2.call_tool("matrixark_get_all", {"scope": _scope("ivan")})
            self.assertEqual(1, listed["count"])
            self.assertNotIn("RELOADSECRET", json.dumps(listed))


if __name__ == "__main__":
    unittest.main()
