#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`read_all` caches the expiry GUARD's answer, never the expiry decision.

The guard walks every record to ask whether any of them carries a TTL, and when the answer is no it
returns the input untouched -- 2.734 ms over 2,125 records, 87% of a read, to prove nothing has
expired. The answer is cached on the same signature the compacted cache uses for itself.

The case that would break it is a stale NO: a store with no TTLs caches "nothing to filter", and a
TTL record then arrives. This walks exactly that sequence.
"""
from __future__ import annotations
import os, tempfile, unittest
from pathlib import Path
import matrixark_mcp_server as mcp

ANCHOR_MS = 1_780_000_000_000

def _scope(user: str = "alice") -> dict:
    return {"account_id": "acct_local", "tenant_id": "tenant_guard", "user_id": user,
            "session_id": "s1", "agent_name": "t"}

class ExpiryGuardMemoCase(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("MATRIXARK_MEMORY_NOW_MS", None)
        self.addCleanup(lambda: os.environ.pop("MATRIXARK_MEMORY_NOW_MS", None))

    def test_a_ttl_record_still_expires_after_a_ttl_free_read(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)

            # 1. a store with no TTLs at all -- this read caches "nothing needs filtering"
            for i in range(5):
                server.call_tool("matrixark_ingest", {
                    "messages": [{"role": "user", "content": f"DURABLE{i}"}],
                    "scope": _scope(), "ingestion_time_ms": ANCHOR_MS + i * 1000,
                })
            before = server.call_tool("matrixark_get_all", {"scope": _scope()})
            self.assertEqual(5, before["count"], "the durable fill must be visible")

            # 2. now one WITH a ttl, written after that cached answer
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "EPHEMERAL secret"}],
                "scope": _scope(), "ingestion_time_ms": ANCHOR_MS + 10_000,
                "expires_at": (ANCHOR_MS + 20_000) / 1000.0,
            })
            os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 11_000)
            live = server.call_tool("matrixark_get_all", {"scope": _scope()})
            self.assertEqual(6, live["count"], "before expiry the ttl record is live")
            self.assertIn("EPHEMERAL", str(live))

            # 3. past its expiry, with NO intervening write -- the promise read_all makes
            os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 30_000)
            after = server.call_tool("matrixark_get_all", {"scope": _scope()})
            self.assertNotIn("EPHEMERAL", str(after),
                             "an expired record survived a read: the guard answer went stale")
            self.assertEqual(5, after["count"])

    def test_the_guard_answer_is_reused_between_writes(self) -> None:
        """The point of the memo: repeated reads do not re-walk the records."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            for i in range(5):
                server.call_tool("matrixark_ingest", {
                    "messages": [{"role": "user", "content": f"FACT{i}"}],
                    "scope": _scope(), "ingestion_time_ms": ANCHOR_MS + i * 1000,
                })
            adapter.read_all()
            memo = getattr(adapter, "_expiry_filter_memo", None)
            self.assertIsNotNone(memo, "a read must leave the guard answer cached")
            self.assertFalse(memo[1], "this store has no ttl records")
            first = memo[0]
            adapter.read_all()
            self.assertEqual(first, getattr(adapter, "_expiry_filter_memo")[0],
                             "a second read with no write must reuse the same signature")

if __name__ == "__main__":
    unittest.main(verbosity=2)
