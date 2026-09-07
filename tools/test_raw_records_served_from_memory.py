#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The raw durable log is served from memory, and a write is still visible immediately.

`_read_raw_records` re-read and re-parsed every line of every retained shard on every call, then
expanded interned metadata over the result. `history` measured 761 ms against a 150-message store
and 960 ms at 600; served from memory it is 5 ms.

The risk a cache introduces here is staleness on a durable path: a record written after a read must
appear in the next one, and a tombstone must be seen by the code that counts them.
"""
from __future__ import annotations
import tempfile, unittest
from pathlib import Path
import matrixark_mcp_server as mcp

ANCHOR_MS = 1_780_000_000_000

def _scope(user: str = "alice") -> dict:
    return {"account_id": "acct_local", "tenant_id": "tenant_raw", "user_id": user,
            "session_id": "s1", "agent_name": "t"}

class RawRecordsMemoryCase(unittest.TestCase):
    def test_a_write_is_visible_to_the_next_raw_read(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            for i in range(4):
                server.call_tool("matrixark_ingest", {
                    "messages": [{"role": "user", "content": f"FACT{i}"}],
                    "scope": _scope(), "ingestion_time_ms": ANCHOR_MS + i * 1000,
                })
            first = adapter._read_raw_records()
            self.assertTrue(first, "the fill must produce raw records")
            again = adapter._read_raw_records()
            self.assertEqual(len(first), len(again), "a second read with no write sees the same log")

            # a write must invalidate: the new row has to be in the next raw read
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "AFTERWARDS"}],
                "scope": _scope(), "ingestion_time_ms": ANCHOR_MS + 90_000,
            })
            after = adapter._read_raw_records()
            self.assertGreater(len(after), len(first),
                               "a record written after a raw read must appear in the next one")
            self.assertIn("AFTERWARDS", str(after),
                          "the raw log went stale: a durable write is missing from it")

    def test_a_delete_is_visible_to_the_raw_reader(self) -> None:
        """Tombstones live in the raw log, and code counts them there."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            for i in range(3):
                server.call_tool("matrixark_ingest", {
                    "messages": [{"role": "user", "content": f"FACT{i}"}],
                    "scope": _scope(), "ingestion_time_ms": ANCHOR_MS + i * 1000,
                })
            adapter._read_raw_records()                     # prime the cache
            before = adapter._count_raw_tombstones()
            listed = server.call_tool("matrixark_get_all", {"scope": _scope(), "limit": 1})
            memory_id = str(listed["memories"][0]["id"])
            server.call_tool("matrixark_delete", {"scope": _scope(), "memory_id": memory_id})
            after = adapter._count_raw_tombstones()
            self.assertGreater(after, before,
                               "a tombstone written after a raw read must be counted")

if __name__ == "__main__":
    unittest.main(verbosity=2)
