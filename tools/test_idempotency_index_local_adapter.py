#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The base adapter answers an idempotency lookup from an index, and the index stays current.

The scan it replaced walked `read_all()` to prove a key absent -- 0.269 ms over 721 records and
4.720 ms over 3,177, because `read_all()` re-materialises the serving view on every call."""
from __future__ import annotations
import tempfile, unittest
from pathlib import Path
import matrixark_mcp_server as mcp

ANCHOR_MS = 1_780_000_000_000

def scope():
    return {"account_id": "acct_local", "tenant_id": "tenant_idem", "user_id": "alice",
            "session_id": "s1", "agent_name": "t"}

class IdempotencyIndexCase(unittest.TestCase):
    def test_index_and_scan_agree(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self.addCleanup(server.close, timeout_s=1.0)
            for i in range(25):
                server.call_tool("matrixark_ingest", {
                    "messages": [{"role": "user", "content": f"FACT{i}"}],
                    "scope": scope(), "ingestion_time_ms": ANCHOR_MS + i * 1000,
                })
            keys = [r.get("key_hash") for r in adapter.read_all()
                    if r.get("record_type") == "matrixark_idempotency"]
            self.assertTrue(keys, "the fill must write idempotency records, or this proves nothing")

            # Index and scan must agree on what the CALLER reads. The replay path uses
            # `record["response"]` and the key; it does not read the serving-materialisation
            # fields (storage_options and friends) that a row picks up on the way back out of the
            # store. The temporal adapter's index stores the same built record for the same
            # reason, so this is the contract both indexes keep -- not a looser one for this fix.
            for key in keys:
                scanned = adapter.find_idempotency_record_by_scan(key)
                indexed = adapter.find_idempotency_record(key)
                self.assertIsNotNone(indexed, "index lost key %r" % key)
                self.assertEqual(scanned.get("key_hash"), indexed.get("key_hash"))
                self.assertEqual(scanned.get("response"), indexed.get("response"),
                                 "index and scan disagree about the replayed response for %r" % key)
                self.assertEqual(scanned.get("tool_name"), indexed.get("tool_name"))
            # and a key that is absent must be absent both ways
            absent = 9_123_456_789
            self.assertIsNone(adapter.find_idempotency_record_by_scan(absent))
            self.assertIsNone(adapter.find_idempotency_record(absent))

            # a record appended AFTER the index was built must be visible
            adapter.append_idempotency_record(
                key_hash=absent, tool_name="matrixark_ingest", raw_key="k",
                identity={"tenant_id": "tenant_idem"}, response={"ok": True},
            )
            found = adapter.find_idempotency_record(absent)
            self.assertIsNotNone(found, "a record appended after the index was built must be found")
            self.assertEqual(found.get("response"),
                             adapter.find_idempotency_record_by_scan(absent).get("response"))

if __name__ == "__main__":
    unittest.main(verbosity=2)
