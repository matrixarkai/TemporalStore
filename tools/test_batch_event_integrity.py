#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A batch ingest must keep every message, not just the first.

The sync-accept path writes ONE pending event for the whole envelope; the commit path re-emits one
event PER MESSAGE from source_event_ids. When a batch arrives with a single source id, the commit
loop used to skip every message past it -- and because the re-emitted event reused that same id,
latest-value compaction replaced the full pending event with one holding only the first message.
Ten ingested messages became one retrievable event, silently, on a call that reported success.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

SCOPE = {"account_id": "acct_local", "tenant_id": "batchtest", "user_id": "u",
         "session_id": "s0", "agent_name": "t"}


def build():
    tmp = tempfile.mkdtemp()
    adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "b.jsonl")
    return adapter, mcp.MatrixArkMcpServer(adapter, access_mode="dev")


def events(adapter):
    return [r for r in adapter.read_all() if r.get("record_type") == "context_event"]


class BatchEventIntegrityCase(unittest.TestCase):
    def test_every_batch_message_survives_commit(self):
        adapter, server = build()
        batch = [{"role": "user", "content": "Batch fact %d: token BRAVO-%02d." % (n, n)}
                 for n in range(10)]
        server.call_tool("matrixark_ingest", {"scope": SCOPE, "finalize": True,
                                              "messages": batch})
        server.call_tool("matrixark_session_commit", {"scope": SCOPE})

        stored = " ".join(str(e.get("text") or "") for e in events(adapter))
        missing = [n for n in range(10) if ("BRAVO-%02d" % n) not in stored]
        self.assertFalse(missing,
                         "commit dropped %d of 10 batch messages from context_event: %s"
                         % (len(missing), missing))

    def test_batch_produces_one_event_per_message(self):
        adapter, server = build()
        batch = [{"role": "user", "content": "Message %d." % n} for n in range(6)]
        server.call_tool("matrixark_ingest", {"scope": SCOPE, "finalize": True,
                                              "messages": batch})
        server.call_tool("matrixark_session_commit", {"scope": SCOPE})
        self.assertGreaterEqual(len(events(adapter)), 6,
                                "a 6-message batch should leave at least 6 events, found %d"
                                % len(events(adapter)))

    def test_single_message_ingest_is_unchanged(self):
        """The fix must not disturb the one-message case, which was always correct."""
        adapter, server = build()
        for n in range(3):
            server.call_tool("matrixark_ingest", {
                "scope": SCOPE, "finalize": True,
                "messages": [{"role": "user", "content": "Single %d." % n}]})
        server.call_tool("matrixark_session_commit", {"scope": SCOPE})
        stored = " ".join(str(e.get("text") or "") for e in events(adapter))
        for n in range(3):
            self.assertIn("Single %d." % n, stored)

    def test_batch_event_ids_are_distinct(self):
        """Reusing one id for many messages is what let compaction hide the loss."""
        adapter, server = build()
        batch = [{"role": "user", "content": "Distinct %d." % n} for n in range(5)]
        server.call_tool("matrixark_ingest", {"scope": SCOPE, "finalize": True,
                                              "messages": batch})
        server.call_tool("matrixark_session_commit", {"scope": SCOPE})
        ids = [e.get("event_id_hash") for e in events(adapter)]
        self.assertEqual(len(ids), len(set(ids)), "duplicate event_id_hash across batch events")


if __name__ == "__main__":
    unittest.main()
