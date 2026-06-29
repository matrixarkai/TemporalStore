#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp


class MatrixArkSummaryWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self._old_interval = mcp.SUMMARY_REFRESH_INTERVAL_MS
        self._old_limit = mcp.SUMMARY_REFRESH_LIMIT

    def tearDown(self) -> None:
        mcp.SUMMARY_REFRESH_INTERVAL_MS = self._old_interval
        mcp.SUMMARY_REFRESH_LIMIT = self._old_limit

    def test_background_worker_refreshes_dirty_nodes_and_embeddings(self) -> None:
        mcp.SUMMARY_REFRESH_INTERVAL_MS = 100
        mcp.SUMMARY_REFRESH_LIMIT = 64
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            scope = {
                "account_id": "acct_local",
                "tenant_id": "tenant_summary_worker",
                "user_id": "worker_user",
                "session_id": "worker_session",
                "agent_name": "test",
            }
            for idx in range(3):
                server.call_tool(
                    "matrixark_ingest",
                    {
                        "messages": [
                            {
                                "role": "user",
                                "content": f"Summary worker test event {idx}: Alice approved Project Aurora item {idx}.",
                            }
                        ],
                        "scope": scope,
                        "metadata": {"node_path": ["tenant:summary", "user:worker", "session:worker"]},
                    },
                )
            deadline = time.time() + 3.0
            records = []
            while time.time() < deadline:
                records = adapter.read_all()
                summary_types = {r.get("summary_type") for r in records if r.get("record_type") == "context_summary"}
                embedding_types = {r.get("embedding_type") for r in records if r.get("record_type") == "context_embedding"}
                if {"node_l0", "node_l1"}.issubset(summary_types) and {"node_l0", "node_l1"}.issubset(embedding_types):
                    break
                time.sleep(0.05)
            summary_types = {r.get("summary_type") for r in records if r.get("record_type") == "context_summary"}
            embedding_types = {r.get("embedding_type") for r in records if r.get("record_type") == "context_embedding"}
            self.assertIn("node_l0", summary_types)
            self.assertIn("node_l1", summary_types)
            self.assertIn("node_l0", embedding_types)
            self.assertIn("node_l1", embedding_types)
            dirty_markers = [r for r in records if r.get("record_type") == "context_summary_dirty"]
            self.assertTrue(any(r.get("status") == "completed" for r in dirty_markers))
            audits = [r for r in records if r.get("record_type") == "context_summary_refresh_audit"]
            self.assertFalse(audits)


if __name__ == "__main__":
    unittest.main()
