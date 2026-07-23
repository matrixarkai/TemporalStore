#!/usr/bin/env python3
from __future__ import annotations

import json
import os
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

    def test_refresh_summaries_uses_openai_compatible_model_for_l1_when_required(self) -> None:
        old_provider = os.environ.get("MATRIXARK_SUMMARY_PROVIDER")
        old_require = os.environ.get("MATRIXARK_REQUIRE_OSS_UNDERSTANDING")
        self.addCleanup(lambda: os.environ.__setitem__("MATRIXARK_SUMMARY_PROVIDER", old_provider) if old_provider is not None else os.environ.pop("MATRIXARK_SUMMARY_PROVIDER", None))
        self.addCleanup(lambda: os.environ.__setitem__("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", old_require) if old_require is not None else os.environ.pop("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", None))

        summary_globals = mcp.synthesize_context_node_summary.__globals__
        old_call = summary_globals["openai_compatible_json_call"]
        self.addCleanup(lambda: summary_globals.__setitem__("openai_compatible_json_call", old_call))

        def fake_json_call(*, system: str, user: str, model: str | None = None, max_tokens: int | None = None) -> dict:
            payload = json.loads(user)
            level = payload["summary_level"]
            return {"summary_text": f"OSS {level} synthesis: Alice approved Aurora, Bob owns procurement, cap is current."}

        summary_globals["openai_compatible_json_call"] = fake_json_call

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {
                "account_id": "acct_local",
                "tenant_id": "tenant_summary_oss",
                "user_id": "worker_user",
                "session_id": "worker_session",
                "agent_name": "test",
            }
            for idx in range(3):
                adapter.ingest(
                    {
                        "messages": [
                            {
                                "role": "user",
                                "content": f"OSS summary event {idx}: Alice approved Project Aurora and Bob owns procurement.",
                            }
                        ],
                        "scope": scope,
                        "metadata": {"node_path": ["tenant:summary", "user:worker", "session:worker"]},
                    }
                )

            os.environ["MATRIXARK_SUMMARY_PROVIDER"] = "openai_compatible"
            os.environ["MATRIXARK_REQUIRE_OSS_UNDERSTANDING"] = "1"
            adapter.refresh_summaries({"scope": scope, "force": True})
            summaries = [record for record in adapter.read_all() if record.get("record_type") == "context_summary"]
            l1 = [record for record in summaries if record.get("summary_type") == "node_l1"]
            self.assertTrue(l1)
            self.assertTrue(any(str(record.get("summary_text", "")).startswith("OSS node_l1 synthesis") for record in l1))
            for record in l1:
                provider = record.get("summary_generation_policy", {}).get("summary_provider", {})
                self.assertEqual("openai_compatible", provider.get("provider"))
                self.assertEqual("llm_json", provider.get("execution_mode"))
                self.assertFalse(provider.get("fallback_used"))



if __name__ == "__main__":
    unittest.main()
