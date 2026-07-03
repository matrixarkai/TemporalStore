#!/usr/bin/env python3
"""Regression tests for compact MatrixArk debug trace reports."""

from __future__ import annotations

import json
import unittest

from tools import matrixark_mcp_core as core
from tools import run_matrixark_message_pdf_debug_trace as trace_runner


class MatrixArkDebugTraceCompactionTest(unittest.TestCase):
    def test_context_pack_compaction_is_idempotent_for_grouped_pack(self) -> None:
        grouped_pack = {
            "context_pack_id": "pack-1",
            "groups": [
                {
                    "type": "event",
                    "n": 1,
                    "items": [{"text": "Alice approved Project Aurora.", "tokens": 5}],
                }
            ],
            "tokens": {"remote": 5, "total": 5, "remote_budget": 100},
        }

        compact = core.compact_context_pack_for_serving(grouped_pack)

        self.assertEqual(compact["groups"], grouped_pack["groups"])
        self.assertEqual(compact["tokens"], grouped_pack["tokens"])

    def test_trace_export_drops_raw_scope_and_replay_payloads(self) -> None:
        trace = {
            "scope": {
                "account_id": "acct_local",
                "tenant_id": "tenant_codex",
                "user_id": "deeproute",
                "session_id": "s1",
                "scope_key": "t=1|u=2|s=3|",
                "session_hash": 3,
            },
            "query": "What changed?",
            "embedding_model": "model",
            "embedding_execution_mode": "deterministic",
            "summary_refresh_policy": {},
            "resources": [{"raw_uri": "/tmp/fixtures/a.pdf", "resource_type": "pdf", "title": "A", "line_count": 1}],
            "calls": [
                {
                    "tool": "matrixark_replay",
                    "result": {
                        "status": "ok",
                        "access": {"scope_key": "t=1|u=2|s=3|", "session_hash": 3},
                        "events": [{"context_event_key": "001:abc", "source_locator": "/tmp/a.pdf#page=1"}],
                    },
                }
            ],
        }

        compact = trace_runner.compact_trace(trace)
        payload = json.dumps(compact, sort_keys=True)

        self.assertIn("matrixark_replay", payload)
        self.assertNotIn("scope_key", payload)
        self.assertNotIn("session_hash", payload)
        self.assertNotIn("context_event_key", payload)
        self.assertNotIn("source_locator", payload)


if __name__ == "__main__":
    unittest.main()
