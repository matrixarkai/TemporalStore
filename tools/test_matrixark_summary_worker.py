#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp
from matrixark_mcp_core import scope_key_from_hashes, stable_hash
from matrixark_mcp_summary_dirty import pending_dirty_node_records


class MatrixArkSummaryWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self._old_interval = mcp.SUMMARY_REFRESH_INTERVAL_MS
        self._old_limit = mcp.SUMMARY_REFRESH_LIMIT

    def tearDown(self) -> None:
        mcp.SUMMARY_REFRESH_INTERVAL_MS = self._old_interval
        mcp.SUMMARY_REFRESH_LIMIT = self._old_limit

    def test_pending_dirty_uses_sets_for_missing_or_empty_summaries(self) -> None:
        dirty_globals = pending_dirty_node_records.__globals__
        old_debug = dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"]
        dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"] = True
        self.addCleanup(lambda: dirty_globals.__setitem__("ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS", old_debug))
        scope = {
            "account_id": "acct_local",
            "tenant_id": "tenant_summary_dirty_set",
            "user_id": "worker_user",
            "session_id": "worker_session",
            "agent_name": "test",
        }
        node_path = ["tenant:summary", "user:worker", "session:worker"]
        node_hash = stable_hash("/".join(node_path))
        event = {
            "record_type": "context_event",
            "event_id_hash": 101,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "text": "summary source event",
            "updated_at_ms": 1780000000000,
        }

        pending = pending_dirty_node_records(
            records=[event],
            scope=scope,
            limit=8,
            refreshed_at_ms=1780000001000,
            max_raw_events_per_node=100,
            min_compression_event_age_ms=0,
            context_event_ingestion_time_ms=lambda record, _debug=None: int(record.get("updated_at_ms") or 0),
        )
        self.assertEqual(["missing_or_empty_summary"], [record["dirty_reason"] for record in pending.values()])
        dirty_hash = next(iter(pending.values()))["dirty_hash"]

        empty_summary = {
            "record_type": "context_summary",
            "summary_type": "node_l0",
            "summary_hash": stable_hash(f"context_summary:node_l0:{node_hash}"),
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "summary_text": "",
            "updated_at_ms": 1780000000500,
        }
        pending_with_empty = pending_dirty_node_records(
            records=[event, empty_summary],
            scope=scope,
            limit=8,
            refreshed_at_ms=1780000001000,
            max_raw_events_per_node=100,
            min_compression_event_age_ms=0,
            context_event_ingestion_time_ms=lambda record, _debug=None: int(record.get("updated_at_ms") or 0),
        )
        self.assertTrue(next(iter(pending_with_empty.values()))["empty_summary_seen"])

        completed = {
            "record_type": "context_summary_dirty",
            "dirty_hash": dirty_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "status": "completed",
            "updated_at_ms": 1780000002000,
        }
        non_empty_summary = {**empty_summary, "summary_text": "fresh summary", "updated_at_ms": 1780000002000}
        pending_after_refresh = pending_dirty_node_records(
            records=[event, empty_summary, completed, non_empty_summary],
            scope=scope,
            limit=8,
            refreshed_at_ms=1780000003000,
            max_raw_events_per_node=100,
            min_compression_event_age_ms=0,
            context_event_ingestion_time_ms=lambda record, _debug=None: int(record.get("updated_at_ms") or 0),
        )
        self.assertEqual({}, pending_after_refresh)

    def test_refresh_dirty_node_summaries_refreshes_missing_summary_without_marker_spam(self) -> None:
        dirty_helper = mcp.MatrixArkLocalAdapter.refresh_dirty_node_summaries.__globals__["pending_dirty_node_records"]
        dirty_globals = dirty_helper.__globals__
        old_debug = dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"]
        dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"] = True
        self.addCleanup(lambda: dirty_globals.__setitem__("ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS", old_debug))
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = {
                "tenant_hash": 4242,
                "scope_key": scope_key_from_hashes(4242, 0, 0),
            }
            node_path = ["tenant:summary", "user:worker", "session:missing"]
            node_hash = stable_hash("/".join(node_path))
            adapter.append(
                {
                    "record_type": "context_node",
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "scope_key": scope["scope_key"],
                    "tenant_hash": scope["tenant_hash"],
                    "updated_at_ms": 1780000000000,
                }
            )
            adapter.append(
                {
                    "record_type": "context_event",
                    "event_id_hash": 202,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "scope_key": scope["scope_key"],
                    "tenant_hash": scope["tenant_hash"],
                    "text": "A node with events but no summary should refresh once.",
                    "updated_at_ms": 1780000000000,
                }
            )

            result = adapter.refresh_dirty_node_summaries(
                scope=scope,
                limit=8,
                refreshed_at_ms=1780000001000,
                max_raw_events_per_node=100,
                min_compression_event_age_ms=0,
            )
            records = adapter.read_all()
            summaries = [record for record in records if record.get("record_type") == "context_summary"]
            dirty_markers = [record for record in records if record.get("record_type") == "context_summary_dirty"]

            self.assertEqual(1, result["refreshed_count"])
            self.assertTrue(summaries)
            self.assertTrue(any(record.get("dirty_reason") == "missing_or_empty_summary" for record in dirty_markers))
            self.assertTrue(any(record.get("status") == "completed" for record in dirty_markers))

    def test_summary_dirty_markers_are_compact_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            dirty_globals = adapter.node_summary_dirty_records.__globals__
            old_debug = dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"]
            dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"] = False
            self.addCleanup(lambda: dirty_globals.__setitem__("ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS", old_debug))
            scope = {
                "account_id": "acct_local",
                "tenant_id": "tenant_summary_compact",
                "user_id": "worker_user",
                "session_id": "worker_session",
                "agent_name": "test",
            }

            adapter.mark_node_summary_dirty(
                node_path=["tenant:summary", "user:worker", "session:compact"],
                scope=scope,
                updated_at_ms=1780000000000,
                source_ref_type="event",
                source_hash_field="source_event_hash",
                source_hash=stable_hash("compact-dirty-event"),
                dirty_reason="new_event",
            )

            dirty_markers = [r for r in adapter.read_all() if r.get("record_type") == "context_summary_dirty"]
            self.assertTrue(dirty_markers)
            for marker in dirty_markers:
                # What "compact" leaves out is the DEBUG lineage, which is what the flag gates.
                self.assertNotIn("changed_ref_count", marker)
                self.assertNotIn("propagate_depth", marker)
                self.assertNotIn("source_role_counts", marker)
                self.assertNotIn("source_codex_events", marker)
                self.assertNotIn("empty_summary_seen", marker)
                # What it keeps is the part the store itself reads. Recovery branches on
                # dirty_reason (profile_entity_promoted), and both dashboards and the summary
                # runtime read all three of these, so a marker without them is not compact --
                # it is broken. Asserting their absence asked for that.
                self.assertEqual("new_event", marker.get("dirty_reason"))
                self.assertEqual("event", marker.get("source_ref_type"))
                self.assertIn("source_event_hash", marker)

    def test_summary_dirty_debug_fields_are_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            dirty_globals = adapter.node_summary_dirty_records.__globals__
            old_debug = dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"]
            dirty_globals["ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS"] = True
            self.addCleanup(lambda: dirty_globals.__setitem__("ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS", old_debug))
            scope = {
                "account_id": "acct_local",
                "tenant_id": "tenant_summary_debug",
                "user_id": "worker_user",
                "session_id": "worker_session",
                "agent_name": "test",
            }

            adapter.mark_node_summary_dirty(
                node_path=["tenant:summary", "user:worker", "session:debug"],
                scope=scope,
                updated_at_ms=1780000000000,
                source_ref_type="event",
                source_hash_field="source_event_hash",
                source_hash=stable_hash("debug-dirty-event"),
                dirty_reason="new_event",
            )

            dirty_markers = [r for r in adapter.read_all() if r.get("record_type") == "context_summary_dirty"]
            self.assertTrue(dirty_markers)
            for marker in dirty_markers:
                # These three are written either way -- see the compact test above -- so they
                # document the shape rather than guard the flag. The gated ones below are what
                # turning it on actually adds.
                self.assertEqual("new_event", marker.get("dirty_reason"))
                self.assertEqual("event", marker.get("source_ref_type"))
                self.assertIn("source_event_hash", marker)
                self.assertEqual(1, marker.get("changed_ref_count"))
                self.assertIn("propagate_depth", marker)

    def test_background_worker_refreshes_dirty_nodes_and_embeddings(self) -> None:
        mcp.SUMMARY_REFRESH_INTERVAL_MS = 100
        mcp.SUMMARY_REFRESH_LIMIT = 64
        # Not a `with` block: the worker started below writes into tmpdir on its own thread,
        # and a `with` removes the directory as soon as the block exits -- before `addCleanup`
        # gets to stop the worker -- so teardown raced it and died with "Directory not empty".
        # Cleanups run last-registered-first, so registering the removal here and the server
        # shutdown below stops the worker first.
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        self.addCleanup(server.close, timeout_s=1.0)
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
        # The loop leaves as soon as the worker has produced everything, so a longer ceiling
        # costs nothing when it is quick and stops the test failing on a busy machine. Three
        # seconds was not enough: on a loaded box this failed about one standalone run in
        # three, always on node_l1 -- the L1 refresh is the last step, so it is the one a short
        # deadline cuts off.
        settle_seconds = 30.0
        deadline = time.time() + settle_seconds
        records = []
        settled = False
        while time.time() < deadline:
            records = adapter.read_all()
            summary_types = {r.get("summary_type") for r in records if r.get("record_type") == "context_summary"}
            embedding_types = {(r.get("embedding_meta") or {}).get("embedding_type")
                               for r in records if r.get("vector")}
            if {"node_l0", "node_l1"}.issubset(summary_types) and {"node_l0", "node_l1"}.issubset(embedding_types):
                settled = True
                break
            time.sleep(0.05)
        summary_types = {r.get("summary_type") for r in records if r.get("record_type") == "context_summary"}
        # Folded: refreshed vectors ride on their owners; the retired rows' embedding_type
        # survives under embedding_meta.
        embedding_types = {(r.get("embedding_meta") or {}).get("embedding_type")
                           for r in records if r.get("vector")}
        # Say that the worker ran out of time, rather than asserting against a half-finished
        # store and reporting a missing summary type as though the worker had produced a wrong
        # answer. The checks below still name exactly what has to be there.
        self.assertTrue(
            settled,
            "the summary worker did not settle within %.0fs: summary_types=%r embedding_types=%r"
            % (settle_seconds, sorted(str(value) for value in summary_types),
               sorted(str(value) for value in embedding_types)),
        )
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

        summary_func = mcp.MatrixArkLocalAdapter.refresh_dirty_node_summaries.__globals__["synthesize_context_node_summary"]
        summary_globals = summary_func.__globals__
        old_call = summary_globals.get("openai_compatible_json_call")
        self.addCleanup(
            lambda: summary_globals.__setitem__("openai_compatible_json_call", old_call)
            if old_call is not None
            else summary_globals.pop("openai_compatible_json_call", None)
        )

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
            summary_records = [record for record in adapter.read_all() if record.get("record_type") == "context_summary"]
            l1 = [record for record in summary_records if record.get("summary_type") == "node_l1"]
            self.assertTrue(l1)
            self.assertTrue(any(str(record.get("summary_text", "")).startswith("OSS node_l1 synthesis") for record in l1))
            for record in l1:
                policy = record.get("summary_generation_policy", {})
                provider = policy.get("summary_provider", policy)
                self.assertEqual("openai_compatible", provider.get("provider"))
                self.assertEqual("llm_json", provider.get("execution_mode"))
                self.assertFalse(provider.get("fallback_used"))



if __name__ == "__main__":
    unittest.main()
