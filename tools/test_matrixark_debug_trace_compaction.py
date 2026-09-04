#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Regression tests for compact MatrixArk debug trace reports."""

from __future__ import annotations

import json
import unittest

from tools import matrixark_mcp_core as core
from tools import matrixark_mcp_context_pack as context_pack
from tools import run_matrixark_message_pdf_debug_trace as trace_runner


class MatrixArkDebugTraceCompactionTest(unittest.TestCase):
    def test_flat_context_pack_refs_expose_memory_layer_without_lineage(self) -> None:
        refs = [
            {
                "ref_type": "entity",
                "text": "Project Aurora owner is Bob.",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_session_ids": ["codex:old"],
                "source_entity_hashes": [11, 22],
            },
            {
                "ref_type": "event",
                "text": "User asked about Project Aurora.",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "source_roles": ["user"],
            },
        ]

        compact = context_pack.compact_context_pack_refs(refs)

        self.assertEqual("profile", compact[0]["memory_layer"])
        self.assertEqual("session", compact[1]["memory_layer"])
        self.assertNotIn("source_session_ids", compact[0])
        self.assertNotIn("source_entity_hashes", compact[0])
        self.assertNotIn("source_roles", compact[1])

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
        again = core.compact_context_pack_for_serving(compact)

        # Idempotence is f(f(x)) == f(x), which is what the entrypoint needs: it may compact a
        # pack an adapter already returned in serving shape, and the second pass must not erase
        # anything. Asserting f(x) == x instead asked compaction to be the identity on a
        # hand-built pack, and this one carries a per-item `tokens` that the serving shape does
        # not have -- nothing builds it, only the pack-level token summary exists -- so the
        # first pass drops it and the test failed on a projection doing its job.
        self.assertEqual(again["groups"], compact["groups"])
        self.assertEqual(again["tokens"], compact["tokens"])
        # The pack-level summary is carried through, and the evidence survives -- without this
        # the equality above would hold just as well on a pack compacted down to nothing.
        self.assertEqual(compact["tokens"], grouped_pack["tokens"])
        self.assertEqual(
            [item["text"] for group in compact["groups"] for item in group["items"]],
            ["Alice approved Project Aurora."],
        )

    def test_trace_export_drops_raw_scope_and_replay_payloads(self) -> None:
        trace = {
            "scope": {
                "account_id": "acct_local",
                "tenant_id": "tenant_codex",
                "user_id": "local_user",
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

    def test_data_model_rows_use_short_aliases_and_drop_forensic_fields(self) -> None:
        aliases = trace_runner.ReportAliases()
        event = trace_runner.compact_context_event(
            {
                "event_id_hash": 1121810234980183195,
                "node_hash": 2100209595829882121,
                "classification": "NEW_EVENT",
                "text": "user: Alice approved Project Aurora.",
                "context_event_key": "00000001782681920521:1121810234980183195",
            },
            aliases,
        )
        entity = trace_runner.compact_context_entity(
            {
                "entity_hash": 7343877841316191174,
                "node_hash": 2100209595829882121,
                "entity_type": "resource_decision",
                "entity_name": "decision:/tmp/fixtures/aurora.pdf:Alice approved the Project Aurora GPU purchase",
                "operator": "LATEST",
                "state": "approved",
                "source_ref": "/tmp/fixtures/aurora.pdf#page=1",
            },
            aliases,
        )
        summary = trace_runner.compact_context_summary(
            {
                "summary_hash": 8695652974415713980,
                "summary_version_hash": 123,
                "node_hash": 2100209595829882121,
                "summary_type": "session_l0",
                "summary_text": "Alice approved Project Aurora.",
            },
            aliases,
        )
        embedding = trace_runner.compact_context_embedding(
            {
                "embedding_type": "event_text",
                "ref_type": "event",
                "ref_hash": 1121810234980183195,
                "model_ref": "model_hash:2794525681328894881",
                "dim": 32,
                "vector": [0.1, 0.2],
            },
            aliases,
        )
        indexes = trace_runner.compact_context_indexes(
            [
                {
                    "data_model": "context_event",
                    "index_name": "event_type:approval",
                    "timestamp_key_ms": 1782681920550,
                    "node_hash": 2100209595829882121,
                    "ref_type": "event",
                    "ref_hashes": [1121810234980183195],
                }
            ],
            aliases,
        )

        payload = json.dumps(
            {"event": event, "entity": entity, "summary": summary, "embedding": embedding, "indexes": indexes},
            sort_keys=True,
        )
        self.assertIn('"event": "e1"', payload)
        self.assertIn('"node": "n1"', payload)
        self.assertIn('"summary": "s1"', payload)
        self.assertIn('"ref": "e1"', payload)
        self.assertIn("Alice approved the Project Aurora GPU purchase", payload)
        self.assertNotIn("1121810234980183195", payload)
        self.assertNotIn("2100209595829882121", payload)
        self.assertNotIn("context_event_key", payload)
        self.assertNotIn("summary_version_hash", payload)
        self.assertNotIn("model_hash", payload)
        self.assertNotIn("NEW_EVENT", payload)
        self.assertNotIn("/tmp/fixtures", payload)
        self.assertNotIn("timestamp_key_ms", payload)

    def test_index_and_placement_rows_are_sampled_and_aliased(self) -> None:
        aliases = trace_runner.ReportAliases()
        indexes = trace_runner.compact_context_indexes(
            [
                {
                    "data_model": "resource_fact",
                    "index_name": "entity_type:resource_owner",
                    "node_hash": 7,
                    "ref_type": "resource_fact",
                    "ref_hashes": list(range(20, 29)),
                },
                {
                    "data_model": "context_batch_commit",
                    "index_name": "event_type:confirmation",
                    "node_hash": 7,
                    "ref_hashes": [1, 2, 3],
                },
            ],
            aliases,
        )
        placements = trace_runner.compact_placement_routes(
            [
                {
                    "record_type": "context_event",
                    "node_hash": 7,
                    "placement_key": "context:t=2466697514329931826|u=7836037686236352053|s=7498925135890267938|:node=7",
                    "placement_hash": 33,
                }
            ],
            aliases,
        )
        inventory = trace_runner.data_field_inventory(
            [
                {"record_type": "context_event", "event_id_hash": 1},
                {"record_type": "context_batch_commit", "commit_hash": 2},
            ]
        )

        self.assertEqual(indexes, [{"model": "resource_fact", "index": "entity_type:resource_owner", "node": "n1", "ref_count": 9, "sample_refs": []}])
        self.assertEqual(placements[0]["placement"], "p1")
        self.assertNotIn("placement_key", placements[0])
        self.assertNotIn("context:", json.dumps(placements))
        self.assertNotIn("context_batch_commit", json.dumps(indexes))
        self.assertNotIn("context_batch_commit", json.dumps(inventory))


if __name__ == "__main__":
    unittest.main()
