#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Owner records persist the embedding vector; the separate records are RETIRED.

The fold-and-drop happens at the adapter's append: an embedding record whose owner is in the
same batch folds its vector onto the owner and vanishes; one whose owner was appended earlier
re-appends an updated owner instead; one with no findable owner (or an unmapped ref type) is
kept, because a vector must never be dropped on the floor. Old logs still carry separate
records and every reader still accepts them -- new logs simply stop growing them.

These assertions read records back OUT of the adapter on purpose: the Rust half of this fold
once had the field, ran the population code, and a hand-written codec dropped the value on
persist with no error anywhere.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

SCOPE = {"account_id": "acct_local", "tenant_id": "inlinevec", "user_id": "u",
         "session_id": "s0", "agent_name": "t"}
CONVO = [{"role": "user", "content": "I am a robotics engineer working on Aurora."},
         {"role": "user", "content": "I live in Seattle and the p99 was 27ms."}]


def ingest():
    adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "v.jsonl")
    server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
    server.call_tool("matrixark_ingest", {"scope": SCOPE, "finalize": True, "messages": CONVO})
    server.call_tool("matrixark_session_commit", {"scope": SCOPE})
    return adapter.read_all()


def of_type(rows, record_type):
    return [r for r in rows if r.get("record_type") == record_type]


class InlineEmbeddingVectorTest(unittest.TestCase):
    def test_events_carry_their_vector(self):
        rows = ingest()
        events = of_type(rows, "context_event")
        self.assertTrue(events, "the ingest wrote no events")
        with_vector = [e for e in events if e.get("vector")]
        self.assertTrue(with_vector,
                        "no context_event persisted an inline vector (%d events)" % len(events))

    def test_entities_carry_their_vector(self):
        rows = ingest()
        entities = of_type(rows, "context_entity")
        self.assertTrue(entities, "the ingest wrote no entities")
        self.assertTrue([e for e in entities if e.get("vector")],
                        "no context_entity persisted an inline vector")

    def test_mapped_embedding_records_are_not_persisted(self):
        """The retirement itself: an embedding whose owner exists folds and vanishes."""
        rows = ingest()
        mapped = {"event", "entity", "summary", "node", "segment", "compression",
                  "resource_chunk", "skill_section"}
        leftovers = [r for r in of_type(rows, "context_embedding")
                     if str(r.get("ref_type") or "") in mapped]
        self.assertEqual(
            [], leftovers,
            "embedding records with a mapped owner must fold onto it, not persist: %r"
            % [(r.get("ref_type"), r.get("ref_hash")) for r in leftovers][:5])

    def test_a_late_embedding_updates_its_earlier_owner(self):
        """Cross-batch: the owner was appended earlier; the embedding folds onto a re-appended
        copy of it instead of persisting separately."""
        import matrixark_mcp_local_adapter as A
        import tempfile as tf
        adapter = mcp.MatrixArkLocalAdapter(Path(tf.mkdtemp()) / "late.jsonl")
        adapter.append({
            "record_type": "context_event",
            "event_id_hash": 4242,
            "text": "an event embedded later",
            "updated_at_ms": 1780000000000,
        })
        adapter.append({
            "record_type": "context_embedding",
            "embedding_type": "event_text",
            "ref_type": "event",
            "ref_hash": 4242,
            "vector": [0.5, 0.25],
            "model": "late-model",
            "updated_at_ms": 1780000000500,
        })
        rows = adapter.read_all()
        self.assertEqual([], of_type(rows, "context_embedding"),
                         "the late embedding must fold, not persist")
        events = [r for r in of_type(rows, "context_event") if r.get("event_id_hash") == 4242]
        self.assertTrue(events and events[-1].get("vector") == [0.5, 0.25],
                        "the earlier owner must carry the late vector")

    def test_an_orphan_embedding_is_kept_not_lost(self):
        """No findable owner: the record persists exactly as before -- a vector is never
        dropped on the floor."""
        import tempfile as tf
        adapter = mcp.MatrixArkLocalAdapter(Path(tf.mkdtemp()) / "orphan.jsonl")
        adapter.append({
            "record_type": "context_embedding",
            "embedding_type": "event_text",
            "ref_type": "event",
            "ref_hash": 999999,
            "vector": [1.0],
            "updated_at_ms": 1780000000000,
        })
        rows = adapter.read_all()
        kept = of_type(rows, "context_embedding")
        self.assertEqual(1, len(kept), "an orphan embedding must be kept")
        self.assertEqual([1.0], kept[0].get("vector"))


if __name__ == "__main__":
    unittest.main()
