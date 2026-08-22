#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Owner records must persist the embedding vector, not just be handed one in memory.

Step 1 of folding embeddings into their owners is a DUAL-WRITE: the vector is copied onto the
context_event / context_entity / context_summary record while the separate context_embedding
record keeps being written and every reader keeps joining through (ref_type, ref_hash).

Nothing reads the inline field yet, so without reading records back OUT of the adapter this
could copy nothing and the whole suite would stay green. That is not hypothetical -- the Rust
half of this fold did exactly that: the struct had the field, the population code ran, and a
hand-written protobuf codec dropped the value on persist with no error anywhere.
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

    def test_inline_vector_equals_the_separate_record(self):
        """Dual-write means identical, not merely present -- a wrong vector is worse than none."""
        rows = ingest()
        by_ref = {(str(r.get("ref_type") or ""), r.get("ref_hash")): r.get("vector")
                  for r in of_type(rows, "context_embedding") if r.get("vector")}
        checked = 0
        for record_type, ref_type, field in (
            ("context_event", "event", "event_id_hash"),
            ("context_entity", "entity", "entity_hash"),
            ("context_summary", "summary", "summary_hash"),
        ):
            for record in of_type(rows, record_type):
                inline = record.get("vector")
                if not inline:
                    continue
                separate = by_ref.get((ref_type, record.get(field)))
                self.assertIsNotNone(
                    separate, "%s has an inline vector with no matching embedding record" % record_type)
                self.assertEqual(separate, inline,
                                 "%s inline vector differs from its embedding record" % record_type)
                checked += 1
        self.assertTrue(checked, "no owner record was cross-checked against its embedding record")

    def test_separate_embedding_records_are_still_written(self):
        """Step 1 must not drop them: every reader still joins through (ref_type, ref_hash)."""
        rows = ingest()
        self.assertTrue(of_type(rows, "context_embedding"),
                        "dual-write must keep the separate embedding records until readers migrate")


if __name__ == "__main__":
    unittest.main()
