#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Retrieval must find its vectors on the owner records, not only on separate embedding rows.

Fold step 2 is DUAL-READ: owner records carry their own vector (written by step 1) and
retrieval falls back to it when no separate context_embedding row supplies one. With both
present nothing changes -- the separate row wins and the two are identical by construction.

The assertion that matters is the one with the separate rows REMOVED: that is the state step 3
creates by ceasing to write them, and it is the only way to know step 3 is safe before doing it.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

SCOPE = {"account_id": "acct_local", "tenant_id": "dualread", "user_id": "u",
         "session_id": "s0", "agent_name": "t"}
FACT = "I am a robotics engineer working on Aurora."


def corpus(strip_embeddings):
    """Ingest, then optionally rebuild the store WITHOUT any separate embedding rows."""
    adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "d.jsonl")
    server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
    server.call_tool("matrixark_ingest", {"scope": SCOPE, "finalize": True,
                                          "messages": [{"role": "user", "content": FACT}]})
    server.call_tool("matrixark_session_commit", {"scope": SCOPE})
    if not strip_embeddings:
        return adapter, server
    kept = [r for r in adapter.read_all() if r.get("record_type") != "context_embedding"]
    stripped = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "s.jsonl")
    stripped.append_many(kept)
    return stripped, mcp.MatrixArkMcpServer(stripped, access_mode="dev")


class InlineVectorDualReadTest(unittest.TestCase):
    def test_owner_records_carry_vectors_to_read(self):
        adapter, _ = corpus(strip_embeddings=False)
        owners = [r for r in adapter.read_all()
                  if r.get("record_type") in ("context_event", "context_entity")
                  and r.get("vector")]
        self.assertTrue(owners, "step 1 wrote no inline vectors, so step 2 has nothing to read")

    def test_retrieval_still_works_with_the_separate_rows_present(self):
        _, server = corpus(strip_embeddings=False)
        out = json.dumps(server.call_tool("matrixark_retrieve",
                                          {"scope": SCOPE, "query": "what is my job?"}))
        self.assertIn("robotics", out.lower())

    def test_scoring_still_receives_a_vector_without_the_separate_rows(self):
        """The step-3 rehearsal, asserted on the MECHANISM rather than the answer.

        An earlier version of this test asserted only that retrieval still returned the fact
        with the embedding rows stripped -- and it passed with dual-read reverted, because
        lexical scoring finds the fact on its own. Under the default 32-dim hash encoder there
        is no semantic similarity to isolate either, so end-to-end retrieval cannot show whether
        a vector was used at all.

        This instead observes what cosine() is handed: with the separate rows gone, the event's
        vector must still reach scoring from the owner record.
        """
        import matrixark_local_adapter_retrieve as retrieve_module

        adapter, server = corpus(strip_embeddings=True)
        self.assertEqual(
            [], [r for r in adapter.read_all() if r.get("record_type") == "context_embedding"],
            "the fixture failed to strip the separate embedding rows")

        seen_non_empty = []
        original = retrieve_module.cosine

        def watched(left, right):
            if right:
                seen_non_empty.append(len(right))
            return original(left, right)

        retrieve_module.cosine = watched
        try:
            server.call_tool("matrixark_retrieve", {"scope": SCOPE, "query": "what is my job?"})
        finally:
            retrieve_module.cosine = original

        self.assertTrue(
            seen_non_empty,
            "scoring received no vector at all once the separate embedding rows were removed; "
            "the inline fallback did not supply one")


if __name__ == "__main__":
    unittest.main()
