#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""get_memory answers one id without walking the whole live store.

Two matches reach a record, and both are indexed because both are needed: an id finds its own event
through ``event_id_hash``, and finds each DERIVED record through the source ids that record's
provenance names. An index on the id alone would return the event and silently lose the entities and
summaries built from it -- `{found: true}` with an empty `derived`, which reads like a memory that
simply has no derivatives.

That is a failure by omission, so the main test is differential and exhaustive: for every id the
corpus mentions, through either match, the indexed answer must equal the whole-store answer,
compared as whole structures. The whole-store side is the base implementation, restored, so the two
cannot drift.
"""
from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

from matrixark_mcp_local_adapter import MatrixArkLocalAdapter  # noqa: E402
import matrixark_mcp_local_adapter as adapter_module  # noqa: E402

ANCHOR_MS = 1_700_000_000_000


class GetMemoryReadsAnIndexNotTheStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.adapter = MatrixArkLocalAdapter(pathlib.Path(self.tmp.name) / "events.jsonl")
        records: list[dict] = []
        # Filler, so any one id's records are a small minority -- the condition the index exists
        # for and the one where a missed key hides.
        for i in range(60):
            records.append({"record_type": "context_event", "event_id_hash": str(9_000 + i),
                            "summary_text": f"filler {i}", "text": f"filler {i}",
                            "updated_at_ms": ANCHOR_MS + i, "timestamp_key_ms": ANCHOR_MS + i})
        # One event with derivatives reaching it by each of the three provenance fields, plus a
        # derivative built from TWO sources, which must appear under both.
        records += [
            {"record_type": "context_event", "event_id_hash": "101", "summary_text": "the memory",
             "text": "the memory", "updated_at_ms": ANCHOR_MS + 100, "timestamp_key_ms": ANCHOR_MS + 100},
            {"record_type": "context_event", "event_id_hash": "102", "summary_text": "another",
             "text": "another", "updated_at_ms": ANCHOR_MS + 101, "timestamp_key_ms": ANCHOR_MS + 101},
            {"record_type": "context_entity", "entity_hash": "ent1", "entity_name": "Ada",
             "source_event_ids": [101], "text": "Ada", "updated_at_ms": ANCHOR_MS + 102},
            {"record_type": "context_summary", "summary_hash": "sum1", "summary_type": "session",
             "source_refs": ["101"], "summary_text": "a summary", "updated_at_ms": ANCHOR_MS + 103},
            {"record_type": "context_segment", "source_event_hash": "101",
             "text": "a segment", "updated_at_ms": ANCHOR_MS + 104},
            {"record_type": "context_summary", "summary_hash": "sum2", "summary_type": "rollup",
             "source_event_ids": [101, 102], "summary_text": "both", "updated_at_ms": ANCHOR_MS + 105},
            # A leaf that is NOT a derivative: it must not be pulled in by the provenance path.
            {"record_type": "context_embedding", "ref_type": "event", "ref_hash": "101",
             "updated_at_ms": ANCHOR_MS + 106},
        ]
        self.adapter.append_many(records)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _by_full_scan(self, memory_id: str) -> dict:
        original = type(self.adapter).records_for_get_memory
        try:
            type(self.adapter).records_for_get_memory = lambda self, memory_id: self.read_all()
            return self.adapter.get_memory({"memory_id": memory_id})
        finally:
            type(self.adapter).records_for_get_memory = original

    def _every_id(self) -> set:
        ids = set()
        for record in self.adapter.read_all():
            own = record.get("event_id_hash")
            if own not in (None, ""):
                ids.add(str(own))
            provenance = adapter_module._record_provenance_source_ids(record)
            if provenance:
                ids.update(str(source) for source in provenance)
        return ids

    def test_every_id_answers_what_the_whole_store_answers(self) -> None:
        ids = self._every_id()
        self.assertGreaterEqual(len(ids), 62, "the corpus must mention many ids or this proves little")
        for memory_id in sorted(ids):
            self.assertEqual(self.adapter.get_memory({"memory_id": memory_id}),
                             self._by_full_scan(memory_id),
                             f"get_memory({memory_id}) differs from the whole-store answer")

    def test_an_id_the_store_never_mentions_agrees_too(self) -> None:
        self.assertEqual(self.adapter.get_memory({"memory_id": "nosuchid"}),
                         self._by_full_scan("nosuchid"))

    def test_the_derivatives_are_actually_there(self) -> None:
        """The positive control: without it, an index returning only the event would pass the
        differential test against a full scan that had been broken the same way."""
        out = self.adapter.get_memory({"memory_id": "101"})
        self.assertTrue(out["found"])
        kinds = sorted(row["record_type"] for row in out["derived"])
        self.assertEqual(kinds, ["context_entity", "context_segment", "context_summary", "context_summary"])

    def test_a_two_source_derivative_reaches_both_ids(self) -> None:
        for memory_id in ("101", "102"):
            texts = [row["text"] for row in self.adapter.get_memory({"memory_id": memory_id})["derived"]]
            self.assertIn("both", texts, f"the two-source summary is missing from {memory_id}")

    def test_a_write_reaches_a_read_already_served(self) -> None:
        before = len(self.adapter.get_memory({"memory_id": "101"})["derived"])
        self.adapter.append({"record_type": "context_entity", "entity_hash": "ent2",
                             "entity_name": "Grace", "source_event_ids": [101], "text": "Grace",
                             "updated_at_ms": ANCHOR_MS + 300})
        self.assertEqual(len(self.adapter.get_memory({"memory_id": "101"})["derived"]), before + 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
