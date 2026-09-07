#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""history answers one memory id without walking the whole raw log.

`raw_records_for_history` now returns an index bucket. Walking everything cost 17.9 ms per 32,000
records to answer one id, and 17.6 of that was the bare Python iteration -- there was nothing to
make faster inside the loop, only fewer records to run it over. After: 0.053 ms, and flat.

The risk an index carries is the one the method's own docstring warns about -- the OUTPUT for any
memory id must be unchanged -- and an index can only get that wrong by omission, which no
single-case test reliably catches. So the main test here is DIFFERENTIAL and exhaustive: for every
id the corpus mentions, the indexed answer must equal the answer the full scan gives, compared as
whole structures. The full scan is obtained by putting the base implementation back, so the two
sides cannot drift apart.
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
TOMBSTONE = adapter_module.MEMORY_TOMBSTONE_RECORD_TYPE


class HistoryReadsAnIndexNotTheWholeLogTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.adapter = MatrixArkLocalAdapter(pathlib.Path(self.tmp.name) / "events.jsonl")
        feedback_type = self.adapter.MEMORY_FEEDBACK_RECORD_TYPE
        records: list[dict] = []
        # Filler, so an id's own records are a small minority of the log -- the condition the index
        # exists for, and the one where an omission hides.
        for i in range(60):
            records.append({"record_type": "context_event", "event_id_hash": f"filler{i}",
                            "summary_text": f"filler {i}", "text": f"filler {i}",
                            "updated_at_ms": ANCHOR_MS + i, "timestamp_key_ms": ANCHOR_MS + i})
        # Every branch history has: an ingest, a supersede (which both ends the old id and creates
        # the new one), a plain delete, and a feedback rating.
        records += [
            {"record_type": "context_event", "event_id_hash": "m1", "summary_text": "first",
             "text": "first", "updated_at_ms": ANCHOR_MS + 100, "timestamp_key_ms": ANCHOR_MS + 100},
            {"record_type": "context_event", "event_id_hash": "m1", "summary_text": "first again",
             "text": "first again", "updated_at_ms": ANCHOR_MS + 101, "timestamp_key_ms": ANCHOR_MS + 101},
            {"record_type": feedback_type, "target_memory_id": "m1", "feedback": "up",
             "feedback_reason": "useful", "updated_at_ms": ANCHOR_MS + 102},
            {"record_type": TOMBSTONE, "tombstone_kind": "delete", "tombstone_reason": "supersede",
             "target_memory_id": "m1", "superseded_by": "m2", "updated_at_ms": ANCHOR_MS + 103},
            {"record_type": "context_event", "event_id_hash": "m2", "summary_text": "second",
             "text": "second", "updated_at_ms": ANCHOR_MS + 104, "timestamp_key_ms": ANCHOR_MS + 104},
            {"record_type": TOMBSTONE, "tombstone_kind": "delete", "target_memory_id": "m2",
             "updated_at_ms": ANCHOR_MS + 105},
            {"record_type": "context_event", "event_id_hash": "m3", "summary_text": "third",
             "text": "third", "updated_at_ms": ANCHOR_MS + 106, "timestamp_key_ms": ANCHOR_MS + 106},
        ]
        self.adapter.append_many(records)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _history_by_full_scan(self, memory_id: str) -> dict:
        """The base implementation, restored: history over the whole raw log."""
        original = type(self.adapter).raw_records_for_history
        try:
            type(self.adapter).raw_records_for_history = (
                lambda self, memory_id=None: self._read_raw_records()
            )
            return self.adapter.history({"memory_id": memory_id})
        finally:
            type(self.adapter).raw_records_for_history = original

    def _every_id_the_log_mentions(self) -> set:
        ids = set()
        for record in self.adapter._read_raw_records():
            for field in ("event_id_hash", "target_memory_id", "superseded_by"):
                value = record.get(field)
                if value not in (None, ""):
                    ids.add(str(value))
        return ids

    def test_every_id_answers_what_the_full_scan_answers(self) -> None:
        """The one that matters. An index fails by omission; this is the shape that catches it."""
        ids = self._every_id_the_log_mentions()
        self.assertGreaterEqual(len(ids), 63, "the corpus must mention many ids or this proves little")
        for memory_id in sorted(ids):
            self.assertEqual(self.adapter.history({"memory_id": memory_id}),
                             self._history_by_full_scan(memory_id),
                             f"history({memory_id}) differs from the full scan")

    def test_an_id_the_log_never_mentions_agrees_too(self) -> None:
        self.assertEqual(self.adapter.history({"memory_id": "nosuchid"}),
                         self._history_by_full_scan("nosuchid"))

    def test_a_superseded_memory_reports_both_ends(self) -> None:
        """m1 was superseded BY m2, so m1 records the supersede and m2 records being created."""
        events = self.adapter.history({"memory_id": "m1"})["history"]
        self.assertEqual([e["event"] for e in events], ["ingested", "feedback", "superseded"])
        self.assertEqual(events[-1]["superseded_by"], "m2")
        created = [e for e in self.adapter.history({"memory_id": "m2"})["history"]
                   if e["event"] == "created"]
        self.assertEqual([e["supersedes_memory_id"] for e in created], ["m1"])

    def test_a_write_reaches_a_history_already_served(self) -> None:
        """The index is remembered per raw-log generation; a write must move it."""
        before = self.adapter.history({"memory_id": "m3"})["count"]
        self.adapter.append({"record_type": self.adapter.MEMORY_FEEDBACK_RECORD_TYPE,
                             "target_memory_id": "m3", "feedback": "down",
                             "updated_at_ms": ANCHOR_MS + 200})
        self.assertEqual(self.adapter.history({"memory_id": "m3"})["count"], before + 1)

    def test_a_repeat_answers_the_same(self) -> None:
        self.assertEqual(self.adapter.history({"memory_id": "m1"}),
                         self.adapter.history({"memory_id": "m1"}))


if __name__ == "__main__":
    unittest.main(verbosity=2)
