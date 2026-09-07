#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A keyed recall finds its key's records without walking the whole live store.

``get_memory_by_identity_key`` filtered every record in the store to find one key: 7.2 ms per
32,000 records. It reads an index now, through a new ``records_for_identity_key`` override point
matching the two beside it.

An index fails by omission -- a live keyed value answering ``{found: false}`` -- so the main test is
differential and exhaustive: for every key the corpus holds, the indexed answer must equal the
whole-store answer, compared as whole structures, with the whole-store side being the base
implementation restored so the two cannot drift.
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

ANCHOR_MS = 1_700_000_000_000
SCOPE = {"tenant_hash": 7, "user_hash": 11, "scope_key": "t=7;u=11"}
OTHER = {"tenant_hash": 7, "user_hash": 22, "scope_key": "t=7;u=22"}


class AKeyedRecallReadsAnIndexTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.adapter = MatrixArkLocalAdapter(pathlib.Path(self.tmp.name) / "events.jsonl")
        records = []
        for i in range(60):
            records.append({"record_type": "context_event", "event_id_hash": f"f{i}",
                            "identity_key": f"filler{i}", "summary_text": f"filler {i}",
                            "text": f"filler {i}", "truth_rank": 1,
                            "updated_at_ms": ANCHOR_MS + i, "timestamp_key_ms": ANCHOR_MS + i,
                            "access_scope": SCOPE})
        # One key held by several records, so the tie-break (highest truth_rank, then most recent)
        # has something to choose between -- an index that returned only one would still look right
        # without this.
        records += [
            {"record_type": "context_event", "event_id_hash": "a1", "identity_key": "home",
             "summary_text": "old home", "text": "old home", "truth_rank": 1,
             "updated_at_ms": ANCHOR_MS + 100, "timestamp_key_ms": ANCHOR_MS + 100,
             "occurred_at_ms": ANCHOR_MS + 100, "access_scope": SCOPE},
            {"record_type": "context_event", "event_id_hash": "a2", "identity_key": "home",
             "summary_text": "new home", "text": "new home", "truth_rank": 3,
             "updated_at_ms": ANCHOR_MS + 101, "timestamp_key_ms": ANCHOR_MS + 101,
             "occurred_at_ms": ANCHOR_MS + 101, "access_scope": SCOPE},
            {"record_type": "context_event", "event_id_hash": "a3", "identity_key": "home",
             "summary_text": "someone else's home", "text": "someone else's home", "truth_rank": 9,
             "updated_at_ms": ANCHOR_MS + 102, "timestamp_key_ms": ANCHOR_MS + 102,
             "occurred_at_ms": ANCHOR_MS + 102, "access_scope": OTHER},
            # A non-event carrying the same key: the type check must still exclude it.
            {"record_type": "context_summary", "identity_key": "home", "summary_hash": "s1",
             "summary_text": "a summary", "updated_at_ms": ANCHOR_MS + 103, "access_scope": SCOPE},
        ]
        self.adapter.append_many(records)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _by_full_scan(self, key: str, scope: dict) -> dict:
        original = type(self.adapter).records_for_identity_key
        try:
            type(self.adapter).records_for_identity_key = lambda self, identity_key: self.read_all()
            return self.adapter.get_memory_by_identity_key({"identity_key": key, "scope": scope})
        finally:
            type(self.adapter).records_for_identity_key = original

    def test_every_key_answers_what_the_whole_store_answers(self) -> None:
        keys = {str(r.get("identity_key")) for r in self.adapter.read_all()
                if r.get("identity_key") not in (None, "")}
        self.assertGreaterEqual(len(keys), 61, "the corpus must hold many keys or this proves little")
        for key in sorted(keys):
            for scope in (SCOPE, OTHER, {}):
                self.assertEqual(
                    self.adapter.get_memory_by_identity_key({"identity_key": key, "scope": scope}),
                    self._by_full_scan(key, scope),
                    f"identity_key={key} scope={scope} differs from the whole-store answer")

    def test_a_key_the_store_never_held_agrees_too(self) -> None:
        self.assertEqual(
            self.adapter.get_memory_by_identity_key({"identity_key": "nosuchkey", "scope": SCOPE}),
            self._by_full_scan("nosuchkey", SCOPE))

    def test_the_highest_truth_rank_in_the_scope_wins(self) -> None:
        """The positive control: an index returning one arbitrary record would pass a differential
        test whose other side was broken the same way, but not this."""
        out = self.adapter.get_memory_by_identity_key({"identity_key": "home", "scope": SCOPE})
        self.assertTrue(out["found"])
        self.assertEqual(out["id"], "a2")
        self.assertEqual(out["truth_rank"], 3)

    def test_another_subjects_record_does_not_win_the_key(self) -> None:
        """a3 has the highest truth_rank of all, and belongs to someone else."""
        self.assertEqual(
            self.adapter.get_memory_by_identity_key({"identity_key": "home", "scope": OTHER})["id"],
            "a3")

    def test_a_write_reaches_a_recall_already_served(self) -> None:
        before = self.adapter.get_memory_by_identity_key({"identity_key": "home", "scope": SCOPE})["id"]
        self.assertEqual(before, "a2")
        self.adapter.append({"record_type": "context_event", "event_id_hash": "a4",
                             "identity_key": "home", "summary_text": "newest", "text": "newest",
                             "truth_rank": 5, "updated_at_ms": ANCHOR_MS + 200,
                             "timestamp_key_ms": ANCHOR_MS + 200,
                             "occurred_at_ms": ANCHOR_MS + 200, "access_scope": SCOPE})
        self.assertEqual(
            self.adapter.get_memory_by_identity_key({"identity_key": "home", "scope": SCOPE})["id"],
            "a4")


if __name__ == "__main__":
    unittest.main(verbosity=2)
