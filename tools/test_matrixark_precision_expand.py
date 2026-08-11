#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""precision_expand_pack: expand matched segments to their source raw events for exact-fact recall."""
import unittest

import matrixark_mcp_local_adapter  # noqa: F401  loads the retrieve mixin (breaks standalone cycle)
import matrixark_local_adapter_retrieve as R


def _pack_and_records():
    pack = {"selected_refs": [{"ref_type": "segment", "ref_hash": 100, "text": "summary", "tokens": 10}],
            "used_context_tokens": 10}
    records = [
        {"record_type": "context_segment", "segment_hash": 100, "source_event_ids": [1, 2]},
        {"record_type": "context_event", "event_id_hash": 1, "text": "user: exact commit 9a803784"},
        {"record_type": "context_event", "event_id_hash": 2, "text": "assistant: ratios 0.12 0.30 0.10"},
    ]
    return pack, records


class PrecisionExpandTest(unittest.TestCase):
    def test_expands_segment_to_raw_events(self):
        pack, recs = _pack_and_records()
        added = R.precision_expand_pack(pack, recs, "fact", max_events=12, budget_tokens=16000)
        self.assertEqual(2, added)
        self.assertEqual(["segment", "event", "event"], [r["ref_type"] for r in pack["selected_refs"]])
        self.assertTrue(any("9a803784" in r.get("text", "") for r in pack["selected_refs"]))
        self.assertEqual(2, pack["precision_expanded"]["raw_events_added"])
        self.assertGreater(pack["used_context_tokens"], 10)

    def test_dedups_events_already_in_pack(self):
        pack, recs = _pack_and_records()
        pack["selected_refs"].append({"ref_type": "event", "ref_hash": 1, "text": "already here"})
        added = R.precision_expand_pack(pack, recs, "fact", max_events=12, budget_tokens=16000)
        self.assertEqual(1, added)  # event 1 already present -> only event 2 added

    def test_budget_cap_respected(self):
        pack, recs = _pack_and_records()
        added = R.precision_expand_pack(pack, recs, "fact", max_events=12, budget_tokens=1)  # ~nothing fits
        self.assertEqual(0, added)

    def test_no_segments_is_noop(self):
        pack = {"selected_refs": [{"ref_type": "entity", "ref_hash": 5, "text": "e"}], "used_context_tokens": 3}
        added = R.precision_expand_pack(pack, [], "fact", max_events=12, budget_tokens=16000)
        self.assertEqual(0, added)
        self.assertNotIn("precision_expanded", pack)

    def test_never_raises_on_bad_input(self):
        self.assertEqual(0, R.precision_expand_pack({}, None, "fact", max_events=12, budget_tokens=100))


if __name__ == "__main__":
    unittest.main()
