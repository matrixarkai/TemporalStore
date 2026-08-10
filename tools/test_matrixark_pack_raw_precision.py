#!/usr/bin/env python3
"""Precision-aware raw preference: raw events win over summaries for precision queries when on."""
import sys
import unittest

import matrixark_mcp_core  # noqa: F401  loads the packing split as part of core
P = sys.modules["matrixark_mcp_core_packing"]

SUMMARY = {"ref_type": "compression", "score": 0.6, "text": "short summary", "source_event_ids": [1, 2, 3]}
EVENT = {"ref_type": "event", "score": 0.6, "text": "raw event with exact commit 9a803784 and ratios 0.12 0.30 0.10 " * 2}


def _prefers_event(qt):
    return P.packing_sort_key(EVENT, qt) > P.packing_sort_key(SUMMARY, qt)


class PackRawPrecisionTest(unittest.TestCase):
    def setUp(self):
        self._orig = P.PACK_RAW_PRECISION

    def tearDown(self):
        P.PACK_RAW_PRECISION = self._orig

    def test_default_off_prefers_summary_for_fact(self):
        P.PACK_RAW_PRECISION = False
        self.assertFalse(_prefers_event("fact"))          # current behavior: density wins

    def test_on_prefers_raw_event_for_precision_types(self):
        P.PACK_RAW_PRECISION = True
        for qt in ("fact", "multi_hop", "evidence", "benchmark_quality", "date"):
            self.assertTrue(_prefers_event(qt), f"expected raw event to win for {qt}")

    def test_on_leaves_non_precision_types_unchanged(self):
        P.PACK_RAW_PRECISION = True
        # gist / current-value queries keep the distilled preference
        self.assertFalse(_prefers_event("profile_memory"))
        self.assertFalse(_prefers_event("current_state"))
        self.assertFalse(_prefers_event("latest"))

    def test_flag_and_types_exposed(self):
        self.assertIn("fact", P.PRECISION_QUESTION_TYPES)
        self.assertIn("evidence", P.PRECISION_QUESTION_TYPES)
        self.assertNotIn("current_state", P.PRECISION_QUESTION_TYPES)  # keeps distilled
        self.assertNotIn("profile_memory", P.PRECISION_QUESTION_TYPES)


if __name__ == "__main__":
    unittest.main()
