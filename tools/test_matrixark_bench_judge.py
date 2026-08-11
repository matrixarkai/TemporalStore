#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tests for the shared Claude-as-judge module."""
import unittest

import matrixark_bench_judge as J


def _report():
    return {
        "aggregate": {"baseline": {"avg_quality": None}, "big_budget": {"avg_quality": None}},
        "turns": [
            {"turn": 1, "query": "q1", "reference": "ref1", "expected_terms": ["a"],
             "configs": {"baseline": {"answer": "ans1b", "quality": None},
                         "big_budget": {"answer": "ans1g", "quality": None}}},
            {"turn": 2, "query": "q2", "reference": "ref2", "expected_terms": ["b"],
             "configs": {"baseline": {"answer": "ans2b", "quality": None},
                         "big_budget": {"answer": "ans2g", "quality": None}}},
        ],
    }


class EmitTest(unittest.TestCase):
    def test_cases_flattened_per_arm(self):
        cases = J.cases_from_arm_turns(_report()["turns"], ["baseline", "big_budget"])
        ids = [c["case_id"] for c in cases]
        self.assertEqual(["t1:baseline", "t1:big_budget", "t2:baseline", "t2:big_budget"], ids)
        c = cases[0]
        self.assertEqual("q1", c["query"])
        self.assertEqual("ans1b", c["answer"])
        self.assertEqual("ref1", c["reference"])
        self.assertEqual(["a"], c["expected_terms"])

    def test_missing_answer_skipped(self):
        turns = [{"turn": 1, "query": "q", "configs": {"baseline": {}}}]  # no answer
        self.assertEqual([], J.cases_from_arm_turns(turns, ["baseline"]))


class ApplyTest(unittest.TestCase):
    def test_scores_merged_and_aggregate_refreshed(self):
        rep = _report()
        scores = {"t1:baseline": 7, "t1:big_budget": 9, "t2:baseline": 3, "t2:big_budget": 5}
        J.apply_judge_scores(rep, scores, ["baseline", "big_budget"])
        self.assertEqual(7.0, rep["turns"][0]["configs"]["baseline"]["quality"])
        self.assertEqual("claude", rep["turns"][0]["configs"]["baseline"]["judge"])
        self.assertEqual(5.0, rep["aggregate"]["baseline"]["avg_quality"])   # (7+3)/2
        self.assertEqual(7.0, rep["aggregate"]["big_budget"]["avg_quality"])  # (9+5)/2
        self.assertEqual("claude", rep["judge"])

    def test_unknown_case_id_ignored(self):
        rep = _report()
        J.apply_judge_scores(rep, {"t9:baseline": 4}, ["baseline"])
        self.assertIsNone(rep["turns"][0]["configs"]["baseline"]["quality"])

    def test_default_judge_is_claude(self):
        self.assertEqual("claude", J.default_judge())


if __name__ == "__main__":
    unittest.main()
