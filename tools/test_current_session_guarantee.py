#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The session a user is talking in must not have to out-rank its own history.

top_k_per_layer is applied PER PARENT, so every session under a user competes for the same places.
The current session is the newest and least summarised, which makes it the likeliest to lose -- and
losing it means asking about something said minutes ago and being handed a two-year-old session
instead. It is therefore admitted outside the ranking, and does not consume a ranked slot.
"""
from __future__ import annotations

import unittest

from matrixark_mcp_core_node_tree import tree_first_traversal


def node(path, score):
    return {"node_path": list(path), "score": score, "depth": len(path),
            "node_hash": abs(hash(tuple(path))) % (10 ** 9)}


def build(current_score=0.0, others=8):
    """One user, several sessions. The current session scores WORST on purpose."""
    scores = {}
    for entry in [node(["tenant:t"], 0.9), node(["tenant:t", "user:u"], 0.9)]:
        scores[entry["node_hash"]] = entry
    for index in range(others):
        entry = node(["tenant:t", "user:u", "session:s%02d" % index], 0.5 + index * 0.01)
        scores[entry["node_hash"]] = entry
    current = node(["tenant:t", "user:u", "session:current"], current_score)
    scores[current["node_hash"]] = current
    return scores, tuple(current["node_path"])


class CurrentSessionGuaranteeCase(unittest.TestCase):
    def test_worst_scoring_current_session_is_still_admitted(self):
        scores, current_path = build(current_score=0.0, others=8)
        result = tree_first_traversal(scores, top_k_per_layer=2,
                                      max_children_scored_per_parent=1000,
                                      guaranteed_path=current_path)
        self.assertIn(current_path, result["selected_paths"],
                      "the current session lost its place to higher-scoring history")

    def test_without_the_guarantee_it_is_evicted(self):
        """Documents the behaviour being fixed, so the guarantee cannot be quietly dropped."""
        scores, current_path = build(current_score=0.0, others=8)
        result = tree_first_traversal(scores, top_k_per_layer=2,
                                      max_children_scored_per_parent=1000)
        self.assertNotIn(current_path, result["selected_paths"])

    def test_guaranteeing_does_not_steal_a_ranked_slot(self):
        scores, current_path = build(current_score=0.0, others=8)
        without = tree_first_traversal(scores, top_k_per_layer=3,
                                       max_children_scored_per_parent=1000)
        with_guarantee = tree_first_traversal(scores, top_k_per_layer=3,
                                              max_children_scored_per_parent=1000,
                                              guaranteed_path=current_path)
        ranked_before = {p for p in without["selected_paths"] if p != current_path}
        ranked_after = {p for p in with_guarantee["selected_paths"] if p != current_path}
        self.assertTrue(ranked_before.issubset(ranked_after),
                        "admitting the current session displaced a node that had earned its place")

    def test_an_unknown_guaranteed_path_changes_nothing(self):
        scores, _current = build(current_score=0.0, others=8)
        absent = ("tenant:t", "user:u", "session:not-in-this-store")
        plain = tree_first_traversal(scores, top_k_per_layer=2,
                                     max_children_scored_per_parent=1000)
        guarded = tree_first_traversal(scores, top_k_per_layer=2,
                                       max_children_scored_per_parent=1000,
                                       guaranteed_path=absent)
        self.assertEqual(plain["selected_paths"], guarded["selected_paths"])


if __name__ == "__main__":
    unittest.main()
