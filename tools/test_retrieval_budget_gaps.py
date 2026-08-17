#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Unit tests for the retrieval-budget gap fixes.

Covers the four fixes that make the returned context pack respect the caller's
budget symmetrically and de-noise it now that defaults return more:

  1. budget-alias      -- the client-facing `budget` knob maps onto
                          max_context_tokens (max_context_tokens wins if both).
  2. harmonized-default -- core and runtime-config agree on the 500000 default.
  3. symmetric-clamp    -- a pack with more-than-budget candidates is trimmed to
                          <= budget; a small query returns a small pack
                          (ceiling, not target); raising the budget allows more.
  4. dedup              -- near-duplicate candidates collapse to the single
                          highest-ranked instance before packing.

Run from the tools/ directory: ``python3 test_retrieval_budget_gaps.py``.
"""
from __future__ import annotations

import unittest

try:  # package path
    from tools import matrixark_mcp_core as mcp_core
    from tools import matrixark_mcp_runtime_config as runtime_config
    from tools import matrixark_http as mcp_http
except ImportError:  # top-level path (run from tools/)
    import matrixark_mcp_core as mcp_core
    import matrixark_mcp_runtime_config as runtime_config
    import matrixark_http as mcp_http


def _fact_candidate(ref_hash: int, text: str, score: float, ref_type: str = "event") -> dict:
    return {"ref_type": ref_type, "ref_hash": ref_hash, "text": text, "score": score}


class RetrieveBudgetAliasTest(unittest.TestCase):
    """Fix 1: budget -> max_context_tokens alias in the gateway."""

    def test_budget_populates_max_context_tokens(self) -> None:
        args = {"query": "q", "budget": 42000}
        mcp_http.apply_retrieve_budget_alias(args)
        self.assertEqual(42000, args["max_context_tokens"])

    def test_explicit_max_context_tokens_wins_over_budget(self) -> None:
        args = {"query": "q", "budget": 42000, "max_context_tokens": 9000}
        mcp_http.apply_retrieve_budget_alias(args)
        self.assertEqual(9000, args["max_context_tokens"])

    def test_empty_max_context_tokens_falls_back_to_budget(self) -> None:
        args = {"query": "q", "budget": 7000, "max_context_tokens": None}
        mcp_http.apply_retrieve_budget_alias(args)
        self.assertEqual(7000, args["max_context_tokens"])

    def test_no_budget_leaves_args_untouched(self) -> None:
        args = {"query": "q"}
        mcp_http.apply_retrieve_budget_alias(args)
        self.assertNotIn("max_context_tokens", args)


class HarmonizedDefaultTest(unittest.TestCase):
    """Fix 2: core and runtime-config agree on the default ceiling."""

    def test_core_and_runtime_config_agree(self) -> None:
        self.assertEqual(
            mcp_core.DEFAULT_MAX_CONTEXT_TOKENS,
            runtime_config.DEFAULT_MAX_CONTEXT_TOKENS,
        )

    def test_default_is_500000_out_of_the_box(self) -> None:
        # Only meaningful when the operator has not overridden the env var.
        import os

        if os.environ.get("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"):
            self.skipTest("env override set; default value not under test")
        self.assertEqual(500000, mcp_core.DEFAULT_MAX_CONTEXT_TOKENS)


class ClampRefsToTokenBudgetTest(unittest.TestCase):
    """Fix 3: the symmetric clamp helper used by the native pack path."""

    def _refs(self, counts: list[int]) -> list[dict]:
        return [
            {"ref_type": "event", "ref_hash": index, "text": f"ref {index}", "token_count": count}
            for index, count in enumerate(counts)
        ]

    def test_over_budget_pack_is_trimmed_to_fit(self) -> None:
        refs = self._refs([5, 5, 5, 5])  # 20 tokens total, ranked best-first
        kept, trimmed, used = mcp_core.clamp_refs_to_token_budget(refs, 12)
        self.assertEqual(2, len(kept))          # 5 + 5 = 10 <= 12; next would be 15 > 12
        self.assertEqual(2, len(trimmed))
        self.assertLessEqual(used, 12)
        self.assertEqual([0, 1], [ref["ref_hash"] for ref in kept])  # highest-ranked kept

    def test_raising_budget_allows_more(self) -> None:
        refs = self._refs([5, 5, 5, 5])
        kept_small, _, _ = mcp_core.clamp_refs_to_token_budget(refs, 12)
        kept_large, trimmed_large, used_large = mcp_core.clamp_refs_to_token_budget(refs, 100)
        self.assertGreater(len(kept_large), len(kept_small))
        self.assertEqual(4, len(kept_large))
        self.assertEqual(0, len(trimmed_large))
        self.assertEqual(20, used_large)

    def test_under_budget_pack_is_not_padded(self) -> None:
        refs = self._refs([3, 4])  # 7 tokens, well under budget -- ceiling not target
        kept, trimmed, used = mcp_core.clamp_refs_to_token_budget(refs, 500000)
        self.assertEqual(2, len(kept))
        self.assertEqual([], trimmed)
        self.assertEqual(7, used)

    def test_top_ref_kept_even_when_over_a_tiny_budget(self) -> None:
        refs = self._refs([50, 50])
        kept, trimmed, used = mcp_core.clamp_refs_to_token_budget(refs, 1)
        self.assertEqual(1, len(kept))          # never zero a relevant pack
        self.assertEqual(1, len(trimmed))


class SymmetricSelectionClampTest(unittest.TestCase):
    """Fix 3: select_token_budgeted_refs enforces the ceiling both directions."""

    def _distinct_candidates(self, n: int) -> list[dict]:
        # Distinct texts (no accidental near-dup) so only the budget gates them.
        words = [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
            "hotel", "india", "juliet", "kilo", "lima", "mike", "november",
        ]
        return [
            _fact_candidate(1000 + i, f"{words[i % len(words)]} distinct fact number {i}", 0.95 - i * 0.01)
            for i in range(n)
        ]

    def test_more_than_budget_candidates_trimmed_to_budget(self) -> None:
        candidates = self._distinct_candidates(12)
        total_tokens = sum(mcp_core.token_count(c["text"]) for c in candidates)
        budget = max(1, total_tokens // 4)
        selected, used_tokens, _dropped = mcp_core.select_token_budgeted_refs(
            candidates, [], max_context_tokens=budget, auxiliary_quota=0, min_score=0.0
        )
        self.assertLessEqual(used_tokens, budget)
        self.assertGreaterEqual(len(selected), 1)
        self.assertLess(len(selected), len(candidates))

    def test_small_query_returns_small_pack_not_padded_to_budget(self) -> None:
        candidates = self._distinct_candidates(2)
        selected, used_tokens, _dropped = mcp_core.select_token_budgeted_refs(
            candidates, [], max_context_tokens=500000, auxiliary_quota=0, min_score=0.0
        )
        self.assertEqual(2, len(selected))              # ceiling, not target
        self.assertLess(used_tokens, 100)               # nowhere near 500000

    def test_raising_budget_allows_more_refs(self) -> None:
        candidates = self._distinct_candidates(12)
        total_tokens = sum(mcp_core.token_count(c["text"]) for c in candidates)
        tight, _, _ = mcp_core.select_token_budgeted_refs(
            candidates, [], max_context_tokens=max(1, total_tokens // 4), auxiliary_quota=0, min_score=0.0
        )
        wide, _, _ = mcp_core.select_token_budgeted_refs(
            candidates, [], max_context_tokens=total_tokens * 4, auxiliary_quota=0, min_score=0.0
        )
        self.assertGreater(len(wide), len(tight))


class NearDuplicateDedupTest(unittest.TestCase):
    """Fix 4: near-duplicate suppression in the ref-selection path."""

    def _dup_candidate_set(self) -> list[dict]:
        # base has 11 unique tokens; the near-dup appends one word -> Jaccard
        # 11/12 ~= 0.92, above the 0.85 default. A distinct fact stays distinct.
        base = "the deployment pipeline was migrated to the staging region on friday morning"
        return [
            _fact_candidate(1, base, 0.97),                # highest-ranked original
            _fact_candidate(2, base + " cleanly", 0.95),   # near-duplicate of #1
            _fact_candidate(3, "quarterly revenue exceeded the forecast by twelve percent", 0.90),
        ]

    def test_near_duplicates_collapse_to_highest_ranked(self) -> None:
        candidates = self._dup_candidate_set()
        selected, _used, dropped = mcp_core.select_token_budgeted_refs(
            candidates, [], max_context_tokens=500000, auxiliary_quota=0, min_score=0.0
        )
        selected_hashes = {ref.get("ref_hash") for ref in selected}
        self.assertIn(1, selected_hashes)          # highest-ranked original kept
        self.assertNotIn(2, selected_hashes)       # near-duplicate collapsed away
        self.assertIn(3, selected_hashes)          # distinct fact kept
        self.assertEqual(1, dropped["near_duplicate"])

    def test_threshold_zero_disables_dedup(self) -> None:
        candidates = self._dup_candidate_set()
        selected, _used, dropped = mcp_core.select_token_budgeted_refs(
            candidates,
            [],
            max_context_tokens=500000,
            auxiliary_quota=0,
            min_score=0.0,
            near_duplicate_overlap_threshold=0.0,
        )
        selected_hashes = {ref.get("ref_hash") for ref in selected}
        self.assertEqual({1, 2, 3}, selected_hashes)
        self.assertEqual(0, dropped["near_duplicate"])

    def test_distinct_candidates_are_all_kept(self) -> None:
        candidates = [
            _fact_candidate(10, "database connection pool exhausted under load", 0.95),
            _fact_candidate(11, "user preference set to dark theme in settings", 0.94),
            _fact_candidate(12, "release cut from the stable branch on tuesday", 0.93),
        ]
        selected, _used, dropped = mcp_core.select_token_budgeted_refs(
            candidates, [], max_context_tokens=500000, auxiliary_quota=0, min_score=0.0
        )
        self.assertEqual(3, len(selected))
        self.assertEqual(0, dropped["near_duplicate"])


if __name__ == "__main__":
    unittest.main()
