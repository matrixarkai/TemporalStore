#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The pack redundancy sweep drops exactly the items a longer one already carries.

`drop_redundant_pack_items` is the largest Python cost in a warm retrieve, and it is quadratic in
the number of pack items. It was rewritten to sort by length once and form only the pairs that
could match, which is only sound because the rule is order-independent: an item is redundant when
some STRICTLY LONGER item's text contains it, and nothing about the visiting order changes that.

An optimisation of a filter is worth nothing if it quietly changes what the filter keeps, and the
failure would be invisible -- a pack with one fact too many, or one fact missing, is still a pack.
So the assertions here are against the RULE, restated independently, rather than against a recorded
output: the rule restated brute-force over random packs, and the specific shapes where reordering could
plausibly go wrong.
"""
from __future__ import annotations

import random
import unittest

import matrixark_mcp_context_pack as pack


def _reference(groups):
    """The rule, written the slowest and most obvious way.

    Deliberately not sharing code with the implementation: a check that reuses the thing it
    checks agrees with it by construction.
    """
    texts = []
    for group in groups:
        for item in group.get("items") or []:
            texts.append((pack._normalized_item_text(item), item))
    dropped = set()
    for text, item in texts:
        if not text or len(text) < 8:
            continue
        for other_text, other in texts:
            if other is item:
                continue
            if len(other_text) > len(text) and text in other_text:
                dropped.add(id(item))
                break
    out = []
    for group in groups:
        kept = [item for item in (group.get("items") or []) if id(item) not in dropped]
        if kept:
            out.append({**group, "items": kept, "n": len(kept)})
    return out


def _shape(groups):
    return [[pack._normalized_item_text(item) for item in group["items"]] for group in groups]


def _pack(texts, per_group=3):
    items = [{"text": text} for text in texts]
    return [{"items": items[i:i + per_group], "n": len(items[i:i + per_group])}
            for i in range(0, len(items), per_group)]


class TheRedundancySweepKeepsWhatTheRuleKeepsTest(unittest.TestCase):

    def test_it_agrees_with_the_rule_on_random_packs(self) -> None:
        """The general case. Random packs with deliberate containment, compared against the rule
        written out independently."""
        rng = random.Random(20260910)
        words = ["kyoto", "matcha", "widget", "deadline", "friday", "budget", "index", "profile"]
        for trial in range(60):
            texts = []
            for _ in range(rng.randint(4, 30)):
                body = " ".join(rng.choice(words) for _ in range(rng.randint(2, 10)))
                texts.append(body)
                if rng.random() < 0.5:
                    texts.append("user: I said %s and then some more words." % body)
            rng.shuffle(texts)
            groups = _pack(texts)
            self.assertEqual(
                _shape(_reference([dict(g) for g in groups])),
                _shape(pack.drop_redundant_pack_items([dict(g) for g in groups])),
                "trial %d: the sweep and the rule disagree" % trial)

    def test_a_chain_keeps_only_the_longest(self) -> None:
        """A contains B contains C. The reordering could plausibly break this: if the middle is
        marked redundant before the shortest is examined, the shortest must still be dropped."""
        groups = _pack(["matcha drink", "I like matcha drink today",
                        "user: I like matcha drink today and yesterday as well."])
        kept = _shape(pack.drop_redundant_pack_items(groups))
        flat = [text for group in kept for text in group]
        self.assertEqual(1, len(flat), "a containment chain must leave exactly one item: %r" % flat)
        self.assertIn("yesterday", flat[0], "the LONGEST item is the one that survives")

    def test_equal_length_items_are_both_kept(self) -> None:
        """Only a STRICTLY longer item wins. Two identical-length texts, even identical ones, must
        both survive -- a sort by length puts them adjacent, which is where an off-by-one in the
        break condition would show."""
        groups = _pack(["kyoto matcha widget", "kyoto matcha widget"])
        flat = [t for g in _shape(pack.drop_redundant_pack_items(groups)) for t in g]
        self.assertEqual(2, len(flat), "equal-length items must both be kept: %r" % flat)

    def test_a_short_item_is_never_dropped(self) -> None:
        """Items under the eight-character floor are exempt, and the floor is checked before any
        pairing. A rewrite that moved the floor inside the loop would drop them."""
        groups = _pack(["abc", "user: abc and a great deal more text besides, at length."])
        flat = [t for g in _shape(pack.drop_redundant_pack_items(groups)) for t in g]
        self.assertIn("abc", flat, "an item below the length floor must survive: %r" % flat)

    def test_a_group_emptied_by_the_sweep_is_dropped(self) -> None:
        """The documented behaviour of the tail half, which the rewrite did not touch and which a
        careless edit to the survivor loop would break."""
        long_text = "user: I like matcha drink today and yesterday as well, at some length."
        items = [{"text": "matcha drink"}]
        groups = [{"items": items, "n": 1}, {"items": [{"text": long_text}], "n": 1}]
        kept = pack.drop_redundant_pack_items(groups)
        self.assertEqual(1, len(kept), "the emptied group should be gone: %r" % _shape(kept))
        self.assertEqual(1, kept[0]["n"], "n must be recomputed from the surviving items")

    def test_nothing_redundant_returns_the_groups_untouched(self) -> None:
        """The early return. It is what keeps the common case cheap, and a rewrite that always
        rebuilt would still pass every assertion above."""
        groups = _pack(["kyoto matcha widget", "deadline friday budget", "index profile summary"])
        self.assertIs(
            groups, pack.drop_redundant_pack_items(groups),
            "with nothing redundant the original list should come back unchanged")


if __name__ == "__main__":
    unittest.main()
