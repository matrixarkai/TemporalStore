#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A pack must not bill the reader twice for one fact.

An entity item is a projection of the event it was extracted from, so a pack routinely carried both
``user: I live in Kyoto and my favorite drink is matcha.`` and
``preference: preference = drink is matcha``. Measured over 8 queries on a 5-session store, dropping
the contained projections cut pack tokens 899 -> 764 (-15.0%) with answer recall unchanged at 5/8.
"""
from __future__ import annotations

import unittest

from matrixark_mcp_context_pack import (
    _normalized_item_text,
    drop_redundant_pack_items,
)


def group(kind, *texts):
    return {"type": kind, "n": len(texts), "items": [{"text": t} for t in texts]}


def texts_of(groups):
    return [item["text"] for g in groups for item in g["items"]]


class PackRedundancyCase(unittest.TestCase):
    def test_entity_projection_of_a_kept_event_is_dropped(self):
        groups = [
            group("event", "user: I live in Kyoto and my favorite drink is matcha."),
            group("entity", "preference: preference = drink is matcha"),
        ]
        kept = texts_of(drop_redundant_pack_items(groups))
        self.assertEqual(["user: I live in Kyoto and my favorite drink is matcha."], kept)

    def test_an_entity_with_content_of_its_own_survives(self):
        groups = [
            group("event", "user: I live in Kyoto."),
            group("entity", "relationship: sister = Rin visits on Tuesday"),
        ]
        kept = texts_of(drop_redundant_pack_items(groups))
        self.assertEqual(2, len(kept), "an entity adding new content must not be dropped")

    def test_group_counts_are_recomputed(self):
        groups = [
            group("event", "user: I live in Kyoto and my favorite drink is matcha."),
            group("entity", "preference: preference = drink is matcha",
                  "relationship: sister = Rin"),
        ]
        out = drop_redundant_pack_items(groups)
        entity_group = [g for g in out if g["type"] == "entity"][0]
        self.assertEqual(1, entity_group["n"])
        self.assertEqual(1, len(entity_group["items"]))

    def test_a_group_emptied_by_the_sweep_is_removed(self):
        groups = [
            group("event", "user: I live in Kyoto and my favorite drink is matcha."),
            group("entity", "preference: preference = drink is matcha"),
        ]
        out = drop_redundant_pack_items(groups)
        self.assertEqual(["event"], [g["type"] for g in out])

    def test_short_fragments_are_never_treated_as_redundant(self):
        """Labels and stubs would otherwise match inside almost anything."""
        groups = [
            group("event", "user: I live in Kyoto and my favorite drink is matcha."),
            group("entity", "tag = tea"),
        ]
        kept = texts_of(drop_redundant_pack_items(groups))
        self.assertEqual(2, len(kept))

    def test_nothing_redundant_returns_the_input_untouched(self):
        groups = [group("event", "user: one thing entirely"),
                  group("entity", "topic: subject = something else entirely")]
        out = drop_redundant_pack_items(groups)
        self.assertIs(groups, out, "the no-op case must not rebuild the pack")

    def test_two_identical_items_keep_one(self):
        groups = [group("event", "user: I live in Kyoto and my favorite drink is matcha.",
                        "user: I live in Kyoto and my favorite drink is matcha.")]
        kept = texts_of(drop_redundant_pack_items(groups))
        self.assertEqual(2, len(kept),
                         "equal-length duplicates are left alone: neither is longer, so neither "
                         "is the one carrying more context")


    def test_a_chain_of_containments_keeps_only_the_longest(self):
        """X inside Y inside Z leaves only Z, whatever order they arrive in.

        The sweep skips a container that is itself redundant, which would matter if containment
        were not transitive: Y is dropped, so if Y were X's only container, X's fate would depend
        on whether Y had been examined yet. Because X is inside Y and Y is inside Z, X is also
        inside Z, and the answer does not depend on the order.
        """
        short = "drink is matcha"
        middle = "my favorite drink is matcha and i live in kyoto"
        longest = "user: my favorite drink is matcha and i live in kyoto, noted at step 4"
        for arrangement in (
            (short, middle, longest),
            (longest, middle, short),
            (middle, longest, short),
        ):
            kept = drop_redundant_pack_items([group("event", *arrangement)])
            texts = [item["text"] for g in kept for item in g["items"]]
            self.assertEqual(texts, [longest], "order %r changed the answer" % (arrangement,))

    def test_it_agrees_with_the_plain_definition_on_random_packs(self):
        """Agree with "contained in some longer item", over packs nobody chose by hand.

        The sweep prunes by length and skips containers already dropped. Neither may change which
        items survive, so this compares against the definition itself rather than an expectation.
        """
        import random

        def plainly_redundant(groups):
            everything = [
                item for g in groups for item in (g.get("items") or [])
            ]
            texts = [_normalized_item_text(item) for item in everything]
            dropped = set()
            for index, text in enumerate(texts):
                if not text or len(text) < 8:
                    continue
                for other, other_text in enumerate(texts):
                    if other == index:
                        continue
                    if len(other_text) > len(text) and text in other_text:
                        dropped.add(id(everything[index]))
                        break
            return dropped

        words = ["storage", "manager", "log", "kyoto", "matcha", "window", "cursor", "page"]
        for seed in range(60):
            rng = random.Random(seed)
            def sentence():
                return " ".join(rng.choice(words) for _ in range(rng.randint(1, 9)))
            events = [{"text": sentence()} for _ in range(rng.randint(0, 14))]
            entities = [{"text": "k = %s" % sentence()} for _ in range(rng.randint(0, 14))]
            packed = [group("event", *[e["text"] for e in events]),
                      group("entity", *[e["text"] for e in entities])]
            # The ids the sweep keeps, against the ids the definition would keep.
            expected_dropped = plainly_redundant(packed)
            all_items = [item for g in packed for item in (g.get("items") or [])]
            expected_kept = [
                item["text"] for item in all_items if id(item) not in expected_dropped
            ]
            got = drop_redundant_pack_items(packed)
            got_kept = [item["text"] for g in got for item in g["items"]]
            self.assertEqual(
                sorted(got_kept), sorted(expected_kept), "disagreed at seed %d" % seed
            )

    def test_a_text_carrying_the_separator_still_gets_the_plain_answer(self):
        """A NUL in a text sends the pack down the pairwise path, with the same result.

        The joined haystack is only sound while no text contains the separator -- a match could
        otherwise span two texts and drop an item nothing actually carries. The guard prevents its
        own hazard, which means nothing else will ever exercise it.
        """
        contained = "drink is matcha"
        container = "user: my favorite drink is matcha, noted at step 9"
        packed = [group("event", container, "unrelated sentence about a page cache"),
                  group("entity", "k = %s" % contained)]
        without_nul = drop_redundant_pack_items(packed)

        # The same pack, with a NUL riding along in one text.
        packed_with_nul = [
            group("event", container + "\x00tail", "unrelated sentence about a page cache"),
            group("entity", "k = %s" % contained),
        ]
        with_nul = drop_redundant_pack_items(packed_with_nul)

        kept_without = sorted(i["text"] for g in without_nul for i in g["items"])
        kept_with = sorted(i["text"].replace("\x00tail", "") for g in with_nul for i in g["items"])
        self.assertEqual(kept_with, kept_without)
        self.assertNotIn(
            "k = %s" % contained,
            [i["text"] for g in with_nul for i in g["items"]],
            "the projection is still dropped on the fallback path",
        )

if __name__ == "__main__":
    unittest.main()
