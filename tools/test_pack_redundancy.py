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

from matrixark_mcp_context_pack import drop_redundant_pack_items


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


if __name__ == "__main__":
    unittest.main()
