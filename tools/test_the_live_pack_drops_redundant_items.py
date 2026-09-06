# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The pack a live retrieve builds drops redundant items, as its own knob says it does.

`MATRIXARK_PACK_DROP_REDUNDANT_ITEMS` defaults to "1" and is declared as a tenant knob
(`pack_drop_redundant_items`, bool, default True). `matrixark_mcp_context_pack` has carried the
filter behind it since it was written, with a docstring that names the exact case: an entity item is
a projection of the event it came from, so ``user: I live in Kyoto and my favorite drink is
matcha.`` and ``preference: preference = drink is matcha`` are both billed to the reader's budget.

`serving_ref_groups_for_pack` is implemented twice, and `matrixark_mcp_core_packing`'s copy never
had the filter -- the string `drop_redundant` did not appear in that file. That is the copy a live
retrieve reaches: `matrixark_local_adapter_retrieve`, `matrixark_mcp_local_adapter` and
`matrixark_mcp_core` all resolve the name there. So the knob was on, the filter existed, and the
pack shipped the entity beside the event that already contained it.

Measured before the fix, on the docstring's own example:

    matrixark_mcp_context_pack   2 items   (the projection dropped)
    matrixark_mcp_core_packing   3 items

This is the third of this shape found in the same family, after the near-duplicate threshold and
`MATRIXARK_PACK_RAW_PRECISION`. The check is written the way those are: not "does the module have a
filter" but "does the SETTING change what a live retrieve packs", with a control that the corpus
can tell the two apart -- an ordinary pack with no projection in it agrees under both copies, and a
comparison built from one would have passed all along.
"""
from __future__ import annotations

import importlib
import os
import unittest

ENV = "MATRIXARK_PACK_DROP_REDUNDANT_ITEMS"
KNOB = "pack_drop_redundant_items"

#: The docstring's own example: an event, the entity projected out of it, and an entity that is NOT
#: contained in anything. The third is what shows the filter is selective.
REFS = [
    {"ref_type": "event", "ref_hash": "h1", "context_class": "event", "score": 0.9,
     "text": "user: I live in Kyoto and my favorite drink is matcha.",
     "memory_scope": "session", "session_continuity": "same_session"},
    {"ref_type": "entity", "ref_hash": "h2", "context_class": "entity", "score": 0.8,
     "text": "preference: preference = drink is matcha",
     "memory_scope": "session", "session_continuity": "same_session"},
    {"ref_type": "entity", "ref_hash": "h3", "context_class": "entity", "score": 0.7,
     "text": "location: location = Kyoto",
     "memory_scope": "session", "session_continuity": "same_session"},
]

#: Everything that builds groups for a pack on a live path.
LIVE_CONSUMERS = (
    "matrixark_local_adapter_retrieve",
    "matrixark_mcp_local_adapter",
    "matrixark_mcp_core",
)


def _import(name: str):
    try:
        return importlib.import_module("tools." + name)
    except ImportError:
        return importlib.import_module(name)


def _texts(groups):
    return [item.get("text", "") for group in groups for item in (group.get("items") or [])]


class TheLivePackDropsRedundantItemsTest(unittest.TestCase):

    def setUp(self) -> None:
        _import("matrixark_mcp_local_adapter")
        self.context_pack = _import("matrixark_mcp_context_pack")
        self.core_packing = _import("matrixark_mcp_core_packing")
        self._env = os.environ.get(ENV)

    def tearDown(self) -> None:
        if self._env is None:
            os.environ.pop(ENV, None)
        else:
            os.environ[ENV] = self._env
        getattr(self.core_packing, "_PACK_REDUNDANCY", {}).clear()

    def _build(self, module):
        return _texts(module.serving_ref_groups_for_pack([dict(r) for r in REFS]))

    def test_the_copy_a_live_retrieve_reaches_drops_the_projection(self) -> None:
        built = self._build(self.core_packing)
        self.assertNotIn(
            "preference: preference = drink is matcha", built,
            "the pack still carries the entity projected out of an event that already contains it, "
            "which is the case the filter exists for and the knob says is handled")
        self.assertIn(
            "location: location = Kyoto", built,
            "the filter removed an entity nothing else carries, so it is not selective and is "
            "losing content rather than de-duplicating it")

    def test_both_group_builders_agree(self) -> None:
        self.assertEqual(
            self._build(self.context_pack), self._build(self.core_packing),
            "the two group builders disagree, so what a pack contains depends on which module the "
            "caller reached")

    def test_the_live_consumers_reach_a_builder_that_filters(self) -> None:
        for name in LIVE_CONSUMERS:
            module = _import(name)
            builder = getattr(module, "serving_ref_groups_for_pack", None)
            if builder is None:
                continue
            built = _texts(builder([dict(r) for r in REFS]))
            self.assertNotIn(
                "preference: preference = drink is matcha", built,
                "%s builds a pack that keeps the projection, so the knob does not reach it" % name)

    def test_the_knob_is_what_decides_it(self) -> None:
        """Control. Without it, the assertions above pass for a builder that always drops."""
        os.environ[ENV] = "0"
        getattr(self.core_packing, "_PACK_REDUNDANCY", {}).clear()
        off = self._build(self.core_packing)
        self.assertIn(
            "preference: preference = drink is matcha", off,
            "setting %s to 0 must turn the filter OFF -- that is how an operator disables it" % ENV)

        os.environ[ENV] = "1"
        getattr(self.core_packing, "_PACK_REDUNDANCY", {}).clear()
        on = self._build(self.core_packing)
        self.assertNotIn("preference: preference = drink is matcha", on)
        self.assertEqual(len(off) - 1, len(on), "exactly one item should differ between the two")

    def test_the_tenant_knob_and_the_code_agree_on_the_default(self) -> None:
        policy = _import("matrixark_tenant_policy")
        declared = policy.KNOBS.get(KNOB)
        self.assertIsNotNone(
            declared,
            "%s is no longer declared as a tenant knob. If it was withdrawn this file should say "
            "so; if renamed, follow it" % KNOB)
        self.assertEqual(
            ENV, declared.env,
            "the knob no longer maps to %s, which is the variable the filter reads" % ENV)
        self.assertIs(
            True, declared.default,
            "the tenant knob declares a default of %r while the code reads 1 -- an operator "
            "reading one surface would be told the opposite of what runs" % declared.default)
        self.assertIn(
            KNOB, policy.READ_PATH_KNOBS,
            "%s is no longer listed as a read-path knob, and it is read while a pack is built"
            % KNOB)


if __name__ == "__main__":
    unittest.main()
