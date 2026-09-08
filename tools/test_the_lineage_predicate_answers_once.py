#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The debug-lineage predicate answers each field name once, not once per occurrence.

`strip_default_debug_lineage_fields` walks the whole pack and asks
`_is_default_hidden_debug_lineage_key` about every key of every dict it meets. A pack's keys are
schema field names, so it is asked the same handful over and over: measured on a served retrieve,
**1,027 calls across 133 distinct keys**, 154x repetition. Each uncached answer costs two string
allocations, a set membership test and three substring scans; the repeats bought nothing.

Remembering the answer is only safe because the predicate is pure -- it reads its argument and two
module constants, and neither constant is reassigned or mutated anywhere in the tree, tests
included. The tests below pin that equivalence rather than the speed, because a cache that returns a
different answer is a correctness bug wearing a performance improvement.
"""
from __future__ import annotations

import unittest

try:  # top-level (run from tools/) ...
    import matrixark_mcp_context_pack as pack
except ImportError:  # ... or package path.
    from tools import matrixark_mcp_context_pack as pack  # type: ignore


class TheAnswerIsRememberedNotRecomputed(unittest.TestCase):
    def setUp(self):
        pack._DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_ANSWERS.clear()
        self.addCleanup(pack._DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_ANSWERS.clear)

    def _keys(self):
        hidden = list(pack.DEFAULT_HIDDEN_DEBUG_LINEAGE_FIELDS)
        keys = list(hidden)
        keys += ["text", "memory_scope", "ref_type", "event_type", "source_role", "memory_layer"]
        keys += ["", " ", "Debug", "DEBUG", "  lineage  ", "x" * 200]
        keys += [name + "_suffix" for name in hidden[:12]]
        keys += ["PREFIX_" + name for name in hidden[:12]]
        keys += [None, 0, False, 1, 2.5]
        return keys

    def test_it_agrees_with_the_uncached_answer(self):
        for key in self._keys():
            self.assertEqual(
                pack._is_default_hidden_debug_lineage_key(key),
                pack._default_hidden_debug_lineage_key(key),
                "disagreed on %r" % (key,))

    def test_it_still_agrees_once_the_answer_is_cached(self):
        # The second ask is the one served from the cache, so the equivalence has to hold twice.
        keys = self._keys()
        for key in keys:
            pack._is_default_hidden_debug_lineage_key(key)
        for key in keys:
            self.assertEqual(
                pack._is_default_hidden_debug_lineage_key(key),
                pack._default_hidden_debug_lineage_key(key),
                "disagreed on the cached answer for %r" % (key,))

    def test_a_remembered_FALSE_is_returned_not_recomputed(self):
        # `answers.get(key)` returns None for a miss, so a cached False must not read as a miss --
        # the bug this would hide is silent, since recomputing returns the same answer, just slower.
        key = "text"
        self.assertFalse(pack._is_default_hidden_debug_lineage_key(key))
        self.assertIn(key, pack._DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_ANSWERS)
        self.assertIs(pack._DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_ANSWERS[key], False)
        self.assertFalse(pack._is_default_hidden_debug_lineage_key(key))

    def test_an_unhashable_key_is_answered_not_raised(self):
        # Dict keys are hashable by construction, so nothing should reach this -- but a serving path
        # must not turn an odd caller into a 500.
        self.assertEqual(
            pack._is_default_hidden_debug_lineage_key(["unhashable"]),
            pack._default_hidden_debug_lineage_key(["unhashable"]))

    def test_the_cache_is_bounded(self):
        cap = pack._DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_ANSWER_CAP
        for i in range(cap + 250):
            pack._is_default_hidden_debug_lineage_key("generated_field_%d" % i)
        self.assertLessEqual(len(pack._DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_ANSWERS), cap)
        # Past the cap it must still answer correctly, just without remembering.
        beyond = "generated_field_%d" % (cap + 249)
        self.assertEqual(
            pack._is_default_hidden_debug_lineage_key(beyond),
            pack._default_hidden_debug_lineage_key(beyond))

    def test_stripping_still_removes_the_hidden_fields(self):
        hidden = sorted(pack.DEFAULT_HIDDEN_DEBUG_LINEAGE_FIELDS)[:3]
        item = {"text": "kept", "nested": [{"text": "kept too"}]}
        for name in hidden:
            item[name] = "dropped"
            item["nested"][0][name] = "dropped"
        out = pack.strip_default_debug_lineage_fields(item)
        self.assertEqual(out["text"], "kept")
        self.assertEqual(out["nested"][0]["text"], "kept too")
        for name in hidden:
            self.assertNotIn(name, out)
            self.assertNotIn(name, out["nested"][0])


if __name__ == "__main__":
    unittest.main()
