# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Index terms no query can ask for are not written, and the two halves cannot drift apart.

Retrieval narrows using groups from `infer_secondary_index_filter_groups`, and
`passes_secondary_index_filters` only ever INTERSECTS a candidate's terms with those groups. The
inference emits a fixed set of KINDS, so a term whose kind is outside that set cannot appear in a
group, cannot intersect one, and cannot narrow a search or earn the hint boost -- whatever its
value.

On a 1 MB skill the unreachable terms were a large share of a 1,471 KB index, written and scanned
to affect nothing. The first cut of this filter claimed 1,418 KB by declaring only 14 kinds, but
seven more were emitted with computed values and had to be given back -- `heading_slug` alone is
990.7 KB. Measure the saving from the declared set, never from the first estimate.

The danger is drift. The declared set and the inference are only correct TOGETHER: a kind added to
the inference but not declared is filtered out at ingest, and the query needing it narrows to
nothing with nothing to notice. The first test reads the kinds straight out of the inference source
so that can only happen loudly.

That guard failed once, in the direction it exists to prevent. It scanned a fixed 12,000-character
slice of a 21,839-character function, found 14 of the 21 kinds, and reported full coverage while
seven were being dropped. It now takes the function's whole extent and asserts that extent -- a
scan that silently stops early is the failure mode, so the length is checked, not assumed.
"""
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from matrixark_mcp_core_query_analysis import (
    INFERABLE_SECONDARY_INDEX_KINDS,
    index_term_is_consultable,
)
from matrixark_mcp_core_scoring import passes_secondary_index_filters


class WhatAQueryCanAskFor(unittest.TestCase):
    def test_the_declared_set_covers_every_kind_the_inference_emits(self):
        """The drift guard, and the reason this file exists.

        Read from the inference SOURCE rather than a hand-kept list: a kind added there and not
        here would be dropped at ingest and the query that needed it would silently match nothing.
        This caught `source_role` missing the first time it ran.
        """
        import matrixark_mcp_core_query_analysis as qa
        with open(qa.__file__, encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("def deterministic_secondary_index_filter_groups")
        marker = chr(10) + "def "
        end = source.find(marker, start + 4)
        body = source[start:end if end != -1 else len(source)]
        # The whole function, not a fixed slice. An earlier version read 12,000 characters of a
        # 21,839-character function, saw 14 of its 21 kinds, and reported full coverage while
        # seven were being dropped at ingest.
        self.assertGreater(len(body), 12000,
                           "the function is shorter than expected; check the extent, not the slice")
        # Both call shapes: a literal value and a computed one. Matching only literals is how the
        # six computed emissions were missed.
        emitted = set(re.findall(r'context_index_name\(\s*"([a-z_]+)"', body))
        self.assertTrue(emitted, "found no emitted kinds -- the parse is wrong, not the code")
        self.assertGreaterEqual(
            len(emitted), 21,
            "expected at least the 21 kinds this function emitted when the guard was written; "
            "fewer means the parse stopped early again")
        missing = emitted - set(INFERABLE_SECONDARY_INDEX_KINDS)
        self.assertEqual(
            set(), missing,
            "the inference can emit %s but ingest would filter them out, so a query using them "
            "would narrow to nothing" % sorted(missing))

    def test_a_consultable_kind_is_kept(self):
        for kind in sorted(INFERABLE_SECONDARY_INDEX_KINDS):
            self.assertTrue(index_term_is_consultable("%s:whatever" % kind))

    def test_the_seven_kinds_the_first_guard_missed_are_consultable(self):
        """A named regression guard for the kinds a truncated scan let through.

        Each of these is emitted by the inference with a COMPUTED value -- `context_index_name`
        called on a variable rather than a literal -- and each sat past the 12,000-character
        window the first guard read. A query inferring any of them narrowed to nothing.
        """
        for kind in ("heading_slug", "memory_selection_quality", "relative_path",
                     "resource_type", "skill_tool", "skill_trigger", "unit_kind"):
            self.assertIn(kind, INFERABLE_SECONDARY_INDEX_KINDS)
            self.assertTrue(index_term_is_consultable("%s:whatever" % kind),
                            "%s is emitted by the inference; filtering it at ingest makes a "
                            "query that asks for it match nothing" % kind)

    def test_terms_no_query_can_reach_are_still_dropped(self):
        # What remains genuinely unreachable: no inference path emits either kind, so neither can
        # appear in a group, intersect one, or earn the hint boost -- whatever its value.
        for term in ("keyword:checkout", "skill_name:acme"):
            self.assertFalse(index_term_is_consultable(term),
                             "%s is filtered at ingest; if a query can now ask for it, the "
                             "declared set must say so" % term)

    def test_a_term_with_no_kind_is_not_consultable(self):
        for term in ("", "novalue", ":", "  "):
            self.assertFalse(index_term_is_consultable(term))

    def test_dropping_them_cannot_change_narrowing(self):
        """The claim, exercised rather than argued.

        Narrowing intersects a candidate's terms with the inferred groups. Adding or removing
        terms whose kind is not in any group must not move the outcome either way.
        """
        groups = [{"entity_type:location"}, {"source_type:message"}]
        kept = {"entity_type:location", "source_type:message"}
        dropped = {"keyword:checkout", "skill_name:acme"}
        self.assertEqual(
            passes_secondary_index_filters(kept, groups),
            passes_secondary_index_filters(kept | dropped, groups),
            "un-consultable terms changed the narrowing outcome")
        # and the negative case, so this is not passing because everything passes
        self.assertFalse(passes_secondary_index_filters(dropped, groups))
        self.assertTrue(passes_secondary_index_filters(kept, groups))


MODULE = "matrixark_mcp_ingest_resource_chunk_records"
VARIABLE = "MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS"


class TheFilterIsOnAndActuallyFilters(unittest.TestCase):
    """Reading the flag's default means re-importing, which is only safe if it is undone.

    These tests drop the module from `sys.modules` so its import-time flag is evaluated again.
    Leaving the replacement behind hands every later importer a different module object than the
    one its collaborators already hold, and they fail for reasons unrelated to themselves. Both
    the module table and the environment are put back.
    """

    def setUp(self):
        self._module = sys.modules.get(MODULE)
        self._variable = os.environ.get(VARIABLE)

    def tearDown(self):
        sys.modules.pop(MODULE, None)
        if self._module is not None:
            sys.modules[MODULE] = self._module
        os.environ.pop(VARIABLE, None)
        if self._variable is not None:
            os.environ[VARIABLE] = self._variable

    def _reimport(self):
        import importlib
        sys.modules.pop(MODULE, None)
        return importlib.import_module(MODULE)

    def test_the_default_is_on(self):
        os.environ.pop(VARIABLE, None)
        self.assertTrue(self._reimport().INDEX_ONLY_CONSULTABLE_TERMS)

    def test_the_escape_hatch_works(self):
        os.environ[VARIABLE] = "0"
        self.assertFalse(self._reimport().INDEX_ONLY_CONSULTABLE_TERMS)


if __name__ == "__main__":
    unittest.main()
