# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Index terms no query can ask for are not written, and the two halves cannot drift apart.

Retrieval narrows using groups from `infer_secondary_index_filter_groups`, and
`passes_secondary_index_filters` only ever INTERSECTS a candidate's terms with those groups. The
inference emits a fixed set of KINDS, so a term whose kind is outside that set cannot appear in a
group, cannot intersect one, and cannot narrow a search or earn the hint boost -- whatever its
value.

On a 1 MB skill those terms were 1,418 KB of a 1,471 KB index, 15.7% of the whole ingest, written
and scanned to affect nothing. Dropping them takes amplification 8.6x to 7.2x and makes embeddings
the majority of the footprint (44.6% -> 53.0%), which is what an embedding-first store should look
like.

The danger is drift. The declared set and the inference are only correct TOGETHER: a kind added to
the inference but not declared would be filtered out at ingest, and the query needing it would
narrow to nothing with nothing to notice. The first test reads the kinds straight out of the
inference source so that can only happen loudly.
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
        emitted = set(re.findall(r'context_index_name\("([a-z_]+)"', source[start:start + 12000]))
        self.assertTrue(emitted, "found no emitted kinds -- the parse is wrong, not the code")
        missing = emitted - set(INFERABLE_SECONDARY_INDEX_KINDS)
        self.assertEqual(
            set(), missing,
            "the inference can emit %s but ingest would filter them out, so a query using them "
            "would narrow to nothing" % sorted(missing))

    def test_a_consultable_kind_is_kept(self):
        for kind in sorted(INFERABLE_SECONDARY_INDEX_KINDS):
            self.assertTrue(index_term_is_consultable("%s:whatever" % kind))

    def test_the_bulk_of_a_skill_ingest_is_not_consultable(self):
        # These are what a skill actually wrote, and together they were 96.4% of its index.
        for term in ("heading_slug:step-1", "keyword:checkout", "unit_kind:section",
                     "resource_type:skill", "skill_name:acme", "relative_path:a/b.md"):
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
        dropped = {"heading_slug:step-1", "keyword:checkout", "skill_name:acme"}
        self.assertEqual(
            passes_secondary_index_filters(kept, groups),
            passes_secondary_index_filters(kept | dropped, groups),
            "un-consultable terms changed the narrowing outcome")
        # and the negative case, so this is not passing because everything passes
        self.assertFalse(passes_secondary_index_filters(dropped, groups))
        self.assertTrue(passes_secondary_index_filters(kept, groups))


class TheFilterIsOnAndActuallyFilters(unittest.TestCase):
    def test_the_default_is_on(self):
        os.environ.pop("MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS", None)
        import importlib
        for name in [m for m in list(sys.modules) if m.startswith("matrixark_mcp_ingest_resource_chunk_records")]:
            del sys.modules[name]
        mod = importlib.import_module("matrixark_mcp_ingest_resource_chunk_records")
        self.assertTrue(mod.INDEX_ONLY_CONSULTABLE_TERMS)

    def test_the_escape_hatch_works(self):
        import importlib
        os.environ["MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS"] = "0"
        for name in [m for m in list(sys.modules) if m.startswith("matrixark_mcp_ingest_resource_chunk_records")]:
            del sys.modules[name]
        mod = importlib.import_module("matrixark_mcp_ingest_resource_chunk_records")
        self.assertFalse(mod.INDEX_ONLY_CONSULTABLE_TERMS)
        os.environ.pop("MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS", None)


if __name__ == "__main__":
    unittest.main()
