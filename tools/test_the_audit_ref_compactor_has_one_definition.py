#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`compact_refs_for_audit` is defined twice, and the two disagree about what an audit records.

The function decides which fields of a selected ref survive into the audit record. Both copies are
live, and every adapter call site binds the same one:

    matrixark_mcp_local_adapter        -> matrixark_mcp_core's copy
    matrixark_local_adapter_ingest     -> matrixark_mcp_core's copy
    matrixark_local_adapter_retrieve   -> matrixark_mcp_core's copy
    matrixark_mcp_deadline_pack        -> matrixark_mcp_core's copy
    matrixark_mcp_context_pack         -> its own

The keep-lists are 39 fields and 62 fields, and the difference is not a superset:

    only matrixark_mcp_core keeps         sharing_scope
    only matrixark_mcp_context_pack keeps memory_scope, entity_type, entity_name,
                                          extraction_phase, current_state_policy,
                                          profile_current_state_representative,
                                          final_session_boundary, source_roles,
                                          source_role_counts, source_hook_types, and 14 more
                                          provenance counters

MEASURED on one ref carrying both kinds of field:

    core         -> ['ref_type', 'sharing_scope', 'text_preview']
    context_pack -> ['memory_scope', 'ref_type', 'source_roles', 'text_preview']

`sharing_scope` is the access-control field -- `scope_matches` reads it to admit a record as
`global_shared` or `tenant_shared` before any other check. So the copy that carries the richer
provenance is the one that does NOT record what sharing scope the served ref had, and the copy the
adapters actually use records the sharing scope but almost none of the provenance.

The two also clip text through differently named helpers: `clip_context_text` in core and
`_clip_context_text` in context_pack.

THIS FILE DOES NOT ASSERT THAT THEY AGREE, because they do not, and a guard that fails on the day
it is written tells nobody anything. It RECORDS the split in both directions, so a new divergence
fails here and a resolved one fails here too. Choosing what an audit record contains is a decision
about an audit trail, not a cleanup -- widening either copy changes what is written and kept.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

FUNCTION = "compact_refs_for_audit"

#: module -> how many fields its keep-list carries.
RECORDED_SIZES = {"matrixark_mcp_core": 39, "matrixark_mcp_context_pack": 62}

#: The asymmetry, stated as names rather than counts.
ONLY_CORE = {"sharing_scope"}
SOME_ONLY_CONTEXT_PACK = {"memory_scope", "entity_type", "entity_name", "extraction_phase",
                          "source_roles", "source_role_counts"}


def _keep_fields(stem):
    path = os.path.join(TOOLS, stem + ".py")
    with open(path, encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != FUNCTION:
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Assign) and getattr(sub.targets[0], "id", "") == "keep_fields":
                return [e.value for e in sub.value.elts
                        if isinstance(e, ast.Constant) and isinstance(e.value, str)]
    return []


class TheAuditRefCompactorHasOneDefinition(unittest.TestCase):

    def test_both_copies_still_carry_a_keep_list(self) -> None:
        """A floor. An empty keep-list would make every comparison below vacuous."""
        for stem, size in sorted(RECORDED_SIZES.items()):
            with self.subTest(module=stem):
                fields = _keep_fields(stem)
                self.assertTrue(fields, "%s.%s no longer has a keep_fields list" % (stem, FUNCTION))
                self.assertEqual(
                    size, len(fields),
                    "%s.%s now keeps %d fields, recorded as %d"
                    % (stem, FUNCTION, len(fields), size))

    def test_the_split_is_not_a_superset_either_way(self) -> None:
        """Recorded, both directions. Neither copy contains the other."""
        core = set(_keep_fields("matrixark_mcp_core"))
        pack = set(_keep_fields("matrixark_mcp_context_pack"))
        self.assertTrue(core - pack, "matrixark_mcp_context_pack now keeps everything core does")
        self.assertTrue(pack - core, "matrixark_mcp_core now keeps everything context_pack does")
        self.assertEqual(
            ONLY_CORE, core - pack,
            "the fields only matrixark_mcp_core keeps changed; recorded as %s" % sorted(ONLY_CORE))

    def test_the_richer_copy_is_the_one_that_drops_the_access_field(self) -> None:
        """The consequence, stated as the thing that is actually wrong.

        `sharing_scope` is what `scope_matches` reads to admit a record as global_shared or
        tenant_shared before any other check, so an audit without it cannot say what sharing scope
        the served ref had.
        """
        core = set(_keep_fields("matrixark_mcp_core"))
        pack = set(_keep_fields("matrixark_mcp_context_pack"))
        self.assertIn("sharing_scope", core)
        self.assertNotIn(
            "sharing_scope", pack,
            "matrixark_mcp_context_pack now records sharing_scope too -- that half of the split is "
            "resolved, so strike this test and say so")
        self.assertGreater(
            len(pack), len(core),
            "matrixark_mcp_context_pack is no longer the larger keep-list, so the sentence above "
            "about the richer copy dropping the access field no longer describes the tree")

    def test_the_provenance_fields_reach_only_one_copy(self) -> None:
        """The other half, asserted by name so a partial merge is visible."""
        core = set(_keep_fields("matrixark_mcp_core"))
        pack = set(_keep_fields("matrixark_mcp_context_pack"))
        for field in sorted(SOME_ONLY_CONTEXT_PACK):
            with self.subTest(field=field):
                self.assertIn(field, pack, "context_pack stopped keeping %s" % field)
                self.assertNotIn(
                    field, core,
                    "matrixark_mcp_core now keeps %s as well; the split is closing and this file "
                    "should record which copy won" % field)

    def test_the_two_copies_clip_text_through_different_helpers(self) -> None:
        """A smaller difference, recorded because it is easy to 'tidy' into one and lose a case."""
        with open(os.path.join(TOOLS, "matrixark_mcp_core.py"), encoding="utf-8",
                  errors="replace") as handle:
            core_source = handle.read()
        with open(os.path.join(TOOLS, "matrixark_mcp_context_pack.py"), encoding="utf-8",
                  errors="replace") as handle:
            pack_source = handle.read()
        self.assertIn("clip_context_text(text, max_chars=preview_chars)", core_source)
        self.assertIn("_clip_context_text(text, max_chars=preview_chars)", pack_source)


if __name__ == "__main__":
    unittest.main()
