#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`serving_ref_for_pack` is defined twice, and the two copies disagree about debug lineage.

The function decides which fields of a candidate reach the serving ContextPack payload. It exists
in two production modules, each calling only its own copy:

    matrixark_mcp_core_packing    18 statements, kwonly (default_session_continuity,
                                                          default_memory_layer)
    matrixark_mcp_context_pack    20 statements, kwonly (default_session_continuity,
                                                          default_memory_layer, include_debug)

They are NOT interchangeable, and the difference is on the payload:

  * `source` precedence. core_packing resolves
        citation or source_ref or source_locator or metadata.source_locator
    so `source_ref` is SECOND and unconditional. context_pack resolves
        citation or source_locator or metadata.source_locator
    and only then admits `source_ref`, and only when `_context_memory_source_ref_is_debug_only`
    says it is not debug-only. So a pack built through core_packing can expose as `source` a value
    the other copy deliberately withholds as debug lineage.

  * `include_debug`. Only context_pack takes it, and only context_pack gates a `source_ref` field
    on `debug_lineage_enabled(include_debug=...)`. core_packing has no way to be told.

  * The profile fields. core_packing handles profile_memory_kind/class with `.strip()` and coerces
    profile_entity_current/profile_summary_current to literal True; context_pack folds all four
    into the alias table and passes the raw value through.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE, because they do not, and a guard that fails the
day it is written tells nobody anything. It RECORDS the split in both directions, so a new
divergence fails here and a resolved one fails here too.

IT IS DELIBERATELY READ FROM THE SOURCE, not by importing the modules. `matrixark_mcp_core_packing`
cannot be imported in any form -- bare or package-qualified -- because `matrixark_mcp_core` and
`matrixark_mcp_core_ref_selection` import each other, which is a separately recorded open item.
Asserting through the syntax is what is available, and it is said here rather than left for the
next person to rediscover by trying.

The pair was missed by an earlier sweep that grouped functions by name and POSITIONAL parameters.
These two match on positional parameters and differ on keyword-only ones, so that sweep called
them the same signature. `test_the_two_copies_are_not_interchangeable` is the correction.
"""
from __future__ import annotations

import ast
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

FUNCTION = "serving_ref_for_pack"

#: module -> (statement count, keyword-only parameter names)
RECORDED = {
    "matrixark_mcp_core_packing": (18, ("default_session_continuity", "default_memory_layer")),
    "matrixark_mcp_context_pack": (20, ("default_session_continuity", "default_memory_layer",
                                        "include_debug")),
}

#: The marker that makes context_pack's copy the debug-aware one.
DEBUG_MARKERS = ("_context_memory_source_ref_is_debug_only", "debug_lineage_enabled")


def _definition(stem):
    path = os.path.join(TOOLS, stem + ".py")
    with open(path, encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == FUNCTION:
            return node
    return None


def _definitions():
    return {stem: _definition(stem) for stem in RECORDED}


class TheServingPackShaperHasOneDefinition(unittest.TestCase):

    def test_both_definitions_are_still_there(self) -> None:
        """A floor. Every assertion below passes vacuously over a missing definition."""
        for stem, node in sorted(_definitions().items()):
            with self.subTest(module=stem):
                self.assertIsNotNone(
                    node,
                    "%s no longer defines %s. If it now imports the other copy, the split is "
                    "resolved -- strike this file and say which behaviour won." % (stem, FUNCTION))

    def test_the_two_copies_are_not_interchangeable(self) -> None:
        """Recorded, both directions, including the keyword-only parameters.

        An earlier sweep grouped by name and POSITIONAL parameters and therefore called these two
        the same signature. They are not: only one takes `include_debug`.
        """
        for stem, (statements, kwonly) in sorted(RECORDED.items()):
            node = _definition(stem)
            with self.subTest(module=stem):
                self.assertIsNotNone(node, "%s no longer defines %s" % (stem, FUNCTION))
                self.assertEqual(
                    list(kwonly), [a.arg for a in node.args.kwonlyargs],
                    "%s.%s changed its keyword-only parameters" % (stem, FUNCTION))
                self.assertEqual(
                    statements, len(node.body),
                    "%s.%s is now %d statements, recorded as %d. If the copies were reconciled, "
                    "strike this file." % (stem, FUNCTION, len(node.body), statements))

        packing, context = (_definition("matrixark_mcp_core_packing"),
                            _definition("matrixark_mcp_context_pack"))
        self.assertNotEqual(
            ast.dump(ast.Module(body=packing.body, type_ignores=[])),
            ast.dump(ast.Module(body=context.body, type_ignores=[])),
            "the two bodies are now identical, so the split is resolved and this file should go")

    def test_only_one_copy_knows_about_debug_lineage(self) -> None:
        """The consequence, stated as the thing that is actually wrong.

        The split matters because of WHAT REACHES THE PAYLOAD. If core_packing ever learns about
        debug lineage, that is the fix -- strike this file with it.
        """
        packing = ast.unparse(_definition("matrixark_mcp_core_packing"))
        context = ast.unparse(_definition("matrixark_mcp_context_pack"))
        for marker in DEBUG_MARKERS:
            with self.subTest(marker=marker):
                self.assertIn(
                    marker, context,
                    "matrixark_mcp_context_pack.%s no longer consults %s" % (FUNCTION, marker))
                self.assertNotIn(
                    marker, packing,
                    "matrixark_mcp_core_packing.%s now consults %s, so both copies understand "
                    "debug lineage and the split is resolved" % (FUNCTION, marker))

    def test_the_source_precedence_still_differs(self) -> None:
        """core_packing puts source_ref SECOND and unconditional; context_pack does not."""
        packing = ast.unparse(_definition("matrixark_mcp_core_packing"))
        context = ast.unparse(_definition("matrixark_mcp_context_pack"))
        self.assertIn(
            "ref.get('citation') or ref.get('source_ref')", packing,
            "matrixark_mcp_core_packing no longer takes source_ref second in the source chain; if "
            "it now matches context_pack, the payloads agree and this file should go")
        self.assertNotIn(
            "ref.get('citation') or ref.get('source_ref')", context,
            "matrixark_mcp_context_pack now takes source_ref second too, so both copies expose "
            "the same source and the split is resolved")

    def test_each_module_calls_only_its_own_copy(self) -> None:
        """Why this is a payload difference and not a crash.

        Neither module imports the other's copy, so a caller never passes `include_debug` to the
        one that cannot take it. If that ever changes, the mismatched signatures become a
        TypeError rather than a quiet difference.
        """
        for stem in sorted(RECORDED):
            other = [s for s in RECORDED if s != stem][0]
            with self.subTest(module=stem):
                with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8",
                          errors="replace") as handle:
                    body = handle.read()
                for line in body.splitlines():
                    if FUNCTION in line and "import" in line and other in line:
                        self.fail(
                            "%s now imports %s from %s. Their keyword-only parameters differ, so "
                            "a call passing include_debug would raise TypeError."
                            % (stem, FUNCTION, other))


if __name__ == "__main__":
    unittest.main()
