#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`compact_dropped_refs_for_context_pack` is defined twice, and one copy explains less.

The function builds the "why were refs dropped" summary carried in a served pack. Both copies are
live:

    matrixark_local_adapter_retrieve  -> matrixark_mcp_core's copy
    matrixark_mcp_context_pack        -> its own

The bodies differ in exactly one line: the list of policy fields the summary keeps.

    matrixark_mcp_core          cross_session_policy, shared_context_policy,
                                source_role_budget_policy, memory_layer_budget_policy,
                                memory_selection_policy_budget_policy,
                                extraction_phase_budget_policy        (six)
    matrixark_mcp_context_pack  the first four                        (four)

A field absent from that loop is never added to the result, so the two missing policies do not
arrive unchanged -- they disappear. MEASURED on one dropped-refs record carrying all of them:

    core         -> ['below_min_score', 'cross_session_policy',
                     'extraction_phase_budget_policy', 'memory_selection_policy_budget_policy']
    context_pack -> ['below_min_score', 'cross_session_policy']

This is a strict subset, so it is the shape of a copy that stopped keeping up rather than a
deliberate difference: the two missing entries are the two newest budget policies.

THIS FILE DOES NOT ASSERT THAT THEY AGREE, because they do not. It RECORDS the split in both
directions, so a new divergence fails here and a resolved one fails here too. Adding the two
fields changes what a served pack reports, which is a payload decision rather than a cleanup.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

FUNCTION = "compact_dropped_refs_for_context_pack"

RECORDED = {
    "matrixark_mcp_core": ["cross_session_policy", "shared_context_policy",
                           "source_role_budget_policy", "memory_layer_budget_policy",
                           "memory_selection_policy_budget_policy",
                           "extraction_phase_budget_policy"],
    "matrixark_mcp_context_pack": ["cross_session_policy", "shared_context_policy",
                                   "source_role_budget_policy", "memory_layer_budget_policy"],
}
MISSING = {"memory_selection_policy_budget_policy", "extraction_phase_budget_policy"}


def _policy_fields(stem):
    """The policy-field list that copy compacts, read out of the syntax."""
    with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name != FUNCTION:
            continue
        for sub in ast.walk(node):
            if not isinstance(sub, ast.For) or not isinstance(sub.iter, (ast.List, ast.Tuple)):
                continue
            # Selected by what the BODY calls, not by how the names look. An earlier loop in this
            # same function iterates ['deadline_exceeded', ..., 'budget_fill_policy'], whose last
            # entry also ends in "_policy" -- a name-shaped rule picked that one and this guard
            # failed on its first run because of it.
            calls_policy_compactor = any(
                isinstance(call, ast.Call)
                and getattr(call.func, "id", getattr(call.func, "attr", ""))
                == "compact_context_pack_policy"
                for call in ast.walk(sub))
            if not calls_policy_compactor:
                continue
            return [e.value for e in sub.iter.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)]
    return []


class TheDroppedRefSummaryHasOneDefinition(unittest.TestCase):

    def test_both_copies_still_compact_policies(self) -> None:
        """A floor. An empty list would make every comparison below vacuous."""
        for stem in sorted(RECORDED):
            with self.subTest(module=stem):
                self.assertTrue(
                    _policy_fields(stem),
                    "%s.%s no longer carries a policy-field list" % (stem, FUNCTION))

    def test_the_policy_lists_are_exactly_what_is_recorded(self) -> None:
        """Recorded, both directions."""
        for stem, expected in sorted(RECORDED.items()):
            with self.subTest(module=stem):
                self.assertEqual(
                    expected, _policy_fields(stem),
                    "%s.%s changed which policies it keeps" % (stem, FUNCTION))

    def test_one_copy_explains_two_fewer_reasons(self) -> None:
        """The consequence, stated as the thing that is actually wrong."""
        core = set(_policy_fields("matrixark_mcp_core"))
        pack = set(_policy_fields("matrixark_mcp_context_pack"))
        self.assertEqual(
            MISSING, core - pack,
            "the policies matrixark_mcp_context_pack omits changed; recorded as %s"
            % sorted(MISSING))
        self.assertEqual(
            set(), pack - core,
            "matrixark_mcp_context_pack now keeps a policy core does not, so this is no longer a "
            "strict subset and the explanation above needs restating")

    def test_a_dropped_record_loses_those_two_through_one_copy(self) -> None:
        """Measured, not read: the missing fields disappear rather than passing through."""
        import matrixark_mcp_context_pack as pack_module
        import matrixark_mcp_core as core_module
        dropped = {"below_min_score": 3,
                   "cross_session_policy": {"mode": "prefer"},
                   "memory_selection_policy_budget_policy": {"mode": "auto"},
                   "extraction_phase_budget_policy": {"mode": "auto"}}
        core_out = core_module.compact_dropped_refs_for_context_pack(dict(dropped))
        pack_out = pack_module.compact_dropped_refs_for_context_pack(dict(dropped))
        for field in sorted(MISSING):
            with self.subTest(field=field):
                self.assertIn(field, core_out)
                self.assertNotIn(
                    field, pack_out,
                    "%s now survives matrixark_mcp_context_pack's copy -- the split is resolved"
                    % field)


if __name__ == "__main__":
    unittest.main()
