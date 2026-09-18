#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`compact_recall_policy_for_audit` has two live definitions, and each is poorer than the other.

Both are called: `matrixark_mcp_core` calls its own, `matrixark_mcp_context_pack` calls its own,
and the only external caller binds core's. Measured by running both on one policy rather than
reading the source:

    include_debug=False   core LEAKS a debug_note into a non-debug audit record, through
                          `async_pipeline_readiness`; context_pack strips it
    include_debug=True    context_pack DROPS the debug content of memory_layer_budget,
                          dropped_memory_layer_budget and memory_layer_pressure; core keeps it

So neither copy is the richer one. `include_debug` is a flag about what an audit record may carry,
and one copy ignores it in each direction: core carries debug content when told not to, and
context_pack withholds it when told to.

The first direction is the one worth reading twice. `include_debug=False` is the ordinary path --
an audit record written for everyday inspection -- and core puts a `debug_note` in it anyway.

THE FIXTURE HAS TO REACH THE BRANCHES, and this is the trap the finding was recorded with. A probe
that carries no `async_pipeline_readiness` and never passes `include_debug=True` reports the two
copies as IDENTICAL, because the input cannot separate them. That happened twice -- once when the
finding was first investigated, and again when I re-checked it before writing this file. The first
test below asserts the fixture reaches both branches before anything compares outputs.

THIS FILE DOES NOT ASSERT THAT THEY AGREE. Which copy is right is a decision about audit content:
one direction adds a field to records already being written, the other removes one a reader may be
using. Recorded in both directions, so a new divergence fails here and a convergence fails here too
and asks which won. The decision is matrixarkai#1873.

An earlier attempt to fix it by forwarding the flag in the three calls that drop it was abandoned on
evidence: in that module the flag is forwarded by some callers and deliberately not by others, so
"a parameter accepted and ignored" is not automatically an omission.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_context_pack as context_pack  # noqa: E402
import matrixark_mcp_core as core  # noqa: E402

HELPER = "compact_recall_policy_for_audit"

COPIES = {
    "matrixark_mcp_context_pack": getattr(context_pack, HELPER),
    "matrixark_mcp_core": getattr(core, HELPER),
}

#: Carries something under every section either copy treats differently. Kept as one fixture so
#: the floor below and the comparisons run on the same input.
POLICY = {
    "query_plan": {"query_type": "fact", "temporal_window": "7d"},
    "tree_traversal": {"enabled": True, "selected_node_count": 3},
    "secondary_index_filter": {"enabled": True, "matched_candidate_count": 30},
    "rerank": {"applied": True},
    "hard_deadline": {"enabled": True, "budget_ms": 800},
    "session_continuity": {"mode": "prefer", "same_session_count": 4},
    "storage_options": {"durability": "sync"},
    "memory_layer_budget": {"session": 5, "profile": 2, "debug_note": "budget debug"},
    "dropped_memory_layer_budget": {"profile": 1, "debug_note": "dropped debug"},
    "memory_layer_pressure": {"profile": 0.4, "debug_note": "pressure debug"},
    "async_pipeline_readiness": {"ready": True, "debug_note": "async debug", "queue_depth": 3},
    "memory_selection_policy_budget_policy": {"cap": 12, "debug_note": "policy debug"},
}

#: The sections whose debug content context_pack drops when told to include it.
DROPPED_WITH_DEBUG = ("memory_layer_budget", "dropped_memory_layer_budget", "memory_layer_pressure")


def _shaped(module_name: str, include_debug: bool) -> dict:
    return COPIES[module_name](dict(POLICY), include_debug=include_debug)


class TheTwoAuditShapersDisagreeBothWays(unittest.TestCase):

    def test_the_two_copies_are_distinct(self) -> None:
        """The floor. Two names for one function would pass every comparison below."""
        codes = {id(fn.__code__) for fn in COPIES.values()}
        self.assertEqual(
            2, len(codes),
            "%s is now ONE function. If the copies were consolidated that is the fix -- strike "
            "this file and say which definition won" % HELPER,
        )

    def test_the_fixture_reaches_both_branches(self) -> None:
        """The other floor, and the reason this file exists in this shape.

        A probe without `async_pipeline_readiness`, or one that never passes `include_debug=True`,
        reports the two copies as identical. That has happened twice. Assert the input qualifies
        before any assertion rests on it.
        """
        self.assertIn(
            "async_pipeline_readiness", POLICY,
            "without this section the include_debug=False direction cannot show",
        )
        for section in DROPPED_WITH_DEBUG:
            with self.subTest(section=section):
                self.assertIn(
                    "debug_note", POLICY[section],
                    "%s carries no debug content, so the include_debug=True direction cannot show"
                    % section,
                )
        self.assertNotEqual(
            _shaped("matrixark_mcp_context_pack", False),
            _shaped("matrixark_mcp_core", False),
            "the fixture no longer separates the copies at include_debug=False",
        )

    def test_core_carries_debug_content_into_a_non_debug_record(self) -> None:
        """`include_debug=False` is the ordinary path, and core puts a debug_note in it anyway."""
        pack_out = _shaped("matrixark_mcp_context_pack", False)
        core_out = _shaped("matrixark_mcp_core", False)
        readiness_pack = pack_out.get("async_pipeline_readiness") or {}
        readiness_core = core_out.get("async_pipeline_readiness") or {}
        self.assertNotIn(
            "debug_note", readiness_pack,
            "context_pack has started carrying debug content into a non-debug audit record too, "
            "so the divergence recorded here has changed shape",
        )
        self.assertIn(
            "debug_note", readiness_core,
            "core no longer leaks debug content into a non-debug audit record. If that is the fix, "
            "strike this assertion and say so in matrixarkai#1873",
        )

    def test_context_pack_withholds_debug_content_when_asked_for_it(self) -> None:
        """And the other direction: `include_debug=True`, three sections, content dropped."""
        pack_out = _shaped("matrixark_mcp_context_pack", True)
        core_out = _shaped("matrixark_mcp_core", True)
        for section in DROPPED_WITH_DEBUG:
            with self.subTest(section=section):
                self.assertNotIn(
                    "debug_note", pack_out.get(section) or {},
                    "context_pack now forwards include_debug to %s. If that is the fix, strike "
                    "this assertion and say so in matrixarkai#1873" % section,
                )
                self.assertIn(
                    "debug_note", core_out.get(section) or {},
                    "core has stopped keeping debug content in %s, so this file no longer "
                    "describes which copy is poorer where" % section,
                )


if __name__ == "__main__":
    unittest.main()
