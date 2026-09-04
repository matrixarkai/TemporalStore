# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`candidate_memory_layer_name` answers `resource_chunk` or `skill_section` for 96% of its calls,
and used to compute a dozen lookups and three set comprehensions before getting there. Deciding
those four answers first is pure code motion -- none of them reads anything from the block they now
precede -- but "pure" is a claim, so these tests pin it.

The risk in moving a return EARLIER is that it shadows a later branch. That can only happen for a
candidate matching both, so the tests below build exactly those candidates: a chunk that also
carries every field the compression, summary, segment, event and entity branches key on. If the
move changed anything, one of them answers with a later branch's name.
"""
import unittest

# Importing matrixark_mcp_core_packing directly trips a cycle through matrixark_mcp_core, so go in
# through core, which is the order the rest of the suite uses. Package path first, bare as the
# fallback, because the suite is run both ways.
try:  # package path
    from tools.matrixark_mcp_core import candidate_memory_layer_name
except ImportError:
    from matrixark_mcp_core import candidate_memory_layer_name

# Every field the block below the moved returns reads, set to values that would drive a later
# branch. A candidate carrying all of these must STILL answer from the early return.
LATER_BRANCH_FIELDS = {
    "memory_scope": "user_profile",
    "session_continuity": "cross_session",
    "profile_memory_class": "memory_feature",
    "profile_memory_kind": "codex_outcome",
    "source_profile_memory_classes": ["memory_feature"],
    "source_profile_memory_kinds": ["codex_outcome"],
    "source_memory_layers": ["cross_session_memory_feature_compression"],
    "event_type": "assistant_response",
}


class LayerNameDecidesEarlyTest(unittest.TestCase):

    def test_this_is_the_copy_the_retrieve_path_calls(self):
        # The name exists on several modules and the suite loads some of them twice, bare and
        # package-prefixed. Reaching a dead copy would make every test below pass while pinning
        # nothing, so assert the function under test is the one DEFINED in the packing module.
        source = getattr(candidate_memory_layer_name, "__module__", "")
        self.assertTrue(source.endswith("matrixark_mcp_core_packing"),
                        "testing a copy defined in %r, not the packing module" % source)

    def test_the_four_early_answers(self):
        self.assertEqual(candidate_memory_layer_name({"ref_type": "resource_chunk"}),
                         "resource_chunk")
        self.assertEqual(candidate_memory_layer_name({"ref_type": "skill_section"}),
                         "skill_section")
        self.assertEqual(candidate_memory_layer_name({"context_class": "resource_fact"}),
                         "resource_fact")
        self.assertEqual(candidate_memory_layer_name({"context_class": "resource_entity_fact"}),
                         "resource_entity_fact")

    def test_a_later_branch_cannot_be_shadowed_by_the_move(self):
        # The shadowing case: a candidate that satisfies an early return AND carries everything a
        # later branch keys on. The early answer must win, exactly as it did when the returns sat
        # below the block -- they were always ahead of these branches in source order.
        for ref_type, expected in (("resource_chunk", "resource_chunk"),
                                   ("skill_section", "skill_section")):
            candidate = dict(LATER_BRANCH_FIELDS, ref_type=ref_type)
            self.assertEqual(candidate_memory_layer_name(candidate), expected,
                             "a later branch shadowed the early answer for %s" % ref_type)

    def test_the_later_branches_still_answer(self):
        # Positive control: if the move had swallowed the rest of the function, these would fall
        # through to the early returns or to the "unknown" tail instead of naming their branch.
        cases = [
            ({"ref_type": "compression", "session_continuity": "same_session"},
             "same_session_compression"),
            ({"ref_type": "summary", "session_continuity": "cross_session"},
             "cross_session_summary"),
            ({"ref_type": "segment", "session_continuity": "same_session"},
             "same_session_segment"),
            ({"ref_type": "event", "session_continuity": "cross_session"},
             "cross_session_event"),
            ({"ref_type": "entity", "memory_scope": "user_profile"}, "profile_entity"),
        ]
        for candidate, expected in cases:
            self.assertEqual(candidate_memory_layer_name(candidate), expected, repr(candidate))

    def test_an_explicit_layer_still_wins_over_everything(self):
        # The explicit-layer return sits ABOVE the moved block and must keep winning, including for
        # a candidate whose ref_type would otherwise take one of the early returns.
        self.assertEqual(
            candidate_memory_layer_name({"memory_layer": "chosen", "ref_type": "resource_chunk"}),
            "chosen")

    def test_metadata_is_consulted_for_the_early_answers(self):
        # ref_type and context_class are read from the record OR its metadata; the move must not
        # have dropped the metadata half of that.
        self.assertEqual(
            candidate_memory_layer_name({"metadata": {"ref_type": "skill_section"}}),
            "skill_section")
        self.assertEqual(
            candidate_memory_layer_name({"metadata": {"context_class": "resource_fact"}}),
            "resource_fact")

    def test_the_derived_ref_types_still_derive(self):
        # ref_type is derived from record_type when absent, above the moved returns, so the later
        # branches keyed on it must still be reachable through that derivation.
        self.assertEqual(
            candidate_memory_layer_name({"record_type": "context_event",
                                         "session_continuity": "same_session"}),
            "same_session_event")
        self.assertEqual(
            candidate_memory_layer_name({"record_type": "context_summary",
                                         "session_continuity": "cross_session"}),
            "cross_session_summary")


if __name__ == "__main__":
    unittest.main()
