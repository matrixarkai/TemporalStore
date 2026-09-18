#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`cross_session_rerank_adjustment` has two live definitions, and three live callers split.

One ranking rule, two implementations, and which one decides a candidate's boost depends on which
module the caller reached:

    matrixark_mcp_core_candidate_policy   ->  matrixark_mcp_core          (profile-memory aware)
    matrixark_mcp_core_packing            ->  matrixark_mcp_core          (profile-memory aware)
    matrixark_mcp_recall_scoring          ->  matrixark_mcp_access_scope  (NOT profile-memory aware)

Resolved by importing each caller and comparing the bound function's code object, not by reading
the import lines: a `try`/`except` import chain can say one thing and deliver another
(`a-tools-prefixed-import-is-a-different-module-object` is the same trap one level down).

MEASURED on one candidate -- `session_continuity=cross_session`, `ref_type=entity`,
`memory_scope=user_profile`, `profile_memory_kind=durable_profile`:

    question_type      access_scope   core
    profile_memory         0.06       0.16
    current_state          0.10       0.14

The `core` copy carries a whole `question_type == "profile_memory"` branch the other lacks, and a
`profile_boost` of 0.04 for `memory_scope == "user_profile"`. That second one is why the difference
is **not** confined to profile-memory questions: any user-profile candidate scores 0.04 lower
through `recall_scoring` than through the packing path, on an ordinary `current_state` question.

THIS FILE DOES NOT ASSERT THAT THEY AGREE. Which boost is right is a ranking decision -- adopting
the richer rule raises user-profile candidates everywhere `recall_scoring` runs, and adopting the
poorer one drops them everywhere else. Picking either changes what retrieval returns. So the state
is recorded in BOTH directions: a new divergence fails here, and a convergence fails here too and
asks which definition won.

Its siblings all have this guard already -- `test_the_storage_option_normalizers_have_one_definition`,
`test_the_record_materializer_has_one_definition` -- and this pair had none, which is the only
reason it is being written now rather than the divergence being new.

The decision itself, and the two things that would make it cheap to settle, is matrixarkai#1871.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_access_scope as access_scope  # noqa: E402
import matrixark_mcp_core as core  # noqa: E402

HELPER = "cross_session_rerank_adjustment"

#: Caller -> the module whose copy it binds today.
BINDINGS = {
    "matrixark_mcp_core_candidate_policy": "matrixark_mcp_core",
    "matrixark_mcp_core_packing": "matrixark_mcp_core",
    "matrixark_mcp_recall_scoring": "matrixark_mcp_access_scope",
}

#: One candidate, and what each copy scores it. A user-profile entity, cross-session, which is the
#: shape the richer branch exists for.
CANDIDATE = {
    "session_continuity": "cross_session",
    "ref_type": "entity",
    "memory_scope": "user_profile",
    "profile_memory_kind": "durable_profile",
}
SCORES = {
    ("matrixark_mcp_access_scope", "profile_memory"): 0.06,
    ("matrixark_mcp_core", "profile_memory"): 0.16,
    ("matrixark_mcp_access_scope", "current_state"): 0.10,
    ("matrixark_mcp_core", "current_state"): 0.14,
}

COPIES = {
    "matrixark_mcp_access_scope": getattr(access_scope, HELPER),
    "matrixark_mcp_core": getattr(core, HELPER),
}


class TheRerankRuleHasTwoDefinitions(unittest.TestCase):

    def test_the_two_copies_are_distinct(self) -> None:
        """The floor. If they became one function every comparison below is vacuous."""
        codes = {name: fn.__code__ for name, fn in COPIES.items()}
        self.assertEqual(
            2, len(set(id(code) for code in codes.values())),
            "%s is now ONE function. If the copies were consolidated that is the fix -- strike "
            "this file and say which definition won" % HELPER,
        )

    def test_each_caller_still_binds_the_copy_recorded_here(self) -> None:
        """Resolved by code object, because an import chain can deliver what its text does not say."""
        for caller, expected in sorted(BINDINGS.items()):
            with self.subTest(caller=caller):
                module = __import__(caller)
                bound = getattr(module, HELPER, None)
                self.assertIsNotNone(
                    bound, "%s no longer holds %s at module scope" % (caller, HELPER))
                actual = [
                    name for name, fn in COPIES.items()
                    if fn.__code__ is bound.__code__
                ]
                self.assertEqual(
                    [expected], actual,
                    "%s now binds %s, recorded as %s -- which copy a caller reaches decides the "
                    "boost, so this is a ranking change however it happened"
                    % (caller, actual or "a THIRD copy", expected),
                )

    def test_the_two_copies_still_score_the_same_candidate_differently(self) -> None:
        """The finding, at the numbers rather than at the source text."""
        for (module_name, question_type), expected in sorted(SCORES.items()):
            with self.subTest(copy=module_name, question_type=question_type):
                self.assertAlmostEqual(
                    expected,
                    COPIES[module_name](dict(CANDIDATE), question_type),
                    places=6,
                    msg="%s scores this candidate differently than recorded for %s. If the two "
                        "copies were reconciled, strike this file and say which won"
                        % (module_name, question_type),
                )

    def test_the_difference_is_not_confined_to_a_profile_memory_question(self) -> None:
        """The part that is easy to miss: the 0.04 user-profile boost applies everywhere.

        Reading only the `question_type == "profile_memory"` branch suggests the copies agree on
        every other question. They do not -- the richer copy adds `profile_boost` to the entity
        branch, so an ordinary `current_state` question already differs.
        """
        poorer = COPIES["matrixark_mcp_access_scope"](dict(CANDIDATE), "current_state")
        richer = COPIES["matrixark_mcp_core"](dict(CANDIDATE), "current_state")
        self.assertNotEqual(
            poorer, richer,
            "the copies now agree on an ordinary question, so the divergence this file records has "
            "narrowed to the profile-memory branch -- say so rather than leaving this assertion",
        )
        self.assertAlmostEqual(0.04, richer - poorer, places=6)


if __name__ == "__main__":
    unittest.main()
