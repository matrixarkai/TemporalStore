#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two patterns in `profile_entity_type_for_memory_text` can match one sentence, and ORDER decides.

`profile_entity_type_for_memory_text` returns the first matching kind. Two of its patterns overlap
on ordinary text: one matches feature and functionality words, the other matches communication
preferences -- reply, respond, answer style, preferred language, locale, timezone. A sentence like

    "always reply in English and focus on features only"

matches BOTH. The live order asks about features first, so it is a `memory_feature_profile`.

THE UNREACHABLE COPY ASKS IN THE OPPOSITE ORDER. `matrixark_mcp_extraction_normalization` holds the
same function with those two clauses swapped, and over sentences both patterns match the two
disagree on **9 of 10**. The diverged-copy ratchet has recorded that pair without anything saying
which order is intended, and no comment in either file mentions the ordering at all.

THIS FILE CHANGES NO BEHAVIOUR. Which order is right is a question about what the product means by a
profile, not something a test should decide -- and the answer is load-bearing, because
`feature_scope_excludes_outcome_evidence` gates on `== "memory_feature_profile"`, so the precedence
decides whether outcome evidence is EXCLUDED for that text. Flipping it would change what is stored
and what is retrieved for every user.

What this does is make the precedence explicit, so it is a decision rather than an accident: the
overlap is real, the order resolves it, and changing the order fails here with the reason attached.
Asserted in both directions -- if the overlap ever stops existing, the guard says so too, because a
precedence rule for patterns that no longer overlap is describing a tree that has moved on.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_core  # noqa: F401  - enter the import cycle from the core side
    from tools.matrixark_mcp_core_codex_outcome import (
        feature_scope_excludes_outcome_evidence,
        profile_entity_type_for_memory_text,
    )
except ImportError:  # run from tools/
    import matrixark_mcp_core  # noqa: F401
    from matrixark_mcp_core_codex_outcome import (
        feature_scope_excludes_outcome_evidence,
        profile_entity_type_for_memory_text,
    )

#: Sentences carrying BOTH a communication preference and a feature word. Every one of these is
#: ambiguous by construction: that is what makes the order matter.
BOTH_PATTERNS_MATCH = (
    "always reply in English and focus on features only",
    "prefer concise answers about functionality",
    "communication style: terse; features only",
    "answer style should describe functionality",
    "preferred format is bullets, focused on features",
    "write replies that focus on features",
    "my timezone is UTC and I want features only",
    "locale is en_GB; functionality first",
    "response style: describe the functionality",
)

#: Communication preferences with no feature word -- the control. If these ever come back
#: `memory_feature_profile`, the feature pattern has widened rather than the order having changed,
#: which is a different fault with a different fix.
COMMUNICATION_ONLY = (
    "always reply in English",
    "my timezone is UTC",
    "preferred format is bullets",
    "locale is en_GB",
)


class TheProfileClassifierOrderIsAPrecedenceRule(unittest.TestCase):

    def test_the_two_patterns_really_do_overlap(self):
        """The floor. Without an overlap there is no precedence to pin and this file says nothing."""
        overlapping = [t for t in BOTH_PATTERNS_MATCH
                       if profile_entity_type_for_memory_text(t) in
                       ("memory_feature_profile", "communication_profile")]
        self.assertEqual(
            len(BOTH_PATTERNS_MATCH), len(overlapping),
            "%d of %d sentences no longer reach either kind, so they are not evidence of an "
            "overlap and the assertions below are about nothing"
            % (len(overlapping), len(BOTH_PATTERNS_MATCH)))

    def test_features_win_when_both_match(self):
        """The precedence as it stands, recorded so changing it is deliberate.

        The unreachable copy in matrixark_mcp_extraction_normalization asks in the opposite order
        and answers `communication_profile` for all of these. Neither file says which is intended.
        """
        wrong = [t for t in BOTH_PATTERNS_MATCH
                 if profile_entity_type_for_memory_text(t) != "memory_feature_profile"]
        self.assertEqual(
            [], wrong,
            "the feature clause no longer precedes the communication clause, so %d ambiguous "
            "sentence(s) now classify as something else: %s. That is a change to what gets stored "
            "and what `feature_scope_excludes_outcome_evidence` excludes -- if it is intended, say "
            "so here and update this file." % (len(wrong), wrong[:3]))

    def test_a_communication_preference_alone_is_not_a_feature(self):
        """The control, and the other direction.

        If these started answering `memory_feature_profile` the feature pattern would have widened
        to swallow plain communication text -- which the test above could not tell apart from an
        order change.
        """
        misread = [t for t in COMMUNICATION_ONLY
                   if profile_entity_type_for_memory_text(t) == "memory_feature_profile"]
        self.assertEqual(
            [], misread,
            "%d communication preference(s) with no feature word now classify as "
            "memory_feature_profile: %s. The feature pattern has widened, which is a different "
            "fault from the ordering." % (len(misread), misread))

    def test_the_precedence_decides_whether_evidence_is_excluded(self):
        """Why the order is load-bearing rather than cosmetic.

        `feature_scope_excludes_outcome_evidence` returns False immediately for anything that is
        not `memory_feature_profile`, so the classification is the gate on it.
        """
        classified = [t for t in BOTH_PATTERNS_MATCH
                      if profile_entity_type_for_memory_text(t) == "memory_feature_profile"]
        self.assertTrue(
            classified,
            "no ambiguous sentence classifies as memory_feature_profile, so the gate below is "
            "unreachable from this set and proves nothing")
        reached = [t for t in classified if feature_scope_excludes_outcome_evidence(t) is not False]
        self.assertTrue(
            reached,
            "feature_scope_excludes_outcome_evidence answers False for every ambiguous sentence "
            "even though all of them classify as memory_feature_profile, so the classification is "
            "no longer what gates it and this file's reason for existing has moved")


if __name__ == "__main__":
    unittest.main()
