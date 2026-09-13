#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The scoring rules are defined twice, and the two copies disagree about profile memory.

`matrixark_mcp_recall_scoring` computes

    final_score = final_recall_score(origin, time, business, weights)
                  + session_continuity_boost(...)
                  + cross_session_rerank_adjustment(...)

and `question_type_ref_boost` feeds the origin score. Every one of those three has a second
definition, and the copy this module reaches is the one WITHOUT profile-memory handling:

    matrixark_local_adapter_retrieve    -> matrixark_mcp_core's copies          (profile memory)
    matrixark_local_adapter_retrieval   -> matrixark_mcp_core's copies          (profile memory)
    matrixark_mcp_recall_scoring        -> matrixark_mcp_access_scope's copies  (none)

Measured on the two `cross_session_rerank_adjustment` bodies: core's opens with a
`question_type == "profile_memory"` block returning 0.16 for an entity, 0.12 for a summary or
compression and 0.06 for a segment, adds +0.04 to an entity whose `memory_scope` is `user_profile`,
and counts `profile_memory` alongside `broad_exploration` in the summary branch. The access_scope
copy has none of it, so the same candidate scores 0.0 there and up to 0.16 through the adapters.

THIS FILE DOES NOT ASSERT THAT THEY AGREE, because they do not, and a guard that fails on the day
it is written tells nobody anything. It RECORDS the divergence exactly, so a new one fails here and
a resolved one fails here too -- the same shape as the orphan and cross-import records. Choosing
which copy wins changes ranking on a serving path, which is a decision rather than a cleanup.

`score_recall_candidate` IS in the list, with its own reason. It diverges as well (12 statements
against 11) and carries no profile-memory markers on either side, so it is a different drift --
but the set is asserted exactly, and leaving it out on the grounds that it is off-topic would mean
the list is not the whole truth about the family. It is recorded as what it is, and nothing here
suggests fixing it alongside the other four.
"""
from __future__ import annotations

import ast
import os
import hashlib
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: (function, the two modules that define it) -> what the copies disagree about.
RECORDED = {
    ("cross_session_rerank_adjustment",
     ("matrixark_mcp_access_scope", "matrixark_mcp_core")):
        "core has the whole question_type == profile_memory block and the user_profile entity "
        "boost; access_scope has neither, and matrixark_mcp_recall_scoring reaches access_scope",
    ("session_continuity_boost",
     ("matrixark_mcp_access_scope", "matrixark_mcp_core")):
        "same pair, same direction: core is one statement longer and that statement is the "
        "profile-memory case",
    ("access_scope_matches_before_scoring",
     ("matrixark_mcp_access_scope", "matrixark_mcp_core")):
        "core admits a user_profile + cross_session record when the identity fields match; "
        "access_scope falls through to scope_matches",
    ("candidate_access_scope",
     ("matrixark_mcp_access_scope", "matrixark_mcp_core")):
        "core reads two more fields off the record when building the scope",
    ("question_type_ref_boost",
     ("matrixark_mcp_core_candidate_policy", "matrixark_mcp_recall_scoring")):
        "core_candidate_policy names profile_memory 15 times and codex_outcome 11; "
        "recall_scoring names them 5 and 1, and has no is_feature_profile_memory at all",
    ("score_recall_candidate",
     ("matrixark_mcp_core_candidate_policy", "matrixark_mcp_recall_scoring")):
        "a DIFFERENT drift, recorded so this list is the whole truth about the family: 12 "
        "statements against 11 and NO profile-memory markers on either side. Resolving it is not "
        "part of the profile-memory question above",
}

FAMILY = ("matrixark_mcp_access_scope", "matrixark_mcp_core", "matrixark_mcp_recall_scoring",
          "matrixark_mcp_core_candidate_policy")


def _bodies():
    """(function, module) -> (digest of the body, statement count) across the scoring family."""
    out = {}
    for stem in FAMILY:
        path = os.path.join(TOOLS, stem + ".py")
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):  # pragma: no cover - an unparseable module fails elsewhere
            continue
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            dumped = ast.dump(ast.Module(body=node.body, type_ignores=[]))
            out[(node.name, stem)] = (
                hashlib.sha256(dumped.encode("utf-8")).hexdigest()[:12],
                tuple(argument.arg for argument in node.args.args),
                len(node.body),
            )
    return out


def _diverged():
    """Functions this family defines more than once, with the same parameters and different bodies."""
    bodies = _bodies()
    by_name = {}
    for (name, stem), value in bodies.items():
        by_name.setdefault(name, []).append((stem, value))
    out = {}
    for name, entries in by_name.items():
        if len(entries) < 2:
            continue
        if len({value[1] for _stem, value in entries}) != 1:
            continue
        if len({value[0] for _stem, value in entries}) == 1:
            continue
        out[(name, tuple(sorted(stem for stem, _value in entries)))] = entries
    return out


class TheScoringRulesHaveOneDefinition(unittest.TestCase):

    def test_the_family_is_there_to_compare(self) -> None:
        """A floor. Every assertion below passes over an empty read."""
        bodies = _bodies()
        self.assertGreater(
            len(bodies), 40,
            "only %d functions were read across %s, so this file is comparing almost nothing"
            % (len(bodies), ", ".join(FAMILY)))
        for stem in FAMILY:
            with self.subTest(module=stem):
                self.assertTrue(
                    [key for key in bodies if key[1] == stem],
                    "%s contributed no functions; a renamed or removed module makes every "
                    "comparison here vacuous" % stem)

    def test_the_divergence_is_exactly_what_is_recorded(self) -> None:
        """Asserted in BOTH directions.

        A new pair fails, so the split cannot grow quietly. A recorded pair that stops diverging
        fails too, because a list allowed to rot describes a tree that no longer exists -- and in
        this case it would hide the fact that somebody had chosen a winner.
        """
        found = set(_diverged())
        recorded = set(RECORDED)
        new = sorted(found - recorded)
        gone = sorted(recorded - found)
        self.assertEqual(
            [], new,
            "these scoring rules now have two definitions that disagree, and nothing records what "
            "they disagree about: %s" % ", ".join("%s in %s" % (n, " and ".join(m)) for n, m in new))
        self.assertEqual(
            [], gone,
            "these are recorded as diverged and no longer are -- strike them, and say which copy "
            "won: %s" % ", ".join("%s in %s" % (n, " and ".join(m)) for n, m in gone))

    def test_recall_scoring_still_reaches_the_copy_without_profile_memory(self) -> None:
        """The consequence, stated as the thing that is actually wrong.

        The divergence matters because of WHICH copy the serving path binds. If this ever fails
        because recall_scoring now reaches core's copy, that is the fix -- strike the entries above
        and delete this test with them.
        """
        with open(os.path.join(TOOLS, "matrixark_mcp_recall_scoring.py"),
                  encoding="utf-8", errors="replace") as handle:
            body = handle.read()
        self.assertIn(
            "matrixark_mcp_access_scope", body,
            "matrixark_mcp_recall_scoring no longer imports from matrixark_mcp_access_scope. If it "
            "now takes the scoring rules from matrixark_mcp_core, the split is resolved.")
        for name in ("cross_session_rerank_adjustment", "session_continuity_boost"):
            with self.subTest(rule=name):
                self.assertIn(name, body, "%s is no longer used by the recall scorer" % name)


if __name__ == "__main__":
    unittest.main()
