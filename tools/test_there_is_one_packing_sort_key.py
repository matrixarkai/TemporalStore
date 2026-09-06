# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One packing sort key, and the flag that shifts it reaches every packer that uses it.

`packing_sort_key` was implemented twice, and both are on live paths.
`matrixark_local_adapter_retrieve` and `matrixark_mcp_core_ref_selection` sort with
`matrixark_mcp_core_packing`; `matrixark_mcp_budget_pack`, which the gateway reaches through
`matrixark_mcp_budget_policies`, sorted with `matrixark_mcp_recall_scoring`.

The copies had drifted in ONE direction. The core_packing one gained three things the other never
did:

  * the `pending_async` penalty of 0.32. Without it, a candidate whose extraction has not finished
    scored 0.90 where the other packer gave the same candidate 0.58 -- so provisional content
    sorted to the TOP on one path and was demoted on the other, on every question type, with no
    flag involved.
  * `MATRIXARK_PACK_RAW_PRECISION`, which shifts events up and summaries down for precision
    questions. It existed in one packer only, so turning it on changed one ordering and left the
    other alone: the flag reached half the system.
  * the feature-profile-memory component, which is why the key was five values on one side and
    four on the other.

This file asserts the property that matters -- the two produce the SAME key -- rather than that
there is one definition, because the second is now a deliberate delegate and a definition count
cannot tell a delegate from a divergence.

The corpus below is chosen to discriminate. Ordinary candidates agreed under both copies before
this change; it takes a pending-async candidate, or the flag turned on with a precision question
type, to tell them apart. `test_the_corpus_can_tell_the_two_apart` pins exactly that, so the
comparison cannot quietly start running on cases that agree either way.
"""
from __future__ import annotations

import ast
import importlib
import os
import subprocess
import unittest
from typing import Any, Dict, List

FLAG_ATTRIBUTE = "PACK_RAW_PRECISION"

#: Every question type the flag distinguishes, plus two it does not, as the control.
QUESTION_TYPES = ("fact", "multi_hop", "evidence", "benchmark_quality", "date",
                  "current_state", "latest", "profile_memory")

#: Candidates that discriminate. The first four are ordinary and agreed under both copies; the
#: pending-async ones are what the 0.32 penalty acts on, and the event/summary pair is what the
#: flag shifts.
CANDIDATES: List[Dict[str, Any]] = [
    {"id": "plain_event", "score": 0.70, "ref_type": "event", "text": "the deploy went out"},
    {"id": "summary", "score": 0.82, "ref_type": "summary", "text": "a summary of the week"},
    {"id": "compression", "score": 0.80, "ref_type": "compression", "text": "compressed",
     "source_event_ids": ["a", "b"]},
    {"id": "profile_entity", "score": 0.75, "ref_type": "entity", "memory_scope": "user_profile",
     "session_continuity": "cross_session", "profile_entity_current": True,
     "profile_revision": 3, "text": "prefers metric units"},
    {"id": "pending_event", "score": 0.72, "ref_type": "event", "event_type": "pending_async",
     "text": "not yet extracted"},
    {"id": "pending_class", "score": 0.71, "ref_type": "event",
     "classification": "PENDING_ASYNC_EXTRACTION", "text": "also not yet"},
    {"id": "pending_phase", "score": 0.69, "ref_type": "entity",
     "extraction_phase": "pending_async", "text": "phase pending"},
    {"id": "feature_profile", "score": 0.66, "ref_type": "entity",
     "profile_memory_kind": "feature", "memory_scope": "user_profile", "text": "feature"},
]


def _import(name: str):
    try:
        return importlib.import_module("tools." + name)
    except ImportError:
        return importlib.import_module(name)


def _packers():
    _import("matrixark_mcp_local_adapter")          # settles the circular imports
    return _import("matrixark_mcp_core_packing"), _import("matrixark_mcp_recall_scoring")


class ThereIsOnePackingSortKeyTest(unittest.TestCase):

    def setUp(self) -> None:
        self.core, self.recall = _packers()
        self._flag = getattr(self.core, FLAG_ATTRIBUTE)

    def tearDown(self) -> None:
        setattr(self.core, FLAG_ATTRIBUTE, self._flag)

    def _disagreements(self):
        found = []
        for flag in (False, True):
            setattr(self.core, FLAG_ATTRIBUTE, flag)
            for question_type in QUESTION_TYPES:
                for candidate in CANDIDATES:
                    a = self.core.packing_sort_key(dict(candidate), question_type)
                    b = self.recall.packing_sort_key(dict(candidate), question_type)
                    if a != b:
                        found.append((flag, question_type, candidate["id"], a, b))
        return found

    def test_both_packers_produce_the_same_key(self) -> None:
        found = self._disagreements()
        detail = ["flag=%s %s %s: core_packing=%s recall_scoring=%s" % row for row in found[:5]]
        self.assertEqual(
            [], detail,
            "the two live packers disagree on %d of %d cases, so the gateway and the retrieve path "
            "order the same candidates differently:\n  %s"
            % (len(found), 2 * len(QUESTION_TYPES) * len(CANDIDATES), "\n  ".join(detail)))

    def test_the_corpus_can_tell_the_two_apart(self) -> None:
        """A comparison is worth what its inputs discriminate, so pin what they must contain.

        Before the delegate, ordinary candidates agreed under both copies at every question type;
        only the pending-async ones and the flag-plus-precision combination disagreed. A corpus
        that lost those would pass while the copies diverged again.
        """
        ids = {candidate["id"] for candidate in CANDIDATES}
        self.assertTrue(
            {"pending_event", "pending_class", "pending_phase"} <= ids,
            "the corpus lost its pending-async candidates, which are what the 0.32 penalty acts "
            "on -- without them the comparison passes whether or not the penalty is applied")
        self.assertTrue(
            {"plain_event", "summary"} <= ids,
            "the corpus lost the event/summary pair the precision flag shifts in opposite "
            "directions")
        precision = {"fact", "multi_hop", "evidence", "benchmark_quality", "date"}
        self.assertTrue(
            precision <= set(QUESTION_TYPES),
            "the question types the flag acts on are not all covered")
        self.assertTrue(
            {"current_state", "latest"} & set(QUESTION_TYPES),
            "no non-precision question type is covered, so nothing shows the flag is SCOPED")

    def test_the_flag_still_changes_the_key_it_is_supposed_to(self) -> None:
        """If the flag stopped doing anything, the equality above would pass for the wrong reason."""
        setattr(self.core, FLAG_ATTRIBUTE, False)
        off = self.core.packing_sort_key(dict(CANDIDATES[0]), "fact")
        setattr(self.core, FLAG_ATTRIBUTE, True)
        on = self.core.packing_sort_key(dict(CANDIDATES[0]), "fact")
        self.assertNotEqual(
            off, on,
            "MATRIXARK_PACK_RAW_PRECISION no longer changes the key for a precision question, so "
            "the agreement asserted above says nothing about the flag reaching both packers")

        setattr(self.core, FLAG_ATTRIBUTE, False)
        off = self.core.packing_sort_key(dict(CANDIDATES[0]), "current_state")
        setattr(self.core, FLAG_ATTRIBUTE, True)
        on = self.core.packing_sort_key(dict(CANDIDATES[0]), "current_state")
        self.assertEqual(
            off, on,
            "the flag changed a NON-precision question type, so it is no longer scoped to "
            "PRECISION_QUESTION_TYPES and this file describes the wrong thing")

    def test_only_one_module_defines_the_predicate_the_penalty_depends_on(self) -> None:
        """The penalty is only consistent if the predicate behind it is.

        `matrixark_mcp_budget_pack` defined a THIRD `is_pending_async_candidate`, without the
        `ref_type == "event"` gate the canonical one applies -- so the tree had one name meaning
        two things, while `matrixark_local_adapter_dashboard._embedding_is_pending` carried a
        comment explaining that it meant one. It is now named for what it is,
        `carries_pending_async_marker`, and both of its call sites keep exactly the function they
        had: one is already inside a branch that established `ref_type == "event"`, so the gate
        would change nothing there, and the other wants the unguarded reading.
        """
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=repo,
                                capture_output=True, text=True).stdout.split()
        definers = []
        for rel in listed:
            if os.path.basename(rel).startswith("test_"):
                continue
            try:
                with open(os.path.join(repo, rel), encoding="utf-8", errors="replace") as handle:
                    tree = ast.parse(handle.read())
            except (OSError, SyntaxError):
                continue
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                        and node.name == "is_pending_async_candidate":
                    definers.append(os.path.basename(rel)[:-len(".py")])
        self.assertEqual(
            ["matrixark_mcp_core_candidate_policy"], sorted(definers),
            "is_pending_async_candidate is defined in more than one module again. A second copy "
            "agrees wherever the caller has already established ref_type == 'event', which is "
            "most call sites, so nothing routine catches the difference")

    def test_only_one_module_defines_the_predicate_the_penalty_depends_on(self) -> None:
        """The penalty is only consistent if the predicate behind it is.

        `matrixark_mcp_budget_pack` defined a THIRD `is_pending_async_candidate`, without the
        `ref_type == "event"` gate the canonical one applies -- so the tree had one name meaning
        two things, while `matrixark_local_adapter_dashboard._embedding_is_pending` carried a
        comment explaining that it meant one. It is now named for what it is,
        `carries_pending_async_marker`, and both of its call sites keep exactly the function they
        had: one is already inside a branch that established `ref_type == "event"`, so the gate
        would change nothing there, and the other wants the unguarded reading.
        """
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=repo,
                                capture_output=True, text=True).stdout.split()
        definers = []
        for rel in listed:
            if os.path.basename(rel).startswith("test_"):
                continue
            try:
                with open(os.path.join(repo, rel), encoding="utf-8", errors="replace") as handle:
                    tree = ast.parse(handle.read())
            except (OSError, SyntaxError):
                continue
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                        and node.name == "is_pending_async_candidate":
                    definers.append(os.path.basename(rel)[:-len(".py")])
        self.assertEqual(
            ["matrixark_mcp_core_candidate_policy"], sorted(definers),
            "is_pending_async_candidate is defined in more than one module again. A second copy "
            "agrees wherever the caller has already established ref_type == 'event', which is "
            "most call sites, so nothing routine catches the difference")

    def test_the_pending_async_penalty_is_applied(self) -> None:
        """The half of the divergence that needed no flag at all."""
        setattr(self.core, FLAG_ATTRIBUTE, False)
        pending = dict(CANDIDATES[4])
        settled = dict(pending)
        settled.pop("event_type")
        self.assertLess(
            self.core.packing_sort_key(pending, "fact")[0],
            self.core.packing_sort_key(settled, "fact")[0],
            "a candidate whose extraction has not finished no longer sorts below the same "
            "candidate once settled")


if __name__ == "__main__":
    unittest.main()
