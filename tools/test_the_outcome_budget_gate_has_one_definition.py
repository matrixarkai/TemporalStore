#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two copies of the outcome-query gate, and one accepts three question types the other refuses.

`codex_outcome_budget_query` decides whether a retrieval query is asking what the assistant DID,
and every budget resolver that answers yes shifts the role split towards assistant and tool text.
It is defined twice, and both are live:

    matrixark_mcp_local_adapter            defines its own; calls it from
                                           auto_source_role_budget_tokens
    matrixark_mcp_retrieve_pre_refresh     defines its own; calls it from three budget
                                           resolvers, and matrixark_mcp_local_adapter,
                                           matrixark_mcp_retrieve_planning and
                                           matrixark_mcp_retrieve_request all import that module

The two are not the same function, and this is not an oversight of the "extracted, never adopted"
kind: `matrixark_mcp_local_adapter` RE-EXPORTS four of this one's immediate neighbours from
`matrixark_mcp_retrieve_pre_refresh` -- `codex_user_goal_budget_query`,
`feature_scope_budget_query`, `codex_outcome_event_segment_layer_fractions` and
`pre_retrieval_summary_refresh_enabled`, each under the comment "the implementation lives in
matrixark_mcp_retrieve_pre_refresh; this module re-exports it". It also imports
`AUTO_BUDGET_QUERY_TYPES` and `FEATURE_MEMORY_BUDGET_QUERY_RE` from there. This one name it keeps
a copy of, and that copy has a wider gate.

MEASURED BY EXECUTION, on one outcome-shaped query run through both copies at every question type
in `AUTO_BUDGET_QUERY_TYPES`:

    question_type      local_adapter   pre_refresh
    benchmark_quality      True           True
    broad_exploration      False          False
    current_state          True           True
    date                   True           False
    evidence               True           True
    latest                 True           True
    multi_hop              True           False
    profile_memory         True           False

Neither copy uses `AUTO_BUDGET_QUERY_TYPES`, the constant the two modules already share for this
exact purpose. Each hard-codes a different subset of it: seven of the eight on one side, four on
the other, and `broad_exploration` is in the constant and accepted by neither.

THE CONSEQUENCE, also executed. Through `auto_source_role_budget_tokens` on a 1000-token remote
budget, the same outcome-shaped query at the same question type gets:

    date, multi_hop     assistant 550 tool 550 user 400   through the local adapter
                        assistant 450 tool 450 user 500   through the pre-refresh resolver
    profile_memory      assistant 550 tool 550 user 400   through the local adapter
                        assistant 500 tool 200 user 700   through the pre-refresh resolver

`auto_source_role_budget_tokens` is ITSELF a diverged pair, recorded separately -- so the token
numbers above are the two divergences compounded, and this file does not claim they are caused by
the gate alone. What it claims, and asserts, is the gate's own answer per question type, which is
a property of these two functions and nothing else.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE. Widening the narrower gate changes the role split
on a live retrieval path for three question types; narrowing the wider one does the same in the
other direction. That is a ranking decision, not a cleanup. So the divergence is recorded in BOTH
directions: a new one fails here, and a resolved one fails here too.

NOT CLAIMED: that either gate is the right one, or that `broad_exploration` belongs in it. Only
that the two disagree, that both are reached, and that the constant both modules share is used by
neither.
"""
from __future__ import annotations

import ast
import importlib
import os
import unittest
from typing import Dict, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))

FUNCTION = "codex_outcome_budget_query"

#: Every live copy, and what reaches it. Every assertion is driven from this dict.
LIVE_COPIES: Dict[str, str] = {
    "matrixark_mcp_local_adapter":
        "its own auto_source_role_budget_tokens calls it",
    "matrixark_mcp_retrieve_pre_refresh":
        "three of its budget resolvers call it, and the local adapter, the retrieve planner "
        "and the retrieve request builder all import that module",
}

#: Production modules that import matrixark_mcp_retrieve_pre_refresh, which is how the second
#: copy is on a live path at all.
PRE_REFRESH_IMPORTERS = (
    "matrixark_mcp_local_adapter",
    "matrixark_mcp_retrieve_planning",
    "matrixark_mcp_retrieve_request",
)

#: A query the outcome pattern matches. Without this the question-type gate is not what decides
#: the answer and every set below is empty for the wrong reason.
OUTCOME_QUERY = "what did codex push and what was done"

#: A query the pattern does NOT match, to show the pattern is load-bearing in both copies.
PLAIN_QUERY = "where did we go for lunch"

#: question types each copy answers True for, over AUTO_BUDGET_QUERY_TYPES plus two types that
#: are not in it. Asserted EXACTLY for every copy, not as a difference.
RECORDED_ACCEPTED = {
    "matrixark_mcp_local_adapter": frozenset({
        "benchmark_quality", "current_state", "date", "evidence", "latest", "multi_hop",
        "profile_memory"}),
    "matrixark_mcp_retrieve_pre_refresh": frozenset({
        "benchmark_quality", "current_state", "evidence", "latest"}),
}

#: In the shared constant and accepted by neither copy.
RECORDED_IN_THE_CONSTANT_AND_NEITHER_GATE = frozenset({"broad_exploration"})

#: Types that are NOT in AUTO_BUDGET_QUERY_TYPES, probed to show the gate refuses them too.
OUTSIDE_THE_CONSTANT = ("fact", "unknown")

#: The role split each copy's module produces for an outcome-shaped query at the question types
#: the two gates disagree about, on a 1000-token remote budget. Recorded as the consequence.
#: auto_source_role_budget_tokens is itself a diverged pair recorded elsewhere, so these numbers
#: are both divergences together -- which is the point: they compound on one live path.
RECORDED_ROLE_BUDGET = {
    "date": {
        "matrixark_mcp_local_adapter": {"assistant": 550, "tool": 550, "user": 400},
        "matrixark_mcp_retrieve_pre_refresh": {"assistant": 450, "tool": 450, "user": 500},
    },
    "multi_hop": {
        "matrixark_mcp_local_adapter": {"assistant": 550, "tool": 550, "user": 400},
        "matrixark_mcp_retrieve_pre_refresh": {"assistant": 450, "tool": 450, "user": 500},
    },
    "profile_memory": {
        "matrixark_mcp_local_adapter": {"assistant": 550, "tool": 550, "user": 400},
        "matrixark_mcp_retrieve_pre_refresh": {"assistant": 500, "tool": 200, "user": 700},
    },
}

REMOTE_BUDGET_TOKENS = 1000
MODULE_SCAN_FLOOR = 200


def _import(stem):
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


def _defining_modules() -> Tuple[Set[str], int]:
    found: Set[str] = set()
    scanned = 0
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_") or name.startswith("__"):
            continue
        scanned += 1
        try:
            with open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):  # pragma: no cover
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == FUNCTION:
                found.add(name[:-3])
    return found, scanned


def _call_sites(stem: str) -> int:
    """How many times the module CALLS the name, not counting the def."""
    with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    return sum(1 for node in ast.walk(tree)
               if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
               and node.func.id == FUNCTION)


def _copies():
    return {stem: getattr(_import(stem), FUNCTION) for stem in LIVE_COPIES}


def _question_types() -> Tuple[str, ...]:
    shared = _import("matrixark_mcp_retrieve_pre_refresh").AUTO_BUDGET_QUERY_TYPES
    return tuple(sorted(set(shared) | set(OUTSIDE_THE_CONSTANT)))


def _accepted(query: str) -> Dict[str, Set[str]]:
    out = {stem: set() for stem in LIVE_COPIES}
    for stem, gate in _copies().items():
        for question_type in _question_types():
            if gate({"query": query}, {}, question_type=question_type):
                out[stem].add(question_type)
    return out


class TwoOutcomeBudgetQueryGates(unittest.TestCase):

    def test_the_copies_are_there_to_compare(self) -> None:
        """A floor and the anchor. Every assertion below passes over an empty read."""
        found, scanned = _defining_modules()
        self.assertGreaterEqual(
            scanned, MODULE_SCAN_FLOOR,
            "the definition scan read only %d production modules" % scanned)
        self.assertEqual(
            sorted(LIVE_COPIES), sorted(found),
            "%s is defined by a different set of production modules than this file records. A new "
            "copy must be added to LIVE_COPIES; a copy that is gone means the split is resolved."
            % FUNCTION)
        copies = _copies()
        for stem, gate in copies.items():
            with self.subTest(module=stem):
                self.assertEqual(
                    stem, gate.__module__.rsplit(".", 1)[-1],
                    "%s.%s is owned by %s" % (stem, FUNCTION, gate.__module__))
        self.assertEqual(
            len(LIVE_COPIES), len({id(gate) for gate in copies.values()}),
            "the copies are the same object, so one module now re-exports the other's and this "
            "file is comparing a function with itself")

    def test_each_copy_is_on_a_live_path(self) -> None:
        """Each copy is called inside its own module, and both modules are reached."""
        for stem in LIVE_COPIES:
            with self.subTest(module=stem):
                self.assertGreater(
                    _call_sites(stem), 0,
                    "%s defines %s and no longer calls it; if the copy is now dead it should go, "
                    "not sit here diverged" % (stem, FUNCTION))
        for stem in PRE_REFRESH_IMPORTERS:
            with self.subTest(importer=stem):
                with open(os.path.join(TOOLS, stem + ".py"),
                          encoding="utf-8", errors="replace") as handle:
                    body = handle.read()
                self.assertIn(
                    "matrixark_mcp_retrieve_pre_refresh", body,
                    "%s no longer imports matrixark_mcp_retrieve_pre_refresh; if nothing does, "
                    "that copy is not live and this record should say so" % stem)

    def test_the_probe_queries_reach_the_gate(self) -> None:
        """A floor on the FIXTURE, not the code.

        Both copies return False for ANY question type when the pattern does not match, so a
        probe query that stopped matching would make every recorded set empty and agree.
        """
        pattern = _import("matrixark_mcp_retrieve_pre_refresh").CODEX_OUTCOME_QUERY_RE
        self.assertTrue(
            pattern.search(OUTCOME_QUERY.lower()),
            "the probe query no longer matches the outcome pattern, so the question-type gate is "
            "not what decides the answers recorded below")
        self.assertFalse(
            pattern.search(PLAIN_QUERY.lower()),
            "the control query now matches the outcome pattern, so it is not a control")
        for stem, accepted in _accepted(PLAIN_QUERY).items():
            with self.subTest(module=stem):
                self.assertEqual(
                    set(), accepted,
                    "%s's copy accepted a query the pattern does not match, so the pattern is "
                    "not load-bearing and the sets below are measuring something else" % stem)
        types = _question_types()
        self.assertGreaterEqual(
            len(types), 8,
            "only %d question types were probed; AUTO_BUDGET_QUERY_TYPES has shrunk and the sets "
            "below cover almost nothing" % len(types))

    def test_the_question_types_each_gate_accepts_are_exactly_these(self) -> None:
        """Asserted as a set, per copy, in both directions -- not as a difference."""
        accepted = _accepted(OUTCOME_QUERY)
        for stem in LIVE_COPIES:
            with self.subTest(module=stem):
                self.assertEqual(
                    sorted(RECORDED_ACCEPTED[stem]), sorted(accepted[stem]),
                    "%s's copy of %s now accepts a different set of question types. Added names "
                    "widen a live budget gate; missing ones narrow it. Either way, say which copy "
                    "won." % (stem, FUNCTION))
        self.assertNotEqual(
            sorted(accepted["matrixark_mcp_local_adapter"]),
            sorted(accepted["matrixark_mcp_retrieve_pre_refresh"]),
            "the two gates now accept the same set, which means the split is resolved and this "
            "file should be struck rather than passing quietly")

    def test_neither_gate_uses_the_constant_both_modules_share(self) -> None:
        """The reason this drifted: a shared constant exists for it and neither copy reads it."""
        shared = set(_import("matrixark_mcp_retrieve_pre_refresh").AUTO_BUDGET_QUERY_TYPES)
        self.assertIs(
            _import("matrixark_mcp_local_adapter").AUTO_BUDGET_QUERY_TYPES,
            _import("matrixark_mcp_retrieve_pre_refresh").AUTO_BUDGET_QUERY_TYPES,
            "the two modules no longer share one AUTO_BUDGET_QUERY_TYPES object, which is a "
            "second divergence and not the one this file records")
        accepted = _accepted(OUTCOME_QUERY)
        for stem in LIVE_COPIES:
            with self.subTest(module=stem):
                self.assertNotEqual(
                    sorted(shared), sorted(accepted[stem]),
                    "%s's gate now accepts exactly AUTO_BUDGET_QUERY_TYPES. If it reads the "
                    "constant, say so and strike this test." % stem)
        neither = shared - set().union(*accepted.values())
        self.assertEqual(
            sorted(RECORDED_IN_THE_CONSTANT_AND_NEITHER_GATE), sorted(neither),
            "a different set of question types is now in AUTO_BUDGET_QUERY_TYPES and accepted by "
            "no gate at all")

    def test_the_role_budget_the_split_produces_is_exactly_this(self) -> None:
        """The consequence, in tokens, for the question types the gates disagree about."""
        for question_type, expected in RECORDED_ROLE_BUDGET.items():
            measured = {}
            for stem, budget in expected.items():
                with self.subTest(question_type=question_type, module=stem):
                    resolver = getattr(_import(stem), "auto_source_role_budget_tokens")
                    produced, mode = resolver(
                        {"query": OUTCOME_QUERY},
                        {"source_role_budget_mode": "auto"},
                        remote_budget_tokens=REMOTE_BUDGET_TOKENS,
                        question_type=question_type)
                    measured[stem] = produced
                    self.assertEqual(
                        "auto", mode,
                        "%s no longer resolves this request to the auto budget mode, so the "
                        "numbers below are not being produced by the path they describe" % stem)
                    self.assertEqual(
                        budget, produced,
                        "%s now splits a %s outcome query differently" % (stem, question_type))
            # Asserted on what was MEASURED, not on the constants above: a record edited into
            # agreement would otherwise satisfy this while the code still disagreed.
            self.assertNotEqual(
                measured["matrixark_mcp_local_adapter"],
                measured["matrixark_mcp_retrieve_pre_refresh"],
                "the two paths now produce the same split for %s, which means the split is "
                "resolved -- strike this entry and say which copy won" % question_type)


if __name__ == "__main__":
    unittest.main()
