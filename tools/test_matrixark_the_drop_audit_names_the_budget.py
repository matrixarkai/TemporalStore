#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A dropped ref says WHICH budget dropped it.

Two modules carried `dropped_candidate_audit_ref`. They had drifted in both directions, so the
consolidation kept the wider recording rule and deleted the other copy on the stated grounds that
the survivor was "the richer record". It was not. The deleted copy carried twenty more fields --
only when the candidate had them -- and every one of them answers the question an audit exists to
answer: which budget, floor or role cap removed this ref.

Twelve tests went red on that merge, all with the same shape::

    KeyError: 'memory_layer_budget_capped_layer'

They were read as belonging to main and merged past. This asserts the union directly, so the
record cannot quietly get poorer again.

**Absent, not empty.** A field the candidate does not carry is left out rather than written as
``""``. A reader distinguishes "this budget did not act" from "this budget capped nothing", and an
empty key says the second when the first is true.

**Why a subprocess.** Reaching `matrixark_mcp_recall_scoring` from a test binds `matrixark_mcp_core`
under whichever name got there first, and the half-built package is left in `sys.modules` for the
next module to trip over: the first version of this file passed alone and reddened its neighbour,
in one import order only. The record is read in a child process, so this suite cannot decide what
any other suite imports.
"""
from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: The names the twelve failures asked for. Written out rather than read from the constant under
#: test: a test that takes its expectations from the thing it checks passes however small it gets.
NAMED_BY_THE_FAILURES = (
    "budget_memory_layer",
    "budget_source_roles",
    "memory_layer_budget_capped_layer",
    "memory_layer_floor_reserved_layer",
    "memory_selection_policy_budget_capped_policies",
    "source_role_budget_capped_roles",
)

_PROBE = r'''
import json, sys
sys.path.insert(0, ".")
import matrixark_mcp_core as _core            # noqa: F401 - binds the graph before the leaf
import matrixark_mcp_recall_scoring as scoring
cases = json.loads(sys.argv[1])
out = {"fields": list(scoring._EXPLANATORY_FIELDS), "refs": {}}
for name, candidate in cases.items():
    base = {"ref_type": "event", "ref_hash": "h", "score": 0.5}
    base.update(candidate)
    out["refs"][name] = scoring.dropped_candidate_audit_ref(
        base, reason="budget", token_estimate=12)
print(json.dumps(out))
'''

_RESULT: list = []


def probe() -> dict:
    """One child process, every case at once -- a process per field would be minutes."""
    if _RESULT:
        return _RESULT[0]
    every = {f: "carried:%s" % f for f in (
        "budget_memory_layer", "budget_memory_selection_policies", "budget_source_role_counts",
        "budget_source_roles", "classification", "event_type", "extraction_mode",
        "extraction_phase", "extraction_status", "memory_layer_budget_capped_layer",
        "memory_layer_floor_reserved_layer", "memory_selection_policy_budget_capped_policies",
        "source_codex_event_counts", "source_codex_events", "source_hook_type_counts",
        "source_hook_types", "source_role", "source_role_budget_capped_roles",
        "source_role_counts", "source_roles")}
    cases = {
        "bare": {},
        "every": every,
        "capped": {"memory_layer_budget_capped_layer": "pending_async_event",
                   "memory_selection_policy_budget_capped_policies": ["assistant_decision"],
                   "source_role_budget_capped_roles": ["assistant"]},
        "empty": {"source_role_budget_capped_roles": [], "budget_memory_layer": ""},
    }
    environ = dict(os.environ)
    environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/matrixark-drop-audit-test.json"
    out = subprocess.run([sys.executable, "-c", _PROBE, json.dumps(cases)],
                         cwd=TOOLS, env=environ, capture_output=True, text=True, timeout=600)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    _RESULT.append(json.loads(out.stdout))
    return _RESULT[0]


class TheRefSaysWhichBudgetDroppedItTest(unittest.TestCase):

    def test_the_fields_the_failures_asked_for_are_offered(self) -> None:
        offered = probe()["fields"]
        for field in NAMED_BY_THE_FAILURES:
            self.assertIn(field, offered, "%s is what a reader asks the audit for" % field)

    def test_every_offered_field_reaches_the_record(self) -> None:
        result = probe()
        ref = result["refs"]["every"]
        for field in result["fields"]:
            with self.subTest(field=field):
                self.assertEqual("carried:%s" % field, ref.get(field),
                                 "%s was on the candidate and not in the audit" % field)

    def test_a_capped_ref_names_the_layer_the_policy_and_the_role(self) -> None:
        """The case the pipeline suite reads, whole rather than field by field."""
        ref = probe()["refs"]["capped"]
        self.assertEqual("pending_async_event", ref["memory_layer_budget_capped_layer"])
        self.assertEqual(["assistant_decision"],
                         ref["memory_selection_policy_budget_capped_policies"])
        self.assertEqual(["assistant"], ref["source_role_budget_capped_roles"])

    def test_a_field_the_candidate_lacks_is_absent_not_empty(self) -> None:
        result = probe()
        ref = result["refs"]["bare"]
        for field in result["fields"]:
            with self.subTest(field=field):
                self.assertNotIn(field, ref, "%s was written as a blank" % field)

    def test_an_empty_value_is_left_out_too(self) -> None:
        """A candidate carrying `[]` for a cap did not have anything capped."""
        ref = probe()["refs"]["empty"]
        self.assertNotIn("source_role_budget_capped_roles", ref)
        self.assertNotIn("budget_memory_layer", ref)

    def test_the_always_fields_are_still_always(self) -> None:
        """The floor. This adds an optional pass; it must not have made the base optional."""
        ref = probe()["refs"]["bare"]
        for field in ("ref_type", "ref_hash", "drop_reason", "reason", "score", "token_estimate",
                      "token_cost", "context_class", "access_decision", "node_path"):
            self.assertIn(field, ref)
        self.assertEqual("budget", ref["drop_reason"])


class TheRecordHasOneBuilderTest(unittest.TestCase):
    """Two copies drifting is what lost the fields. One is the whole remedy."""

    @staticmethod
    def _definers() -> list:
        found = []
        for name in sorted(os.listdir(TOOLS)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            try:
                with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
                    tree = ast.parse(handle.read())
            except (OSError, SyntaxError):
                continue
            for node in tree.body:
                if isinstance(node, ast.FunctionDef) and node.name == "dropped_candidate_audit_ref":
                    found.append(name[:-3])
        return found

    def test_exactly_one_module_defines_it(self) -> None:
        self.assertEqual(["matrixark_mcp_recall_scoring"], self._definers(),
                         "a second copy is how the record got poorer the first time")

    def test_the_scan_would_see_a_second_one(self) -> None:
        """The floor for the test above: it must be finding a real definition, not nothing."""
        self.assertTrue(self._definers())


if __name__ == "__main__":
    unittest.main()
