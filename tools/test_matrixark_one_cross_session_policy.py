#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""There is one cross-session policy builder, and both paths reach it.

`build_cross_session_policy` existed twice. `matrixark_local_adapter_retrieve` and
`matrixark_mcp_retrieve_planning` bound the `matrixark_mcp_core_scoring` copy;
`matrixark_mcp_budget_pack` bound the `matrixark_mcp_budget_policies` one. Both were live, reached
by different callers, and they had drifted -- so which cross-session behaviour a request got
depended on which module the caller happened to import.

The drift was one-directional. `budget_policies` matched a single profile-memory pattern where
`core_scoring` matches three, and had no handling for an explicitly requested bridge. Every line
unique to it was a narrower form of a line in the other; it held nothing the other lacked.

These tests assert the OUTCOME rather than the wiring: identical arguments through either module
must produce an identical policy. Asserting that both names point at one function would pass just as
well if the shared function were the narrower one, which is the failure this replaced.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter  # noqa: E402,F401  (establishes the package first)
import matrixark_mcp_budget_policies as budget_policies  # noqa: E402
import matrixark_mcp_core_scoring as core_scoring  # noqa: E402

# A standing-rule phrasing. The retired copy matched only PROFILE_MEMORY_QUERY_RE, so it did not
# recognise this as profile memory; the surviving one also checks
# PROFILE_MEMORY_STANDING_RULE_QUERY_RE and ACTIVE_MEMORY_GOAL_QUERY_RE.
STANDING_RULE = {"query": "what is my standing rule about deploys"}
PLAIN = {"query": "what did we ship last week"}

CASES = (
    ("standing rule, prefer", STANDING_RULE, "profile_memory", "prefer"),
    ("standing rule, session only", STANDING_RULE, "profile_memory", "only"),
    ("plain query, prefer", PLAIN, "latest", "prefer"),
    ("explicit bridge requested", STANDING_RULE, "current_state", "only"),
)


def _both(args, ranking, question_type, session_scope, budget=4096):
    kwargs = dict(question_type=question_type, session_scope=session_scope,
                  remote_budget_tokens=budget)
    return (budget_policies.build_cross_session_policy(args, ranking, **kwargs),
            core_scoring.build_cross_session_policy(args, ranking, **kwargs))


class BothModulesGiveTheSamePolicyTest(unittest.TestCase):

    def test_identical_for_every_case(self) -> None:
        for label, args, question_type, session_scope in CASES:
            with self.subTest(case=label):
                through_pack, through_retrieve = _both(args, {}, question_type, session_scope)
                self.assertEqual(
                    through_retrieve, through_pack,
                    "the two modules still disagree for %r: a request gets different "
                    "cross-session behaviour depending on which one its caller imported" % label)

    def test_an_explicitly_requested_bridge_is_honoured_through_both(self) -> None:
        # `explicit_cross_session_enabled` / `explicit_profile_bridge_requested` existed only in the
        # surviving copy, so this is one of the behaviours the pack path did without.
        ranking = {"cross_session": {"enabled": True, "min_entity_bridge_refs": 2}}
        through_pack, through_retrieve = _both(STANDING_RULE, ranking, "profile_memory", "only")
        self.assertEqual(through_retrieve, through_pack)
        self.assertTrue(through_pack.get("enabled"),
                        "an explicitly enabled cross-session bridge was not honoured")

    def test_the_result_is_not_trivially_empty(self) -> None:
        # Two empty dicts compare equal. Without this, every assertion above would hold on a
        # builder that returned nothing at all.
        policy, _ = _both(STANDING_RULE, {}, "profile_memory", "prefer")
        self.assertGreater(len(policy), 5, "the policy came back nearly empty: %r" % policy)
        for key in ("enabled", "budget_tokens", "max_candidates"):
            self.assertIn(key, policy)


class ThereIsOnlyOneImplementationTest(unittest.TestCase):
    """The duplicate must not come back."""

    def test_budget_policies_does_not_define_its_own_body(self) -> None:
        import ast
        import io
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "matrixark_mcp_budget_policies.py")
        with io.open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "build_cross_session_policy":
                body = node.end_lineno - node.lineno
                self.assertLess(
                    body, 45,
                    "build_cross_session_policy in matrixark_mcp_budget_policies is %d lines. It "
                    "should delegate. A second implementation here drifted once already and the "
                    "two disagreed for months." % body)
                return
        self.fail("build_cross_session_policy is not defined in matrixark_mcp_budget_policies")


if __name__ == "__main__":
    unittest.main()
