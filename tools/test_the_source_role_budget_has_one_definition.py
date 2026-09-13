#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`auto_source_role_budget_tokens` is defined twice, and one copy never learned profile memory.

The function splits a retrieval budget between the assistant, tool and user roles. Both copies are
live, and which one serves a query depends on the path:

    matrixark_mcp_local_adapter        <- matrixark_local_adapter_retrieve, matrixark_temporal_direct_read
    matrixark_mcp_retrieve_pre_refresh <- matrixark_mcp_retrieve_planning (as pre_refresh_helpers)

They are the same length and the same signature. The difference is one branch:

    feature_profile_query = feature_profile_memory_budget_query(args, ranking, ...)
    ...
    elif feature_profile_query:
        defaults.update({"assistant": 0.5, "tool": 0.2, "user": 0.7})

`matrixark_mcp_retrieve_pre_refresh` has it. `matrixark_mcp_local_adapter` does not -- although it
DEFINES `feature_profile_memory_budget_query` itself and calls it 33 lines earlier, in
`_default_memory_budget_mode`, to pick the budget MODE. So that module uses the predicate to decide
whether to budget at all and then ignores it when deciding the split.

MEASURED, remote_budget_tokens=10000, query "what are my standing preferences":

    question_type     local_adapter                          pre_refresh
    profile_memory    assistant 5000 tool 4500 user 5000     assistant 5000 tool 2000 user 7000
    fact              assistant 4500 tool 3500 user 6000     assistant 5000 tool 2000 user 7000
    evidence          assistant 3500 tool 5000 user 4500     assistant 5000 tool 2000 user 7000

The branch fires on the QUERY TEXT as well as on question_type -- that query matches the standing
rule pattern -- so in the pre_refresh copy it wins over every question-type branch. The tool budget
differs by 2.5x on an evidence question, and the two paths disagree for every question type tested,
not only for profile_memory.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE, because they do not, and a guard that fails on the
day it is written tells nobody anything. It RECORDS the split in both directions, so a new
divergence fails here and a resolved one fails here too. Choosing which budget wins changes what is
packed on a serving path, which is a decision rather than a cleanup.

This is the same feature, in a second place, as the recorded scoring split: one live copy behind on
profile memory while its sibling has it.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_local_adapter as local_adapter_module
import matrixark_mcp_retrieve_pre_refresh as pre_refresh_module

BUDGET = 10000
QUERY = "what are my standing preferences"

#: question_type -> (local_adapter result, pre_refresh result), measured.
RECORDED = {
    "profile_memory": ({"assistant": 5000, "tool": 4500, "user": 5000},
                       {"assistant": 5000, "tool": 2000, "user": 7000}),
    "fact": ({"assistant": 4500, "tool": 3500, "user": 6000},
             {"assistant": 5000, "tool": 2000, "user": 7000}),
    "evidence": ({"assistant": 3500, "tool": 5000, "user": 4500},
                 {"assistant": 5000, "tool": 2000, "user": 7000}),
}

COPIES = {
    "matrixark_mcp_local_adapter": local_adapter_module,
    "matrixark_mcp_retrieve_pre_refresh": pre_refresh_module,
}


def _budget(module, question_type):
    budgets, mode = module.auto_source_role_budget_tokens(
        {"query": QUERY, "source_role_budget_mode": "auto"}, {},
        remote_budget_tokens=BUDGET, question_type=question_type)
    return budgets, mode


class TheSourceRoleBudgetHasOneDefinition(unittest.TestCase):

    def test_both_copies_are_there_and_budget_something(self) -> None:
        """A floor. A copy that returns nothing makes every comparison below vacuous."""
        for name, module in sorted(COPIES.items()):
            with self.subTest(module=name):
                budgets, mode = _budget(module, "fact")
                self.assertEqual("auto", mode, "%s did not take the auto budget mode" % name)
                self.assertTrue(budgets, "%s produced no budgets at all" % name)
                self.assertTrue(
                    all(1 <= v <= BUDGET for v in budgets.values()),
                    "%s returned a budget outside 1..%d: %s" % (name, BUDGET, budgets))
                self.assertEqual(
                    {"assistant", "tool", "user"}, set(budgets),
                    "%s no longer splits across the three roles: %s" % (name, sorted(budgets)))

    def test_the_two_paths_budget_differently(self) -> None:
        """Recorded, both directions, measured rather than read."""
        for question_type, (expected_local, expected_pre) in sorted(RECORDED.items()):
            with self.subTest(question_type=question_type):
                local, _ = _budget(local_adapter_module, question_type)
                pre, _ = _budget(pre_refresh_module, question_type)
                self.assertEqual(
                    expected_local, local,
                    "matrixark_mcp_local_adapter's budget changed for %s" % question_type)
                self.assertEqual(
                    expected_pre, pre,
                    "matrixark_mcp_retrieve_pre_refresh's budget changed for %s" % question_type)
                self.assertNotEqual(
                    local, pre,
                    "the two copies now agree for %s -- the split is resolved. Strike this file "
                    "and say which budget won." % question_type)

    def test_only_one_copy_consults_the_profile_memory_predicate(self) -> None:
        """The mechanism, stated as the thing that is actually wrong.

        matrixark_mcp_local_adapter DEFINES feature_profile_memory_budget_query and uses it in
        _default_memory_budget_mode, so the predicate is right there. Its budget split does not
        call it.
        """
        import inspect
        local_source = inspect.getsource(local_adapter_module.auto_source_role_budget_tokens)
        pre_source = inspect.getsource(pre_refresh_module.auto_source_role_budget_tokens)

        self.assertIn(
            "feature_profile_memory_budget_query", pre_source,
            "matrixark_mcp_retrieve_pre_refresh stopped consulting the profile-memory predicate")
        self.assertNotIn(
            "feature_profile_memory_budget_query", local_source,
            "matrixark_mcp_local_adapter's budget split now consults the profile-memory predicate "
            "too, so the split is resolved -- strike this file")
        self.assertTrue(
            callable(getattr(local_adapter_module, "feature_profile_memory_budget_query", None)),
            "matrixark_mcp_local_adapter no longer defines the predicate it does not use; if it "
            "moved, this file's explanation needs restating")

    def test_the_branch_fires_on_query_text_not_only_question_type(self) -> None:
        """Why the split reaches every question type, not just profile_memory.

        The predicate matches the QUERY as well, so in the copy that has the branch it wins over
        every question-type case. A query with no profile wording should narrow the difference.
        """
        plain = {"query": "which release fixed the timeout", "source_role_budget_mode": "auto"}
        local, _ = local_adapter_module.auto_source_role_budget_tokens(
            dict(plain), {}, remote_budget_tokens=BUDGET, question_type="evidence")
        pre, _ = pre_refresh_module.auto_source_role_budget_tokens(
            dict(plain), {}, remote_budget_tokens=BUDGET, question_type="evidence")
        self.assertEqual(
            local, pre,
            "with no profile wording in the query the two copies should agree for evidence; they "
            "now differ, so the split is wider than this file records")


if __name__ == "__main__":
    unittest.main()
