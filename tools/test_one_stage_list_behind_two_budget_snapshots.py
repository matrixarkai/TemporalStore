#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two live producers write `stage_latency_budgets`, and each iterates its own copy of the stage list.

`pack["recall_policy"]["stage_latency_budgets"]` is written by two different functions, both live:

  * `matrixark_local_adapter_retrieve.retrieve` defines `stage_budget_snapshot` as a nested closure
    and calls it at three sites. It iterates a local literal, `stage_names`.
  * `matrixark_mcp_retrieve_planning.stage_budget_snapshot` is reached through
    `matrixark_mcp_retrieve_deadline.stage_budget_snapshot`, which delegates to it. It iterates the
    module constant `RETRIEVAL_STAGE_NAMES`.

The two bodies are otherwise identical -- so identical that a name-blind copy scan reports them as
one body under two signatures. That is exactly what hides the difference: the scan abstracts
identifiers, so `stage_names` and `RETRIEVAL_STAGE_NAMES` normalise to the same placeholder and the
pair reads as a harmless duplicate. What they ITERATE is the one thing that is not compared.

The two lists are equal today. Nothing checks that they stay equal, and no test names either the
constant or the function. If they drift, one served field reports a different set of stages
depending on which path produced it -- and both paths write the same key, so nothing downstream can
tell which producer it got.

This file changes no behaviour. It pins the two lists together and pins the number of copies, so a
third one has to be deliberate.
"""
from __future__ import annotations

import ast
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

PLANNING = TOOLS / "matrixark_mcp_retrieve_planning.py"
ADAPTER = TOOLS / "matrixark_local_adapter_retrieve.py"
DEADLINE = TOOLS / "matrixark_mcp_retrieve_deadline.py"

#: The served field both producers write.
SERVED_FIELD = "stage_latency_budgets"

#: Modules allowed to spell the stage list out. Two today: the constant, and the closure literal.
RECORDED_LIST_COPIES = 2


def _module_constant(path: pathlib.Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    return None


def _assigned_literal(path: pathlib.Path, name: str):
    """The first list literal assigned to `name` anywhere in the file, nested scopes included."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets):
            try:
                return ast.literal_eval(node.value)
            except ValueError:
                return None
    return None


def _list_literals_matching(first_element: str):
    """Every list literal in the tree whose first element is `first_element`."""
    found = []
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.List) or not node.elts:
                continue
            head = node.elts[0]
            if isinstance(head, ast.Constant) and head.value == first_element:
                try:
                    found.append((path.stem, node.lineno, ast.literal_eval(node)))
                except ValueError:
                    continue
    return found


def _calls_named(path: pathlib.Path, name: str):
    """Call sites whose callee is exactly `name`, by AST -- not a substring search.

    `"x" in source` is true for `x_retired` too, which is the one rename that matters here.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        spelled = (callee.attr if isinstance(callee, ast.Attribute)
                   else callee.id if isinstance(callee, ast.Name) else None)
        if spelled == name:
            found.append(node.lineno)
    return found


class OneStageListBehindTwoBudgetSnapshots(unittest.TestCase):

    def test_both_lists_are_still_there(self):
        """The floor. If either name moved, everything below compares None to None and passes."""
        constant = _module_constant(PLANNING, "RETRIEVAL_STAGE_NAMES")
        inline = _assigned_literal(ADAPTER, "stage_names")
        self.assertIsInstance(
            constant, list,
            "RETRIEVAL_STAGE_NAMES is no longer a module-level list literal in %s; this file cannot "
            "compare what it cannot read" % PLANNING.name)
        self.assertIsInstance(
            inline, list,
            "no `stage_names = [...]` literal in %s any more. If the closure was changed to use the "
            "constant, that is the fix this file exists to make unnecessary -- delete it and say so"
            % ADAPTER.name)
        self.assertTrue(constant, "the stage list is empty, so comparing it proves nothing")

    def test_the_two_stage_lists_agree(self):
        """The finding: one served field, two producers, two copies of the list they iterate."""
        constant = _module_constant(PLANNING, "RETRIEVAL_STAGE_NAMES")
        inline = _assigned_literal(ADAPTER, "stage_names")
        self.assertEqual(
            constant, inline,
            "the two stage lists have drifted. `%s` is written by two live producers -- the closure "
            "in %s and the module function in %s reached through %s -- and they now iterate "
            "different stages, so the same field reports a different set depending on which path "
            "produced it. Nothing downstream can tell them apart."
            % (SERVED_FIELD, ADAPTER.name, PLANNING.name, DEADLINE.name))

    def test_there_are_still_only_two_copies_of_the_list(self):
        """A third copy is how this becomes hard to fix rather than easy."""
        constant = _module_constant(PLANNING, "RETRIEVAL_STAGE_NAMES")
        copies = _list_literals_matching(constant[0])
        equal_copies = [(module, line) for module, line, value in copies if value == constant]
        self.assertEqual(
            RECORDED_LIST_COPIES, len(equal_copies),
            "the stage list is now spelled out in %d places rather than %d: %s. Each new copy is "
            "another thing to keep in step with the other two."
            % (len(equal_copies), RECORDED_LIST_COPIES,
               ", ".join("%s:%d" % row for row in equal_copies)))

    def test_both_producers_write_the_same_served_field(self):
        """Why a drift would be invisible: the two producers are indistinguishable downstream.

        The name is resolved through the AST rather than searched for as a substring. A substring
        check passes against `stage_budget_snapshot_retired`, which is exactly the rename that would
        mean the second producer had gone -- the mutation run caught that, and it is the reason this
        test compares parsed names.
        """
        self.assertIn(
            SERVED_FIELD, ADAPTER.read_text(encoding="utf-8"),
            "%s no longer writes %s, so it may have stopped being a producer -- re-read this file"
            % (ADAPTER.name, SERVED_FIELD))
        reached = _calls_named(DEADLINE, "stage_budget_snapshot")
        self.assertTrue(
            reached,
            "%s no longer calls anything named exactly `stage_budget_snapshot`, so the second "
            "producer may be gone and this file would be guarding a hazard that no longer exists. "
            "Calls found: %s" % (DEADLINE.name, reached))


if __name__ == "__main__":
    unittest.main()
