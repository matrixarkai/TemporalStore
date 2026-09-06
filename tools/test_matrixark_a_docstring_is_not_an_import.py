#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A sentence naming a module does not reach it.

`test_a_module_only_tests_reach_is_not_live` builds its import graph from statements AND from words
inside string literals, because ``importlib.import_module("tools.x")`` is a real edge that reading
`import` statements alone would miss. A docstring is a string literal too, so this line, added to a
reachable module while documenting a mirror:

    Mirrors `summary_provider()` in matrixark_mcp_summaries and must keep mirroring it

marked three recorded modules live. Nothing changed about what runs. One sentence did it.

That is the check's own purpose running backwards. It exists because "a copy that is wrong and
unreachable cannot fail today, and is exactly what somebody reaches for tomorrow" -- and a module
that stays hidden because somebody once explained it is the same defect, protected by prose.

**Excluding docstrings cannot lose a real edge**, which is why this is the rule rather than a
looser one: a docstring is never the argument to `import_module`, so no module is reached only by
one. The alternative tried first -- count a string only if the whole string is a dotted path --
stranded thirty-three modules that real string edges reach, and is refused by
`test_a_dynamic_import_string_still_reaches`.

Applying it revealed two modules held live by exactly that sentence, each a second copy of a name
the live tree serves from somewhere else. `TheTwoPackersAreRealTest` checks that from the source
rather than restating it.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)


def _guard():
    """The module this one checks, imported on use.

    Importing it at import time reorders `unittest discover`, which is the thing
    the cross-import guard exists to prevent -- so this module does not do it.
    """
    import test_a_module_only_tests_reach_is_not_live as guard

    return guard


def graph_for(source: str, targets=("matrixark_mcp_core",)) -> set:
    """The edges `_edges` finds out of a one-module tree naming `targets`."""
    modules = {"caller": ("tools/caller.py", ast.parse(source))}
    for name in targets:
        modules[name] = ("tools/%s.py" % name, ast.parse(""))
    return _guard()._edges(modules).get("caller", set())


class ADocstringIsNotAnEdgeTest(unittest.TestCase):

    def test_a_module_docstring_naming_a_module_is_not_an_edge(self) -> None:
        self.assertEqual(set(), graph_for('"""Mirrors matrixark_mcp_core, and must."""\n'))

    def test_a_function_docstring_is_not_either(self) -> None:
        self.assertEqual(set(), graph_for(
            'def f():\n    """Unlike matrixark_mcp_core, this one is cheap."""\n    return 1\n'))

    def test_a_method_docstring_is_not_either(self) -> None:
        """The walk has to reach nested definitions, not just the top level."""
        self.assertEqual(set(), graph_for(
            'class C:\n    def m(self):\n        """See matrixark_mcp_core."""\n        return 1\n'))

    def test_a_dynamic_import_string_still_reaches(self) -> None:
        """The control. Without this the rule could be "drop every string edge", which is the
        change that would break the check rather than fix it."""
        self.assertEqual({"matrixark_mcp_core"}, graph_for(
            'import importlib\nm = importlib.import_module("tools.matrixark_mcp_core")\n'))

    def test_an_ordinary_string_still_reaches(self) -> None:
        """A name in a table, a registry entry, a subprocess argument -- all still edges."""
        self.assertEqual({"matrixark_mcp_core"}, graph_for('BACKENDS = ["matrixark_mcp_core"]\n'))

    def test_a_string_that_is_not_the_first_statement_is_not_a_docstring(self) -> None:
        """A bare string after real code is not documentation and is not treated as such."""
        self.assertEqual({"matrixark_mcp_core"}, graph_for(
            'def f():\n    x = 1\n    "matrixark_mcp_core"\n    return x\n'))

    def test_an_import_statement_is_still_an_edge(self) -> None:
        self.assertEqual({"matrixark_mcp_core"}, graph_for(
            'from tools.matrixark_mcp_core import thing\n'))


class TheTwoPackersAreRealTest(unittest.TestCase):
    """Both were revealed by the rule. Checked here from the source, not asserted from the list."""

    PACKERS = {
        "matrixark_mcp_budget_pack": ("select_token_budgeted_refs",
                                      "matrixark_mcp_core_ref_selection"),
        "matrixark_mcp_dashboard": ("latest_async_pipeline_rows",
                                    "matrixark_mcp_async_readiness"),
    }

    @staticmethod
    def _defines(module: str, function: str) -> bool:
        path = os.path.join(TOOLS, module + ".py")
        with open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        return any(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function
                   for n in tree.body)

    def test_each_is_recorded_as_unreachable(self) -> None:
        recorded = {name for group in _guard().UNREACHABLE.values() for name in group}
        for module in self.PACKERS:
            self.assertIn(module, recorded)

    def test_each_holds_a_copy_of_a_name_a_live_module_also_defines(self) -> None:
        """Not merely unused: a second definition of a name the tree serves elsewhere, which is
        what makes reading one of them as current a real mistake rather than a tidiness point."""
        for module, (function, live) in self.PACKERS.items():
            with self.subTest(module=module):
                self.assertTrue(self._defines(module, function),
                                "%s no longer defines %s" % (module, function))
                self.assertTrue(self._defines(live, function),
                                "%s no longer defines %s, so the pair is gone" % (live, function))

    def test_the_live_copy_is_the_one_production_reaches(self) -> None:
        """The floor for the test above: a duplicate only matters if the OTHER one is live."""
        _library, reachable = _guard().reachable_from_production()
        for module, (_function, live) in self.PACKERS.items():
            with self.subTest(module=module):
                self.assertIn(live, reachable)
                self.assertNotIn(module, reachable)


if __name__ == "__main__":
    unittest.main()
