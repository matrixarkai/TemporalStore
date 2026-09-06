#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every env-backed constant defined in BOTH config modules must agree.

``matrixark_mcp_core`` and ``matrixark_mcp_runtime_config`` each define a large
set of ``os.environ.get(NAME, LITERAL)`` constants, and most of those names are
defined in both files. When the two fallback literals differ, the effective
default depends on which module the calling code happens to import from -- an
operator who sets nothing gets one ceiling on one path and a different ceiling
on another. ``DEFAULT_MAX_CONTEXT_TOKENS`` hit exactly this (128000 vs 500000)
and was fixed by having one module import the other's value; eleven further
constants had drifted the same way, which is what this test now pins.

Two deliberate choices:

* The comparison is STATIC (source parsing, not import). These modules take part
  in a circular import graph, so importing them here would make the test's own
  result depend on import order -- the very thing it is checking.
* It parses with ``ast``, not a line regex. Several of these constants are
  written wrapped across lines, and a line-oriented matcher silently skips
  exactly those -- which is how one of the divergences stayed hidden.
"""

from __future__ import annotations

import ast
import pathlib
import os
import unittest


TOOLS = pathlib.Path(__file__).resolve().parent


def _env_get_call(node: ast.AST) -> ast.Call | None:
    """The ``os.environ.get("ENV", "DEFAULT")`` call inside an assignment, if any.

    Looks through wrappers such as ``int(...)``, ``float(...)`` and trailing
    ``.strip().lower()`` chains.
    """
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        func = child.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "get"
            and isinstance(func.value, ast.Attribute)
            and func.value.attr == "environ"
            and len(child.args) == 2
            and isinstance(child.args[0], ast.Constant)
            and isinstance(child.args[1], ast.Constant)
        ):
            return child
    return None


def env_backed_defaults(path: pathlib.Path) -> dict[str, tuple[str, object]]:
    """Map module-level constant name -> (env var, fallback literal)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: dict[str, tuple[str, object]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or not target.id.isupper():
            continue
        call = _env_get_call(node.value)
        if call is not None:
            found[target.id] = (call.args[0].value, call.args[1].value)
    return found


class RuntimeConfigAgreesWithCore(unittest.TestCase):
    def setUp(self) -> None:
        self.core = env_backed_defaults(TOOLS / "matrixark_mcp_core.py")
        self.runtime = env_backed_defaults(TOOLS / "matrixark_mcp_runtime_config.py")

    def test_both_modules_were_actually_parsed(self) -> None:
        # Guard the guard: an extractor that silently matched nothing would make
        # the agreement assertion below vacuously true.
        self.assertGreater(len(self.core), 50, "core parse found too few constants")
        self.assertGreater(len(self.runtime), 50, "runtime_config parse found too few")
        self.assertGreater(
            len(set(self.core) & set(self.runtime)),
            50,
            "expected a large shared constant set between the two modules",
        )

    def test_wrapped_definitions_are_covered(self) -> None:
        # A constant written across several lines: the line-based matcher this replaced skipped
        # exactly that shape, and a regression to line matching must fail here rather than pass
        # quietly.
        #
        # The example is FOUND rather than named. It used to name
        # DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS, and when that definition was collapsed
        # onto one line this floor failed for a reason that had nothing to do with the parser --
        # a floor pinned to one example breaks whenever the example moves.
        import re

        with open(os.path.join(TOOLS, "matrixark_mcp_core.py"), encoding="utf-8") as handle:
            source = handle.read()
        wrapped = re.findall(r"^([A-Z_][A-Z0-9_]*) = \w+\(\s*$", source, re.M)
        self.assertTrue(wrapped, "no constant is written across lines any more; this floor is inert")
        for name in wrapped:
            with self.subTest(constant=name):
                self.assertIn(name, self.core,
                              "multi-line constant definitions are not being parsed")

    def test_shared_constants_have_identical_fallback_defaults(self) -> None:
        divergent = []
        for name in sorted(set(self.core) & set(self.runtime)):
            core_env, core_default = self.core[name]
            run_env, run_default = self.runtime[name]
            if core_env != run_env or core_default != run_default:
                divergent.append(
                    "  {name}\n"
                    "    matrixark_mcp_core          : {ce}={cd!r}\n"
                    "    matrixark_mcp_runtime_config: {re_}={rd!r}".format(
                        name=name, ce=core_env, cd=core_default, re_=run_env, rd=run_default
                    )
                )
        self.assertEqual(
            divergent,
            [],
            "these constants resolve to different defaults depending on which "
            "module the caller imports from:\n" + "\n".join(divergent),
        )


if __name__ == "__main__":
    unittest.main()
