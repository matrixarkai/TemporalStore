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


def _or_fallback(node: ast.AST) -> object | None:
    """The literal after ``or`` in ``os.environ.get(NAME, "").strip() or "8"``.

    THE SECOND ARGUMENT IS NOT THE DEFAULT IN THIS TREE, and reading it as one made this guard
    inert for 42 of the 43 constants it compares: nearly every constant here is written
    ``int(os.environ.get("MATRIXARK_X", "").strip() or "8")``, so the second argument is the empty
    string on BOTH sides and the agreement assertion was comparing "" with "". Two modules could
    have differed by any amount -- 8 against 99 -- and this file would have passed, which is
    exactly the divergence it was written after. Verified by mutation: injecting a second
    DEFAULT_TOP_K_PER_LAYER of 99 beside a runtime_config of 8 passed before this and fails now.

    The empty second argument is not sloppiness: `os.environ.get(NAME, "").strip() or "8"` treats a
    variable set to whitespace the same as one not set at all, which a bare default does not.
    `test_numeric_defaults_agree` already resolves the same shape; this is that rule applied to the
    two config modules.
    """
    for child in ast.walk(node):
        if isinstance(child, ast.BoolOp) and isinstance(child.op, ast.Or):
            last = child.values[-1]
            if isinstance(last, ast.Constant):
                return last.value
    return None


def _declared_here(path: pathlib.Path) -> dict[str, tuple[str, object]]:
    """Constants this file declares itself: name -> (env var, effective fallback literal)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: dict[str, tuple[str, object]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or not target.id.isupper():
            continue
        call = _env_get_call(node.value)
        if call is None:
            continue
        fallback = call.args[1].value
        if fallback == "":
            through_or = _or_fallback(node.value)
            if through_or is not None:
                fallback = through_or
        found[target.id] = (call.args[0].value, fallback)
    return found


def _imported_from(path: pathlib.Path) -> dict[str, str]:
    """Upper-case names this file gets by importing them, and the module they come from."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        module = node.module.rsplit(".", 1)[-1]
        for alias in node.names:
            if alias.asname is None and alias.name.isupper():
                out[alias.name] = module
    return out


def env_backed_defaults(path: pathlib.Path) -> dict[str, tuple[str, object]]:
    """Map constant name -> (env var, fallback literal), INCLUDING ones reached by import.

    A name a module gets by importing it is that module's constant as far as any reader is
    concerned: `from matrixark_mcp_runtime_config import DEFAULT_TOP_K_PER_LAYER` and a local
    `DEFAULT_TOP_K_PER_LAYER = int(os.environ.get(...))` are indistinguishable at every call site.
    Reading only local declarations made this guard's subject VANISH when the duplicates it exists
    to compare were folded into one definition: the shared set went to zero and the vacuity floor
    below fired -- a floor measured from a population that the improvement removes, which is the
    failure mode the comment on that floor already warns about.

    Following the import does not weaken the agreement question, it settles it: two names that
    resolve to ONE definition cannot disagree, where two kept in step by hand can. What the floor
    now protects is that the extractor still finds them at all.
    """
    found = _declared_here(path)
    for name, module in _imported_from(path).items():
        if name in found:
            continue
        source = path.parent / (module + ".py")
        if not source.exists():
            continue
        declared = _declared_here(source)
        if name in declared:
            found[name] = declared[name]
    return found


class RuntimeConfigAgreesWithCore(unittest.TestCase):
    def setUp(self) -> None:
        self.core = env_backed_defaults(TOOLS / "matrixark_mcp_core.py")
        self.runtime = env_backed_defaults(TOOLS / "matrixark_mcp_runtime_config.py")

    def test_both_modules_were_actually_parsed(self) -> None:
        # Guard the guard: an extractor that silently matched nothing would make
        # the agreement assertion below vacuously true.
        #
        # The numbers say what they are FOR, not what the two modules currently hold. They were 50
        # and the runtime module held 51, so folding away flags nothing sets -- a change that
        # removes env-backed constants on purpose and alters no value -- took the count to exactly
        # 50 and failed here. A vacuity floor pinned to a measurement tracks the tree instead of
        # the property: an extractor that stopped matching returns approximately nothing, and 20
        # fails loudly on that while surviving any legitimate move in the count.
        self.assertGreater(len(self.core), 20, "core parse found too few constants")
        self.assertGreater(len(self.runtime), 20, "runtime_config parse found too few")
        shared = set(self.core) & set(self.runtime)
        self.assertGreater(
            len(shared),
            20,
            "expected a large shared constant set between the two modules",
        )
        # The shared set is now shared by IMPORT rather than by duplication, so this asserts the
        # mechanism that makes it so. Without it the extractor could quietly go back to reading
        # local declarations only, the shared set would collapse to whatever duplication remains,
        # and the agreement assertion would be true of almost nothing.
        # The comparison must be about real values. It was not: reading the second argument of
        # os.environ.get gave "" for 42 of these 43, so the agreement below held between two empty
        # strings. A count of how many carry a real literal is the thing to hold.
        with_a_value = [n for n in shared if self.core[n][1] != ""]
        self.assertGreater(
            len(with_a_value), 20,
            "only %d of %d shared constants resolve to a real fallback literal. The rest compare "
            "\"\" against \"\", which is an agreement assertion that cannot fail -- see "
            "_or_fallback." % (len(with_a_value), len(shared)))
        imported = _imported_from(TOOLS / "matrixark_mcp_core.py")
        self.assertGreater(
            len(set(imported) & shared), 20,
            "at most %d of the shared constants reach matrixark_mcp_core by import. They were "
            "folded into one definition on purpose; if they are being counted as shared because "
            "they are DUPLICATED again, that is the divergence this file exists to prevent."
            % len(set(imported) & shared))

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
