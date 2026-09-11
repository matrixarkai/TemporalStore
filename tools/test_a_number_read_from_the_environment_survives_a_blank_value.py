#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A number read from the environment at import must survive the variable being set to nothing.

``os.environ.get("X", "8")`` returns the default only when X is ABSENT. ``export X=`` sets it to the
empty string, the default does not apply, and ``int("")`` raises. At module scope that is not a bad
value -- it is a failed IMPORT, which takes down every module that imports it too.

Demonstrated before this check existed: ``MATRIXARK_TOP_K_PER_LAYER=`` made matrixark_mcp_core fail
to import, and with it everything built on it. An operator who comments out a value in a deploy
template, or a config generator that writes an empty string for an unset field, produces exactly
that.

The shape this requires is the one the rest of the tree already uses:

    int(os.environ.get("X", "").strip() or "8")

``.strip()`` and not a bare ``or`` because ``float("   ")`` raises too and whitespace is truthy.

What this does NOT require is that a malformed value be swallowed. ``X=abc`` still raises, exactly
as before -- "unset" and "set to nothing" should mean the same thing, and a typo should not.

Only module scope is checked. The same read inside a function raises for one caller, which is a
smaller problem and a different fix.
"""
from __future__ import annotations

import ast
import importlib.util
import os
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent


def _unreachable():
    spec = importlib.util.spec_from_file_location(
        "_reach_for_env", TOOLS / "test_a_module_only_tests_reach_is_not_live.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names = set()
    for members in module.UNREACHABLE.values():
        names.update(members)
    return names


def unsafe_module_scope_reads(tree):
    """int()/float() over os.environ.get(...) at module scope with no blank guard."""
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node

    def enclosing_function(node):
        while node in parents:
            node = parents[node]
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return True
        return False

    found = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in {"int", "float"} and node.args):
            continue
        text = ast.unparse(node)
        if "environ" not in text:
            continue
        if enclosing_function(node):
            continue
        guarded = any(isinstance(sub, ast.BoolOp) and isinstance(sub.op, ast.Or)
                      for sub in ast.walk(node)) or ".strip()" in text
        if not guarded:
            found.append((node.lineno, text))
    return found


def _live_modules():
    unreachable = _unreachable()
    for entry in sorted(os.listdir(TOOLS)):
        if not entry.endswith(".py") or entry.startswith("test_"):
            continue
        if pathlib.Path(entry).stem in unreachable:
            continue
        try:
            yield pathlib.Path(entry).stem, ast.parse((TOOLS / entry).read_text(encoding="utf-8"))
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue


CASES = [
    ("bare default, no guard", True,
     'import os\nX = int(os.environ.get("A", "8"))\n'),
    ("float, bare default", True,
     'import os\nX = float(os.environ.get("A", "0.5"))\n'),
    ("guarded with strip and or", False,
     'import os\nX = int(os.environ.get("A", "").strip() or "8")\n'),
    ("guarded with a bare or", False,
     'import os\nX = int(os.environ.get("A", "0") or 0)\n'),
    ("inside a function: not this check's business", False,
     'import os\ndef f():\n    return int(os.environ.get("A", "8"))\n'),
    ("not an environment read at all", False,
     'X = int("8")\n'),
]


class ANumberReadFromTheEnvironmentSurvivesABlankValue(unittest.TestCase):

    def test_the_scan_reaches_the_tree(self):
        """The floor: no modules scanned would mean no offenders found, for the wrong reason."""
        modules = list(_live_modules())
        self.assertGreater(len(modules), 120,
                           "found %d live modules, expected the whole tools tree" % len(modules))
        reads = sum(
            1 for _stem, tree in modules for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id in {"int", "float"} and node.args
            and "environ" in ast.unparse(node))
        self.assertGreater(
            reads, 100,
            "found only %d int/float reads of the environment in the whole tree -- the scan is "
            "not matching, so a clean result below would mean nothing" % reads)

    def test_the_check_agrees_with_its_worked_examples(self):
        for label, should_flag, source in CASES:
            found = unsafe_module_scope_reads(ast.parse(source))
            self.assertEqual(
                should_flag, bool(found),
                "the blank-value check %s %r" % ("missed" if should_flag else "wrongly flagged",
                                                 label))

    def test_no_module_scope_number_breaks_on_a_blank_value(self):
        offenders = []
        for stem, tree in _live_modules():
            for line, text in unsafe_module_scope_reads(tree):
                offenders.append("%s:%d  %s" % (stem, line, text[:70]))
        self.assertEqual(
            [], sorted(offenders),
            "os.environ.get returns its default only when the name is ABSENT, so `export NAME=` "
            "gives these the empty string and the conversion raises -- at module scope, which is "
            "a failed import rather than a bad value. Use "
            'int(os.environ.get(NAME, "").strip() or DEFAULT): %s' % "; ".join(offenders))


if __name__ == "__main__":
    unittest.main()
