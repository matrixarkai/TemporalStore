#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A signature that promises a value returns one -- and two more shapes this tree has none of.

A function annotated ``-> str`` that can reach the end of its body returns None instead, and the
annotation says it cannot. The caller does not find out at the call: it finds out later, wherever
the None is first used, which is usually somewhere else entirely.

Measured across every live module: ZERO. So is a mutable default argument, and so is a bare
``except:``. All three are cheap to check while a scan is already walking every module, and all
three are at zero today, so each is a plain ratchet with nothing recorded against it.

**A check that asserts "none" has to prove it can find one.** Every check here carries its own
worked examples -- offenders it must flag and safe shapes it must not -- and the examples run on
every invocation. That is not ceremony: the first version of the terminator analysis reported 125
offenders because it treated any trailing if/try as falling through, and the version after it
reported zero because it mishandled ``try/finally`` with no ``except``. Both were wrong, and both
looked plausible. The controls are what tell those apart.
"""
from __future__ import annotations

import ast
import importlib.util
import os
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent


def _unreachable_modules():
    spec = importlib.util.spec_from_file_location(
        "_reach_for_returns", TOOLS / "test_a_module_only_tests_reach_is_not_live.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names = set()
    for members in module.UNREACHABLE.values():
        names.update(members)
    return names


def terminates(stmts) -> bool:
    """True when control cannot reach the end of this statement list."""
    for stmt in stmts:
        if isinstance(stmt, (ast.Return, ast.Raise, ast.Continue, ast.Break)):
            return True
        if isinstance(stmt, ast.If):
            if stmt.orelse and terminates(stmt.body) and terminates(stmt.orelse):
                return True
        elif isinstance(stmt, (ast.With, ast.AsyncWith)):
            if terminates(stmt.body):
                return True
        elif isinstance(stmt, ast.Try):
            if stmt.finalbody and terminates(stmt.finalbody):
                return True
            if not stmt.handlers:
                if terminates(stmt.body):
                    return True
                continue
            arms = [stmt.body if not stmt.orelse else stmt.orelse]
            arms.extend(handler.body for handler in stmt.handlers)
            if all(terminates(arm) for arm in arms):
                return True
        elif isinstance(stmt, ast.While):
            if (isinstance(stmt.test, ast.Constant) and stmt.test.value is True
                    and not any(isinstance(s, ast.Break) for s in ast.walk(stmt))):
                return True
        elif isinstance(stmt, ast.Match):
            wildcard = any(isinstance(case.pattern, ast.MatchAs)
                           and case.pattern.pattern is None and case.guard is None
                           for case in stmt.cases)
            if stmt.cases and wildcard and all(terminates(case.body) for case in stmt.cases):
                return True
    return False


def _excludes_none(annotation) -> bool:
    text = ast.unparse(annotation)
    return not ("None" in text or text.startswith("Optional") or text in {"Any", "NoReturn"})


def promises_a_value_but_can_return_none(node) -> bool:
    if node.returns is None or not _excludes_none(node.returns):
        return False
    if any(isinstance(s, (ast.Yield, ast.YieldFrom)) for s in ast.walk(node)):
        return False          # a generator reaching the end of its body is how it stops
    if not any(isinstance(s, ast.Return) and s.value is not None for s in ast.walk(node)):
        return False          # annotated but never returns anything: a different problem
    return not terminates(node.body)


def mutable_defaults(node):
    out = []
    for default in list(node.args.defaults) + [d for d in node.args.kw_defaults if d]:
        if isinstance(default, (ast.List, ast.Dict, ast.Set)):
            out.append(ast.unparse(default)[:24])
        elif isinstance(default, ast.Call):
            callee = getattr(default.func, "id", None) or getattr(default.func, "attr", "")
            if callee in {"list", "dict", "set"}:
                out.append(callee + "()")
    return out


def _live_modules():
    unreachable = _unreachable_modules()
    for entry in sorted(os.listdir(TOOLS)):
        if not entry.endswith(".py"):
            continue
        stem = pathlib.Path(entry).stem
        if stem.startswith(("test_", "run_", "validate_")) or stem in unreachable:
            continue
        try:
            yield stem, ast.parse((TOOLS / entry).read_text(encoding="utf-8"))
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue


RETURN_CASES = [
    ("an if with no else", True,
     "def f(x) -> str:\n    if x:\n        return 'a'\n"),
    ("a loop that returns only on a match", True,
     "def f(xs) -> int:\n    for x in xs:\n        if x:\n            return x\n"),
    ("if and else both return", False,
     "def f(x) -> str:\n    if x:\n        return 'a'\n    else:\n        return 'b'\n"),
    ("try/finally whose body returns", False,
     "def f(x) -> str:\n    try:\n        return 'a'\n    finally:\n        pass\n"),
    ("try and except both return", False,
     "def f(x) -> str:\n    try:\n        return 'a'\n    except Exception:\n        return 'b'\n"),
    ("a trailing raise", False,
     "def f(x) -> str:\n    if x:\n        return 'a'\n    raise ValueError(x)\n"),
    ("a generator", False,
     "def f(xs) -> 'Iterator[int]':\n    for x in xs:\n        yield x\n"),
    ("annotated Optional", False,
     "def f(x) -> 'str | None':\n    if x:\n        return 'a'\n"),
]


class ASignatureThatPromisesAValueReturnsOne(unittest.TestCase):

    def test_the_scan_reaches_the_tree(self):
        """The floor: a scan that found no modules would report every shape below at zero."""
        modules = list(_live_modules())
        self.assertGreater(len(modules), 120,
                           "found %d live modules, expected the whole tools tree" % len(modules))
        functions = sum(
            1 for _stem, tree in modules for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)))
        self.assertGreater(functions, 2000,
                           "found only %d functions -- the scan is not descending" % functions)

    def test_the_return_check_agrees_with_its_worked_examples(self):
        """The control. Two earlier versions of this analysis were wrong in opposite directions --
        one reported 125 offenders, the next reported zero -- and both looked reasonable."""
        for label, should_flag, source in RETURN_CASES:
            node = ast.parse(source).body[0]
            self.assertEqual(
                should_flag, promises_a_value_but_can_return_none(node),
                "the return check %s %r" % ("missed" if should_flag else "wrongly flagged", label))

    def test_no_function_promises_a_value_and_can_return_none(self):
        offenders = [
            "%s.%s:%d -> %s" % (stem, node.name, node.lineno, ast.unparse(node.returns))
            for stem, tree in _live_modules() for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and promises_a_value_but_can_return_none(node)]
        self.assertEqual(
            [], sorted(offenders),
            "these are annotated to return a value but can reach the end of the body and hand "
            "back None, which the caller finds out about somewhere else: %s" % "; ".join(offenders))

    def test_the_mutable_default_check_agrees_with_its_worked_examples(self):
        flagged = ast.parse("def f(a=[], b={}, *, c=set()):\n    return a\n").body[0]
        self.assertEqual(3, len(mutable_defaults(flagged)),
                         "the mutable-default check does not see a list, a dict and a set")
        safe = ast.parse("def f(a=None, b=(), c=0, d='x'):\n    return a\n").body[0]
        self.assertEqual([], mutable_defaults(safe),
                         "the mutable-default check flags immutable defaults")

    def test_no_mutable_default_argument(self):
        offenders = [
            "%s.%s:%d (%s)" % (stem, node.name, node.lineno, ", ".join(found))
            for stem, tree in _live_modules() for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            for found in [mutable_defaults(node)] if found]
        self.assertEqual(
            [], sorted(offenders),
            "a default argument is built once, at import, and shared by every call -- a mutation "
            "by one caller is seen by the next: %s" % "; ".join(offenders))

    def test_no_bare_except(self):
        offenders = [
            "%s:%d" % (stem, node.lineno)
            for stem, tree in _live_modules() for node in ast.walk(tree)
            if isinstance(node, ast.ExceptHandler) and node.type is None]
        self.assertEqual(
            [], sorted(offenders),
            "a bare except catches KeyboardInterrupt and SystemExit too, so it swallows the "
            "shutdown it was never meant to see: %s" % "; ".join(offenders))


if __name__ == "__main__":
    unittest.main()
