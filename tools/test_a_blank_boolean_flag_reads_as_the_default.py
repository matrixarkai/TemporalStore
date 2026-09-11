#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A blank boolean flag reads as the default, the way env_bool already reads it.

env_bool is this tree's canonical reader, and it is explicit: "" is in neither TRUE_VALUES nor
FALSE_VALUES, so a blank value returns the DEFAULT. Six hand-rolled reads disagreed with it -- five
carried "" in their own falsey set, and one tested membership of the TRUE set -- so `export X=`
turned a default-ON flag OFF with no error and no log. Three of the five sat eight lines from
INDEX_SKIP_OWNER_DERIVABLE_TERMS, which already omitted "" and therefore already behaved.

The numeric version of this is loud: int("") raises. The boolean version is silent, which is worse,
because the operator who cleared a field to "leave it at the default" gets the opposite of it.

**Evaluated, not pattern-matched.** The first scan I wrote for this flagged sixteen reads and was
wrong about ten, because `not in {"0","false","no","off"}` is blank-SAFE by construction and a scan
that does not know that reports the tree rather than the defect. So each comparison is evaluated
twice -- once with the default substituted for the environment read, once with "" -- and only a
disagreement counts.
"""
from __future__ import annotations

import ast
import os
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent


def _environ_get(node):
    """The os.environ.get(...) call inside this expression, if there is exactly one shape of it."""
    for sub in ast.walk(node):
        if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                and sub.func.attr == "get"
                and isinstance(sub.func.value, ast.Attribute)
                and sub.func.value.attr == "environ"
                and sub.args and isinstance(sub.args[0], ast.Constant)):
            return sub
    return None


def blank_disagrees_with_absent(node):
    """(name, absent, blank) when a blank value answers differently, else None."""
    getter = _environ_get(node)
    if getter is None:
        return None
    if not (len(getter.args) >= 2 and isinstance(getter.args[1], ast.Constant)
            and isinstance(getter.args[1].value, str)):
        return None
    default = getter.args[1].value
    expression = ast.unparse(node)
    getter_source = ast.unparse(getter)
    if getter_source not in expression:
        return None
    try:
        absent = bool(eval(expression.replace(getter_source, repr(default)),
                           {"__builtins__": {}}, {}))
        blank = bool(eval(expression.replace(getter_source, repr("")),
                          {"__builtins__": {}}, {}))
    except Exception:
        return None
    if absent == blank:
        return None
    return getter.args[0].value, absent, blank


def _comparisons():
    for entry in sorted(os.listdir(TOOLS)):
        if not entry.endswith(".py") or entry.startswith("test_"):
            continue
        try:
            tree = ast.parse((TOOLS / entry).read_text(encoding="utf-8"))
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare) and _environ_get(node) is not None:
                yield pathlib.Path(entry).stem, node


CASES = [
    ("blank in the falsey set flips a default-ON flag", True,
     'import os\nX = os.environ.get("A", "1").strip().lower() '
     'not in {"0", "false", "no", "off", ""}\n'),
    ("membership of the TRUE set flips a default-ON flag", True,
     'import os\nX = os.environ.get("A", "1").strip().lower() in {"1", "true", "yes", "on"}\n'),
    ("the plain falsey set is blank-safe", False,
     'import os\nX = os.environ.get("A", "1").strip().lower() not in {"0", "false", "no", "off"}\n'),
    ("a default-OFF flag cannot be flipped by a blank", False,
     'import os\nX = os.environ.get("A", "0").strip().lower() in {"1", "true", "yes", "on"}\n'),
    ("not equal to zero is blank-safe", False,
     'import os\nX = os.environ.get("A", "1") != "0"\n'),
]


class ABlankBooleanFlagReadsAsTheDefault(unittest.TestCase):

    def test_the_scan_finds_the_comparisons(self):
        """The floor: no comparisons found would report a clean tree for the wrong reason."""
        found = list(_comparisons())
        self.assertGreater(
            len(found), 20,
            "found %d boolean comparisons over os.environ.get in the whole tree -- the scan "
            "stopped matching, so the check below proves nothing" % len(found))

    def test_the_check_agrees_with_its_worked_examples(self):
        """The control, and it has already earned its place: the pattern-matching version of this
        check called ten blank-safe reads defective."""
        for label, should_flag, source in CASES:
            node = next(n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Compare))
            self.assertEqual(
                should_flag, blank_disagrees_with_absent(node) is not None,
                "the blank-boolean check %s %r" % ("missed" if should_flag else "wrongly flagged",
                                                   label))

    def test_no_blank_value_flips_a_flag(self):
        offenders = []
        for stem, node in _comparisons():
            verdict = blank_disagrees_with_absent(node)
            if verdict is not None:
                name, absent, blank = verdict
                offenders.append("%s:%d %s (absent=%s, blank=%s)"
                                 % (stem, node.lineno, name, absent, blank))
        self.assertEqual(
            [], sorted(offenders),
            "env_bool returns the DEFAULT for a blank value because \"\" is in neither "
            "TRUE_VALUES nor FALSE_VALUES. These read it differently, so clearing the variable "
            "silently changes the flag rather than leaving it alone: %s" % "; ".join(offenders))


if __name__ == "__main__":
    unittest.main()
