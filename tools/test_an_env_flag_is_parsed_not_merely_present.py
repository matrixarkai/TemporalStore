#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A flag that MEANS a boolean is parsed, not merely tested for presence.

`os.environ.get("X")` returns a string, and every non-empty string is truthy. So

    if os.environ.get("MATRIXARK_SOMETHING"):

turns the feature ON for `MATRIXARK_SOMETHING=false`, `=0` and `=off` -- the three spellings an
operator reaches for to turn it OFF. The tree has parsers for exactly this: `flag_bool` and
`env_bool` in `matrixark_mcp_env`, and three module-local `_env_bool`s.

## Truthiness is usually RIGHT, which is why this cannot be a blanket rule

Most env reads in this tree are presence checks -- a path, a command, a prefix, an override:

    if os.environ.get("TEMPORALSTORE_TEST_CORPUS"):    # "did the operator supply one?"

That is correct and there are 20 of them. A guard failing on those would be noise, and noise is
how a guard gets an exemption list, which is a hiding place.

So the subject is narrower and it is an INCONSISTENCY rather than a style: a variable that this
repository ELSEWHERE treats as a boolean -- declares as `Knob(..., "bool", ...)`, or hands to one
of the bool parsers -- and HERE decides by truthiness. One of the two readings is wrong about
what the operator typed, and the truthy one is wrong in the direction that ignores "false".

## What it reads

The scan follows one local-variable hop inside a function, because that is how the code is
actually written:

    raw = os.environ.get("MATRIXARK_SHARE_SERVING_VALUES")
    if raw:                                            # <- the defect, one line later

A detector that only saw the inline form would be easier than the defect, and would report zero
on a tree that had it. The control below plants exactly that two-line shape.
"""
from __future__ import annotations

import ast
import io
import os
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: Anything that turns a string into a bool by reading what it SAYS.
BOOL_PARSERS = ("flag_bool", "env_bool", "_env_bool", "parse_bool", "as_bool", "to_bool")

#: Floors on the two scans, not on the finding. The first catches a scan that stopped matching
#: env reads; the second a scan that stopped finding the declarations that do the discriminating.
#: 20 and 19 respectively today.
MINIMUM_TRUTHINESS_READS = 8
MINIMUM_BOOLEAN_NAMES = 12


def _env_name_read(node) -> str | None:
    """The variable name if this expression is `os.environ.get(...)` / `os.getenv(...)`."""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Attribute):
        if not (func.attr == "getenv"
                or (func.attr == "get" and "environ" in ast.unparse(func.value))):
            return None
    elif isinstance(func, ast.Name):
        if func.id != "getenv":
            return None
    else:
        return None
    for arg in node.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
    return "?"


def _parsed(node) -> bool:
    text = ast.unparse(node)
    return any(parser in text for parser in BOOL_PARSERS)


def _production_files(root: str):
    return sorted(f for f in os.listdir(root)
                  if f.endswith(".py") and not f.startswith("test_"))


def truthiness_reads(root: str, files):
    """(file, line, env name) for env reads decided by truthiness, inline or one hop."""
    found = set()
    for rel in files:
        try:
            with io.open(os.path.join(root, rel), encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (SyntaxError, OSError):
            continue
        for scope in ast.walk(tree):
            if not isinstance(scope, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            single, rebound = {}, set()
            for node in ast.walk(scope):
                if not (isinstance(node, ast.Assign) and len(node.targets) == 1
                        and isinstance(node.targets[0], ast.Name)):
                    continue
                target = node.targets[0].id
                name = _env_name_read(node.value)
                if name and not _parsed(node.value):
                    if target in single:
                        rebound.add(target)
                    single[target] = name
                elif target in single:
                    # Reassigned from something else; the hop no longer says what it holds.
                    rebound.add(target)
            for target in rebound:
                single.pop(target, None)

            def decided_by(test, line):
                inline = _env_name_read(test)
                if inline and not _parsed(test):
                    found.add((rel, line, inline))
                elif isinstance(test, ast.Name) and test.id in single:
                    found.add((rel, line, single[test.id]))
                elif (isinstance(test, ast.Call) and isinstance(test.func, ast.Name)
                      and test.func.id == "bool" and test.args):
                    decided_by(test.args[0], line)

            for node in ast.walk(scope):
                if isinstance(node, (ast.If, ast.While, ast.IfExp)):
                    decided_by(node.test, node.lineno)
                elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                      and node.func.id == "bool" and node.args):
                    decided_by(node.args[0], node.lineno)
    return sorted(found)


def names_that_mean_a_boolean(root: str, files):
    """Env names this repository treats as booleans somewhere.

    Two sources, both read from the source rather than listed here: a `Knob(..., "bool", ENV)`
    declaration, and a literal env name handed to one of the bool parsers.
    """
    names = set()
    for rel in files:
        try:
            with io.open(os.path.join(root, rel), encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (SyntaxError, OSError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            label = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            literals = [a.value for a in node.args
                        if isinstance(a, ast.Constant) and isinstance(a.value, str)]
            if label == "Knob" and len(literals) >= 3 and literals[1] == "bool":
                names.add(literals[2])
            elif label in BOOL_PARSERS:
                names.update(v for v in literals if v.isupper())
    return names


def inconsistent(root: str, files):
    """Names read as a boolean somewhere and decided by truthiness here."""
    boolean = names_that_mean_a_boolean(root, files)
    return [(rel, line, name) for rel, line, name in truthiness_reads(root, files)
            if name in boolean]


class AnEnvFlagIsParsedNotMerelyPresentTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.files = _production_files(TOOLS)
        cls.reads = truthiness_reads(TOOLS, cls.files)
        cls.boolean = names_that_mean_a_boolean(TOOLS, cls.files)

    def test_the_truthiness_scan_still_finds_env_reads(self) -> None:
        """Vacuity floor on the SCAN. Most of what it finds is legitimate; that is the point --
        if it finds nothing, the check below is comparing an empty set against the declarations."""
        self.assertGreater(
            len(self.reads), MINIMUM_TRUTHINESS_READS,
            "only %d env reads decided by truthiness, below the floor of %d; the scan has "
            "stopped matching the shape" % (len(self.reads), MINIMUM_TRUTHINESS_READS))

    def test_the_declarations_that_discriminate_are_still_found(self) -> None:
        """The other half. Without these every truthiness read looks innocent."""
        self.assertGreater(
            len(self.boolean), MINIMUM_BOOLEAN_NAMES,
            "only %d env names are recognisable as booleans, below the floor of %d; the Knob "
            "declaration or the parser names have changed shape, and this check would pass on a "
            "tree full of the defect" % (len(self.boolean), MINIMUM_BOOLEAN_NAMES))

    def test_no_boolean_flag_is_decided_by_truthiness(self) -> None:
        bad = [(rel, line, name) for rel, line, name in self.reads if name in self.boolean]
        self.assertEqual(
            [], bad,
            "this variable is treated as a boolean elsewhere -- declared `Knob(..., \"bool\", "
            "...)` or handed to a bool parser -- and decided by truthiness here, so setting it "
            "to \"false\", \"0\" or \"off\" turns the feature ON. Read it through `flag_bool` or "
            "`env_bool` in matrixark_mcp_env:\n  "
            + "\n  ".join("%s:%d %s" % row for row in bad))

    def test_the_scan_catches_the_two_line_shape_and_spares_a_presence_check(self) -> None:
        """The control, through the same functions, on the shape the defect actually takes.

        An inline-only detector would report zero here, so this plants the local hop. The
        presence check beside it must stay quiet, because that is what makes the rule usable.
        """
        with tempfile.TemporaryDirectory() as root:
            def write(name: str, body: str) -> None:
                with io.open(os.path.join(root, name), "w", encoding="utf-8") as handle:
                    handle.write(body)

            write("registry.py",
                  'KNOBS = [Knob("a_switch", "bool", "MATRIXARK_A_SWITCH", False, "doc"),\n'
                  '         Knob("a_size", "int", "MATRIXARK_A_SIZE", 8, "doc")]\n')
            write("defect.py",
                  "import os\n\n\ndef go():\n"
                  '    raw = os.environ.get("MATRIXARK_A_SWITCH")\n'
                  "    if raw:\n        return True\n    return False\n")
            write("fine_presence_check.py",
                  "import os\n\n\ndef go():\n"
                  '    corpus = os.environ.get("MATRIXARK_A_CORPUS")\n'
                  "    if corpus:\n        return corpus\n    return None\n")
            write("fine_parsed.py",
                  "import os\n\n\ndef go():\n"
                  '    return flag_bool(os.environ.get("MATRIXARK_A_SWITCH"), False)\n')

            files = _production_files(root)
            boolean = names_that_mean_a_boolean(root, files)
            self.assertIn("MATRIXARK_A_SWITCH", boolean)
            self.assertNotIn("MATRIXARK_A_SIZE", boolean, "an int knob is not a boolean")

            bad = inconsistent(root, files)
            self.assertEqual(1, len(bad), bad)
            self.assertEqual("defect.py", bad[0][0])
            self.assertEqual("MATRIXARK_A_SWITCH", bad[0][2])

            # The presence check is seen by the scan and cleared by the discriminator; that
            # division is the whole design, so assert both halves rather than only the verdict.
            seen = {(rel, name) for rel, _, name in truthiness_reads(root, files)}
            self.assertIn(("fine_presence_check.py", "MATRIXARK_A_CORPUS"), seen)
            self.assertNotIn("MATRIXARK_A_CORPUS", boolean)


if __name__ == "__main__":
    unittest.main()
