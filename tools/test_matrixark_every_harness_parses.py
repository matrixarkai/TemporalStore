#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every page harness is valid JavaScript.

Two changes fixed the same broken tests at the same time. Both gave the browse harness's sandbox a
``window``: one lifted the timestamp formatter out of the page, the other ran the whole shared
helper block. They touched different lines, so the merge was clean -- and the file that came out
of it declared ``const win`` twice.

``SyntaxError: Identifier 'win' has already been declared``. Every test in that file, on main,
from a merge that had no conflict to report. Nothing about either change was wrong on its own,
which is the point: a conflict that a diff cannot see needs something that reads the result.

``node --check`` parses a file without running it, so this costs a few milliseconds per harness
and answers before any test that uses one has to. A harness that cannot parse fails every test it
serves, with a stack trace instead of an assertion -- and the failure names the harness, never the
change that broke it.
"""
from __future__ import annotations

import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")


def harnesses() -> list:
    return sorted(f for f in os.listdir(PORTAL) if f.endswith(".js"))


class EveryHarnessParsesTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_each_one_parses(self) -> None:
        for name in harnesses():
            with self.subTest(harness=name):
                out = subprocess.run(["node", "--check", os.path.join(PORTAL, name)],
                                     capture_output=True, text=True, timeout=120)
                self.assertEqual(0, out.returncode,
                                 "%s does not parse:\n%s" % (name, out.stderr.strip()[:500]))

    def test_there_are_harnesses_to_check(self) -> None:
        """The positive control. The assertion above passes perfectly over an empty list, and a
        rename or a moved directory is exactly what would produce one."""
        found = harnesses()
        self.assertGreaterEqual(len(found), 5, "found %s" % (found or "no harness at all"))

    def test_no_harness_declares_the_same_sandbox_global_twice(self) -> None:
        """What actually happened, named rather than left to the parser.

        ``node --check`` already catches a repeated ``const``. This says which mistake it was, so
        the next person merging two fixes for one bug reads the reason instead of a stack trace.
        """
        import io
        import re
        for name in harnesses():
            with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
                source = handle.read()
            # Column zero only. The same name declared inside two different functions is ordinary
            # JavaScript and legal; a first draft allowed any indentation and reported those.
            declared = re.findall(r"^(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=", source, re.M)
            twice = sorted({n for n in declared if declared.count(n) > 1})
            self.assertEqual([], twice,
                             "%s declares %s more than once at the top level"
                             % (name, ", ".join(twice)))


if __name__ == "__main__":
    unittest.main()
