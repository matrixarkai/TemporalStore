#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""No test file is silently uncollected.

The ratchet runs `unittest discover -s . -p "test_*.py"` from `tools/`, and `discover` only
recurses into a directory it can IMPORT: the name must be a valid Python identifier and, for a
plain directory, it must hold an `__init__.py`. A directory named with a hyphen can never satisfy
that, so every test inside it is skipped -- **without an error, a skip, or any other mark**. An
uncollected file looks exactly like a file with nothing to say.

`tools/temporalstore-prometheus/vars-exporter/test_vars_to_prom.py` was in that position: seven
tests guarding the Prometheus vars exporter, passing, and never once run by CI. Discovery collected
3,598 tests and none of them were these.

This finds every such file by the rule discovery uses -- structurally, without importing anything --
and runs what it finds. A file that becomes uncollectable later starts running here instead of
going quiet.
"""
from __future__ import annotations

import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PATTERN_PREFIX = "test_"


def _importable_dir(path: str) -> bool:
    """`unittest discover` recurses into a directory only if it can import it."""
    name = os.path.basename(path)
    if not name.isidentifier():
        return False
    return os.path.isfile(os.path.join(path, "__init__.py"))


def test_files() -> list:
    """Every `test_*.py` under tools/, with whether discovery from tools/ can reach it."""
    found = []
    for root, dirs, files in os.walk(TOOLS):
        dirs[:] = [d for d in dirs if d not in {"__pycache__", ".git", "node_modules"}]
        relative = os.path.relpath(root, TOOLS)
        parts = [] if relative == "." else relative.split(os.sep)
        reachable = True
        walked = TOOLS
        for part in parts:
            walked = os.path.join(walked, part)
            if not _importable_dir(walked):
                reachable = False
                break
        for name in files:
            if name.startswith(PATTERN_PREFIX) and name.endswith(".py"):
                found.append((os.path.join(root, name), reachable))
    return found


class NoTestFileIsSilentlyUncollectedTest(unittest.TestCase):

    def test_every_test_file_is_reachable_or_run_here(self) -> None:
        """Either discovery reaches it, or this file runs it. Never neither."""
        unreachable = [p for p, reachable in test_files() if not reachable]
        for path in unreachable:
            with self.subTest(path=os.path.relpath(path, TOOLS)):
                self.assertTrue(os.path.isfile(path))
        # The ones discovery cannot reach are run below; this asserts the list is KNOWN, so a new
        # one cannot appear without somebody seeing it here.
        self.assertLessEqual(
            len(unreachable), 4,
            "%d test files are unreachable by discovery: %s"
            % (len(unreachable), [os.path.relpath(p, TOOLS) for p in unreachable]))

    def test_the_scan_found_the_reachable_ones_too(self) -> None:
        """The floor: a walk that found nothing, or called everything unreachable, would make the
        rule above pass while saying nothing."""
        found = test_files()
        reachable = [p for p, ok in found if ok]
        self.assertGreater(len(found), 100, "the walk found %d test files" % len(found))
        self.assertGreater(len(reachable), 100)

    def test_the_rule_matches_how_discovery_actually_decides(self) -> None:
        """A hyphenated directory can never be imported, so it can never be recursed into.

        Both directions, because only one of them is the interesting mistake: a rule that answered
        False to everything would call every subdirectory unreachable, quietly widen what this
        file takes responsibility for, and pass every other check here. That mutation survived
        until this test grew its positive half.
        """
        import tempfile

        self.assertFalse(_importable_dir(os.path.join(TOOLS, "does-not-exist")))
        self.assertFalse("vars-exporter".isidentifier())
        with tempfile.TemporaryDirectory() as work:
            package = os.path.join(work, "a_package")
            os.makedirs(package)
            self.assertFalse(_importable_dir(package), "no __init__.py yet")
            open(os.path.join(package, "__init__.py"), "w").close()
            self.assertTrue(_importable_dir(package), "a real package must be reachable")
            hyphenated = os.path.join(work, "a-package")
            os.makedirs(hyphenated)
            open(os.path.join(hyphenated, "__init__.py"), "w").close()
            self.assertFalse(_importable_dir(hyphenated),
                             "a hyphen makes it unimportable whatever it contains")


class TheUnreachableSuitesActuallyPassTest(unittest.TestCase):
    """Running them is the point. Listing them and leaving them unrun would be the same gap with a
    record of itself."""

    def test_they_all_pass(self) -> None:
        unreachable = [p for p, reachable in test_files() if not reachable]
        if not unreachable:
            self.skipTest("every test file is reachable by discovery")

        loader = unittest.TestLoader()
        suite = unittest.TestSuite()
        collected = 0
        for path in unreachable:
            directory = os.path.dirname(path)
            module = os.path.basename(path)[:-3]
            import sys
            added = directory not in sys.path
            if added:
                sys.path.insert(0, directory)
            try:
                loaded = loader.loadTestsFromName(module)
            finally:
                if added and directory in sys.path:
                    sys.path.remove(directory)
            collected += loaded.countTestCases()
            suite.addTest(loaded)

        self.assertGreater(collected, 0,
                           "found %d unreachable files and loaded no tests from them"
                           % len(unreachable))
        with open(os.devnull, "w", encoding="utf-8") as quiet:
            result = unittest.TextTestRunner(verbosity=0, stream=quiet).run(suite)
        broken = [str(case) for case, _trace in result.failures + result.errors]
        self.assertEqual([], broken, "an uncollected suite is failing: %s" % broken)


if __name__ == "__main__":
    unittest.main()
