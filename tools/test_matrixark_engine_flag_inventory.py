#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The engine flag inventory matches the engine.

98 `TS_*` variables is a lot, and the first useful thing anyone can do with that number is see the
list grouped by what each flag decides. A hand-kept list of 98 is wrong within a week, and wrong
quietly -- so the document is generated and this regenerates it and compares.

The failure message says how to fix it, because a byte-comparison failure with no instruction is
the kind of test people delete.
"""
from __future__ import annotations

import io
import os
import subprocess
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS)
BUILDER = os.path.join(TOOLS, "build_engine_flag_inventory.py")
DOC = os.path.join(ROOT, "docs", "ops", "temporalstore-engine-flags.md")


def _regenerate_into(directory: str) -> str:
    """Run the builder against a tree whose doc lives in `directory`, and return what it wrote."""
    os.makedirs(os.path.join(directory, "docs", "ops"), exist_ok=True)
    # the builder reads the engine source from the tree it is given, so point it at the real one
    # by symlinking rather than copying a large directory
    link = os.path.join(directory, "crates")
    if not os.path.exists(link):
        try:
            os.symlink(os.path.join(ROOT, "crates"), link)
        except OSError:
            return ""
    tools_link = os.path.join(directory, "tools")
    if not os.path.exists(tools_link):
        try:
            os.symlink(TOOLS, tools_link)
        except OSError:
            pass
    subprocess.run([sys.executable, BUILDER, directory],
                   capture_output=True, text=True, timeout=300, check=False)
    written = os.path.join(directory, "docs", "ops", "temporalstore-engine-flags.md")
    if not os.path.exists(written):
        return ""
    with io.open(written, encoding="utf-8") as handle:
        return handle.read()


class TheInventoryMatchesTheEngineTest(unittest.TestCase):

    def test_the_document_exists(self) -> None:
        self.assertTrue(os.path.exists(DOC),
                        "the flag inventory is missing; run tools/build_engine_flag_inventory.py")

    def test_regenerating_produces_the_same_document(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fresh = _regenerate_into(directory)
        if not fresh:
            self.skipTest("could not regenerate (symlinks unavailable on this filesystem)")
        with io.open(DOC, encoding="utf-8") as handle:
            current = handle.read()
        self.assertEqual(
            fresh, current,
            "the flag inventory no longer matches the engine. A flag was added, removed or "
            "renamed. Regenerate it: python3 tools/build_engine_flag_inventory.py .")

    def test_it_covers_a_plausible_number_of_flags(self) -> None:
        """A generator that produced an empty table would satisfy the comparison above."""
        with io.open(DOC, encoding="utf-8") as handle:
            text = handle.read()
        rows = [line for line in text.splitlines() if line.startswith("| `TS_")]
        self.assertGreaterEqual(
            len(rows), 60,
            "the inventory lists only %d flags; the engine reads far more, so the generator has "
            "stopped seeing them" % len(rows))

    def test_every_portal_engine_setting_appears(self) -> None:
        """The 16 a customer can set must be in the list a reader consults."""
        sys.path.insert(0, TOOLS)
        import matrixark_gateway_config as cfgmod

        with io.open(DOC, encoding="utf-8") as handle:
            text = handle.read()
        missing = [s.env for s in cfgmod.SETTINGS
                   if s.env.startswith("TS_") and ("`%s`" % s.env) not in text]
        self.assertEqual(
            [], missing,
            "these are offered on the portal and absent from the flag inventory, so the one "
            "document that lists engine knobs does not list the ones a customer can change: %s"
            % ", ".join(missing))


class TheDefaultRootIsThisRepositoryTest(unittest.TestCase):
    """Run with no argument, the builder must regenerate THIS checkout.

    It used to default to an absolute path naming one worktree on one machine, so running it
    anywhere else rewrote the document from a tree the caller had never heard of. Every test here
    passes a directory explicitly, which is why the default went unexercised -- so these assert on
    the default itself rather than through a run.
    """

    def setUp(self) -> None:
        with io.open(BUILDER, encoding="utf-8") as handle:
            self.source = handle.read()

    def test_no_absolute_path_to_somebody_checkout(self) -> None:
        for bad in ('"/root/', "'/root/", '"/home/', "'/home/", '"C:'):
            self.assertNotIn(bad, self.source,
                             "the builder names an absolute path on one machine, so it does not "
                             "regenerate the repository it was run from")

    def test_the_default_follows_the_script(self) -> None:
        line = [l for l in self.source.splitlines() if l.startswith("ROOT = ")]
        self.assertTrue(line, "ROOT is no longer assigned where this test can see it")
        window = self.source[self.source.index("ROOT = "):]
        window = window[:window.index("SRC =")]
        self.assertIn("__file__", window,
                      "the default root is not derived from the script location, so it depends on "
                      "where the caller happens to be")


if __name__ == "__main__":
    unittest.main()
