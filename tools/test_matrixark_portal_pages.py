#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The committed portal pages must be what the generator emits.

Five of the seven pages are generated so they share one stylesheet and one nav. That only holds if
nobody hand-edits the output: an edit made directly to setup_portal.html survives until the next
person runs the generator, and then vanishes, which is a confusing way to lose work.

Run in a copy of the directory so the test never rewrites the tree it is checking.
"""
from __future__ import annotations

import filecmp
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
GENERATOR = "build_portal_pages.py"
def _written_by(call: str) -> tuple:
    """The pages `main()` names, read out of the generator instead of copied beside it.

    These were two tuples, and both were two pages behind: the one-box and mem0 pages were added to
    `main()` and not here, so the check that nobody hand-edits a generated page had stopped
    covering the two newest generated pages. A list that has to be kept in step by hand is a
    denominator that goes stale quietly, because nothing fails when it does.
    """
    with io.open(os.path.join(PORTAL, GENERATOR), encoding="utf-8") as handle:
        text = handle.read()
    body = text[text.index("def main() -> None:"):]
    return tuple(sorted(set(re.findall(r'%s\(\s*"([a-z0-9_]+_portal\.html)"' % call, body))))


GENERATED = _written_by("emit")
# Hand-maintained; the generator only refreshes the nav block inside them.
HAND_WRITTEN = _written_by("inject")


class GeneratedPagesTest(unittest.TestCase):
    def test_the_two_lists_account_for_every_shipped_page(self) -> None:
        """The floor, on the DENOMINATOR, which is what was wrong here.

        Reading the generator cannot notice a page it stopped writing, and the check below passes
        perfectly over a short list. Every page on disk has to be in one list or the other.
        """
        shipped = {name for name in os.listdir(PORTAL) if name.endswith("_portal.html")}
        self.assertGreaterEqual(len(shipped), 9, "only %d pages on disk" % len(shipped))
        self.assertEqual(shipped, set(GENERATED) | set(HAND_WRITTEN),
                         "generated=%s injected=%s shipped=%s"
                         % (GENERATED, HAND_WRITTEN, sorted(shipped)))
        self.assertEqual(set(), set(GENERATED) & set(HAND_WRITTEN),
                         "a page cannot be both written and only injected into")

    def test_the_committed_pages_match_what_the_generator_emits(self) -> None:
        with tempfile.TemporaryDirectory() as work:
            copy = os.path.join(work, "portal")
            shutil.copytree(PORTAL, copy)
            result = subprocess.run([sys.executable, os.path.join(copy, GENERATOR)],
                                    capture_output=True, text=True, cwd=work)
            self.assertEqual(0, result.returncode,
                             "the generator failed:\n%s%s" % (result.stdout, result.stderr))
            stale = [name for name in GENERATED + HAND_WRITTEN
                     if not filecmp.cmp(os.path.join(PORTAL, name),
                                        os.path.join(copy, name), shallow=False)]
            self.assertEqual([], stale,
                             "hand-edited, or generated from an older script: %s. Change "
                             "%s and re-run it instead." % (", ".join(stale), GENERATOR))

    def test_a_failure_while_rendering_leaves_the_page_intact(self) -> None:
        """`io.open(path, "w").write(render())` truncates before render() runs, so a failure in
        there destroyed the page rather than declining to update it. These two are hand-maintained
        and not reproducible from the generator, so an empty one is lost work."""
        with tempfile.TemporaryDirectory() as work:
            copy = os.path.join(work, "portal")
            shutil.copytree(PORTAL, copy)
            generator = os.path.join(copy, GENERATOR)
            with open(generator, encoding="utf-8") as handle:
                text = handle.read()
            broken = text.replace(
                "def _with_nav_js(text):",
                "def _with_nav_js(text):" + chr(10) + '    raise ValueError("rendering failed")',
                1)
            self.assertNotEqual(text, broken, "could not find the function to break")
            with open(generator, "w", encoding="utf-8", newline=chr(10)) as handle:
                handle.write(broken)

            result = subprocess.run([sys.executable, generator],
                                    capture_output=True, text=True, cwd=work)
            self.assertNotEqual(0, result.returncode,
                                "the generator was supposed to fail here:" + result.stdout)
            for name in HAND_WRITTEN:
                emitted = os.path.join(copy, name)
                self.assertGreater(os.path.getsize(emitted), 0,
                                   "%s was emptied by a build that failed" % name)
                self.assertTrue(filecmp.cmp(os.path.join(PORTAL, name), emitted, shallow=False),
                                "%s was modified by a build that failed" % name)

    def test_running_the_generator_twice_changes_nothing(self) -> None:
        """The nav and tab scripts are replaced in place on the hand-maintained pages, and finding
        the block to replace used to depend on how long that block was."""
        with tempfile.TemporaryDirectory() as work:
            copy = os.path.join(work, "portal")
            shutil.copytree(PORTAL, copy)
            for run in (1, 2):
                result = subprocess.run([sys.executable, os.path.join(copy, GENERATOR)],
                                        capture_output=True, text=True, cwd=work)
                self.assertEqual(0, result.returncode,
                                 "build %d failed: %s%s" % (run, result.stdout, result.stderr))
            drifted = [name for name in GENERATED + HAND_WRITTEN
                       if not filecmp.cmp(os.path.join(PORTAL, name),
                                          os.path.join(copy, name), shallow=False)]
            self.assertEqual([], drifted,
                             "a second build changed: %s -- the injected blocks are stacking or "
                             "being placed differently each time" % ", ".join(drifted))

    def test_the_generator_writes_only_inside_its_own_directory(self) -> None:
        # It resolves its output from __file__, so a copy of the directory is a complete sandbox.
        # If that ever became an absolute path again, the test above would silently be checking the
        # real tree against itself and could never fail.
        with io.open(os.path.join(PORTAL, GENERATOR), encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("PORTAL = os.path.dirname(os.path.abspath(__file__))", source)
        self.assertNotIn("/root/", source)


if __name__ == "__main__":
    unittest.main()
