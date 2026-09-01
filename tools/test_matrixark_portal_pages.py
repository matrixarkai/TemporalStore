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
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
GENERATOR = "build_portal_pages.py"
GENERATED = ("overview_portal.html", "api_portal.html", "explore_portal.html",
             "setup_portal.html", "catalog_portal.html")
# Hand-maintained; the generator only refreshes the nav block inside them.
HAND_WRITTEN = ("api_key_portal.html", "ingestion_portal.html")


class GeneratedPagesTest(unittest.TestCase):
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

    def test_the_generator_writes_only_inside_its_own_directory(self) -> None:
        # It resolves its output from __file__, so a copy of the directory is a complete sandbox.
        # If that ever became an absolute path again, the test above would silently be checking the
        # real tree against itself and could never fail.
        source = open(os.path.join(PORTAL, GENERATOR), encoding="utf-8").read()
        self.assertIn("PORTAL = os.path.dirname(os.path.abspath(__file__))", source)
        self.assertNotIn("/root/", source)


if __name__ == "__main__":
    unittest.main()
