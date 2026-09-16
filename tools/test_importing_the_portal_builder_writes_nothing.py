#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Importing the page builder used to write nine files.

`tools/portal/build_portal_pages.py` did its work at MODULE SCOPE: seven `emit(...)` calls that each
write an HTML file, then two `inject(...)` calls that rewrite two more. So `import build_portal_pages`
rewrote nine tracked files, and because `inject` calls `sys.exit(1)` when its anchor is missing, an
import could end the importing process rather than fail one test.

Nothing in this tree imported it -- both existing tests run it with
`subprocess.run([sys.executable, "build_portal_pages.py"])` -- so nothing was broken. The reason it
matters is that this repository is public and people fork it. A stranger's editor, linter, type
checker or `pytest --collect-only` imports every module it finds, and this one answered by
rewriting nine tracked files in their checkout.

The work now lives in `main()` behind a `__main__` guard, which leaves both existing callers
unchanged because running the file still executes as `__main__`.

ASSERTED IN BOTH DIRECTIONS, because only one of them is the interesting failure:

  * importing it must write NOTHING -- the property this change created;
  * running it must still write the pages -- if a future tidy-up leaves the module importable and
    inert but stops it building, every page silently stops being regenerated and the first test
    would still pass. A guard for "no side effects" that does not also check the tool still works
    is a guard that rewards deleting the tool.

BOTH RUN IN A COPY. The builder writes in place; a test that ran it in the working tree would
rewrite tracked files to prove that it rewrites tracked files.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

#: This file lives in tools/, and the builder lives in tools/portal/.
PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
BUILDER = "build_portal_pages.py"

#: The two pages `inject` edits IN PLACE rather than generating. They must keep their anchors --
#: `inject` exits 1 without them, which is the tool being right, not broken.
INJECT_TARGETS = ("ingestion_portal.html", "api_key_portal.html")


def _fingerprint(directory):
    """name -> sha256 for every file in the directory, so a rewrite is visible even if same-size."""
    out = {}
    for root, _dirs, names in os.walk(directory):
        for name in sorted(names):
            path = os.path.join(root, name)
            with open(path, "rb") as handle:
                out[os.path.relpath(path, directory)] = hashlib.sha256(handle.read()).hexdigest()
    return out


class ImportingThePortalBuilderWritesNothing(unittest.TestCase):

    def setUp(self):
        self.assertTrue(
            os.path.isfile(os.path.join(PORTAL, BUILDER)),
            "%s is not at %s -- this test is looking in the wrong place and everything below "
            "would pass vacuously" % (BUILDER, PORTAL))

    def _copy(self):
        directory = tempfile.mkdtemp(prefix="portal_builder_")
        self.addCleanup(shutil.rmtree, directory, True)
        shutil.copytree(PORTAL, os.path.join(directory, "portal"))
        return os.path.join(directory, "portal")

    def test_importing_it_writes_nothing(self):
        """The property this change created."""
        work = self._copy()
        before = _fingerprint(work)
        self.assertGreater(
            len(before), 3,
            "the copy holds %d files, so 'nothing changed' would be almost free to satisfy"
            % len(before))
        result = subprocess.run(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0, %r); import build_portal_pages" % work],
            capture_output=True, text=True, cwd=work, timeout=300)
        self.assertEqual(
            0, result.returncode,
            "importing %s failed: %s" % (BUILDER, (result.stderr or "")[-400:]))
        after = _fingerprint(work)
        changed = sorted(name for name in before if before[name] != after.get(name))
        self.assertEqual(
            [], changed,
            "importing %s rewrote %d file(s): %s. Its work belongs in main() behind a "
            "`__main__` guard -- a public repository gets imported by editors, linters and test "
            "collectors, and an import must not rewrite the checkout."
            % (BUILDER, len(changed), ", ".join(changed[:6])))
        self.assertEqual(
            "", result.stdout.strip(),
            "importing %s printed %r. Nothing should happen at import." % (BUILDER, result.stdout[:200]))

    def test_running_it_still_writes_the_pages(self):
        """The other direction: inert on import is only correct if the tool still builds.

        Without this, deleting the body of main() would pass the test above.
        """
        work = self._copy()
        emptied_names = []
        for name in sorted(os.listdir(work)):
            # Only the pages `emit` GENERATES. The two `inject` edits in place, and it exits 1 when
            # its anchor is missing -- emptying those made the builder fail correctly and the test
            # read that as the builder being broken.
            if name.endswith("_portal.html") and name not in INJECT_TARGETS:
                with open(os.path.join(work, name), "w", encoding="utf-8") as handle:
                    handle.write("<!-- emptied by the test -->\n")
                emptied_names.append(name)
        self.assertGreaterEqual(
            len(emptied_names), 7,
            "emptied only %d generated pages (%s), so 'it rebuilt them' proves little"
            % (len(emptied_names), ", ".join(emptied_names)))
        emptied = _fingerprint(work)
        result = subprocess.run([sys.executable, BUILDER],
                                capture_output=True, text=True, cwd=work, timeout=300)
        self.assertEqual(
            0, result.returncode,
            "running %s failed: %s" % (BUILDER, (result.stderr or "")[-400:]))
        rebuilt = _fingerprint(work)
        restored = sorted(name for name in emptied if emptied[name] != rebuilt.get(name))
        self.assertGreaterEqual(
            len(restored), 7,
            "running %s rebuilt only %d page(s) (%s). It is importable and inert but no longer "
            "builds, so the pages would silently stop being regenerated."
            % (BUILDER, len(restored), ", ".join(restored)))

    def test_the_pages_it_writes_match_the_ones_committed(self):
        """A copy, built from scratch, must reproduce what is in the tree.

        This is what makes the two tests above about a MOVE rather than a rewrite: if putting the
        calls behind a guard had changed their order or their arguments, the output would differ
        from what is committed and this fails.
        """
        work = self._copy()
        committed = _fingerprint(work)
        result = subprocess.run([sys.executable, BUILDER],
                                capture_output=True, text=True, cwd=work, timeout=300)
        self.assertEqual(
            0, result.returncode,
            "running %s failed: %s" % (BUILDER, (result.stderr or "")[-400:]))
        rebuilt = _fingerprint(work)
        differing = sorted(name for name in committed
                           if name.endswith(".html") and committed[name] != rebuilt.get(name))
        self.assertEqual(
            [], differing,
            "building from a clean copy does not reproduce the committed pages: %s"
            % ", ".join(differing[:6]))


if __name__ == "__main__":
    unittest.main()
