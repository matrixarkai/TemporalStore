#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Importing the flag-inventory builder used to write a document.

`tools/build_engine_flag_inventory.py` did `OUT.parent.mkdir(...)` and
`io.open(OUT, "w").write(...)` at MODULE SCOPE, so `import build_engine_flag_inventory` created a
directory and wrote `docs/ops/temporalstore-engine-flags.md`. The same block ends a conflict scan
with `raise SystemExit(...)`, which on an import ends the importing process rather than failing the
build that asked for it.

The tree already knew. `test_matrixark_engine_flag_inventory._builder_prelude` exec's only the part
of the file above the work, and its docstring said why: "the script does its work at module level --
importing it regenerates the document". That sentence is now false, and the note has been rewritten
to the reason that is still true (the module-scope scan costs ~24 seconds, so the helper stops short
of it).

WHAT IS AND IS NOT FIXED. Only the tail moved: the conflict scan, the mkdir, the write and the two
prints. Everything above stays at module scope, so importing still does the whole scan -- slow, but
it reads files and builds lists and changes nothing outside the process. Moving the rest is not
free: `NAME`, `SETTERS`, `SHELL_SETTERS`, `CONFIG_NAME`, `FLAG_NAME_ONLY`, `SET_ROOTS` and `SET_BY`
are read by module-level functions or by each other, so the set that must stay is closed under
"read by something that stays" rather than being simply "the constants".

THE TEST RUNS IN AN EMPTY DIRECTORY, which is what makes it fast and is also what makes it sharp.
The builder tolerates a tree with nothing in it: it finds no flags and -- under the old code --
wrote the document anyway, with `flags: 0`. So an empty tree exercises the write path in about a
second instead of the twenty-four a real scan costs, and the old behaviour is still plainly visible.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
BUILDER = os.path.join(TOOLS, "build_engine_flag_inventory.py")

#: What the builder writes, relative to the directory it is pointed at.
DOCUMENT = os.path.join("docs", "ops", "temporalstore-engine-flags.md")


class ImportingTheInventoryBuilderWritesNothing(unittest.TestCase):

    def setUp(self):
        self.assertTrue(
            os.path.isfile(BUILDER),
            "%s is missing, so everything below would pass without testing anything" % BUILDER)
        self.work = tempfile.mkdtemp(prefix="flag_inventory_")
        self.addCleanup(shutil.rmtree, self.work, True)
        # The builder goes in a `tools/` subdirectory, mirroring the real layout, because it
        # resolves its output path from the PARENT of its own directory. Copying it to the top of
        # the sandbox puts that parent OUTSIDE the sandbox: the first version of this test did
        # exactly that, so the shipped builder wrote to /tmp/docs/ops/... , the check below looked
        # in the sandbox, found nothing, and PASSED against the defective code. It also meant the
        # test itself wrote outside its own directory.
        self.tools = os.path.join(self.work, "tools")
        os.makedirs(self.tools)
        self.builder = os.path.join(self.tools, os.path.basename(BUILDER))
        shutil.copy2(BUILDER, self.builder)

    def _written(self):
        """Everything under the work directory except the builder and Python's bytecode cache.

        `__pycache__` is written by the interpreter, not by the module, and appears whenever the
        importing process was started without `-B`. It is not the write this file is about, and
        counting it would make the test fail for a reason that has nothing to do with the builder.
        The subprocesses below pass `-B` as well, so normally there is nothing to skip.
        """
        out = []
        for root, _dirs, names in os.walk(self.work):
            if "__pycache__" in root.split(os.sep):
                continue
            for name in names:
                path = os.path.relpath(os.path.join(root, name), self.work)
                if path != os.path.join("tools", os.path.basename(BUILDER)):
                    out.append(path)
        return sorted(out)

    def test_importing_it_writes_nothing(self):
        """The property this change created."""
        result = subprocess.run(
            [sys.executable, "-B", "-c",
             "import sys; sys.path.insert(0, %r); import build_engine_flag_inventory" % self.tools],
            capture_output=True, text=True, cwd=self.tools, timeout=900)
        self.assertEqual(
            0, result.returncode,
            "importing the builder failed: %s" % (result.stderr or "")[-500:])
        self.assertEqual(
            [], self._written(),
            "importing build_engine_flag_inventory created %s. Its write belongs in main() behind "
            "a `__main__` guard -- this repository is public, and editors, linters and test "
            "collectors import every module they find." % ", ".join(self._written()))

    def test_running_it_still_writes_the_document(self):
        """The other direction: inert on import is only right if the builder still builds.

        Without this, deleting the body of main() passes the test above.
        """
        result = subprocess.run(
            [sys.executable, "-B", os.path.join("tools", os.path.basename(BUILDER)), "."],
            capture_output=True, text=True, cwd=self.work, timeout=900)
        self.assertEqual(
            0, result.returncode,
            "running the builder failed: %s" % (result.stderr or "")[-500:])
        self.assertIn(
            DOCUMENT, self._written(),
            "running the builder did not write %s (wrote: %s). It is importable and inert but no "
            "longer builds, so the document would silently stop being regenerated."
            % (DOCUMENT, ", ".join(self._written()) or "nothing"))

    def test_a_default_conflict_does_not_end_the_importing_process(self):
        """The SystemExit moved too, and that is the half a write-only check would miss.

        `raise SystemExit` at module scope ends whatever imported the file. Asserted by the shape
        of the file rather than by provoking a conflict: producing two disagreeing production sites
        from a test would mean writing Rust into the work directory, and the assertion here is
        about WHERE the statement lives, which is exactly what went wrong.
        """
        with open(BUILDER, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
        guard = [i for i, line in enumerate(lines) if line.startswith('if __name__ == "__main__":')]
        self.assertEqual(
            1, len(guard),
            "expected exactly one `__main__` guard, found %d -- this test cannot locate the "
            "boundary it is checking" % len(guard))
        raises = [i for i, line in enumerate(lines) if line.lstrip().startswith("raise SystemExit")]
        self.assertTrue(raises, "no `raise SystemExit` found; this test is watching nothing")
        for i in raises:
            self.assertTrue(
                lines[i].startswith(" ") or lines[i].startswith("\t"),
                "line %d raises SystemExit at module scope: %r. On an import that ends the "
                "importing process rather than failing the build."
                % (i + 1, lines[i][:80]))


if __name__ == "__main__":
    unittest.main()
