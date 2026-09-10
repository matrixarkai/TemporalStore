#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A module that says it has no import cycle must be importable on its own.

Twenty-three modules in tools/ end their docstring with a claim that there is no import-time cycle.
It was false in five of them, and a false claim of this kind is expensive: it is the first thing a
reader checks when an import fails and the last thing they doubt, because the module says so.

The claim is testable, so it is tested. Importing a module in a fresh interpreter with nothing else
loaded is exactly what "no import-time cycle" asserts.

`matrixark_mcp_core` imports several split-out modules from the BOTTOM of its own body and each
imports names back from it. Ten modules are in that loop by the import graph; only five actually
break, and which five depends on whether the names they want are defined ABOVE or BELOW that import
at the end of the aggregator. That is positional -- moving a definition inside `matrixark_mcp_core`
can break a sixth module without touching it, and nothing else in the tree would notice. This is
where it would be noticed.

A ratchet: the five are listed, the list may only shrink, and a module that stops being cyclic must
be struck from it.
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
import unittest

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TOOLS_DIR)

#: The modules that carry the no-cycle claim and cannot honour it. Every one is imported by
#: `matrixark_mcp_core` from the end of its body and imports names back from it, so importing one
#: first hands it a half-built aggregator.
#:
#: Fixing one is not a matter of re-pointing its imports at the modules that own the names. Some of
#: what these take -- `entity_patch`, `summarize_text`, `estimated_context_tokens`,
#: `ordered_normalized_role_list`, the ANTHROPIC_* constants -- is defined ONLY in the aggregator,
#: so the name has to move out to a module below it before the edge can be dropped at all.
#:
#: Their docstrings now say they cannot be imported alone. Strike a name from here when that stops
#: being true, and correct the docstring in the same change.
KNOWN_CYCLIC = frozenset((
    "matrixark_mcp_core_candidate_policy",
    "matrixark_mcp_core_codex_outcome",
    "matrixark_mcp_core_extraction",
    "matrixark_mcp_core_packing",
    "matrixark_mcp_core_scoring",
))

_CLAIM = re.compile(r"no\s+import-time\s+cycle", re.IGNORECASE)


def _tracked_modules() -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files", "tools/*.py"], cwd=REPO_ROOT,
        capture_output=True, text=True, check=False).stdout.split()
    return [rel for rel in listed if not os.path.basename(rel).startswith("test_")]


def _modules_claiming_no_cycle() -> list[str]:
    """Modules whose own docstring asserts there is no import-time cycle.

    Read from the docstring through the AST rather than by scanning the file, so the same sentence
    quoted in a comment -- or in this module -- is not counted as a claim.
    """
    out = []
    for rel in _tracked_modules():
        try:
            with open(os.path.join(REPO_ROOT, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (SyntaxError, OSError):
            continue
        if _CLAIM.search(ast.get_docstring(tree) or ""):
            out.append(os.path.basename(rel)[:-3])
    return sorted(out)


def _imports_alone(module: str) -> tuple[bool, str]:
    """Import the module in a fresh interpreter with nothing else loaded.

    A fresh process is the whole point: once `matrixark_mcp_core` is in `sys.modules` every one of
    these imports cleanly, which is why the tree can be green while five modules are unimportable.
    """
    result = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0, 'tools'); import " + module],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=300,
        env={"PATH": "/usr/bin:/bin", "HOME": os.environ.get("HOME", "/root"),
             "PYTHONPATH": REPO_ROOT + os.pathsep + TOOLS_DIR})
    return result.returncode == 0, (result.stderr or "").strip().splitlines()[-1:] and \
        (result.stderr or "").strip().splitlines()[-1] or ""


class AModuleThatClaimsNoCycleCanBeImportedTest(unittest.TestCase):

    def test_every_module_that_claims_it_can_honour_it(self) -> None:
        """The claim, tested. A module asserting it has no import-time cycle must import in a
        fresh interpreter -- that sentence has no other meaning."""
        broken = []
        for module in _modules_claiming_no_cycle():
            if module in KNOWN_CYCLIC:
                continue
            ok, last = _imports_alone(module)
            if not ok:
                broken.append("%s: %s" % (module, last[:160]))
        self.assertEqual(
            [], broken,
            "these modules say they have no import-time cycle and cannot be imported on their "
            "own; fix the import or correct the docstring, but do not leave it claiming this")

    def test_the_listed_modules_are_still_cyclic(self) -> None:
        """Tight in the other direction. A list that keeps names after they are fixed stops being
        a record of what is left, and the docstrings it excuses stay wrong."""
        fixed = []
        for module in sorted(KNOWN_CYCLIC):
            ok, _ = _imports_alone(module)
            if ok:
                fixed.append(module)
        self.assertEqual(
            [], fixed,
            "these import cleanly now; strike them from KNOWN_CYCLIC and restore the no-cycle "
            "sentence in their docstrings")

    def test_the_listed_modules_say_so_in_their_docstring(self) -> None:
        """A reader who hits the ImportError looks at the module, not at this test. If the module
        still claims the opposite, the correction never reaches them."""
        still_claiming = sorted(set(_modules_claiming_no_cycle()) & KNOWN_CYCLIC)
        self.assertEqual(
            [], still_claiming,
            "these cannot be imported alone but their docstring says there is no import-time "
            "cycle")

    def test_the_docstring_scan_actually_finds_things(self) -> None:
        """A floor. If the scan stopped matching, the first assertion would pass over an empty
        list and this file would test nothing at all."""
        claiming = _modules_claiming_no_cycle()
        self.assertGreater(
            len(claiming), 15,
            "the docstring scan came back nearly empty, so an empty broken list says nothing")
        self.assertNotIn(
            "test_a_module_that_claims_no_cycle_can_be_imported", claiming,
            "the scan is counting this file, which quotes the sentence rather than claiming it")

    def test_the_import_probe_can_actually_fail(self) -> None:
        """A floor under the probe itself. If it reported success for everything -- a swallowed
        error, a wrong cwd -- every assertion above would pass while checking nothing."""
        ok, _ = _imports_alone("matrixark_module_that_does_not_exist")
        self.assertFalse(ok, "the import probe reported success for a module that does not exist")
        ok, _ = _imports_alone("matrixark_mcp_core")
        self.assertTrue(ok, "the import probe cannot import the aggregator, so it is misconfigured")


if __name__ == "__main__":
    unittest.main()
