# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The citation builder everyone gets by star-import is the one that suppresses nothing.

`serving_ref_for_pack`, `serving_refs_for_pack` and `serving_ref_groups_for_pack` are each defined
twice, and the two copies do not take the same arguments::

    matrixark_mcp_context_pack   (..., include_debug)
    matrixark_mcp_core_packing   (...)

The context_pack copies suppress a debug-only `source_ref` from a served citation unless debug
lineage is on -- `_context_memory_source_ref_is_debug_only` and `debug_lineage_enabled` are named
right there, so it is a decision somebody made. The core_packing copies have neither the helper nor
the parameter and always use `source_ref`. Measured on eight refs, **five get a different citation
source**; the clearest is a ref with both a debug-only `source_ref` and a `source_locator`, where
one answers `loc:8` and the other `evt:8`.

**All three names are in `matrixark_mcp_core_packing.__all__`, and `matrixark_mcp_core` star-imports
that module**, so the copies without the suppression are the ones that land in every module doing
`from matrixark_mcp_core import *` -- the live adapters among them. The suppressing copies never
reach core's namespace at all: core star-imports `matrixark_mcp_core_context_pack`, which is a
different module.

## Why this is latent, and exactly what would make it live

Nothing outside the two defining modules calls any of the three. `serving_refs_for_pack` has no
call site anywhere; `serving_ref_groups_for_pack` has one, in `matrixark_mcp_context_pack`, calling
its own. So the served pack is built by the suppressing chain today.

**One new call site in an adapter changes that silently.** The name is already in scope there --
nobody has to import anything -- and the copy it resolves to is the permissive one. That is what
this file watches: it fails when a call to any of the three appears in a module that does not define
it.

It does NOT assert that the two agree, because they do not, and which behaviour a served citation
should carry is a decision about what leaves the system, not a cleanup.
"""

from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

NAMES = ("serving_ref_for_pack", "serving_refs_for_pack", "serving_ref_groups_for_pack")

#: The module whose copies carry the debug-only suppression, and the one whose copies do not.
SUPPRESSING = "matrixark_mcp_context_pack"
PERMISSIVE = "matrixark_mcp_core_packing"

#: Modules allowed to call these: the two that define them. Anything else is picking up whichever
#: copy its namespace happens to hold, which is the permissive one.
DEFINING = {SUPPRESSING, PERMISSIVE}

_PROBE = """
import sys, json, inspect
sys.path.insert(0, {tools!r})
import matrixark_mcp_core as core
import {suppressing} as suppressing
import {permissive} as permissive
out = {{}}
for name in {names!r}:
    entry = {{}}
    for label, module in (("suppressing", suppressing), ("permissive", permissive)):
        fn = getattr(module, name, None)
        entry[label] = None if fn is None else list(inspect.signature(fn).parameters)
    fn = getattr(core, name, None)
    entry["core"] = None if fn is None else list(inspect.signature(fn).parameters)
    out[name] = entry
print(json.dumps(out))
"""


def _signatures():
    code = _PROBE.format(tools=str(TOOLS), suppressing=SUPPRESSING, permissive=PERMISSIVE,
                         names=list(NAMES))
    result = subprocess.run([sys.executable, "-B", "-c", code],
                            capture_output=True, text=True, timeout=600)
    lines = result.stdout.strip().splitlines()
    if not lines:
        raise AssertionError("the probe produced nothing: %s"
                             % (result.stderr.strip().splitlines() or ["<none>"])[-1])
    return json.loads(lines[-1])


def _call_sites():
    """name -> [(module, line, module_defines_it)] for every call by bare name."""
    found = {name: [] for name in NAMES}
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        defines = {node.name for node in tree.body
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id in found):
                found[node.func.id].append((path.stem, node.lineno, node.func.id in defines))
    return found


class TheCitationBuilderEveryoneGetsSuppressesNothingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.signatures = _signatures()
        except AssertionError as exc:
            raise unittest.SkipTest(str(exc))
        cls.sites = _call_sites()

    def test_both_copies_are_still_there(self) -> None:
        """Vacuity floor. If one copy is gone the comparison has nothing to compare, and a file
        that checks nothing reads the same as one that passes."""
        for name in NAMES:
            with self.subTest(name=name):
                entry = self.signatures[name]
                self.assertIsNotNone(entry["suppressing"],
                                     "%s no longer defines %s" % (SUPPRESSING, name))
                self.assertIsNotNone(entry["permissive"],
                                     "%s no longer defines %s" % (PERMISSIVE, name))

    def test_the_two_copies_still_take_different_arguments(self) -> None:
        """The hazard itself. If they take the same arguments now, somebody consolidated them and
        this file should be re-read rather than left asserting a shape that has gone."""
        for name in NAMES:
            with self.subTest(name=name):
                entry = self.signatures[name]
                self.assertIn("include_debug", entry["suppressing"],
                              "%s.%s no longer takes include_debug" % (SUPPRESSING, name))
                self.assertNotIn(
                    "include_debug", entry["permissive"],
                    "%s.%s now takes include_debug too. If the two were made to agree that is "
                    "good news and this file has nothing left to watch -- retire it deliberately."
                    % (PERMISSIVE, name))

    def test_core_still_re_exports_the_permissive_copy(self) -> None:
        """Recorded, not asserted as correct. If core ever re-exported the suppressing copy
        instead, every star-importer's behaviour would change in one commit, and that should be a
        decision somebody made rather than a side effect of import order."""
        for name in NAMES:
            with self.subTest(name=name):
                params = self.signatures[name]["core"]
                self.assertIsNotNone(params,
                                     "matrixark_mcp_core no longer carries %s at all" % name)
                self.assertNotIn(
                    "include_debug", params,
                    "matrixark_mcp_core now re-exports the SUPPRESSING %s. That changes what a "
                    "served citation contains for every module that takes names from core by "
                    "star-import. If it was intended, say so here." % name)

    def test_nothing_outside_the_defining_modules_calls_them(self) -> None:
        """What keeps the divergence latent, and the one thing that would end it.

        The names are already in every star-importer's namespace, so a new call site needs no
        import and reads as ordinary code -- and resolves to the copy that suppresses nothing.
        """
        for name in NAMES:
            outsiders = sorted({stem for stem, _line, defines in self.sites[name]
                                if not defines and stem not in DEFINING})
            with self.subTest(name=name):
                self.assertEqual(
                    [], outsiders,
                    "%s is now called in %s, which does not define it. The name is in scope there "
                    "by star-import from matrixark_mcp_core, and the copy it resolves to is "
                    "%s's -- the one that puts a debug-only source_ref into the citation. Either "
                    "call the suppressing copy explicitly or decide that the permissive one is "
                    "right." % (name, ", ".join(outsiders), PERMISSIVE))


if __name__ == "__main__":
    unittest.main()
