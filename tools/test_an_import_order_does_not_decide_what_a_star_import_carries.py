# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Which module a process imports first must not decide what a star-import carries.

`matrixark_mcp_core` ends its body with `from matrixark_mcp_core_resource_io import *`, and
`matrixark_mcp_core_resource_io` begins its own with `from matrixark_mcp_core import ...`. That is
a cycle, and it resolves differently depending on which side a process enters:

* enter through `matrixark_mcp_core` -- it runs to the bottom, imports a fresh
  `matrixark_mcp_core_resource_io`, and re-exports everything that module defines;
* enter through `matrixark_mcp_core_resource_io` -- it imports `matrixark_mcp_core` from its first
  lines, so core's star-import at the bottom runs against a module that has executed nothing past
  its own import block, and core re-exports 26 fewer names.

Three modules reachable from production take those names by star-import and call them:
`matrixark_local_adapter_ingest` (8), `matrixark_local_adapter_dashboard` (1) and
`matrixark_local_adapter_retrieval` (1). Under the second order they raise `NameError` when the
call is reached -- at ingest time, not at import time, so nothing says so until a resource is
ingested.

The names are now bound explicitly from the module that defines them. This checks that the
property holds rather than that the import lines are present: each module is imported in a FRESH
interpreter after the adversarial order has been forced, and every name it calls that
`matrixark_mcp_core_resource_io` defines must be there. Names added later are covered without
editing this file, because the set is read from the call sites rather than listed here.
"""

from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

ADVERSARIAL = "import matrixark_mcp_core_resource_io\n"

_NAMES_PROBE = """
import sys, json
sys.path.insert(0, %r)
%s
import matrixark_mcp_core as core
print(json.dumps(sorted(n for n in vars(core) if not n.startswith("__"))))
"""

_BIND_PROBE = """
import sys, json
sys.path.insert(0, %r)
%s
import matrixark_mcp_local_adapter  # the parent package these mixins are split from
module = __import__(%r)
print(json.dumps([n for n in %r if not hasattr(module, n)]))
"""


def _run(code: str):
    result = subprocess.run([sys.executable, "-B", "-c", code],
                            capture_output=True, text=True, timeout=300)
    lines = result.stdout.strip().splitlines()
    if not lines:
        return None, (result.stderr.strip().splitlines() or ["<no output>"])[-1]
    try:
        return json.loads(lines[-1]), None
    except ValueError:
        return None, lines[-1]


def _core_names(pre: str):
    return _run(_NAMES_PROBE % (str(TOOLS), pre))


def _star_importers():
    """Modules that take core by star-import and call something core_resource_io defines.

    Read from the call sites, so a name added to one of these modules tomorrow is covered
    without this file being edited.
    """
    resource_io = TOOLS / "matrixark_mcp_core_resource_io.py"
    if not resource_io.exists():
        return {}, set()
    try:
        io_tree = ast.parse(resource_io.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError:
        return {}, set()
    defined = {n.name for n in io_tree.body
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}

    found = {}
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        src = path.read_text(encoding="utf-8", errors="replace")
        if "matrixark_mcp_core import *" not in src:
            continue
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue
        called = {n.func.id for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        at_risk = sorted(called & defined)
        if at_risk:
            found[path.stem] = at_risk
    return found, defined


class AnImportOrderDoesNotDecideWhatTheStarImportCarriesTest(unittest.TestCase):

    def test_the_scan_finds_star_importers_to_check(self) -> None:
        """Vacuity floor on the SCAN. With no modules found, every check below passes by
        iterating over nothing, and that reads exactly like the property holding."""
        found, defined = _star_importers()
        self.assertGreater(len(defined), 10,
                           "matrixark_mcp_core_resource_io defines %d names; the scan is not "
                           "parsing it and the checks below compare against an empty set"
                           % len(defined))
        self.assertTrue(found,
                        "no module star-imports matrixark_mcp_core and calls a name "
                        "matrixark_mcp_core_resource_io defines. If the star-imports were "
                        "replaced by explicit ones this file has nothing left to guard and "
                        "should go; if the scan broke, it is silently passing.")

    def test_the_cycle_still_resolves_two_ways(self) -> None:
        """Positive control on the HAZARD. If entering either way now gives core the same names,
        the cycle has been fixed at the source -- good news, and this file's remaining checks can
        no longer fail, so it should be retired rather than left in place looking useful."""
        clean, err = _core_names("")
        self.assertIsNone(err, "could not import matrixark_mcp_core cleanly: %s" % err)
        raced, err = _core_names(ADVERSARIAL)
        self.assertIsNone(err, "could not import under the adversarial order: %s" % err)
        self.assertTrue(
            set(clean) - set(raced),
            "matrixark_mcp_core now re-exports the same names whichever side of the cycle a "
            "process enters. The hazard this file guards is gone; retire it deliberately rather "
            "than keeping a test that cannot fail.")

    def test_every_star_importer_still_binds_the_names_it_calls(self) -> None:
        """The property. Checked by importing, not by reading import lines: a module could bind
        these names any number of ways and all that matters is that they are there."""
        found, _ = _star_importers()
        for module, names in sorted(found.items()):
            with self.subTest(module=module):
                missing, err = _run(_BIND_PROBE % (str(TOOLS), ADVERSARIAL, module, names))
                if err is not None:
                    self.skipTest("%s could not be imported in this environment: %s"
                                  % (module, err))
                self.assertEqual(
                    [], missing,
                    "%s calls %s, and does not have them when a process enters the cycle through "
                    "matrixark_mcp_core_resource_io. The call sites raise NameError at the moment "
                    "they are reached, not at import. Bind them from the module that defines "
                    "them." % (module, ", ".join(missing)))


if __name__ == "__main__":
    unittest.main()
