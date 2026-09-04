#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A refusal for want of a scope says which scope.

`apiFetch` reduced every failure to ``data.error || data.detail || "HTTP <status>"``. For the one
failure an operator is most likely to meet -- a key without the scope for what they just clicked --
the edge answers:

    403 {"error": "insufficient_scope", "required": ["admin:api_key"]}

and the page rendered *"Usage failed: insufficient_scope"*. The `required` list was carried on the
error object and never shown, leaving the operator to guess at the one thing the response had
already answered. It arrives on the Usage tab through `/v1/admin/api_key_usage`, and every other
call on this page goes through the same helper.

The scope names are shown verbatim, because they are the strings to tick in the Scopes box above.

The two halves are asserted separately: that the refusal is reported at all, and that it carries
the scope. Removing the change reddens only the second, which is how the pair earns its place --
a single check covering both would pass on a page that says nothing useful.
"""
from __future__ import annotations

import io
import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
PAGE = os.path.join(PORTAL, "api_key_portal.html")
HARNESS = os.path.join(PORTAL, "key_copy_harness.js")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ARefusalNamesTheScopeTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=180)

    def test_the_page_runs_clean(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_refusal_is_reported(self) -> None:
        self.assertIn("ok   H the refusal is reported", self._run().stdout)

    def test_the_message_names_the_scope(self) -> None:
        self.assertIn("ok   H it names the scope that was wanted", self._run().stdout)


class TheHelperReadsTheFieldTheEdgeSendsTest(unittest.TestCase):
    """`required` is the field name the gateway uses; reading a different one would leave the
    behaviour above passing only because the harness invented the same mistake."""

    def test_the_page_reads_required(self) -> None:
        with io.open(PAGE, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("data.required", source)

    def test_the_edge_still_sends_that_field(self) -> None:
        gateway = os.path.join(TOOLS, "matrixark_v1_gateway.py")
        with io.open(gateway, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn('"required": sorted(', source,
                      "the edge no longer sends `required`, so the page has nothing to show")


if __name__ == "__main__":
    unittest.main()
