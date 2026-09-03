#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The overrides panel renders, and renders safely.

Per-tenant telemetry deliberately does not exist -- `/v1/metrics` carries no tenant identity so it
stays safe to scrape without credentials -- so this view lives on an admin-gated page rather than a
dashboard, the same place per-key usage lives.

Run rather than grepped, and this one has a reason beyond principle: the first version of the panel
called `adminHeaders()`, a helper that does not exist on that page. The page loaded fine, because
the renderer only runs on a button press. Grepping for the function name would have found it and
called it present.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "overrides_panel_harness.js")
PAGE = os.path.join(PORTAL, "api_key_portal.html")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page's own JS cannot be run")
class TheOverridesPanelRendersTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=120)

    def test_every_case_renders(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode,
                         "the overrides panel did not render as expected:\n%s%s"
                         % (proc.stdout, proc.stderr))
        self.assertIn("PASS", proc.stdout)

    def test_a_hostile_setting_value_cannot_inject_markup(self) -> None:
        """Settings are tenant-supplied. A value reaching the page unescaped is an injection."""
        proc = self._run()
        self.assertIn("escapes the value", proc.stdout)
        self.assertNotIn("FAIL a hostile value", proc.stdout)

    def test_the_harness_checks_a_meaningful_number_of_cases(self) -> None:
        proc = self._run()
        checked = sum(1 for line in proc.stdout.splitlines() if line.startswith("ok "))
        self.assertGreaterEqual(checked, 8,
                                "only %d checks ran, so passing says little" % checked)


class ThePanelUsesHelpersThatExistTest(unittest.TestCase):
    """The bug that prompted the harness: a call to a helper the page does not define."""

    def test_no_call_to_an_undefined_page_helper(self) -> None:
        with open(PAGE, encoding="utf-8") as handle:
            page = handle.read()
        for helper in ("adminHeaders",):
            self.assertNotIn(
                helper + "(", page,
                "%s() is called and not defined on this page, so the panel fails on click while "
                "the page loads clean" % helper)


if __name__ == "__main__":
    unittest.main()
