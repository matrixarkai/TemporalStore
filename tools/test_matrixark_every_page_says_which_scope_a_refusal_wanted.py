#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every page says which scope a refusal wanted, not just that it was refused.

The key portal was fixed for this. Four other places answered the same way and dropped the same
field: the policy save, the settings save, the settings import and the skill update all reduced a
failure to ``detail || error`` and threw away the ``required`` list beside it.

The edge answers a scope refusal as ``{"error": "insufficient_scope", "required":
["admin:api_key"]}`` and the operator read *insufficient_scope*. That became a likelier answer
rather than a rarer one when configuration writes stopped accepting ``admin:audit`` — the settings
save and the policy save are exactly the calls a read-scoped key now loses.

One helper in the shared nav script rather than four copies, because four copies of a sentence is
how three of them come to say something slightly different.

Two things here are not readable from the source, and both are about which half survives:
``detail`` names the setting and the reason and must win over the bare code, and the scope list is
*appended* to that rather than replacing it. A message that shows one and drops the other is the
same defect wearing different words.
"""
from __future__ import annotations

import glob
import io
import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "refusal_message_harness.js")

# Every portal page carries the shared nav script, so every one of them carries this.
MIN_PAGES = 7


def pages() -> list:
    return sorted(glob.glob(os.path.join(PORTAL, "*.html")))


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheHelperAnswersWithBothHalvesTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS] + pages(),
                              capture_output=True, text=True, timeout=180)

    def test_every_page_carries_it(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertIn("ok   every page given carries the helper", proc.stdout)

    def test_a_scope_refusal_names_the_scope(self) -> None:
        self.assertIn("a scope refusal names the scope", self._run().stdout)

    def test_a_rejected_value_shows_the_reason_not_the_code(self) -> None:
        self.assertIn("a rejected value shows the reason, not the code", self._run().stdout)

    def test_neither_half_swallows_the_other(self) -> None:
        self.assertIn("detail and scope both survive", self._run().stdout)


class NoCallerStillDropsTheFieldTest(unittest.TestCase):
    """The helper is only worth having if the callers use it; a page keeping its own
    `detail || error` would go on dropping the scope while this file reports success."""

    GENERATOR = os.path.join(PORTAL, "build_portal_pages.py")

    def test_the_scan_covers_the_pages_it_claims_to(self) -> None:
        self.assertGreaterEqual(len(pages()), MIN_PAGES,
                                "found %d portal pages" % len(pages()))

    def test_no_page_reduces_a_failure_to_detail_or_error(self) -> None:
        stale = []
        for path in pages() + [self.GENERATOR]:
            with io.open(path, encoding="utf-8") as handle:
                source = handle.read()
            for pattern in ("res.body.detail || res.body.error",
                            "res.b.detail || res.b.error"):
                if pattern in source:
                    stale.append("%s: %s" % (os.path.basename(path), pattern))
        self.assertEqual([], stale,
                         "these still drop the scope the edge named: %r" % stale)

    def test_the_helper_is_defined_where_the_callers_can_reach_it(self) -> None:
        """It lives in the shared nav script, which is emitted after each page's own — so a caller
        reaching for it at load time would throw, and inside a handler will not."""
        for path in pages():
            with io.open(path, encoding="utf-8") as handle:
                source = handle.read()
            if "__matrixarkWhy(" not in source:
                continue
            self.assertIn("window.__matrixarkWhy = function", source,
                          "%s calls the helper but does not carry it" % os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
