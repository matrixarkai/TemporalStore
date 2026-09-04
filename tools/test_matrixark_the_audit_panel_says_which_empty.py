#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The audit panel, and the difference between the two kinds of empty.

The scope catalogue calls ``admin:audit`` "Read the audit log", the admin preset issues keys
carrying it, and a route now serves one. Nothing displayed it, so the only way a customer saw who
reached for what and was refused was to call the endpoint by hand.

It sits on the key portal, beside the keys and the usage those keys generated: every record in it
is about one of them.

The panel has two jobs beyond showing rows.

It never polls. The read walks the whole record log to find the audit rows, so a panel that
refreshed itself would put that walk on a timer for as long as somebody left the tab open.

And an empty list means two entirely different things. ``MATRIXARK_AUDIT_MODE`` defaults to off, so
on most deployments an empty audit log means nothing was *kept* -- not that nothing happened. A
panel rendering "no records" over both is at its most reassuring exactly when it should not be:
every refusal was discarded, and the page implies there were none. The endpoint reports the
recording mode beside the rows for this reason, and whether the panel uses it is behaviour, so it is
run rather than read.
"""
from __future__ import annotations

import io
import os
import re
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
PAGE = os.path.join(PORTAL, "api_key_portal.html")
HARNESS = os.path.join(PORTAL, "audit_panel_harness.js")


def page() -> str:
    with io.open(PAGE, encoding="utf-8") as handle:
        return handle.read()


class ThePanelIsThereTest(unittest.TestCase):

    def test_the_key_portal_has_an_audit_tab(self) -> None:
        self.assertIn('id="tab-audit"', page())

    def test_the_tab_has_a_pane(self) -> None:
        self.assertIn('id="pane-audit"', page())

    def test_it_starts_hidden_like_the_other_secondary_panes(self) -> None:
        """A fourth pane that ships visible puts two panels on screen at once."""
        tag = re.search(r'<section[^>]*id="pane-audit"[^>]*>', page()).group(0)
        self.assertIn("hidden", tag)

    def test_it_calls_the_route_that_serves_the_trail(self) -> None:
        self.assertIn('"/v1/admin/audit"', page())

    def test_the_footer_names_the_endpoint(self) -> None:
        """That footer is the page's own list of what it talks to; a panel missing from it is one
        nobody can audit from the outside."""
        footer = page()[page().index("<footer>"):]
        self.assertIn("/v1/admin/audit", footer)


class ItIsNotPutOnATimerTest(unittest.TestCase):
    """The read walks the record log. Refreshing it on a schedule is a standing cost for as long
    as the tab is open, which is precisely the shape the other panels were careful to avoid."""

    def test_nothing_polls_it(self) -> None:
        self.assertIsNone(re.search(r"setInterval\([^)]*loadAudit", page()))

    def test_it_is_wired_to_a_button(self) -> None:
        self.assertIn('$("auditBtn")', page())


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class AnEmptyTableSaysWhichEmptyTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_nothing_kept_is_not_reported_as_nothing_happened(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   an empty log with recording off says nothing is being recorded", out)
        self.assertIn("ok   it does not also claim nothing has happened yet", out)

    def test_it_warns_rather_than_reassures_and_says_what_to_change(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   and warns rather than reassures", out)
        self.assertIn("ok   and says an empty list is not evidence", out)
        self.assertIn("ok   and says where to change it", out)

    def test_nothing_happened_is_still_available_as_an_answer(self) -> None:
        """The warning must not swallow the ordinary case, or a deployment that IS recording reads
        as broken."""
        out = self._run().stdout
        self.assertIn("ok   an empty log with recording on says nothing has happened yet", out)
        self.assertIn("ok   and does not warn", out)
        self.assertIn("ok   the two empties do not read the same", out)

    def test_records_render_and_a_refusal_looks_like_one(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   records are rendered", out)
        self.assertIn("ok   a refusal is marked as one", out)
        self.assertIn("ok   and says what was asked for", out)
        self.assertIn("ok   a success is not marked as a refusal", out)

    def test_a_failed_read_is_not_an_empty_trail(self) -> None:
        """The worst available outcome for this panel: a read that could not run, drawn as a clean
        audit log."""
        out = self._run().stdout
        self.assertIn("ok   a failed read says so", out)
        self.assertIn("ok   and names what went wrong", out)
        self.assertIn("ok   a failed read leaves no table pretending to be an empty trail", out)


if __name__ == "__main__":
    unittest.main()
