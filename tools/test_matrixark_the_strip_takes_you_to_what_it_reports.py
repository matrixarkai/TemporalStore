#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A segment that reports something needing attention takes you to it.

The status strip runs across all seven pages and five of its segments are links. Three named a
place: ``#encoding`` and ``#traffic`` inside the setup page's panes, and the ingestion page, which
has no tabs and so needs no fragment. **The two that mean something needs you** -- the warning count
and the count of settings waiting for a restart -- pointed at a bare ``/v1/admin/setup``.

A bare link opens the FIRST tab, which is Access. So the strip said work was waiting and the click
put the reader in front of the key input instead. The link cannot report that it failed; it went to
the page it named.

Getting to the right pane is only half of it for a pending setting. The page never said WHICH
settings were waiting: the name existed only as a badge on the field itself, and four of the nine
groups render shut -- **26 of the 47 restart-scoped settings live in one of them**. So the count on
every page had nowhere to go, and following it landed on a closed triangle.

This adds the panel that names them, with each name taking the reader to that control: opening the
group it is folded into, uncovering the row if a filter has hidden it, switching to the pane, and
focusing it.

The rule the guard below states is the general one, because the specific hrefs are what drifted:
**a segment pointing at a page that has tabs must name a fragment.** The existing fragment sweep in
``test_matrixark_a_badge_that_names_a_tab_opens_it`` checks that a named fragment resolves; it
cannot catch a link that names nothing at all, and that is exactly what these two were.
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
HARNESS = os.path.join(PORTAL, "awaiting_restart_harness.js")
SETUP = os.path.join(PORTAL, "setup_portal.html")

SEGMENT = re.compile(r'<a class="live-seg[^"]*" href="([^"]+)"[^>]*id="(\w+)"')

# Which file each portal route serves. Taken from the gateway in the neighbouring suite; here only
# the two the strip points at are needed, and both are unambiguous.
SERVES = {"/v1/admin/setup": "setup_portal.html", "/v1/admin/ingestion": "ingestion_portal.html"}


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def pages() -> list:
    return sorted(n for n in os.listdir(PORTAL) if n.endswith("_portal.html"))


def segments(page_text: str) -> list:
    return SEGMENT.findall(page_text)


def has_tabs(filename: str) -> bool:
    return 'role="tab"' in read(os.path.join(PORTAL, filename))


class ASegmentNamesWhereToLookTest(unittest.TestCase):

    def test_there_are_segments_to_check(self) -> None:
        """The rule below is a loop; over an empty list it proves nothing."""
        found = segments(read(SETUP))
        self.assertGreaterEqual(len(found), 5, found)

    def test_the_rule_has_something_to_bite_on(self) -> None:
        """If no destination had tabs, every bare link would be fine and the guard would be inert."""
        destinations = {href.split("#")[0] for href, _id in segments(read(SETUP))}
        tabbed = [d for d in destinations if d in SERVES and has_tabs(SERVES[d])]
        self.assertTrue(tabbed, "no strip destination has tabs, so this guard checks nothing")

    def test_every_segment_pointing_into_a_tabbed_page_names_a_place(self) -> None:
        for page in pages():
            for href, ident in segments(read(os.path.join(PORTAL, page))):
                route = href.split("#")[0]
                if route not in SERVES or not has_tabs(SERVES[route]):
                    continue
                with self.subTest(page=page, segment=ident):
                    self.assertIn("#", href,
                                  "%s links to a page with tabs and names no place in it, so it "
                                  "opens whichever tab is first" % ident)

    def test_the_two_that_report_trouble_are_the_ones_this_was_about(self) -> None:
        """Named, because they are the two that were wrong and the sweep above would keep passing
        if a later edit dropped them from the strip entirely."""
        for page in pages():
            found = dict((ident, href) for href, ident in
                         segments(read(os.path.join(PORTAL, page))))
            with self.subTest(page=page):
                self.assertIn("liveWarn", found)
                self.assertIn("liveWaiting", found)
                self.assertIn("#", found["liveWarn"])
                self.assertIn("#", found["liveWaiting"])


class TheNamedPlaceExistsTest(unittest.TestCase):

    def test_the_panel_is_on_the_page(self) -> None:
        self.assertIn('id="awaitingRestart"', read(SETUP))

    def test_it_is_inside_the_settings_pane(self) -> None:
        """A fragment resolves to whichever pane contains it; in the wrong one it would open the
        wrong tab, which is the fault this change is about."""
        page = read(SETUP)
        start = page.index('id="pane-settings"')
        depth, end = 0, len(page)
        for match in re.finditer(r"<section\b|</section>", page[start:]):
            depth += -1 if match.group(0) == "</section>" else 1
            if depth == 0:
                end = start + match.end()
                break
        self.assertIn('id="awaitingRestart"', page[start:end])


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ThePanelWorksTest(unittest.TestCase):
    """Run, not read: a renderer that never opens the folded group reads the same in a diff."""

    def _run(self):
        return subprocess.run(["node", HARNESS, SETUP], capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_it_ran_all_of_its_checks(self) -> None:
        """A harness that throws part way through still prints the checks it reached. Counting them
        is what tells a stopped run from a passing one."""
        out = self._run().stdout
        self.assertIn("all good", out)
        self.assertGreaterEqual(out.count("ok   "), 20, out)

    def test_it_names_the_settings_that_are_waiting(self) -> None:
        out = self._run().stdout
        for line in ("ok   it says how many",
                     "ok   it names the first",
                     "ok   it names the second",
                     "ok   one waiting reads as one"):
            with self.subTest(line=line):
                self.assertIn(line, out)

    def test_a_name_uncovers_the_control_it_points_at(self) -> None:
        out = self._run().stdout
        for line in ("ok   FLOOR: the group starts shut",
                     "ok   the group it was folded into is opened",
                     "ok   a filtered-out row is uncovered",
                     "ok   the settings tab is opened",
                     "ok   the control is scrolled to"):
            with self.subTest(line=line):
                self.assertIn(line, out)

    def test_clicking_a_name_is_wired_to_going_there(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   clicking a name goes to that setting", out)
        self.assertIn("ok   clicking the surrounding text goes nowhere", out)

    def test_something_actually_draws_the_panel(self) -> None:
        """The one the rest of this file could not catch.

        Every check above proves the panel works when it is run. None of them proved the page ever
        runs it -- deleting the call from the settings renderer left all of them green, which is the
        same shape as the defect this change exists to fix. So the renderer is extracted and called
        too, with the panel stubbed, and asked whether it passed the pending list along.
        """
        out = self._run().stdout
        for line in ("ok   FLOOR: the settings renderer drew its groups",
                     "ok   drawing the settings also draws the waiting panel",
                     "ok   and hands it what the deployment says is waiting",
                     "ok   FLOOR: an advanced group really does render shut"):
            with self.subTest(line=line):
                self.assertIn(line, out)

    def test_it_says_nothing_when_nothing_is_waiting(self) -> None:
        """The common case. A panel that always shows is one people stop reading."""
        out = self._run().stdout
        self.assertIn("ok   nothing waiting: the panel is not shown", out)


if __name__ == "__main__":
    unittest.main()
