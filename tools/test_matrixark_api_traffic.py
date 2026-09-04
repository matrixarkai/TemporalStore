#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The API page reports what each documented route is actually serving.

The page lists 55 routes and said nothing about whether any of them was used, while the edge had
been counting every one and the live stream had been carrying those counters to every page.

Three things have to be true for that to be worth showing.

**Every documented route is measured.** The edge collapses paths to a bounded set of labels and
anything unmatched becomes `other`. Two documented endpoints were landing there --
`/v1/admin/deployment` and `/v1/admin/deployment/plan` -- so the traffic a customer generated
composing a deployment was indistinguishable from every other unmatched path. A route that is
documented and unmeasured is the kind of gap nothing complains about, so it is asserted here.

**The page is told which counter is which.** 55 paths map to 46 labels. The server says which,
because re-deriving `route_label` in JavaScript would put a second copy of that rule in the tree,
and when two copies drift the failure is a number rendered against the wrong route -- which looks
exactly like a number rendered against the right one.

**A shared counter says so.** Four documented paths are counted with a neighbour. A row showing
that figure as its own overstates itself by however much the neighbour is used.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
sys.path.insert(0, TOOLS)

import matrixark_gateway_metrics as gwm  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402


class EveryDocumentedRouteIsMeasuredTest(unittest.TestCase):

    def test_no_documented_route_falls_into_other(self) -> None:
        unmeasured = [entry["path"] for entry in gw.ROUTE_DOCS
                      if gwm.route_label(entry["path"]) == "other"]
        self.assertEqual([], unmeasured,
                         "these are documented and served but counted under 'other', so their "
                         "traffic cannot be told apart from any unmatched path: %r" % unmeasured)

    def test_every_route_is_served_with_its_counter(self) -> None:
        rows = gw.documented_routes()
        self.assertEqual(len(gw.ROUTE_DOCS), len(rows))
        missing = [r["path"] for r in rows if not r.get("metric")]
        self.assertEqual([], missing, "served without naming a counter: %r" % missing)

    def test_the_catalogue_constant_is_not_mutated(self) -> None:
        """Serving must not enrich the module-level list, or it accumulates on every request."""
        gw.documented_routes()
        gw.documented_routes()
        self.assertFalse(any("metric" in entry for entry in gw.ROUTE_DOCS),
                         "documented_routes() wrote into ROUTE_DOCS, so the catalogue grows a "
                         "field per call and the second response differs from the first")

    def test_a_shared_counter_names_who_it_is_shared_with(self) -> None:
        rows = gw.documented_routes()
        by_label: dict = {}
        for row in rows:
            by_label.setdefault(row["metric"], []).append(row["path"])
        for row in rows:
            others = sorted(set(by_label[row["metric"]]) - {row["path"]})
            if others:
                self.assertEqual(others, row.get("metric_shared_with"),
                                 "%s is counted with %r and does not say so"
                                 % (row["path"], others))
            else:
                self.assertNotIn("metric_shared_with", row,
                                 "%s claims to share a counter with nothing" % row["path"])

    def test_the_shared_case_actually_occurs(self) -> None:
        """If nothing shared a counter, the rule above would be vacuous."""
        rows = gw.documented_routes()
        shared = [r for r in rows if r.get("metric_shared_with")]
        self.assertTrue(shared, "no route shares a counter, so the shared-counter rule is untested")


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ThePageShowsThoseCountersTest(unittest.TestCase):

    def _run(self, harness, *pages):
        return subprocess.run(
            ["node", os.path.join(PORTAL, harness)] + [os.path.join(PORTAL, p) for p in pages],
            capture_output=True, text=True, timeout=180)

    def test_each_row_reports_its_traffic(self) -> None:
        proc = self._run("api_traffic_harness.js", "api_portal.html")
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_nothing_is_claimed_before_a_frame_arrives(self) -> None:
        """A route reading '0 requests' when nobody has reported is the strip's own rule broken."""
        out = self._run("api_traffic_harness.js", "api_portal.html").stdout
        self.assertIn("ok   nothing is claimed before a frame arrives", out, out)

    def test_an_unused_route_says_so_rather_than_staying_blank(self) -> None:
        out = self._run("api_traffic_harness.js", "api_portal.html").stdout
        self.assertIn("ok   a route absent from the frame reports no requests", out, out)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class EveryPageThatAsksForFramesGetsThemTest(unittest.TestCase):
    """Two pages asked for live frames and received none, in production, silently.

    `window.__matrixarkOnFrame` is defined by the nav script, which is emitted after the page's own
    -- deliberately, so a page running its own stream can claim it first. A page registering at its
    top level therefore called a function that did not exist yet, and guarded with
    `if (window.__matrixarkOnFrame)` the guard was simply false.

    `page_watchers_harness` could not catch it: its stub defines the function up front, so every
    page registered there. This runs the scripts in document order against a window that starts
    without it.
    """

    def test_every_page_that_registers_a_watcher_receives_a_frame(self) -> None:
        pages = sorted(p for p in os.listdir(PORTAL) if p.endswith(".html"))
        proc = subprocess.run(
            ["node", os.path.join(PORTAL, "frame_registration_harness.js")]
            + [os.path.join(PORTAL, p) for p in pages],
            capture_output=True, text=True, timeout=180)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_watcher_queued_before_the_nav_script_still_receives(self) -> None:
        """The case the queue exists for, and the one a late-registering probe cannot prove."""
        pages = ["catalog_portal.html", "explore_portal.html"]
        proc = subprocess.run(
            ["node", os.path.join(PORTAL, "frame_registration_harness.js")]
            + [os.path.join(PORTAL, p) for p in pages],
            capture_output=True, text=True, timeout=180)
        self.assertNotIn("never received a frame", proc.stdout, proc.stdout)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheApiSurfaceIsGroupedIntoTabsTest(unittest.TestCase):
    """55 routes in six groups arrived as one column; Administration alone is 21 of them.

    Two behaviours here cannot be read off the source. The list is rebuilt on every keystroke, so
    whether the open tab survives a re-render is behaviour. And the text filter searches every
    group while one pane is visible -- a filter quietly restricted to the open tab would answer
    "nothing matches that" while holding the match one tab away, which is the worst answer a
    reference page can give.
    """

    def _run(self):
        return subprocess.run(
            ["node", os.path.join(PORTAL, "api_tabs_harness.js"),
             os.path.join(PORTAL, "api_portal.html")],
            capture_output=True, text=True, timeout=180)

    def test_the_tabs_work(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_filter_searches_every_group(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   the filter searched every group, not just the open one", out, out)

    def test_the_open_tab_survives_a_keystroke(self) -> None:
        """The list is rebuilt on every keystroke; losing the tab each time makes it unusable."""
        out = self._run().stdout
        self.assertIn("ok   the open tab survives a keystroke", out, out)

    def test_matches_in_another_group_are_pointed_at(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   it says the matches are in another group", out, out)

    def test_the_notice_is_not_shown_when_the_open_group_has_matches(self) -> None:
        """Announcing "matches elsewhere" over a list full of matches is noise."""
        out = self._run().stdout
        self.assertIn("ok   the shared term really does match two groups", out, out)
        self.assertIn("ok   a match in the open group is not announced as elsewhere", out, out)


if __name__ == "__main__":
    unittest.main()
