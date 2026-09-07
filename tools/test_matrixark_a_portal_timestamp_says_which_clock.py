#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A timestamp on the portal did not say which clock it came from.

Four places showed one -- when the configuration was last written, each entry in its history, the
catalog's updated column, and a stored memory's time -- all with `toLocaleString()`, and **none of
them named the zone**. The records they describe carry the GATEWAY's time: a configuration write,
an audit entry, a stored memory. So correlating a portal timestamp with a server log was guesswork,
and on a browser whose clock is off -- which the setup page can now measure and say, since
mx#1181 -- it was wrong as well as ambiguous.

The four now go through one helper that names the zone. Not a sweep of every `toLocaleString()` in
the tree: most of those are `Number(...).toLocaleString()` putting separators in a count, and a
first pass at this counted twenty-four sites when there are four.

It also fixes something the old sites got wrong on the way: a missing timestamp rendered as
`Invalid Date` wherever the call was not already guarded, and two of the four were not.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "when_harness.js")

#: Every page, including the two that are hand-maintained and only have the nav injected.
PAGES = ("setup_portal.html", "overview_portal.html", "catalog_portal.html",
         "explore_portal.html", "api_portal.html", "ingestion_portal.html",
         "api_key_portal.html")

AN_INSTANT = 1760000000000


def rendered(values: list, page: str = "setup_portal.html", timezone: str = "UTC") -> list:
    """What the shipped helper returns, with the zone pinned so this reads the same anywhere."""
    environ = dict(os.environ)
    environ["TZ"] = timezone
    out = subprocess.run(["node", HARNESS, os.path.join(PORTAL, page), json.dumps(values)],
                         capture_output=True, text=True, timeout=300, env=environ)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    return json.loads(out.stdout)


def source(name: str) -> str:
    with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        return handle.read()


class ItNamesTheZoneTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_rendered_time_carries_its_zone(self) -> None:
        text = rendered([AN_INSTANT])[0]
        self.assertIn("UTC", text, "the zone is still not named: %r" % text)

    def test_the_zone_is_the_browsers_own(self) -> None:
        """The floor: if it printed a constant, the test above would pass on a lie."""
        self.assertIn("UTC", rendered([AN_INSTANT], timezone="UTC")[0])
        self.assertNotIn("UTC", rendered([AN_INSTANT], timezone="Asia/Tokyo")[0])

    def test_it_still_shows_the_time(self) -> None:
        text = rendered([AN_INSTANT])[0]
        self.assertIn("2025", text)


class AMissingTimeIsADashNotAnInvalidDateTest(unittest.TestCase):
    """Two of the four sites had no guard, so an absent timestamp read `Invalid Date`."""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_nothing_renders_as_a_dash(self) -> None:
        for value in (None, "", 0):
            with self.subTest(value=value):
                self.assertEqual("—", rendered([value])[0])

    def test_garbage_renders_as_a_dash(self) -> None:
        self.assertEqual("—", rendered(["not a time"])[0])

    def test_nothing_ever_reads_invalid_date(self) -> None:
        for value in (None, "", 0, "not a time", []):
            with self.subTest(value=value):
                self.assertNotIn("Invalid", rendered([value])[0])


class EveryPageHasItTest(unittest.TestCase):
    """It lives in the shared nav, so the two hand-maintained pages get it too."""

    def test_every_page_carries_the_helper(self) -> None:
        for page in PAGES:
            with self.subTest(page=page):
                self.assertIn("window.__matrixarkWhen = function", source(page))

    def test_the_four_sites_go_through_it(self) -> None:
        builder = source("build_portal_pages.py")
        for site in ("window.__matrixarkWhen(settings.updated_at * 1000)",
                     "esc(window.__matrixarkWhen(e.at * 1000))",
                     "function when(ms) { return window.__matrixarkWhen(ms); }",
                     "esc(window.__matrixarkWhen(when))"):
            with self.subTest(site=site[:40]):
                self.assertIn(site, builder)

    def test_no_timestamp_site_formats_its_own(self) -> None:
        """The point of one helper. A `new Date(x).toLocaleString()` anywhere is a fifth site that
        does not name its zone -- number formatting, which is the same method on a Number, is not
        matched by this."""
        builder = source("build_portal_pages.py")
        offenders = [line.strip() for line in builder.splitlines()
                     if "toLocaleString()" in line and "new Date(" in line]
        self.assertEqual([], offenders, offenders)


if __name__ == "__main__":
    unittest.main()
