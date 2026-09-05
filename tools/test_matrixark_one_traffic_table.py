#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The traffic panel draws the same table whichever source drew it.

Setup draws that panel from two places. At load it reads ``/v1/metrics`` and parses the Prometheus
text -- which needs no key, so it works before the reader has authenticated to anything. A moment
later the first live frame arrives and redraws the same element from the metrics snapshot.

They built different tables::

    scrape   Route | Requests | Errors | Mean latency
    frame    Route | Requests | Answers | Errors | Mean | 95% within

So the panel changed shape under the reader on every load. Worse, a deployment whose stream never
connects -- an enforced one with no key in the box -- stayed on the four-column table, with nothing
to say that a tail statistic existed. The mean is the one the metrics module itself dismisses:
*"fifty requests at 3 ms and one at nine seconds average out to something describing neither"*.

One builder now, two callers normalising into it. The scrape fills five of the six columns: the
status breakdown was in the ``status`` label all along and was being summed away, so a route
answering 401 to somebody with the wrong key read the same as one answering 500.

It leaves *95% within* empty. The text carries the buckets, but walking them in JS beside the walk
the gateway already does in Python is two implementations of one quantile, which is how they come to
disagree -- and the first frame fills the column in a second later.

Two builders producing "the same" table is not something reading the source settles, so both are run
over equivalent inputs and their output compared.
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
PAGE = os.path.join(PORTAL, "setup_portal.html")
HARNESS = os.path.join(PORTAL, "traffic_table_harness.js")


def page() -> str:
    with io.open(PAGE, encoding="utf-8") as handle:
        return handle.read()


class ThereIsOneBuilderTest(unittest.TestCase):

    def test_the_page_defines_it_once(self) -> None:
        self.assertEqual(1, page().count("function trafficTable("))

    def test_both_renderers_use_it(self) -> None:
        source = page()
        for name in ("renderTraffic", "renderLiveTraffic"):
            with self.subTest(renderer=name):
                start = source.index("function %s(" % name)
                body = source[start:source.index("\n  }", start)]
                self.assertIn("trafficTable(", body)

    def test_no_second_traffic_table_is_built_by_hand(self) -> None:
        """The header row is the tell: two of them meant two tables that could differ."""
        self.assertEqual(1, page().count("<th>Requests</th>"))


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class BothSourcesDrawTheSameTableTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_columns_match(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   both draw the same columns", out)
        self.assertIn("ok   and the columns are the six the frame can fill", out)

    def test_an_empty_deployment_reads_the_same_either_way(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   an empty deployment reads the same either way", out)
        self.assertIn("ok   and says so rather than drawing an empty table", out)

    def test_the_busiest_route_is_first_in_both(self) -> None:
        self.assertIn("ok   the busiest route is first in both", self._run().stdout)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class WhatEachSourceCanSayTest(unittest.TestCase):

    def _run(self):
        return subprocess.run(["node", HARNESS, PAGE], capture_output=True, text=True, timeout=300)

    def test_the_scrape_shows_the_answers_it_was_discarding(self) -> None:
        """The status is a label on the counter series. Summed away, a route answering 401 to
        somebody with the wrong key was indistinguishable from one answering 500."""
        out = self._run().stdout
        self.assertIn("ok   the scrape shows the status breakdown it was throwing away", out)
        self.assertIn("ok   the scrape marks a 4xx as bad", out)

    def test_the_scrape_does_not_guess_the_tail(self) -> None:
        """Computing it here would be a second implementation of the gateway's bucket walk, and the
        first frame fills the column in a second later."""
        self.assertIn("ok   the scrape leaves the tail column empty rather than guessing",
                      self._run().stdout)

    def test_the_frame_fills_the_tail(self) -> None:
        self.assertIn("ok   the frame fills the tail column", self._run().stdout)

    def test_an_unknown_number_is_a_dash_not_a_missing_row(self) -> None:
        """A route with no timing yet still has requests worth showing."""
        out = self._run().stdout
        self.assertIn("ok   a route with no timing keeps its row", out)
        self.assertIn("ok   and shows a dash where the number is unknown", out)


if __name__ == "__main__":
    unittest.main()
