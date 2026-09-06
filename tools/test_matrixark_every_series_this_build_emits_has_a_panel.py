#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every series this build emits has a panel, including the ones added last.

`test_matrixark_dashboards` already asserts that. It collects what the build emits **by rendering
it** -- calling the renderers and reading the series names out of the text -- which is the right
way to ask, and it worked until three gauges were emitted from somewhere no renderer runs.

The worker footprint gauges were appended to the `/v1/metrics` response inside the route:

    extra.append("# TYPE matrixark_gateway_worker_resident_bytes gauge")

So they reached every scrape and no renderer, the rule could not see them, and they shipped with no
panel and no alert -- silently, because a rule that cannot see a series reports nothing rather than
a gap.

They are rendered by `prometheus_text` now, which is what makes the existing rule cover them. This
suite pins the property that made the gap possible: the scrape is what the renderers produce, and
the route adds nothing of its own.
"""
from __future__ import annotations

import ast
import json
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_metrics as gwm  # noqa: E402

DASHBOARD = os.path.abspath(
    os.path.join(TOOLS, os.pardir, "docs", "ops", "matrixark-gateway-dashboard.json"))
FOOTPRINT = ("matrixark_gateway_worker_resident_bytes",
             "matrixark_gateway_worker_peak_bytes",
             "matrixark_gateway_workers")


def rendered() -> str:
    return gwm.prometheus_text(
        {"extraction": {"provider": "deterministic"},
         "embedding": {"provider": "deterministic"}, "warnings": []})


class TheGaugesComeFromTheRendererTest(unittest.TestCase):

    def test_each_one_is_in_what_the_renderer_produces(self) -> None:
        text = rendered()
        for series in FOOTPRINT:
            with self.subTest(series=series):
                self.assertIn(series, text)

    def test_each_carries_a_type(self) -> None:
        """A series with no TYPE is one a scraper takes a guess at, and the rule that collects
        emitted names reads HELP and TYPE lines."""
        text = rendered()
        for series in FOOTPRINT:
            with self.subTest(series=series):
                self.assertIn("# TYPE %s gauge" % series, text)

    def test_the_route_appends_none_of_them(self) -> None:
        """The property that made the gap possible. Anything the route appends to the scrape is
        outside every renderer, and therefore outside the rule that checks for a panel."""
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            source = handle.read()
        for series in FOOTPRINT:
            with self.subTest(series=series):
                self.assertNotIn('extra.append("# TYPE %s' % series, source)
                self.assertNotIn('extra.append("%s' % series, source)

    def test_the_route_still_appends_something(self) -> None:
        """The floor: `extra` is how the ingestion registry's lines get in, so a rule that
        forbade appending altogether would be wrong as well as untested."""
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("extra = _jobs.prometheus_text()", source)


class ADeadReadingIsAbsentNotZeroTest(unittest.TestCase):
    """A worker whose resident size could not be read is not one holding nothing, and a zero here
    would average into a fleet figure as though it were."""

    def test_an_unreadable_measurement_emits_no_sample(self) -> None:
        original = gwm.worker_resident
        gwm.worker_resident = lambda: {"resident_bytes": None, "peak_bytes": None,
                                       "source": "unavailable"}
        try:
            lines = gwm.worker_lines()
        finally:
            gwm.worker_resident = original
        samples = [line for line in lines if not line.startswith("#")]
        self.assertEqual(["matrixark_gateway_workers %d" % gwm.worker_count()], samples)

    def test_a_readable_one_does(self) -> None:
        """The floor: a renderer that emitted nothing ever would satisfy the test above."""
        samples = [line for line in gwm.worker_lines() if not line.startswith("#")]
        self.assertGreater(len(samples), 1)

    def test_the_source_travels_with_the_number(self) -> None:
        """kilobytes on Linux and bytes on macOS: two readings that are not comparable, so which
        one this is has to be on the sample."""
        for line in gwm.worker_lines():
            if line.startswith("matrixark_gateway_worker_"):
                with self.subTest(line=line):
                    self.assertIn('source="', line)


class TheDashboardShowsThemTest(unittest.TestCase):

    def queried(self) -> set:
        with open(DASHBOARD, encoding="utf-8") as handle:
            doc = json.load(handle)
        found = set()
        for panel in doc.get("panels", []):
            for target in panel.get("targets", []):
                found.update(part for part in FOOTPRINT if part in target.get("expr", ""))
        return found

    def test_all_three_are_charted(self) -> None:
        self.assertEqual(set(FOOTPRINT), self.queried())

    def test_the_panels_explain_why_they_are_not_summed(self) -> None:
        """Per worker, and the panel has to say so: adding resident sets together produces a
        number larger than the machine is using."""
        with open(DASHBOARD, encoding="utf-8") as handle:
            doc = json.load(handle)
        text = " ".join(panel.get("description", "") for panel in doc["panels"]).lower()
        self.assertIn("not summed", text)


if __name__ == "__main__":
    unittest.main()
