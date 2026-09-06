#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The page shows the budget and the timing, from the response it already fetched.

The pack budget and the retrieve timing were computed, returned by `/v1/admin/overview`, and shown
to nobody: the setup page fetched that endpoint for the footprint panel and read one key out of it.
A number a customer cannot see is a number that cannot inform a decision, which is the whole
argument for every panel on that page.

Both are rendered now, from the same fetch. That is the part worth testing rather than asserting:
three panels drawn from one response is a claim about how many times the deployment is asked, and
only a fetch count shows it.

The page is RUN here, not read. A renderer exists whether or not anything calls it, and a figure
written into the wrong cell looks the same in a diff as one written into the right cell.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_v1_gateway as gateway  # noqa: E402

PORTAL = os.path.join(TOOLS, "portal")
PAGE = os.path.join(PORTAL, "setup_portal.html")
GENERATOR = os.path.join(PORTAL, "build_portal_pages.py")


class ThePanelsAreOnThePageTest(unittest.TestCase):

    def page(self) -> str:
        with open(PAGE, encoding="utf-8") as handle:
            return handle.read()

    def test_both_panels_have_somewhere_to_draw(self) -> None:
        for anchor in ('id="budgets"', 'id="latency"'):
            with self.subTest(anchor=anchor):
                self.assertIn(anchor, self.page())

    def test_both_have_a_renderer(self) -> None:
        for fn in ("function renderBudgets", "function renderLatency"):
            with self.subTest(renderer=fn):
                self.assertIn(fn, self.page())

    def test_the_markup_lives_in_the_generator_not_the_output(self) -> None:
        """`setup_portal.html` is generated. An edit made to the output survives exactly until the
        next person runs the generator, and then vanishes."""
        with open(GENERATOR, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn('id="budgets"', source)
        self.assertIn("function renderLatency", source)


class TheServerSendsWhatThePageReadsTest(unittest.TestCase):
    """The page reads `d.config.skills.budgets` and `d.latency`. Those paths are decided on the
    server, and a rename there would leave the panels blank on every deployment -- which reads as a
    deployment with nothing to report rather than as a page looking in the wrong place."""

    def test_the_latency_summary_is_a_top_level_key(self) -> None:
        summary = gateway._latency_summary()
        self.assertTrue(summary.get("available"))
        for field in ("deadline_ms", "transport_request_timeout_ms", "transport_io_timeout_ms",
                      "deadline_is_cooperative", "deadline_can_be_overrun_by_ms"):
            with self.subTest(field=field):
                self.assertIn(field, summary)

    def test_the_budget_summary_is_where_the_page_looks_for_it(self) -> None:
        snapshot = gateway._model_config_snapshot()
        budgets = ((snapshot.get("skills") or {}).get("budgets")) or {}
        self.assertTrue(budgets.get("available"),
                        "the page reads config.skills.budgets; it is not there")
        for field in ("paths", "paths_differ", "skills", "resources"):
            with self.subTest(field=field):
                self.assertIn(field, budgets)

    def test_each_path_carries_what_the_row_prints(self) -> None:
        snapshot = gateway._model_config_snapshot()
        paths = (((snapshot.get("skills") or {}).get("budgets")) or {}).get("paths") or []
        self.assertGreaterEqual(len(paths), 2)
        for row in paths:
            with self.subTest(path=row.get("path")):
                for field in ("label", "context_budget_tokens", "sections"):
                    self.assertIn(field, row)
                for section in ("skills", "resources"):
                    self.assertIn(section, row["sections"])

    def test_each_share_carries_what_decided_it(self) -> None:
        budgets = ((gateway._model_config_snapshot().get("skills") or {}).get("budgets")) or {}
        for name in ("skills", "resources"):
            with self.subTest(section=name):
                for field in ("percent", "asked_percent", "guard_percent", "bound_by"):
                    self.assertIn(field, budgets[name])


class ThePageDrawsThemTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def harness(self, mode: str) -> str:
        out = subprocess.run(
            ["node", "overview_panels_harness.js", "setup_portal.html", mode],
            cwd=PORTAL, capture_output=True, text=True, timeout=600)
        return out.stdout + out.stderr

    def test_two_callers_with_different_budgets(self) -> None:
        out = self.harness("differ")
        self.assertIn("all ok", out, out)

    def test_and_the_case_where_they_agree(self) -> None:
        """The floor for the panel's warning: it must not claim a difference that is not there,
        nor invent an overrun where no deadline is set."""
        out = self.harness("aligned")
        self.assertIn("all ok", out, out)

    def test_the_harness_counts_the_fetches(self) -> None:
        """The claim is one response for three panels. If the harness stopped counting, the two
        tests above would pass with the page asking three times."""
        with open(os.path.join(PORTAL, "overview_panels_harness.js"), encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("overviewCalls === 1", source)


if __name__ == "__main__":
    unittest.main()
