#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two of the three model roles were alertable. The one called most was not.

`matrixark_gateway_extraction_model_active` and `matrixark_gateway_embedding_semantic` are gauges
with panels and alerts, for the reason `config_health_lines` gives: a misconfigured deployment is
indistinguishable from a healthy one at the API surface, so a dashboard charting only rate and
latency shows it as perfectly healthy for months.

Summaries had neither, and they are the role called most -- extraction runs once per ingest and
**every context node gets a summary** that retrieval then walks.

The case that makes it a third series rather than a label on an existing one:

    extraction provider   extraction_model_active   summary_model_active
    openai_compatible     1                         1
    anthropic             1                         0
    deterministic         0                         0

An Anthropic deployment is a real model for extraction and returns rule-written summaries. On the
existing metric it reads as fully model-backed. The set of names `config_health_lines` uses to
decide the other two cannot express that -- the SAME provider name means 1 on one line and 0 on
this one -- so this value is taken from the snapshot's own `summary.writes`, which is decided by
the one classifier that mirrors the summariser.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

import matrixark_gateway_metrics as metrics  # noqa: E402

SERIES = "matrixark_gateway_summary_model_active"


def emitted(extraction: str) -> dict:
    """The gauges a deployment with this extraction provider would publish."""
    script = ("import json, matrixark_v1_gateway as gw, matrixark_gateway_metrics as m;"
              "s = gw._model_config_snapshot();"
              "print(json.dumps({'writes': s['summary']['writes'],"
              " 'lines': m.config_health_lines(s)}))")
    environ = dict(os.environ)
    environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = extraction
    environ.pop("MATRIXARK_SUMMARY_PROVIDER", None)
    environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/matrixark-summary-metric-test.json"
    out = subprocess.run([sys.executable, "-c", script], cwd=TOOLS, env=environ,
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    payload = json.loads(out.stdout)
    values = {}
    for line in payload["lines"]:
        if line.startswith("matrixark_"):
            name, _, value = line.partition(" ")
            values[name] = int(float(value))
    payload["values"] = values
    return payload


class TheSeriesSaysWhatTheOthersCannotTest(unittest.TestCase):

    def test_a_model_deployment_reports_one(self) -> None:
        self.assertEqual(1, emitted("openai_compatible")["values"][SERIES])

    def test_a_rules_deployment_reports_zero(self) -> None:
        self.assertEqual(0, emitted("deterministic")["values"][SERIES])

    def test_anthropic_is_a_model_for_extraction_and_rules_for_summaries(self) -> None:
        """The whole reason this is a third series. If it agreed with extraction on every
        provider it would be a duplicate of it."""
        values = emitted("anthropic")["values"]
        self.assertEqual(1, values["matrixark_gateway_extraction_model_active"])
        self.assertEqual(0, values[SERIES])

    def test_it_does_not_simply_track_extraction(self) -> None:
        """The floor for the test above, stated over the whole set: the two series must disagree
        somewhere, or one of them is noise."""
        pairs = {(emitted(p)["values"]["matrixark_gateway_extraction_model_active"],
                  emitted(p)["values"][SERIES])
                 for p in ("openai_compatible", "anthropic", "deterministic")}
        self.assertIn((1, 0), pairs, "the two series agree everywhere; one is redundant")

    def test_it_matches_what_the_snapshot_says(self) -> None:
        """One answer to the question. The gauge and the portal must not be able to disagree."""
        for provider in ("openai_compatible", "anthropic", "deterministic"):
            with self.subTest(provider=provider):
                payload = emitted(provider)
                self.assertEqual(1 if payload["writes"] == "model" else 0,
                                 payload["values"][SERIES])


class ItDoesNotInventAHealthyAnswerTest(unittest.TestCase):
    """A metric that guesses is worse than one that is missing: it is charted as fact."""

    def test_no_snapshot_reports_zero_rather_than_one(self) -> None:
        lines = metrics.config_health_lines(None)
        value = [l for l in lines if l.startswith(SERIES + " ")]
        self.assertEqual([SERIES + " 0"], value)

    def test_a_snapshot_without_a_summary_block_reports_zero(self) -> None:
        """An older snapshot, from a gateway that predates the block."""
        lines = metrics.config_health_lines({"warnings": []})
        self.assertIn(SERIES + " 0", lines)


class ItIsChartedAndAlertedLikeTheOthersTest(unittest.TestCase):

    @staticmethod
    def _dashboard() -> dict:
        path = os.path.join(REPO, "docs", "ops", "matrixark-gateway-dashboard.json")
        with io.open(path, encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _alerts() -> str:
        path = os.path.join(TOOLS, "temporalstore-prometheus", "matrixark-gateway-alerts.yml")
        with io.open(path, encoding="utf-8") as handle:
            return handle.read()

    def test_a_panel_charts_it(self) -> None:
        exprs = [t.get("expr", "") for p in self._dashboard()["panels"]
                 for t in p.get("targets", [])]
        self.assertTrue(any(SERIES in e for e in exprs), "the series is charted nowhere")

    def test_the_three_roles_sit_together(self) -> None:
        """Read as a set or not at all: three separate answers to "is this deployment using a
        model" are only useful side by side."""
        wanted = {"matrixark_gateway_embedding_semantic",
                  "matrixark_gateway_extraction_model_active", SERIES}
        rows = {}
        for panel in self._dashboard()["panels"]:
            exprs = " ".join(t.get("expr", "") for t in panel.get("targets", []))
            for series in wanted:
                if series in exprs:
                    rows[series] = panel["gridPos"]["y"]
        self.assertEqual(wanted, set(rows), rows)
        self.assertEqual(1, len(set(rows.values())), "the three roles are on different rows: %s" % rows)

    def test_no_two_panels_overlap(self) -> None:
        """Moving a panel to make room is how two end up in the same cell."""
        seen = set()
        for panel in self._dashboard()["panels"]:
            grid = panel["gridPos"]
            for x in range(grid["x"], grid["x"] + grid["w"]):
                for y in range(grid["y"], grid["y"] + grid["h"]):
                    self.assertNotIn((x, y), seen,
                                     "two panels occupy (%d,%d); one is %r" % (x, y, panel["title"]))
                    seen.add((x, y))

    def test_an_alert_fires_on_it(self) -> None:
        text = self._alerts()
        self.assertIn("MatrixArkSummariesNotUsingAModel", text)
        self.assertIn(SERIES + " == 0", text)

    def test_the_alert_says_what_is_happening_not_what_is_set(self) -> None:
        text = self._alerts()
        start = text.index("MatrixArkSummariesNotUsingAModel")
        block = text[start:start + 700]
        self.assertIn("written by rules", block)


if __name__ == "__main__":
    unittest.main()
