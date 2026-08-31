#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The dashboards and the metrics this build emits must describe the same thing.

Both directions fail silently and neither fails loudly:

* A panel querying a series nobody emits is not an error anywhere. It is a **blank panel**, and a
  blank panel on a monitoring dashboard reads as "no traffic" — the most misleading thing a
  dashboard can say, because the operator concludes the system is idle rather than that the chart
  is wrong.
* A metric emitted and charted nowhere is work nobody sees. Two of the ones this caught were added
  precisely because the state they describe is invisible otherwise: documents waiting for a retry,
  and when somebody last changed the configuration.
"""
from __future__ import annotations

import io
import json
import os
import re
import sys
import unittest
from typing import Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as gwm  # noqa: E402
import matrixark_ingestion_jobs as jobs  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
GATEWAY_DASHBOARD = os.path.join(REPO, "docs", "ops", "matrixark-gateway-dashboard.json")
INGESTION_DASHBOARD = os.path.join(REPO, "docs", "ops", "matrixark-ingestion-dashboard.json")
ALERTS = os.path.join(TOOLS, "temporalstore-prometheus", "matrixark-gateway-alerts.yml")

# Series a dashboard has no business charting. `_start_time_seconds` is charted, but as an uptime
# derivation rather than by name, and the histogram's three suffixes come from one HELP line.
_NOT_A_PANEL: Set[str] = set()


def _read_json(path: str):
    """Read and CLOSE. `json.load(io.open(...))` leaks the handle, and a suite that prints
    ResourceWarnings on every run trains people to skim past its output."""
    with io.open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _read_text(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def _base(name: str) -> str:
    for suffix in ("_bucket", "_sum", "_count"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _emitted() -> Set[str]:
    """Every series name this build can produce, from the renderers themselves."""
    metrics = gwm.GatewayMetrics()
    metrics.record("/v1/ingest", "POST", 202, 0.01, 10, 20)
    text = "\n".join(metrics.prometheus_lines())
    text += "\n" + gwm.prometheus_text(
        {"extraction": {"provider": "deterministic"},
         "embedding": {"provider": "deterministic"}, "warnings": []})
    text += "\n" + jobs.prometheus_text()
    names = set()
    for line in text.splitlines():
        if line.startswith("#"):
            parts = line.split()
            if len(parts) >= 3 and parts[1] in ("HELP", "TYPE"):
                names.add(parts[2])
            continue
        match = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)", line)
        if match:
            names.add(_base(match.group(1)))
    return {name for name in names if name.startswith("matrixark_")}


def _queried(path: str) -> Set[str]:
    doc = _read_json(path)
    found = set()
    for panel in doc.get("panels", []):
        for target in panel.get("targets", []):
            found |= set(re.findall(r"\b(matrixark_[a-z0-9_]+)", target.get("expr", "")))
    for variable in doc.get("templating", {}).get("list", []):
        found |= set(re.findall(r"\b(matrixark_[a-z0-9_]+)", variable.get("query", "")))
    return {_base(name) for name in found}


def _alerted() -> Set[str]:
    text = _read_text(ALERTS)
    return {_base(name) for name in re.findall(r"\b(matrixark_[a-z0-9_]+)", text)}


EMITTED = _emitted()
SHOWN = _queried(GATEWAY_DASHBOARD) | _queried(INGESTION_DASHBOARD) | _alerted()


class DashboardMetricsTest(unittest.TestCase):
    def test_no_panel_queries_a_series_nobody_emits(self) -> None:
        for label, path in (("gateway", GATEWAY_DASHBOARD), ("ingestion", INGESTION_DASHBOARD)):
            missing = _queried(path) - EMITTED
            with self.subTest(dashboard=label):
                self.assertEqual(set(), missing,
                                 "%s dashboard charts series this build does not emit: %s"
                                 % (label, sorted(missing)))

    def test_no_alert_fires_on_a_series_nobody_emits(self) -> None:
        # An alert on a missing series never fires, which looks exactly like a healthy system.
        missing = _alerted() - EMITTED
        self.assertEqual(set(), missing,
                         "alert rules reference series this build does not emit: %s"
                         % sorted(missing))

    def test_every_emitted_series_is_charted_or_alerted(self) -> None:
        unseen = EMITTED - SHOWN - _NOT_A_PANEL
        self.assertEqual(set(), unseen,
                         "emitted and shown nowhere -- add a panel or an alert: %s"
                         % sorted(unseen))

    def test_the_two_silent_states_are_alerted_on(self) -> None:
        # These are the whole reason the gauges exist: every other panel looks healthy while
        # retrieval runs on hash vectors, and nothing raises its hand when an import leaves work
        # behind.
        alerted = _alerted()
        for name in ("matrixark_gateway_embedding_semantic",
                     "matrixark_ingestion_documents_retryable"):
            with self.subTest(metric=name):
                self.assertIn(name, alerted)

    def test_the_dashboards_are_valid_and_carry_their_descriptions(self) -> None:
        for label, path in (("gateway", GATEWAY_DASHBOARD), ("ingestion", INGESTION_DASHBOARD)):
            doc = _read_json(path)
            with self.subTest(dashboard=label):
                self.assertTrue(doc.get("uid"))
                self.assertTrue(doc.get("panels"))
                ids = [panel["id"] for panel in doc["panels"]]
                self.assertEqual(len(ids), len(set(ids)), "duplicate panel ids")
                for panel in doc["panels"]:
                    # A number without a sentence saying what it means is a number nobody trusts.
                    self.assertTrue(panel.get("description", "").strip(),
                                    "%s panel has no description" % panel["title"])

    def test_panels_do_not_overlap(self) -> None:
        # Grafana will render overlapping panels; it just looks broken.
        for label, path in (("gateway", GATEWAY_DASHBOARD), ("ingestion", INGESTION_DASHBOARD)):
            doc = _read_json(path)
            cells = set()
            with self.subTest(dashboard=label):
                for panel in doc["panels"]:
                    pos = panel["gridPos"]
                    for x in range(pos["x"], pos["x"] + pos["w"]):
                        for y in range(pos["y"], pos["y"] + pos["h"]):
                            self.assertNotIn((x, y), cells,
                                             "%s overlaps another panel" % panel["title"])
                            cells.add((x, y))


class ServedAssetsTest(unittest.TestCase):
    """The dashboards a customer can actually get hold of.

    The portal used to name a repo path. Someone running this as a managed service has no checkout,
    and even with one the file on their disk is whatever their copy is rather than what this build
    emits — which is the whole failure the drift test above exists to prevent. Serving them from
    the process closes that, but only if the path resolves: the first version pointed one directory
    too high and answered 404 for both dashboards while the alerts file, on a different relative
    path, worked — so a smoke test on one asset would have passed.
    """

    def setUp(self) -> None:
        import matrixark_v1_gateway as gateway
        from test_matrixark_v1_gateway import _FakeServer, _cfg
        self.gw = gateway
        self.app = gateway.make_v1_app(_FakeServer(), _cfg())
        gateway._GRAFANA_CACHE.clear()

    def tearDown(self) -> None:
        self.gw._GRAFANA_CACHE.clear()

    def test_every_named_asset_resolves(self) -> None:
        from test_matrixark_v1_gateway import drive
        for name in sorted(self.gw._GRAFANA_ASSETS):
            with self.subTest(asset=name):
                status, headers, body = drive(
                    self.app, method="GET", path="/v1/admin/monitoring/" + name,
                    headers={"Authorization": "Bearer k-acme"})
                self.assertEqual(200, status, "%s did not resolve" % name)
                self.assertGreater(len(body), 200, "%s came back suspiciously small" % name)
                self.assertTrue(headers["content-type"])

    def test_the_dashboards_served_are_the_dashboards_on_disk(self) -> None:
        from test_matrixark_v1_gateway import drive
        for name, path in (("gateway", GATEWAY_DASHBOARD), ("ingestion", INGESTION_DASHBOARD)):
            with self.subTest(asset=name):
                _st, _h, body = drive(self.app, method="GET",
                                      path="/v1/admin/monitoring/" + name,
                                      headers={"Authorization": "Bearer k-acme"})
                self.assertEqual(_read_json(path), json.loads(body))

    def test_an_unknown_asset_says_what_it_knows(self) -> None:
        from test_matrixark_v1_gateway import drive
        status, _h, body = drive(self.app, method="GET",
                                 path="/v1/admin/monitoring/nope",
                                 headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(404, status)
        payload = json.loads(body)
        self.assertEqual("unknown_asset", payload["error"])
        for name in self.gw._GRAFANA_ASSETS:
            self.assertIn(name, payload["detail"])

    def test_it_needs_a_key(self) -> None:
        from test_matrixark_v1_gateway import drive
        status, _h, _b = drive(self.app, method="GET", path="/v1/admin/monitoring/gateway")
        self.assertEqual(401, status)


if __name__ == "__main__":
    unittest.main()
