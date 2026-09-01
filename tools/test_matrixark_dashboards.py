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


def _series_in(path: str) -> Set[str]:
    """Metric names a dashboard's panels query."""
    with io.open(path, encoding="utf-8") as handle:
        text = handle.read()
    names: Set[str] = set()
    for panel in json.loads(text).get("panels") or []:
        for target in panel.get("targets") or []:
            names.update(re.findall(r"\b((?:temporalstore|matrixark|rust)_[a-z0-9_]+)",
                                    str(target.get("expr", ""))))
    return names


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


class CataloguedAssetsTest(unittest.TestCase):
    """Every monitoring asset in the tree is offered, and offered against the right target.

    Both halves are things that failed here. The engine dashboard and the engine alert rules sat in
    docs/ops from the beginning and were served by nothing: a customer setting up monitoring from
    the portal got the gateway and the importer, and had no way to discover that Raft, the
    metaserver, the proxy, the stores and replication were monitorable at all. The existing
    forward check -- every named asset resolves -- passes perfectly while that is true, because it
    only ever looks at the names already in the registry.

    The second half is the reason serving them is not enough. These series come from the engine
    processes' own /metrics, not from the gateway's /v1/metrics, and importing the dashboard
    against the wrong one is not an error: it is twelve blank panels, which is precisely the
    "reads as no traffic" failure this file exists to prevent.
    """

    def setUp(self) -> None:
        import matrixark_v1_gateway as gateway
        self.gw = gateway

    def test_every_monitoring_asset_in_the_tree_is_served(self) -> None:
        served = set()
        for relative, _ct in self.gw._GRAFANA_ASSETS.values():
            served.add(os.path.basename(relative))
        ops = os.path.join(REPO, "docs", "ops")
        on_disk = {
            name for name in os.listdir(ops)
            if name.endswith(".json") or (name.endswith(".yml") and "alert" in name)
        }
        missing = sorted(on_disk - served)
        self.assertEqual(
            [], missing,
            "monitoring assets exist in docs/ops that the portal offers no way to get: %s. An "
            "asset nobody serves is one a customer never learns exists." % missing)

    def test_the_catalogue_and_the_registry_agree(self) -> None:
        catalogue = self.gw.monitoring_catalogue("http://127.0.0.1:17002")
        listed = {entry["asset"] for entry in catalogue["assets"]}
        self.assertEqual(
            set(self.gw._GRAFANA_ASSETS), listed,
            "the portal lists a different set of assets than the gateway can serve, so either a "
            "row downloads a 404 or a served asset is invisible on the page")
        for entry in catalogue["assets"]:
            with self.subTest(asset=entry["asset"]):
                relative, _ct = self.gw._GRAFANA_ASSETS[entry["asset"]]
                self.assertEqual(os.path.basename(relative), entry["filename"])
                self.assertIn(entry["scrape"], catalogue["targets"])
                self.assertTrue(entry["covers"].strip())

    def test_the_engine_target_is_taken_from_the_configured_datanode(self) -> None:
        # A placeholder host would make the copied scrape config something to hand-edit, and the
        # deployment already knows the answer: the data node serves /metrics on the same listener
        # it serves /blob on, which is the URL the gateway is configured to dial.
        catalogue = self.gw.monitoring_catalogue("http://10.1.2.3:17002")
        self.assertEqual("10.1.2.3:17002", catalogue["targets"]["engine"]["host"])
        self.assertEqual("/metrics", catalogue["targets"]["engine"]["metrics_path"])
        self.assertEqual("/v1/metrics", catalogue["targets"]["gateway"]["metrics_path"])

    def test_the_engine_dashboard_would_be_blank_against_the_gateway(self) -> None:
        # The claim the "scraped from" column makes, checked rather than asserted in prose: none of
        # the engine dashboard's series are in the gateway's export, so pointing that dashboard at
        # /v1/metrics really does produce empty panels rather than merely a slower query.
        engine = _series_in(os.path.join(REPO, "docs", "ops", "temporalstore-dashboard.json"))
        self.assertTrue(engine, "read no series out of the engine dashboard")
        overlap = sorted(engine & EMITTED)
        self.assertEqual(
            [], overlap,
            "the engine dashboard and the gateway export share series (%s), so the two scrape "
            "targets are no longer distinct and the portal's advice is stale" % overlap)


class ShippedStackTest(unittest.TestCase):
    """A rule file with no job behind it is permanent silence, and silence reads as health."""

    def _prometheus(self) -> dict:
        import yaml
        with io.open(os.path.join(TOOLS, "temporalstore-prometheus", "prometheus.yml"),
                     encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def test_the_engine_rules_have_a_job_that_feeds_them(self) -> None:
        config = self._prometheus()
        loaded = " ".join(config.get("rule_files") or [])
        self.assertIn("engine-alerts", loaded,
                      "the engine alert rules are not loaded by the shipped Prometheus config")
        # Asking whether ANY job uses /metrics is not a check: node-exporter's job omits
        # metrics_path, which defaults to /metrics, so that question answers itself while the
        # engine job points anywhere at all. The job that feeds these rules has to be named.
        jobs_by_name = {job["job_name"]: job for job in config["scrape_configs"]}
        self.assertIn(
            "temporalstore_engine", jobs_by_name,
            "the engine alert rules are loaded but no job scrapes the engine, so every one of "
            "those rules evaluates against nothing and can never fire -- and a rule that cannot "
            "fire is indistinguishable from a healthy cluster")
        engine = jobs_by_name["temporalstore_engine"]
        self.assertEqual(
            "/metrics", engine.get("metrics_path", "/metrics"),
            "the engine job is not reading the engine's own Prometheus endpoint; the gateway's "
            "/v1/metrics carries none of these series")
        gateway_hosts = set()
        for job in config["scrape_configs"]:
            if job["job_name"] == "matrixark_gateway":
                for static in job.get("static_configs") or []:
                    gateway_hosts.update(str(t) for t in static.get("targets") or [])
        engine_hosts = set()
        for static in engine.get("static_configs") or []:
            engine_hosts.update(str(t) for t in static.get("targets") or [])
        self.assertTrue(engine_hosts, "the engine job has no targets")
        self.assertEqual(
            set(), engine_hosts & gateway_hosts,
            "the engine job and the gateway job scrape the same address, so one of them is "
            "reading an endpoint that does not serve its series")

    def test_the_engine_job_targets_the_service_ports(self) -> None:
        config = self._prometheus()
        engine = [j for j in config["scrape_configs"] if j["job_name"] == "temporalstore_engine"]
        self.assertEqual(1, len(engine), "no engine scrape job")
        targets = engine[0]["static_configs"][0]["targets"]
        # proxy / metaserver / datanode, as config/temporalstore.toml defines them.
        ports = sorted(str(t).rsplit(":", 1)[-1] for t in targets)
        self.assertEqual(["17000", "17001", "17002"], ports)


class MonitoringTableRendersTest(unittest.TestCase):
    """The section is built from the payload, so the payload is what it has to be built against.

    Reading the page source proves only that the table-building code is present. It cannot tell a
    table that renders from one whose builder is never reached because the response is shaped
    differently than the page expects -- the two pages contain identical text. So this feeds the
    page the real GET /v1/admin/config body, runs its scripts, and reads back what landed in the
    DOM, including what a click on the engine row actually requests.
    """

    def setUp(self) -> None:
        import shutil
        if not shutil.which("node"):
            self.skipTest("node is not installed")
        import matrixark_v1_gateway as gateway
        from test_matrixark_v1_gateway import _FakeServer, _cfg, drive
        gateway._GRAFANA_CACHE.clear()
        app = gateway.make_v1_app(_FakeServer(), _cfg())
        status, _h, body = drive(app, method="GET", path="/v1/admin/config",
                                 headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, status)
        self.payload = body.decode("utf-8") if isinstance(body, bytes) else body

    def _run(self) -> dict:
        import subprocess
        import tempfile
        page = os.path.join(TOOLS, "portal", "setup_portal.html")
        harness = os.path.join(TOOLS, "portal", "monitoring_table_harness.js")
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            handle.write(self.payload)
            fixture = handle.name
        try:
            proc = subprocess.run(["node", harness, page, fixture],
                                  capture_output=True, text=True, timeout=60)
        finally:
            os.unlink(fixture)
        self.assertEqual(0, proc.returncode, proc.stderr)
        return json.loads(proc.stdout)

    def test_the_table_renders_a_row_per_served_asset(self) -> None:
        import matrixark_v1_gateway as gateway
        result = self._run()
        self.assertEqual([], result["errors"], "the page's scripts threw")
        # One header row plus one per asset.
        self.assertEqual(len(gateway._GRAFANA_ASSETS) + 1, result["rows"], result["table"])
        self.assertIn("Engine and storage", result["table"])
        self.assertIn("Engine alert rules", result["table"])

    def test_the_engine_row_names_the_engine_endpoint(self) -> None:
        result = self._run()
        self.assertIn("/metrics", result["table"])
        self.assertIn("the engine processes", result["table"])
        self.assertIn("this gateway", result["table"])

    def test_the_scrape_config_carries_both_jobs(self) -> None:
        result = self._run()
        scrape = result["scrape"]
        self.assertIn("job_name: matrixark_gateway", scrape)
        self.assertIn("job_name: matrixark_engine", scrape)
        self.assertIn("metrics_path: /v1/metrics", scrape)
        self.assertIn("metrics_path: /metrics", scrape)
        # The gateway job takes the browser's host; the engine job takes the datanode this
        # deployment is configured to dial, so neither is a placeholder to be edited by hand.
        self.assertIn("gw.example:8080", scrape)

    def test_clicking_the_engine_row_downloads_the_engine_dashboard(self) -> None:
        result = self._run()
        self.assertEqual(["/v1/admin/monitoring/engine"], result["clicked"],
                         "the delegated handler did not reach the download")


if __name__ == "__main__":
    unittest.main()
