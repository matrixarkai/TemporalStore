#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The production readiness gate reports build state, and now says so.

temporalstore_production_readiness_ready is the headline stat on the engine dashboard and carries
two alerts. It is computed from build and configuration state only: the sub-reports behind it read
no atomics, no filesystem, no clock and no network, and 146 hardcoded true values sit among them.
It reports the same value for a healthy cluster and for one with every node down.

That is a reasonable thing to compute. It is not a health signal, and the metric name, its
dashboard position and its alert wording all read as one -- which is the risk: an operator sees
green and concludes the system is serving.

This pins both halves. If a runtime read is ever added to that surface the first test fails, and
the wording that now says it does not observe runtime health has to be revisited rather than
quietly becoming wrong in the other direction.
"""
from __future__ import annotations

import os
import re
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUST_SRC = os.path.join(ROOT, "crates", "temporalstore-rust", "src")
PROMETHEUS = os.path.join(RUST_SRC, "proxy", "prometheus.rs")
DASHBOARD = os.path.join(ROOT, "docs", "ops", "temporalstore-dashboard.json")
ALERTS = os.path.join(ROOT, "docs", "ops", "temporalstore-alerts.yml")

GATE_FUNCTIONS = (
    "production_readiness_report",
    "ingestion_readiness_report",
    "client_routing_readiness_report",
    "proxy_serving_readiness_report",
    "data_node_service_readiness_report",
    "metaserver_control_plane_readiness_report",
    "storage_production_posture_report",
    "feature_module_production_readiness_report",
    "context_workflow_production_readiness_report",
    "storage_cache_dependency_matrix_report",
    "storage_ssd_cache_pressure_readiness_report",
    "storage_migration_corpus_readiness_report",
)

RUNTIME_READ = re.compile(
    r"env::var|[.]load[(]Ordering|Instant::now|SystemTime::now|fs::read|fs::write|"
    r"fs::metadata|read_to_string|[.]lock[(][)]|TcpStream|reqwest")

CAVEAT = "does not observe runtime health"


def _rust_files() -> list:
    out = []
    for root, _dirs, files in os.walk(RUST_SRC):
        if "tests" in root.replace(os.sep, "/").split("/"):
            continue
        out.extend(os.path.join(root, name) for name in files if name.endswith(".rs"))
    return out


def _body(text: str, start: int) -> str:
    open_at = text.find("{", start)
    if open_at < 0:
        return ""
    depth = 0
    for index in range(open_at, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[open_at:index + 1]
    return text[open_at:]


def _gate_bodies() -> dict:
    found = {}
    for path in _rust_files():
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
        for name in GATE_FUNCTIONS:
            if name in found:
                continue
            match = re.search(r"\bfn\s+" + name + r"\s*[(<]", text)
            if match:
                found[name] = (os.path.basename(path), _body(text, match.start()))
    return found


def _read(path: str) -> str:
    with open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


class TheGateReadsBuildStateTest(unittest.TestCase):

    def setUp(self) -> None:
        self.bodies = _gate_bodies()

    def test_every_gate_function_was_found(self) -> None:
        """Without this the next test passes by finding nothing to check."""
        missing = [name for name in GATE_FUNCTIONS if name not in self.bodies]
        self.assertEqual([], missing,
                         "these gate functions were not found, so the scan below proves nothing "
                         "about them: %s" % ", ".join(missing))

    def test_the_gate_reads_no_runtime_state(self) -> None:
        offenders = []
        for name, (where, body) in sorted(self.bodies.items()):
            hits = sorted(set(RUNTIME_READ.findall(body)))
            if hits:
                offenders.append("%s (%s): %s" % (name, where, ", ".join(hits)))
        self.assertEqual(
            [], offenders,
            "the readiness gate now reads runtime state: %s. That may well be an improvement, but "
            "the metric HELP text, the dashboard panel title and both alert descriptions say it "
            "does NOT observe runtime health. Update those together with this test."
            % "; ".join(offenders))


class TheWordingSaysSoTest(unittest.TestCase):

    def test_the_scrape_carries_the_caveat(self) -> None:
        lines = [line for line in _read(PROMETHEUS).splitlines()
                 if "HELP temporalstore_production_readiness_ready" in line]
        self.assertTrue(lines, "the readiness gauge no longer declares a HELP line")
        self.assertIn(CAVEAT, lines[0].lower(),
                      "the HELP text stopped saying this is not a runtime health signal, which is "
                      "the only thing stopping green being read as healthy")

    def test_the_dashboard_stat_is_renamed(self) -> None:
        quote = chr(34)
        stale = quote + "title" + quote + ": " + quote + "Production Readiness" + quote
        self.assertNotIn(stale, _read(DASHBOARD),
                         "the headline stat is named as though it reports whether the deployment "
                         "is production-ready at runtime")

    def test_the_alerts_say_what_firing_means(self) -> None:
        self.assertIn(CAVEAT, _read(ALERTS).lower(),
                      "the readiness alert no longer explains that it watches build and "
                      "configuration state, so an operator reads it as an outage signal")


if __name__ == "__main__":
    unittest.main()
