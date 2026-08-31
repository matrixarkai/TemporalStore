#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Edge metrics: bounded label cardinality, correct Prometheus shapes, and config health as gauges.

The cardinality property is the one worth a test of its own. Request paths carry customer-chosen
identifiers (memory ids, blob keys); labelling a series by the raw path would let any client create
unbounded series in the operator's Prometheus, which is a denial of service against the monitoring
rather than merely untidy data.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as m  # noqa: E402


class RouteLabelTest(unittest.TestCase):
    def test_identifiers_in_the_path_collapse_to_one_series(self) -> None:
        self.assertEqual("/v1/memory/{id}", m.route_label("/v1/memory/abc123"))
        self.assertEqual("/v1/memory/{id}", m.route_label("/v1/memory/zzz999/history"))
        self.assertEqual("/v1/blob/{key}", m.route_label("/v1/blob/resources/ab/deadbeef"))
        # One template for every action under the jobs prefix: labelling a retry as a cancel
        # would be worse than labelling neither.
        self.assertEqual("/v1/admin/ingestion/jobs/{id}",
                         m.route_label("/v1/admin/ingestion/jobs/job-7/cancel"))
        self.assertEqual("/v1/admin/ingestion/jobs/{id}",
                         m.route_label("/v1/admin/ingestion/jobs/job-7/retry"))

    def test_named_routes_keep_their_own_series(self) -> None:
        for path in ("/v1/ingest", "/v1/retrieve", "/v1/memories", "/v1/skills", "/v1/resources",
                     "/v1/memory/by-key", "/v1/admin/config", "/v1/metrics"):
            with self.subTest(path=path):
                self.assertEqual(path, m.route_label(path))

    def test_an_unknown_path_never_becomes_its_own_series(self) -> None:
        self.assertEqual("other", m.route_label("/v1/whatever-a-client-invents"))
        self.assertEqual("other", m.route_label("/not-v1-at-all"))


class RecordingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.metrics = m.GatewayMetrics()

    def test_counters_and_the_histogram_agree(self) -> None:
        self.metrics.record("/v1/ingest", "POST", 202, 0.004, request_bytes=100)
        self.metrics.record("/v1/ingest", "POST", 202, 0.030, request_bytes=200)
        self.metrics.record("/v1/ingest", "POST", 500, 12.0)
        text = "\n".join(self.metrics.prometheus_lines())
        self.assertIn('matrixark_gateway_requests_total{route="/v1/ingest",method="POST",'
                      'status="202"} 2', text)
        self.assertIn('matrixark_gateway_requests_total{route="/v1/ingest",method="POST",'
                      'status="500"} 1', text)
        self.assertIn('matrixark_gateway_request_duration_seconds_count{route="/v1/ingest"} 3', text)
        self.assertIn('matrixark_gateway_request_bytes_total{route="/v1/ingest"} 300', text)

    def test_histogram_buckets_are_cumulative_and_end_at_the_request_count(self) -> None:
        for duration in (0.004, 0.030, 12.0):
            self.metrics.record("/v1/retrieve", "POST", 200, duration)
        lines = self.metrics.prometheus_lines()
        buckets = {}
        for line in lines:
            if line.startswith('matrixark_gateway_request_duration_seconds_bucket{route="/v1/retrieve"'):
                edge = line.split('le="')[1].split('"')[0]
                buckets[edge] = float(line.rsplit(" ", 1)[1])
        self.assertEqual(1.0, buckets["0.005"])   # the 4 ms request
        self.assertEqual(2.0, buckets["0.05"])    # + the 30 ms request
        self.assertEqual(2.0, buckets["10"])      # the 12 s request is still outside
        self.assertEqual(3.0, buckets["+Inf"])
        ordered = [buckets[e] for e in ("0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5",
                                        "1", "2.5", "5", "10", "30", "+Inf")]
        self.assertEqual(ordered, sorted(ordered))

    def test_a_bad_status_never_raises(self) -> None:
        # Metrics must not be able to fail a request; a malformed observation is dropped, not raised.
        self.metrics.record("/v1/ingest", "POST", None, 0.01)  # type: ignore[arg-type]
        self.assertIn("status=\"0\"", "\n".join(self.metrics.prometheus_lines()))

    def test_the_json_snapshot_matches_the_counters(self) -> None:
        self.metrics.record("/v1/ingest", "POST", 202, 0.010)
        self.metrics.record("/v1/ingest", "POST", 429, 0.020)
        snap = self.metrics.snapshot()
        self.assertEqual(2, snap["total_requests"])
        self.assertEqual(1, snap["total_errors"])
        self.assertEqual(1, snap["routes"]["/v1/ingest"]["errors"])
        self.assertEqual(15.0, snap["routes"]["/v1/ingest"]["avg_ms"])

    def test_in_flight_returns_to_zero(self) -> None:
        self.metrics.begin()
        self.metrics.begin()
        self.assertEqual(2, self.metrics.snapshot()["in_flight"])
        self.metrics.end()
        self.metrics.end()
        self.metrics.end()  # an extra end must not drive the gauge negative
        self.assertEqual(0, self.metrics.snapshot()["in_flight"])


class ConfigHealthTest(unittest.TestCase):
    def test_a_deterministic_deployment_reports_zero_on_both_gauges(self) -> None:
        text = "\n".join(m.config_health_lines(
            {"extraction": {"provider": "deterministic"},
             "embedding": {"provider": "deterministic"},
             "warnings": ["a", "b"]}))
        self.assertIn("matrixark_gateway_extraction_model_active 0", text)
        self.assertIn("matrixark_gateway_embedding_semantic 0", text)
        self.assertIn("matrixark_gateway_config_warnings 2", text)

    def test_a_configured_deployment_reports_one(self) -> None:
        text = "\n".join(m.config_health_lines(
            {"extraction": {"provider": "openai_compatible"},
             "embedding": {"provider": "openai_compatible"},
             "warnings": []}))
        self.assertIn("matrixark_gateway_extraction_model_active 1", text)
        self.assertIn("matrixark_gateway_embedding_semantic 1", text)
        self.assertIn("matrixark_gateway_config_warnings 0", text)

    def test_the_rendered_text_ends_in_a_newline_and_carries_help_and_type(self) -> None:
        text = m.prometheus_text({"extraction": {"provider": "deterministic"},
                                  "embedding": {"provider": "deterministic"}, "warnings": []},
                                 ["# appended by the caller"])
        self.assertTrue(text.endswith("\n"))
        self.assertIn("# HELP matrixark_gateway_requests_total", text)
        self.assertIn("# TYPE matrixark_gateway_request_duration_seconds histogram", text)
        self.assertIn("# appended by the caller", text)


if __name__ == "__main__":
    unittest.main()
