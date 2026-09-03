#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""What failed, and what each route actually answers.

Two things the edge knew and never said.

`_requests` is keyed by (route, method, status) and the snapshot collapsed all of it into one
number, `errors`. A route answering 401 because a customer holds the wrong key was indistinguishable
from one answering 500, and both read on the portal as the gateway being broken.

And nothing anywhere recorded a failure *happening*. The counters say seven requests failed; they
cannot say whether that was seven in the last minute or seven last Tuesday, which is the difference
between an incident and a footnote.

The ring is deliberately thin, and that is the property most worth guarding: route label, method,
status, time. No key, no key id, no tenant, no user, no path parameters, no bodies. This is shown to
anyone who can read the portal, and identity added here would be invisible in the panel and
permanent in the process.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as gwm  # noqa: E402


class TheStatusBreakdownIsSurfacedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.metrics = gwm.GatewayMetrics()

    def test_a_route_reports_each_status_it_answered(self) -> None:
        for _ in range(3):
            self.metrics.record("/v1/memories", "GET", 200, 0.004)
        self.metrics.record("/v1/memories", "GET", 404, 0.001)
        statuses = self.metrics.snapshot()["routes"]["/v1/memories"]["statuses"]
        self.assertEqual({"200": 3, "404": 1}, statuses)

    def test_the_error_count_still_agrees_with_the_breakdown(self) -> None:
        """Two numbers describing the same thing must not be able to disagree."""
        self.metrics.record("/v1/retrieve", "POST", 200, 0.01)
        self.metrics.record("/v1/retrieve", "POST", 401, 0.01)
        self.metrics.record("/v1/retrieve", "POST", 503, 0.01)
        route = self.metrics.snapshot()["routes"]["/v1/retrieve"]
        from_breakdown = sum(count for status, count in route["statuses"].items()
                             if int(status) >= 400)
        self.assertEqual(route["errors"], from_breakdown)

    def test_a_401_is_not_reported_the_same_as_a_500(self) -> None:
        """The whole point: those want completely different things done about them."""
        self.metrics.record("/v1/retrieve", "POST", 401, 0.01)
        other = gwm.GatewayMetrics()
        other.record("/v1/retrieve", "POST", 500, 0.01)
        self.assertNotEqual(self.metrics.snapshot()["routes"]["/v1/retrieve"]["statuses"],
                            other.snapshot()["routes"]["/v1/retrieve"]["statuses"])


class TheFailureRingTest(unittest.TestCase):

    def setUp(self) -> None:
        self.metrics = gwm.GatewayMetrics()

    def test_a_failure_is_remembered(self) -> None:
        self.metrics.record("/v1/retrieve", "POST", 503, 0.01)
        seen = self.metrics.snapshot()["recent_failures"]
        self.assertEqual(1, len(seen))
        self.assertEqual("/v1/retrieve", seen[0]["route"])
        self.assertEqual(503, seen[0]["status"])
        self.assertEqual("POST", seen[0]["method"])

    def test_a_success_is_not(self) -> None:
        self.metrics.record("/v1/memories", "GET", 200, 0.01)
        self.assertEqual([], self.metrics.snapshot()["recent_failures"])

    def test_the_newest_failure_is_first(self) -> None:
        """A panel reads top-down and the useful one is the last thing that happened."""
        self.metrics.record("/v1/memories", "GET", 404, 0.01)
        self.metrics.record("/v1/retrieve", "POST", 503, 0.01)
        seen = self.metrics.snapshot()["recent_failures"]
        self.assertEqual(["/v1/retrieve", "/v1/memories"], [f["route"] for f in seen])

    def test_it_is_bounded(self) -> None:
        """Process-lifetime structure on a hot path: unbounded is a leak on the worst day."""
        for _ in range(gwm.RECENT_FAILURES * 3):
            self.metrics.record("/v1/retrieve", "POST", 503, 0.001)
        self.assertEqual(gwm.RECENT_FAILURES,
                         len(self.metrics.snapshot()["recent_failures"]))

    def test_the_bound_is_small_enough_to_be_free(self) -> None:
        self.assertLessEqual(gwm.RECENT_FAILURES, 200,
                             "this rides in a process that lives for weeks")

    def test_it_carries_nothing_about_who_asked(self) -> None:
        """The property that decides whether this is safe to show on the portal at all."""
        self.metrics.record("/v1/retrieve", "POST", 503, 0.01, 100, 200)
        entry = self.metrics.snapshot()["recent_failures"][0]
        self.assertEqual({"at", "route", "method", "status"}, set(entry),
                         "the failure log grew a field; if it identifies a caller it is no longer "
                         "safe to render to whoever can read the portal")

    def test_the_route_is_the_bounded_label_not_the_raw_path(self) -> None:
        """A raw path carries ids. The label is the same bounded template the counters use."""
        self.metrics.record("/v1/memory/abc-123-secret", "GET", 404, 0.01)
        entry = self.metrics.snapshot()["recent_failures"][0]
        self.assertNotIn("abc-123-secret", entry["route"])
        self.assertEqual(gwm.route_label("/v1/memory/abc-123-secret"), entry["route"])


class TheLiveFrameCarriesThemTest(unittest.TestCase):

    def test_the_frame_carries_a_bounded_slice(self) -> None:
        import asyncio

        import matrixark_v1_gateway as gw
        from test_matrixark_v1_gateway import _FakeServer, _cfg

        for index in range(gw.LIVE_FAILURES * 2):
            gwm.METRICS.record("/v1/retrieve", "POST", 500 + (index % 3), 0.001)
        gw._reset_live_cache()
        frame = asyncio.run(gw._event_frame(_FakeServer(), _cfg(), "k", "acme", None, None))
        carried = frame["traffic"]["recent_failures"]
        self.assertEqual(gw.LIVE_FAILURES, len(carried),
                         "the frame carries %d failures; it goes to every viewer on every tick "
                         "that changes" % len(carried))


class TheTailFigureTest(unittest.TestCase):
    """A mean is the one latency figure that cannot describe the requests people complain about."""

    def setUp(self) -> None:
        self.metrics = gwm.GatewayMetrics()

    def test_the_mean_hides_a_tail_that_the_p95_does_not(self) -> None:
        for _ in range(10):
            self.metrics.record("/v1/ingest", "POST", 200, 0.002)
        for _ in range(10):
            self.metrics.record("/v1/ingest", "POST", 200, 2.0)
        route = self.metrics.snapshot()["routes"]["/v1/ingest"]
        self.assertGreater(route["p95_ms"], route["avg_ms"],
                           "half the requests took two seconds and the tail figure is not above "
                           "the mean, so it is not describing the tail")

    def test_a_route_nobody_called_has_no_tail_figure(self) -> None:
        """None, not zero. A route with no observations is not a very fast route."""
        self.assertIsNone(gwm.bucket_quantile([], 0.95, 0.0))

    def test_beyond_the_largest_bucket_it_reports_the_observed_maximum(self) -> None:
        """The overflow bucket has no upper edge, and infinity is not a true answer."""
        self.metrics.record("/v1/ingest", "POST", 200, 120.0)
        route = self.metrics.snapshot()["routes"]["/v1/ingest"]
        self.assertEqual(route["max_ms"], route["p95_ms"])

    def test_it_reports_a_bucket_edge_not_an_invented_precision(self) -> None:
        """A histogram supports "95% finished within 250 ms", not "the 95th percentile was 212"."""
        for _ in range(100):
            self.metrics.record("/v1/ingest", "POST", 200, 0.003)
        p95 = self.metrics.snapshot()["routes"]["/v1/ingest"]["p95_ms"]
        edges_ms = [round(edge * 1000.0, 2) for edge in gwm._BUCKETS]
        self.assertIn(p95, edges_ms,
                      "%r is not one of the histogram's edges, so it claims a precision the "
                      "buckets cannot support" % p95)


if __name__ == "__main__":
    unittest.main()
