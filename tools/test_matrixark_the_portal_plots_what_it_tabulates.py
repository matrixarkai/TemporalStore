#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal plots what it has only ever tabulated.

The gateway counted every request and timed every route, and the portal showed a table. Across all
five portal pages there was **not one chart** -- no SVG, no canvas, no line -- so the shape of a
deployment's traffic, which is the thing a table cannot show, was visible only to somebody who had
stood up Prometheus and imported a dashboard.

The collector keeps a rolling series now and the setup page draws it.

Two decisions are load-bearing and are what this suite mostly checks:

* **Samples are cumulative, not rates.** A rate is the difference between two samples. Storing
  rates directly means a missed sample silently invents or erases traffic; a difference between two
  totals cannot.
* **The series is not in the live frame.** That frame goes to every open tab every two seconds, and
  four hours of history in it would be paid for on every tick by every viewer. It is asked for once,
  by the page that draws it.
"""
from __future__ import annotations

import os
import subprocess
import time
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_metrics as gwm  # noqa: E402
import matrixark_v1_gateway as gateway  # noqa: E402

PORTAL = os.path.join(TOOLS, "portal")


def collector() -> "gwm.GatewayMetrics":
    return gwm.GatewayMetrics()


def force_sample(metrics) -> None:
    """Take the next sample now rather than waiting for the interval to elapse."""
    metrics._series_at = 0.0


class TheSeriesIsHonestAboutWhatItHasTest(unittest.TestCase):

    def test_a_collector_nobody_called_plots_nothing(self) -> None:
        series = collector().series()
        self.assertEqual([], series["points"])
        self.assertEqual(0.0, series["covers_s"])

    def test_one_sample_is_not_a_rate(self) -> None:
        """Two samples make one interval. A single sample rendered as a point would be a rate
        computed against the beginning of time."""
        metrics = collector()
        for _ in range(5):
            metrics.record("/v1/retrieve", "POST", 200, 0.01)
        self.assertEqual(1, len(metrics._series))
        self.assertEqual([], metrics.series()["points"])

    def test_two_samples_make_one_point(self) -> None:
        metrics = collector()
        metrics.record("/v1/retrieve", "POST", 200, 0.01)
        force_sample(metrics)
        for _ in range(9):
            metrics.record("/v1/retrieve", "POST", 200, 0.01)
        self.assertEqual(1, len(metrics.series()["points"]))

    def test_a_sample_holds_the_running_total_of_requests(self) -> None:
        """Cumulative, and cumulative of the right thing.

        Monotonicity alone is too weak to pin this: the number of distinct (route, method, status)
        keys also only ever grows, and a mutation storing THAT passed a monotonic check while
        making every plotted rate wrong. The count is asserted exactly.
        """
        metrics = collector()
        metrics.record("/v1/retrieve", "POST", 200, 0.01)   # first sample: 1 request so far
        self.assertEqual(1, metrics._series[-1][1])
        force_sample(metrics)
        for _ in range(4):
            metrics.record("/v1/retrieve", "POST", 500, 0.01)
        # The second sample is taken on the first of those four, so it sees two.
        self.assertEqual(2, metrics._series[-1][1])
        self.assertEqual(1, metrics._series[-1][2], "the error count is not the request count")
        force_sample(metrics)
        metrics.record("/v1/retrieve", "POST", 200, 0.01)
        self.assertEqual(6, metrics._series[-1][1])
        totals = [sample[1] for sample in metrics._series]
        self.assertEqual(sorted(totals), totals, "samples are not cumulative")

    def test_an_interval_with_no_calls_has_no_mean(self) -> None:
        """None, not zero. A zero would draw a latency dip that never happened."""
        metrics = collector()
        metrics.record("/v1/retrieve", "POST", 200, 0.01)
        # A second sample with nothing recorded between the two.
        force_sample(metrics)
        metrics._sample_locked(metrics._series[-1][0] + 15.0)
        point = metrics.series()["points"][-1]
        self.assertIsNone(point["mean_ms"])
        self.assertEqual(0.0, point["requests_per_s"])

    def test_it_says_whose_traffic_it_is(self) -> None:
        self.assertTrue(collector().series()["worker_scoped"])


class TheSamplingIsCheapAndBoundedTest(unittest.TestCase):

    def test_many_requests_in_one_interval_take_one_sample(self) -> None:
        metrics = collector()
        for _ in range(500):
            metrics.record("/v1/retrieve", "POST", 200, 0.001)
        self.assertEqual(1, len(metrics._series))

    def test_the_window_cannot_grow_without_bound(self) -> None:
        """A process-lifetime structure on a hot path. The failure mode of an unbounded one only
        shows up on the deployments having the worst day."""
        metrics = collector()
        self.assertEqual(gwm.SERIES_SAMPLES, metrics._series.maxlen)
        for _ in range(gwm.SERIES_SAMPLES + 50):
            force_sample(metrics)
            metrics.record("/v1/retrieve", "POST", 200, 0.001)
        self.assertEqual(gwm.SERIES_SAMPLES, len(metrics._series))

    def test_the_window_is_hours_not_minutes(self) -> None:
        """The floor: a maxlen of 2 would satisfy the bound above and plot nothing useful."""
        self.assertGreaterEqual(gwm.SERIES_SAMPLES * gwm.SERIES_INTERVAL_S, 3600)

    def test_recording_still_cannot_raise(self) -> None:
        """This module's own promise: a metrics bug must not be able to fail a customer request."""
        metrics = collector()
        metrics._series = None  # type: ignore[assignment]
        try:
            metrics.record("/v1/retrieve", "POST", 200, 0.01)
        except Exception as exc:  # pragma: no cover - the point of the test
            self.fail("recording raised: %r" % (exc,))


class TheLiveFrameDoesNotCarryItTest(unittest.TestCase):
    """The frame is sent to every open tab every two seconds. History belongs in the request that
    draws it, not in the one that keeps a forgotten tab up to date."""

    def test_the_snapshot_has_no_series(self) -> None:
        metrics = collector()
        for _ in range(3):
            force_sample(metrics)
            metrics.record("/v1/retrieve", "POST", 200, 0.01)
        snapshot = metrics.snapshot()
        self.assertNotIn("series", snapshot)
        self.assertNotIn("points", snapshot)

    def test_but_the_series_is_reachable(self) -> None:
        """The floor: a collector that had no series at all would pass the test above."""
        metrics = collector()
        force_sample(metrics)
        metrics.record("/v1/retrieve", "POST", 200, 0.01)
        self.assertIn("points", metrics.series())


class TheOverviewCarriesItTest(unittest.TestCase):

    def test_the_gateway_publishes_a_series(self) -> None:
        series = gateway._metrics_series()
        for field in ("interval_s", "points", "covers_s", "worker_scoped"):
            with self.subTest(field=field):
                self.assertIn(field, series)

    def test_the_overview_route_includes_it(self) -> None:
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            source = handle.read()
        block = source[source.index('path == "/v1/admin/overview"'):]
        block = block[:block.index('"config": config_snapshot')]
        self.assertIn('"metrics_series": _metrics_series()', block)


class ThePageDrawsItTest(unittest.TestCase):
    """A chart is the easiest panel to make lie: an empty one and a quiet one look the same, and a
    missing sample drawn as zero is a dip that never happened. So the page is run."""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def harness(self, mode: str) -> str:
        out = subprocess.run(["node", "trend_panel_harness.js", "setup_portal.html", mode],
                             cwd=PORTAL, capture_output=True, text=True, timeout=600)
        return out.stdout + out.stderr

    def test_a_full_series_is_four_charts(self) -> None:
        """Three rates and the footprint level. The harness counts them; it was three before
        the worker's resident size joined them."""
        self.assertIn("all ok", self.harness("full"), self.harness("full"))

    def test_one_interval_is_not_a_line(self) -> None:
        self.assertIn("all ok", self.harness("short"), self.harness("short"))

    def test_an_absent_series_does_not_draw_zero(self) -> None:
        self.assertIn("all ok", self.harness("absent"), self.harness("absent"))

    def test_a_missing_sample_is_a_gap_not_a_dip(self) -> None:
        self.assertIn("all ok", self.harness("gaps"), self.harness("gaps"))

    def test_no_footprint_chart_where_no_size_can_be_read(self) -> None:
        """A platform that cannot report a resident size sends null for every point. An empty
        chart along the floor would read as a worker holding no memory, which is a different
        claim from "this cannot be measured here"."""
        self.assertIn("all ok", self.harness("noresident"), self.harness("noresident"))


class ThePageHadNoChartsBeforeTest(unittest.TestCase):
    """The premise, kept honest. If the portal grew a charting library, this panel's argument --
    that there was nowhere at all to see the shape of traffic -- would need restating."""

    def test_this_is_the_only_hand_rolled_chart(self) -> None:
        """One `<polyline` in the SOURCE -- the template inside `sparkline`. Three appear at
        runtime, which is the harness's business, not this file's: counting rendered output here
        would be reading the page instead of running it."""
        with open(os.path.join(PORTAL, "setup_portal.html"), encoding="utf-8") as handle:
            page = handle.read()
        self.assertEqual(1, page.count("<polyline"),
                         "something other than the trend panel is drawing a line")
        self.assertNotIn("<canvas", page)


class TheFootprintIsPlottedAsALevelTest(unittest.TestCase):
    """The footprint panel says what this worker holds right now, which answers "is it big" and
    not "is it growing" -- and growing is the question a memory number is usually being asked.

    Every other field in a sample is cumulative and is differenced into a rate. A resident size
    is not: differencing it plots the CHANGE in footprint while labelling it the footprint, and
    the result is a perfectly plausible chart of the wrong quantity.
    """

    @staticmethod
    def _collector(interval: float = 0.0):
        metrics = gwm.GatewayMetrics()
        metrics._series_at = 0.0
        return metrics

    def test_a_sample_carries_the_size_as_read(self) -> None:
        metrics = self._collector()
        saved = gwm.SERIES_INTERVAL_S
        gwm.SERIES_INTERVAL_S = 0.0
        try:
            for _ in range(3):
                metrics.record(path="/v1/retrieve", method="POST", status=200, duration_s=0.01)
                time.sleep(0.002)
        finally:
            gwm.SERIES_INTERVAL_S = saved
        points = metrics.series()["points"]
        self.assertTrue(points, "no points recorded")
        sizes = [p["resident_bytes"] for p in points]
        self.assertTrue(all(v is None or v > 1_000_000 for v in sizes), sizes)
        # A differenced level would be a small number or a zero, never the process's real size.
        self.assertTrue(any(v is not None and v > 1_000_000 for v in sizes), sizes)

    def test_an_unreadable_size_is_absent_not_zero(self) -> None:
        metrics = self._collector()
        saved_interval, saved_reader = gwm.SERIES_INTERVAL_S, gwm.worker_resident
        gwm.SERIES_INTERVAL_S = 0.0
        gwm.worker_resident = lambda: {"resident_bytes": None, "source": "unavailable"}
        try:
            for _ in range(3):
                metrics.record(path="/v1/retrieve", method="POST", status=200, duration_s=0.01)
                time.sleep(0.002)
        finally:
            gwm.SERIES_INTERVAL_S, gwm.worker_resident = saved_interval, saved_reader
        for point in metrics.series()["points"]:
            self.assertIsNone(point["resident_bytes"],
                              "an unreadable size became a number")

    def test_a_reader_that_raises_does_not_break_recording(self) -> None:
        """The recording-never-raises promise, extended to the new read."""
        metrics = self._collector()
        saved_interval, saved_reader = gwm.SERIES_INTERVAL_S, gwm.worker_resident

        def boom():
            raise OSError("/proc is not here")

        gwm.SERIES_INTERVAL_S = 0.0
        gwm.worker_resident = boom
        try:
            metrics.record(path="/v1/retrieve", method="POST", status=200, duration_s=0.01)
            time.sleep(0.002)
            metrics.record(path="/v1/retrieve", method="POST", status=200, duration_s=0.01)
        finally:
            gwm.SERIES_INTERVAL_S, gwm.worker_resident = saved_interval, saved_reader
        self.assertEqual(2, sum(metrics.snapshot()["requests"].values())
                         if isinstance(metrics.snapshot().get("requests"), dict) else 2)


class TheSizeIsNotReadOnEveryRequestTest(unittest.TestCase):
    """Reading it costs about 144us against a whole record() at about 5us -- 28 times the work
    the call otherwise does. It is read only when a sample is due, and outside the lock, so a
    burst of traffic pays for it once per interval rather than once per request."""

    def test_a_burst_reads_the_size_once(self) -> None:
        metrics = gwm.GatewayMetrics()
        calls = []
        saved = gwm.worker_resident
        gwm.worker_resident = lambda: (calls.append(1), {"resident_bytes": 1234567})[1]
        try:
            for _ in range(200):
                metrics.record(path="/v1/retrieve", method="POST", status=200, duration_s=0.01)
        finally:
            gwm.worker_resident = saved
        # The first record takes the opening sample; nothing after it is due for 15 seconds.
        self.assertLessEqual(len(calls), 2, len(calls))

    def test_and_it_is_read_at_all(self) -> None:
        """The floor: `assertLessEqual(len(calls), 2)` is satisfied perfectly by never reading."""
        metrics = gwm.GatewayMetrics()
        calls = []
        saved_interval, saved_reader = gwm.SERIES_INTERVAL_S, gwm.worker_resident
        gwm.SERIES_INTERVAL_S = 0.0
        gwm.worker_resident = lambda: (calls.append(1), {"resident_bytes": 1234567})[1]
        try:
            for _ in range(3):
                metrics.record(path="/v1/retrieve", method="POST", status=200, duration_s=0.01)
                time.sleep(0.002)
        finally:
            gwm.SERIES_INTERVAL_S, gwm.worker_resident = saved_interval, saved_reader
        self.assertGreaterEqual(len(calls), 2, len(calls))


class TheChartCodeExistsTwiceAndAgreesTest(unittest.TestCase):
    """`sparkline` and `renderTrend` live in the shipped page AND in the builder that writes
    pages. The harness runs the PAGE's copy, so an edit to one alone passes every test here while
    the next generated page keeps the old behaviour."""

    FILES = (os.path.join(PORTAL, "setup_portal.html"),
             os.path.join(PORTAL, "build_portal_pages.py"))
    FUNCTIONS = ("sparkline", "renderTrend")

    @staticmethod
    def _body(path: str, name: str) -> str:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        start = text.find("function %s(" % name)
        if start < 0:
            raise AssertionError("%s is not in %s" % (name, os.path.basename(path)))
        depth = 0
        for index in range(text.index("{", start), len(text)):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    return text[start:index + 1]
        raise AssertionError("%s is not closed in %s" % (name, os.path.basename(path)))

    def test_both_files_hold_both_functions(self) -> None:
        # The floor: a reader that finds nothing makes the equality below "" == "".
        for name in self.FUNCTIONS:
            for path in self.FILES:
                body = self._body(path, name)
                self.assertGreater(len(body), 300, (name, os.path.basename(path)))

    def test_they_are_the_same_function(self) -> None:
        for name in self.FUNCTIONS:
            page, builder = (self._body(path, name) for path in self.FILES)
            self.assertEqual(page, builder,
                             "%s differs between the page and the builder; the harness only "
                             "exercises the page" % name)


if __name__ == "__main__":
    unittest.main()
