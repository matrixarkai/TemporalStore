#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The strip says when the backend is down.

Readiness answers 503 when the datanode cannot serve, and there is a metric for it. Both are for
machines. Someone looking at the portal -- the surface a customer opens when something seems wrong
-- had no way to see it: the strip showed requests, imports, encoding and warnings, and stayed
reassuring while every write was failing.

Two halves, and they need different tests. Whether the GATEWAY puts the state on the frame is
Python; whether the STRIP draws it is JavaScript, and the strip's caller wraps render() in a catch
that ignores, so a segment that throws does nothing at all and looks fine in a diff.

The probe is deliberately not the one readiness records. That value only moves when something calls
/v1/readyz, so a deployment nobody probes would show a stale answer or none -- the strip should not
depend on somebody else's health check being configured.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")


def _helpers():
    """Imported per test, not at module import: importing a test module from a test module
    reorders `unittest discover` and can fail tests it has nothing to do with."""
    from test_matrixark_v1_gateway import _cfg, _factory_for, _FakeResponse, _FakeServer
    return _cfg, _factory_for, _FakeResponse, _FakeServer


def _refusing(_cfg_unused):
    raise OSError("connection refused")


class TheFrameCarriesTheDatanodeStateTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cfg, self.factory_for, self.FakeResponse, self.FakeServer = _helpers()
        gw._reset_live_cache()
        self.addCleanup(gw._reset_live_cache)

    def _frame_for(self, factory):
        cfg = self.cfg(blob_connection_factory=factory)
        state = asyncio.run(gw._datanode_for_frame(cfg))
        return asyncio.run(gw._event_frame(self.FakeServer(), cfg, "k", "acme", None, None,
                                           datanode=state))

    def test_a_healthy_datanode_is_reported_as_ok(self) -> None:
        self.assertEqual("ok", self._frame_for(self.factory_for(self.FakeResponse(200)))["datanode"])

    def test_an_erroring_datanode_is_reported(self) -> None:
        frame = self._frame_for(self.factory_for(self.FakeResponse(503)))
        self.assertEqual("erroring", frame["datanode"])

    def test_an_unreachable_datanode_is_reported(self) -> None:
        self.assertEqual("unreachable", self._frame_for(_refusing)["datanode"])

    def test_the_field_is_actually_on_the_frame(self) -> None:
        """The harness feeds frames directly, so only this can catch the field being dropped."""
        frame = self._frame_for(self.factory_for(self.FakeResponse(200)))
        self.assertIn("datanode", frame)

    def test_it_is_probed_once_and_shared(self) -> None:
        """Deployment-wide state, so eight open tabs must not mean eight probes per tick."""
        calls = {"n": 0}
        real = gw._probe_datanode

        def counted(cfg):
            calls["n"] += 1
            return real(cfg)

        gw._probe_datanode = counted
        self.addCleanup(setattr, gw, "_probe_datanode", real)
        gw._reset_live_cache()
        cfg = self.cfg(blob_connection_factory=self.factory_for(self.FakeResponse(200)))

        async def eight_viewers():
            return await asyncio.gather(*[gw._datanode_for_frame(cfg) for _ in range(8)])

        asyncio.run(eight_viewers())
        self.assertEqual(1, calls["n"],
                         "eight viewers caused %d probes of one deployment's backend" % calls["n"])

    def test_the_refresh_is_slower_than_the_tick(self) -> None:
        """It is the one part of a frame that costs an outbound connection."""
        self.assertGreater(gw.DATANODE_REFRESH_S, gw.EVENT_TICK_S * 5)

    def test_probing_keeps_the_metric_fresh(self) -> None:
        """So the series does not depend on an orchestrator polling readiness."""
        import matrixark_gateway_metrics as gwm
        before = getattr(gwm.METRICS, "_datanode", None)
        self.addCleanup(setattr, gwm.METRICS, "_datanode", before)
        gw._reset_live_cache()
        asyncio.run(gw._datanode_for_frame(self.cfg(blob_connection_factory=_refusing)))
        got = [l for l in gwm.METRICS.prometheus_lines()
               if l.startswith("matrixark_gateway_datanode_reachable")]
        self.assertEqual(["matrixark_gateway_datanode_reachable 0"], got)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the strip JS cannot be run")
class TheStripDrawsItTest(unittest.TestCase):
    """render() is wrapped by its caller in a catch that ignores, so a throwing segment is silent."""

    def _run(self, page):
        return subprocess.run(
            ["node", os.path.join(PORTAL, "datanode_segment_harness.js"),
             os.path.join(PORTAL, page)],
            capture_output=True, text=True, timeout=180)

    def test_the_segment_works_on_a_generated_page(self) -> None:
        proc = self._run("overview_portal.html")
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_segment_works_on_a_hand_kept_page(self) -> None:
        """The strip is shared, so a change to it has to land on both kinds of page."""
        proc = self._run("api_key_portal.html")
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_healthy_backend_takes_no_space(self) -> None:
        out = self._run("overview_portal.html").stdout
        self.assertIn("ok   a healthy datanode takes no space", out, out)

    def test_an_unexpected_value_is_not_rendered(self) -> None:
        """A closed set of known states, not escaping: this block has no esc() to reach for."""
        out = self._run("overview_portal.html").stdout
        self.assertIn("ok   an unexpected value renders nothing at all", out, out)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheDiagnosticsBundleCarriesItTest(unittest.TestCase):
    """What a customer sends to support should include what was wrong.

    The bundle already carried the overview, the config and the whole /v1/metrics text, so the
    counters were in it -- how many requests failed. It could not say WHEN, because the failure
    timeline exists only on the live frame and a bundle assembled from endpoints alone cannot
    reach it. Nor did it record the readiness verdict, which is the single clearest statement of
    whether the deployment could serve at all.

    Run rather than read: the bundle is built in a closure from a frame plus three fetches, and a
    field referencing something the page never stored comes out null while the source looks right.
    """

    def _run(self):
        return subprocess.run(
            ["node", os.path.join(PORTAL, "bundle_harness.js"),
             os.path.join(PORTAL, "overview_portal.html")],
            capture_output=True, text=True, timeout=180)

    def test_the_bundle_assembles_and_carries_the_new_evidence(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_it_still_carries_what_it_always_did(self) -> None:
        """Adding to a support bundle must not quietly drop what was already in it."""
        out = self._run().stdout
        self.assertIn("ok   it still carries what it always did", out, out)

    def test_a_failing_readiness_is_recorded_rather_than_dropped(self) -> None:
        """Readiness answers 503 when the backend is down -- that is the answer, not a failure
        to collect. A bundle that omits the one thing that was wrong is worse than one that says
        it could not look."""
        out = self._run().stdout
        self.assertIn("ok   a 503 from readiness is recorded, not dropped as a failed collection",
                      out, out)

    def test_the_timeline_says_when(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   the timeline says when, not just how many", out, out)


if __name__ == "__main__":
    unittest.main()
