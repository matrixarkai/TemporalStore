#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Readiness reports what the probe found.

`/v1/readyz` probed the datanode, learned it could not serve, and answered
`200 {"ready": true}` regardless. Orchestrators route on the status code, so a gateway whose
backend was unreachable stayed in rotation and kept being handed requests it could not fulfil --
the one thing a readiness probe exists to prevent.

The same fix has already been made twice elsewhere in this system: a drained proxy fails its
readiness probe, and a metaserver that cannot serve fails its readiness probe. The gateway's own
was missed.

The two failure labels were also the wrong way round, which is why reading the body did not give it
away. The probe returned None when the connection failed -- nothing listening at all -- and that
was reported as `"unknown"`; it returned False when the datanode answered with a 5xx, and that was
reported as `"unreachable"`. The reassuring word described the worse state.

The only test here before was the happy path, which is exactly why none of this was caught.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import (  # noqa: E402
    _cfg, _factory_for, _FakeResponse, _FakeServer, drive,
)


def _refusing(_cfg_unused):
    raise OSError("connection refused")


class ReadinessReportsWhatItFoundTest(unittest.TestCase):

    def setUp(self) -> None:
        self.server = _FakeServer()

    def _readyz(self, factory):
        app = gw.make_v1_app(self.server, _cfg(blob_connection_factory=factory))
        status, _headers, body = drive(app, method="GET", path="/v1/readyz")
        return status, json.loads(body)

    def test_a_healthy_datanode_is_ready(self) -> None:
        status, body = self._readyz(_factory_for(_FakeResponse(200)))
        self.assertEqual(200, status)
        self.assertTrue(body["ready"])
        self.assertEqual("ok", body["datanode"])

    def test_a_datanode_answering_5xx_is_not_ready(self) -> None:
        status, body = self._readyz(_factory_for(_FakeResponse(503)))
        self.assertEqual(503, status,
                         "the probe found a datanode that cannot serve and the gateway still "
                         "reported itself routable")
        self.assertFalse(body["ready"])
        self.assertEqual("erroring", body["datanode"])

    def test_a_datanode_that_cannot_be_reached_is_not_ready(self) -> None:
        status, body = self._readyz(_refusing)
        self.assertEqual(503, status)
        self.assertFalse(body["ready"])
        self.assertEqual("unreachable", body["datanode"],
                         "a refused connection is the least ambiguous failure there is, and it "
                         "used to be the one reported as 'unknown'")

    def test_the_status_code_and_the_body_agree(self) -> None:
        """A load balancer reads the code and a human reads the body; they must say one thing."""
        for factory in (_factory_for(_FakeResponse(200)), _factory_for(_FakeResponse(500)),
                        _refusing):
            status, body = self._readyz(factory)
            self.assertEqual(status == 200, body["ready"],
                             "HTTP %s with ready=%r" % (status, body["ready"]))

    def test_the_datanode_state_is_one_of_the_named_ones(self) -> None:
        for factory in (_factory_for(_FakeResponse(200)), _factory_for(_FakeResponse(500)),
                        _refusing):
            _status, body = self._readyz(factory)
            self.assertIn(body["datanode"], {"ok", "erroring", "unreachable"})


class LivenessIsNotReadinessTest(unittest.TestCase):
    """`/v1/healthz` must keep answering 200 while the process is alive.

    A liveness probe that fails on a dependency gets the container killed and restarted, which
    fixes nothing -- the datanode is still down -- and throws away whatever the process was doing.
    Readiness takes it out of rotation; liveness decides whether it should exist at all.
    """

    def setUp(self) -> None:
        self.server = _FakeServer()

    def test_liveness_survives_a_datanode_that_is_gone(self) -> None:
        app = gw.make_v1_app(self.server, _cfg(blob_connection_factory=_refusing))
        status, _headers, body = drive(app, method="GET", path="/v1/healthz")
        self.assertEqual(200, status,
                         "liveness failed because a dependency is down, so the orchestrator will "
                         "restart a process that has nothing wrong with it")
        self.assertEqual("ok", json.loads(body)["status"])

    def test_readiness_and_liveness_disagree_when_they_should(self) -> None:
        """The whole point of having two: same process, different questions, different answers."""
        app = gw.make_v1_app(self.server, _cfg(blob_connection_factory=_refusing))
        live, _h1, _b1 = drive(app, method="GET", path="/v1/healthz")
        ready, _h2, _b2 = drive(app, method="GET", path="/v1/readyz")
        self.assertEqual(200, live)
        self.assertEqual(503, ready)


class TheProbeNamesItsStatesTest(unittest.TestCase):

    def test_it_returns_a_name_rather_than_a_tri_state_bool(self) -> None:
        """None/False/True read fine at the call site and were reported under swapped names."""
        self.assertEqual("ok", gw._probe_datanode(_cfg(
            blob_connection_factory=_factory_for(_FakeResponse(204)))))
        self.assertEqual("erroring", gw._probe_datanode(_cfg(
            blob_connection_factory=_factory_for(_FakeResponse(500)))))
        self.assertEqual("unreachable", gw._probe_datanode(_cfg(
            blob_connection_factory=_refusing)))

    def test_a_4xx_from_the_datanode_is_still_serving(self) -> None:
        """The datanode answered. A 404 on /health is a wrong path, not a dead backend."""
        self.assertEqual("ok", gw._probe_datanode(_cfg(
            blob_connection_factory=_factory_for(_FakeResponse(404)))))


class MonitoringCanSeeItTooTest(unittest.TestCase):
    """The orchestrator acts on the status code. Nobody else could see the probe at all.

    Without a series, a deployment cannot alert on "the backend has been unreachable for two
    minutes" -- only notice later that instances were being cycled.
    """

    def setUp(self) -> None:
        import matrixark_gateway_metrics as gwm
        self.gwm = gwm
        self.metrics = gwm.GatewayMetrics()

    def _series(self, name):
        return [l for l in self.metrics.prometheus_lines()
                if l.startswith(name) and not l.startswith("#")]

    def test_the_family_is_declared_before_anything_has_looked(self) -> None:
        """An alert on a series nothing declares is an alert that can never fire."""
        declared = [l for l in self.metrics.prometheus_lines()
                    if l.startswith("# TYPE matrixark_gateway_datanode")]
        self.assertEqual(2, len(declared), declared)

    def test_there_is_no_sample_before_anything_has_looked(self) -> None:
        """A 0 would say the datanode is down when the truth is that nobody has asked."""
        self.assertEqual([], self._series("matrixark_gateway_datanode_reachable"))

    def test_a_healthy_probe_reads_one(self) -> None:
        self.metrics.note_datanode("ok")
        self.assertEqual(["matrixark_gateway_datanode_reachable 1"],
                         self._series("matrixark_gateway_datanode_reachable"))

    def test_either_failure_reads_zero(self) -> None:
        for state in ("erroring", "unreachable"):
            metrics = self.gwm.GatewayMetrics()
            metrics.note_datanode(state)
            got = [l for l in metrics.prometheus_lines()
                   if l.startswith("matrixark_gateway_datanode_reachable")]
            self.assertEqual(["matrixark_gateway_datanode_reachable 0"], got, state)

    def test_the_answer_comes_with_its_age(self) -> None:
        """A gauge with no age reads as current, and a stale 1 says fine for ever."""
        self.metrics.note_datanode("ok")
        age = self._series("matrixark_gateway_datanode_probe_age_seconds")
        self.assertEqual(1, len(age), age)
        self.assertLess(float(age[0].split()[-1]), 5.0)

    def test_calling_readiness_records_it(self) -> None:
        """The route is what writes the gauge; if it does not, the series never moves."""
        self.gwm.METRICS.note_datanode("ok")
        app = gw.make_v1_app(_FakeServer(), _cfg(blob_connection_factory=_refusing))
        drive(app, method="GET", path="/v1/readyz")
        got = [l for l in self.gwm.METRICS.prometheus_lines()
               if l.startswith("matrixark_gateway_datanode_reachable")]
        self.assertEqual(["matrixark_gateway_datanode_reachable 0"], got,
                         "readiness found an unreachable datanode and the gauge still says 1")


if __name__ == "__main__":
    unittest.main()
