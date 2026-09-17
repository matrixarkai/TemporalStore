#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A retrieve that answered nothing is counted as having answered nothing.

Under load this gateway stops retrieving and starts SHEDDING. A shed response is:

    {"context_pack_id": "...", "groups": [], "tokens": {},
     "warnings": ["retrieval_deadline_exceeded:service_backpressure", "service_backpressure"],
     "partial": true, "insufficient_context": true}

**HTTP 200, ~471 bytes, ~100 ms**, against 42,000-62,000 bytes and hundreds of milliseconds for a
pack that carried something. Every series this build emitted before this was transport-level --
requests, duration, bytes -- so a deployment answering nothing looked not merely healthy but
FASTER THAN USUAL, because p50 improves as the packs empty out. It cost one of my own measurement
runs, which warmed each arm "until HTTP 200" and then measured a latency made entirely of
rejections.

The signal was already in the body. `warnings`, `partial` and `insufficient_context` are right
there and nothing read them -- not the metrics, not the portal, not the gateway. So this classifies
from fields the response already carries rather than from its size; gating on size is the mistake
that voided the run.

`empty` and `shed` are counted separately because they are different problems. Shed is load: it
clears when the load does. Empty with no warning is a populated store returning nothing, which does
not clear, and is the one nobody could see at all.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as metricsmod  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402

# The gateway fixtures live in another TEST module, and importing one of those at import time
# reorders `unittest discover` -- which can fail tests this file has nothing to do with, in CI,
# while passing locally. They are imported where they are used instead.

# The shape measured on this stack, kept verbatim.
SHED = {"context_pack_id": "p", "groups": [], "tokens": {},
        "warnings": ["retrieval_deadline_exceeded:service_backpressure", "service_backpressure"],
        "partial": True, "insufficient_context": True}
EMPTY = {"context_pack_id": "p", "groups": [], "tokens": {}}
SERVED = {"context_pack_id": "p", "groups": [{"refs": [{"text": "staging = 1.9.2"}]}],
          "tokens": {"total": 7}}


def _pack_server(pack):
    """A fake server that answers every retrieve with one chosen pack.

    A factory rather than a module-level subclass: the base class lives in another test module, and
    naming it at import time is what the cross-import guard forbids. Subclassing rather than
    reimplementing, so this cannot drift from the fixture every other gateway test uses.
    """
    from test_matrixark_v1_gateway import _FakeServer

    class _PackServer(_FakeServer):
        def __init__(self, chosen):
            super().__init__()
            self.pack = chosen

        def call_tool(self, name, args):
            self.calls.append((name, dict(args)))
            if name == "matrixark_retrieve":
                return dict(self.pack)
            return {"ok": name}

    return _PackServer(pack)


def outcomes():
    return dict(metricsmod.METRICS.retrieve_outcomes())


def delta(before, after):
    return {k: after.get(k, 0) - before.get(k, 0)
            for k in set(before) | set(after) if after.get(k, 0) != before.get(k, 0)}


class TheClassifierReadsTheBodyTest(unittest.TestCase):
    """The rule itself, over the shapes it exists to tell apart."""

    def classify(self, payload):
        raw = json.dumps(payload).encode()
        return gw._retrieve_outcome(raw, len(raw))

    def test_a_shed_pack_is_shed(self) -> None:
        self.assertEqual("shed", self.classify(SHED))

    def test_an_empty_pack_with_no_warning_is_empty(self) -> None:
        """Not shed. Nothing said why, which is the whole difference."""
        self.assertEqual("empty", self.classify(EMPTY))

    def test_a_pack_with_a_group_is_served(self) -> None:
        self.assertEqual("served", self.classify(SERVED))

    def test_an_empty_pack_with_an_unrelated_warning_is_still_empty(self) -> None:
        """Only backpressure means shedding. A pack that warns about something else and came back
        empty is the condition that does not clear on its own."""
        self.assertEqual("empty", self.classify(
            dict(EMPTY, warnings=["summary_truncated"])))

    def test_a_body_past_the_cap_is_served_without_parsing(self) -> None:
        """A pack with no groups is small by construction, so anything large carried content.
        This is also what keeps a 62 KB pack from being copied and parsed on the response path."""
        self.assertEqual("served", gw._retrieve_outcome(b"", gw._RETRIEVE_CLASSIFY_CAP + 1))

    def test_an_unreadable_body_is_not_reported_as_a_fault(self) -> None:
        """A body this cannot parse is not evidence of anything, and inventing a fault from it
        would put noise into the one series that exists to be trusted."""
        self.assertEqual("served", gw._retrieve_outcome(b"{not json", 9))
        self.assertEqual("served", gw._retrieve_outcome(b"[]", 2))

    def test_a_200_that_is_not_a_pack_is_not_counted_as_empty(self) -> None:
        self.assertEqual("served", self.classify({"ok": True}))


class ItIsActuallyWiredTest(unittest.TestCase):
    """The control. The rule above could be perfect and never called.

    Driven end to end through the real app, so this also proves the observation point sees both
    dispatch paths -- it wraps every response rather than instrumenting a branch.
    """

    def _drive(self, pack):
        from test_matrixark_v1_gateway import _cfg, drive
        app = gw.make_v1_app(_pack_server(pack), _cfg())
        before = outcomes()
        status, _, _ = drive(app, method="POST", path="/v1/retrieve",
                             headers={"Authorization": "Bearer k-acme"},
                             body={"query": "anything", "scope": {"user_id": "u"}})
        return status, delta(before, outcomes())

    def test_a_shed_retrieve_moves_the_shed_counter(self) -> None:
        status, moved = self._drive(SHED)
        self.assertEqual(200, status, "the whole point is that this is a 200")
        self.assertEqual({"shed": 1}, moved)

    def test_an_empty_retrieve_moves_the_empty_counter(self) -> None:
        status, moved = self._drive(EMPTY)
        self.assertEqual(200, status)
        self.assertEqual({"empty": 1}, moved)

    def test_a_served_retrieve_moves_the_served_counter(self) -> None:
        status, moved = self._drive(SERVED)
        self.assertEqual(200, status)
        self.assertEqual({"served": 1}, moved)

    def test_another_route_that_also_answers_200_moves_nothing(self) -> None:
        """Only /v1/retrieve is classified. A counter that also moved on another route would make
        the ratio in the alert meaningless.

        The route has to answer **200**. The first version of this drove /v1/ingest, which answers
        202 -- so the classifier never ran on it for a reason that had nothing to do with the path
        check, and a mutation making every route count as a retrieve sailed past.
        """
        from test_matrixark_v1_gateway import _cfg, drive
        app = gw.make_v1_app(_pack_server(SERVED), _cfg())
        status, _, _ = drive(app, method="POST", path="/v1/memories",
                             headers={"Authorization": "Bearer k-acme"},
                             body={"scope": {"user_id": "u"}})
        self.assertEqual(200, status,
                         "this route must answer 200, or it cannot test the path check")
        before = outcomes()
        drive(app, method="POST", path="/v1/memories",
              headers={"Authorization": "Bearer k-acme"},
              body={"scope": {"user_id": "u"}})
        self.assertEqual({}, delta(before, outcomes()))


class TheSeriesIsSafeToScrapeTest(unittest.TestCase):

    def test_every_outcome_is_emitted_even_at_zero(self) -> None:
        """A counter that appears only once it fires cannot be alerted on before it has fired,
        which is the moment the alert was worth having -- and `served` at zero beside a rising
        `shed` is exactly the shape somebody needs to see.

        The counters are CLEARED first. Without that this ran after its siblings had already fired
        all three outcomes, so the emitted set contained them however the lines were built, and a
        mutation emitting only what had fired passed.
        """
        with metricsmod.METRICS._lock:
            saved = dict(metricsmod.METRICS._retrieve)
            metricsmod.METRICS._retrieve.clear()
        try:
            text = "\n".join(metricsmod.retrieve_lines())
        finally:
            with metricsmod.METRICS._lock:
                metricsmod.METRICS._retrieve.update(saved)
        for outcome in metricsmod.RETRIEVE_OUTCOMES:
            with self.subTest(outcome=outcome):
                self.assertIn('outcome="%s"' % outcome, text)
                self.assertIn('outcome="%s"} 0' % outcome, text,
                              "emitted, but not at zero -- the clear above did not take")

    def test_the_outcome_set_is_closed(self) -> None:
        """This is a metric label. A value taken from an open source is a cardinality bomb."""
        self.assertEqual(("served", "empty", "shed"), metricsmod.RETRIEVE_OUTCOMES)

    def test_an_unrecognised_outcome_is_dropped(self) -> None:
        """The recorder is not a trusted source of label values, even though its only caller is
        this module: one new call site passing something through is all it would take."""
        before = outcomes()
        metricsmod.METRICS.record("/v1/retrieve", "POST", 200, 0.01,
                                  retrieve_outcome="tenant-acme-corp")
        self.assertEqual({}, delta(before, outcomes()))

    def test_it_reaches_the_scrape(self) -> None:
        """Every assertion above passes on a function nothing calls."""
        text = metricsmod.prometheus_text({"extraction": {"provider": "deterministic"},
                                           "embedding": {"provider": "deterministic"},
                                           "warnings": []})
        self.assertIn("matrixark_gateway_retrieve_outcomes_total", text)


if __name__ == "__main__":
    unittest.main()
