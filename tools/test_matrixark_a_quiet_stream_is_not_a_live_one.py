#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A stream that has stopped arriving still said "live".

`onState("live")` fired when the connection OPENED. Nothing after that depended on anything
arriving, so a gateway that stopped producing frames while the socket stayed open left the page
showing the last frame's numbers under a green dot, indefinitely.

**Silence alone is not the signal.** When nothing has changed the gateway sends `: keepalive`
instead of a frame, so a quiet stream on an idle deployment is healthy, and a page that treated "no
frame" as "stale" would say so on every deployment that was working. What is not normal is nothing
at all -- no frame and no keepalive.

The threshold is three ticks of the gateway's OWN cadence, which every frame now carries. A page holding its own copy of that interval would be a second place for it to be wrong, and
`test_a_slower_gateway_gets_a_longer_leash` is what stops it becoming one: with a ten-second
cadence, eight seconds of quiet is not a stall.

A gateway that sends no cadence claims nothing at all -- an older one behind a newer page keeps
exactly the behaviour it had.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "stall_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")


def drive(**case) -> dict:
    out = subprocess.run(["node", HARNESS, PAGE, json.dumps(case)],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-900:])
    result = json.loads(out.stdout)
    result["names"] = [entry["state"] for entry in result["states"]]
    return result


class AQuietStreamIsReportedTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_two_ticks_of_quiet_is_not_a_stall(self) -> None:
        """The floor. One missed tick is scheduling, two is a slow one -- and an idle deployment
        that is working must not be reported as broken."""
        self.assertEqual(["connecting", "live"], drive(tickS=2, advanceMs=[2000, 2000])["names"])

    def test_four_ticks_of_quiet_is(self) -> None:
        result = drive(tickS=2, advanceMs=[2000, 2000, 2000, 2000])
        self.assertEqual(["connecting", "live", "stalled"], result["names"])

    def test_it_says_how_long_it_has_been_quiet(self) -> None:
        result = drive(tickS=2, advanceMs=[2000, 2000, 2000, 2000])
        self.assertEqual(8, result["states"][-1]["seconds"])

    def test_it_is_reported_once_not_every_second(self) -> None:
        """The watchdog runs on a timer; a report per tick would be a page that scrolls."""
        result = drive(tickS=2, advanceMs=[2000, 2000, 2000, 2000, 1000, 1000, 1000])
        self.assertEqual(1, result["names"].count("stalled"))


class ItIsWithdrawnWhenTheStreamSpeaksTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_keepalive_is_enough_to_say_it_is_live_again(self) -> None:
        """A keepalive carries no frame and no data, and is exactly the evidence that the stream is
        alive -- so it must count."""
        result = drive(tickS=2, advanceMs=[2000, 2000, 2000, 2000], thenAKeepaliveAfterMs=100)
        self.assertEqual(["connecting", "live", "stalled", "live"], result["names"])


class TheThresholdIsTheGatewaysOwnCadenceTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_slower_gateway_gets_a_longer_leash(self) -> None:
        """Eight seconds of quiet is a stall at a two-second cadence and not at a ten-second one.
        A page with the interval written into it would call both a stall."""
        self.assertNotIn("stalled", drive(tickS=10, advanceMs=[2000, 2000, 2000, 2000])["names"])
        self.assertIn("stalled", drive(tickS=2, advanceMs=[2000, 2000, 2000, 2000])["names"])

    def test_a_gateway_that_states_no_cadence_is_not_second_guessed(self) -> None:
        """An older gateway sends frames without `tick_s`. Nothing is claimed about it, even after
        a minute."""
        result = drive(hello=False, advanceMs=[2000, 2000, 2000, 2000, 60000])
        self.assertNotIn("stalled", result["names"])

    def test_the_watchdog_is_actually_running(self) -> None:
        """The floor for the two above: they must be quiet because nothing was wrong, not because
        no timer was ever registered."""
        self.assertTrue(drive(hello=False, advanceMs=[1000])["watchdogRegistered"])


class TheGatewayStatesItsCadenceTest(unittest.TestCase):

    @staticmethod
    def _source(name: str) -> str:
        with io.open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
            return handle.read()

    def test_the_frame_carries_the_cadence(self) -> None:
        """On the frame rather than as its own event. Every reader of this stream treats a `data:`
        block as a frame, so a new event name arrives as a frame with none of the fields they
        expect -- which is what it did to four of the stream's own tests."""
        source = self._source("matrixark_v1_gateway.py")
        self.assertIn('"tick_s": EVENT_TICK_S,', source)
        self.assertNotIn('event: hello', source)

    def test_the_first_frame_always_goes_out(self) -> None:
        """What makes carrying it on the frame as early as announcing it: the loop starts with no
        recorded signature, so the first tick can never match and always emits."""
        source = self._source("matrixark_v1_gateway.py")
        self.assertIn("last_signature: Optional[bytes] = None", source)
        self.assertIn("if signature == last_signature:", source)

    def test_the_page_holds_no_copy_of_that_interval(self) -> None:
        """The whole reason it is sent. A number in both places is a number that can disagree."""
        with io.open(os.path.join(PORTAL, "build_portal_pages.py"), encoding="utf-8") as handle:
            builder = handle.read()
        start = builder.index("function streamStalledFor()")
        block = builder[start:builder.index("}", builder.index("return quiet", start))]
        self.assertIn("liveTickMs", block)
        self.assertNotIn("2000", block)

    def test_the_quiet_state_has_a_colour_of_its_own(self) -> None:
        """It is rendered with a class, and a class with no rule is an invisible dot."""
        self.assertIn(".dot.warn{background:var(--warn)}",
                      self._source(os.path.join("portal", "ingestion_portal.html")))
        self.assertIn('conn("warn", "no update for "',
                      self._source(os.path.join("portal", "setup_portal.html")))


if __name__ == "__main__":
    unittest.main()
