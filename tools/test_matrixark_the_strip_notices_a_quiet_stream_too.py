#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The strip is on every page and said "live" whenever the socket was open.

mx#1189 taught the page-side stream client that a connection which has stopped delivering is not a
live one. **The strip runs its own client** -- five of the seven pages never load the other one --
so on those five the dot stayed green however long the gateway had been silent.

Silence alone is still not the signal: when nothing has changed the gateway sends `: keepalive`
instead of a frame, so a quiet stream on an idle deployment is healthy. What is not normal is
nothing at all, for three ticks of the gateway's own cadence, which every frame carries.

Two clients now apply one rule, which is a thing that can drift.
`TheTwoClientsAgreeTest` is the answer to that: both must read `tick_s` off the frame, both must
stamp on EVERY block rather than on frames, and neither may write the interval down.
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "strip_quiet_harness.js")
PAGE = os.path.join(PORTAL, "explore_portal.html")   # a page that runs the STRIP's own client


def drive(**case) -> list:
    out = subprocess.run(["node", HARNESS, PAGE, json.dumps(case)],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-900:])
    return json.loads(out.stdout)["states"]


def final(states: list) -> tuple:
    return states[-1]["className"], states[-1]["title"]


class TheDotNoticesSilenceTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_two_ticks_of_quiet_leaves_it_alone(self) -> None:
        """The floor: an idle deployment sends keepalives and is perfectly healthy."""
        self.assertEqual(("live-dot", "live"), final(drive(tickS=2, advanceMs=[2000, 2000])))

    def test_four_ticks_of_quiet_turns_it(self) -> None:
        css, title = final(drive(tickS=2, advanceMs=[2000, 2000, 2000, 2000]))
        self.assertIn("stale", css)
        self.assertEqual("no update for 8s", title)

    def test_anything_arriving_turns_it_back(self) -> None:
        """A keepalive carries no frame and no data and is exactly the evidence of life."""
        states = drive(tickS=2, advanceMs=[2000, 2000, 2000, 2000], thenAKeepalive=100)
        self.assertIn("stale", states[-2]["className"])
        self.assertEqual(("live-dot", "live"), final(states))

    def test_a_slower_gateway_gets_a_longer_leash(self) -> None:
        """Eight seconds is a stall at a two-second cadence and not at a ten-second one."""
        self.assertEqual(("live-dot", "live"),
                         final(drive(tickS=10, advanceMs=[2000, 2000, 2000, 2000])))

    def test_a_gateway_that_states_no_cadence_is_not_second_guessed(self) -> None:
        self.assertEqual(("live-dot", "live"),
                         final(drive(advanceMs=[2000, 2000, 2000, 2000, 60000])))


class TheTwoClientsAgreeTest(unittest.TestCase):
    """One rule, written twice, is a rule that drifts. These are the parts of it that matter.

    Both regions are cut by matching braces from a named anchor rather than by taking a fixed
    number of characters -- the first version of this test took 6,000 either side and checked
    neither the code it meant to.
    """

    @staticmethod
    def _builder() -> str:
        with io.open(os.path.join(PORTAL, "build_portal_pages.py"), encoding="utf-8") as handle:
            return handle.read()

    @staticmethod
    def _braced(text: str, anchor: str) -> str:
        start = text.index(anchor)
        depth = 0
        for index in range(text.index("{", start), len(text)):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    return text[start:index + 1]
        raise AssertionError("unclosed block at %r" % anchor)

    def _rules(self) -> dict:
        """The staleness decision in each client, as source."""
        text = self._builder()
        strip_marker = text.index('__matrixarkLive = "strip"')
        return {
            "shared": self._braced(text, "function streamStalledFor()"),
            "strip": self._braced(text[strip_marker:], "function watchQuiet()"),
        }

    def test_each_client_has_one(self) -> None:
        """The floor: an empty string satisfies every assertion below."""
        for name, rule in self._rules().items():
            with self.subTest(client=name):
                self.assertGreater(len(rule), 80, rule)

    def test_both_wait_three_ticks(self) -> None:
        for name, rule in self._rules().items():
            with self.subTest(client=name):
                self.assertTrue(re.search(r"\*\s*3\b", rule),
                                "this client no longer waits three ticks: %s" % rule)

    def test_both_take_the_interval_from_the_gateway(self) -> None:
        """The whole reason the gateway states it. A literal here is a second place to be wrong."""
        for name, rule in self._rules().items():
            with self.subTest(client=name):
                comparison = [line for line in rule.splitlines() if "* 3" in line]
                self.assertTrue(comparison, "no three-tick comparison in: %s" % rule)
                for line in comparison:
                    # The thing multiplied by three must be the cadence, not a number. Checking the
                    # comparison rather than banning digits: `Math.round(quiet / 1000)` is a
                    # milliseconds-to-seconds divisor and caught a cruder rule.
                    self.assertTrue(re.search(r"\b(tickMs|liveTickMs)\s*\*\s*3\b", line),
                                    "three ticks of WHAT? %s" % line.strip())

    def _bodies(self) -> dict:
        """Each client's whole body. Both `tick_s` reads are inside them -- the shared one in
        `handle`, the strip's in its pump -- so anything narrower misses the code it checks."""
        text = self._builder()
        strip_marker = text.index('__matrixarkLive = "strip"')
        return {
            "shared": self._braced(text, "function liveStream(options)"),
            "strip": self._braced(text[strip_marker:], "function open()"),
        }

    def test_both_read_the_cadence_off_a_frame(self) -> None:
        for name, body in self._bodies().items():
            with self.subTest(client=name):
                self.assertIn("tick_s", body)

    def test_both_bodies_are_really_the_clients(self) -> None:
        """The floor for the test above: a region that happened to be empty would pass nothing."""
        for name, body in self._bodies().items():
            with self.subTest(client=name):
                self.assertIn("getReader", body, "this is not a stream client: %s" % body[:120])

    def test_the_strip_stamps_on_every_block_not_every_frame(self) -> None:
        """A frame arrives only when something changed; a keepalive is what says an idle stream is
        alive. Stamping on frames would call every idle deployment stale."""
        text = self._builder()
        region = text[text.index('__matrixarkLive = "strip"'):]
        stamp = region.index("blockAt = Date.now();")
        status_check = region.index('if (name !== "status"')
        self.assertLess(stamp, status_check,
                        "the stamp happens after the status check, so keepalives do not count")


if __name__ == "__main__":
    unittest.main()
