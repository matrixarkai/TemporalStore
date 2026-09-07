#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The diagnostics bundle is the file that leaves the building.

It is downloaded and sent to somebody who was not at the keyboard, cannot ask which machine
collected it, and cannot ask whether the page was still receiving anything at the time. It carried
neither answer.

**It dated itself with the browser's clock.** `collected_at` came from `new Date()`, while
`recent_failures` inside the same file carries GATEWAY timestamps -- so a reader lining the two up
was comparing two machines, with nothing in the document saying so. It now carries the gateway's
own time for the same instant, and the difference between them.

**It did not say how fresh its live parts were.** `recent_failures` and `datanode` come off the
stream; everything else is fetched when the button is pressed. On a stalled stream the file holds
one current half and one arbitrarily old half, presented as one document.

Both answers are absent rather than guessed when no frame has arrived: repeating the browser's
clock under a second name would be worse than saying nothing, because it would read as
corroboration.
"""
from __future__ import annotations

import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
#: Not `bundle_harness.js`: that one drives the whole page and reports its own ok/FAIL
#: lines, and overwriting it took four of its tests with it.
HARNESS = os.path.join(PORTAL, "bundle_clock_harness.js")
PAGE = os.path.join(PORTAL, "overview_portal.html")

#: The harness's browser clock, in seconds. A server timestamp equal to this is two clocks agreeing.
BROWSER_TS = 1760000000


def collect(**case) -> dict:
    out = subprocess.run(["node", HARNESS, PAGE, json.dumps(case)],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-900:])
    return json.loads(out.stdout)["bundle"]


class ItCarriesBothClocksTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_it_carries_the_gateways_time_as_well_as_the_browsers(self) -> None:
        bundle = collect(serverTs=BROWSER_TS)
        self.assertIn("collected_at", bundle)
        self.assertEqual(bundle["collected_at"], bundle["gateway_time"])
        self.assertEqual(0, bundle["clock_skew_ms"])

    def test_a_skewed_browser_is_recorded_not_hidden(self) -> None:
        """The case the file exists for: the reader can see which clock to trust."""
        bundle = collect(serverTs=BROWSER_TS + 60)
        self.assertNotEqual(bundle["collected_at"], bundle["gateway_time"])
        self.assertEqual(60_000, bundle["clock_skew_ms"])

    def test_with_no_frame_it_says_nothing_rather_than_guessing(self) -> None:
        """Repeating the browser's clock under a second name would read as corroboration."""
        bundle = collect(withStream=False)
        self.assertIsNone(bundle["gateway_time"])
        self.assertIsNone(bundle["clock_skew_ms"])
        self.assertIsNotNone(bundle["collected_at"])


class ItSaysHowFreshItsLivePartsAreTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_live_stream_is_recorded_as_current(self) -> None:
        stream = collect(serverTs=BROWSER_TS)["stream"]
        self.assertTrue(stream["connected"])
        self.assertFalse(stream["stalled"])
        self.assertEqual(0, stream["since_last_block_ms"])

    def test_a_stalled_stream_is_recorded_as_stalled(self) -> None:
        stream = collect(serverTs=BROWSER_TS, advanceMs=8000)["stream"]
        self.assertTrue(stream["connected"])
        self.assertTrue(stream["stalled"])
        self.assertEqual(8000, stream["since_last_block_ms"])

    def test_never_connected_is_not_the_same_as_quiet(self) -> None:
        stream = collect(withStream=False)["stream"]
        self.assertFalse(stream["connected"])
        self.assertIsNone(stream["since_last_block_ms"])


class TheRestOfTheBundleIsUnchangedTest(unittest.TestCase):
    """The floor. Everything this file already carried has to still be in it."""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_the_existing_fields_are_all_still_there(self) -> None:
        bundle = collect(serverTs=BROWSER_TS)
        for field in ("collected_at", "origin", "overview", "config", "metrics", "readiness",
                      "recent_failures", "datanode"):
            self.assertIn(field, bundle)

    def test_the_failures_off_the_frame_still_arrive(self) -> None:
        """`recent_failures` exists only on the live frame, which is why the freshness of the
        stream is worth recording beside it."""
        bundle = collect(serverTs=BROWSER_TS)
        self.assertTrue(bundle["recent_failures"])
        self.assertEqual("ok", bundle["datanode"])


if __name__ == "__main__":
    unittest.main()
