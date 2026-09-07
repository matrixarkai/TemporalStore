#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
""""14s ago" was this browser's clock minus a server timestamp.

Every live frame carries the gateway's own `ts`, and no page read it. `agoText` computed
``Date.now() / 1000 - at`` where `at` comes from the server, and clamped the result at zero -- so
the number measured the difference between two machines as much as the age of the thing.

Measured through the shipped code, for a failure recorded 90 seconds before the gateway's clock:

    browser in step with the gateway     2m ago     (right)
    browser 60s BEHIND                   30s ago    (wrong)
    browser 60s AHEAD                    3m ago     (wrong)

A browser far enough behind reads "0s ago" for everything, for ever -- on the one panel whose
stated job is to tell you whether a failure was a minute ago or last Tuesday.

The stream now records the server's clock from each frame and `agoText` measures against it. The
skew is reported too, because it is not only these ages that were wrong: every absolute time on
these pages is rendered with `toLocaleString()` from the same browser clock, so one wrong clock
makes all of them wrong together, and only this panel can notice.

**Driven through the shipped stream parser.** Setting the clock variables directly would test the
arithmetic rather than the part that did not exist -- reading `ts` off a frame.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "clock_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")

#: The gateway's clock in these cases, and a failure recorded 90 seconds before it.
SERVER_TS = 1760000000
FAILED_AT = SERVER_TS - 90


FAILURE_ROW = [{"at": FAILED_AT, "status": 500, "method": "POST", "route": "/v1/ingest"}]


def drive(browser_offset_ms: int, frames=None, at: int = FAILED_AT) -> dict:
    payload = {"browserOffsetMs": browser_offset_ms, "at": at, "failures": FAILURE_ROW,
               "frames": [{"ts": SERVER_TS}] if frames is None else frames}
    out = subprocess.run(["node", HARNESS, PAGE, json.dumps(payload)],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-900:])
    return json.loads(out.stdout)


class TheAgeIsTheDeploymentsTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_browser_in_step_is_unchanged(self) -> None:
        """The floor: this is what the panel already showed, and it must keep showing it."""
        result = drive(0)
        self.assertEqual("2m ago", result["before"]["ago"])
        self.assertEqual("2m ago", result["after"]["ago"])

    def test_a_browser_behind_the_gateway_used_to_read_the_age_short(self) -> None:
        result = drive(-60_000)
        self.assertEqual("30s ago", result["before"]["ago"], "the defect is not reproduced")
        self.assertEqual("2m ago", result["after"]["ago"])

    def test_a_browser_ahead_of_the_gateway_used_to_read_it_long(self) -> None:
        result = drive(60_000)
        self.assertEqual("3m ago", result["before"]["ago"], "the defect is not reproduced")
        self.assertEqual("2m ago", result["after"]["ago"])

    def test_a_browser_far_behind_no_longer_reads_zero_for_everything(self) -> None:
        """The worst form: the clamp at zero turned every age into "0s ago" and nothing said why."""
        result = drive(-3_600_000)
        self.assertEqual("0s ago", result["before"]["ago"])
        self.assertEqual("2m ago", result["after"]["ago"])


class TheSkewIsReportedTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_it_says_which_way_the_browser_is_wrong(self) -> None:
        behind = drive(-60_000)["after"]["note"]
        ahead = drive(60_000)["after"]["note"]
        self.assertIn("60s behind", behind)
        self.assertIn("60s ahead of", ahead)

    def test_it_says_nothing_when_the_clocks_agree(self) -> None:
        self.assertEqual("", drive(0)["after"]["note"])

    def test_a_small_difference_is_not_worth_a_notice(self) -> None:
        """Below five seconds it cannot change how any of these ages read, and a caveat on every
        deployment is a caveat nobody reads."""
        self.assertEqual("", drive(-3_000)["after"]["note"])
        self.assertEqual("2m ago", drive(-3_000)["after"]["ago"])

    def test_the_notice_says_absolute_times_are_still_wrong(self) -> None:
        """The reason it is worth saying at all: the ages are fixed, the other timestamps on these
        pages are not, because they are rendered from the browser."""
        self.assertIn("every absolute time on these pages", drive(-60_000)["after"]["note"])


class TheNoticeReachesThePanelTest(unittest.TestCase):
    """Computing the sentence is not showing it. `renderFailures` is run and its output read."""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_the_panel_carries_the_notice(self) -> None:
        panel = drive(-60_000)["after"]["panel"]
        self.assertIn("60s behind", panel)

    def test_the_empty_panel_carries_it_too(self) -> None:
        """A deployment with no failures still has a wrong clock, and every other timestamp on the
        page is still being read against it."""
        self.assertIn("60s behind", drive(-60_000)["after"]["emptyPanel"])

    def test_the_panel_still_shows_the_failures(self) -> None:
        """The floor: the notice must be added to the table, not put in place of it."""
        panel = drive(-60_000)["after"]["panel"]
        self.assertIn("2m ago", panel)
        self.assertIn("/v1/ingest", panel)

    def test_a_panel_on_a_correct_clock_carries_no_notice(self) -> None:
        panel = drive(0)["after"]["panel"]
        self.assertNotIn("browser", panel)
        self.assertIn("2m ago", panel)


class AFrameWithoutAClockChangesNothingTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_frame_with_no_ts_leaves_the_browser_clock_in_use(self) -> None:
        """An older gateway, or a frame that carries no clock, must not invent one."""
        result = drive(-60_000, frames=[{"traffic": {}}])
        self.assertEqual(result["before"]["ago"], result["after"]["ago"])
        self.assertEqual(0, result["after"]["skewMs"])
        self.assertEqual("", result["after"]["note"])

    def test_before_any_frame_the_browser_clock_is_used(self) -> None:
        """The page renders once before the first frame arrives; it must not show nothing."""
        self.assertEqual("30s ago", drive(-60_000, frames=[])["before"]["ago"])


class TheGatewayStillSendsItTest(unittest.TestCase):
    """All of the above rests on the frame carrying `ts`. If it stops, the pages fall back to the
    browser's clock silently -- which is the state this change is fixing."""

    def test_the_frame_carries_the_servers_clock(self) -> None:
        with io.open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn('"ts": time.time(),', source)


if __name__ == "__main__":
    unittest.main()
