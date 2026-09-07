#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A goodbye that is a fault is not a goodbye that is a rotation.

The gateway closes a stream every ten minutes on purpose and says so, and it now says so when it
breaks as well. Both are ``event: bye``. The client set ``planned`` for either -- and ``planned``
is precisely what makes the next reconnect immediate, so a gateway that had just said it was
failing got reconnected into at once, by a page showing nothing wrong.

That is the hot loop the reason exists to make visible, driven from this end.

A rotation still reconnects at once, because it is not a fault. A fault clears ``planned``, takes
the backoff, and reports itself with the token that names the log entry -- the one string in the
message that leads anywhere.

Driven through the SHIPPED client. What a page does with a goodbye is the whole change, and a
reading of the source cannot tell an immediate reconnect from a backed-off one.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "bye_reason_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")


def drive(bye: dict, keep_open: bool = False) -> dict:
    out = subprocess.run(
        ["node", HARNESS, PAGE, json.dumps({"bye": bye, "keepOpen": keep_open})],
        capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-900:])
    return json.loads(out.stdout)


class AGoodbyeThatIsAFaultTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_rotation_is_not_reported_as_a_fault(self) -> None:
        """It happens every ten minutes on every open tab. Reporting it would put a fault on the
        screen of every healthy deployment, six times an hour."""
        said = drive({"reason": "stream_max_age"}, keep_open=True)
        self.assertFalse(said["reportedAFault"])

    def test_a_fault_is_reported(self) -> None:
        said = drive({"reason": "server_error", "incident": "fa9ce4d24331"})
        self.assertTrue(said["reportedAFault"])

    def test_the_fault_carries_the_token(self) -> None:
        """Without it the page has a sentence and no way to get any further, and the operator has
        a log they cannot tie to the report."""
        self.assertEqual("fa9ce4d24331",
                         drive({"reason": "server_error", "incident": "fa9ce4d24331"})["faultIncident"])

    def test_a_fault_backs_off_instead_of_reconnecting_at_once(self) -> None:
        """The behaviour, not the message. Reconnecting immediately into a gateway that has just
        said it is failing is the loop the reason was added to expose."""
        said = drive({"reason": "server_error", "incident": "abc"})
        self.assertTrue(said["backedOff"])
        self.assertFalse(said["reconnectedImmediately"])

    def test_a_rotation_still_reconnects_at_once(self) -> None:
        """The complement, and the reason a fault cannot simply be treated as one: a rotation is
        expected and reconnecting immediately is right for it. Losing that would put every open
        tab through a backoff every ten minutes for no reason."""
        said = drive({"reason": "stream_max_age"})
        self.assertTrue(said["reconnectedImmediately"])
        self.assertFalse(said["reportedAFault"])

    def test_a_goodbye_with_no_reason_is_still_a_rotation(self) -> None:
        """An older gateway says goodbye with no body at all, and it is not faulty for that."""
        self.assertFalse(drive({}, keep_open=True)["reportedAFault"])

    def test_the_stream_gets_as_far_as_live(self) -> None:
        """The floor. Every assertion above is satisfied by a client that never connects, and a
        client that never connects reports no faults for the most boring reason there is."""
        said = drive({"reason": "server_error", "incident": "abc"})
        states = [entry["state"] for entry in said["states"]]
        self.assertIn("live", states, "the harness never got a working stream: %s" % states)


class BothClientsReadTheReasonTest(unittest.TestCase):
    """The strip has its own reader, on every page, and it had the same bug. It has no message
    line, so what it can say goes in the dot's title -- but it must still tell the two apart."""

    def test_the_strip_tells_a_rotation_from_a_fault(self) -> None:
        for name in sorted(f for f in os.listdir(PORTAL) if f.endswith("_portal.html")):
            with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
                text = handle.read()
            with self.subTest(page=name):
                self.assertIn("the stream failed on the gateway", text,
                              "%s never says a stream failed" % name)
                self.assertGreaterEqual(
                    text.count("stream_max_age"), 1,
                    "%s treats every goodbye the same" % name)


if __name__ == "__main__":
    unittest.main()
