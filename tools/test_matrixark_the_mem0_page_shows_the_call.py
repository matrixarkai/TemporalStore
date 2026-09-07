#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The mem0 API has its own page, and the page shows the call.

The console was a tab on Explore, beside four panels about *memories* rather than about the API.
It previewed the request line and printed the answer's body. What it never showed is what somebody
debugging a call actually asks for:

* the headers that went out -- "did it send Content-Type?" is a real question, and the form cannot
  answer it;
* the headers that came back;
* the **incident token**, when the gateway declines to say more. It is in the body, and it is the
  one string that leads anywhere, so it is called out rather than left to be found in the JSON;
* what the previous call did. Two runs side by side is how you tell a request that changed from a
  deployment that did.

**The key is never rendered.** The Authorization header is rebuilt as a redaction rather than
filtered out, so the header NAMES that went out are still visible while the value that would end up
pasted into a ticket is not in the DOM at all.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "mem0_wire_harness.js")
PAGE = os.path.join(PORTAL, "mem0_portal.html")

SENT = {
    "method": "POST",
    "url": "/v1/retrieve",
    "headers": {"Content-Type": "application/json", "Authorization": "Bearer sk-super-secret"},
    "body": {"query": "when do we ship?"},
}


def run(calls: list) -> dict:
    out = subprocess.run(["node", HARNESS, PAGE, json.dumps({"calls": calls})],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-900:])
    return json.loads(out.stdout)


def one(status: int = 200, body=None, headers=None, ms: int = 12) -> dict:
    return {"op": {"label": "search()"}, "sent": SENT,
            "answer": {"status": status, "ms": ms, "headers": headers or {"content-type": "application/json"},
                       "text": json.dumps(body or {"results": []}, indent=2), "body": body or {"results": []}}}


class TheExchangeIsShownTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_it_shows_what_went_out(self) -> None:
        wire = run([one()])["wire"]
        self.assertIn("POST", wire)
        self.assertIn("/v1/retrieve", wire)
        self.assertIn("Content-Type", wire)
        self.assertIn("when do we ship?", wire)

    def test_it_shows_what_came_back(self) -> None:
        wire = run([one(status=200, headers={"content-type": "application/json", "x-request-id": "r7"})])["wire"]
        self.assertIn("200", wire)
        self.assertIn("12 ms", wire)
        self.assertIn("x-request-id", wire)
        self.assertIn("r7", wire)

    def test_the_key_is_never_in_the_page(self) -> None:
        """The one header worth redacting, and the one people paste into tickets by accident."""
        wire = run([one()])["wire"]
        self.assertNotIn("sk-super-secret", wire)
        self.assertIn("Authorization", wire, "the header name is still worth showing")
        self.assertIn("not shown", wire)

    def test_an_incident_is_called_out_with_where_to_look(self) -> None:
        """It is the one string in a 5xx body that leads anywhere."""
        wire = run([one(status=500, body={"error": "backend_error", "incident": "a3f9c2d10b4e"})])["wire"]
        self.assertIn("a3f9c2d10b4e", wire)
        self.assertIn("matrixark.gateway", wire, "it does not say where to grep")

    def test_a_clean_answer_mentions_no_incident(self) -> None:
        """The floor. A banner on every call is a banner nobody reads."""
        self.assertNotIn("Incident", run([one()])["wire"])


class TheCallLogRemembersTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_each_call_adds_a_row(self) -> None:
        result = run([one(status=200), one(status=500, body={"incident": "beef"})])
        self.assertEqual(2, result["count"])
        self.assertIn("500", result["log"])
        self.assertIn("200", result["log"])

    def test_the_newest_is_first(self) -> None:
        """A log that grows downwards puts the call you just made off the bottom of the panel."""
        log = run([one(status=200, ms=11), one(status=418, ms=22)])["log"]
        self.assertLess(log.index("418"), log.index("200"))

    def test_a_row_carries_its_incident(self) -> None:
        log = run([one(status=500, body={"incident": "a3f9c2d10b4e"})])["log"]
        self.assertIn("a3f9c2d10b4e", log)

    def test_a_row_without_one_says_so_rather_than_blank(self) -> None:
        self.assertIn("—", run([one()])["log"])

    def test_the_time_goes_through_the_shared_helper(self) -> None:
        """So a row says which clock it is, like every other timestamp on the portal."""
        self.assertIn("AT:", run([one()])["log"])


class TheRunPathUsesThemTest(unittest.TestCase):
    """Computing the panels is not showing them. This is the wiring, read from the builder."""

    @staticmethod
    def _mem0_js() -> str:
        with io.open(os.path.join(PORTAL, "build_portal_pages.py"), encoding="utf-8") as handle:
            src = handle.read()
        return src[src.index("MEM0_JS = "):src.index("CATALOG_BODY = ")]

    @staticmethod
    def _branches(js: str) -> dict:
        """runOp's two endings, separately. Searching the whole function let the failure branch
        answer for the success branch -- a mutation that removed the success call survived."""
        run_op = js[js.index("function runOp()"):]
        # `.catch(function (` -- the catch now takes the error, because it classifies the failure
        # rather than assuming one. Anchoring on the empty argument list pinned a detail
        # that had nothing to do with what this test is about.
        catch_at = run_op.index(".catch(function (")
        return {"success": run_op[:catch_at], "failure": run_op[catch_at:catch_at + 800]}

    def test_a_completed_call_fills_both(self) -> None:
        success = self._branches(self._mem0_js())["success"]
        self.assertIn("renderWire(sent, answer)", success)
        self.assertIn("recordCall(op, sent, answer)", success)

    def test_a_request_that_never_left_is_recorded_too(self) -> None:
        """Otherwise the log omits exactly the runs somebody is trying to explain."""
        failure = self._branches(self._mem0_js())["failure"]
        self.assertIn("recordCall(op, sent, answer)", failure)
        self.assertIn("renderWire(sent, answer)", failure)

    def test_the_page_is_served_and_navigable(self) -> None:
        with io.open(os.path.join(PORTAL, "mem0_portal.html"), encoding="utf-8") as handle:
            page = handle.read()
        self.assertIn('id="opWire"', page)
        self.assertIn('id="opLog"', page)
        self.assertIn("/v1/admin/mem0", page)


if __name__ == "__main__":
    unittest.main()
