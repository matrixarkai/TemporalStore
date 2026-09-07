#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The per-key meter counts one process, and the panel is called "Live edge usage".

`GET /v1/admin/api_key_usage` returns the in-process edge counters. On a deployment started with
more than one worker those are the counters of whichever worker answered the request and no other,
so the totals are a share of the traffic rather than the whole of it.

The empty case is the more misleading of the two: a key used only through another worker is absent
from this worker's meter, which reads exactly like a key nobody used. Somebody checking whether a
key is live, or reading these numbers for billing, is wrong by a factor of the worker count.

The gateway already knows the number. `_worker_count()` exists for this reason -- its own docstring
says a live value is applied to "the environment of whichever worker served the request, and no
other" -- and two sibling reads already report it. This one did not, so a caller could not tell.

The panel is checked by RUNNING the shipped `loadUsage` against a DOM stub. Reading the source
would pass on a function that builds the sentence and never shows it.
"""
from __future__ import annotations

import ast
import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "usage_share_harness.js")
PAGE = os.path.join(PORTAL, "api_key_portal.html")


def render(usage: list, workers) -> dict:
    payload = {"response": {"status": "ok", "usage": usage, "count": len(usage)}}
    if workers is not None:
        payload["response"]["workers"] = workers
    out = subprocess.run(["node", HARNESS, PAGE, json.dumps(payload)],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    return json.loads(out.stdout)


def said(result: dict) -> str:
    return " ".join(entry["text"] for entry in result["shown"])


ROW = [{"api_key_hash": "h", "total": 7}]


class ThePanelSaysWhoseCountersTheseAreTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_an_empty_meter_on_several_workers_says_why(self) -> None:
        """The misleading case: a key used through another worker reads as a key nobody used."""
        text = said(render([], 4))
        self.assertIn("this deployment runs 4", text)
        self.assertIn("not counted here", text)

    def test_rows_on_several_workers_say_it_too(self) -> None:
        text = said(render(ROW, 4))
        self.assertIn("one worker's counters", text)

    def test_a_single_worker_is_told_nothing_extra(self) -> None:
        """The floor. A caveat on every deployment is a caveat nobody reads -- and on one worker
        the counters ARE the deployment's."""
        self.assertNotIn("one worker's counters", said(render(ROW, 1)))
        self.assertNotIn("one worker's counters", said(render([], 1)))

    def test_the_empty_message_still_says_it_is_empty(self) -> None:
        """The caveat is added to the existing sentence, not swapped for it."""
        self.assertIn("No usage recorded yet", said(render([], 4)))
        self.assertIn("No usage recorded yet", said(render([], 1)))

    def test_a_response_without_the_field_is_treated_as_one_worker(self) -> None:
        """An older gateway behind a newer page says nothing, rather than guessing a number."""
        self.assertNotIn("one worker's counters", said(render(ROW, None)))

    def test_the_table_still_appears_and_still_hides(self) -> None:
        """The floor for all of the above: the panel's actual job must be unchanged."""
        self.assertEqual("", render(ROW, 4)["tableShown"])
        self.assertEqual("none", render([], 4)["tableShown"])


class TheEndpointReportsItTest(unittest.TestCase):

    @staticmethod
    def _source() -> str:
        with io.open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            return handle.read()

    def test_the_usage_response_carries_the_count(self) -> None:
        self.assertIn('"workers": _worker_count()', self._source())

    def test_it_asks_the_same_helper_the_other_reads_ask(self) -> None:
        """One answer to the question. A second way of counting workers is a second answer that
        can disagree with the warning about workers not sharing a store."""
        self.assertGreaterEqual(self._source().count("_worker_count()"), 3)

    def test_the_helper_is_defined_once(self) -> None:
        tree = ast.parse(self._source())
        found = [node for node in ast.walk(tree)
                 if isinstance(node, ast.FunctionDef) and node.name == "_worker_count"]
        self.assertEqual(1, len(found))


if __name__ == "__main__":
    unittest.main()
