#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The panel printed an incident id and said nothing about what it was for.

The token is the whole route from "something failed" to the traceback: the gateway returns it to
the caller in place of the error, and logs the same token with the exception. Joining the two is
the only way to see what the caller was deliberately not told. The failures panel printed the id in
its own column and named it nowhere, so the one thing on that table that leads anywhere read as an
opaque string.

A sentence of explanation is only worth adding if it stays true, so each claim it makes is checked
against the code here rather than left to be read once and drift:

* the logger it names is the logger the gateway logs incidents to;
* the level it names is the level they are logged at;
* a dash really does mean no incident was minted -- the refusals it cites return their own body and
  never reach the failure path.
"""
from __future__ import annotations

import ast
import io
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


PAGE = read(os.path.join(PORTAL, "setup_portal.html"))
GATEWAY = read(os.path.join(TOOLS, "matrixark_v1_gateway.py"))


class ThePanelExplainsTheColumnTest(unittest.TestCase):

    def test_the_panel_says_what_an_incident_is(self) -> None:
        self.assertIn("An <b>incident</b> is the token the gateway gave the", PAGE)

    def test_it_still_prints_the_column_it_explains(self) -> None:
        """The floor: an explanation of a column that is gone is worse than none."""
        self.assertIn("<th>Incident</th>", PAGE)


class EveryClaimItMakesIsTrueTest(unittest.TestCase):

    def test_the_logger_it_names_is_the_one_incidents_go_to(self) -> None:
        self.assertIn("<code>matrixark.gateway</code>", PAGE)
        self.assertIn('_GATEWAY_LOG = logging.getLogger("matrixark.gateway")', GATEWAY)

    def test_the_level_it_names_is_the_level_they_are_logged_at(self) -> None:
        self.assertIn("at ERROR level", PAGE)
        tree = ast.parse(GATEWAY)
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_incident":
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) \
                            and isinstance(sub.func.value, ast.Name) \
                            and sub.func.value.id == "_GATEWAY_LOG":
                        calls.append(sub.func.attr)
        self.assertEqual(["error"], calls,
                         "the incident is no longer logged at the level the panel names")

    def test_the_token_really_does_reach_the_caller(self) -> None:
        """The sentence says the same token is in both places. If the body stopped carrying it,
        the instruction to grep for it would send somebody looking for nothing."""
        self.assertIn('"incident": _incident(scope, code, exc),', GATEWAY)

    def test_a_refusal_mints_no_incident(self) -> None:
        """The claim behind the dash. An unauthorized reply is its own body and never goes through
        the failure path, so nothing is logged and nothing is there to look up."""
        self.assertIn('return await _json(send, 401, {"error": "unauthorized"})', GATEWAY)
        tree = ast.parse(GATEWAY)
        minted = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                    and node.func.id == "_incident":
                minted.add(node.lineno)
        self.assertTrue(minted, "nothing mints an incident, so the column cannot fill")
        for line in minted:
            context = GATEWAY.splitlines()[line - 1]
            self.assertNotIn("unauthorized", context)


class TheExplanationIsWhereTheTableIsTest(unittest.TestCase):
    """It belongs beside the column, not in a document somebody would have to already know about."""

    def test_it_sits_in_the_failures_panel(self) -> None:
        panel = PAGE[PAGE.index("Recent failures"):]
        panel = panel[:panel.index('<div id="failures"')]
        self.assertIn("An <b>incident</b> is", panel)

    def test_the_builder_and_the_page_agree(self) -> None:
        """The page is emitted; the builder is what to edit."""
        self.assertIn("An <b>incident</b> is the token the gateway gave the",
                      read(os.path.join(PORTAL, "build_portal_pages.py")))


if __name__ == "__main__":
    unittest.main()
