#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The console says it is generated from MEM0_OPERATIONS. Nothing made that true.

The block in `build_portal_pages.py` carries this comment:

    Generated from the gateway's own MEM0_OPERATIONS, so an operation that gains an argument gains
    a field here without anyone remembering to add one.

It is a hand-written copy of that list, and the only check tying the two together asserted that
each operation's **id** appears in the page. Every summary, label, default, help, method, path and
scope could differ, and an operation that gained an argument would gain a field here only if
somebody remembered -- which is what the comment says is unnecessary.

This asserts the two are the same document. The claim then holds because a copy that drifts fails
here, rather than because of the sentence.

`get_all` is the operation that shows why it matters. Its summary said *"Every live memory in the
scope"* while its limit defaulted to 50, and its limit was the one field in the whole console with
no help -- so the console described a listing as complete and said nothing about which fifty. It
takes the **newest**, which was itself a fix: sorting ascending and taking the head answered
``get_all(limit=10)`` on a thousand memories with the ten oldest.
"""
from __future__ import annotations

import io
import json
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
sys.path.insert(0, TOOLS)

import matrixark_v1_gateway as gw  # noqa: E402


def console_ops(path: str) -> list:
    """The `MEM0_OPS` array as the page ships it."""
    with io.open(path, encoding="utf-8") as handle:
        text = handle.read()
    start = text.index("var MEM0_OPS = ")
    open_bracket = text.index("[", start)
    depth = 0
    for index in range(open_bracket, len(text)):
        if text[index] == "[":
            depth += 1
        elif text[index] == "]":
            depth -= 1
            if depth == 0:
                return json.loads(text[open_bracket:index + 1])
    raise AssertionError("the MEM0_OPS array is not closed")


class TheConsoleIsTheGatewaysListTest(unittest.TestCase):

    def setUp(self) -> None:
        self.page = console_ops(os.path.join(PORTAL, "mem0_portal.html"))

    def test_the_two_are_the_same_document(self) -> None:
        """Not "every id appears": the same operations, in the same order, field for field."""
        self.assertEqual(json.loads(json.dumps(gw.MEM0_OPERATIONS)), self.page)

    def test_the_builder_carries_what_the_page_does(self) -> None:
        """The page is emitted from the builder, so the builder's copy is the one to edit -- and a
        page regenerated from a stale builder would otherwise satisfy the test above only until the
        next build."""
        self.assertEqual(self.page, console_ops(os.path.join(PORTAL, "build_portal_pages.py")))

    def test_the_comparison_is_looking_at_something(self) -> None:
        """The floor. Two empty lists are equal."""
        self.assertGreater(len(self.page), 5)
        self.assertTrue(all(op.get("id") for op in self.page))

    def test_the_comment_the_check_backs_up_is_still_there(self) -> None:
        """If the claim is deleted this test is pointless; if the claim is kept it must be true."""
        with io.open(os.path.join(PORTAL, "build_portal_pages.py"), encoding="utf-8") as handle:
            self.assertIn("Generated from the gateway's own MEM0_OPERATIONS", handle.read())


class GetAllSaysWhichMemoriesTest(unittest.TestCase):

    @staticmethod
    def _op() -> dict:
        return next(op for op in gw.MEM0_OPERATIONS if op["id"] == "get_all")

    def test_it_no_longer_calls_a_limited_listing_every_memory(self) -> None:
        self.assertNotIn("Every live memory", self._op()["summary"])

    def test_the_summary_says_a_limit_takes_the_newest(self) -> None:
        self.assertIn("NEWEST", self._op()["summary"])

    def test_the_limit_field_explains_itself(self) -> None:
        field = next(f for f in self._op()["fields"] if f["name"] == "limit")
        self.assertIn("help", field, "the one field in the console with nothing said about it")
        self.assertIn("NEWEST", field["help"])

    def test_every_field_a_customer_types_into_says_what_to_put_in_it(self) -> None:
        """The general form of the same point: a field with nothing said about it is a guess.

        A field carrying `from_scope` is exempt because the console fills it from the scope inputs
        above rather than asking -- and `test_the_exempt_fields_really_are_filled_from_the_scope`
        checks that is what they are, so the exemption cannot become a place to put a field nobody
        documented.
        """
        bare = []
        for op in gw.MEM0_OPERATIONS:
            for field in op.get("fields") or []:
                if field.get("from_scope"):
                    continue
                if not (field.get("help") or field.get("placeholder")):
                    bare.append("%s.%s" % (op["id"], field["name"]))
        self.assertEqual([], bare, "these say nothing about what to put in them: %s" % bare)

    def test_the_exempt_fields_really_are_filled_from_the_scope(self) -> None:
        """What the exclusion above hides, stated. Each exempt field names one of the three scope
        inputs the page has, so "from_scope" cannot be a word that silences the check."""
        exempt = [(op["id"], field["name"], field["from_scope"])
                  for op in gw.MEM0_OPERATIONS
                  for field in op.get("fields") or []
                  if field.get("from_scope")]
        self.assertTrue(exempt, "nothing is exempt, so the exemption above is untested")
        for op_id, name, source in exempt:
            with self.subTest(field="%s.%s" % (op_id, name)):
                self.assertIn(source, ("user", "agent", "session"))


if __name__ == "__main__":
    unittest.main()
