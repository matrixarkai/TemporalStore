#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A catalog list that stopped at its limit is not the catalogue.

`GET /v1/skills` and `/v1/resources` cap at 500 rows however many are asked for, and returned no
sign of it. A tenant holding 4,000 skills got 500, a summary card reading "500 skills", and no way
to tell that from a deployment holding exactly 500.

The cap was written down twice: the page clamped to 500 before sending, and the gateway clamped to
500 on arrival. Two copies of one number that had to agree, with nothing making them -- and the
page's copy also hid the clamp from the person who typed the larger number, the same defect as a
select that quietly rewrites an off-list value.

The page now sends what was typed and reports what came back:

* a list whose length reached the applied limit says so, per list, since one can be cut while the
  other is not;
* asking for more than the endpoint serves says what the maximum is.

**Only written when there is something to say.** The auto-refresh writes its own message into the
same element, and a blank write would take it away.

The page is checked by RUNNING the shipped `load`, because a page that computes the sentence and
never shows it would pass a source read.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "catalog_limit_harness.js")
PAGE = os.path.join(PORTAL, "catalog_portal.html")


def writes(skills: int, resources: int, typed: int, applied=None, cap=500,
           keep_message: bool = False) -> list:
    """Every write the panel makes into the message element, in order.

    `keep_message` is the auto-refresh path: it does NOT clear the element first, because the
    refresh has just written its own message there.
    """
    applied = min(typed, cap) if applied is None else applied

    def body(key: str, count: int) -> dict:
        out = {key: [{"name": "x%d" % i} for i in range(count)]}
        if cap is not None:
            out["limit_max"] = cap
        if applied:
            out["limit"] = applied
        return out

    payload = {"typed": typed, "keepMessage": keep_message,
               "bodies": [body("skills", skills), body("resources", resources)]}
    out = subprocess.run(["node", HARNESS, PAGE, json.dumps(payload)],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    return json.loads(out.stdout)["said"]


def load(skills: int, resources: int, typed: int, applied=None, cap=500) -> str:
    """What the panel says for a response of this shape. `applied` defaults to the real clamp."""
    return " ".join(entry["text"]
                    for entry in writes(skills, resources, typed, applied, cap)
                    if entry["text"])


class ThePanelSaysWhenTheListWasCutTest(unittest.TestCase):

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_list_at_the_limit_says_so(self) -> None:
        said = load(skills=100, resources=4, typed=100)
        self.assertIn("Skills at 100", said)
        self.assertIn("there may be more", said)

    def test_it_names_which_list_was_cut(self) -> None:
        """One can be cut while the other is not, and "the list" would not say which."""
        self.assertIn("Resources at 100", load(skills=4, resources=100, typed=100))
        self.assertNotIn("Skills at", load(skills=4, resources=100, typed=100))

    def test_both_are_named_when_both_were_cut(self) -> None:
        said = load(skills=500, resources=500, typed=5000)
        self.assertIn("Skills at 500 and Resources at 500", said)

    def test_a_list_under_the_limit_says_nothing(self) -> None:
        """The floor. A note on every listing is a note nobody reads, and these lists are almost
        always short."""
        self.assertEqual("", load(skills=3, resources=4, typed=100))

    def test_asking_for_more_than_it_serves_is_told_so(self) -> None:
        """The half the page used to hide by clamping before it sent."""
        said = load(skills=10, resources=10, typed=5000)
        self.assertIn("The most this endpoint returns is 500", said)
        self.assertIn("5000 was asked for", said)

    def test_a_response_without_the_fields_says_nothing(self) -> None:
        """An older gateway behind a newer page stays quiet rather than inventing a cap."""
        self.assertEqual("", load(skills=100, resources=100, typed=100, applied=0, cap=None))


class ItDoesNotTakeAwaySomebodyElsesMessageTest(unittest.TestCase):
    """The auto-refresh writes "this list has been refreshed" into the same element and then calls
    `load(true)`, which deliberately does not clear it. A blank write from here would take that
    message away a fraction of a second after it appeared."""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_nothing_to_say_writes_nothing_at_all(self) -> None:
        self.assertEqual([], writes(skills=3, resources=4, typed=100, keep_message=True))

    def test_something_to_say_still_says_it_on_that_path(self) -> None:
        """The floor: silence must be because there was nothing to say, not because this path is
        silent."""
        said = writes(skills=100, resources=4, typed=100, keep_message=True)
        self.assertEqual(1, len(said))
        self.assertIn("Skills at 100", said[0]["text"])

    def test_the_ordinary_path_still_clears_first(self) -> None:
        """Unchanged behaviour: a fresh listing starts from a blank message."""
        said = writes(skills=3, resources=4, typed=100, keep_message=False)
        self.assertEqual([""], [entry["text"] for entry in said])


class TheGatewayReportsWhatItApplied(unittest.TestCase):

    @staticmethod
    def _source() -> str:
        with io.open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            return handle.read()

    def test_the_cap_is_named_once(self) -> None:
        source = self._source()
        self.assertIn("CATALOG_LIST_LIMIT_MAX = 500", source)
        self.assertIn("min(int(limit), CATALOG_LIST_LIMIT_MAX)", source)

    def test_the_response_carries_both(self) -> None:
        source = self._source()
        self.assertIn('body["limit_max"] = CATALOG_LIST_LIMIT_MAX', source)
        self.assertIn('body["limit"] = args["limit"]', source)

    def test_the_page_no_longer_holds_its_own_copy(self) -> None:
        """The point of naming it. A second copy in the browser is a second place to be wrong."""
        with io.open(os.path.join(PORTAL, "build_portal_pages.py"), encoding="utf-8") as handle:
            builder = handle.read()
        self.assertNotIn("Math.min(limit, 500)", builder)
        with io.open(PAGE, encoding="utf-8") as handle:
            self.assertNotIn("Math.min(limit, 500)", handle.read())


if __name__ == "__main__":
    unittest.main()
