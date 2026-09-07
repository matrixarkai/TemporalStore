#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A helper a page calls is defined before the call, or the call asks whether it exists.

The portal emits three script blocks per page: shared helpers, then the page's own script, then
the live strip. A helper more than one page calls belongs in the first block. Four of them were in
the LAST one instead, and every page still worked -- a browser has run all three blocks long
before anyone clicks anything, so nothing on screen could tell you.

What it cost showed up somewhere else. Explore's browse path catches everything its two fetches
can throw and writes one message: "Could not reach the gateway." A call to a helper that is not
defined yet is a ReferenceError, the catch does not care which it caught, and the page reported a
network incident about a gateway it had never asked.

**Two ways to be safe, and the second is not a loophole.** The strip's own helpers stay in the
nav block, because they close over the state it keeps between frames -- moving them out cost five
strip tests, which is "defined on window" and "shared" turning out to be different things. Page
code reaches those through ``if (window.__matrixarkLiveFrame) { ... }``, which is correct on a
page where the strip may not be installed at all. So the exception is read off the guard in the
code rather than kept here as a list of names: a list would have to be edited by the same change
that breaks it.

Two things a first draft got wrong, both worth keeping in mind when editing this:

* a helper is not always assigned a function literal. ``api_key`` aliases one of its own locals
  (``window.__matrixarkCopy = copyText``), which is a definition like any other. Matching only
  ``= function`` reported that page as calling something nothing defines.
* ``__matrixarkCopy`` is a PREFIX of ``__matrixarkCopyText``, and both are on that page. A plain
  substring search finds the longer name when asked for the shorter one, so the name has to end
  where the search says it does.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

CALL = re.compile(r"window\.(__matrixark[A-Za-z0-9_]*)\s*\(")
SHARED_BLOCK = "Helpers every page may call"
LOOKBACK = 200


def defined(helper: str) -> re.Pattern:
    """Any assignment to the name, and only to that name -- `\\s*=` cannot follow a longer one."""
    return re.compile(r"window\." + re.escape(helper) + r"\s*=\s*")


def guarded(helper: str) -> re.Pattern:
    """The name tested rather than called: `if (window.X)`, `window.X &&`, `window.X ?`."""
    return re.compile(r"window\." + re.escape(helper) + r"\s*(?:\)|&&|\?)")


def pages() -> list:
    return sorted(f for f in os.listdir(PORTAL) if f.endswith("_portal.html"))


def read(name: str) -> str:
    with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        return handle.read()


def first_unguarded_call(text: str, helper: str) -> int:
    """Where this page first calls the helper without asking whether it is there. -1 if never."""
    check = guarded(helper)
    for match in CALL.finditer(text):
        if match.group(1) != helper:
            continue
        if not check.search(text[max(0, match.start() - LOOKBACK):match.start()]):
            return match.start()
    return -1


def called(text: str) -> set:
    return set(match.group(1) for match in CALL.finditer(text))


class ASharedHelperIsDefinedFirstTest(unittest.TestCase):

    def test_every_call_reaches_something_that_exists(self) -> None:
        """The floor: a name nothing on the page assigns is a ReferenceError however it is used."""
        for name in pages():
            text = read(name)
            for helper in sorted(called(text)):
                self.assertIsNotNone(
                    defined(helper).search(text),
                    "%s calls %s and nothing on the page defines it" % (name, helper))

    def test_an_unguarded_call_has_its_definition_above_it(self) -> None:
        for name in pages():
            text = read(name)
            for helper in sorted(called(text)):
                call_at = first_unguarded_call(text, helper)
                if call_at < 0:
                    continue          # every call asks first, which is safe wherever it sits
                at = defined(helper).search(text).start()
                self.assertLess(
                    at, call_at,
                    "%s calls %s at %d without asking whether it is there, and defines it at "
                    "%d -- anything that runs this page's own script by itself gets a "
                    "ReferenceError, and Explore's browse path reports that as a gateway failure"
                    % (name, helper, call_at, at))

    def test_it_is_defined_once_per_page(self) -> None:
        """Two copies is two things to keep in step, and the later one silently wins."""
        for name in pages():
            text = read(name)
            for helper in sorted(called(text)):
                found = len(defined(helper).findall(text))
                self.assertEqual(1, found, "%s defines %s %d times" % (name, helper, found))

    def test_the_pages_really_do_call_one(self) -> None:
        """The positive control. Every assertion above passes perfectly on a portal that calls no
        shared helper at all, and that is the state a bad edit would leave behind."""
        calling = [name for name in pages() if called(read(name))]
        self.assertGreaterEqual(len(calling), 3, "found calls on %s" % (calling or "no page"))

    def test_some_call_is_actually_unguarded(self) -> None:
        """The other positive control, for the exception rather than the rule. If every call on
        every page were read as guarded -- one loose pattern would do it -- the ordering assertion
        above would pass while checking nothing at all."""
        unguarded = [(name, helper) for name in pages() for helper in sorted(called(read(name)))
                     if first_unguarded_call(read(name), helper) >= 0]
        self.assertGreaterEqual(len(unguarded), 5, "only %d unguarded calls found" % len(unguarded))

    def test_the_shared_block_is_on_every_page_exactly_once(self) -> None:
        """Once, not at least once: the block is injected into the two hand-maintained pages on
        every build, and a build that adds a copy instead of replacing one leaves the page with
        two definitions of everything in it."""
        for name in pages():
            self.assertEqual(1, read(name).count(SHARED_BLOCK),
                             "%s does not carry exactly one shared helper block" % name)


if __name__ == "__main__":
    unittest.main()
