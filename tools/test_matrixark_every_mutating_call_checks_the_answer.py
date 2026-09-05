#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A portal call that changes something looks at what came back.

``fetch`` rejects only on a network failure. A 403 for want of a scope, a 409, a 404 -- all of them
*resolve*, so a handler that goes straight to its success path treats a refusal as a success. On a
panel that means the table refreshes, nothing is said, and the reader concludes the action was slow
rather than refused.

That happened once, to ``cancelJob``: it posted a cancel and reloaded the job list whatever came
back, while ``retryJob`` directly above it -- the sibling action, same page -- checked ``res.ok``
and explained the refusal. One function out of step with its neighbour is not something reading the
page catches, because everything around it is right.

So it is counted instead. Every ``fetch`` in the portal carrying a mutating method must look at the
response, and the count is asserted so a matcher that stopped matching cannot pass by finding
nothing. Run against the page before that fix, this reports exactly one::

    ingestion  cancelJob  POST  NO
    13 mutating calls, 1 that never looks at what came back

A chain handed back to a caller is that caller's business and is not counted here -- ``post()`` on
the ingestion page returns its promise and the callers check it.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

# Methods that change something. GET is not here: a read that fails shows an empty panel, which is
# its own problem and a different one.
MUTATING = re.compile(r'method:\s*["\'](POST|PUT|DELETE|PATCH)["\']')
# How a handler can be looking at the answer. Any of these means the code saw the response.
CHECKED = re.compile(r"\.ok\b|\.status\b")


def scripts(text: str) -> str:
    return "\n".join(re.findall(r"<script>([\s\S]*?)</script>", text))


def _closing(text: str, start: int, opener: str, closer: str) -> int:
    depth = 0
    index = text.index(opener, start)
    while index < len(text):
        if text[index] == opener:
            depth += 1
        elif text[index] == closer:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return len(text) - 1


def chain_at(script: str, start: int) -> str:
    """The fetch call and everything chained onto it.

    Matched by parenthesis rather than by line: these chains run to twenty lines and a
    line-oriented scan reads the first ``.then`` as the whole handler.
    """
    end = _closing(script, start, "(", ")")
    cursor = end + 1
    while True:
        link = re.match(r"\s*\.\s*(then|catch|finally)\s*\(", script[cursor:cursor + 40])
        if not link:
            break
        cursor = _closing(script, cursor + link.end() - 1, "(", ")") + 1
    return script[start:cursor]


def enclosing(script: str, position: int) -> str:
    name = "?"
    for match in re.finditer(r"\n\s*function (\w+)\s*\(", script):
        if match.start() < position:
            name = match.group(1)
    return name


def mutating_calls() -> tuple:
    """(every mutating call, the ones that ignore the response)."""
    every, ignored = [], []
    for name in sorted(os.listdir(PORTAL)):
        if not name.endswith(".html"):
            continue
        with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
            script = scripts(handle.read())
        for match in re.finditer(r"\bfetch\(", script):
            chain = chain_at(script, match.start())
            if not MUTATING.search(chain):
                continue
            where = "%s %s" % (name.replace("_portal.html", ""), enclosing(script, match.start()))
            every.append(where)
            line_start = script.rfind("\n", 0, match.start()) + 1
            if script[line_start:match.start()].strip().endswith("return"):
                # Handed to a caller, which is where the check belongs.
                continue
            if not CHECKED.search(chain):
                ignored.append(where)
    return every, ignored


class EveryMutatingCallChecksTheAnswerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.every, self.ignored = mutating_calls()

    def test_there_are_calls_to_check(self) -> None:
        """A matcher that stopped matching finds nothing and reports everything as fine."""
        self.assertGreaterEqual(len(self.every), 10, self.every)

    def test_none_of_them_ignores_the_response(self) -> None:
        self.assertEqual([], self.ignored,
                         "these change something and never look at whether it worked, so a "
                         "refusal is indistinguishable from success: %r" % self.ignored)

    def test_the_scan_reads_a_whole_chain(self) -> None:
        """These chains run to twenty lines. A scan that stopped at the first `.then` would call
        every one of them unchecked, and this test would be a wall of false alarms rather than a
        guard -- so the shape it depends on is asserted directly."""
        with io.open(os.path.join(PORTAL, "ingestion_portal.html"), encoding="utf-8") as handle:
            script = scripts(handle.read())
        start = script.index("fetch(", script.index("function retryJob("))
        chain = chain_at(script, start)
        self.assertIn(".then(", chain)
        self.assertIn(".catch(", chain, "the chain was cut short of its own error handler")


if __name__ == "__main__":
    unittest.main()
