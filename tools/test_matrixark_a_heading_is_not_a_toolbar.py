#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A section heading says what the section is, and nothing else.

Eight headings across four pages had their action button inside the ``<h2>``. A heading's
accessible name is its contents, so the name became the title plus the button:

    Grafana copy scrape config
    Move this configuration export copy as curl
    Launch a deployment copy env file

Moving through a long page by heading is how that name gets used -- it is the table of contents
for anyone who cannot see the page -- so the one place the wording has to be exact is the one
place a button label was being appended to it.

The row is unchanged. The ``h2`` was already ``display:flex`` with the actions pushed right by
``margin-left:auto``; ``.sechead`` takes those properties over and the heading keeps its own type.
Measured, not assumed: the two builds were served side by side and every box on Overview landed on
the same pixel, with the same document height.

**Parsed, not matched.** A first attempt used a regex for ``<h2>...<span class=aux>...</span></h2>``
and the ``.*?`` between them crossed element boundaries: given a heading with no actions followed
by one with them, it ran from the first open tag to the second close tag and swallowed everything
between. The markup still looked plausible; the rendered page showed a heading row 323 pixels tall.
HTMLParser cannot make that mistake.
"""
from __future__ import annotations

import io
import os
import unittest
from html.parser import HTMLParser

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

HEADINGS = ("h1", "h2", "h3", "h4", "h5", "h6")
CONTROLS = ("button", "select", "textarea", "input")


class Reader(HTMLParser):
    """Every heading on the page: its text, and any control that opened inside it."""

    def __init__(self) -> None:
        HTMLParser.__init__(self, convert_charrefs=True)
        self.headings = []          # (tag, text, [controls opened inside])
        self.rows = []              # (has_heading, control_count) for each .sechead
        self._heading = None
        self._row = None
        self._depth = 0

    def handle_starttag(self, tag, attrs):
        classes = dict(attrs).get("class", "").split()
        if tag == "div" and "sechead" in classes:
            self._row = [False, 0]
            self._depth = 0
        elif self._row is not None and tag == "div":
            self._depth += 1
        if tag in HEADINGS:
            self._heading = [tag, [], []]
            if self._row is not None:
                self._row[0] = True
        if tag in CONTROLS or (tag == "a" and "btn" in classes):
            if self._heading is not None:
                self._heading[2].append(tag)
            if self._row is not None:
                self._row[1] += 1

    def handle_endtag(self, tag):
        if tag in HEADINGS and self._heading is not None:
            self.headings.append((self._heading[0], "".join(self._heading[1]).strip(),
                                  self._heading[2]))
            self._heading = None
        elif tag == "div" and self._row is not None:
            if self._depth == 0:
                self.rows.append(tuple(self._row))
                self._row = None
            else:
                self._depth -= 1

    def handle_data(self, data):
        if self._heading is not None:
            self._heading[1].append(data)


def pages() -> list:
    return sorted(f for f in os.listdir(PORTAL) if f.endswith("_portal.html"))


def read(name: str) -> Reader:
    parser = Reader()
    with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        parser.feed(handle.read())
    return parser


class AHeadingIsNotAToolbarTest(unittest.TestCase):

    def test_no_heading_contains_a_control(self) -> None:
        for name in pages():
            for tag, text, controls in read(name).headings:
                self.assertEqual(
                    [], controls,
                    "%s: <%s> %r contains %s, so the heading announces itself as the title plus "
                    "that control's label" % (name, tag, text[:60], ", ".join(controls)))

    def test_the_row_still_carries_its_actions(self) -> None:
        """The other half. Deleting the buttons would satisfy the assertion above perfectly."""
        for name in pages():
            for has_heading, controls in read(name).rows:
                self.assertTrue(has_heading, "%s: a heading row with no heading in it" % name)
                self.assertGreater(
                    controls, 0,
                    "%s: a heading row with no action left in it -- the row exists to hold one, "
                    "so an empty one means the control was dropped rather than moved" % name)

    def test_the_pages_have_such_rows_at_all(self) -> None:
        """The positive control. Both assertions above pass on a portal with no actions anywhere,
        which is exactly what a bad edit would leave behind."""
        rows = sum(len(read(name).rows) for name in pages())
        self.assertGreaterEqual(rows, 5, "only %d heading rows carry an action" % rows)

    def test_a_heading_that_lost_its_words_is_not_a_pass(self) -> None:
        """A heading emptied of text also contains no control. Every heading still says something."""
        for name in pages():
            for tag, text, _ in read(name).headings:
                self.assertTrue(text.strip(), "%s: an empty <%s>" % (name, tag))


if __name__ == "__main__":
    unittest.main()
