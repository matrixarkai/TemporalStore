#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every tab pane wraps the content it is named after.

A pane is only a tab if the markup nests it that way, and on the key portal it did not. The three
panes had been spliced at the wrong offsets -- each opened just before a card's table and closed
just after it -- which a customer met as three separate faults:

  * The Tenant ID field and the Save connection / Clear key buttons sat inside the Keys pane, so
    half the Connection panel disappeared whenever Policy or Usage was open. That panel is the one
    holding the admin key; it is needed on every tab.
  * The key table sat inside the Policy pane. Clicking "Keys" hid the list of keys and clicking
    "Policy" showed it.
  * `pane-keys` opened inside one column of the Connection panel's two-column row, so the Keys tab
    rendered in half the page width, and the markup was left with an unclosed div and a stray
    `</section>`.

None of this was visible to the JS harnesses. They read panes out of the markup with a regular
expression and drive `showTab`, which answers "is exactly one pane visible?" perfectly well while
having no idea what is inside them. Nesting needs a parser, so this builds the tree.

The checks run over every portal page rather than the one that was broken, and the count of pages
carrying panes is asserted -- otherwise a glob that stopped matching would report success over
nothing at all.
"""
from __future__ import annotations

import glob
import io
import os
import unittest
from html.parser import HTMLParser

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param",
        "source", "track", "wbr"}

# Pages carrying a tab strip today. Asserted as a floor, so this cannot pass over an empty glob.
PAGES_WITH_PANES = 4


class Tree(HTMLParser):
    """Enough of a tree to answer what contains what. Scripts are excluded: their string literals
    contain markup that no browser parses as markup."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.panes = {}          # pane id -> the elements it holds directly
        self.parent = {}         # pane id -> what it sits in
        self.ancestors = {}      # element id -> the ancestor chain above it
        self.mismatched = []

    @staticmethod
    def describe(tag, attrs) -> str:
        d = dict(attrs)
        if d.get("id"):
            return tag + "#" + d["id"]
        if d.get("class"):
            return tag + "." + d["class"].split()[0]
        return tag

    def _open_pane(self):
        for entry in reversed(self.stack):
            if "#" in entry and entry.split("#", 1)[1] in self.panes:
                return entry.split("#", 1)[1]
        return None

    def handle_starttag(self, tag, attrs) -> None:
        d = dict(attrs)
        me = self.describe(tag, attrs)
        if d.get("id"):
            self.ancestors[d["id"]] = list(self.stack)
        if "pane" in (d.get("class") or "").split() and d.get("id"):
            self.panes[d["id"]] = []
            self.parent[d["id"]] = self.stack[-1] if self.stack else "(top level)"
        else:
            here = self._open_pane()
            if here is not None and self.stack and self.stack[-1].endswith("#" + here):
                self.panes[here].append(me)
        if tag not in VOID:
            self.stack.append(me)

    def handle_endtag(self, tag) -> None:
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i].split("#")[0].split(".")[0] == tag:
                if i != len(self.stack) - 1:
                    self.mismatched.append("</%s> closed while %s was still open"
                                           % (tag, ", ".join(self.stack[i + 1:])))
                del self.stack[i:]
                return
        self.mismatched.append("stray </%s>" % tag)


def tree_for(path: str) -> Tree:
    with io.open(path, encoding="utf-8") as handle:
        body = handle.read().split("<script>")[0]
    tree = Tree()
    tree.feed(body)
    return tree


def pages() -> list:
    return sorted(glob.glob(os.path.join(PORTAL, "*.html")))


def pane_of(tree: Tree, element_id: str):
    """Which pane holds `element_id`, or None if it sits outside every pane."""
    for entry in tree.ancestors.get(element_id, []):
        if "#" in entry and entry.split("#", 1)[1] in tree.panes:
            return entry.split("#", 1)[1]
    return None


class EveryPaneHoldsSomethingTest(unittest.TestCase):

    def test_the_glob_still_finds_pages_with_tabs(self) -> None:
        """Without this the checks below would pass over nothing and report success."""
        with_panes = [p for p in pages() if tree_for(p).panes]
        self.assertGreaterEqual(len(with_panes), PAGES_WITH_PANES,
                                "expected at least %d pages carrying a tab strip, found %r"
                                % (PAGES_WITH_PANES, [os.path.basename(p) for p in with_panes]))

    def test_no_pane_is_empty(self) -> None:
        """An empty pane means its content was left outside the tab that claims to show it."""
        for path in pages():
            tree = tree_for(path)
            for pane, held in tree.panes.items():
                self.assertTrue(held, "%s: %s holds nothing, so switching to that tab shows an "
                                      "empty page" % (os.path.basename(path), pane))

    def test_no_pane_is_nested_inside_a_card(self) -> None:
        """A pane inside a card takes part of that card with it when the tab is switched away."""
        for path in pages():
            tree = tree_for(path)
            for pane, parent in tree.parent.items():
                self.assertFalse(parent.startswith("section"),
                                 "%s: %s is nested inside %s, so hiding that tab hides part of "
                                 "the card too" % (os.path.basename(path), pane, parent))

    def test_the_markup_balances(self) -> None:
        for path in pages():
            tree = tree_for(path)
            self.assertEqual([], tree.mismatched,
                             "%s: %r" % (os.path.basename(path), tree.mismatched))


class TheKeyPortalPutsEachControlInTheRightPlaceTest(unittest.TestCase):
    """The specific placements that were wrong, named so a future splice cannot quietly undo them."""

    def setUp(self) -> None:
        self.tree = tree_for(os.path.join(PORTAL, "api_key_portal.html"))

    def test_the_connection_panel_is_on_every_tab(self) -> None:
        """It holds the admin key, and nothing on any tab works without it."""
        for control in ("adminKey", "mgmtBase", "gwBase", "ctxAccount", "ctxTenant",
                        "saveConn", "clearKey"):
            self.assertIsNone(pane_of(self.tree, control),
                              "%s sits inside a tab pane, so it disappears on the other tabs"
                              % control)

    def test_each_table_is_behind_the_tab_that_names_it(self) -> None:
        for element, pane in (("createBtn", "pane-keys"), ("createKeyOut", "pane-keys"),
                              ("keysTable", "pane-keys"), ("overridesTable", "pane-policy"),
                              ("usageTable", "pane-usage")):
            self.assertEqual(pane, pane_of(self.tree, element),
                             "%s is behind %r, not %r" % (element, pane_of(self.tree, element),
                                                          pane))

    def test_the_tab_strip_is_not_inside_a_card(self) -> None:
        chain = self.tree.ancestors.get("tab-keys", [])
        self.assertTrue(any(entry.startswith("div.tabs") for entry in chain), chain)
        self.assertFalse([entry for entry in chain if entry.startswith("section")],
                         "the tab strip sits inside %r" % chain)


if __name__ == "__main__":
    unittest.main()
