#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A link that names a place inside a tab has to open that tab.

The status strip runs across the top of every portal page, and two of its segments are links:
``/v1/admin/setup#encoding`` when the deployment is retrieving on hash vectors, and
``/v1/admin/setup#traffic`` when requests are being refused. Both name a div that lives inside a tab
pane, and every pane but the first loads ``hidden``.

A browser scrolls to a fragment once, at load. There is nothing to scroll to -- the element is
inside a hidden section -- so it doesn't, the Access tab comes up, and the reader is looking at the
key input instead of the thing the badge was warning them about. On the setup page itself it is
worse: no document changes, so nothing at all happens, and the badge is indistinguishable from a
dead control.

``location.hash`` appeared nowhere in the portal. The helper now resolves the fragment to whichever
pane contains it, opens that tab, and scrolls -- and handles ``hashchange``, which is the same-page
case.

Two things are checked, because they fail differently:

* every fragment any page links to names an element that exists on the page it points at -- a
  cheap check that catches a target being renamed out from under the link;
* the tab helper, run against the real markup, actually opens the pane holding it.

The second needs to know what contains what, and a flat DOM stub cannot answer that -- it agrees
with whatever it is told, which is how three panes once shipped spliced into the wrong parents with
every harness green. So the harness reads containment out of the page by matching ``<section>``
against ``</section>``, and asserts those extents are sane before trusting them.
"""
from __future__ import annotations

import io
import os
import re
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "deep_link_harness.js")
FOLD_HARNESS = os.path.join(PORTAL, "folded_answer_harness.js")
GATEWAY = os.path.join(TOOLS, "matrixark_v1_gateway.py")


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def pages() -> dict:
    return {name: read(os.path.join(PORTAL, name))
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


def markup(text: str) -> str:
    """The page without its scripts.

    Script blocks build markup out of strings, so ``class="pane"`` appears in a page that declares
    no pane at all -- the API page assembles its tabs from the route catalogue after a fetch, and
    reading its source for panes finds the template rather than the page.
    """
    return re.sub(r"<script[\s\S]*?</script>", "", text)


def routes_to_pages() -> dict:
    """Which file a portal route serves, taken from the gateway rather than assumed.

    The names do not follow from each other -- ``/v1/admin/portal`` serves the key page and
    ``/v1/admin`` serves the overview -- so guessing the mapping would check the wrong file and
    pass.
    """
    source = read(GATEWAY)
    served = re.findall(
        r'path (?:==|in) \(?"(/v1/admin[^"]*)"[^\n]*\n\s*return await _html\(send, 200, (\w+)\(\)\)',
        source)
    files = {}
    for helper in {name for _, name in served}:
        body = source[source.index("def %s(" % helper):]
        match = re.search(r'"([a-z_]+_portal\.html)"', body[:2000])
        if match:
            files[helper] = match.group(1)
    return {route: files[helper] for route, helper in served if helper in files}


def fragment_links() -> list:
    """Every ``/v1/admin/...#anchor`` any portal page links to, with the page it is written on."""
    found = []
    for name, text in pages().items():
        for route, anchor in re.findall(r'href="(/v1/admin[a-z/]*)#([^"]+)"', text):
            found.append((name, route, anchor))
    return found


class TheLinksPointAtSomethingTest(unittest.TestCase):

    def setUp(self) -> None:
        self.links = fragment_links()
        self.routes = routes_to_pages()

    def test_there_are_links_to_check(self) -> None:
        """Without this the two tests below pass on an empty list and say nothing."""
        self.assertTrue(self.links, "no page links to a fragment; these checks are vacuous")

    def test_every_route_linked_to_is_one_the_gateway_serves(self) -> None:
        unserved = sorted({route for _, route, _ in self.links if route not in self.routes})
        self.assertEqual([], unserved,
                         "pages link to routes that serve no page: %r" % unserved)

    def test_every_anchor_exists_on_the_page_it_points_at(self) -> None:
        """A renamed div leaves the link pointing nowhere, and it still navigates."""
        broken = []
        for source_page, route, anchor in self.links:
            target = self.routes.get(route)
            if not target:
                continue
            if ('id="%s"' % anchor) not in read(os.path.join(PORTAL, target)):
                broken.append("%s -> %s#%s" % (source_page, target, anchor))
        self.assertEqual([], broken, "links whose target does not exist: %r" % broken)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheTabOpensTest(unittest.TestCase):

    def _run(self, page_name, anchors):
        return subprocess.run(
            ["node", HARNESS, os.path.join(PORTAL, page_name)] + sorted(set(anchors)),
            capture_output=True, text=True, timeout=300)

    def _targets(self):
        routes = routes_to_pages()
        grouped = {}
        for _, route, anchor in fragment_links():
            target = routes.get(route)
            if target:
                grouped.setdefault(target, []).append(anchor)
        return grouped

    def _by_mechanism(self, page_name, anchors):
        """Which anchors a tab answers, and which something else does.

        Not every link into a page names something a tab holds: the key portal answers "where does
        the first key come from" in a <details> above its tablist. The harness reports those rather
        than failing them, and they are checked below through the helper that actually opens them.
        """
        out = self._run(page_name, anchors).stdout
        folds = {a for a in set(anchors) if ("skip #%s is not inside a tab pane" % a) in out}
        return out, sorted(set(anchors) - folds), sorted(folds)

    def test_the_pages_that_are_linked_into_open_the_right_tab(self) -> None:
        grouped = self._targets()
        self.assertTrue(grouped, "nothing is linked into; this check is vacuous")
        opened = 0
        for page_name, anchors in sorted(grouped.items()):
            with self.subTest(page=page_name):
                proc = self._run(page_name, anchors)
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
                _out, tabbed, _folds = self._by_mechanism(page_name, anchors)
                for anchor in tabbed:
                    self.assertIn("ok   arriving at #%s opens the tab that holds it" % anchor,
                                  proc.stdout)
                opened += len(tabbed)
        self.assertGreater(opened, 0, "no link opens a tab; this check is quantified over nothing")

    def test_the_reader_is_taken_to_the_target_not_just_the_pane(self) -> None:
        """The pane can be metres long. Opening it and leaving them at the top is most of the way
        to the original problem: they are looking at a page that does not obviously concern them."""
        for page_name, anchors in sorted(self._targets().items()):
            out, tabbed, _folds = self._by_mechanism(page_name, anchors)
            for anchor in tabbed:
                self.assertIn("ok   arriving at #%s scrolls to it once the pane is showing" % anchor,
                              out)

    def test_the_same_page_case_is_handled(self) -> None:
        """The strip is on the setup page too, so its own segments load no document at all."""
        for page_name, anchors in sorted(self._targets().items()):
            out, tabbed, _folds = self._by_mechanism(page_name, anchors)
            if not tabbed:
                continue
            self.assertIn("ok   the page listens for the fragment changing under it", out)
            self.assertIn("ok   changing the fragment without reloading still opens the tab", out)

    def test_a_link_no_tab_answers_is_answered_by_a_fold(self) -> None:
        """The other half of the split, so "not my business" cannot become "nobody's business"."""
        checked = 0
        for page_name, anchors in sorted(self._targets().items()):
            _out, _tabbed, folds = self._by_mechanism(page_name, anchors)
            for anchor in folds:
                proc = subprocess.run(["node", FOLD_HARNESS, os.path.join(PORTAL, page_name)],
                                      capture_output=True, text=True, timeout=300)
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
                self.assertIn("ok   arriving at #%s unfolds it" % anchor, proc.stdout,
                              "no tab holds it and no fold opens it either")
                checked += 1
        self.assertGreater(checked, 0,
                           "no link is answered by a fold; this check is quantified over nothing")

    def test_a_fragment_that_means_nothing_here_changes_nothing(self) -> None:
        for page_name, anchors in sorted(self._targets().items()):
            out = self._run(page_name, anchors).stdout
            self.assertIn("ok   a fragment naming nothing leaves the default tab up", out)
            self.assertIn("ok   a fragment that is not valid escaping is survivable", out)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class EveryTabbedPageAnswersAFragmentTest(unittest.TestCase):
    """Not only the pages something links into today.

    The helper is shared, so a page growing tabs gets this behaviour without anyone deciding it
    should. Running every tabbed page through the harness -- with the fragment derived from its own
    markup rather than supplied -- is what keeps that true.
    """

    def test_every_page_with_tabs_and_panes(self) -> None:
        """Pages whose tabs are in the markup. The API page's are built from the route catalogue
        after a fetch, and it calls the helper once they exist, so a fragment naming one of them is
        resolved then -- there is just nothing in the file for a source scan to check."""
        tabbed = [name for name, text in pages().items()
                  if 'role="tab"' in markup(text) and 'class="pane"' in markup(text)]
        self.assertGreaterEqual(len(tabbed), 4, tabbed)
        for name in tabbed:
            with self.subTest(page=name):
                proc = subprocess.run(["node", HARNESS, os.path.join(PORTAL, name)],
                                      capture_output=True, text=True, timeout=300)
                self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
                self.assertIn("ok   there is a fragment to follow", proc.stdout)


if __name__ == "__main__":
    unittest.main()
