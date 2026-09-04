#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every panel has one main region, one title, and no control without a name.

Six of the seven pages opened their content with ``<div class="wrap">``. The key portal opened with
``<main class="wrap">``, so the intent was settled and the rest had simply never been given it.
Without a main landmark there is nothing to skip to: a screen reader arriving on Setup walks the
nav, the status strip and the tab list again on every visit, and a "skip to content" command has no
target.

The API page's route filter was an ``<input type="search">`` under an ``<h2>Filter</h2>`` with no
label of any kind. A heading above a control is not its name -- announced on its own that field was
"search edit", which is exactly what a customer gets who cannot see where it sits.

Every other control on all seven pages was already named, most by a label that WRAPS it rather than
one that points at it. Scanning only for ``for=`` reports fourteen of those as broken and hides the
one that is, which is why the check here looks inside each label's extent as well.

These are sweeps, so each carries a floor: a selector that stopped matching would otherwise leave
every one of them quantified over nothing and passing.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")


def pages() -> dict:
    out = {}
    for name in sorted(os.listdir(PORTAL)):
        if not name.endswith(".html"):
            continue
        with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
            out[name] = handle.read()
    return out


def markup(text: str) -> str:
    """The document without its scripts or stylesheet.

    Pages build markup out of strings -- the API page assembles its whole route table that way --
    so a scan of the raw file finds template fragments and counts them as structure.
    """
    text = re.sub(r"<script[\s\S]*?</script>", "", text)
    return re.sub(r"<style[\s\S]*?</style>", "", text)


def unnamed_controls(body: str) -> list:
    """Controls nothing announces.

    Three things name a control: a ``<label for=id>``, a ``<label>`` that wraps it, or an
    ``aria-label`` on the control. The wrapping form is the one this portal mostly uses.
    """
    pointed = set(re.findall(r'<label[^>]*for="([^"]+)"', body))
    wrapped = set()
    for opening in re.finditer(r"<label\b[^>]*>", body):
        end = body.find("</label>", opening.end())
        if end == -1:
            continue
        for tag in re.findall(r"<(?:input|select|textarea)\b[^>]*>", body[opening.end():end]):
            found = re.search(r'\sid="([^"]+)"', tag)
            wrapped.add(found.group(1) if found else tag)

    bare = []
    for tag in re.findall(r"<(?:input|select|textarea)\b[^>]*>", body):
        if 'type="hidden"' in tag or "aria-label" in tag:
            continue
        found = re.search(r'\sid="([^"]+)"', tag)
        key = found.group(1) if found else tag
        if key not in pointed and key not in wrapped:
            bare.append(key)
    return bare


class EveryPanelIsBuiltTheSameWayTest(unittest.TestCase):

    def setUp(self) -> None:
        self.pages = pages()
        self.assertGreaterEqual(len(self.pages), 7,
                                "fewer pages than the portal has; this sweep is not seeing them")

    def test_each_has_exactly_one_main_region(self) -> None:
        """The landmark a reader skips to. Two would make "the content" ambiguous, none leaves the
        command with no target at all."""
        wrong = {name: len(re.findall(r"<main\b", markup(text)))
                 for name, text in self.pages.items()}
        self.assertEqual({}, {k: v for k, v in wrong.items() if v != 1}, wrong)

    def test_the_main_region_holds_the_content(self) -> None:
        """A landmark wrapped around nothing is worse than none: it answers "skip to content" with
        an empty room.

        Not asserted: that the ``<h1>`` is inside it. The key portal puts its title in a banner
        header above the main region, which is a perfectly good structure -- and a check that
        forbade it would be demanding uniformity rather than navigability.
        """
        for name, text in self.pages.items():
            with self.subTest(page=name):
                body = markup(text)
                start = body.index("<main")
                end = body.index("</main>", start)
                inside = body[start:end]
                self.assertGreater(len(inside), 2000, "the main region is nearly empty")
                self.assertIn("<section", inside, "no content section inside the main region")
                after = body[end:]
                self.assertNotIn("<section", after,
                                 "a content section sits after the main region, where skipping to "
                                 "the content skips past it")

    def test_each_has_exactly_one_title(self) -> None:
        counts = {name: len(re.findall(r"<h1\b", markup(text)))
                  for name, text in self.pages.items()}
        self.assertEqual({}, {k: v for k, v in counts.items() if v != 1}, counts)

    def test_no_heading_level_is_skipped(self) -> None:
        """An h2 followed by an h4 reads as a missing section to anything navigating by heading."""
        broken = {}
        for name, text in self.pages.items():
            levels = [int(m.group(1)) for m in re.finditer(r"<h([1-6])\b", markup(text))]
            self.assertTrue(levels, "%s has no headings at all" % name)
            previous = 0
            for level in levels:
                if previous and level > previous + 1:
                    broken[name] = "h%d after h%d" % (level, previous)
                    break
                previous = level
        self.assertEqual({}, broken)

    def test_every_control_has_a_name(self) -> None:
        bare = {name: unnamed_controls(markup(text)) for name, text in self.pages.items()}
        self.assertEqual({}, {k: v for k, v in bare.items() if v},
                         "controls nothing announces: %r" % {k: v for k, v in bare.items() if v})

    def test_the_sweep_actually_sees_controls(self) -> None:
        """Without this the check above passes on a regex that stopped matching inputs."""
        total = sum(len(re.findall(r"<input\b", markup(text))) for text in self.pages.values())
        self.assertGreater(total, 20, total)


class ANameForAReaderIsNotShownTwiceTest(unittest.TestCase):
    """Where a heading already says what a control is for, the label is for the reader who cannot
    see the heading -- and a class that does not exist would put it on screen next to it."""

    def test_every_page_using_the_class_defines_it(self) -> None:
        used = {name for name, text in pages().items() if 'class="sr-only"' in text}
        self.assertTrue(used, "nothing uses the class; this check is quantified over nothing")
        undefined = sorted(name for name in used if ".sr-only{" not in pages()[name])
        self.assertEqual([], undefined,
                         "a visually-hidden label with no rule to hide it: %r" % undefined)

    def test_it_is_clipped_rather_than_removed(self) -> None:
        """display:none and visibility:hidden take an element out of the accessibility tree too,
        which would hide the name from exactly the reader it was written for."""
        for name, text in pages().items():
            if ".sr-only{" not in text:
                continue
            with self.subTest(page=name):
                rule = text[text.index(".sr-only{"):]
                rule = rule[:rule.index("}") + 1]
                self.assertNotIn("display:none", rule)
                self.assertNotIn("visibility:hidden", rule)
                self.assertIn("clip:", rule)


if __name__ == "__main__":
    unittest.main()
