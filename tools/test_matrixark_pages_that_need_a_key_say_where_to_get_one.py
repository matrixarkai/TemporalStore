#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A page that demands a key says where the first one comes from.

The key portal explains how an admin key is minted. It is the only page that does, and somebody who
has no key is not on it -- they are on Setup, or Overview, or Ingestion, reading "Enter an admin
key" with nothing beside it. So those three now link at the answer.

Catalogue and Explore deliberately do not. They ask for ``skill:read``/``resource:read`` and
``context:*``, and the command that block prints mints admin scopes -- pointing them there would
hand them a key that looks right and is refused, which is the exact trap the block exists to warn
about. Which pages get the link is therefore derived from what each one asks for, not from a list
somebody has to remember to update.

The link's destination is a ``<details>``, shut by default because it answers a question somebody
has once. A link to a closed ``<details>`` arrives with the answer still folded up: right page,
summary line, no better off. Browsers disagree about expanding it, so the shared strip helper does
it, and that is behaviour -- a helper wired to nothing reads exactly like one that works, so it is
run rather than grepped.
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
HARNESS = os.path.join(PORTAL, "folded_answer_harness.js")
KEY_PAGE = os.path.join(PORTAL, "api_key_portal.html")
POINTER = 'href="/v1/admin/portal#firstkey"'


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def pages() -> dict:
    return {name: read(os.path.join(PORTAL, name))
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


def asks_for(text: str) -> list:
    """What each key input on a page says it wants, taken from its own placeholder."""
    return re.findall(r'<input id="key"[^>]*placeholder="([^"]*)"', text)


class TheAdminPagesPointAtItTest(unittest.TestCase):

    def setUp(self) -> None:
        self.pages = pages()
        self.admin = {name for name, text in self.pages.items()
                      if any("admin scope" in ask for ask in asks_for(text))}
        self.other = {name for name, text in self.pages.items()
                      if asks_for(text) and name not in self.admin}

    def test_there_are_pages_of_both_kinds(self) -> None:
        """Both checks below are about a split; with everything on one side they say nothing."""
        self.assertGreaterEqual(len(self.admin), 3, sorted(self.admin))
        self.assertGreaterEqual(len(self.other), 1, sorted(self.other))

    def test_every_page_asking_for_an_admin_key_says_where_to_get_one(self) -> None:
        silent = sorted(name for name in self.admin
                        if POINTER not in self.pages[name] and name != "api_key_portal.html")
        self.assertEqual([], silent,
                         "pages that demand an admin key and do not say how to get one: %r" % silent)

    def test_the_pages_wanting_other_scopes_are_not_sent_there(self) -> None:
        """The block prints a command minting admin scopes. A page whose work needs skill:read
        would get a key that looks right and is refused -- the trap the block warns about."""
        misdirected = sorted(name for name in self.other if POINTER in self.pages[name])
        self.assertEqual([], misdirected,
                         "pages sent to a command that mints the wrong scopes: %r" % misdirected)

    def test_the_key_page_does_not_link_to_itself(self) -> None:
        self.assertNotIn(POINTER, read(KEY_PAGE))


class TheDestinationExistsTest(unittest.TestCase):

    def test_the_anchor_is_on_the_key_page(self) -> None:
        self.assertIn('id="firstkey"', read(KEY_PAGE))

    def test_the_anchor_is_the_fold_itself(self) -> None:
        """Naming something inside it would work too, but naming the fold is what makes the
        summary line the thing the reader lands on."""
        self.assertRegex(read(KEY_PAGE), r'<details[^>]*id="firstkey"')

    def test_it_is_still_shut_by_default(self) -> None:
        """If it ships open, every check that it gets opened passes without the helper."""
        tag = re.search(r'<details[^>]*id="firstkey"[^>]*>', read(KEY_PAGE)).group(0)
        self.assertNotRegex(tag, r"\sopen[\s>]")

    def test_every_page_can_unfold_a_named_target(self) -> None:
        """The helper rides the shared strip, so a link added from anywhere later already works."""
        without = sorted(name for name, text in pages().items() if "function reveal()" not in text)
        self.assertEqual([], without, "pages that would arrive with the answer folded up: %r" % without)


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class TheLinkActuallyUnfoldsItTest(unittest.TestCase):

    def _run(self):
        proc = subprocess.run(["node", HARNESS, KEY_PAGE],
                              capture_output=True, text=True, timeout=300)
        return proc

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_arriving_at_the_anchor_opens_it(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   arriving at #firstkey unfolds it", out)
        self.assertIn("ok   arriving at #firstkey scrolls to it", out)

    def test_something_inside_it_opens_it_too(self) -> None:
        self.assertIn("ok   a fragment naming something inside the fold unfolds it too",
                      self._run().stdout)

    def test_it_stays_shut_for_everybody_else(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   with no fragment it stays shut", out)
        self.assertIn("ok   a fragment naming nothing leaves it shut", out)
        self.assertIn("ok   a fragment naming something outside any fold leaves it shut", out)

    def test_following_the_link_from_the_same_page_works(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   the page listens for the fragment changing under it", out)
        self.assertIn("ok   changing the fragment without reloading unfolds it", out)


if __name__ == "__main__":
    unittest.main()
