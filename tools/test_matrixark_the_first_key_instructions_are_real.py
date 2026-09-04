#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The instructions for getting a first key describe a command that exists.

Every portal page asks for an admin key and is inert without one. A deployment enforcing keys
starts with none, and the key page cannot mint the first: minting is itself an admin-scoped call.
So the first thing a new customer met was a page demanding something they had no way to obtain.

The Connection panel now says where the first one comes from. Instructions on a page are the kind
of thing that goes quietly wrong -- a flag is renamed, a scope is retired, the tool moves -- and
nobody notices until somebody following them gets an error the page cannot explain. So every part
of the printed command is checked against the thing it describes:

  * the tool is at the path the page prints,
  * every ``--flag`` shown is a flag that tool accepts,
  * every scope named is a scope the gateway knows.

The scopes matter most. Left off, the provisioning tool mints the four ``context:*`` scopes, and a
key carrying those is refused by this page -- so the obvious command produces a key that looks
right and does not work. The page says so, and the flag it tells you to pass has to keep existing.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PAGE = os.path.join(TOOLS, "portal", "api_key_portal.html")
TOOL = os.path.join(TOOLS, "matrixark_provision_api_key.py")


def page() -> str:
    with io.open(PAGE, encoding="utf-8") as handle:
        return handle.read()


def panel() -> str:
    """The block itself, not the stylesheet.

    Both carry the word `firstkey`, and the stylesheet comes first in the file -- so searching for
    the name and slicing forward reads CSS and finds none of the prose.
    """
    source = page()
    start = source.index('<details class="firstkey">')
    return source[start:source.index("</details>", start)]


def command() -> str:
    """The command the page prints, and nothing else."""
    block = panel()
    start = block.index('<pre class="firstkey-cmd">')
    return block[start:block.index("</pre>", start)]


class ThePageAnswersTheQuestionTest(unittest.TestCase):

    def test_the_connection_panel_says_where_a_first_key_comes_from(self) -> None:
        self.assertIn("No key yet?", page())

    def test_it_says_the_page_cannot_mint_it(self) -> None:
        """The reason is the part that stops somebody hunting for a button that is not there."""
        self.assertIn("cannot mint the first", page())

    def test_it_warns_that_the_key_is_shown_once(self) -> None:
        self.assertIn("printed once", panel())


class TheCommandIsRealTest(unittest.TestCase):

    def test_the_tool_it_names_exists(self) -> None:
        self.assertIn("matrixark_provision_api_key.py", command())
        self.assertTrue(os.path.exists(TOOL),
                        "the page tells people to run a tool that is not in the tree")

    def test_every_flag_shown_is_one_the_tool_accepts(self) -> None:
        with io.open(TOOL, encoding="utf-8") as handle:
            tool = handle.read()
        shown = sorted(set(re.findall(r"--[a-z][a-z-]+", command())))
        self.assertTrue(shown, "no flags found in the command; this check is vacuous")
        unknown = [flag for flag in shown if ('"%s"' % flag) not in tool]
        self.assertEqual([], unknown,
                         "the page prints flags this tool does not accept: %r" % unknown)

    def test_every_scope_named_is_one_the_gateway_knows(self) -> None:
        """A retired scope in printed instructions mints a key that is refused, and the customer
        has no way to tell the instructions were stale."""
        import matrixark_v1_gateway as gw

        known = {entry["scope"] for entry in gw.SCOPE_CATALOG}
        shown = sorted(set(re.findall(r"[a-z]+:[a-z_]+", command())))
        self.assertTrue(shown, "no scopes found in the command; this check is vacuous")
        unknown = [scope for scope in shown if scope not in known]
        self.assertEqual([], unknown,
                         "the page names scopes this deployment does not have: %r" % unknown)

    def test_the_scopes_shown_can_actually_use_this_page(self) -> None:
        """The whole reason the flag is spelled out: the tool's default scopes cannot."""
        self.assertIn("admin:api_key", command(),
                      "the command omits the one scope this page requires")


if __name__ == "__main__":
    unittest.main()
