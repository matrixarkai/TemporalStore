#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every value a control offers is explained by that control.

A dropdown is a promise that each entry does something. Three of the seven choices across the two
provider controls were never mentioned in their own help::

    extraction.provider   anthropic   -- and it reads different fields from the other two
    embedding.provider    voyage      -- same fields, its own defaults
    embedding.provider    local       -- a synonym for deterministic: it runs NO encoder

``local`` is the one that misleads. It reads like "run a model on this box" and produces the same
hash vectors as ``deterministic`` -- the code says so by listing it in the deterministic set, and the
warning about hash vectors fires for it. The way to run an encoder locally is ``openai_compatible``
pointed at the bundled server, which is what the Local MiniLM preset does.

The rule below is the general one, because the specific omissions are not interesting: **every
choice a control offers must appear in that control's help.** A value a customer can pick and cannot
look up is a guess, and one of these three was a guess that produced hash vectors.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402


def with_choices():
    return [s for s in cfg.SETTINGS if s.choices]


class EveryOfferedValueIsExplainedTest(unittest.TestCase):

    def test_there_are_choice_lists_to_check(self) -> None:
        """The rule is a loop; over an empty list it proves nothing."""
        self.assertGreaterEqual(len(with_choices()), 5)

    def test_every_choice_appears_in_its_own_help(self) -> None:
        for setting in with_choices():
            for choice in setting.choices:
                if not choice:            # the blank "same as the one above" option
                    continue
                with self.subTest(setting=setting.key, choice=choice):
                    self.assertIn(choice, setting.help or "",
                                  "%s offers %r and never says what it does"
                                  % (setting.key, choice))

    def test_the_blank_choice_is_explained_as_a_word(self) -> None:
        """A blank cannot appear in the help as itself, so the control has to name the idea."""
        for setting in with_choices():
            if "" not in setting.choices:
                continue
            with self.subTest(setting=setting.key):
                self.assertRegex(setting.help or "", r"[Bb]lank",
                                 "%s offers a blank option and never says what blank means"
                                 % setting.key)


class TheThreeThatWereMissingTest(unittest.TestCase):
    """Named, because they are why this file exists and each says something different."""

    def test_anthropic_is_told_it_uses_other_fields(self) -> None:
        help_text = cfg.SETTINGS_BY_KEY["extraction.provider"].help
        self.assertIn("anthropic", help_text)
        self.assertIn("Anthropic model", help_text)
        self.assertIn("Anthropic base URL", help_text)

    def test_voyage_is_told_it_uses_the_same_fields(self) -> None:
        help_text = cfg.SETTINGS_BY_KEY["embedding.provider"].help
        self.assertIn("voyage", help_text)
        self.assertIn("VOYAGE_API_KEY", help_text)
        self.assertIn("voyage-3", help_text)

    def test_local_is_told_it_runs_no_encoder(self) -> None:
        """The one that misleads: it reads like a local model and is not one."""
        help_text = cfg.SETTINGS_BY_KEY["embedding.provider"].help
        self.assertIn("local is a synonym for deterministic", help_text)
        self.assertIn("NO encoder", help_text)

    def test_local_is_pointed_at_the_thing_that_does_work(self) -> None:
        """Saying what it is not, without saying what to use, leaves the reader stuck."""
        help_text = cfg.SETTINGS_BY_KEY["embedding.provider"].help
        self.assertIn("Local MiniLM", help_text)


class TheClaimsAboutLocalAreTrueTest(unittest.TestCase):
    """Prose against behaviour, so the help cannot quietly become false."""

    def _sets(self):
        with open(os.path.join(TOOLS, "matrixark_mcp_embeddings.py"), encoding="utf-8") as handle:
            source = handle.read()
        api = re.search(r"_API_EMBEDDING_PROVIDERS\s*=\s*\{([^}]*)\}", source)
        # Read from the assignment, as the API set above already is. The encoder used to spell
        # these four spellings out at each branch and now names them once; the check that the
        # constant is what the code actually DISPATCHES on lives in
        # test_matrixark_an_unrecognised_provider_name_is_named, which follows the name.
        oss = re.search(r"_OSS_EMBEDDING_PROVIDERS\s*=\s*\{([^}]*)\}", source)
        strip = lambda text: {v.strip().strip('"').strip("'") for v in text.split(",") if v.strip()}
        return strip(api.group(1)), strip(oss.group(1))

    def test_local_selects_neither_encoder_path(self) -> None:
        api, oss = self._sets()
        self.assertNotIn("local", api, "local now calls an API encoder; the help says it does not")
        self.assertNotIn("local", oss, "local now loads a local encoder; the help says it does not")

    def test_the_two_that_do_run_an_encoder_still_do(self) -> None:
        """The floor. If neither set matched anything, the test above would pass by vacuity."""
        api, _oss = self._sets()
        self.assertIn("openai_compatible", api)
        self.assertIn("voyage", api)

    def test_the_deterministic_set_still_counts_local_as_one_of_its_own(self) -> None:
        """What makes `local` a synonym rather than an unknown value: the warning that fires for
        deterministic fires for it too, and it is never reported as a name nothing recognises.

        Asserted through the classifier rather than by matching a literal set in the gateway source.
        That set existed in three copies and caught only the names written into it -- a misspelt
        provider fell past all three -- so the classifier that replaced it is now the thing that has
        to keep treating `local` as a deliberate choice.
        """
        try:
            from tools import matrixark_gateway_config as gwcfg  # type: ignore
        except ImportError:
            import matrixark_gateway_config as gwcfg  # type: ignore
        self.assertEqual("hash", gwcfg.embedding_provider_effect("local"))
        for group in ("embedding", "extraction"):
            with self.subTest(group=group):
                self.assertFalse(gwcfg.provider_is_unrecognised(group, "local"))


if __name__ == "__main__":
    unittest.main()
