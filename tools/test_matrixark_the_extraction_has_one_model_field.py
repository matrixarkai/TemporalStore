#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal offers one extraction model field, and it lands where the provider reads it.

There were two: ``extraction.model`` and ``extraction.anthropic_model``. A deployment uses one
provider at a time, so one of the two fields was always inert -- and which one was inert depended on
a dropdown three rows above it. The Anthropic path reads ``MATRIXARK_ANTHROPIC_MODEL`` and ignores
``MATRIXARK_EXTRACTION_MODEL`` entirely, so a customer on Anthropic who filled in "Extraction model"
had configured nothing at all.

One field now, whose variable follows the selected provider -- the same mechanism the API key uses,
for the same reason: the value a customer types has to reach the code that reads it, and which
variable that is is not the customer's problem.

Blank means the provider's own default, which differs per provider, so the field declares no default
of its own. That is the honest state for a value with more than one fallback, and the help names
both so nobody has to read the source to find out what is running.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

try:
    from tools import matrixark_v1_gateway as gw  # type: ignore
except ImportError:
    import matrixark_v1_gateway as gw  # type: ignore

cfg = gw._gwconfig

OPENAI_VARIABLE = "MATRIXARK_EXTRACTION_MODEL"
ANTHROPIC_VARIABLE = "MATRIXARK_ANTHROPIC_MODEL"
SUMMARY_VARIABLE = "MATRIXARK_SUMMARY_MODEL"
PARTICIPATING = ("MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
                 OPENAI_VARIABLE, ANTHROPIC_VARIABLE, SUMMARY_VARIABLE,
                 "MATRIXARK_EXTRACTION_BASE_URL", "MATRIXARK_ANTHROPIC_API_BASE", "OPENAI_MODEL")


class Case(unittest.TestCase):
    """Resolution reads the process environment, so it is controlled here -- a sibling suite leaving
    a provider set is the difference between passing alone and failing the full run."""

    def setUp(self) -> None:
        self._saved = {n: os.environ.get(n) for n in PARTICIPATING}
        for name in PARTICIPATING:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def variable_for(self, provider: str) -> str:
        return cfg._env_name(cfg.SETTINGS_BY_KEY["extraction.model"],
                             {"extraction.provider": provider})

    def on(self, provider: str, **environment) -> None:
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = provider
        for name, value in environment.items():
            os.environ[name] = value


class ThePortalOffersOneFieldTest(unittest.TestCase):

    def test_the_second_field_is_gone(self) -> None:
        self.assertNotIn("extraction.anthropic_model", cfg.SETTINGS_BY_KEY)

    def test_exactly_one_setting_names_an_extraction_model(self) -> None:
        """Counted, so a third cannot appear beside it the next time a provider is added."""
        naming = [key for key, setting in cfg.SETTINGS_BY_KEY.items()
                  if setting.group == "extraction" and key.endswith("model")]
        self.assertEqual(["extraction.model"], naming)

    def test_it_declares_no_default_of_its_own(self) -> None:
        """The build's fallback differs per provider, so naming one here would be wrong for the
        other -- and `export_settings` writes a declared default to a clone as an explicit value."""
        self.assertEqual("", cfg.SETTINGS_BY_KEY["extraction.model"].default)

    def test_blank_means_a_different_model_on_each_provider(self) -> None:
        """The reason the field declares no default: the two answers differ, and the one it gives
        has to be the one the help names. A single answer for both would satisfy the help test on
        its own, which is how a mutation collapsing this survived a first pass."""
        anthropic = cfg.extraction_model_default("anthropic")
        openai = cfg.extraction_model_default("openai_compatible")
        self.assertNotEqual(anthropic, openai)
        help_text = cfg.SETTINGS_BY_KEY["extraction.model"].help
        self.assertIn(anthropic, help_text)
        self.assertIn(openai, help_text)

    def test_the_help_names_what_blank_means_on_each_provider(self) -> None:
        help_text = cfg.SETTINGS_BY_KEY["extraction.model"].help
        self.assertIn("claude-sonnet-5", help_text)
        self.assertIn("qwen2.5:1.5b", help_text)


class TheValueLandsWhereTheProviderReadsItTest(Case):

    def test_anthropic_gets_its_own_variable(self) -> None:
        self.assertEqual(ANTHROPIC_VARIABLE, self.variable_for("anthropic"))

    def test_everything_else_gets_the_general_one(self) -> None:
        for provider in ("openai_compatible", "deterministic", "", "typo_provider"):
            with self.subTest(provider=provider):
                self.assertEqual(OPENAI_VARIABLE, self.variable_for(provider))

    def test_the_provider_may_come_from_the_launcher(self) -> None:
        """A deployment that sets the provider in the environment and the model in the portal is the
        mixed case, and it has to resolve the same way."""
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "anthropic"
        self.assertEqual(ANTHROPIC_VARIABLE,
                         cfg._env_name(cfg.SETTINGS_BY_KEY["extraction.model"], {}))

    def test_a_write_reaches_the_variable_the_provider_reads(self) -> None:
        """The whole point, end to end: apply the stored value and see where it lands."""
        seeded = cfg.apply_boot({"values": {"extraction.provider": "anthropic",
                                            "extraction.model": "claude-opus-5"}})
        self.assertIn(ANTHROPIC_VARIABLE, seeded)
        self.assertEqual("claude-opus-5", os.environ.get(ANTHROPIC_VARIABLE))
        self.assertIsNone(os.environ.get(OPENAI_VARIABLE))

    def test_the_same_write_on_the_other_provider_lands_elsewhere(self) -> None:
        """The floor: if it always wrote one variable, the assertion above would pass by accident."""
        seeded = cfg.apply_boot({"values": {"extraction.provider": "openai_compatible",
                                            "extraction.model": "gpt-4o-mini"}})
        self.assertIn(OPENAI_VARIABLE, seeded)
        self.assertEqual("gpt-4o-mini", os.environ.get(OPENAI_VARIABLE))
        self.assertIsNone(os.environ.get(ANTHROPIC_VARIABLE))


class ThePanelAndTheProbeFollowItTest(Case):

    def test_the_panel_reports_the_model_in_use(self) -> None:
        self.on("anthropic", **{ANTHROPIC_VARIABLE: "claude-opus-5",
                                OPENAI_VARIABLE: "gpt-4o"})
        self.assertEqual("claude-opus-5",
                         gw._model_config_snapshot()["extraction"]["model"])

    def test_the_other_provider_reports_the_other_one(self) -> None:
        self.on("openai_compatible", **{ANTHROPIC_VARIABLE: "claude-opus-5",
                                        OPENAI_VARIABLE: "gpt-4o"})
        self.assertEqual("gpt-4o", gw._model_config_snapshot()["extraction"]["model"])

    def test_the_picker_writes_the_one_field(self) -> None:
        """It used to choose between two keys. There is one, so there is nothing to choose."""
        self.on("anthropic")
        self.assertEqual("extraction.model", gw._model_picker_body("extraction")["key"])
        self.on("openai_compatible")
        self.assertEqual("extraction.model", gw._model_picker_body("extraction")["key"])


class TheSummaryHasNoModelOfItsOwnTest(Case):
    """A node summary is made by the extraction endpoint with the extraction key. A separate model
    was a second name for the same call, and the two could name models one endpoint does not both
    serve -- with no screen showing the pair."""

    def test_the_control_is_gone(self) -> None:
        self.assertNotIn("summary.model", cfg.SETTINGS_BY_KEY)

    def test_what_is_left_of_the_summary_group_is_not_a_model(self) -> None:
        """provider and max_tokens stay: they are choices about the summary, not a second model."""
        remaining = sorted(k for k in cfg.SETTINGS_BY_KEY if k.startswith("summary."))
        self.assertEqual(["summary.max_tokens", "summary.provider"], remaining)

    def test_a_launcher_that_still_sets_it_is_told(self) -> None:
        """It stopped mattering rather than never existing, which is the worse of the two to leave
        silent."""
        self.on("openai_compatible", **{SUMMARY_VARIABLE: "something-cheap"})
        named = [w for w in gw._model_config_snapshot()["warnings"]
                 if SUMMARY_VARIABLE in w]
        self.assertEqual(1, len(named))
        self.assertIn("something-cheap", named[0])
        self.assertIn("no longer read", named[0])

    def test_it_says_which_field_decides_now(self) -> None:
        self.on("openai_compatible", **{SUMMARY_VARIABLE: "something-cheap"})
        named = [w for w in gw._model_config_snapshot()["warnings"] if SUMMARY_VARIABLE in w][0]
        self.assertIn("Extraction model", named)

    def test_nothing_is_said_when_it_is_not_set(self) -> None:
        self.on("openai_compatible")
        self.assertEqual([], [w for w in gw._model_config_snapshot()["warnings"]
                              if SUMMARY_VARIABLE in w])


if __name__ == "__main__":
    unittest.main()
