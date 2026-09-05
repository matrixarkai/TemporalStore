#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal's API key lands in the variable the SELECTED provider actually reads.

Both secrets are written into whatever variable ``*.api_key_env`` names. That name used to fall back
to ``OPENAI_API_KEY`` for every provider, while the provider code's own fallback is per provider:
the encoder reads ``VOYAGE_API_KEY`` on Voyage, and extraction reads ``ANTHROPIC_API_KEY`` on
Anthropic. So a customer who picked either of those, left the variable field alone and typed their
key got it written into a variable their provider never reads -- with no error, because an
unreachable encoder falls back to hash vectors unless "Fail instead of falling back" is on.

Every expectation here is PARSED OUT OF THE PROVIDER MODULES rather than restated, so a new provider
branch, or a renamed fallback, fails this instead of silently reopening the hole.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

ENCODER = "matrixark_mcp_embeddings.py"
CORE = "matrixark_mcp_core.py"
SECRETS = {"embedding": "embedding.api_key", "extraction": "extraction.api_key"}


def parse(filename: str) -> ast.Module:
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        return ast.parse(handle.read(), filename=filename)


def _environ_get_default(node: ast.AST, variable: str) -> str | None:
    """The literal fallback in ``os.environ.get(variable, "...")``, wherever it sits under node."""
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call) or len(sub.args) != 2:
            continue
        target = sub.func
        if not (isinstance(target, ast.Attribute) and target.attr in {"get", "getenv"}):
            continue
        first, second = sub.args
        if (isinstance(first, ast.Constant) and first.value == variable
                and isinstance(second, ast.Constant) and isinstance(second.value, str)):
            return second.value
    return None


def encoder_key_variables() -> dict:
    """{provider or None for 'anything else': the variable the encoder reads}, from the source."""
    tree = parse(ENCODER)
    function = next(node for node in ast.walk(tree)
                    if isinstance(node, ast.FunctionDef) and node.name == "_api_embedding_config")
    found = {}
    for node in ast.walk(function):
        if not isinstance(node, ast.If):
            continue
        names = [c.value for c in ast.walk(node.test)
                 if isinstance(c, ast.Constant) and isinstance(c.value, str)]
        variable = "MATRIXARK_EMBEDDING_API_KEY_ENV"
        for branch, key in ((node.body, names[0] if names else None), (node.orelse, None)):
            if not branch:
                continue
            default = _environ_get_default(ast.Module(body=list(branch), type_ignores=[]), variable)
            if default is not None:
                found[key] = default
    return found


def extraction_key_variables() -> dict:
    """Same, for the two module constants extraction resolves its key variable through."""
    tree = parse(CORE)
    by_constant = {"ANTHROPIC_LLM_API_KEY_ENV": "anthropic", "EXTRACTION_LLM_API_KEY_ENV": None}
    found = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in by_constant:
                default = _environ_get_default(node, "MATRIXARK_EXTRACTION_API_KEY_ENV")
                if default is not None:
                    found[by_constant[target.id]] = default
    return found


def resolved(group: str, values: dict) -> str:
    return cfg._env_name(cfg.SETTINGS_BY_KEY[SECRETS[group]], values)


class _NothingInherited(unittest.TestCase):
    """Resolution reads the process environment on purpose: a variable the LAUNCHER set outranks the
    provider default, and the provider code resolves the same way, so the two still agree. That
    makes these assertions -- which compare a runtime resolution against the fallback written in the
    source -- true only when nothing else has set one. Sibling suites in the same process do set
    them, which is why this passed alone and failed in the full run.
    """

    PARTICIPATING = ("MATRIXARK_EXTRACTION_API_KEY_ENV", "MATRIXARK_EMBEDDING_API_KEY_ENV",
                     "MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
                     "MATRIXARK_EMBEDDING_PROVIDER")

    def setUp(self) -> None:
        self._inherited = {n: os.environ.get(n) for n in self.PARTICIPATING}
        for name in self.PARTICIPATING:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._inherited.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


class TheSourceStillSaysWhatWeThinkItSaysTest(unittest.TestCase):
    """The rest of this file is only as good as the parse. If these stop finding branches, every
    other assertion here would pass vacuously."""

    def test_the_encoder_has_a_per_provider_fallback_and_a_general_one(self) -> None:
        found = encoder_key_variables()
        self.assertIn(None, found, "no else branch found in _api_embedding_config")
        self.assertTrue([k for k in found if k], "no per-provider branch found; the parse broke")

    def test_extraction_has_both_constants(self) -> None:
        self.assertEqual({"anthropic", None}, set(extraction_key_variables()))


class TheKeyLandsWhereTheProviderLooksTest(_NothingInherited):
    """The defect, from the customer's side: pick a provider, type a key, and it must reach it."""

    def test_every_encoder_branch_agrees(self) -> None:
        for provider, reads in encoder_key_variables().items():
            chosen = provider or "openai_compatible"
            with self.subTest(provider=chosen):
                self.assertEqual(reads, resolved("embedding", {"embedding.provider": chosen}))

    def test_every_extraction_branch_agrees(self) -> None:
        for provider, reads in extraction_key_variables().items():
            chosen = provider or "openai_compatible"
            with self.subTest(provider=chosen):
                self.assertEqual(reads, resolved("extraction", {"extraction.provider": chosen}))

    def test_every_offered_provider_choice_reaches_a_variable_its_code_reads(self) -> None:
        """Nothing selectable in the portal may resolve to a name its own module never mentions."""
        sources = {"embedding": ENCODER, "extraction": CORE}
        for group, filename in sources.items():
            with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
                text = handle.read()
            for choice in cfg.SETTINGS_BY_KEY[group + ".provider"].choices:
                with self.subTest(group=group, provider=choice):
                    name = resolved(group, {group + ".provider": choice})
                    self.assertTrue(name, "resolved to no variable at all")
                    self.assertIn(name, text)

    def test_the_provider_may_come_from_the_launcher_rather_than_the_portal(self) -> None:
        """A deployment that sets the provider in the environment and the key in the portal is the
        mixed case, and it has to resolve the same way."""
        setting = cfg.SETTINGS_BY_KEY["embedding.provider"]
        previous = os.environ.get(setting.env)
        os.environ[setting.env] = "voyage"
        try:
            self.assertEqual(encoder_key_variables()["voyage"], resolved("embedding", {}))
        finally:
            if previous is None:
                os.environ.pop(setting.env, None)
            else:
                os.environ[setting.env] = previous


class AnExplicitVariableStillWinsTest(_NothingInherited):
    """The field is an override, not a suggestion -- a deployment pointing at a variable its own
    launcher fills, DEEPSEEK_API_KEY being the shipped example, must keep working."""

    def test_a_stored_name_beats_the_provider_default(self) -> None:
        self.assertEqual("DEEPSEEK_API_KEY", resolved(
            "extraction", {"extraction.provider": "anthropic",
                           "extraction.api_key_env": "DEEPSEEK_API_KEY"}))

    def test_a_launcher_set_name_beats_the_provider_default(self) -> None:
        variable = cfg.SETTINGS_BY_KEY["embedding.api_key_env"].env
        previous = os.environ.get(variable)
        os.environ[variable] = "LAUNCHER_KEY"
        try:
            self.assertEqual("LAUNCHER_KEY",
                             resolved("embedding", {"embedding.provider": "voyage"}))
        finally:
            if previous is None:
                os.environ.pop(variable, None)
            else:
                os.environ[variable] = previous


class TheDefaultNoLongerPinsOneProviderTest(_NothingInherited):

    def test_it_defaults_the_way_every_other_provider_dependent_field_does(self) -> None:
        """A blank default means "follow the provider", and it is only honest where the code's
        fallback really is per provider. These two were the exception -- one name for every
        provider -- and they were the two that misrouted the key.

        extraction.base_url and extraction.model were on this list and have since left it: their
        fallback is a single concrete value the build calls, so they now declare it. The remaining
        neighbours are the ones the encoder resolves per provider.
        """
        neighbours = ("embedding.api_base", "embedding.model")
        for key in neighbours:
            self.assertEqual("", cfg.SETTINGS_BY_KEY[key].default, key + " changed shape")
        for key in ("embedding.api_key_env", "extraction.api_key_env"):
            with self.subTest(setting=key):
                self.assertEqual("", cfg.SETTINGS_BY_KEY[key].default)

    def test_the_help_names_the_variable_each_provider_gets(self) -> None:
        """A customer has to be able to answer 'where did my key go' from the portal alone."""
        embedding = cfg.SETTINGS_BY_KEY["embedding.api_key_env"].help
        self.assertIn(encoder_key_variables()["voyage"], embedding)
        self.assertIn(encoder_key_variables()[None], embedding)
        extraction = cfg.SETTINGS_BY_KEY["extraction.api_key_env"].help
        self.assertIn(extraction_key_variables()["anthropic"], extraction)
        self.assertIn(extraction_key_variables()[None], extraction)


class EveryPresetLandsItsKeySomewhereTheProviderReadsTest(_NothingInherited):
    """Presets were never wrong, because every one of them named a variable explicitly. The two
    providers that had no preset were the two that broke, so both now have one."""

    def test_each_preset_resolves_to_what_its_provider_reads(self) -> None:
        encoder, extraction = encoder_key_variables(), extraction_key_variables()
        for name, preset in cfg.PRESETS.items():
            values = dict(preset["values"])
            for group, table in (("embedding", encoder), ("extraction", extraction)):
                if group + ".provider" not in values:
                    continue
                with self.subTest(preset=name, group=group):
                    provider = values[group + ".provider"]
                    expected = values.get(group + ".api_key_env") or table.get(
                        provider, table[None])
                    self.assertEqual(expected, resolved(group, values))

    def test_both_previously_uncovered_providers_now_have_a_starting_point(self) -> None:
        chosen = set()
        for preset in cfg.PRESETS.values():
            for key in ("embedding.provider", "extraction.provider"):
                if key in preset["values"]:
                    chosen.add(preset["values"][key])
        self.assertIn("voyage", chosen)
        self.assertIn("anthropic", chosen)

    def test_no_preset_carries_a_secret(self) -> None:
        for name, preset in cfg.PRESETS.items():
            for key in preset["values"]:
                with self.subTest(preset=name, setting=key):
                    self.assertNotEqual("secret", cfg.SETTINGS_BY_KEY[key].kind)

    def test_every_preset_value_is_a_real_setting_with_a_permitted_value(self) -> None:
        for name, preset in cfg.PRESETS.items():
            for key, value in preset["values"].items():
                with self.subTest(preset=name, setting=key):
                    setting = cfg.SETTINGS_BY_KEY[key]
                    if setting.choices:
                        self.assertIn(value, setting.choices)


class TheStatusPageReportsTheSameVariableTest(_NothingInherited):
    """The portal's model-configuration panel names the variable a customer should put their key in.
    It used to work that name out for itself, flat across providers, so it could disagree with where
    the key was actually written -- and the customer would follow the wrong instruction."""

    @classmethod
    def setUpClass(cls) -> None:
        try:
            from tools import matrixark_v1_gateway as gw  # type: ignore
        except ImportError:
            import matrixark_v1_gateway as gw  # type: ignore
        cls.gw = gw
        # The gateway binds its own module object for the registry. Comparing against a separately
        # imported one compares two different modules, which is how an earlier suite passed while
        # the thing it tested was broken.
        cls.cfg = gw._gwconfig

    def _snapshot_for(self, provider_variable: str, provider: str) -> dict:
        previous = os.environ.get(provider_variable)
        os.environ[provider_variable] = provider
        try:
            return self.gw._model_config_snapshot()
        finally:
            if previous is None:
                os.environ.pop(provider_variable, None)
            else:
                os.environ[provider_variable] = previous

    def test_the_encoder_panel_names_what_the_encoder_reads(self) -> None:
        for provider, reads in encoder_key_variables().items():
            chosen = provider or "openai_compatible"
            with self.subTest(provider=chosen):
                snapshot = self._snapshot_for("MATRIXARK_EMBEDDING_PROVIDER", chosen)
                self.assertEqual(reads, snapshot["embedding"]["api_key_env"])

    def test_the_extraction_panel_names_what_extraction_reads(self) -> None:
        for provider, reads in extraction_key_variables().items():
            chosen = provider or "openai_compatible"
            with self.subTest(provider=chosen):
                snapshot = self._snapshot_for("MATRIXARK_UNDERSTANDING_PROVIDER", chosen)
                self.assertEqual(reads, snapshot["extraction"]["api_key_env"])

    def test_the_panel_and_the_registry_are_one_answer_not_two(self) -> None:
        """Both sides must be read while the SAME provider is selected. Taking the registry's answer
        after the environment is restored compares two different deployments, which is how this
        first failed against a snapshot that was already right."""
        for group, variable in (("embedding", "MATRIXARK_EMBEDDING_PROVIDER"),
                                ("extraction", "MATRIXARK_UNDERSTANDING_PROVIDER")):
            secret = self.cfg.SETTINGS_BY_KEY[group + ".api_key"]
            for choice in self.cfg.SETTINGS_BY_KEY[group + ".provider"].choices:
                with self.subTest(group=group, provider=choice):
                    previous = os.environ.get(variable)
                    os.environ[variable] = choice
                    try:
                        panel = self.gw._model_config_snapshot()[group]["api_key_env"]
                        registry = self.cfg._env_name(secret, {})
                    finally:
                        if previous is None:
                            os.environ.pop(variable, None)
                        else:
                            os.environ[variable] = previous
                    self.assertEqual(registry, panel)

    def test_the_warning_still_tells_the_customer_where_the_key_goes(self) -> None:
        """The name is only useful because a warning quotes it; if the warning stops naming a
        variable the panel is back to being a number nobody can act on."""
        snapshot = self._snapshot_for("MATRIXARK_EMBEDDING_PROVIDER", "voyage")
        named = [w for w in snapshot["warnings"] if snapshot["embedding"]["api_key_env"] in w]
        self.assertTrue(named, "no warning names the variable the encoder key goes into")


if __name__ == "__main__":
    unittest.main()
