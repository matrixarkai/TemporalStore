#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The model picker offers what the provider serves, into the field the provider reads.

Two things were wrong for an Anthropic deployment, and both were silent.

The suggestion list was six OpenAI-compatible models. Anthropic serves none of them, and the models
it does serve were not on it.

Worse, every pick was written into ``extraction.model``. The Anthropic path reads
``MATRIXARK_ANTHROPIC_MODEL`` and ignores that field entirely, so picking a model filled in a form,
said it was set, and changed nothing: the deployment stayed on the default.

Which field a pick belongs in is now decided once, on the route, from the selected provider; the
page follows what the route names. The page half of that is exercised by
``portal/model_key_harness.js``, which pulls the resolver and the click handler out of the BUILT
page and runs them -- a rewrite of that handler left a variable dangling, and only running it found
that.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

try:
    from tools import matrixark_v1_gateway as gw  # type: ignore
except ImportError:
    import matrixark_v1_gateway as gw  # type: ignore

cfg = gw._gwconfig

PARTICIPATING = ("MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
                 "MATRIXARK_EXTRACTION_MODEL", "MATRIXARK_ANTHROPIC_MODEL",
                 "MATRIXARK_EMBEDDING_MODEL", "MATRIXARK_EMBEDDING_PROVIDER")


class Case(unittest.TestCase):
    """Everything here reads the process environment, so it controls it: a sibling suite leaving a
    provider set is the difference between passing alone and failing the full run."""

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

    def on(self, provider: str) -> None:
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = provider


class TheSuggestionsAreOnesTheProviderServesTest(Case):

    def test_an_anthropic_deployment_is_offered_anthropic_models(self) -> None:
        self.on("anthropic")
        models = [entry["model"] for entry in cfg.model_catalogue("extraction")]
        self.assertTrue(models, "an Anthropic deployment was offered nothing at all")
        self.assertTrue(all(name.startswith("claude-") for name in models), models)

    def test_it_is_not_offered_models_anthropic_does_not_have(self) -> None:
        self.on("anthropic")
        models = [entry["model"] for entry in cfg.model_catalogue("extraction")]
        for absent in ("gpt-4o", "gpt-4o-mini", "deepseek-chat", "qwen2.5:7b"):
            self.assertNotIn(absent, models)

    def test_an_openai_compatible_deployment_keeps_the_list_it_had(self) -> None:
        self.on("openai_compatible")
        models = [entry["model"] for entry in cfg.model_catalogue("extraction")]
        for present in ("gpt-4o-mini", "deepseek-chat", "qwen2.5:7b"):
            self.assertIn(present, models)
        self.assertFalse([m for m in models if m.startswith("claude-")], models)

    def test_a_provider_that_calls_nothing_is_shown_everything(self) -> None:
        """There is nothing wrong to narrow towards yet, and an empty picker beside a provider
        dropdown reads as a broken page rather than a choice still to be made."""
        self.on("deterministic")
        self.assertEqual(len(cfg.EXTRACTION_CATALOGUE),
                         len(cfg.model_catalogue("extraction")))

    def test_every_entry_says_which_dispatch_it_belongs_to(self) -> None:
        """An entry with no `serves` would vanish from every filtered list without anyone noticing,
        because the filter drops what it cannot classify."""
        effects = set()
        for entry in cfg.EXTRACTION_CATALOGUE:
            with self.subTest(model=entry.get("model")):
                self.assertIn(entry.get("serves"), ("openai", "anthropic"))
            effects.add(entry["serves"])
        self.assertEqual({"openai", "anthropic"}, effects, "one dispatch has no suggestions")

    def test_the_default_model_is_among_the_suggestions(self) -> None:
        """The registry's own default for the Anthropic model has to be pickable, or the list
        disagrees with the field beside it."""
        self.on("anthropic")
        models = [entry["model"] for entry in cfg.model_catalogue("extraction")]
        self.assertIn(cfg.extraction_model_default("anthropic"), models)

    def test_embeddings_are_still_refused_here(self) -> None:
        """Encoders come from the measured catalogue in the gateway. A second hand-written list is
        exactly what this function exists to refuse."""
        with self.assertRaises(ValueError):
            cfg.model_catalogue("embedding")


class ThePickGoesIntoTheFieldTheProviderReadsTest(Case):

    def variable(self, provider: str) -> str:
        return cfg._env_name(cfg.SETTINGS_BY_KEY["extraction.model"],
                             {"extraction.provider": provider})

    def test_anthropic_writes_its_own_variable(self) -> None:
        """There is one model field now, so there is no key to choose between -- the field itself
        lands in the variable the provider reads."""
        self.assertEqual("MATRIXARK_ANTHROPIC_MODEL", self.variable("anthropic"))

    def test_everything_else_writes_the_general_one(self) -> None:
        for provider in ("openai_compatible", "deterministic", "", "typo_provider"):
            with self.subTest(provider=provider):
                self.assertEqual("MATRIXARK_EXTRACTION_MODEL", self.variable(provider))

    def test_the_key_it_names_is_on_the_form(self) -> None:
        """A key that is not in the registry would reach the page and query a field that is not on
        the form, which the page reports as 'not loaded' -- a dead end rather than an error."""
        for provider in ("anthropic", "openai_compatible"):
            with self.subTest(provider=provider):
                self.on(provider)
                self.assertIn(gw._model_picker_body("extraction")["key"], cfg.SETTINGS_BY_KEY)


class TheRouteAnswersWithBothTest(Case):
    """The page is told the field and shown the narrowed list, so it decides neither itself."""

    def body(self, target: str = "extraction") -> dict:
        """The route's own function, not a copy of it. Rebuilding the body here passed every
        assertion below while the route reported the wrong field -- a mutation proved it."""
        return gw._model_picker_body(target)

    def test_current_is_read_from_the_variable_that_is_in_use(self) -> None:
        self.on("anthropic")
        os.environ["MATRIXARK_ANTHROPIC_MODEL"] = "claude-opus-5"
        os.environ["MATRIXARK_EXTRACTION_MODEL"] = "gpt-4o"
        body = self.body()
        self.assertEqual("extraction.model", body["key"])
        self.assertEqual("claude-opus-5", body["current"],
                         "the panel showed the variable this provider does not read")

    def test_the_other_field_is_not_what_is_reported(self) -> None:
        self.on("openai_compatible")
        os.environ["MATRIXARK_ANTHROPIC_MODEL"] = "claude-opus-5"
        os.environ["MATRIXARK_EXTRACTION_MODEL"] = "gpt-4o"
        self.assertEqual("gpt-4o", self.body()["current"])

    def test_the_catalogue_it_carries_is_the_narrowed_one(self) -> None:
        self.on("anthropic")
        self.assertTrue(all(entry["model"].startswith("claude-")
                            for entry in self.body()["catalogue"]))

    def test_the_embedding_side_is_untouched(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = "voyage-3"
        body = self.body("embedding")
        self.assertEqual("embedding.model", body["key"])
        self.assertEqual("voyage-3", body["current"])


class ThePageFollowsTheFieldTheRouteNamesTest(unittest.TestCase):
    """Run the picker's own code out of the built page. Reading it is not enough -- the rewrite this
    covers left a variable dangling, which is a ReferenceError only at the moment of the click."""

    def test_the_harness_passes(self) -> None:
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is not available")
        result = subprocess.run(
            [node, os.path.join(PORTAL, "model_key_harness.js"),
             os.path.join(PORTAL, "setup_portal.html")],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("all ok", result.stdout)

    def test_the_built_page_is_what_the_generator_produces(self) -> None:
        """The page is generated. A change made only to the generator ships nothing, and a change
        made only to the page is reverted by the next build."""
        built = os.path.join(PORTAL, "setup_portal.html")
        with open(built, encoding="utf-8") as handle:
            page = handle.read()
        self.assertIn("function modelKey(target) {", page)
        self.assertIn("var key = modelKey(target);", page)


class TheDiscoveryAsksTheEndpointInUseTest(Case):
    """The panel's other half. "Ask the endpoint what it serves" exists to stop a misspelt model
    name reaching ingest, where it fails hours later as a silent fall back -- so pointing it at the
    endpoint the OTHER provider uses removes exactly the check it is there to provide."""

    def setUp(self) -> None:
        super().setUp()
        self._get_json = cfg._get_json
        self._load = cfg.load
        cfg.load = lambda: {"values": {}}
        self.asked = []

        def recorder(url, headers, timeout):
            self.asked.append({"url": url, "headers": dict(headers or {})})
            return 200, {"data": [{"id": "claude-sonnet-5"}]}

        cfg._get_json = recorder
        for name in ("MATRIXARK_ANTHROPIC_API_BASE", "MATRIXARK_EXTRACTION_BASE_URL",
                     "MATRIXARK_ANTHROPIC_VERSION", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
                     "MATRIXARK_EXTRACTION_API_KEY_ENV"):
            self._saved.setdefault(name, os.environ.get(name))
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        cfg._get_json = self._get_json
        cfg.load = self._load
        super().tearDown()

    def key(self, value: str) -> None:
        os.environ[cfg._env_name(cfg.SETTINGS_BY_KEY["extraction.api_key"], {})] = value

    def test_anthropic_is_asked_at_its_own_base(self) -> None:
        self.on("anthropic")
        self.key("k")
        result = cfg.discover_models("extraction")
        self.assertEqual(1, len(self.asked))
        self.assertEqual("https://api.anthropic.com/v1/models", self.asked[0]["url"])
        self.assertTrue(result["available"])
        self.assertIn("claude-sonnet-5", result["models"])

    def test_it_authenticates_the_way_anthropic_does(self) -> None:
        self.on("anthropic")
        self.key("k")
        cfg.discover_models("extraction")
        headers = self.asked[0]["headers"]
        self.assertEqual("k", headers.get("x-api-key"))
        self.assertIn("anthropic-version", headers)
        self.assertNotIn("Authorization", headers)

    def test_a_configured_anthropic_base_is_respected(self) -> None:
        self.on("anthropic")
        os.environ["MATRIXARK_ANTHROPIC_API_BASE"] = "https://anthropic.example/"
        self.key("k")
        cfg.discover_models("extraction")
        self.assertEqual("https://anthropic.example/v1/models", self.asked[0]["url"])

    def test_it_no_longer_asks_for_a_field_this_provider_does_not_read(self) -> None:
        """Shipped behaviour: no OpenAI base URL, so it answered "Set the base URL first"."""
        self.on("anthropic")
        self.key("k")
        self.assertNotEqual("no_base_url", cfg.discover_models("extraction").get("reason"))

    def test_an_openai_compatible_endpoint_is_asked_exactly_as_before(self) -> None:
        """The floor: if the branch swallowed everything, the assertions above would still pass."""
        self.on("openai_compatible")
        os.environ["MATRIXARK_EXTRACTION_BASE_URL"] = "https://api.example/v1"
        self.key("k")
        cfg.discover_models("extraction")
        self.assertEqual("https://api.example/v1/models", self.asked[0]["url"])
        self.assertEqual("Bearer k", self.asked[0]["headers"]["Authorization"])

    def test_the_embedding_side_is_untouched(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_API_BASE"] = "https://encoder.example/v1"
        os.environ[cfg._env_name(cfg.SETTINGS_BY_KEY["embedding.api_key"], {})] = "k"
        cfg.discover_models("embedding")
        self.assertEqual("https://encoder.example/v1/models", self.asked[0]["url"])
        self.assertEqual("Bearer k", self.asked[0]["headers"]["Authorization"])

    def test_no_key_still_asks_without_authenticating(self) -> None:
        """A self-hosted endpoint that needs no auth has to stay askable."""
        self.on("openai_compatible")
        os.environ["MATRIXARK_EXTRACTION_BASE_URL"] = "http://127.0.0.1:11434/v1"
        cfg.discover_models("extraction")
        self.assertEqual({}, self.asked[0]["headers"])


if __name__ == "__main__":
    unittest.main()
