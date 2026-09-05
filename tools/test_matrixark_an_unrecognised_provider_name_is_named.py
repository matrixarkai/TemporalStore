#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A provider name nothing recognises is named, not treated as configured.

Three places asked "is this provider deterministic?" and each answered with the same hand-written
literal set, ``{"", "deterministic", "rules", "local"}``. That set catches the names a customer
chooses on purpose. It does not catch a name nothing matches -- ``openai_compatibl`` with one
character missing, or ``cohere``, which simply is not supported. Both fall through to the same local
path, and all three places reported the deployment as configured:

* the model panel raised no warning at all;
* the setup checklist printed a tick and *"Retrieval encodes with openai_compatibl."*

The classification now lives once, beside the sets it reads, and the tests derive those sets from
the provider modules. The distinction it draws is the point: ``deterministic`` is a choice and keeps
its own explanation; an unrecognised value is a mistake, and the message names the value.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

try:
    from tools import matrixark_v1_gateway as gw  # type: ignore
except ImportError:
    import matrixark_v1_gateway as gw  # type: ignore

# The gateway binds its own module object for the registry; a separate import is a different module.
cfg = gw._gwconfig

ENCODER = "matrixark_mcp_embeddings.py"
CORE = "matrixark_mcp_core.py"
PARTICIPATING = ("MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_UNDERSTANDING_PROVIDER",
                 "MATRIXARK_EXTRACTION_PROVIDER", "MATRIXARK_EMBEDDING_API_BASE",
                 "MATRIXARK_EMBED_BASE_URL", "MATRIXARK_EXTRACTION_BASE_URL",
                 "MATRIXARK_EXTRACTION_MODEL", "MATRIXARK_EMBEDDING_MODEL",
                 "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS")


def parse(filename: str) -> ast.Module:
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        return ast.parse(handle.read(), filename=filename)


def assigned_string_set(filename: str, name: str) -> set:
    for node in parse(filename).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if not isinstance(node.value, (ast.Set, ast.List, ast.Tuple)):
                        continue
                    return {e.value for e in node.value.elts
                            if isinstance(e, ast.Constant) and isinstance(e.value, str)}
    raise AssertionError("%s not found in %s" % (name, filename))


def provider_membership_sets(filename: str) -> list:
    found = []
    for node in ast.walk(parse(filename)):
        if not (isinstance(node, ast.Compare) and len(node.ops) == 1
                and isinstance(node.ops[0], ast.In)):
            continue
        left, right = node.left, node.comparators[0]
        if not (isinstance(left, ast.Name) and left.id == "provider"):
            continue
        if isinstance(right, ast.Name):
            # A dispatch may NAME its set rather than spell it out, and one now does: the encoder
            # hoisted the four spellings that select the in-process model into a constant, because
            # writing them at each branch was four places for a new spelling to reach one and miss
            # the others. Refusing to follow the name would make this check pass only for modules
            # that repeat themselves, and go quiet on the ones that stopped -- which is the wrong
            # way round, since what it is really asking is what the code DISPATCHES on.
            try:
                named = assigned_string_set(filename, right.id)
            except AssertionError:
                continue
            if named:
                found.append(named)
            continue
        if isinstance(right, (ast.Set, ast.List, ast.Tuple)):
            members = {e.value for e in right.elts
                       if isinstance(e, ast.Constant) and isinstance(e.value, str)}
            if members:
                found.append(members)
    return found


class Case(unittest.TestCase):
    """Every one of these reads the process environment, so it must control it -- a sibling suite
    leaving a provider set is the difference between passing alone and failing the full run."""

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

    def snapshot(self, **environment) -> dict:
        for name, value in environment.items():
            os.environ[name] = value
        return gw._model_config_snapshot()

    def check(self, snapshot: dict, check_id: str) -> dict:
        for row in gw._readiness_checks(snapshot, {}, None):
            if row["id"] == check_id:
                return row
        raise AssertionError("no readiness check %r" % check_id)

    def named(self, snapshot: dict) -> list:
        return [w for w in snapshot["warnings"] if "nothing recognises" in w]


class TheSetsAreTheProviderCodesTest(unittest.TestCase):
    """These decide whether a deployment is reported as working. Restating them by hand is how the
    three copies drifted; if the parse stops matching, the classification is guessing again."""

    def test_the_api_set_is_the_encoders(self) -> None:
        self.assertEqual(assigned_string_set(ENCODER, "_API_EMBEDDING_PROVIDERS"),
                         cfg._API_EMBEDDING_PROVIDERS)

    def test_the_in_process_set_is_a_dispatch_set_in_the_encoder(self) -> None:
        self.assertIn(cfg._OSS_EMBEDDING_PROVIDERS, provider_membership_sets(ENCODER))

    def test_both_extraction_sets_are_dispatch_sets_in_the_core(self) -> None:
        sets = provider_membership_sets(CORE)
        self.assertIn(cfg._OPENAI_EXTRACTION_PROVIDERS, sets)
        self.assertIn(cfg._ANTHROPIC_EXTRACTION_PROVIDERS, sets)

    def test_the_deliberate_set_is_the_cores(self) -> None:
        """"deterministic", "rules" and "local" are what a customer asks for. The core treats them
        as one set, and so must this, or a deliberate choice gets reported as a mistake."""
        self.assertTrue(any(members <= cfg._DELIBERATE_FALLBACK_PROVIDERS
                            and {"deterministic", "rules", "local"} <= members
                            for members in provider_membership_sets(CORE)))


class TheClassifierMatchesWhatTheCodeDoesTest(unittest.TestCase):

    def test_every_api_provider_is_called_api(self) -> None:
        for provider in assigned_string_set(ENCODER, "_API_EMBEDDING_PROVIDERS"):
            with self.subTest(provider=provider):
                self.assertEqual("api", cfg.embedding_provider_effect(provider))

    def test_an_in_process_encoder_is_not_called_a_hash(self) -> None:
        """It makes no HTTP call, but it produces real embeddings. Reporting it as the hash
        fallback would be a new wrong answer in the other direction."""
        for provider in cfg._OSS_EMBEDDING_PROVIDERS:
            with self.subTest(provider=provider):
                self.assertEqual("local_model", cfg.embedding_provider_effect(provider))

    def test_an_unrecognised_name_is_the_hash_fallback(self) -> None:
        for provider in ("openai_compatibl", "cohere", "OpenAI-Compatible-Typo"):
            with self.subTest(provider=provider):
                self.assertEqual("hash", cfg.embedding_provider_effect(provider))

    def test_extraction_names_map_to_their_dispatch(self) -> None:
        for provider in cfg._OPENAI_EXTRACTION_PROVIDERS:
            self.assertEqual("openai", cfg.extraction_provider_effect(provider))
        for provider in cfg._ANTHROPIC_EXTRACTION_PROVIDERS:
            self.assertEqual("anthropic", cfg.extraction_provider_effect(provider))
        for provider in ("anthropi", "gemini", ""):
            self.assertEqual("rules", cfg.extraction_provider_effect(provider))

    def test_case_and_padding_do_not_change_the_answer(self) -> None:
        """The provider code lowercases and strips before dispatching, so a value that works there
        must not be reported as unrecognised here."""
        self.assertEqual("api", cfg.embedding_provider_effect("  Voyage  "))
        self.assertEqual("anthropic", cfg.extraction_provider_effect(" ANTHROPIC "))

    def test_a_deliberate_choice_is_not_a_mistake(self) -> None:
        for group in ("embedding", "extraction"):
            for provider in ("", "deterministic", "rules", "local"):
                with self.subTest(group=group, provider=provider):
                    self.assertFalse(cfg.provider_is_unrecognised(group, provider))


class TheMisspeltProviderIsReportedTest(Case):

    def test_the_warning_names_the_value(self) -> None:
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="openai_compatibl",
                                 MATRIXARK_EMBEDDING_API_BASE="https://encoder.example/v1")
        named = self.named(snapshot)
        self.assertEqual(1, len(named), snapshot["warnings"])
        self.assertIn("openai_compatibl", named[0])

    def test_the_warning_says_what_would_have_worked(self) -> None:
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="cohere")
        self.assertIn("voyage", self.named(snapshot)[0])

    def test_the_checklist_row_is_not_green(self) -> None:
        """The sharpest form of the bug: a tick, with the misspelt name printed beside it."""
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="openai_compatibl",
                                 MATRIXARK_EMBEDDING_API_BASE="https://encoder.example/v1")
        self.assertNotEqual("ok", self.check(snapshot, "embedding")["status"])

    def test_the_extraction_checklist_row_is_not_green_either(self) -> None:
        snapshot = self.snapshot(MATRIXARK_UNDERSTANDING_PROVIDER="openai_compatibl",
                                 MATRIXARK_EXTRACTION_BASE_URL="https://api.example/v1",
                                 MATRIXARK_EXTRACTION_MODEL="gpt-4o-mini")
        self.assertNotEqual("ok", self.check(snapshot, "extraction")["status"])

    def test_each_side_is_reported_on_its_own(self) -> None:
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="cohere",
                                 MATRIXARK_UNDERSTANDING_PROVIDER="gemini")
        named = self.named(snapshot)
        self.assertEqual(2, len(named))
        self.assertTrue(any("cohere" in w for w in named))
        self.assertTrue(any("gemini" in w for w in named))


class ADeliberateChoiceKeepsItsOwnExplanationTest(Case):

    def test_deterministic_is_explained_not_reported_as_a_mistake(self) -> None:
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="deterministic")
        self.assertEqual([], self.named(snapshot))
        self.assertTrue([w for w in snapshot["warnings"] if "hash vectors, not semantic" in w])

    def test_local_is_still_explained_the_way_it_was(self) -> None:
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="local")
        self.assertEqual([], self.named(snapshot))
        self.assertTrue([w for w in snapshot["warnings"] if "hash vectors, not semantic" in w])

    def test_a_misspelt_name_is_not_told_both_things(self) -> None:
        """Two warnings for one setting, one of them saying it is deterministic when the customer
        did not choose that, reads as two problems."""
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="cohere")
        self.assertEqual([], [w for w in snapshot["warnings"]
                              if "hash vectors, not semantic" in w])


class AWorkingDeploymentIsStillReportedWorkingTest(Case):
    """The floor. Every assertion above would pass on a build that called everything unrecognised."""

    def test_an_api_encoder_raises_nothing_new(self) -> None:
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="voyage",
                                 MATRIXARK_EMBEDDING_API_BASE="https://api.voyageai.example/v1",
                                 MATRIXARK_REQUIRE_MODEL_EMBEDDINGS="1")
        self.assertEqual([], self.named(snapshot))
        self.assertEqual("ok", self.check(snapshot, "embedding")["status"])

    def test_an_in_process_encoder_is_treated_as_configured(self) -> None:
        snapshot = self.snapshot(MATRIXARK_EMBEDDING_PROVIDER="oss",
                                 MATRIXARK_REQUIRE_MODEL_EMBEDDINGS="1")
        self.assertEqual([], self.named(snapshot))
        self.assertEqual("ok", self.check(snapshot, "embedding")["status"])

    def test_an_openai_compatible_extraction_is_still_green(self) -> None:
        snapshot = self.snapshot(MATRIXARK_UNDERSTANDING_PROVIDER="openai_compatible",
                                 MATRIXARK_EXTRACTION_BASE_URL="https://api.example/v1",
                                 MATRIXARK_EXTRACTION_MODEL="gpt-4o-mini")
        self.assertEqual([], self.named(snapshot))
        self.assertEqual("ok", self.check(snapshot, "extraction")["status"])

    def test_anthropic_counts_as_a_configured_extraction_provider(self) -> None:
        snapshot = self.snapshot(MATRIXARK_UNDERSTANDING_PROVIDER="anthropic")
        self.assertEqual([], self.named(snapshot))
        self.assertEqual("ok", self.check(snapshot, "extraction")["status"])


if __name__ == "__main__":
    unittest.main()
