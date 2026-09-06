#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""No surface in the gateway classifies a provider by hand.

"Is this provider deterministic?" was answered separately in four places, each with its own literal.
Three were consolidated when a misspelt provider name turned out to be reported as configured. The
fourth survived, because it is written as a **tuple** rather than a set and the sweep that found the
others looked for ``deterministic = {...}``:

    deterministic = provider.lower() in ("", "deterministic")

It is the incomplete kind, and it feeds the encoder summary served beside the backlog counts. On
``local`` -- a synonym for the hash fallback -- and on any misspelt provider name, it reported
``semantic: true`` and an empty note, so a deployment making nothing but hash vectors read as a
working semantic encoder with nothing to say about it.

The guard below is the point of this change: it fails on ANY collection literal in the gateway that
classifies a provider by name, so a fifth copy cannot be added quietly. Finding these one at a time
is how the fourth survived three rounds of fixing the same defect.
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

cfg = gw._gwconfig

# Names that only appear in a collection because something is deciding what a provider IS.
PROVIDER_NAMES = {"deterministic", "openai_compatible", "openai_compatible_llm", "anthropic",
                  "voyage", "oss", "open_source", "sentence_transformers", "claude"}
# The classifier's own sets, which are the one place this may be written. The last two belong to
# the SUMMARY path: they were written as named sets on purpose, so the mirror test can compare them
# against the writer's own literals rather than restate them, which is the shape this guard asks
# for. Adding a name here is only correct for a set the classifier owns -- a literal written inline
# at a decision site is the thing being forbidden, and it does not become allowed by being named.
CLASSIFIER_SETS = {"_OPENAI_EXTRACTION_PROVIDERS", "_ANTHROPIC_EXTRACTION_PROVIDERS",
                   "_API_EMBEDDING_PROVIDERS", "_OSS_EMBEDDING_PROVIDERS",
                   "_DELIBERATE_FALLBACK_PROVIDERS",
                   "_SUMMARY_OSS_ALIASES", "_SUMMARY_MODEL_PROVIDERS"}
PARTICIPATING = ("MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_EMBEDDING_MODEL",
                 "MATRIXARK_EMBED_DRAINER")


def classifying_literals(filename: str, allow_assigned_to=frozenset()) -> list:
    """Collection literals holding provider names, other than the ones allowed by name.

    A `choices=[...]` on a Setting is not a classification -- it is the list a customer picks from --
    so a literal that is an argument to Setting() is skipped.
    """
    path = os.path.join(TOOLS, filename)
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=filename)

    allowed_nodes = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in allow_assigned_to:
                    allowed_nodes.add(id(node.value))
        # The offered choices, not a classification of what a provider does.
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id == "Setting":
            for argument in list(node.args) + [k.value for k in node.keywords]:
                allowed_nodes.add(id(argument))

    found = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Set, ast.Tuple, ast.List)) or id(node) in allowed_nodes:
            continue
        members = {e.value for e in node.elts
                   if isinstance(e, ast.Constant) and isinstance(e.value, str)}
        if members & PROVIDER_NAMES:
            found.append((node.lineno, sorted(members)))
    return found


class Case(unittest.TestCase):

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

    def summary(self, provider: str) -> dict:
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = provider
        return gw._encoder_summary()


class NoSurfaceClassifiesAProviderByHandTest(unittest.TestCase):
    """The guard. Four copies of one question were found one at a time, over three changes; this
    fails on the fifth before it ships."""

    def test_the_gateway_carries_none(self) -> None:
        self.assertEqual([], classifying_literals("matrixark_v1_gateway.py"))

    def test_the_registry_carries_only_the_classifier_and_its_choices(self) -> None:
        self.assertEqual([], classifying_literals("matrixark_gateway_config.py",
                                                  allow_assigned_to=CLASSIFIER_SETS))

    def test_every_allow_listed_name_is_a_real_set_in_the_registry(self) -> None:
        """An allow-list nobody checks is a place to put things. A name that stops existing --
        renamed, deleted, or mistyped -- silently exempts nothing and hides nothing, and the next
        person adds one more rather than asking why it is there."""
        import ast as _ast
        with open(os.path.join(TOOLS, "matrixark_gateway_config.py"), encoding="utf-8") as handle:
            tree = _ast.parse(handle.read())
        defined = {target.id for node in _ast.walk(tree) if isinstance(node, _ast.Assign)
                   for target in node.targets if isinstance(target, _ast.Name)}
        self.assertEqual(set(), CLASSIFIER_SETS - defined,
                         "allow-listed but not defined in the registry")

    def test_the_detector_would_notice_one(self) -> None:
        """The floor. If this stopped recognising a classification, both assertions above would pass
        on a file full of them."""
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "sample.py")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write('x = provider in ("", "deterministic")\n')
            saved, sys.modules_dir = TOOLS, None
            try:
                globals()["TOOLS"] = directory
                self.assertEqual(1, len(classifying_literals("sample.py")))
            finally:
                globals()["TOOLS"] = saved

    def test_the_classifier_sets_are_still_there_to_be_allowed(self) -> None:
        """If they were renamed, the allow-list would silently stop matching and the registry test
        would fail for the wrong reason -- or worse, pass because they moved out of reach."""
        for name in CLASSIFIER_SETS:
            with self.subTest(constant=name):
                self.assertTrue(getattr(cfg, name), name + " is missing or empty")


class TheEncoderSummaryReportsWhatIsHappeningTest(Case):

    def test_a_working_encoder_is_semantic(self) -> None:
        for provider in ("openai_compatible", "voyage", "oss"):
            with self.subTest(provider=provider):
                summary = self.summary(provider)
                self.assertTrue(summary["semantic"])
                self.assertEqual("", summary["note"])

    def test_the_hash_fallback_is_not_semantic(self) -> None:
        """`local` is the case the old pair missed: a synonym for the fallback, reported as a
        working encoder."""
        for provider in ("deterministic", "local", ""):
            with self.subTest(provider=provider):
                summary = self.summary(provider)
                self.assertFalse(summary["semantic"])
                self.assertIn("hash fallback", summary["note"])

    def test_a_name_nothing_recognises_is_named(self) -> None:
        summary = self.summary("openai_compatibl")
        self.assertFalse(summary["semantic"])
        self.assertIn("openai_compatibl", summary["note"])
        self.assertIn("nothing recognises", summary["note"])

    def test_a_deliberate_choice_is_not_called_a_mistake(self) -> None:
        self.assertNotIn("nothing recognises", self.summary("local")["note"])

    def test_it_agrees_with_the_classifier_for_every_offered_choice(self) -> None:
        """One answer, so the backlog panel and the warnings cannot disagree about the same
        deployment."""
        for choice in cfg.SETTINGS_BY_KEY["embedding.provider"].choices:
            with self.subTest(provider=choice):
                self.assertEqual(cfg.embedding_provider_effect(choice) != "hash",
                                 self.summary(choice)["semantic"])

    def test_the_rest_of_the_summary_is_unchanged(self) -> None:
        """The floor: everything beside `semantic` and `note` still reports what it did."""
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = "voyage-3"
        os.environ["MATRIXARK_EMBED_DRAINER"] = "1"
        summary = self.summary("voyage")
        self.assertEqual("voyage", summary["provider"])
        self.assertEqual("voyage-3", summary["model"])
        self.assertTrue(summary["drainer_enabled"])


if __name__ == "__main__":
    unittest.main()
