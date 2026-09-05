#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal says when extraction and embedding are sharing one key variable between two endpoints.

Both key controls default to ``OPENAI_API_KEY``, and the key a customer types is written into
whatever that control names. Set a DeepSeek extraction key and an OpenAI embedding key without
renaming either variable and both land in ``OPENAI_API_KEY``: the last one saved wins, and the
portal reports **both** secrets as configured, because each is stored. Reproduced before this was
written::

    extraction -> OPENAI_API_KEY          extraction base_url : https://api.deepseek.com/v1
    embedding  -> OPENAI_API_KEY          embedding api_base  : https://api.openai.com/v1
    the variable holds                    'sk-deepseek-key'
    the portal reports extraction.api_key configured=True
    the portal reports embedding.api_key  configured=True
    warnings shown: nothing about it

One endpoint is then called with the other's key, 401s, and falls back silently -- deterministic
extraction, hash vectors -- which is the shape every warning in this list exists for.

**Only when the endpoints differ.** One provider serving both sides is the ``openai`` preset, where
sharing the key is the point; warning there would put a permanent complaint on a correct
configuration, and a warning that is always on is one nobody reads.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_v1_gateway as gw  # noqa: E402

KNOBS = ("MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
         "MATRIXARK_EXTRACTION_BASE_URL", "MATRIXARK_EXTRACTION_API_KEY_ENV",
         "MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_EMBEDDING_API_BASE",
         "MATRIXARK_EMBEDDING_API_KEY_ENV", "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS",
         "OPENAI_API_KEY", "DEEPSEEK_API_KEY", "MATRIXARK_EMBED_BASE_URL")

DEEPSEEK = "https://api.deepseek.com/v1"
OPENAI = "https://api.openai.com/v1"


def sharing(warnings):
    return [w for w in warnings if "both go into" in w]


class _Configured(unittest.TestCase):

    def setUp(self) -> None:
        previous = {name: os.environ.get(name) for name in KNOBS}

        def restore() -> None:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

        self.addCleanup(restore)
        for name in KNOBS:
            os.environ.pop(name, None)

    def configure(self, **env) -> list:
        for name, value in env.items():
            os.environ[name] = value
        return gw._model_config_snapshot().get("warnings") or []

    def two_providers(self, **overrides):
        env = {
            "MATRIXARK_UNDERSTANDING_PROVIDER": "openai_compatible",
            "MATRIXARK_EXTRACTION_BASE_URL": DEEPSEEK,
            "MATRIXARK_EMBEDDING_PROVIDER": "openai_compatible",
            "MATRIXARK_EMBEDDING_API_BASE": OPENAI,
            "OPENAI_API_KEY": "sk-whichever-was-last",
            "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS": "1",
        }
        env.update(overrides)
        return self.configure(**env)


class OneVariableTwoEndpointsIsCalledOutTest(_Configured):

    def test_it_is_named(self) -> None:
        found = sharing(self.two_providers())
        self.assertEqual(1, len(found), self.two_providers())

    def test_it_says_which_variable_and_which_endpoints(self) -> None:
        """It names the two CONTROLS, because that is what a reader on the Setup page can act on,
        and the variable they share, because that is what an API reader sees."""
        said = sharing(self.two_providers())[0]
        self.assertIn("Extraction API key", said)
        self.assertIn("Embedding API key", said)
        self.assertIn("OPENAI_API_KEY", said)
        self.assertIn(DEEPSEEK, said)
        self.assertIn(OPENAI, said)

    def test_it_says_what_to_do(self) -> None:
        said = sharing(self.two_providers())[0]
        self.assertIn("Extraction key variable", said)
        self.assertIn("Embedding key variable", said)
        self.assertIn("set that key again", said)


class WhenSharingIsRightItStaysQuietTest(_Configured):

    def test_one_provider_serving_both_sides_is_not_warned_about(self) -> None:
        """The `openai` preset: one endpoint, one key, correctly shared."""
        found = sharing(self.two_providers(MATRIXARK_EXTRACTION_BASE_URL=OPENAI))
        self.assertEqual([], found, found)

    def test_a_trailing_slash_is_not_a_different_endpoint(self) -> None:
        found = sharing(self.two_providers(MATRIXARK_EXTRACTION_BASE_URL=OPENAI + "/",
                                           MATRIXARK_EMBEDDING_API_BASE=OPENAI))
        self.assertEqual([], found, found)

    def test_separate_variables_are_not_warned_about(self) -> None:
        """The fix the warning asks for must silence it."""
        found = sharing(self.two_providers(MATRIXARK_EXTRACTION_API_KEY_ENV="DEEPSEEK_API_KEY"))
        self.assertEqual([], found, found)

    def test_a_deterministic_side_needs_no_key(self) -> None:
        for side in ("MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EMBEDDING_PROVIDER"):
            with self.subTest(deterministic=side):
                found = sharing(self.two_providers(**{side: "deterministic"}))
                self.assertEqual([], found, found)

    def test_an_endpoint_nobody_named_is_not_compared(self) -> None:
        """Nothing to compare is not the same as a collision, and a warning on an unconfigured
        deployment would fire on every fresh install."""
        found = sharing(self.two_providers(MATRIXARK_EXTRACTION_BASE_URL=""))
        self.assertEqual([], found, found)

    def test_a_fresh_deployment_is_not_warned_about(self) -> None:
        """The floor for all of the above: nothing configured, nothing said."""
        self.assertEqual([], sharing(self.configure()))


class ItRidesTheStripTest(_Configured):
    """The count on every page is the length of this list, so this reaches a customer who is not on
    the settings tab."""

    def test_the_count_moves(self) -> None:
        """Both arms carry a key in the variable they name, so the only difference between them is
        the sharing itself. Without that, naming an unset variable trades this warning for the
        empty-key one and the count does not move -- which reads as "the warning does nothing"."""
        gw._LIVE_SHARED = None
        self.addCleanup(setattr, gw, "_LIVE_SHARED", None)
        quiet = self.two_providers(MATRIXARK_EXTRACTION_API_KEY_ENV="DEEPSEEK_API_KEY",
                                   DEEPSEEK_API_KEY="sk-its-own")
        noisy = self.two_providers(MATRIXARK_EXTRACTION_API_KEY_ENV="OPENAI_API_KEY")
        self.assertEqual([], sharing(quiet), quiet)
        self.assertEqual(1, len(sharing(noisy)), noisy)
        self.assertEqual(len(quiet) + 1, len(noisy), (quiet, noisy))


if __name__ == "__main__":
    unittest.main()
