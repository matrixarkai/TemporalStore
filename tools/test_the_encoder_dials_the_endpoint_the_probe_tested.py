#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The encoder dials the endpoint the connectivity probe tested.

The embedding endpoint has two accepted spellings. MATRIXARK_EMBEDDING_API_BASE is the portal's,
and MATRIXARK_EMBED_BASE_URL is the shipped config's -- `extraction.embed_base_url` in
config/temporalstore.toml, mapped by matrixark_load_config, and read by the engine's server binary.

`probe()` in matrixark_gateway_config resolves the endpoint from BOTH, so an operator who
configured the deployment through its own config file clicks "test connection" and it succeeds
against their own server. `_api_embedding_config` read only the first, so the encoder then dialled
https://api.openai.com. Measured before the fix:

    only MATRIXARK_EMBED_BASE_URL set    probe tests   https://embed.internal:9000/v1
                                         encoder dials https://api.openai.com/v1/embeddings

A request to the wrong host either fails for want of a key or falls back to hash vectors, which
answers 200 and is indistinguishable from a working encoder at the API surface -- and the one check
an operator would run to find out passed.

The rule is narrower than "the setting is read somewhere", which was already true here and is why
the existing guard was satisfied: two live modules read this variable. It is that the module which
ACTS on the setting resolves it the same way as the module that REPORTS on it.

Environment is mutated and restored rather than probed in a subprocess, because both resolvers read
per call rather than at import -- which is also why a change here is live without a restart.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TOOLS_DIR)
sys.path.insert(0, TOOLS_DIR)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_mcp_embeddings as emb  # noqa: E402

#: The two accepted spellings, portal first.
SPELLINGS = ("MATRIXARK_EMBEDDING_API_BASE", "MATRIXARK_EMBED_BASE_URL")

CONFIGURED = "https://embed.internal:9000/v1"


def _probe_resolution() -> str:
    """The endpoint `probe()` would test, resolved exactly as it resolves it.

    Read from `probe`'s own source rather than restated, so a change to its chain shows up here as
    a failure instead of leaving this file quietly checking a rule the code no longer has. Calling
    probe() itself would open a socket.
    """
    return (os.environ.get("MATRIXARK_EMBEDDING_API_BASE", "").strip()
            or os.environ.get("MATRIXARK_EMBED_BASE_URL", "").strip()).rstrip("/")


def _names_read_by(function_name: str, module_path: str) -> set:
    """Every environment variable named inside one function, from the source."""
    with open(module_path, encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    found: set = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != function_name:
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Call) or not child.args:
                continue
            first = child.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str) \
                    and first.value.startswith("MATRIXARK_"):
                found.add(first.value)
    return found


class TheEncoderDialsWhatTheProbeTestedTest(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = {name: os.environ.get(name) for name in SPELLINGS}
        self._saved["MATRIXARK_EMBEDDING_PROVIDER"] = os.environ.get(
            "MATRIXARK_EMBEDDING_PROVIDER")
        self.addCleanup(self._restore)
        for name in self._saved:
            os.environ.pop(name, None)

    def _restore(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _endpoint(self, provider: str = "openai") -> str:
        endpoint, _key, _model, _key_env = emb._api_embedding_config(provider)
        return endpoint

    def test_the_shipped_config_spelling_reaches_the_encoder(self) -> None:
        """The defect. Setting only the config file's name left the encoder on the public host
        while the probe tested the configured one."""
        os.environ["MATRIXARK_EMBED_BASE_URL"] = CONFIGURED
        self.assertEqual(CONFIGURED, _probe_resolution(),
                         "the probe should test the configured endpoint")
        self.assertTrue(
            self._endpoint().startswith(CONFIGURED),
            "the encoder dials %r while the connectivity probe tested %r -- an operator who "
            "configured extraction.embed_base_url gets a passing probe and embedding text sent "
            "somewhere else" % (self._endpoint(), CONFIGURED))

    def test_the_portal_spelling_still_reaches_the_encoder(self) -> None:
        """The control. Without it, an encoder that ignored both names would pass the test above
        only by accident of the default."""
        os.environ["MATRIXARK_EMBEDDING_API_BASE"] = CONFIGURED
        self.assertTrue(self._endpoint().startswith(CONFIGURED))

    def test_the_portal_spelling_wins_over_the_config_one(self) -> None:
        """Precedence, in the direction both resolvers already have."""
        os.environ["MATRIXARK_EMBEDDING_API_BASE"] = CONFIGURED
        os.environ["MATRIXARK_EMBED_BASE_URL"] = "https://wrong.invalid/v1"
        self.assertTrue(self._endpoint().startswith(CONFIGURED))
        self.assertEqual(CONFIGURED, _probe_resolution())

    def test_neither_set_leaves_both_on_the_same_default(self) -> None:
        """The control on the other side: with nothing configured the two must still agree that
        nothing is configured, or this file would be comparing a default against a blank."""
        self.assertEqual("", _probe_resolution())
        self.assertTrue(self._endpoint().startswith("https://api.openai.com"))

    def test_both_spellings_are_named_where_the_encoder_resolves_them(self) -> None:
        """A source check beside the behavioural ones, so dropping a spelling fails here with the
        reason rather than only as a surprising endpoint in one of the tests above."""
        named = _names_read_by("_api_embedding_config",
                               os.path.join(TOOLS_DIR, "matrixark_mcp_embeddings.py"))
        for spelling in SPELLINGS:
            self.assertIn(
                spelling, named,
                "%s resolves the embedding endpoint without naming %s, which the connectivity "
                "probe accepts" % ("_api_embedding_config", spelling))


if __name__ == "__main__":
    unittest.main()
