#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The encoder call has the timeout control the extraction call already had.

The portal configures two model calls and offered a timeout for one. ``extraction.timeout_sec``
covers the extraction HTTP call; the encoder call reads ``MATRIXARK_EMBEDDING_API_TIMEOUT_S``, also
defaulting to 30, and had no control -- so a customer running a slow or local encoder could lengthen
one and not the other.

``live`` here is derived, not assumed. The variable is read **inside** the request function rather
than captured into a module constant, so the next call takes the new value; the extraction timeout
is captured at import, which is why that one is ``restart`` and this one is not. That difference is
asserted by parsing both modules, so a change to either binding fails this rather than leaving the
portal promising the wrong thing.
"""
from __future__ import annotations

import ast
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

SETTING = "embedding.timeout_sec"
VARIABLE = "MATRIXARK_EMBEDDING_API_TIMEOUT_S"
ENCODER = "matrixark_mcp_embeddings.py"


def module_scope_reads(filename: str) -> set:
    """Variables the module captures in a top-level assignment: bound once, at import."""
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=filename)
    found = set()
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Call) or not sub.args:
                continue
            target = sub.func
            reads = ((isinstance(target, ast.Attribute) and target.attr in {"get", "getenv"})
                     or (isinstance(target, ast.Name) and target.id == "getenv"))
            first = sub.args[0]
            if reads and isinstance(first, ast.Constant) and isinstance(first.value, str):
                found.add(first.value)
    return found


def source_of(filename: str) -> str:
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        return handle.read()


class TheControlExistsTest(unittest.TestCase):

    def test_it_is_offered(self) -> None:
        self.assertIn(SETTING, cfg.SETTINGS_BY_KEY)

    def test_it_sits_with_the_encoder_it_times(self) -> None:
        self.assertEqual("embedding", cfg.SETTINGS_BY_KEY[SETTING].group)

    def test_it_names_the_variable_the_encoder_reads(self) -> None:
        """A control pointed at a variable nothing reads is decorative."""
        self.assertEqual(VARIABLE, cfg._env_name(cfg.SETTINGS_BY_KEY[SETTING], {}))
        self.assertIn(VARIABLE, source_of(ENCODER))


class TheDeclaredDefaultIsTheRealOneTest(unittest.TestCase):
    """The portal shows a setting's default when nothing is stored, so a wrong one misdescribes the
    deployment."""

    def test_it_matches_what_the_encoder_falls_back_to(self) -> None:
        found = re.search(r'MATRIXARK_EMBEDDING_API_TIMEOUT_S"\s*,\s*"([0-9.]+)"',
                          source_of(ENCODER))
        self.assertIsNotNone(found, "the encoder no longer defaults this inline")
        self.assertEqual(float(found.group(1)),
                         float(cfg.SETTINGS_BY_KEY[SETTING].default))


class TheLiveClaimIsEarnedTest(unittest.TestCase):
    """`live` is a promise the portal makes on save. Here it is derived from where the read is."""

    def test_the_encoder_reads_it_per_call(self) -> None:
        self.assertNotIn(VARIABLE, module_scope_reads(ENCODER),
                         "the encoder now captures the timeout at import, so the control cannot "
                         "honestly say live")

    def test_the_control_says_live(self) -> None:
        self.assertEqual("live", cfg.SETTINGS_BY_KEY[SETTING].applies)

    def test_the_extraction_timeout_is_the_contrast_and_still_restart(self) -> None:
        """The sibling is captured at import, which is why it is restart. If that ever changes, the
        two should be reconsidered together rather than drifting apart."""
        self.assertIn("MATRIXARK_EXTRACTION_TIMEOUT_SEC", module_scope_reads("matrixark_mcp_core.py"))
        self.assertEqual("restart", cfg.SETTINGS_BY_KEY["extraction.timeout_sec"].applies)


class TheTwoCallsAreNowSymmetricTest(unittest.TestCase):

    def test_both_model_calls_have_a_timeout_control(self) -> None:
        for key in ("extraction.timeout_sec", SETTING):
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)
                self.assertEqual("float", cfg.SETTINGS_BY_KEY[key].kind)

    def test_they_agree_on_the_default(self) -> None:
        """Two names for the same length of patience; a customer would read a difference as
        meaningful."""
        self.assertEqual(float(cfg.SETTINGS_BY_KEY["extraction.timeout_sec"].default),
                         float(cfg.SETTINGS_BY_KEY[SETTING].default))

    def test_the_help_says_what_exceeding_it_costs(self) -> None:
        """A timeout a customer cannot reason about is a number they will not touch."""
        help_text = cfg.SETTINGS_BY_KEY[SETTING].help
        self.assertIn("hash vectors", help_text)
        self.assertIn("Fail instead of falling back", help_text)


if __name__ == "__main__":
    unittest.main()
