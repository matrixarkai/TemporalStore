#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A customer who picks anthropic can choose its model.

``Extraction provider`` offers ``anthropic``, and the anthropic call sends ``ANTHROPIC_LLM_MODEL``
to ``ANTHROPIC_API_BASE``. Neither had a control, and neither falls back to the extraction fields
sitting beside them -- so typing ``claude-opus-4`` into **Extraction model** produced a call to
``claude-sonnet-5``, and a proxy in **Extraction base URL** still went to ``api.anthropic.com``.
Nothing failed and nothing said so::

    a customer picks anthropic and types claude-opus-4 into Extraction model
       the anthropic call would send model : 'claude-sonnet-5'
       ...to base                          : 'https://api.anthropic.com'
       while Extraction model says         : 'claude-opus-4'

**The asymmetry is exact, and it is why there are two controls here and not four.** Max tokens and
timeout already fall back to their extraction controls; the model and the endpoint do not. The two
fields with no control were precisely the two that ignore the controls next to them. That fallback
behaviour is asserted below, so if the model ever gains one, these controls get reconsidered rather
than quietly becoming a second way to say the same thing.

Everything is measured in a subprocess: these are module-scope constants bound at import, so a test
that set the variable afterwards would be testing something no deployment does.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

ANTHROPIC_CONTROLS = ("extraction.anthropic_model", "extraction.anthropic_base_url")

PROBE = """
import json, sys
sys.path.insert(0, %r)
import matrixark_mcp_core as core
print(json.dumps({
    "model":      core.ANTHROPIC_LLM_MODEL,
    "base":       core.ANTHROPIC_API_BASE,
    "max_tokens": core.ANTHROPIC_LLM_MAX_TOKENS,
    "timeout":    core.ANTHROPIC_LLM_TIMEOUT_SEC,
    "extraction_model": core.EXTRACTION_LLM_MODEL,
}))
""" % TOOLS

CLEAR = ("MATRIXARK_ANTHROPIC_MODEL", "MATRIXARK_ANTHROPIC_API_BASE",
         "MATRIXARK_ANTHROPIC_MAX_TOKENS", "MATRIXARK_ANTHROPIC_TIMEOUT_SEC",
         "MATRIXARK_EXTRACTION_MODEL", "MATRIXARK_EXTRACTION_BASE_URL",
         "MATRIXARK_EXTRACTION_MAX_TOKENS", "MATRIXARK_EXTRACTION_TIMEOUT_SEC", "OPENAI_MODEL")


def started_with(**overrides):
    env = dict(os.environ)
    for name in CLEAR:
        env.pop(name, None)
    env.update({k: v for k, v in overrides.items() if v is not None})
    proc = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                          timeout=600, env=env, cwd=TOOLS)
    if proc.returncode != 0:
        raise AssertionError("the probe did not run: %s" % proc.stderr[-400:])
    return json.loads(proc.stdout.strip().splitlines()[-1])


class TheControlsExistTest(unittest.TestCase):

    def test_both_are_offered(self) -> None:
        for key in ANTHROPIC_CONTROLS:
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)

    def test_they_sit_with_the_provider_that_selects_them(self) -> None:
        for key in ANTHROPIC_CONTROLS:
            with self.subTest(setting=key):
                self.assertEqual("extraction", cfg.SETTINGS_BY_KEY[key].group)

    def test_anthropic_is_still_a_provider_a_customer_can_pick(self) -> None:
        """If it ever stops being offered, these two controls stop having a reason to exist."""
        self.assertIn("anthropic", cfg.SETTINGS_BY_KEY["extraction.provider"].choices)


class TheDeclaredDefaultsAreTheRealOnesTest(unittest.TestCase):
    """The portal shows a setting's default when nothing is stored, so a wrong one is a lie about
    what the deployment is doing."""

    def test_the_model_default_is_what_the_code_uses(self) -> None:
        self.assertEqual(started_with()["model"],
                         cfg.SETTINGS_BY_KEY["extraction.anthropic_model"].default)

    def test_the_base_default_is_what_the_code_uses(self) -> None:
        self.assertEqual(started_with()["base"],
                         cfg.SETTINGS_BY_KEY["extraction.anthropic_base_url"].default)


class TheControlsActuallySelectTheModelTest(unittest.TestCase):
    """Driven through the variable each control DECLARES, not one written out here.

    Setting `MATRIXARK_ANTHROPIC_MODEL` directly would pass just as well if the control pointed at
    a variable nothing reads -- which is the way a control becomes decorative.
    """

    def variable(self, key):
        return cfg._env_name(cfg.SETTINGS_BY_KEY[key], {})

    def test_naming_a_model_changes_what_is_called(self) -> None:
        got = started_with(**{self.variable("extraction.anthropic_model"): "claude-opus-4"})
        self.assertEqual("claude-opus-4", got["model"])

    def test_naming_an_endpoint_changes_where_it_is_called(self) -> None:
        got = started_with(**{self.variable("extraction.anthropic_base_url"): "https://proxy.example"})
        self.assertEqual("https://proxy.example", got["base"])


class TheAsymmetryThatExplainsTheScopeTest(unittest.TestCase):
    """Why two controls and not four."""

    def test_the_model_does_not_fall_back_to_the_extraction_control(self) -> None:
        """The reason a separate control is needed. If this ever gains a fallback, this test fails
        and somebody decides whether the control is still wanted."""
        got = started_with(MATRIXARK_EXTRACTION_MODEL="claude-opus-4")
        self.assertEqual("claude-opus-4", got["extraction_model"])
        self.assertNotEqual("claude-opus-4", got["model"])

    def test_the_endpoint_does_not_either(self) -> None:
        got = started_with(MATRIXARK_EXTRACTION_BASE_URL="https://my-proxy.example/v1")
        self.assertNotEqual("https://my-proxy.example/v1", got["base"])

    def test_max_tokens_does_fall_back_so_it_needs_no_control(self) -> None:
        self.assertEqual(4321, started_with(MATRIXARK_EXTRACTION_MAX_TOKENS="4321")["max_tokens"])

    def test_the_timeout_does_too(self) -> None:
        self.assertEqual(77.0, started_with(MATRIXARK_EXTRACTION_TIMEOUT_SEC="77")["timeout"])


class TheOtherTwoControlsSayTheyAreIgnoredTest(unittest.TestCase):
    """A customer on anthropic reads Extraction model first; it has to say it is not the one."""

    def test_the_model_control_says_so(self) -> None:
        self.assertIn("anthropic ignores this",
                      cfg.SETTINGS_BY_KEY["extraction.model"].help)

    def test_the_base_url_control_says_so(self) -> None:
        self.assertIn("anthropic ignores this",
                      cfg.SETTINGS_BY_KEY["extraction.base_url"].help)

    def test_each_points_at_the_one_that_is_used(self) -> None:
        self.assertIn("Anthropic model", cfg.SETTINGS_BY_KEY["extraction.model"].help)
        self.assertIn("Anthropic base URL", cfg.SETTINGS_BY_KEY["extraction.base_url"].help)


if __name__ == "__main__":
    unittest.main()
