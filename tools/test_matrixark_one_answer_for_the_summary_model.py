#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Two modules answer "which model writes a summary", and they must not disagree.

``matrixark_mcp_summaries`` imports nothing from this project on purpose -- five modules pull it in,
and a cycle would be worse than a duplicated line -- so it re-derives what ``matrixark_mcp_core``
defines. Its chain for the summary model open-codes what mcp_core calls ``EXTRACTION_LLM_MODEL``,
and it used to end in a different literal::

    core       MATRIXARK_SUMMARY_MODEL -> MATRIXARK_EXTRACTION_MODEL -> OPENAI_MODEL -> qwen2.5:1.5b
    summaries  MATRIXARK_SUMMARY_MODEL -> MATRIXARK_EXTRACTION_MODEL -> OPENAI_MODEL -> gpt-4o-mini

Both modules **use** it: each sends ``model=SUMMARY_LLM_MODEL`` to the configured endpoint. So a
deployment that chose a summary provider and named no model asked for a different model depending on
which of them happened to summarise, and nothing anywhere said so. On an OpenAI endpoint one of
those two silently names a paid model the customer never chose.

The values are bound at import, so each case below is measured in its own process with the
environment set before either module is imported. Reading the expressions and re-implementing them
here would test this file's copy of the logic rather than the modules'.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

PROBE = """
import json, sys
sys.path.insert(0, %r)
import matrixark_mcp_core as core
import matrixark_mcp_summaries as summaries
print(json.dumps({
    "core": core.SUMMARY_LLM_MODEL,
    "summaries": summaries.SUMMARY_LLM_MODEL,
    "core_provider": core.SUMMARY_LLM_PROVIDER,
    "summaries_provider": summaries.SUMMARY_LLM_PROVIDER,
    "core_tokens": core.SUMMARY_LLM_MAX_TOKENS,
    "summaries_tokens": summaries.SUMMARY_LLM_MAX_TOKENS,
}))
""" % TOOLS

# What a deployment can plausibly have set when a summary is written.
CASES = {
    "nothing set": {},
    "a provider chosen, no model named": {"MATRIXARK_SUMMARY_PROVIDER": "openai_compatible"},
    "only OPENAI_MODEL": {"OPENAI_MODEL": "gpt-4o"},
    "an extraction model named": {"MATRIXARK_EXTRACTION_MODEL": "deepseek-chat"},
    "a summary model named": {"MATRIXARK_SUMMARY_MODEL": "something-cheap"},
    "extraction named and OPENAI_MODEL set": {"MATRIXARK_EXTRACTION_MODEL": "deepseek-chat",
                                              "OPENAI_MODEL": "gpt-4o"},
}

CLEAR = ("MATRIXARK_SUMMARY_MODEL", "MATRIXARK_SUMMARY_PROVIDER", "MATRIXARK_EXTRACTION_MODEL",
         "MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER", "OPENAI_MODEL")


def _resolve(overrides):
    """Both modules' constants, from a process that started with this environment."""
    env = dict(os.environ)
    for name in CLEAR:
        env.pop(name, None)
    env.update(overrides)
    proc = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                          timeout=600, env=env, cwd=TOOLS)
    if proc.returncode != 0:
        raise AssertionError("the probe did not run: %s" % (proc.stderr[-500:]))
    return json.loads(proc.stdout.strip().splitlines()[-1])


class BothModulesNameTheSameModelTest(unittest.TestCase):

    def test_the_cases_actually_differ_from_each_other(self) -> None:
        """A floor. If every case resolved to one value, the comparisons below would hold for a
        pair of constants that never move, and the guard would be worth nothing."""
        seen = {_resolve(env)["core"] for env in CASES.values()}
        self.assertGreaterEqual(len(seen), 3, sorted(seen))

    def test_they_agree_on_the_model(self) -> None:
        for label, env in CASES.items():
            with self.subTest(case=label):
                got = _resolve(env)
                self.assertEqual(got["core"], got["summaries"],
                                 "%s: mcp_core would ask for %r and mcp_summaries for %r"
                                 % (label, got["core"], got["summaries"]))

    def test_they_agree_on_the_provider_and_the_token_budget(self) -> None:
        """These two already matched. Asserted so that fixing one drift does not leave the others
        unwatched -- all three constants are duplicated in the same way."""
        for label, env in CASES.items():
            with self.subTest(case=label):
                got = _resolve(env)
                self.assertEqual(got["core_provider"], got["summaries_provider"], label)
                self.assertEqual(got["core_tokens"], got["summaries_tokens"], label)


class TheValueIsActuallySentTest(unittest.TestCase):
    """A guard over two constants nobody reads would be tidy and pointless."""

    def _source(self, name):
        with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
            return handle.read()

    def test_each_module_sends_the_model_it_resolved(self) -> None:
        for name in ("matrixark_mcp_core.py", "matrixark_mcp_summaries.py"):
            with self.subTest(module=name):
                self.assertIn("model=SUMMARY_LLM_MODEL", self._source(name),
                              "%s resolves a summary model and never sends it" % name)


if __name__ == "__main__":
    unittest.main()
