#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The third model role is reported: the one that writes the summaries retrieval walks.

A deployment runs three model roles. The model status surface carried two. Extraction runs once per
ingest; **every context node gets a summary**, so the summary path is the model called most, and it
had no block at all -- a deployment writing its summaries with rules looked exactly like one
writing them with a model.

It is not a hypothetical configuration. ``summary.provider``'s own help says it:

    openai_compatible is the only value that calls a model: anthropic returns rule-written
    summaries and no error, so a deployment on anthropic extraction gets deterministic summaries
    unless it names openai_compatible here.

Documented, and reported on no screen. The snapshot's own docstring says ``warnings`` exists for
exactly this: *the silent-degradation cases … indistinguishable from a healthy system at the API
surface.*

**The classifier mirrors the writer.** ``summary_provider_effect`` answers the same question as
``summary_provider()`` in matrixark_mcp_summaries, and a screen that disagrees with the writer is
worse than one that says nothing -- so the mirror is asserted here against the writer's own
literals rather than against a copy of them.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402


def snapshot(extraction: str, summary) -> dict:
    """The snapshot a deployment with these two providers would show.

    `summary=None` means the variable is NOT SET, which follows the extraction provider;
    `summary=""` means it is set to nothing, which does not. The writer reads
    `os.environ.get(NAME, <chain>)`, so the two differ, and a helper that flattened them would
    test something no deployment has.

    A subprocess because the summariser binds its provider chain into module constants at import,
    and this must be read the way a gateway reads it rather than after mutating a live process.
    """
    script = (
        "import json, matrixark_v1_gateway as gw;"
        "print(json.dumps(gw._model_config_snapshot()))"
    )
    environ = dict(os.environ)
    environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = extraction
    if summary is None:
        environ.pop("MATRIXARK_SUMMARY_PROVIDER", None)
    else:
        environ["MATRIXARK_SUMMARY_PROVIDER"] = summary
    environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/matrixark-summary-test.json"
    out = subprocess.run([sys.executable, "-c", script], cwd=TOOLS, env=environ,
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-600:])
    import json
    return json.loads(out.stdout)


def warned(snap: dict) -> bool:
    return any("Summaries are written by rules" in w for w in snap.get("warnings", []))


class TheClassifierMirrorsTheWriterTest(unittest.TestCase):
    """`summary_provider_effect` and `summary_provider()` decide the same question."""

    NAMES = ("", "deterministic", "openai_compatible", "anthropic", "oss", "open_source",
             "local_llm", "oss_llm", "local", "rules", "openai", "nonsense")

    @staticmethod
    def _writer(provider: str, extraction: str) -> str:
        script = (
            "import matrixark_mcp_summaries as sm;"
            "p = sm.summary_provider();"
            "print('model' if p in {'openai', 'openai_compatible', 'openai_compatible_llm'} "
            "else 'rules')"
        )
        environ = dict(os.environ)
        environ["MATRIXARK_SUMMARY_PROVIDER"] = provider
        environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = extraction
        environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/matrixark-summary-test.json"
        out = subprocess.run([sys.executable, "-c", script], cwd=TOOLS, env=environ,
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            raise AssertionError(out.stderr[-600:])
        return out.stdout.strip()

    def test_every_name_gets_the_same_answer_from_both(self) -> None:
        for extraction in ("openai_compatible", "anthropic", "deterministic"):
            for name in self.NAMES:
                with self.subTest(summary=name, extraction=extraction):
                    self.assertEqual(self._writer(name, extraction),
                                     cfg.summary_provider_effect(name, extraction),
                                     "the screen would disagree with the writer")

    def test_the_comparison_sees_both_answers(self) -> None:
        """The floor: if every name gave "rules" the loop above would pass on a constant."""
        answers = {cfg.summary_provider_effect(n, "deterministic") for n in self.NAMES}
        self.assertEqual({"model", "rules"}, answers, answers)


class TheSummaryBlockIsReportedTest(unittest.TestCase):

    def test_it_is_there_at_all(self) -> None:
        block = snapshot("openai_compatible", None)["summary"]
        self.assertEqual({"provider", "follows_extraction", "writes", "model",
                          # Added with the switch that claims to stop rule summaries, which the
                          # summariser on this path does not read.
                          "require_model", "require_model_enforced_by"}, set(block))

    def test_an_unset_provider_follows_extraction(self) -> None:
        block = snapshot("openai_compatible", None)["summary"]
        self.assertTrue(block["follows_extraction"])
        self.assertEqual("model", block["writes"])

    def test_but_set_to_nothing_does_not_follow_it(self) -> None:
        """The distinction the writer makes and this file first missed. `os.environ.get(NAME,
        chain)` falls through only when the name is absent; set to an empty string it returns
        that, and the summary path lands on rules."""
        block = snapshot("openai_compatible", "")["summary"]
        self.assertFalse(block["follows_extraction"])
        self.assertEqual("rules", block["writes"])

    def test_anthropic_extraction_writes_summaries_with_rules(self) -> None:
        """The case the help documents and nothing reported."""
        snap = snapshot("anthropic", None)
        self.assertEqual("rules", snap["summary"]["writes"])
        self.assertTrue(warned(snap), "the surprising case is still unreported")

    def test_naming_a_provider_fixes_it(self) -> None:
        """The remedy the warning offers has to work, or it is worse than silence."""
        snap = snapshot("anthropic", "openai_compatible")
        self.assertEqual("model", snap["summary"]["writes"])
        self.assertFalse(warned(snap))

    def test_the_model_is_named_only_when_one_is_called(self) -> None:
        self.assertEqual("", snapshot("anthropic", None)["summary"]["model"])
        self.assertTrue(snapshot("openai_compatible", "oss")["summary"]["writes"] == "model")


class TheWarningIsNotNoiseTest(unittest.TestCase):
    """A warning on every deployment is a warning nobody reads."""

    def test_rules_everywhere_is_not_warned_about_twice(self) -> None:
        snap = snapshot("deterministic", None)
        self.assertEqual("rules", snap["summary"]["writes"])
        self.assertFalse(warned(snap),
                         "extraction already says this deployment calls no model; saying it "
                         "again about summaries is noise")

    def test_but_the_extraction_warning_is_still_there(self) -> None:
        """The floor for the test above: it must be silent because the other warning covers it,
        not because nothing warns at all."""
        snap = snapshot("deterministic", None)
        self.assertTrue(any("deterministic" in w.lower() for w in snap["warnings"]),
                        snap["warnings"])

    def test_a_deliberate_opt_out_is_still_reported(self) -> None:
        """Choosing rules while extraction calls a model is a choice, and still worth stating:
        the summary is what retrieval walks."""
        snap = snapshot("openai_compatible", "deterministic")
        self.assertTrue(warned(snap))


if __name__ == "__main__":
    unittest.main()
