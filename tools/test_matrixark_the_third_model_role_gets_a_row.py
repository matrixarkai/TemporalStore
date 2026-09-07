#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The readiness checklist has a row for the model role it was missing.

Three model roles run in a deployment. The checklist carried three rows for two of them --
`extraction`, `extraction_key`, `embedding`, `fail_closed` -- and none for summaries, which is the
role called most: extraction runs once per ingest, and EVERY context node gets a summary that
retrieval then walks.

So a deployment writing its summaries with rules read as complete. That is not a corner case:
`summary.provider`'s own help says an Anthropic extraction provider returns rule-written summaries
and no error, and the switch that claims to stop it is read by the engine and not by the summariser
on the local-adapter path.

**Only when extraction reaches a model.** Rules everywhere is one decision and the extraction row
already states it; saying it twice is how a checklist teaches people to skim. `test_it_is_absent_...`
holds that line, and `test_the_extraction_row_still_says_it` is its floor -- silence has to be
because the other row speaks, not because nothing does.
"""
from __future__ import annotations

import ast
import io
import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

_PROBE = r'''
import json, sys
sys.path.insert(0, ".")
import matrixark_v1_gateway as gw
snap = gw._model_config_snapshot()
rows = gw._readiness_checks(snap, {}, type("c", (), {"require_auth": False})())
print(json.dumps({"rows": rows, "summary": snap.get("summary")}))
'''


def checks(extraction: str, summary=None, require: str = "0") -> dict:
    """The checklist a deployment with these would show. `summary=None` is the variable UNSET,
    which follows the extraction provider; `""` is set to nothing, which does not."""
    environ = dict(os.environ)
    environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = extraction
    environ["MATRIXARK_REQUIRE_MODEL_SUMMARIES"] = require
    environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/matrixark-summary-row-test.json"
    if summary is None:
        environ.pop("MATRIXARK_SUMMARY_PROVIDER", None)
    else:
        environ["MATRIXARK_SUMMARY_PROVIDER"] = summary
    out = subprocess.run([sys.executable, "-c", _PROBE], cwd=TOOLS, env=environ,
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    payload = json.loads(out.stdout)
    payload["by_id"] = {row["id"]: row for row in payload["rows"]}
    return payload


class TheRowIsThereTest(unittest.TestCase):

    def test_a_deployment_writing_rule_summaries_is_warned(self) -> None:
        """The documented case: Anthropic extraction, summaries by rules, no error anywhere."""
        row = checks("anthropic")["by_id"]["summaries"]
        self.assertEqual("warn", row["status"])
        self.assertIn("written by rules", row["detail"])

    def test_a_deployment_writing_them_with_a_model_is_not(self) -> None:
        row = checks("openai_compatible")["by_id"]["summaries"]
        self.assertEqual("ok", row["status"])

    def test_naming_the_provider_fixes_it(self) -> None:
        """The remedy the row offers has to work, or the row is worse than no row."""
        self.assertEqual("ok", checks("anthropic", "openai_compatible")["by_id"]["summaries"]
                         ["status"])

    def test_it_is_absent_when_the_whole_deployment_runs_on_rules(self) -> None:
        """One decision, one row. The extraction row above already says this."""
        self.assertNotIn("summaries", checks("deterministic")["by_id"])

    def test_the_extraction_row_still_says_it(self) -> None:
        """The floor for the test above: silence has to be because the other row speaks."""
        row = checks("deterministic")["by_id"]["extraction"]
        self.assertEqual("todo", row["status"])


class TheRowSaysWhatItIsBasedOnTest(unittest.TestCase):
    """`_CHECK_SOURCES` is declared per check rather than defaulted, so a row added later has to
    say which kind of claim it makes instead of inheriting the one that looks authoritative."""

    @staticmethod
    def _sources() -> dict:
        with io.open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", "") == \
                    "_CHECK_SOURCES":
                return {k.value: v.value for k, v in zip(node.value.keys, node.value.values)}
            if isinstance(node, ast.Assign) and any(
                    getattr(t, "id", "") == "_CHECK_SOURCES" for t in node.targets):
                return {k.value: v.value for k, v in zip(node.value.keys, node.value.values)}
        raise AssertionError("_CHECK_SOURCES not found")

    def test_the_new_row_declares_one(self) -> None:
        self.assertEqual("configuration", self._sources().get("summaries"))

    def test_every_row_a_deployment_can_see_declares_one(self) -> None:
        """Not just the new one: an undeclared row prints nothing about where its answer came
        from, which is the distinction this map exists to keep."""
        sources = self._sources()
        seen = set()
        for extraction in ("anthropic", "openai_compatible", "deterministic"):
            seen |= {row["id"] for row in checks(extraction)["rows"]}
        self.assertEqual(set(), seen - set(sources), sorted(seen - set(sources)))

    def test_the_sweep_sees_the_new_row(self) -> None:
        """The floor for the test above: it must be looking at a set that contains it."""
        seen = {row["id"] for row in checks("anthropic")["rows"]}
        self.assertIn("summaries", seen)


class TheRowNamesTheSwitchWhenItIsOnTest(unittest.TestCase):
    """The switch says it makes this state fail. Where the local adapter writes the summaries it
    does not, so a reader who set it needs the row to say so rather than repeat the promise."""

    def test_the_detail_names_it(self) -> None:
        detail = checks("anthropic", None, "1")["by_id"]["summaries"]["detail"]
        self.assertIn("nothing in this build reads that switch", detail)

    def test_it_is_not_mentioned_when_it_is_off(self) -> None:
        detail = checks("anthropic", None, "0")["by_id"]["summaries"]["detail"]
        self.assertNotIn("that switch", detail)


if __name__ == "__main__":
    unittest.main()
