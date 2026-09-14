#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The model-config snapshot must not read a flag by case.

`_model_config_snapshot` reports two require-model guarantees:

    "require_model": _require_model_summaries,
    "require_model_enforced_by": "engine",

and it read both flags with a membership test against `{"1","true","yes","on"}` applied to a value
its own `_env` helper had only STRIPPED, never lowercased. The complete word list, decided by case.

Measured across 16 values, against the three other readers of the same two flags -- this file's own
`_env_bool`, `matrixark_mcp_embeddings._truthy_env`, and the engine's `env_flag::parse_bool`:
`TRUE`, `True`, `YES`, `Yes`, `ON` and `On` were on to all three and off to the snapshot. Six of
sixteen. So `MATRIXARK_REQUIRE_MODEL_SUMMARIES=ON` made the snapshot report the guarantee as OFF in
the same object where the very next field names the engine as its enforcer -- and the engine had it
on.

This file asserts the snapshot's ANSWER, by calling it, rather than the shape of the expression
that produces it. `test_no_flag_reader_decides_by_case` is the shape rule for the rust side and it
is worth having; here the function is cheap to call, and an answer is a stronger thing to pin than
a spelling.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

try:
    from tools import matrixark_v1_gateway as gw  # type: ignore
except Exception:  # pragma: no cover - direct layout
    import matrixark_v1_gateway as gw  # type: ignore

SUMMARIES = "MATRIXARK_REQUIRE_MODEL_SUMMARIES"
EMBEDDINGS = "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS"
PARTICIPATING = (SUMMARIES, EMBEDDINGS)

#: Every spelling of on that the shared vocabulary accepts, in the cases an operator writes.
ON_WORDS = ("1", "true", "TRUE", "True", "yes", "YES", "Yes", "on", "ON", "On", " on ", "  1")
OFF_WORDS = ("0", "false", "FALSE", "no", "NO", "off", "OFF", "Off", " 0 ")
#: Outside both halves: these must follow the default, which for both flags is off.
NEITHER = ("", "   ", "y", "n", "enabled", "ture", "garbage")


class TheSnapshotDoesNotReadItsFlagsByCase(unittest.TestCase):

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

    def _summaries(self, value=None):
        if value is None:
            os.environ.pop(SUMMARIES, None)
        else:
            os.environ[SUMMARIES] = value
        return gw._model_config_snapshot()["summary"]["require_model"]

    def _embeddings(self, value=None):
        if value is None:
            os.environ.pop(EMBEDDINGS, None)
        else:
            os.environ[EMBEDDINGS] = value
        snapshot = gw._model_config_snapshot()
        # The embedding block spells it in full; the summary block calls it `require_model` and
        # names its enforcer beside it. Two names for one kind of claim, which is why this
        # accessor exists rather than one shared lookup.
        return snapshot["embedding"]["require_model_embeddings"]

    def test_the_value_space_is_not_empty(self) -> None:
        """A sweep that stops matching reads exactly like a clean sweep."""
        self.assertGreaterEqual(len(ON_WORDS), 10)
        self.assertGreaterEqual(len(OFF_WORDS), 8)
        self.assertGreaterEqual(len(NEITHER), 5)

    def test_the_snapshot_reports_the_fields_this_file_is_about(self) -> None:
        """A floor: if either field is gone or renamed, every assertion below is vacuous."""
        snapshot = gw._model_config_snapshot()
        self.assertIn("require_model", snapshot["summary"])
        self.assertIn("require_model_embeddings", snapshot["embedding"])
        self.assertEqual(
            "engine", snapshot["summary"].get("require_model_enforced_by"),
            "the summary block no longer names the engine as the enforcer. That naming is why "
            "this flag disagreeing with the engine matters; re-read the docstring before "
            "changing this.")

    def test_every_on_word_is_on_whatever_its_case(self) -> None:
        for value in ON_WORDS:
            with self.subTest(flag=SUMMARIES, value=value):
                self.assertTrue(
                    self._summaries(value),
                    "%s=%r reports require_model false while the engine, this file's own "
                    "_env_bool and mcp_embeddings._truthy_env all read it as on"
                    % (SUMMARIES, value))
            with self.subTest(flag=EMBEDDINGS, value=value):
                self.assertTrue(self._embeddings(value),
                                "%s=%r reports require_model false" % (EMBEDDINGS, value))

    def test_every_off_word_is_off_whatever_its_case(self) -> None:
        for value in OFF_WORDS:
            with self.subTest(flag=SUMMARIES, value=value):
                self.assertFalse(self._summaries(value))
            with self.subTest(flag=EMBEDDINGS, value=value):
                self.assertFalse(self._embeddings(value))

    def test_a_value_in_neither_half_follows_the_default(self) -> None:
        """Both flags default OFF, so an unrecognised value must not read as a guarantee."""
        for value in NEITHER:
            with self.subTest(flag=SUMMARIES, value=value):
                self.assertFalse(
                    self._summaries(value),
                    "%s=%r is outside the vocabulary and must follow the default, not be "
                    "reported as a guarantee" % (SUMMARIES, value))
            with self.subTest(flag=EMBEDDINGS, value=value):
                self.assertFalse(self._embeddings(value))

    def test_unset_is_off_for_both(self) -> None:
        self.assertFalse(self._summaries(None))
        self.assertFalse(self._embeddings(None))

    def test_the_snapshot_agrees_with_the_other_python_reader(self) -> None:
        """The divergence that made this worth fixing: two python readers, one flag."""
        try:
            import matrixark_mcp_embeddings as emb  # type: ignore
        except Exception as exc:  # pragma: no cover
            self.skipTest("matrixark_mcp_embeddings not importable: %s" % exc)
        truthy = getattr(emb, "_truthy_env", None)
        self.assertIsNotNone(
            truthy, "_truthy_env is gone, so this comparison proves nothing")
        for value in ON_WORDS + OFF_WORDS:
            with self.subTest(value=value):
                os.environ[EMBEDDINGS] = value
                self.assertEqual(
                    truthy(EMBEDDINGS), self._embeddings(value),
                    "the gateway snapshot and mcp_embeddings._truthy_env disagree about "
                    "%s=%r" % (EMBEDDINGS, value))


if __name__ == "__main__":
    unittest.main()
