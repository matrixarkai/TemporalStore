#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The storage-tuning validator has to run, not just be importable.

It raised `KeyError: 'native_runtime'` on every invocation -- it read a key that no version of its
`files` map has contained since that entry was dropped -- so it had never reported anything, right
or wrong. Nothing noticed, because the one test that mentions it imports `EXPECTED_DEFAULTS` for a
constant and never calls `main()`.

That is the shape worth guarding against rather than the specific bug: a validator is only doing
its job if something executes it and reads the exit code.
"""
from __future__ import annotations

import io
import os
import sys
import unittest
from contextlib import redirect_stdout, redirect_stderr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import validate_storage_tuning_conformance as tuning  # noqa: E402


class TheTuningValidatorRunsTest(unittest.TestCase):

    def test_it_completes_and_agrees_with_the_engine(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = tuning.main()
        self.assertEqual(
            0, code,
            "the storage tuning validator reports a disagreement between what it expects and what "
            "the engine declares:\n%s%s" % (out.getvalue(), err.getvalue()))

    def test_it_checks_a_meaningful_number_of_knobs(self) -> None:
        """A validator that expects nothing passes everything."""
        self.assertGreaterEqual(
            len(tuning.EXPECTED_KNOBS), 6,
            "only %d knobs are expected, so agreeing with the engine proves little"
            % len(tuning.EXPECTED_KNOBS))
        self.assertGreaterEqual(
            len(tuning.EXPECTED_DEFAULTS), 6,
            "only %d defaults are recorded, so the drift check below covers almost nothing"
            % len(tuning.EXPECTED_DEFAULTS))

    def test_every_expected_knob_has_a_recorded_default(self) -> None:
        # Otherwise a knob can be listed as required and have its value drift unchecked.
        missing = sorted(tuning.EXPECTED_KNOBS - set(tuning.EXPECTED_DEFAULTS))
        self.assertEqual(
            [], missing,
            "these knobs are required to exist but no default is recorded for them, so a change "
            "to their value is not checked: %s" % ", ".join(missing))

    def test_the_engine_declares_a_default_for_each(self) -> None:
        """The half that catches a rename: a constant that no longer exists reads as no default."""
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        found = tuning.extract_rust_defaults(
            root / "crates" / "temporalstore-rust" / "src" / "storage_config.rs")
        missing = sorted(set(tuning.EXPECTED_DEFAULTS) - set(found))
        self.assertEqual(
            [], missing,
            "the engine declares no default constant for these, which usually means the constant "
            "was renamed and this validator still asks for the old name: %s" % ", ".join(missing))


if __name__ == "__main__":
    unittest.main()
