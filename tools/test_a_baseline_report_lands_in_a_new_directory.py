#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A baseline runner asked for a report in a new directory must write it, not traceback.

The three external-baseline runners each define their own `finish`, and it is the function that
writes the run's JSON report and returns the exit code. Two of the three create the report's
parent directory first. `run_external_baseline_direct_retrieval` did not, so
`--report /somewhere/new/report.json` raised `FileNotFoundError` out of `Path.write_text`.

WHY IT MATTERED MORE THAN A MISSING DIRECTORY USUALLY DOES. `finish` is not only the success
path. That runner calls it four times before the benchmark starts -- once for each way the run can
be misconfigured -- to write a report whose `blockers` say what was wrong and exit non-zero. So
the case where the report is the ONLY output is exactly the case that crashed instead of
producing it, and the operator got a traceback about a path rather than the reason their run could
not start.

The fix is one statement and cannot change any value: `mkdir(parents=True, exist_ok=True)` is a
no-op for a directory that exists, which is every directory the default `--report` path has used.
`test_an_existing_directory_is_unaffected` pins that.

WHAT THIS DELIBERATELY DOES NOT DO. The three copies still differ in a second way: one appends a
trailing newline to the JSON and two do not. That is the file's bytes, so unifying it is a change
to output rather than a fix, and it is left alone -- the assertions below read the report back as
JSON and never compare the three byte for byte.
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import time
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

RUNNERS = (
    "run_external_baseline_direct_retrieval",
    "run_external_baseline_locomo_source_retrieval",
    "run_external_baseline_longmem_source_retrieval",
)


def _finish(stem):
    try:
        module = importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        module = importlib.import_module(stem)
    return getattr(module, "finish", None)


class ABaselineReportLandsInANewDirectory(unittest.TestCase):

    def test_every_runner_defines_the_writer(self) -> None:
        """A floor. Without it a renamed `finish` empties every assertion below."""
        for stem in RUNNERS:
            with self.subTest(runner=stem):
                self.assertIsNotNone(
                    _finish(stem),
                    "%s no longer defines finish, so this file is testing nothing" % stem)

    def test_the_report_directory_is_created(self) -> None:
        """The defect, asserted for all three so the odd one out cannot come back."""
        with tempfile.TemporaryDirectory() as root:
            for stem in RUNNERS:
                with self.subTest(runner=stem):
                    target = os.path.join(root, stem, "nested", "report.json")
                    self.assertFalse(
                        os.path.exists(os.path.dirname(target)),
                        "fixture floor: the directory must NOT exist, or this test passes for "
                        "the one reason it must never pass for")

                    code = _finish(stem)({"blockers": ["reader unreachable"]}, target,
                                         time.time(), 2)

                    self.assertTrue(
                        os.path.exists(target),
                        "%s returned without writing its report to a new directory" % stem)
                    self.assertEqual(
                        2, code, "%s changed the exit code it hands back" % stem)
                    body = json.loads(open(target, encoding="utf-8").read())
                    self.assertEqual(
                        ["reader unreachable"], body.get("blockers"),
                        "%s wrote a report that is not the one it was given" % stem)
                    self.assertFalse(
                        body.get("ready"),
                        "%s reported ready on a run that exited 2 with a blocker" % stem)

    def test_an_existing_directory_is_unaffected(self) -> None:
        """The control that makes the change additive rather than a behaviour change.

        Every default `--report` path writes into a directory that already exists. If creating
        the parent altered anything for that case, it would show here.
        """
        with tempfile.TemporaryDirectory() as root:
            for stem in RUNNERS:
                with self.subTest(runner=stem):
                    target = os.path.join(root, stem + ".json")
                    code = _finish(stem)({"blockers": []}, target, time.time() - 1.0, 0)

                    self.assertEqual(0, code)
                    body = json.loads(open(target, encoding="utf-8").read())
                    self.assertTrue(
                        body.get("ready"),
                        "%s no longer marks a clean run ready" % stem)
                    self.assertEqual(
                        ["blockers", "duration_seconds", "ready"], sorted(body),
                        "%s now writes a different set of report fields" % stem)
                    self.assertGreaterEqual(
                        body.get("duration_seconds"), 1.0,
                        "%s no longer measures the run's duration" % stem)


if __name__ == "__main__":
    unittest.main()
