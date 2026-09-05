#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""One implementation of the retrieval audit policy, and two defaults that are declared, not hidden.

``retrieval_audit_policy`` and the block in ``matrixark_local_adapter_retrieve`` were line-for-line
identical -- same branches, same validation, same messages, same exception types -- except for one
literal: what an unset ``MATRIXARK_CONTEXT_AUDIT_MODE`` means. The helper said ``telemetry_only``;
the copy said ``off``.

That literal decides whether a retrieve records anything, since ``telemetry_record`` is
``audit_mode != "off"``. So on a default deployment two of the three retrieve paths write audit
telemetry and one does not, and the only way to find that out was to read both implementations.

**This file does not settle which default is right.** That is a policy question with an operational
cost -- adopting ``telemetry_only`` starts writing records on a path that currently writes none --
and both were introduced in the same commit with no comment saying which was intended. What this
does is remove the duplicate that let them drift apart, and pin each caller's default by name, so
changing one is a deliberate edit to a test that says what it means.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_mcp_retrieve_planning as planning  # noqa: E402

MatrixArkError = planning.MatrixArkError

KNOBS = ("MATRIXARK_CONTEXT_AUDIT_MODE", "MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE")


class _CleanEnv(unittest.TestCase):

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


class TheDefaultIsTheCallersToDeclareTest(_CleanEnv):

    def test_the_helper_still_defaults_to_telemetry(self) -> None:
        """What the request path and the direct read take."""
        mode, _rate = planning.retrieval_audit_policy({})
        self.assertEqual("telemetry_only", mode)

    def test_a_caller_can_declare_a_different_one(self) -> None:
        """What the local adapter takes. The difference is the argument, not a second copy."""
        mode, _rate = planning.retrieval_audit_policy({}, default="off")
        self.assertEqual("off", mode)

    def test_the_environment_beats_either_default(self) -> None:
        os.environ["MATRIXARK_CONTEXT_AUDIT_MODE"] = "full"
        self.assertEqual("full", planning.retrieval_audit_policy({})[0])
        self.assertEqual("full", planning.retrieval_audit_policy({}, default="off")[0])

    def test_the_request_beats_the_environment(self) -> None:
        os.environ["MATRIXARK_CONTEXT_AUDIT_MODE"] = "off"
        mode, _rate = planning.retrieval_audit_policy({"audit_mode": "telemetry_only"})
        self.assertEqual("telemetry_only", mode)


class TheRulesAreTheSameWhicheverDefaultTest(_CleanEnv):
    """The point of one implementation: everything except the default behaves identically."""

    def test_an_unknown_mode_is_refused_either_way(self) -> None:
        for default in ("telemetry_only", "off"):
            with self.subTest(default=default):
                with self.assertRaises(MatrixArkError) as caught:
                    planning.retrieval_audit_policy({"audit_mode": "sometimes"}, default=default)
                self.assertIn("audit_mode must be full, telemetry_only, or off", str(caught.exception))

    def test_full_means_every_request_either_way(self) -> None:
        for default in ("telemetry_only", "off"):
            with self.subTest(default=default):
                self.assertEqual(1.0,
                                 planning.retrieval_audit_policy({"audit_mode": "full"},
                                                                 default=default)[1])

    def test_the_sample_rate_falls_back_to_one_percent_either_way(self) -> None:
        for default in ("telemetry_only", "off"):
            with self.subTest(default=default):
                self.assertAlmostEqual(0.01,
                                       planning.retrieval_audit_policy({}, default=default)[1])

    def test_an_explicit_rate_wins_either_way(self) -> None:
        for default in ("telemetry_only", "off"):
            with self.subTest(default=default):
                self.assertAlmostEqual(
                    0.5, planning.retrieval_audit_policy({"audit_sample_rate": 0.5},
                                                         default=default)[1])

    def test_a_rate_that_is_not_a_number_is_refused_either_way(self) -> None:
        for default in ("telemetry_only", "off"):
            with self.subTest(default=default):
                with self.assertRaises(MatrixArkError):
                    planning.retrieval_audit_policy({"audit_sample_rate": "soon"}, default=default)


class TheCallersDeclareWhatTheyTakeTest(unittest.TestCase):
    """Pinned by name. Whichever way the policy question is settled, it should be settled here as
    well as in the code -- and a silent change to either should fail."""

    def _source(self, name):
        with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
            return handle.read()

    def test_the_local_adapter_declares_off(self) -> None:
        source = self._source("matrixark_local_adapter_retrieve.py")
        self.assertIn('retrieval_audit_policy(\n            args, default="off")', source)

    def test_the_local_adapter_no_longer_carries_its_own_copy(self) -> None:
        """The change this file exists for. Without it the two can drift apart again exactly as
        they did, and nothing here would notice."""
        source = self._source("matrixark_local_adapter_retrieve.py")
        self.assertNotIn("MATRIXARK_CONTEXT_AUDIT_MODE", source,
                         "the local adapter resolves the audit mode itself again")
        self.assertNotIn("audit_mode must be full", source,
                         "the local adapter validates the audit mode itself again")

    def test_the_request_path_takes_the_helpers_own_default(self) -> None:
        source = self._source("matrixark_mcp_retrieve_request.py")
        self.assertIn("retrieval_audit_policy(args)", source)

    def test_only_one_module_reads_the_variable_for_this_decision(self) -> None:
        """The direct-read path has its own resolution with its own fallback-on-invalid, so it is
        not folded in here; this pins the count so a fourth copy cannot appear unnoticed."""
        readers = []
        for name in sorted(os.listdir(TOOLS)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
                if "MATRIXARK_CONTEXT_AUDIT_MODE" in handle.read():
                    readers.append(name)
        self.assertEqual(["matrixark_mcp_retrieve_planning.py",
                          "matrixark_temporal_direct_read.py"], readers, readers)


if __name__ == "__main__":
    unittest.main()
