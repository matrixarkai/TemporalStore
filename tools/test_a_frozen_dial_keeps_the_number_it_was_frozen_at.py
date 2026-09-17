#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A dial that stopped being deployment-configurable keeps the number it was frozen at.

matrixarkai#1823 retired eight settings a deployment could set. Each was behaviour-neutral on the
day: the value the shipped config carried already equalled the module constant, so freezing the
constant changed nothing that was running.

WHAT THAT TOOK AWAY. Until then the number had two independent statements of it -- the constant and
the line in `config/temporalstore.toml` -- and `test_matrixark_the_portal_declares_the_budget_the_
build_runs` compared them, so moving one alone failed. Retiring the config line removed the second
statement AND the comparison with it. It also removed the recovery: an operator who found a frozen
default wrong could set the variable, and now cannot. A number in that position with nothing
asserting it is the worst of the three states, so the assertion moves here.

MEASURED, not assumed. Five of these eight survived mutation with the retirement in place and no
guard naming them -- the two cross-session shares, the async-parse threshold, the readiness timeout
and the gateway context budget -- while the shard size, the summary cap and the near-duplicate
threshold were killed by guards that already existed. This file is written for the five and covers
all eight, because a reader asking "what pins this number" should find one answer and not two rules.

THE LIST IS CLOSED AND THE FILE SAYS SO. A guard that lists names can feed on its own list: drop an
entry and the rule still passes, smaller. Two things stop that here. The list is EXACTLY the eight
that matrixarkai#1823 retired and gains an entry only when another retirement adds one, which is
what `test_the_list_is_the_size_it_says_it_is` holds it to. And the second rule is DERIVED rather
than listed: none of the eight variables may be read by production Python again, which is what
"retired" means and is read out of the tree rather than remembered.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

#: variable -> (module, constant, the number it was frozen at)
#:
#: The variable is carried although nothing reads it any more: it is what an operator searching for
#: the knob they used to set will type, and it is what the second rule below looks for.
FROZEN = {
    "MATRIXARK_LOCAL_JSONL_MAX_BYTES": (
        "matrixark_mcp_local_adapter", "LOCAL_JSONL_MAX_BYTES", 64 * 1024 * 1024),
    "MATRIXARK_RESOURCE_ASYNC_DEFAULT_BYTES": (
        "matrixark_mcp_runtime_config", "RESOURCE_ASYNC_DEFAULT_BYTES", 2 * 1024 * 1024),
    "MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD": (
        "matrixark_mcp_runtime_config", "DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD", 0.85),
    "MATRIXARK_BACKEND_READINESS_TIMEOUT_MS": (
        "matrixark_mcp_runtime_config", "BACKEND_READINESS_TIMEOUT_MS", 30000),
    "MATRIXARK_CROSS_SESSION_BUDGET_RATIO": (
        "matrixark_mcp_runtime_config", "DEFAULT_CROSS_SESSION_BUDGET_RATIO", 0.12),
    "MATRIXARK_CROSS_SESSION_PROFILE_BUDGET_RATIO": (
        "matrixark_mcp_runtime_config", "DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO", 0.30),
    "MATRIXARK_GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS": (
        "matrixark_http", "GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS", 500000),
    # Two copies, both frozen. Checked as a pair below rather than here, because freezing one of a
    # pair at the wrong number is the failure a single lookup cannot see.
    "MATRIXARK_SUMMARY_MAX_TOKENS": (
        "matrixark_mcp_core", "SUMMARY_LLM_MAX_TOKENS", 900),
}

#: The second copy of the summary cap.
SUMMARY_SECOND_COPY = ("matrixark_mcp_summaries", "SUMMARY_LLM_MAX_TOKENS", 900)

#: How many matrixarkai#1823 retired. Moves when another retirement adds an entry, not otherwise.
RETIRED_IN_1823 = 8


def _module(name):
    try:
        return __import__("tools." + name, fromlist=["*"])
    except ImportError:
        return __import__(name)


def _tracked_production_python():
    out = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                         capture_output=True, text=True).stdout.split()
    return [rel for rel in out if not os.path.basename(rel).startswith("test_")]


class AFrozenDialKeepsItsNumberTest(unittest.TestCase):

    def test_each_constant_holds_the_number_it_was_frozen_at(self) -> None:
        """The literal, NOT the constant read back from the module it lives in.

        Written the other way first, and mutation caught it: a test asserting
        `resolved == module.CONSTANT` moves both sides together, so changing the constant leaves it
        green. The number has to be written out here to be an independent statement of it, which is
        the job the retired config file line used to do.
        """
        for variable, (module_name, constant, number) in sorted(FROZEN.items()):
            with self.subTest(variable=variable):
                value = getattr(_module(module_name), constant)
                if isinstance(number, float):
                    self.assertAlmostEqual(
                        number, float(value), places=6,
                        msg="%s.%s is %s; it was frozen at %s when %s was retired, and nothing "
                            "else states that number any more"
                            % (module_name, constant, value, number, variable))
                else:
                    self.assertEqual(
                        number, value,
                        "%s.%s is %s; it was frozen at %s when %s was retired, and nothing else "
                        "states that number any more"
                        % (module_name, constant, value, number, variable))

    def test_both_copies_of_the_summary_cap_hold_it(self) -> None:
        """One of a pair frozen at the wrong number is what a single lookup cannot see."""
        first_module, first_name, number = FROZEN["MATRIXARK_SUMMARY_MAX_TOKENS"]
        second_module, second_name, _ = SUMMARY_SECOND_COPY
        self.assertEqual(number, getattr(_module(first_module), first_name))
        self.assertEqual(
            number, getattr(_module(second_module), second_name),
            "the second copy of the summary cap is not %s. Both copies were frozen together and "
            "test_matrixark_one_answer_for_the_summary_model asserts they agree, so a disagreement "
            "here means one of them was moved on purpose and the other forgotten" % number)

    def test_none_of_them_is_read_from_the_environment_again(self) -> None:
        """DERIVED, so this file cannot go quietly out of date.

        "Retired" means the variable is not a route into the build any more. If one comes back, the
        number above stops being the whole story and the configurable surface has grown without the
        ratchet being told.
        """
        back = {}
        for rel in _tracked_production_python():
            try:
                with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                    text = handle.read()
            except OSError:  # pragma: no cover
                continue
            for variable in FROZEN:
                # A mention in a comment is how the retirement is recorded, so only a READ counts:
                # the name inside quotes, which is every shape a lookup takes.
                if re.search(r'["\']%s["\']' % re.escape(variable), text):
                    back.setdefault(variable, []).append(rel)
        self.assertEqual(
            {}, back,
            "these variables are named in production Python again, so they are not retired: %s"
            % back)

    def test_the_list_is_the_size_it_says_it_is(self) -> None:
        """The floor for a file that lists names. Not a population count: this list is CLOSED at
        the eight matrixarkai#1823 retired, so it may only grow, and shrinking is the failure --
        an entry dropped here takes its number's only statement with it."""
        self.assertGreaterEqual(
            len(FROZEN), RETIRED_IN_1823,
            "%d dials are pinned here and matrixarkai#1823 retired %d. An entry has been dropped, "
            "which leaves that number stated nowhere" % (len(FROZEN), RETIRED_IN_1823))

    def test_the_scan_it_derives_from_reads_the_tree(self) -> None:
        """The vacuity guard, on the SCAN rather than on the result above: the result going empty
        is the outcome that file wants, so a floor on it would fail on success."""
        files = _tracked_production_python()
        self.assertGreater(
            len(files), 100,
            "only %d production Python files were listed, so the rule above passed over almost "
            "nothing" % len(files))
        # NAMED, because a count cannot prove the reader works: a file that is listed but read as
        # empty scans clean. This one names a variable that IS still read.
        joined = "".join(
            open(os.path.join(REPO, rel), encoding="utf-8", errors="replace").read()
            for rel in files if "runtime_config" in rel)
        self.assertIn(
            '"MATRIXARK_CROSS_SESSION_MAX_CANDIDATES"', joined,
            "the reader found no live flag read in matrixark_mcp_runtime_config, so it would find "
            "no retired one either and the rule above is inert")


if __name__ == "__main__":
    unittest.main()
