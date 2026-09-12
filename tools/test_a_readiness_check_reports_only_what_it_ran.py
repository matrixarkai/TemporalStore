#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A readiness check may not report a verification it did not perform.

`MatrixArkLocalAdapter.ensure_backend_ready` returned three checks, all hard-coded True. Two of
them describe work the JSONL adapter does not do: it has no namespace and no table to open, and it
issues no warmup hset/hget, so `slot_coverage_verified_by_warmup_hset_hget` asserted a round-trip
that cannot happen on that backend.

The engine adapters already spell the convention: `matrixark_temporal_direct_write` initialises
both to False and flips them True only after the warmup actually round-trips. False means "not
verified" there, not "failed" -- which is why reporting False here costs nothing and reporting
True cost the reader their ability to tell the two backends apart.

WHAT THIS FILE CHECKS, and what it deliberately does not. It does not require any particular check
to be False: a backend that really does open a namespace should say True, and pinning values would
make this file a copy of the implementation. It requires that a literal True is not the ONLY thing
standing behind a check name -- that somewhere in the function that produced it, the value came
from something other than the constant. For the local adapter that is now two Falses and one True
whose name is `mcp_process_started`, which is the one claim the function can make about itself.
"""
from __future__ import annotations

import ast
import os
import pathlib
import unittest

TOOLS = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

#: The check names that describe a ROUND TRIP -- work against a store, not a statement about the
#: process. A backend that cannot do the work must not claim it.
ROUND_TRIP_CHECKS = ("namespace_table_opened", "slot_coverage_verified_by_warmup_hset_hget")


def _literal_true_checks(path):
    """(function, check name) for every round-trip check set to a literal True in that file."""
    out = []
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, OSError):  # pragma: no cover
        return out
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Dict):
                continue
            for key, value in zip(sub.keys, sub.values):
                if (isinstance(key, ast.Constant) and key.value in ROUND_TRIP_CHECKS
                        and isinstance(value, ast.Constant) and value.value is True):
                    out.append((node.name, key.value, sub.lineno))
    return out


class AReadinessCheckReportsOnlyWhatItRanTest(unittest.TestCase):

    def test_the_scan_reads_the_tree(self) -> None:
        """Control: a glob matching nothing would satisfy everything below."""
        modules = list(TOOLS.glob("*.py"))
        self.assertGreater(len(modules), 100,
                           "only %d modules found; the scan is not reading the tree" % len(modules))

    def test_the_scan_still_finds_these_check_names(self) -> None:
        """Positive control on the names. If they are renamed this file silently checks nothing."""
        found = set()
        for path in TOOLS.glob("*.py"):
            if path.name.startswith("test_"):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:  # pragma: no cover
                continue
            for name in ROUND_TRIP_CHECKS:
                if name in text:
                    found.add(name)
        self.assertEqual(
            set(ROUND_TRIP_CHECKS), found,
            "these round-trip check names are no longer in the tree: %s -- rename them here or "
            "drop them, because this file is now asserting about nothing"
            % ", ".join(sorted(set(ROUND_TRIP_CHECKS) - found)))

    def test_no_backend_claims_a_round_trip_with_a_literal_true(self) -> None:
        """The rule. A check naming a store round-trip must be earned, not asserted."""
        claims = []
        for path in sorted(TOOLS.glob("*.py")):
            if path.name.startswith("test_"):
                continue
            for func, check, line in _literal_true_checks(path):
                claims.append("%s.%s sets %s to True at line %d" % (path.stem, func, check, line))
        self.assertEqual(
            [], claims,
            "a readiness report states a store round-trip succeeded without performing one. "
            "Initialise it False and set it True where the work happens, the way "
            "matrixark_temporal_direct_write does: %s" % "; ".join(claims))


if __name__ == "__main__":
    unittest.main()
