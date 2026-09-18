#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The two summary writers differ by exactly one field, and this records which.

`refresh_dirty_node_summaries` is defined twice. The adapter binds
`matrixark_local_adapter_summaries` -- asked at runtime by ``__module__``, not read off the imports,
because a ``tools.``-prefixed import is a different module object. That is the copy that runs.

Its ``context_summary`` record carries 39 keys. The copy in `matrixark_mcp_summary_runtime` carries
40. The one it does not write is ``profile_promotion_policy``, and nothing goes the other way: the
live record is a strict subset.

WHY THIS IS RECORDED RATHER THAN FIXED. Every reader of that field defaults it --
``record.get("profile_promotion_policy", "")`` at seven sites in `matrixark_local_adapter_retrieve`
and again in the context pack -- so a summary without it reads as the empty string rather than
failing. Whether a ``context_summary`` SHOULD carry the field is a question about what the product
stores, and answering it by making one writer match the other changes stored data on a live path.
That is a decision, not a cleanup.

So this asserts the difference EXACTLY, in both directions. If the field starts being written, this
fails and someone confirms it was meant to. If a SECOND field drifts apart, this fails and the
divergence does not grow quietly, which is what happened to the ranker in
matrixarkai#1898: a fix reached one copy of a pair and the other kept the defect for months.

Reading the key sets out of the SOURCE rather than by calling the writers: both need an adapter,
a store and a scope to run, and a guard that needs a live backend is a guard that gets skipped.
"""
from __future__ import annotations

import ast
import inspect
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Reached through the parent on purpose: matrixark_local_adapter_summaries and
# matrixark_mcp_local_adapter import each other, so importing the summaries module first raises
# ImportError. Production binds the mixin through the adapter, and so does this.
import matrixark_mcp_local_adapter  # noqa: F401,E402
import matrixark_local_adapter_summaries as adapter_copy  # noqa: E402
import matrixark_mcp_summary_runtime as runtime_copy  # noqa: E402

#: The one field the live writer omits, with the reason it is recorded rather than added.
RECORDED_DIFFERENCE = {"profile_promotion_policy"}


def _widest_record(module, record_type: str) -> frozenset:
    """The largest dict literal in `module` that builds `record_type`.

    Largest, because each writer builds the record once in full and may also build narrower
    fragments; the full one is the shape that reaches the store.
    """
    tree = ast.parse(inspect.getsource(module))
    best: frozenset = frozenset()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        keys, kind = [], None
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.append(key.value)
                if key.value == "record_type" and isinstance(value, ast.Constant):
                    kind = value.value
        if kind == record_type and len(keys) > len(best):
            best = frozenset(keys)
    return best


class TheTwoSummaryWritersDifferByOneFieldTest(unittest.TestCase):

    def setUp(self) -> None:
        self.live = _widest_record(adapter_copy, "context_summary")
        self.other = _widest_record(runtime_copy, "context_summary")

    def test_both_writers_were_actually_found(self) -> None:
        """Vacuity floor. Two empty sets differ by nothing and would pass every assertion below."""
        self.assertGreater(len(self.live), 20,
                           "found %d keys in the adapter writer; the scan is not reading it"
                           % len(self.live))
        self.assertGreater(len(self.other), 20,
                           "found %d keys in the runtime writer; the scan is not reading it"
                           % len(self.other))

    def test_the_live_writer_is_the_adapter_copy(self) -> None:
        """Which copy RUNS, asked of the bound attribute rather than of the imports.

        Compared on the LAST segment, because `__module__` carries the entry style: run from the
        repository root the same function reports `tools.matrixark_local_adapter_summaries`, and
        run from `tools/` it reports `matrixark_local_adapter_summaries`. The first version of
        this asserted the bare spelling, passed where it was written and failed in CI, which is
        the very `tools.`-prefix trap the docstring above is about -- one line up from the line
        that warns about it.
        """
        import matrixark_mcp_local_adapter as adapter

        bound = getattr(adapter.MatrixArkLocalAdapter, "refresh_dirty_node_summaries", None)
        home = getattr(bound, "__module__", "")
        self.assertEqual(
            "matrixark_local_adapter_summaries", home.rpartition(".")[2],
            "the adapter now binds a different copy (%r), so the direction recorded below -- "
            "which writer is the live one -- may have inverted." % (home,))

    def test_the_difference_is_exactly_what_is_recorded(self) -> None:
        missing_from_live = self.other - self.live
        missing_from_other = self.live - self.other
        self.assertEqual(
            RECORDED_DIFFERENCE, missing_from_live,
            "the fields the LIVE summary writer omits have changed. If that set shrank, the field "
            "is now written and this record should shrink with it; if it grew, a second field has "
            "drifted apart and the two writers are diverging further.")
        self.assertEqual(
            set(), missing_from_other,
            "the live writer now carries a field the other does not, so the two are no longer a "
            "subset relationship and 'which is poorer' is no longer the right question.")


if __name__ == "__main__":
    unittest.main()
