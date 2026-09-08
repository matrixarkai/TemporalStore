# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The flush must walk every status it reads, and no more.

`pre_retrieval_idle_commit_flush` runs before every retrieve and used to walk every pipeline-task
row the index returned. On a real store that is 572 rows of which 89 are actionable: the rest carry
`pending`, `extraction_committed`, `summary_completed` or `idle_commit_skipped`, and neither loop
does anything with them.

It now filters to `IDLE_COMMIT_ACTED_ON_STATUSES` before walking. That is safe only while the
constant names every status the loops read, so this derives them from the function's own source:

  * the membership test against IDLE_COMMIT_RESOLVED_STATUSES  (loop 1, collects task hashes)
  * the equality test against IDLE_COMMIT_SCHEDULED_STATUS     (loop 2, the due candidates)

If a third status is ever read, the filter would remove those rows before the loop could see them --
no error, no failing assertion, the flush simply stops finding work. Derived, not listed: a test
repeating the four names would pass unchanged after a fifth was added.

This says nothing about what counts as RESOLVED. `idle_commit_skipped` is deliberately outside
IDLE_COMMIT_RESOLVED_STATUSES -- see the comment above that constant -- and this change does not
move it; it only stops walking rows the loops ignore.
"""
from __future__ import annotations

import ast
import os
import unittest

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(TOOLS_DIR, "matrixark_mcp_retrieve_request.py")
FUNCTION = "pre_retrieval_idle_commit_flush"
FILTER_CONSTANT = "IDLE_COMMIT_ACTED_ON_STATUSES"


def _module():
    with open(SOURCE, encoding="utf-8") as handle:
        return ast.parse(handle.read())


def _function(tree, name=FUNCTION):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _status_sources_read(fn):
    """What a RECORD's status is compared against, inside the loops over `task_records`.

    Scoped to those loops on purpose. The function also computes its own return status and compares
    that against "committed" -- unrelated to any record, and a scan matching every comparison
    containing the word "status" picks it up and reports a filter gap that does not exist. That
    false positive is why this is narrowed to the loops that read rows.
    """
    read = set()
    for loop in ast.walk(fn):
        if not isinstance(loop, ast.For):
            continue
        if not (isinstance(loop.iter, ast.Name) and loop.iter.id == "task_records"):
            continue
        for node in ast.walk(loop):
            if not isinstance(node, ast.Compare):
                continue
            if "status" not in ast.unparse(node):
                continue
            for comparator in node.comparators:
                if isinstance(comparator, ast.Name):
                    if comparator.id != FILTER_CONSTANT:
                        read.add(comparator.id)
                elif isinstance(comparator, ast.Constant) and isinstance(comparator.value, str):
                    read.add(repr(comparator.value))
    return read


class TheIdleFlushWalksWhatItReadsTest(unittest.TestCase):

    def test_the_scan_finds_the_function_and_its_status_tests(self):
        """A scan matching nothing would report the filter complete."""
        fn = _function(_module())
        self.assertIsNotNone(fn, "%s is gone; the filter it protects has no owner" % FUNCTION)
        read = _status_sources_read(fn)
        self.assertGreaterEqual(
            len(read), 2,
            "found %d status comparisons in %s, expected at least 2 (the resolved set and the "
            "scheduled status) -- the scan stopped matching, so the check below proves nothing"
            % (len(read), FUNCTION))

    def test_every_status_the_loops_read_is_in_the_filter(self):
        """The filter is built from these two names; if a third appears it must join them."""
        try:  # package path
            from tools.matrixark_mcp_retrieve_request import (
                IDLE_COMMIT_ACTED_ON_STATUSES,
                IDLE_COMMIT_RESOLVED_STATUSES,
                IDLE_COMMIT_SCHEDULED_STATUS,
            )
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_retrieve_request import (
                IDLE_COMMIT_ACTED_ON_STATUSES,
                IDLE_COMMIT_RESOLVED_STATUSES,
                IDLE_COMMIT_SCHEDULED_STATUS,
            )

        read = _status_sources_read(_function(_module()))
        known = {"IDLE_COMMIT_RESOLVED_STATUSES", "IDLE_COMMIT_SCHEDULED_STATUS"}
        unexpected = read - known
        self.assertEqual(
            set(), unexpected,
            "%s compares a status against %s, which the filter does not account for. Rows carrying "
            "it are removed before the loop can see them, and the only symptom is the flush "
            "quietly finding no work." % (FUNCTION, sorted(unexpected)))

        self.assertEqual(
            set(IDLE_COMMIT_RESOLVED_STATUSES) | {IDLE_COMMIT_SCHEDULED_STATUS},
            set(IDLE_COMMIT_ACTED_ON_STATUSES),
            "the filter set is no longer the union of the two the loops read")

    def test_the_skipped_status_is_still_not_treated_as_resolved(self):
        """Narrowing the walk must not quietly settle a status the resolved set excludes."""
        try:  # package path
            from tools.matrixark_mcp_retrieve_request import IDLE_COMMIT_RESOLVED_STATUSES
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_retrieve_request import IDLE_COMMIT_RESOLVED_STATUSES
        self.assertNotIn(
            "idle_commit_skipped", IDLE_COMMIT_RESOLVED_STATUSES,
            "idle_commit_skipped is deliberately not resolved -- see the comment above the "
            "constant. Narrowing which rows are WALKED must not change that.")

    def test_the_filter_keeps_a_scheduled_row_and_drops_an_inert_one(self):
        """The behaviour, not the spelling."""
        try:  # package path
            from tools.matrixark_mcp_retrieve_request import (
                IDLE_COMMIT_ACTED_ON_STATUSES, IDLE_COMMIT_SCHEDULED_STATUS)
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_retrieve_request import (
                IDLE_COMMIT_ACTED_ON_STATUSES, IDLE_COMMIT_SCHEDULED_STATUS)
        self.assertIn(IDLE_COMMIT_SCHEDULED_STATUS, IDLE_COMMIT_ACTED_ON_STATUSES)
        for inert in ("pending", "extraction_committed", "summary_completed"):
            self.assertNotIn(
                inert, IDLE_COMMIT_ACTED_ON_STATUSES,
                "%r is walked but neither loop reads it" % inert)


if __name__ == "__main__":
    unittest.main()
