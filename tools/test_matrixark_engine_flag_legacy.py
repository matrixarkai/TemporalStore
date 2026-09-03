#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""What counts as evidence that a flag keeps an older code path alive.

The inventory's "legacy-shaped" column decides which flags are candidates for retirement. A loose
rule there does not merely mislabel a row: it puts flags on a retirement list that were never
legacy, and leaves the real ones beside them where nobody can tell which is which. It marked 11 of
98; six survive a rule that asks for evidence.

The defect was that the evidence was a mood rather than a claim. Matching "before", "previous" or
"escape hatch" anywhere in a doc comment catches every bound ever written -- "how many entries the
queue may hold BEFORE a write stops deferring" -- and every toggle. TS_META_ADMIN_TOKEN, an auth
token, and TS_MALLOC_TRIM, an allocator knob, were both filed as older code paths kept alive.
TS_WAL_LEGACY_RECOVERY was filed on the word BEFORE in a sentence about when pages are fsynced.

What marks a legacy path is a sentence saying the non-default setting RESTORES something, and that
is what is required now.
"""
from __future__ import annotations

import os
import pathlib
import re
import unittest

TOOLS = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TOOL = TOOLS / "build_engine_flag_inventory.py"
DOC = TOOLS.parent / "docs" / "ops" / "temporalstore-engine-flags.md"


def predicates():
    """The tool's own predicates, without running it.

    The script does its work at module level -- importing it regenerates the document -- so only
    the part above that work is executed. Testing a copy of the regex would pin the copy, and the
    copy is not what classifies anything.
    """
    source = TOOL.read_text(encoding="utf-8")
    head = source.split("sources = {}")[0]
    namespace: dict = {}
    exec(compile(head, str(TOOL), "exec"), namespace)  # noqa: S102 - the tool's own prelude
    return namespace


class TheEvidenceForALegacyPathTest(unittest.TestCase):

    def setUp(self) -> None:
        self.legacy = predicates()["LEGACY"]

    def _is_legacy(self, doc: str) -> bool:
        return bool(self.legacy.search(doc))

    def test_a_bound_is_not_a_legacy_path(self) -> None:
        """The false positive that classified a queue depth: "before" is how bounds are written."""
        self.assertFalse(self._is_legacy(
            "How many entries the async queue may hold before a write stops being allowed to "
            "defer it."))

    def test_an_escape_hatch_alone_is_not_a_legacy_path(self) -> None:
        """Every toggle has an escape hatch. Only some restore an older shape."""
        self.assertFalse(self._is_legacy(
            "The escape hatch exists because a trim costs a walk of the heap."))

    def test_describing_what_unset_does_is_not_a_legacy_path(self) -> None:
        """The admin token doc: about its own default, not about an older path."""
        self.assertFalse(self._is_legacy(
            "If it is not set, the surface stays open, which is the previous behavior."))

    def test_a_barrier_ordering_sentence_is_not_a_legacy_path(self) -> None:
        """What actually misclassified TS_WAL_LEGACY_RECOVERY -- the word "before", once."""
        self.assertFalse(self._is_legacy(
            "flush_shard_index fsyncs every page BEFORE advancing the watermark, and re-derives "
            "the rest on recovery."))

    def test_restoring_the_previous_behaviour_is_a_legacy_path(self) -> None:
        self.assertTrue(self._is_legacy("`0` restores the previous unbounded behaviour."))

    def test_restoring_a_named_legacy_shape_is_a_legacy_path(self) -> None:
        self.assertTrue(self._is_legacy(
            "Set to 0 to restore the legacy per-shard live set (a kill-switch only)."))

    def test_restoring_a_described_older_behaviour_is_a_legacy_path(self) -> None:
        """The hatch that restores growing appends: no legacy noun, still a legacy path."""
        self.assertTrue(self._is_legacy(
            "The env var remains the escape hatch (=0 restores growing appends)."))

    def test_writing_them_as_they_were_is_a_legacy_path(self) -> None:
        self.assertTrue(self._is_legacy(
            "Write byte payloads as an array of numbers, as they were written before."))


class TheDocMustBeAboutTheFlagTest(unittest.TestCase):
    """A doc comment above a function reading several flags is evidence about none of them.

    This affects no flag today: nine functions read more than one flag and not one of them carries
    a doc comment, so the inventory reports zero. The rule is driven directly here rather than
    through that zero, because an assertion resting on a count of zero passes most easily when the
    mechanism producing it has stopped working.
    """

    def setUp(self) -> None:
        self.ns = predicates()

    def _lines(self, *flags: str):
        body = ["/// Something about the first one.", "fn under_test() -> bool {"]
        body += ['    let _ = std::env::var("%s");' % f for f in flags]
        body += ["    true", "}", "fn after() {}"]
        return body

    def test_a_lone_flag_keeps_the_doc_above_its_function(self) -> None:
        doc, shared = self.ns["doc_for_flag"](
            "TS_ONE", "Something about the first one.", self._lines("TS_ONE"), 1)
        self.assertEqual("Something about the first one.", doc)
        self.assertEqual([], shared)

    def test_a_doc_shared_by_two_flags_is_credited_to_neither(self) -> None:
        doc, shared = self.ns["doc_for_flag"](
            "TS_ONE", "Something about the first one.", self._lines("TS_ONE", "TS_TWO"), 1)
        self.assertEqual("", doc, "this comment was credited to a flag it never mentions")
        self.assertEqual(["TS_TWO"], shared)

    def test_a_doc_that_names_the_flag_is_kept_even_when_shared(self) -> None:
        """Naming the flag is the author saying which one the sentence is about."""
        doc, shared = self.ns["doc_for_flag"](
            "TS_ONE", "TS_ONE decides the shape.", self._lines("TS_ONE", "TS_TWO"), 1)
        self.assertEqual("TS_ONE decides the shape.", doc)
        self.assertEqual([], shared)

    def test_function_end_finds_the_closing_brace(self) -> None:
        lines = [
            "fn outer() -> bool {",
            '    let a = std::env::var("TS_ONE");',
            "    if a.is_ok() {",
            "        return true;",
            "    }",
            "    false",
            "}",
            '    let _ = std::env::var("TS_ELSEWHERE");',
        ]
        self.assertEqual(7, self.ns["function_end"](lines, 0),
                         "the span ran past the function, so a flag read AFTER it would count as "
                         "sharing the doc comment of this one")


class TheInventoryReportsWhatItMeasuredTest(unittest.TestCase):

    def setUp(self) -> None:
        self.text = DOC.read_text(encoding="utf-8")
        self.rows = [line for line in self.text.splitlines() if re.match(r"^\| `TS_", line)]

    def test_a_flag_is_not_marked_legacy_on_a_barrier_sentence(self) -> None:
        row = [line for line in self.rows if line.startswith("| `TS_WAL_LEGACY_RECOVERY`")]
        self.assertTrue(row, "the flag is missing from the inventory entirely")
        self.assertNotEqual("yes", row[0].split("|")[4].strip(),
                            "marked as keeping an older path alive, on a doc comment that "
                            "describes when pages are fsynced and nothing else")

    def test_the_count_matches_the_rows(self) -> None:
        marked = [r for r in self.rows if r.split("|")[4].strip() == "yes"]
        stated = re.search(r"documented as keeping an older path alive \| (\d+) \|", self.text)
        self.assertIsNotNone(stated, "the summary no longer states the count")
        self.assertEqual(int(stated.group(1)), len(marked),
                         "the summary says %s and %d rows are marked"
                         % (stated.group(1), len(marked)))

    def test_a_meaningful_number_of_flags_are_not_legacy(self) -> None:
        """If nearly everything is legacy-shaped the column has stopped distinguishing."""
        marked = [r for r in self.rows if r.split("|")[4].strip() == "yes"]
        self.assertLess(len(marked), len(self.rows) // 4,
                        "%d of %d flags are marked legacy-shaped, which reads as a rule matching "
                        "prose rather than a claim about code paths"
                        % (len(marked), len(self.rows)))


if __name__ == "__main__":
    unittest.main()
