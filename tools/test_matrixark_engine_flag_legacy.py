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
ROOT_PATH = TOOLS.parent
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


class TheDefaultOfAnInlineReadTest(unittest.TestCase):
    """`default_of_statement` reads a default off a flag read that has no accessor of its own.

    `default_of` needs a one-flag function returning bool, so a read in the middle of a larger
    function reported nothing -- and the summary row that matters most, flags defaulting ON that
    nothing sets, counted zero while `MATRIXARK_RUST_PROXY_ASYNC_CACHE_WARM_ON_LOAD` sat inline,
    defaulting on, set by nothing. It was not that there were none; it was that the scan could
    only see one shape.

    The unit is the STATEMENT and not a window of lines, and these pin why: the `!` that inverts
    `!matches!(env::var(X)...)` sits above the name, so a window has to guess how far up to look
    while a statement contains it by construction.
    """

    def setUp(self) -> None:
        self.ns = predicates()

    def _read(self, source: str) -> str:
        lines = source.split("\n")
        read = next(i for i, line in enumerate(lines) if "env::var(" in line)
        return self.ns["default_of_statement"](lines, read)

    def test_a_negated_matches_reads_as_on(self) -> None:
        """The shape that a line window gets wrong: the `!` is three lines above the name."""
        self.assertEqual("on", self._read(
            'let warm = !matches!(\n'
            '    env::var("TS_ONE")\n'
            '        .unwrap_or_default()\n'
            '        .as_str(),\n'
            '    "0" | "false"\n'
            ');'))

    def test_a_bare_matches_reads_as_off(self) -> None:
        self.assertEqual("off", self._read(
            'let gate = matches!(\n'
            '    env::var("TS_ONE").unwrap_or_default().as_str(),\n'
            '    "1" | "true"\n'
            ');'))

    def test_an_unwrap_or_true_reads_as_on(self) -> None:
        self.assertEqual("on", self._read(
            'let scoring = env::var("TS_ONE")\n'
            '    .map(|v| matches!(v.as_str(), "1"))\n'
            '    .unwrap_or(true);'))

    def test_a_string_fallback_reads_as_on(self) -> None:
        """`unwrap_or_else(|_| "1")` states the default as the value, not as a bool."""
        self.assertEqual("on", self._read(
            'let warm = !matches!(\n'
            '    env::var("TS_ONE").unwrap_or_else(|_| "1".to_string()).as_str(),\n'
            '    "0" | "false"\n'
            ');'))

    def test_a_millisecond_count_states_no_default(self) -> None:
        """A number has no ON/OFF, and reporting one would be read as fact."""
        self.assertEqual("", self._read(
            'let timeout = env::var("TS_ONE")\n'
            '    .ok()\n'
            '    .and_then(|v| v.parse::<u64>().ok())\n'
            '    .unwrap_or(5_000);'))

    def test_a_statement_reading_two_flags_states_no_default(self) -> None:
        """Two flags in one statement have no single default to attribute to either."""
        self.assertEqual("", self._read(
            'let both = matches!(env::var("TS_ONE").unwrap_or_default().as_str(), "1")\n'
            '    || matches!(env::var("TS_TWO").unwrap_or_default().as_str(), "1");'))

    def test_it_does_not_reach_past_the_statement(self) -> None:
        """A default belonging to the NEXT statement is not this one's.

        The first statement is deliberately boolean-shaped and default-less, so that if the scan
        ran past the `;` the `unwrap_or(true)` below would answer "on" -- which is the failure
        this exists to catch. A non-boolean first statement would hide behind the shape check
        instead of testing the bound.
        """
        self.assertEqual("", self._read(
            'let named = matches!(env::var("TS_ONE").ok().as_deref(), Some("x"));\n'
            'let other = std::env::args().next().is_some();\n'
            'let third = other.then_some(false).unwrap_or(true);'))


class TheInventoryReportsWhatItMeasuredTest(unittest.TestCase):

    def setUp(self) -> None:
        self.text = DOC.read_text(encoding="utf-8")
        # All three prefixes. Matching `TS_` alone made every marked MATRIXARK_ or
        # TEMPORALSTORE_ flag invisible here, so `test_the_count_matches_the_rows` compared the
        # summary against a subset and reported the document as inconsistent with itself.
        self.rows = [line for line in self.text.splitlines()
                     if re.match(r"^\| `(TS|MATRIXARK|TEMPORALSTORE)_", line)]
        self.legacy_column = self._column_index("keeps an older path")

    def _column_index(self, heading: str) -> int:
        """Which cell of a row holds `heading`, found by reading the header.

        This used to be the literal 4, and adding a column to the table moved the legacy flag
        under it: every assertion here then read the new column instead, and "0 rows are marked"
        is a passing-looking answer to the wrong question in two of the three tests below.
        """
        for line in self.text.splitlines():
            if line.startswith("| flag ") and heading in line:
                cells = line.split("|")
                for index, cell in enumerate(cells):
                    if heading in cell:
                        return index
        raise AssertionError("no table header carries a %r column" % heading)

    def _marked(self):
        return [r for r in self.rows if r.split("|")[self.legacy_column].strip() == "yes"]

    def test_the_legacy_mark_on_the_recovery_hatch_is_backed_by_its_own_doc(self) -> None:
        """The mark has to come from a restore-claim, not from a sentence about fsync ordering.

        This test used to assert the row was NOT marked, and that was right while the doc the
        generator credited to `TS_WAL_LEGACY_RECOVERY` was block_store's -- which says when pages
        are fsynced and nothing else, a fact about ordering rather than about a path kept alive.

        Consolidating that flag's readers into one moved the credited doc to the engine's, which
        says the non-default setting "falls back to the legacy multi-barrier write path" and
        exists "so an operator can revert the default in the field". That is exactly the claim
        this column is about, so the mark is now earned rather than inferred.

        Pinning the answer would make this test fail for a correct change -- as it did. So it pins
        the REASON: however the row reads, the flag's own doc must agree.
        """
        source = (ROOT_PATH / "crates" / "temporalstore-rust" / "src" / "engine.rs").read_text(
            encoding="utf-8")
        lines = source.splitlines()
        index = next(
            (i for i, line in enumerate(lines)
             if line.lstrip().endswith("fn wal_legacy_recovery() -> bool {")), None)
        self.assertIsNotNone(
            index, "no reader for the hatch in engine.rs; this test lost its subject")
        doc = []
        for back in range(index - 1, -1, -1):
            stripped = lines[back].lstrip()
            if not stripped.startswith("///"):
                break
            doc.insert(0, stripped[3:].strip())
        self.assertTrue(doc, "the hatch's reader carries no doc comment to judge")

        # The flag's own NAME carries the word "legacy", so it comes out of the prose before the
        # prose is asked whether it claims one. Leaving it in makes this check inert: `restores`
        # could not be false for a flag called TS_WAL_LEGACY_RECOVERY whatever its doc said.
        prose = " ".join(doc).lower().replace("ts_wal_legacy_recovery", "the hatch")
        restores = any(word in prose for word in ("falls back", "revert", "restores"))

        row = [line for line in self.rows if line.startswith("| `TS_WAL_LEGACY_RECOVERY`")]
        self.assertTrue(row, "the flag is missing from the inventory entirely")
        marked = row[0].split("|")[self.legacy_column].strip() == "yes"
        self.assertEqual(
            restores, marked,
            "the row says marked=%s while the flag's own doc says restores=%s: %s"
            % (marked, restores, prose[:200]))

    def test_the_count_matches_the_rows(self) -> None:
        marked = self._marked()
        stated = re.search(r"documented as keeping an older path alive \| (\d+) \|", self.text)
        self.assertIsNotNone(stated, "the summary no longer states the count")
        self.assertEqual(int(stated.group(1)), len(marked),
                         "the summary says %s and %d rows are marked"
                         % (stated.group(1), len(marked)))

    def test_a_meaningful_number_of_flags_are_not_legacy(self) -> None:
        """If nearly everything is legacy-shaped the column has stopped distinguishing."""
        marked = self._marked()
        self.assertLess(len(marked), len(self.rows) // 4,
                        "%d of %d flags are marked legacy-shaped, which reads as a rule matching "
                        "prose rather than a claim about code paths"
                        % (len(marked), len(self.rows)))


class TheDefaultOfAHelperCallTest(unittest.TestCase):
    """A reader can state its default in an argument, or in the helper's own name.

    Both shapes were invisible until the portal refused to offer a knob whose default nothing
    could derive -- the check that an offered knob shows a number the source agrees with is only
    as good as the shapes this can read.
    """

    @staticmethod
    def _default_of(body: str) -> str:
        return predicates()["default_of"](body, 1)

    def test_env_bool_states_its_default_in_an_argument(self) -> None:
        self.assertEqual("off", self._default_of(
            'fn f() -> bool {\n    env_bool("MATRIXARK_X", false)\n}'))
        self.assertEqual("on", self._default_of(
            'fn f() -> bool {\n    env_bool("MATRIXARK_X", true)\n}'))

    def test_env_flag_on_is_the_default_off_reader(self) -> None:
        self.assertEqual("off", self._default_of(
            'fn f() -> bool {\n    env_flag_on("TS_X")\n}'))

    def test_env_flag_default_on_is_not_read_as_its_opposite(self) -> None:
        """The two helper names differ by one word, and one contains no substring of the other."""
        self.assertEqual("on", self._default_of(
            'fn f() -> bool {\n    env_flag_default_on("TS_X")\n}'))
        self.assertEqual("on", self._default_of(
            'fn f() -> bool {\n    wal_env_flag_default_on("TS_X")\n}'))

    def test_a_function_reading_two_flags_still_states_nothing(self) -> None:
        self.assertEqual("", predicates()["default_of"](
            'fn f() -> bool {\n    env_flag_on("TS_A") && env_flag_on("TS_B")\n}', 2))


if __name__ == "__main__":
    unittest.main()
