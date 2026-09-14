#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A flag reader takes the whole word list, or it takes none of it.

`test_no_flag_reader_decides_by_case` keeps `"TRUE"` out of the crate, and it works: no reader
spells an upper-case boolean word any more. But a reader can fold case correctly and still be
wrong, by accepting a SHORTER list than the one the operator was told about. Five did:

    wal.rs           TS_WAL_DATA_ONLY                    off-set was `0`, `false`
    model_provider   MATRIXARK_REQUIRE_MODEL_SUMMARIES   on-set was `1`, `true`
    model_provider   MATRIXARK_REQUIRE_MODEL_EMBEDDINGS  on-set was `1`, `true`
    engine/context   MATRIXARK_CONTEXT_COMPRESSION_ENABLED  (a local `env_bool` shadow)
    embed_drainer    MATRIXARK_EMBED_DRAINER                (the same shadow, copied)

Measured against `crate::env_flag::parse_bool` over 30 values: the four default-off readers
answered `false` for `yes`, `YES`, `on`, `ON`, `On`, `" 1"`, `"1 "`, `" on "` and `"\\ttrue\\n"`
-- nine ways of writing "turn this on" that turned nothing on. `TS_WAL_DATA_ONLY`, whose default
is on and whose variable opts OUT, answered `true` for `no`, `NO`, `off`, `OFF`, `Off`, `" 0"`
and `"0 "`. Its own neighbour sixty lines below, `wal_outcome_items_enabled`, already took the
whole off-set.

Two of the five were a private `fn env_bool` shadowing the crate's. Those did something worse
than drop words: they answered `false` for anything unrecognised instead of returning the
default they had been handed, which is the failure `env_flag.rs` was written to end.

The rule here is not "call parse_bool". Fourteen readers spell the vocabulary inline and are
right to; `control.rs` deliberately also takes `enabled` and `raft.rs` takes `y`/`n`, and a rule
demanding one call site would either forbid those or need an allowlist that rots. The rule is:
whichever half of the vocabulary a reader uses, it takes ALL of that half, and it trims first.

A note on the scan. The obvious window -- everything from `env::var(` to the next `;` -- is
wrong, and wrong silently. A rust function whose last expression IS the env read carries no
semicolon, so the window runs past the closing brace and swallows the NEXT read. That is how
`MATRIXARK_REQUIRE_MODEL_EMBEDDINGS` stayed invisible while its identical twin eight lines above
it was found. Forty-two of the crate's reads sit in that position. `_window` below ends at the
first of `;`, a `\\n}` at column 0, or the next `env::var`, and `test_the_window_does_not_swallow
_the_next_read` pins that on a fixture rather than on the tree.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
SRC = os.path.join(REPO, "crates", "temporalstore-rust", "src")

VAR = re.compile(
    r'env::var(?:_os)?\(\s*(?:&)?(?:"(?P<lit>[A-Z][A-Z0-9_]*)"|(?P<konst>[A-Za-z_:]+))\s*\)')

#: A boolean word written as a string literal. Lower-case only: an upper-case spelling is
#: `test_no_flag_reader_decides_by_case`'s business, not this file's.
WORD = re.compile(r'"(1|0|true|yes|no|on|off|false)"')

ON = frozenset(("1", "true", "yes", "on"))
OFF = frozenset(("0", "false", "no", "off"))

#: Every assertion below passes on an empty scan, so all three populations are floored.
SOURCE_FLOOR = 100
READ_FLOOR = 120
DECISION_FLOOR = 10


def _sources():
    for base, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d != "target"]
        for name in sorted(files):
            if name.endswith(".rs"):
                yield os.path.join(base, name)


def _window(text, start):
    """From just after an `env::var(...)` call to the end of its statement or item.

    Bounded by the FIRST of `;`, a `\\n}` at column 0, or the next `env::var`. A `;`-only
    bound runs past a semicolon-less function tail into the next item; see the docstring.
    """
    ends = []
    for found in (text.find(";", start), text.find("\n}", start)):
        if found != -1:
            ends.append(found)
    following = VAR.search(text, start + 1)
    if following:
        ends.append(following.start())
    return text[start:min(ends)] if ends else text[start:start + 700]


def _in_test_module(head):
    return "#[cfg(test)]" in head and head.rfind("#[cfg(test)]") > head.rfind("\n}\n")


def _scan():
    """(path, line, flag, trims, on_words, off_words) for each hand-rolled boolean decision."""
    sources = list(_sources())
    reads = 0
    decisions = []
    for path in sources:
        with io.open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
        for match in VAR.finditer(text):
            reads += 1
            tail = _window(text, match.end())
            words = {found.lower() for found in WORD.findall(tail)}
            if not words & (ON | OFF):
                continue
            if "parse_bool" in tail or "env_bool" in tail:
                continue
            if _in_test_module(text[:match.start()]):
                continue
            decisions.append((
                os.path.relpath(path, REPO), text.count("\n", 0, match.start()) + 1,
                match.group("lit") or match.group("konst"),
                ".trim()" in tail, words & ON, words & OFF))
    return sources, reads, decisions


class AFlagReaderTakesTheWholeWordList(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sources, cls.reads, cls.decisions = _scan()

    def test_the_scan_still_sees_the_crate(self) -> None:
        """A floor on the file corpus. Every rule below passes on an empty scan."""
        self.assertGreaterEqual(
            len(self.sources), SOURCE_FLOOR,
            "only %d rust sources under %s; below %d the rules below cannot fail"
            % (len(self.sources), SRC, SOURCE_FLOOR))

    def test_the_scan_still_sees_the_flag_reads(self) -> None:
        """A floor on the reads, not just the files. A regex that stops matching reads clean."""
        self.assertGreaterEqual(
            self.reads, READ_FLOOR,
            "only %d env::var reads found; the pattern has stopped matching" % self.reads)

    def test_the_scan_still_sees_hand_rolled_decisions(self) -> None:
        """And a floor on the population this file is actually about."""
        self.assertGreaterEqual(
            len(self.decisions), DECISION_FLOOR,
            "only %d hand-rolled boolean decisions found. If the crate really has adopted "
            "parse_bool everywhere, lower this floor deliberately -- do not let a scan that "
            "found nothing pass as a crate that has nothing wrong." % len(self.decisions))

    def test_every_reader_takes_the_whole_half_it_uses(self) -> None:
        """The rule. Not which words: how many of the ones it has already started spelling."""
        for path, line, flag, _trims, on_words, off_words in self.decisions:
            if on_words:
                with self.subTest(flag=flag, half="on"):
                    self.assertEqual(
                        ON, frozenset(on_words),
                        "%s:%d reads %s with the on-words %s. The operator was told the "
                        "vocabulary is %s, and the words this reader drops are ones written to "
                        "turn something ON -- so it fails in the direction that looks like "
                        "nothing happening." % (path, line, flag, sorted(on_words), sorted(ON)))
            if off_words:
                with self.subTest(flag=flag, half="off"):
                    self.assertEqual(
                        OFF, frozenset(off_words),
                        "%s:%d reads %s with the off-words %s, not %s"
                        % (path, line, flag, sorted(off_words), sorted(OFF)))

    def test_every_reader_trims_before_it_decides(self) -> None:
        """A unit file, a heredoc and a shell export all leave whitespace behind."""
        for path, line, flag, trims, _on, _off in self.decisions:
            with self.subTest(flag=flag):
                self.assertTrue(
                    trims, "%s:%d decides %s without trimming, so `\" 1\"` misses the "
                    "vocabulary entirely" % (path, line, flag))

    def test_no_second_env_bool_spells_its_own_vocabulary(self) -> None:
        """The shadow, kept out by name.

        A private `fn env_bool` is fine as a local name for the crate's. It is not fine as a
        second implementation: the two that existed answered `false` for an unrecognised value
        instead of returning the default they were handed.
        """
        canonical = os.path.join(SRC, "env_flag.rs")
        found = 0
        for path in self.sources:
            if os.path.abspath(path) == os.path.abspath(canonical):
                continue
            with io.open(path, encoding="utf-8", errors="replace") as handle:
                text = handle.read()
            for match in re.finditer(r'fn env_bool(?:_any)?\s*\(', text):
                found += 1
                body = text[match.end():match.end() + 500]
                with self.subTest(path=os.path.relpath(path, REPO)):
                    self.assertTrue(
                        "env_flag::env_bool" in body or "parse_bool" in body,
                        "%s:%d defines env_bool without going through env_flag. A second "
                        "function with the canonical name and a different answer is the "
                        "hardest kind of divergence to see from a call site."
                        % (os.path.relpath(path, REPO),
                           text.count("\n", 0, match.start()) + 1))
        self.assertGreaterEqual(
            found, 1,
            "no env_bool definitions found outside env_flag.rs at all -- the rule above "
            "cannot fail, so check the pattern before trusting it")

    def test_the_window_does_not_swallow_the_next_read(self) -> None:
        """The detector's own bug, pinned on a fixture rather than on the tree.

        A `;`-only window runs past a semicolon-less function tail and eats the next read.
        Asserting this against the crate would only confirm whatever the crate happens to
        contain today.
        """
        fixture = (
            'fn first() -> bool {\n'
            '    std::env::var("FLAG_ONE")\n'
            '        .ok()\n'
            '        .map(|v| v == "1")\n'
            '        .unwrap_or(false)\n'
            '}\n'
            '\n'
            'fn second() -> bool {\n'
            '    let raw = std::env::var("FLAG_TWO").ok();\n'
            '    raw.map(|v| v == "yes").unwrap_or(false)\n'
            '}\n')
        found = [m.group("lit") for m in VAR.finditer(fixture)]
        self.assertEqual(["FLAG_ONE", "FLAG_TWO"], found)

        naive = fixture[fixture.index('"FLAG_ONE"'):fixture.index(";", fixture.index("FLAG_ONE"))]
        self.assertIn("FLAG_TWO", naive,
                      "the fixture no longer reproduces the swallow, so the check below proves "
                      "nothing -- first() must end on an expression with no semicolon")

        start = fixture.index(")", fixture.index('"FLAG_ONE"')) + 1
        self.assertNotIn("FLAG_TWO", _window(fixture, start),
                         "_window swallowed the following read; every flag after a "
                         "semicolon-less function tail would be invisible to this file")


if __name__ == "__main__":
    unittest.main()
