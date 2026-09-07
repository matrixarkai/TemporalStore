#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A flag reader that spells `"TRUE"` has forgotten to lowercase what it read.

`tools/test_env_flag_vocabulary.py` settled this for the python side and recorded why: "Boolean
flags were parsed in six different vocabularies. They disagreed on the two words an operator is
most likely to reach for." The rust side had the same defect and no guard, so it kept it.

Nine readers -- in `metaserver`, `server`, `raft_node`, `storage_backend`, the proxy's
single-shot debug switch, `env_bool_any`, the scale harness and four benchmark knobs -- matched
the value against `"1" | "true" | "TRUE" | "yes" | "YES"`. Spelling both cases of a word is only
ever necessary because the value was never lowercased, and a list of cases is never complete.
Measured, against a flag whose default is `true`:

    "on" -> false      "On" -> false     "True" -> false
    " 1" -> false      "wat" -> false      "" -> false

Every default-on flag in the tree reached one of them, including
`TS_META_AUTO_REBALANCE_BALANCE` and `TS_RAFT_ALLOW_PLAINTEXT`. Writing `on` to keep one of
those on turned it off, and an unreadable value turned it off rather than leaving it at its
default.

This does not dictate which words a reader accepts -- `control.rs` deliberately also takes
`enabled`, and `raft.rs` takes `y`/`n`. It says only that a reader must not decide by case,
which `crate::env_flag::parse_bool` handles for anyone who calls it.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
SRC = os.path.join(REPO, "crates", "temporalstore-rust", "src")
CANONICAL = os.path.join(SRC, "env_flag.rs")

#: An upper-case spelling of a boolean word, as a string literal.
UPPER_WORD = re.compile(r'"(?:TRUE|FALSE|YES|NO|ON|OFF)"')

#: Both assertions below pass on an empty scan, so the corpus is floored.
EXPECTED_SOURCE_FLOOR = 100


def _sources():
    for base, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d != "target"]
        for name in sorted(files):
            if name.endswith(".rs"):
                yield os.path.join(base, name)


def _read(path):
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _offenders():
    found = []
    for path in _sources():
        if os.path.abspath(path) == os.path.abspath(CANONICAL):
            continue
        for number, line in enumerate(_read(path).split("\n"), 1):
            # Only an ALTERNATION arm decides by case. A test naming the spellings it expects a
            # reader to accept lists them in an array, with no `|` -- and is asserting the very
            # property this guard exists to keep.
            if "|" not in line:
                continue
            hit = UPPER_WORD.search(line)
            if hit:
                found.append("%s:%d %s" % (
                    os.path.relpath(path, REPO).replace(os.sep, "/"), number, hit.group(0)))
    return found


class NoFlagReaderDecidesByCaseTest(unittest.TestCase):

    def test_the_scan_still_sees_the_crate(self) -> None:
        """The rule below passes on an empty corpus, which would read exactly like compliance."""
        sources = list(_sources())
        self.assertGreaterEqual(
            len(sources), EXPECTED_SOURCE_FLOOR,
            "found %d rust sources under %s, expected at least %d -- with an empty corpus the "
            "rule below cannot fail" % (len(sources), SRC, EXPECTED_SOURCE_FLOOR))

    def test_the_canonical_reader_is_where_the_vocabulary_lives(self) -> None:
        self.assertTrue(os.path.exists(CANONICAL), "crates/.../src/env_flag.rs is gone")
        text = _read(CANONICAL)
        for name in ("pub fn parse_bool", "pub fn env_bool"):
            self.assertIn(name, text, "env_flag.rs no longer offers %s" % name)

    def test_no_reader_spells_an_upper_case_boolean_word(self) -> None:
        self.assertEqual(
            [], _offenders(),
            "these decide a boolean by case, which is how the tree ended up with readers that "
            "disagreed about `on`. Call crate::env_flag::parse_bool, which lowercases and trims "
            "first: %s" % _offenders())


if __name__ == "__main__":
    unittest.main()
