#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A flag value is trimmed before it is parsed, and never handed to Rust's `bool` parser.

The numeric twin of the defect `test_no_flag_reader_decides_by_case.py` records. A shell
export, a systemd `Environment=` line and a heredoc all leave whitespace behind, and
`" 64".parse::<usize>()` is an `Err` -- so the reader falls back to its default and the value
the operator set is discarded with no message at all. Sixteen readers were in that state,
including three on serving paths: `MATRIXARK_RUST_PROXY_CACHE_BYTES`,
`MATRIXARK_RETRIEVAL_TRAVERSAL_TOP_K` and `MATRIXARK_RETRIEVAL_MAX_CANDIDATES`.

`parse::<bool>()` is the same failure, at its worst. `bool::from_str` accepts exactly two
strings, `"true"` and `"false"`. `MATRIXARK_RUST_PROXY_ASYNC_STORAGE` was read that way while
the comment directly above it said the switch was "opt-in only, via an explicit **truthy**"
value -- so `1`, `on`, `yes` and `TRUE` all read as false. Every launcher in the tree happens to
write the literal `true`, which is why nothing ever noticed.

Both rules are about the value, not the vocabulary, so neither dictates which words a flag
accepts -- `crate::env_flag::parse_bool` decides that.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
SRC = os.path.join(REPO, "crates", "temporalstore-rust", "src")

#: `env::var("NAME") ... .parse::<T>()`, with whatever the chain does in between. The window is
#: bounded by `;` so it cannot run past the end of the statement into an unrelated parse.
READ = re.compile(
    r'env::var(?:_os)?\(\s*(?:&)?(?:"(?P<lit>[A-Z][A-Z0-9_]*)"|(?P<konst>[A-Za-z_:]+))\s*\)'
    r'(?P<tail>(?:[^;]{0,400}?))\.parse::<(?P<ty>[a-z0-9]+)>\(\)',
    re.S)

#: A scan that finds nothing passes both rules below, so the corpus is floored.
EXPECTED_READER_FLOOR = 15


def _sources():
    for base, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d != "target"]
        for name in sorted(files):
            if name.endswith(".rs"):
                yield os.path.join(base, name)


def _read(path):
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _parsed_flag_reads():
    """(where, flag, type, trims) for every env value the crate parses."""
    for path in _sources():
        text = _read(path)
        for match in READ.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            yield ("%s:%d" % (os.path.relpath(path, REPO).replace(os.sep, "/"), line),
                   match.group("lit") or match.group("konst"),
                   match.group("ty"),
                   ".trim()" in match.group("tail"))


class AFlagValueIsTrimmedBeforeItIsParsedTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.reads = list(_parsed_flag_reads())

    def test_the_scan_still_finds_the_readers(self) -> None:
        """Both rules pass on an empty scan, which would read exactly like compliance."""
        self.assertGreaterEqual(
            len(self.reads), EXPECTED_READER_FLOOR,
            "found %d parsed flag reads, expected at least %d -- if the chain shape changed, "
            "the rules below are deciding nothing" % (len(self.reads), EXPECTED_READER_FLOOR))

    def test_every_parsed_flag_value_is_trimmed_first(self) -> None:
        untrimmed = ["%s  %s (%s)" % (where, flag, ty)
                     for where, flag, ty, trims in self.reads if not trims]
        self.assertEqual(
            [], untrimmed,
            "these parse a flag value without trimming it, so a value with surrounding "
            "whitespace is discarded silently and the default applies instead: %s" % untrimmed)

    def test_no_flag_is_handed_to_the_rust_bool_parser(self) -> None:
        """`bool::from_str` accepts `"true"` and `"false"` and nothing else -- not `1`, not `on`,
        not `TRUE`. A flag wants `crate::env_flag::parse_bool`."""
        offenders = []
        for path in _sources():
            text = _read(path)
            for found in re.finditer(r"parse::<bool>\(\)", text):
                line = text.count("\n", 0, found.start()) + 1
                offenders.append("%s:%d" % (
                    os.path.relpath(path, REPO).replace(os.sep, "/"), line))
        self.assertEqual(
            [], offenders,
            "these ask rust's bool parser what a flag means, so only the exact strings true "
            "and false do anything. Call crate::env_flag::parse_bool: %s" % offenders)


if __name__ == "__main__":
    unittest.main()
