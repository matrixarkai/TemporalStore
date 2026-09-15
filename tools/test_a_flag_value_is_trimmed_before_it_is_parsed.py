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

The scan that found those sixteen then reported a clean tree for months while forty-three
untrimmed reads sat in it, because it required the turbofish: `.parse::<usize>()`. A reader
whose type comes from its own signature writes `.parse()`, and fourteen of them did --

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
    }

-- one such helper in each of seven files, carrying 93 distinct flags between them, plus
`TS_CACHE_MEMORY_BYTES`, `TS_SERVER_NODE_ID`, `TS_SERVER_HEARTBEAT_INTERVAL_MS`, `TS_SHARD_ID`
and four `MATRIXARK_BACKFILL_*` sizes read inline the same way. The second hole was the type
itself: `[a-z0-9]+` cannot spell `DataRaftReadMode`, and that read does not fall back quietly --
`TS_DATA_RAFT_READ_MODE="leader "` reached `panic!("invalid TS_DATA_RAFT_READ_MODE")` and took
the data node down at startup.

So the floor below is not the only thing that keeps this scan honest, and it never was: the
floor was met the whole time. `test_the_inferred_type_shape_is_in_scope` is the part that
matters, because it fails if the shape this scan can see is ever narrowed back.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
SRC = os.path.join(REPO, "crates", "temporalstore-rust", "src")

#: `env::var("NAME") ... .parse()`, with whatever the chain does in between. The window is
#: bounded by `;` and `}` so it cannot run past the end of the statement -- or out of the
#: function -- into an unrelated parse, a doc comment included.
#:
#: The turbofish is OPTIONAL and the type inside it is not restricted to lowercase. Both were
#: required once, and between them they hid every reader below: a helper takes its type from its
#: own return type and writes a bare `.parse()`, and an enum flag spells its own type name.
READ = re.compile(
    r'env::var(?:_os)?\(\s*(?:&)?(?:"(?P<lit>[A-Z][A-Z0-9_]*)"|(?P<konst>[A-Za-z_:]+))\s*\)'
    r'(?P<tail>(?:[^;}]{0,400}?))\.parse(?:::<(?P<ty>[^<>]{1,80})>)?\s*\(\)',
    re.S)

#: A scan that finds nothing passes every rule below, so the corpus is floored. This floor was
#: met -- 36 reads, all trimmed -- while 43 more sat outside the shape the scan could see, which
#: is why `test_the_inferred_type_shape_is_in_scope` exists alongside it.
EXPECTED_READER_FLOOR = 60

#: Of those, how many must be reads whose type the compiler infers (no turbofish). Narrowing the
#: regex back to `.parse::<t>()` takes this to zero, which is exactly the regression to catch.
EXPECTED_INFERRED_FLOOR = 25


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
                   match.group("ty") or "inferred",
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

    def test_the_inferred_type_shape_is_in_scope(self) -> None:
        """The hole, held open on purpose.

        Requiring `.parse::<T>()` is a detector asking IS where it means CONTAINS, and it reads
        exactly like a clean tree: the floor above was satisfied by 36 trimmed reads while 43
        untrimmed ones were invisible. A reader that writes `.parse()` and lets its signature
        supply the type is the common shape here -- one per numeric helper -- so if none are
        being found, the scan has been narrowed back and is deciding nothing about them.
        """
        inferred = [r for r in self.reads if r[2] == "inferred"]
        self.assertGreaterEqual(
            len(inferred), EXPECTED_INFERRED_FLOOR,
            "found %d flag reads whose parse type is inferred, expected at least %d -- the scan "
            "no longer sees the turbofish-less shape, which is the one that hid 43 untrimmed "
            "reads" % (len(inferred), EXPECTED_INFERRED_FLOOR))

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
