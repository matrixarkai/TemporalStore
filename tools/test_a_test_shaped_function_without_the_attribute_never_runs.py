#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A test-shaped function with no `#[test]` above it never runs, and nothing says so.

`#[test]` is one line, it sits above the function rather than inside it, and a file missing one
still compiles, still passes, and still reports every other test as green. Nothing in the
toolchain mentions it: `cargo test` cannot report a test it was never told about, and review
reads the body, which is a perfectly good test.

Two were in that state, both in `index_log.rs`, both between neighbours that had the attribute:

  * `meta_item_without_zones_serializes_byte_identically_to_pre_fold` -- the test asserting the
    compatibility invariant that `TS_INDEX_CATALOG_FOLD`'s off side rested on. The flag was
    retired on the strength of that invariant; the test proving it had never executed once.
  * `append_delta_grows_log_by_only_the_changed_items` -- that an index-log append writes only
    its own item and that the sequence tail survives a reopen, on a durable write path.

Both pass. That is the point: nothing was broken, so nothing ever drew attention to them, and
they would have gone on covering nothing for as long as they existed.

The rule is derived rather than listed. A helper inside a test module legitimately has no
attribute -- and it gets CALLED, which is what separates it from a test. So: a function taking
no arguments and returning nothing, in a file that has tests, with no attribute above it and no
caller anywhere in the crate, is a test that does not run.

`fn main` is excluded because a binary's entry point is called by the language rather than by
any line of Rust. That is the one exemption, and it is a property of the toolchain, not a
list of this project's names.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
CRATES = os.path.join(REPO, "crates")

#: `fn name()` -- no parameters, no return arrow. The shape a `#[test]` function has.
_FN = re.compile(
    r"^(?:\s*)(?:pub(?:\([a-z]+\))?\s+)?fn\s+([a-z_][a-z0-9_]*)\s*\(\s*\)\s*\{", re.M)

#: A scan that finds nothing passes every assertion below, so both directions are floored:
#: the crate has well over a thousand attributed tests and plenty of un-attributed helpers.
EXPECTED_ATTRIBUTED_FLOOR = 500
EXPECTED_CANDIDATE_FLOOR = 20


def _rust_sources():
    for base, dirs, files in os.walk(CRATES):
        dirs[:] = [d for d in dirs if d != "target"]
        for name in sorted(files):
            if name.endswith(".rs"):
                yield os.path.join(base, name)


def _read(path):
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _scan():
    """(attributed, unattributed) test-shaped functions across the crate tree.

    `unattributed` carries (relative path, line, name) and still includes helpers; the caller
    search below is what separates a helper from a test that does not run.
    """
    attributed, unattributed = [], []
    for path in _rust_sources():
        text = _read(path)
        if "#[test]" not in text:
            continue
        lines = text.split("\n")
        for match in _FN.finditer(text):
            name = match.group(1)
            line_no = text.count("\n", 0, match.start()) + 1
            probe = line_no - 2
            has_attribute = False
            while probe >= 0:
                above = lines[probe].strip()
                if above == "" or above.startswith("//"):
                    probe -= 1
                    continue
                has_attribute = above.startswith("#[")
                break
            entry = (os.path.relpath(path, REPO).replace(os.sep, "/"), line_no, name)
            (attributed if has_attribute else unattributed).append(entry)
    return attributed, unattributed


class ATestShapedFunctionWithoutTheAttributeTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.attributed, cls.unattributed = _scan()
        cls.corpus = "\n".join(_read(path) for path in _rust_sources())

    def test_the_scan_still_finds_the_attributed_tests(self) -> None:
        """Both assertions below pass on an empty scan. If the `fn` shape or the attribute
        placement ever changes, this is what says so instead of a silent green."""
        self.assertGreaterEqual(
            len(self.attributed), EXPECTED_ATTRIBUTED_FLOOR,
            "found %d functions carrying an attribute, expected at least %d -- the scan is not "
            "matching what it used to" % (len(self.attributed), EXPECTED_ATTRIBUTED_FLOOR))

    def test_the_scan_still_finds_the_un_attributed_helpers(self) -> None:
        """The other floor. If nothing lands in the un-attributed bucket then the rule below is
        deciding nothing, and a real one would not be noticed either."""
        self.assertGreaterEqual(
            len(self.unattributed), EXPECTED_CANDIDATE_FLOOR,
            "found %d functions without an attribute, expected at least %d -- with an empty "
            "bucket the rule below cannot fail" % (len(self.unattributed),
                                                   EXPECTED_CANDIDATE_FLOOR))

    def test_every_test_shaped_function_either_runs_or_is_called(self) -> None:
        orphans = []
        for path, line_no, name in self.unattributed:
            if name == "main":
                # Called by the language, not by any line of Rust. The only exemption.
                continue
            # One occurrence is the definition itself; a helper is named again where it is used.
            if len(re.findall(r"\b%s\b" % re.escape(name), self.corpus)) <= 1:
                orphans.append("%s:%d %s" % (path, line_no, name))
        self.assertEqual(
            [], orphans,
            "these take no arguments, return nothing, and are called by nobody -- so they are "
            "tests missing `#[test]` and have never run: %s" % orphans)


if __name__ == "__main__":
    unittest.main()
