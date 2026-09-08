#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Both languages read the same values of `TS_META_ADDR` as "no metaserver".

The rust side calls itself the one implementation of that rule:

    /// This is the one implementation of that rule. The datanode decides whether to
    /// register with a metaserver from it ... A second copy would let the name in the log
    /// drift away from the topology it claims to describe, which is the failure this
    /// function exists to make impossible.

There is a second copy. `matrixark_deployment_plan.META_SENTINELS` is the same five values in
python, and until this file nothing compared them -- the python test checked that list against
itself, and no test in `tools/` mentioned the rust constant at all.

Drift is not hypothetical here; it already happened in the other direction.
`matrixark_rust_proxy_impl::open_remote_store` re-derived the rule with `is_empty()` and got a
narrower answer, so a one-box (which sets `TS_META_ADDR=local`) handed `"local"` to the client as
a literal socket address:

    record_log_remote_execute_failed: http error: io error: invalid socket address

Every write failed -- 123,435 of them in one soak -- while the sample column read all zeros. That
call site now routes through `single_node()`. This checks the remaining copy, the one in another
language, where the compiler cannot help.

Normalisation is part of the rule, not decoration: rust compares
`value.trim().to_ascii_lowercase()` and python compares `_clean(...).lower()`, so `" Local "` is
a sentinel on both sides. A change to either list alone, or to either normalisation alone, fails
this.
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
RUST = os.path.join(REPO, "crates", "temporalstore-rust", "src", "storage_backend.rs")

sys.path.insert(0, TOOLS)

import matrixark_deployment_plan as plan  # noqa: E402

#: `const META_ADDR_SENTINELS: [&str; 5] = ["", "local", ...];` — the declared length is captured
#: too, so a list that grew without its annotation being updated is caught rather than trimmed.
_RUST_CONST = re.compile(
    r'const\s+META_ADDR_SENTINELS\s*:\s*\[\s*&str\s*;\s*(?P<len>\d+)\s*\]'
    r'\s*=\s*\[(?P<body>[^\]]*)\]')
_ITEM = re.compile(r'"([^"]*)"')


def _rust_source():
    with io.open(RUST, encoding="utf-8") as handle:
        return handle.read()


def rust_sentinels():
    """(declared length, values) read from the rust source; it cannot be imported from here."""
    found = _RUST_CONST.search(_rust_source())
    if not found:
        raise AssertionError(
            "META_ADDR_SENTINELS is no longer declared the way this reads it, in %s. If the "
            "constant moved or changed shape, move this check with it rather than deleting it."
            % os.path.relpath(RUST, REPO))
    return int(found.group("len")), tuple(_ITEM.findall(found.group("body")))


class BothLanguagesReadTheSameSentinelsTest(unittest.TestCase):

    def test_the_rust_constant_is_still_readable(self) -> None:
        """Every assertion below compares against this. If the scan returned nothing they would
        all pass on an empty tuple, and the file would be deciding nothing."""
        declared, values = rust_sentinels()
        self.assertGreaterEqual(
            len(values), 3, "read %d sentinels out of the rust source" % len(values))
        self.assertEqual(
            declared, len(values),
            "the array is annotated [&str; %d] but holds %d values" % (declared, len(values)))

    def test_python_names_the_same_values(self) -> None:
        _, values = rust_sentinels()
        self.assertEqual(
            sorted(values), sorted(plan.META_SENTINELS),
            "the two copies of the no-metaserver rule disagree. rust=%r python=%r -- a value "
            "only one side knows gets passed to the client as a literal socket address, which "
            "is how every write on a one-box failed once already."
            % (sorted(values), sorted(plan.META_SENTINELS)))

    def test_the_rust_side_still_normalises(self) -> None:
        # Searched here rather than with assertRegex, whose failure message prints the whole
        # haystack -- 45 KB of storage_backend.rs to say one line changed.
        normalises = re.search(
            r"META_ADDR_SENTINELS\s*\.contains\(&value\.trim\(\)\.to_ascii_lowercase\(\)",
            _rust_source())
        self.assertTrue(
            normalises,
            "the rust side no longer trims and lowercases before testing the sentinel list, so "
            "` Local ` would reach the client as an address while python calls it standalone")

    def test_the_python_side_still_normalises(self) -> None:
        """Driven, not read: these are the spellings a unit file or a heredoc actually leaves."""
        for written in (" local ", "LOCAL", "\tOff\n", "None", "Standalone"):
            with self.subTest(written=written):
                self.assertTrue(
                    plan.is_standalone({"TS_META_ADDR": written}),
                    "python read %r as a real metaserver address" % written)

    def test_a_real_address_is_still_a_real_address(self) -> None:
        """The positive control. Without it every assertion above passes on a function that
        answers `standalone` to everything."""
        self.assertFalse(
            plan.is_standalone({"TS_META_ADDR": "10.0.0.4:17001"}),
            "python called a real metaserver address standalone")


if __name__ == "__main__":
    unittest.main()
