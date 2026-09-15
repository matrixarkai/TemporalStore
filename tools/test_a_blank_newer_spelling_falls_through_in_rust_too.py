#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A blank newer spelling falls through to the older one, in rust too.

`test_a_blank_flag_falls_through_to_the_older_spelling` settled this for python and recorded why:
a chain written `get(NEW, get(OLD, default))` consults OLD only when NEW is ABSENT, and a variable
that is PRESENT AND BLANK -- what `export NEW=$UNSET` leaves behind, and what clearing a field
means -- resolved to "" while OLD was never read, however correctly it was set.

`std::env::var` has the same edge and the rust side had no guard, so it kept it. `env::var(NEW)`
answers `Ok("")` for a blank variable, and `.or_else(|_| env::var(OLD))` takes an `Err` as its cue.
Eighteen expressions in the crate read one control under two spellings. Two filtered the blank out
by hand -- `context_workflow/model_provider.rs`, with a comment saying the newer name wins when
both are set -- and sixteen did not.

Measured before the fix, with the older spelling set correctly every time:

    TS_BLOCK_SLAB_TARGET_BYTES=""       previous name held 2097152
                                        -> neither honoured, the built-in 1 GiB default applied
    TS_DATA_RAFT_READ_MODE=""           TS_SERVER_RAFT_READ_MODE=linearizable
                                        -> panic!("invalid TS_DATA_RAFT_READ_MODE"), node exits
    MATRIXARK_RUST_PROXY_PAGE_
      COMPRESSION_MIN_BYTES=""          TS_PAGE_STORE_COMPRESSION_MIN_BYTES=1024
                                        -> 4096, the built-in default

The raft one is the sharp end, and it is the same sharp end python had: a blank on the newer name
takes the serving path down at startup, and the message names neither the blank variable nor the
one that was set correctly.

WHY THE SCAN COUNTS BOTH SHAPES
-------------------------------
The fix replaces `env::var(NEW)` with `env_flag::env_value(NEW)`, which would have taken every
fixed site OUT of a scan that looked for `env::var` -- the denominator would have fallen from
eighteen to nine and the remaining holes would have read as the whole population. That happened
once here, and the floor caught it. Both spellings are counted, and the floor is on the TOTAL.

The second name must sit inside a FALLBACK combinator's own argument, not merely later in the
statement. Reading to the end of the statement reported `MATRIXARK_BULK_INGEST_REPLAY_FROM_SEQUENCE`
as chained to `MATRIXARK_BULK_INGEST_EXPECTED_WAL_COMMANDS`; they are two different controls, one
read inside the other's `.map()` closure, and calling that a spelling chain would have put a wrong
entry in front of anyone reading this.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
SRC = os.path.join(REPO, "crates", "temporalstore-rust", "src")

#: A scan that stops matching passes every rule below and reads exactly like compliance.
EXPECTED_CHAIN_FLOOR = 15

READ = {
    "env::var chain": re.compile(
        r'(?:[A-Za-z_][A-Za-z0-9_]*::)*env::var\(\s*"(?P<n>[A-Z][A-Z0-9_]*)"\s*\)'),
    "env_value chain": re.compile(
        r'(?:[A-Za-z_][A-Za-z0-9_]*::)*env_value\(\s*"(?P<n>[A-Z][A-Z0-9_]*)"\s*\)'),
}
GETTER_CHAIN = re.compile(r"\bget\(\s*(?P<n>[A-Z][A-Z0-9_]+)\s*\)")
ANY_CALL = re.compile(
    r"\b(?P<helper>env_(?:bool|usize|u64|u32|i32|i64|f64)_any)\s*\(\s*&\[(?P<names>[^\]]*)\]")
ANY_DEF = re.compile(r"fn\s+(?P<helper>env_[a-z0-9]+_any)\s*\(")
FALLBACK = re.compile(r"\.(?:or_else|unwrap_or_else|unwrap_or)\(")
#: What makes a chain safe: the value is asked for through a reader that answers None for a blank,
#: or the blank is filtered out where it is read.
BLANK_SAFE = ("is_empty", "env_value")


def _sources():
    for base, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d != "target"]
        for name in sorted(files):
            if name.endswith(".rs"):
                yield os.path.join(base, name)


def _read(path):
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _helper_bodies(text):
    """helper name -> its body, so a call site is judged by the function it actually calls."""
    bodies = {}
    for match in ANY_DEF.finditer(text):
        brace = text.find("{", match.end())
        if brace < 0:
            continue
        depth, index = 0, brace
        while index < len(text):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    break
            index += 1
        bodies[match.group("helper")] = text[brace:index + 1]
    return bodies


def _balanced(text, open_paren):
    depth = 0
    for i in range(open_paren, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _fallback_arguments(text, start):
    """The arguments of every fallback combinator chained onto the read at `start`.

    Stops at the end of the expression, so a second control read inside a `.map()` closure is not
    mistaken for the older spelling of this one.
    """
    out, i, depth = [], start, 0
    while i < len(text) and i < start + 1500:
        char = text[i]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth < 0:
                break
        elif char in (";", ",") and depth <= 0:
            break
        if depth == 0:
            match = FALLBACK.match(text, i)
            if match:
                close = _balanced(text, match.end() - 1)
                if close > 0:
                    out.append(text[match.end():close])
                    i = close
                    continue
        i += 1
    return out


def spelling_chains():
    """(file, line, first, second, blank_falls_through, shape) for every two-spelling read."""
    found = []
    for path in _sources():
        text = _read(path)
        rel = os.path.relpath(path, REPO).replace(os.sep, "/")
        for shape, pattern in READ.items():
            for match in pattern.finditer(text):
                arguments = _fallback_arguments(text, match.end())
                second = None
                for argument in arguments:
                    for inner in pattern.finditer(argument):
                        if inner.group("n") != match.group("n"):
                            second = inner.group("n")
                            break
                    if second:
                        break
                if not second:
                    continue
                whole = text[match.start():match.end() + sum(len(a) + 12 for a in arguments)]
                found.append((rel, text.count("\n", 0, match.start()) + 1,
                              match.group("n"), second,
                              any(s in whole for s in BLANK_SAFE), shape))
        for match in GETTER_CHAIN.finditer(text):
            arguments = _fallback_arguments(text, match.end())
            second = next((inner.group("n") for argument in arguments
                           for inner in GETTER_CHAIN.finditer(argument)
                           if inner.group("n") != match.group("n")), None)
            if not second:
                continue
            start = text.rfind("pub fn from_getter", 0, match.start())
            safe = start >= 0 and "!value.trim().is_empty()" in text[start:match.start()]
            found.append((rel, text.count("\n", 0, match.start()) + 1,
                          match.group("n"), second, safe, "get() chain"))
        bodies = _helper_bodies(text)
        for match in ANY_CALL.finditer(text):
            names = [n.strip().strip('"') for n in match.group("names").split(",") if n.strip()]
            if len(names) < 2:
                continue
            # Per HELPER, not per file. Asking whether the file contains a safe lookup anywhere
            # let one of four helpers revert and hide behind its three siblings -- a mutation
            # walked past this guard exactly that way.
            body = bodies.get(match.group("helper"), "")
            safe = bool(body) and any(s in body for s in BLANK_SAFE)
            found.append((rel, text.count("\n", 0, match.start()) + 1,
                          names[0], names[1], safe, "env_*_any"))
    return sorted(set(found))


class ABlankNewerSpellingFallsThroughTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.chains = spelling_chains()

    def test_the_scan_still_finds_the_chains(self) -> None:
        """Counted across BOTH spellings on purpose: the fix rewrites `env::var` to `env_value`,
        and a scan that knew only the first would have watched its own subject disappear."""
        self.assertGreaterEqual(
            len(self.chains), EXPECTED_CHAIN_FLOOR,
            "found %d expressions reading one control under two names, expected at least %d -- "
            "the shapes have changed and the rule below is deciding nothing about them"
            % (len(self.chains), EXPECTED_CHAIN_FLOOR))

    def test_both_spellings_are_represented(self) -> None:
        """If every site were one shape, the two-shape scan would be untested and could have
        lost the other one without anybody noticing."""
        shapes = {shape for *_, shape in self.chains}
        self.assertGreaterEqual(
            len(shapes), 3,
            "the scan is only finding %s; it claims to count several shapes" % sorted(shapes))

    def test_a_blank_newer_spelling_never_shadows_the_older_one(self) -> None:
        shadowing = ["%s:%d  %s then %s [%s]" % (rel, line, first, second, shape)
                     for rel, line, first, second, safe, shape in self.chains if not safe]
        self.assertEqual(
            [], shadowing,
            "these read one control under two names and take the FIRST as answered whenever it "
            "is set at all -- so blanking it discards the older spelling as well and the "
            "built-in default applies, or the parse of an empty string fails: %s" % shadowing)


if __name__ == "__main__":
    unittest.main()
