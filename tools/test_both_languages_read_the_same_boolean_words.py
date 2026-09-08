#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Both languages read the same words as a boolean flag, and normalise them the same way.

`tools/test_env_flag_vocabulary.py` settled the vocabulary for python and recorded why: "Boolean
flags were parsed in six different vocabularies. They disagreed on the two words an operator is
most likely to reach for." Its scan reads `os.environ.get` shapes under `tools/`, so it could not
see the rust half of the same codebase — which kept the defect until mx#1308, where nine readers
matched `"1" | "true" | "TRUE" | "yes" | "YES"` with no trim and no lowercasing. Against a
default-on flag, every one of these came back **false**:

    "on"    "On"    "ON"    "True"    " 1"    "wat"    ""

Every default-on flag in the tree went through one of them, so writing `on` to keep one on turned
it off.

There are now two canonical parsers, one per language, and they agree:

    crate::env_flag::parse_bool          "1"|"true"|"yes"|"on" / "0"|"false"|"no"|"off"
    matrixark_mcp_env.env_bool           TRUE_VALUES          / FALSE_VALUES

Nothing compared them. This does, including the normalisation, because the word list is only half
the rule: rust does `raw.trim().to_ascii_lowercase()` and python does `env_text(...).strip()` then
`.lower()`, so `" On "` is true on both sides. One side losing either half is a deployment where
the same value means different things to the engine and to the tools that configure it.

Deliberately about the CANONICAL parsers only. `raft.rs` also accepts `y`/`n` and `control.rs`
also accepts `enabled`; both are intentional supersets that normalise correctly, and neither is
what a flag reader reaches for by default.
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
RUST = os.path.join(REPO, "crates", "temporalstore-rust", "src", "env_flag.rs")

sys.path.insert(0, TOOLS)

import matrixark_mcp_env as env  # noqa: E402

#: The two match arms of `parse_bool`, each a run of quoted words separated by `|`.
_ARM = re.compile(r'^\s*(?P<words>"[a-z0-9]+"(?:\s*\|\s*"[a-z0-9]+")*)\s*=>\s*Some\((?P<v>true|false)\)',
                  re.M)
_WORD = re.compile(r'"([^"]*)"')


def _rust_source():
    with io.open(RUST, encoding="utf-8") as handle:
        return handle.read()


def rust_vocabulary():
    """{True: {...}, False: {...}} read out of the rust source; it cannot be imported here."""
    found = {}
    for match in _ARM.finditer(_rust_source()):
        found[match.group("v") == "true"] = set(_WORD.findall(match.group("words")))
    return found


class BothLanguagesReadTheSameBooleanWordsTest(unittest.TestCase):

    def test_the_rust_arms_are_still_readable(self) -> None:
        """Both comparisons below would pass on an empty scan, so this decides whether they are
        comparing anything at all."""
        found = rust_vocabulary()
        self.assertEqual(
            sorted(found), [False, True],
            "read %d match arms out of env_flag.rs; parse_bool no longer has the shape this "
            "reads. If it moved, move this with it rather than deleting it." % len(found))
        for side in (True, False):
            self.assertGreaterEqual(len(found[side]), 3,
                                    "only %d words on the %s side" % (len(found[side]), side))

    def test_the_true_words_match(self) -> None:
        self.assertEqual(
            sorted(rust_vocabulary()[True]), sorted(env.TRUE_VALUES),
            "the two canonical boolean parsers disagree about what reads as TRUE")

    def test_the_false_words_match(self) -> None:
        self.assertEqual(
            sorted(rust_vocabulary()[False]), sorted(env.FALSE_VALUES),
            "the two canonical boolean parsers disagree about what reads as FALSE")

    def test_the_rust_side_still_trims_and_lowercases(self) -> None:
        normalises = re.search(r"raw\s*\.trim\(\)\s*\.to_ascii_lowercase\(\)", _rust_source())
        self.assertTrue(
            normalises,
            "env_flag::parse_bool no longer trims and lowercases, so `On` and ` 1 ` stop being "
            "understood on the rust side while python still reads them")

    def test_the_python_side_still_trims_and_lowercases(self) -> None:
        """Driven, not read: the spellings a unit file, an export and a heredoc leave behind."""
        name = "MATRIXARK_XLANG_VOCABULARY_PROBE"
        try:
            for written in (" on ", "ON", "\tTrue\n", " 1"):
                os.environ[name] = written
                with self.subTest(written=written):
                    self.assertTrue(env.env_bool(name, False),
                                    "python read %r as not-true" % written)
            for written in (" off ", "OFF", "\tFalse\n", " 0"):
                os.environ[name] = written
                with self.subTest(written=written):
                    self.assertFalse(env.env_bool(name, True),
                                     "python read %r as not-false" % written)
        finally:
            os.environ.pop(name, None)

    def test_an_unreadable_value_falls_back_to_the_default_on_both_sides(self) -> None:
        """The half that is easy to lose: `.map(...)` before `.unwrap_or(default)` returns false
        for a typo instead of the default, which is what mx#1308 was."""
        self.assertRegex(_rust_source(), r"_\s*=>\s*None",
                         "parse_bool no longer answers None for a word it does not know")
        name = "MATRIXARK_XLANG_VOCABULARY_PROBE"
        try:
            os.environ[name] = "wat"
            self.assertTrue(env.env_bool(name, True), "an unreadable value flipped a default-on")
            self.assertFalse(env.env_bool(name, False), "an unreadable value flipped a default-off")
        finally:
            os.environ.pop(name, None)


if __name__ == "__main__":
    unittest.main()
