#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One vocabulary for boolean environment flags, and nothing hand-rolling its own.

Boolean flags were parsed in six different vocabularies. They disagreed on the two words an
operator is most likely to reach for: `{"1","true","yes"}` read `on` as OFF, and the deny-list
`{"0","false","no"}` read `off` as ON. The second is the dangerous direction -- three kill-switches
stayed on when set to `off`, which is the opposite of what a kill-switch is for.

These tests pin the single vocabulary, and then check that the flags which were demonstrably
misread are not being parsed by hand any more.
"""
from __future__ import annotations

import os
import pathlib
import re
import unittest

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool

TOOLS = pathlib.Path(__file__).resolve().parent

# Every flag below was measured reading the OPPOSITE of what the value says, before the sites were
# routed through the one parser. The value column is what an operator would plausibly write.
PREVIOUSLY_MISREAD = [
    ("MATRIXARK_AUDIT_DEBUG_PAYLOAD", "on", True, False),
    ("MATRIXARK_CONTEXT_DEBUG_RECORDS", "on", True, False),
    ("MATRIXARK_DIRECT_WRITE_QUEUE_AUTOSTART", "off", False, True),
    ("MATRIXARK_RUST_PROXY_SHARED_PROCESS", "off", False, True),
    ("MATRIXARK_LOCAL_READ_CACHE_COPY", "off", False, True),
]

# The shape that caused it: a literal env read, lowercased, tested against a set written in place.
INLINE_PARSE = re.compile(
    r"""os\.environ\.get\(\s*["'](?P<name>[A-Z][A-Z0-9_]+)["'][^)]*\)"""
    r"""\s*\.strip\(\)\s*\.lower\(\)\s*(?:not\s+in|in)\s*\{"""
)


class EnvFlagVocabulary(unittest.TestCase):
    def setUp(self):
        self._saved = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._saved)

    def test_both_words_are_honoured_in_both_directions(self):
        """`on` and `off` decide, and they decide the same way whatever the default is."""
        for value in ("1", "true", "yes", "on", "TRUE", " On "):
            for default in (True, False):
                os.environ["MATRIXARK_TEST_VOCAB"] = value
                self.assertTrue(env_bool("MATRIXARK_TEST_VOCAB", default),
                                f"{value!r} must read as true (default={default})")
        for value in ("0", "false", "no", "off", "OFF", " Off "):
            for default in (True, False):
                os.environ["MATRIXARK_TEST_VOCAB"] = value
                self.assertFalse(env_bool("MATRIXARK_TEST_VOCAB", default),
                                 f"{value!r} must read as false (default={default})")

    def test_an_unrecognised_value_falls_back_rather_than_guessing(self):
        """A typo must not silently mean ON. The deny-list spelling made `nope` true."""
        for value in ("nope", "disabled", "2", "enabled"):
            os.environ["MATRIXARK_TEST_VOCAB"] = value
            self.assertFalse(env_bool("MATRIXARK_TEST_VOCAB", False), value)
            self.assertTrue(env_bool("MATRIXARK_TEST_VOCAB", True), value)

    def test_unset_returns_the_default(self):
        os.environ.pop("MATRIXARK_TEST_VOCAB", None)
        self.assertTrue(env_bool("MATRIXARK_TEST_VOCAB", True))
        self.assertFalse(env_bool("MATRIXARK_TEST_VOCAB", False))

    def test_the_flags_that_were_misread_now_do_what_they_say(self):
        for name, value, intended, default in PREVIOUSLY_MISREAD:
            os.environ[name] = value
            self.assertEqual(
                env_bool(name, default), intended,
                f"{name}={value} must read as {intended}; it used to read as {not intended}")

    def test_no_module_hand_rolls_a_vocabulary_for_those_flags(self):
        """The sites themselves must be routed through the parser, not merely agree with it.

        Scans every module and asserts its own extent: a guard that silently scanned nothing would
        pass while the code drifted straight back.
        """
        scanned, offenders = 0, []
        misread = {name for name, _, _, _ in PREVIOUSLY_MISREAD}
        for path in sorted(TOOLS.glob("*.py")):
            if path.name.startswith("test_"):
                continue
            scanned += 1
            for match in INLINE_PARSE.finditer(path.read_text(encoding="utf-8", errors="replace")):
                if match.group("name") in misread:
                    offenders.append(f"{path.name}: {match.group('name')}")
        self.assertGreater(scanned, 100,
                           "the scan covered almost no modules -- it is not proving anything")
        self.assertEqual(offenders, [], "these flags are parsed by hand again")


if __name__ == "__main__":
    unittest.main()
