#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`MATRIXARK_HOOK_FAIL_OPEN` is read in two languages, so both must read it the same way.

The hook wrappers decide whether a failing hook prints the fail-open `{}` and exits 0.
`matrixark_codex_hook` decides the same thing in Python through the shared `env_bool`. They tested
for different things:

    shell    [[ "$FAIL_OPEN" == "1" ]]        -- the literal 1, nothing else
    python   env_bool(NAME, True)             -- 1/true/yes/on, any case

so `MATRIXARK_HOOK_FAIL_OPEN=true` was fail-open to Python and fail-CLOSED to the wrapper, and a
hook error then blocked the turn the wrapper's own header promises it will never block. `TRUE`,
`yes` and `on` behaved the same way.

The wrappers now use `matrixark_flag_on`, which mirrors `env_bool` exactly -- including the part
that is easy to get wrong in the dangerous direction. `env_bool`'s sets are NARROW:

    on      1, true, yes, on
    off     0, false, no, off
    neither -> the DEFAULT, which is where `y`, `n`, `enabled`, `` and every typo land

A first version of this fix treated everything outside the on-set as off. That reads like
conservatism and is not: it would have turned `MATRIXARK_HOOK_FAIL_OPEN=ture` into a blocked turn,
failing in the same direction as the defect being fixed.

This test extracts `matrixark_flag_on` FROM THE SHIPPED WRAPPER and runs it, rather than restating
the rule here. A copy of the rule in this file would be a second implementation of the thing the
file exists to say there is only one of.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

from matrixark_mcp_env import env_bool

WRAPPERS = ("matrixark_claude_hook.sh", "matrixark_codex_rust_hook.sh")

#: Every value worth asking about: both vocabularies, the case variants, the ones that belong to
#: neither set, and a typo of a real one.
VALUES = ("1", "0", "true", "TRUE", "True", "false", "yes", "no", "on", "off", "ON", "OFF",
          "y", "n", "enabled", "disabled", "", "ture", "garbage")


def _shell_says(wrapper, value, default="1"):
    """What `matrixark_flag_on` in that wrapper answers, run from the file itself."""
    path = os.path.join(TOOLS, wrapper)
    script = (
        "set -u\n"
        "sed -n '/^matrixark_flag_on() {/,/^}/p' %s > /tmp/_flagfn_$$.sh\n"
        "if [ ! -s /tmp/_flagfn_$$.sh ]; then echo MISSING; exit 0; fi\n"
        ". /tmp/_flagfn_$$.sh\n"
        "rm -f /tmp/_flagfn_$$.sh\n"
        'if matrixark_flag_on "$1" "$2"; then echo on; else echo off; fi\n'
    ) % path
    out = subprocess.run(["bash", "-c", script, "_", value, default],
                         capture_output=True, text=True)
    return (out.stdout or "").strip()


def _python_says(value, default=True):
    name = "MATRIXARK_HOOK_FAIL_OPEN"
    previous = os.environ.get(name)
    try:
        if value == "":
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
        return "on" if env_bool(name, default) else "off"
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


class OneFlagHasOneBooleanVocabulary(unittest.TestCase):

    def test_both_wrappers_define_the_shared_test(self) -> None:
        """A floor. A missing function would make every comparison below answer MISSING."""
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                self.assertNotEqual(
                    "MISSING", _shell_says(wrapper, "1"),
                    "%s no longer defines matrixark_flag_on, so it is free to grow its own "
                    "boolean vocabulary again" % wrapper)

    def test_the_shell_and_python_agree_on_every_value(self) -> None:
        """The property, asserted across the value space rather than on a chosen example."""
        for wrapper in WRAPPERS:
            for value in VALUES:
                with self.subTest(wrapper=wrapper, value=value):
                    self.assertEqual(
                        _python_says(value), _shell_says(wrapper, value or "1"),
                        "%s and env_bool disagree about %r. A value that is fail-open to one and "
                        "fail-closed to the other means a hook error blocks a turn on one path and "
                        "not the other." % (wrapper, value))

    def test_an_unrecognised_value_follows_the_default_both_ways(self) -> None:
        """The half that was wrong first, and wrong in the defect's own direction.

        Treating an unknown value as off reads like caution. It would make a typo block the turn,
        which is exactly what the original defect did.
        """
        for wrapper in WRAPPERS:
            for value in ("ture", "garbage", "y", "n", "enabled"):
                with self.subTest(wrapper=wrapper, value=value):
                    self.assertEqual(
                        "on", _shell_says(wrapper, value, "1"),
                        "%s treats %r as off; env_bool falls back to the default, so a typo would "
                        "start blocking turns" % (wrapper, value))
                    self.assertEqual(
                        "off", _shell_says(wrapper, value, "0"),
                        "%s ignores the default it was given for %r" % (wrapper, value))

    def test_no_wrapper_still_tests_the_flag_against_a_literal(self) -> None:
        """The shape that caused it, kept out by name."""
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                with open(os.path.join(TOOLS, wrapper), encoding="utf-8",
                          errors="replace") as handle:
                    body = handle.read()
                self.assertNotIn(
                    '"$FAIL_OPEN" == "1"', body,
                    "%s compares the flag against a literal again" % wrapper)
                self.assertNotIn(
                    '"$MATRIXARK_HOOK_FAIL_OPEN" == "1"', body,
                    "%s compares the flag against a literal again" % wrapper)


if __name__ == "__main__":
    unittest.main()
