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

`MATRIXARK_HOOK_FAIL_OPEN` was not the only flag both wrappers read. `MATRIXARK_BACKFILL_ON_START`
is read by both too, and each had written its own off-set:

    claude   case 0|false|False|FALSE|no|No|NO|off|Off|OFF   -- ten spellings, `off` among them
    codex    != "0" && != "false" && != "no"                 -- three, lowercase, no `off`

so `MATRIXARK_BACKFILL_ON_START=off` stopped the backfill daemon under Claude and started it under
Codex; `OFF`, `Off`, `False`, `FALSE`, `No` and `NO` behaved the same way. That daemon writes into
the operator's store, so the two readers did not merely report differently -- one of them ingested
history the operator had asked it not to touch. Both now go through `matrixark_flag_on`, and the
tests below run each wrapper's shipped decision rather than describing it.

What is NOT fixed, and is asserted here so it cannot be fixed silently: `matrixark_flag_on` does not
trim. `env_bool` does, via `env_text`. A value of `" 0"` -- which is what a systemd `Environment=`
line or a careless export can leave -- is off to Python and ON to both wrappers. Both wrappers agree
with each other about it, which is why it is recorded rather than repaired here: changing it changes
`MATRIXARK_HOOK_FAIL_OPEN` too, and that is a separate decision.
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


def _python_says(value, default=True, name="MATRIXARK_HOOK_FAIL_OPEN"):
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


BACKFILL_FLAG = "MATRIXARK_BACKFILL_ON_START"

#: The unset default is the literal `auto`, so the case variants and `auto` itself both matter.
BACKFILL_VALUES = ("1", "0", "true", "TRUE", "True", "false", "False", "FALSE", "yes", "no", "No",
                   "NO", "on", "off", "ON", "OFF", "Off", "auto", "y", "n", "enabled", "ture",
                   "garbage", "")

#: Recorded, not repaired: the shell helper does not trim and `env_bool` does. Asserted in BOTH
#: directions below, so that adding trimming to `matrixark_flag_on` fails this file instead of
#: leaving a stale note behind.
UNTRIMMED = (" 0", "0 ", " false")


def _backfill_says(wrapper, value):
    """What the shipped wrapper decides about the backfill daemon, run from the file itself."""
    path = os.path.join(TOOLS, wrapper)
    script = (
        "set -u\n"
        "sed -n '/^matrixark_flag_on() {/,/^}/p' %s > /tmp/_bf_$$.sh\n"
        "sed -n '/^_matrixark_backfill_enabled() {/,/^}/p' %s >> /tmp/_bf_$$.sh\n"
        "if ! grep -q '_matrixark_backfill_enabled' /tmp/_bf_$$.sh; then echo MISSING; exit 0; fi\n"
        ". /tmp/_bf_$$.sh\n"
        "rm -f /tmp/_bf_$$.sh\n"
        'if _matrixark_backfill_enabled; then echo on; else echo off; fi\n'
    ) % (path, path)
    environment = dict(os.environ)
    environment[BACKFILL_FLAG] = value
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=environment)
    return (out.stdout or "").strip()


class TheBackfillSwitchHasOneVocabularyToo(unittest.TestCase):
    """The second flag both wrappers read, and the one whose readers had drifted apart."""

    def test_both_wrappers_expose_the_decision(self) -> None:
        """A floor. Without it every comparison below would compare MISSING to MISSING."""
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                self.assertNotEqual(
                    "MISSING", _backfill_says(wrapper, "1"),
                    "%s no longer defines _matrixark_backfill_enabled, so its answer for %s is not "
                    "being tested at all" % (wrapper, BACKFILL_FLAG))

    def test_the_value_space_is_not_empty(self) -> None:
        """A sweep that stops matching reads exactly like a clean sweep."""
        self.assertGreaterEqual(len(BACKFILL_VALUES), 20)
        self.assertGreaterEqual(len(WRAPPERS), 2)

    def test_the_two_wrappers_agree_on_every_value(self) -> None:
        """The property that was broken: one flag, two hooks, two answers."""
        first, second = WRAPPERS[0], WRAPPERS[1]
        for value in BACKFILL_VALUES + UNTRIMMED:
            with self.subTest(value=value):
                self.assertEqual(
                    _backfill_says(first, value), _backfill_says(second, value),
                    "%s and %s disagree about %s=%r. The daemon writes to the operator's store, so "
                    "one hook ingests history the other was told to leave alone."
                    % (first, second, BACKFILL_FLAG, value))

    def test_each_wrapper_agrees_with_env_bool(self) -> None:
        """And both agree with the vocabulary the rest of the tree parses with."""
        for wrapper in WRAPPERS:
            for value in BACKFILL_VALUES:
                with self.subTest(wrapper=wrapper, value=value):
                    self.assertEqual(
                        _python_says(value or "auto", True, BACKFILL_FLAG),
                        _backfill_says(wrapper, value or "auto"),
                        "%s and env_bool disagree about %s=%r"
                        % (wrapper, BACKFILL_FLAG, value))

    def test_the_untrimmed_divergence_is_still_exactly_what_is_recorded(self) -> None:
        """Both directions. This fails when a new untrimmed value diverges AND when the
        recorded one stops diverging, so the note above cannot rot into decoration."""
        for value in UNTRIMMED:
            with self.subTest(value=value):
                self.assertEqual(
                    "off", _python_says(value, True, BACKFILL_FLAG),
                    "env_bool no longer trims %r, so the recorded divergence is misstated" % value)
                for wrapper in WRAPPERS:
                    self.assertEqual(
                        "on", _backfill_says(wrapper, value),
                        "%s now trims %r. That is an improvement, and it makes the note in this "
                        "file's docstring wrong -- fix the note, and check whether "
                        "MATRIXARK_HOOK_FAIL_OPEN changed with it." % (wrapper, value))

    def test_neither_wrapper_still_spells_its_own_off_set(self) -> None:
        """The shape that caused it, kept out by name."""
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                with open(os.path.join(TOOLS, wrapper), encoding="utf-8",
                          errors="replace") as handle:
                    body = handle.read()
                self.assertNotIn('!= "false"', body,
                                 "%s lists its own off-words again" % wrapper)
                self.assertNotIn("|false|False|FALSE|", body,
                                 "%s lists its own off-words again" % wrapper)


if __name__ == "__main__":
    unittest.main()
