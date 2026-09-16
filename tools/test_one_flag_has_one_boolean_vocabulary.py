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

The trimming half of this is REPAIRED: `matrixark_flag_on` now deletes whitespace
before it matches, in all five copies, so the two languages agree on ` 0`, `0 ` and
` false`. Measured across 12 values and both wrappers: 0 disagreements.
"""
from __future__ import annotations

import os
import re
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

#: REPAIRED. These three used to diverge: `env_bool` trimmed and `matrixark_flag_on` did not, so
#: `0 ` fell through the shell `case` to the DEFAULT while Python read it as off. The note here
#: said "adding trimming to matrixark_flag_on fails this file instead of leaving a stale note
#: behind", and that is what happened -- the helper trims now.
#:
#: They stay as WITNESSES rather than being deleted. An empty tuple makes the test below iterate
#: over nothing, which reads exactly like agreement; keeping them means the repair is asserted and
#: removing the trim fails this file from the other side.
FORMERLY_UNTRIMMED = (" 0", "0 ", " false")

#: Kept under the old name so the sweep above still covers them.
UNTRIMMED = FORMERLY_UNTRIMMED


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

    def test_the_repaired_trimming_still_holds_in_both_languages(self) -> None:
        """The divergence these three recorded is repaired; this keeps it repaired.

        Both languages must now read a whitespace-bearing value as the word it carries. It fails
        if either side stops trimming -- Python by no longer stripping, the shell by losing the
        `tr -d` from `matrixark_flag_on` -- so the repair cannot be undone quietly.
        """
        self.assertTrue(FORMERLY_UNTRIMMED,
                        "no witnesses left: with an empty tuple this test iterates over nothing "
                        "and passes whatever the two languages do")
        for value in FORMERLY_UNTRIMMED:
            with self.subTest(value=value):
                python_answer = _python_says(value, True, BACKFILL_FLAG)
                self.assertEqual(
                    "off", python_answer,
                    "env_bool no longer trims %r; it now answers %s"
                    % (value, python_answer))
                for wrapper in WRAPPERS:
                    self.assertEqual(
                        "off", _backfill_says(wrapper, value),
                        "%s no longer trims %r, so it falls through to the default and the "
                        "operator gets the opposite of what they wrote. matrixark_flag_on trims "
                        "with `tr -d '[:space:]'`; check it is still there."
                        % (wrapper, value))

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


#: MATRIXARK_LOCAL_MODE is compared against `"1"` in matrixark_codex_rust_hook.sh, and that is
#: NOT a boolean: `1` is an alias for embedded mode there, alongside `no-metaserver` and
#: `embedded`. Routing it through a boolean helper would be wrong, so it is exempt -- and
#: `test_the_one_exemption_is_still_a_mode_flag` below fails if it ever stops being a mode flag,
#: so the exemption cannot quietly become a hiding place.
MODE_FLAGS = ("MATRIXARK_LOCAL_MODE",)

#: A boolean word on the right of a comparison.
BOOLEAN_LITERAL = re.compile(r'(?:==|!=)\s*"?(?:1|0|true|false|yes|no|on|off)"?(?:\s|$|\]|\))', re.I)

#: A MATRIXARK_-family name on the left.
FLAG_NAME = re.compile(r'\$\{?(MATRIXARK_[A-Z0-9_]+)')

#: Each wrapper made at least this many matrixark_flag_on calls when this was written
#: (claude 7, codex 4). A floor, so "no literal comparisons" cannot pass because the switches
#: left rather than because they were fixed.
CALL_FLOOR = 4


def _code_lines(wrapper):
    with open(os.path.join(TOOLS, wrapper), encoding="utf-8", errors="replace") as handle:
        return [line for line in handle.read().splitlines()
                if not line.lstrip().startswith("#")]


class NoWrapperSwitchIsDecidedByALiteral(unittest.TestCase):
    """Both wrappers carried the shared helper and still decided most switches by `== "1"`.

    Eight sites across the two files. Run out of the shipped files over 24 values each, that is
    84 of 192 answers that disagreed with `env_bool` -- and every single one of them `off` where
    `env_bool` says `on`. Four of the eight default ON, which is the dangerous half: writing
    `MATRIXARK_CLAUDE_HOOK_PROXY_DAEMON=true` to KEEP the daemon on turned it off.

    The rule is general rather than a list of the eight, because a list only ever catches the
    ones that were there when it was written.
    """

    def test_the_scan_still_reaches_both_wrappers(self) -> None:
        """A floor. Every rule below passes on an empty file."""
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                lines = _code_lines(wrapper)
                self.assertGreater(
                    len(lines), 80,
                    "%s has only %d non-comment lines; the rules below are checking almost "
                    "nothing" % (wrapper, len(lines)))

    def test_the_flag_name_scan_still_matches(self) -> None:
        """A vacuity guard on the SCAN, not on the group it produces.

        Mutation-testing found this: break FLAG_NAME and the rule below sees no names, finds no
        offenders and passes. A scan that has stopped matching reads exactly like a tree with
        nothing wrong in it, which is the failure this whole file exists to prevent elsewhere.

        53 distinct names were visible when this floor was set.
        """
        seen = set()
        for wrapper in WRAPPERS:
            seen |= set(FLAG_NAME.findall("\n".join(_code_lines(wrapper))))
        self.assertGreaterEqual(
            len(seen), 31,
            "the flag-name scan finds only %d MATRIXARK_ names across the wrappers; it has "
            "stopped reaching them, and the rule below is passing on an empty set: %s"
            % (len(seen), sorted(seen)))

    def test_no_switch_is_decided_by_a_boolean_literal(self) -> None:
        """The rule."""
        offenders = []
        for wrapper in WRAPPERS:
            for number, line in enumerate(_code_lines(wrapper), 1):
                names = [n for n in FLAG_NAME.findall(line) if n not in MODE_FLAGS]
                if not names:
                    continue
                if BOOLEAN_LITERAL.search(line):
                    offenders.append((wrapper, number, names, line.strip()[:80]))
        self.assertEqual(
            [], offenders,
            "a wrapper decides a switch by comparing it against a boolean literal. Both files "
            "carry matrixark_flag_on; a comparison is a second vocabulary starting in a file "
            "that runs on every turn. %s" % (offenders,))

    def test_each_wrapper_actually_calls_the_shared_test(self) -> None:
        """The positive control. A file with no switches would satisfy the rule above."""
        for wrapper in WRAPPERS:
            calls = [line.strip() for line in _code_lines(wrapper)
                     if "matrixark_flag_on " in line and "() {" not in line]
            with self.subTest(wrapper=wrapper):
                self.assertGreaterEqual(
                    len(calls), CALL_FLOOR,
                    "%s makes only %d matrixark_flag_on calls; the rule above may be passing "
                    "because the switches left rather than because they were fixed: %s"
                    % (wrapper, len(calls), calls))

    def test_every_exemption_is_really_a_mode_flag(self) -> None:
        """The exemption list is the one place this rule can be silenced, so it is checked.

        An earlier version asserted things about MATRIXARK_LOCAL_MODE by name. Mutation-testing
        found the hole: adding a REAL boolean switch to MODE_FLAGS turned the rule off and
        nothing failed. The check now runs over every entry, and asks the question that makes an
        entry legitimate -- does this name get compared against a non-boolean string? -- rather
        than trusting the list.
        """
        lines = (_code_lines("matrixark_codex_rust_hook.sh")
                 + _code_lines("matrixark_claude_hook.sh"))
        self.assertTrue(MODE_FLAGS, "MODE_FLAGS is empty; drop the exemption machinery instead")
        for name in MODE_FLAGS:
            with self.subTest(flag=name):
                mentions = [line for line in lines if name in line]
                self.assertTrue(
                    mentions,
                    "%s is exempt from the rule above and no longer appears at all -- drop it "
                    "from MODE_FLAGS rather than leaving a standing exemption for nothing" % name)
                # A mode flag is one compared against a string that is NOT a boolean word.
                non_boolean = [
                    line for line in mentions
                    if re.search(r'(?:==|!=)\s*"([A-Za-z][A-Za-z0-9_-]*)"', line)
                    and not all(
                        m.lower() in ("1", "0", "true", "false", "yes", "no", "on", "off")
                        for m in re.findall(r'(?:==|!=)\s*"([^"]*)"', line))]
                self.assertTrue(
                    non_boolean,
                    "%s is in MODE_FLAGS, which exempts it from the boolean-literal rule, but it "
                    "is never compared against a non-boolean string. That makes it an ordinary "
                    "switch hiding behind the exemption: route it through matrixark_flag_on and "
                    "take it out of MODE_FLAGS." % name)

    def test_the_two_allow_build_sites_keep_their_own_defaults(self) -> None:
        """One name, two questions, opposite safe answers -- recorded, not unified.

        MATRIXARK_CLAUDE_HOOK_ALLOW_BUILD defaults ON where it gates the proxy build inside the
        SessionStart budget, and OFF where it gates a build that could land in the 30s
        UserPromptSubmit budget. Unifying them changes what a hook does on a cold checkout, so
        the split is asserted rather than tidied away.
        """
        lines = _code_lines("matrixark_claude_hook.sh")
        defaults = sorted({line.split("MATRIXARK_CLAUDE_HOOK_ALLOW_BUILD:-")[1][0]
                           for line in lines if "MATRIXARK_CLAUDE_HOOK_ALLOW_BUILD:-" in line})
        self.assertEqual(
            ["0", "1"], defaults,
            "MATRIXARK_CLAUDE_HOOK_ALLOW_BUILD no longer has both defaults in this file "
            "(found %s). If the two sites were deliberately unified, that is a behaviour change "
            "on a cold checkout and this test should be removed in the same commit that makes "
            "it -- not before." % defaults)


if __name__ == "__main__":
    unittest.main()
