#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The backfill daemon's four switches had three vocabularies between them.

`tools/matrixark_backfill_daemon.sh` is launched detached by both hook wrappers and reads four
boolean flags. Each had been written on its own:

    MATRIXARK_BACKFILL_FORCE                   [[ "$FORCE" == "1" ]]
    MATRIXARK_BACKFILL_REEMIT_ON_FRESH         != "0" && != "false" && != "no"
    MATRIXARK_BACKFILL_SORT_JSONL_BY_SESSION   != "0" && != "false" && != "no"
    MATRIXARK_BACKFILL_CARGO_OFFLINE           == "1" || == "true"

Run out of the shipped file over a 24-value space, that is 29 answers across the four flags that
disagreed with `env_bool`. `MATRIXARK_BACKFILL_FORCE=true` did not force. `=yes` and `=on` did
not either, and `MATRIXARK_BACKFILL_FORCE=1` is the only spelling `docs/INSTALL.md` shows, so
nothing in the tree would have told an operator that the other three words were inert.
`REEMIT_ON_FRESH=off` and `SORT_JSONL_BY_SESSION=off` read as ON -- `off` is in the shared
FALSE_VALUES, and this file simply did not list it.

All four now go through `matrixark_flag_on`, the function the two wrappers already share, and
that helper now TRIMS as well: it used to case-fold the value without stripping it, so ` 1`
was on in Python and off in the shell. The trimming divergence this file recorded as open is
closed, and the witnesses below are what hold it closed.

That makes three copies of one function in three files. They are copies because all three are
standalone scripts: the wrappers are hook entry points and this one is launched with
`setsid bash`, so none of them sources anything. Copies drift, which is the whole reason this
file exists -- so `test_the_three_copies_are_the_same_text` compares them, and the behaviour
tests below run THIS file's copy rather than a restatement of the rule.
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

DAEMON = "matrixark_backfill_daemon.sh"

#: Every file that carries a copy of the shared shell boolean test.
CARRIERS = (DAEMON, "matrixark_claude_hook.sh", "matrixark_codex_rust_hook.sh",
            "matrixark_mcp_rust_server.sh")

#: flag -> default, as the daemon sets it. The default matters: it is the answer for every
#: value in neither half of the vocabulary, and two of these four default ON.
FLAGS = {
    "MATRIXARK_BACKFILL_FORCE": "0",
    "MATRIXARK_BACKFILL_REEMIT_ON_FRESH": "1",
    "MATRIXARK_BACKFILL_SORT_JSONL_BY_SESSION": "1",
    "MATRIXARK_BACKFILL_CARGO_OFFLINE": "0",
}

VALUES = ("1", "0", "true", "TRUE", "True", "false", "False", "FALSE", "yes", "YES", "no", "No",
          "NO", "on", "ON", "On", "off", "OFF", "Off", "auto", "y", "n", "ture", "garbage")

#: REPAIRED. The shell copy did not trim and `env_bool` did, so a value carrying a stray
#: space -- what a .env line or a compose `environment:` entry produces without anyone seeing
#: it -- was read one way by the daemon and the other way by Python. All four carriers now
#: trim, so these three are kept as WITNESSES of a closed divergence rather than a record of
#: an open one: they are what the assertion below iterates over, and an empty tuple would
#: make it pass without comparing anything.
FORMERLY_UNTRIMMED = (" 1", "1 ", " 0")


def _helper_text(name):
    """The `matrixark_flag_on` definition as it appears in that file, or None."""
    with open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
        body = handle.read()
    match = re.search(r'^matrixark_flag_on\(\) \{.*?^\}', body, re.S | re.M)
    return match.group(0) if match else None


def _daemon_says(value, default):
    """What the daemon's own copy answers, run from the daemon file."""
    helper = _helper_text(DAEMON)
    if helper is None:
        return "MISSING"
    script = helper + '\nif matrixark_flag_on "$1" "$2"; then echo on; else echo off; fi\n'
    out = subprocess.run(["bash", "-c", script, "_", value, default],
                         capture_output=True, text=True)
    return (out.stdout or "").strip()


def _python_says(name, value, default):
    previous = os.environ.get(name)
    try:
        os.environ[name] = value
        return "on" if env_bool(name, default == "1") else "off"
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


class TheBackfillDaemonReadsItsOwnSwitches(unittest.TestCase):

    def test_the_daemon_still_carries_the_shared_test(self) -> None:
        """A floor. Without it every behaviour comparison below answers MISSING."""
        self.assertIsNotNone(
            _helper_text(DAEMON),
            "%s no longer defines matrixark_flag_on, so it is free to grow its own boolean "
            "vocabulary again -- which is how it came to have three." % DAEMON)

    def test_the_value_space_is_not_empty(self) -> None:
        """A sweep that stops matching reads exactly like a clean sweep."""
        self.assertGreaterEqual(len(VALUES), 20)
        self.assertGreaterEqual(len(FLAGS), 4)
        self.assertGreaterEqual(len(CARRIERS), 4)

    def test_every_copy_is_the_same_text(self) -> None:
        """The control on the copies. One of them drifting is the failure this file prevents.

        Named for the property, not the count: it was `..._the_three_copies_...` when there were
        three, and the fourth arrived with tools/matrixark_mcp_rust_server.sh. A test name that
        carries a number goes stale the first time the number changes, and a stale name is read
        as a stale test.
        """
        texts = {name: _helper_text(name) for name in CARRIERS}
        for name, text in texts.items():
            self.assertIsNotNone(text, "%s no longer carries matrixark_flag_on" % name)
        first = CARRIERS[0]
        for other in CARRIERS[1:]:
            self.assertEqual(
                texts[first], texts[other],
                "the matrixark_flag_on in %s and in %s are no longer the same text. They are "
                "copies because all three scripts are standalone; a copy that drifts is a "
                "second vocabulary again." % (first, other))

    def test_each_switch_answers_what_env_bool_answers(self) -> None:
        """The property, over the value space rather than on a chosen example."""
        for flag, default in sorted(FLAGS.items()):
            for value in VALUES:
                with self.subTest(flag=flag, value=value):
                    self.assertEqual(
                        _python_says(flag, value, default), _daemon_says(value, default),
                        "the daemon and env_bool disagree about %s=%r" % (flag, value))

    def test_an_unrecognised_value_follows_each_flags_own_default(self) -> None:
        """Two of these four default ON, so "unknown means off" is not a safe simplification."""
        for flag, default in sorted(FLAGS.items()):
            for value in ("ture", "garbage", "auto", "y"):
                with self.subTest(flag=flag, value=value):
                    self.assertEqual(
                        "on" if default == "1" else "off", _daemon_says(value, default),
                        "%s=%r does not fall back to the flag's own default (%s)"
                        % (flag, value, default))

    def test_the_repaired_trimming_holds_in_both_languages(self) -> None:
        """The divergence this file recorded is closed; this is what keeps it closed.

        It used to assert that the shell copy did NOT trim, in both directions, so that repairing
        it would fail here rather than leave a stale note. It was repaired, and this failed --
        which is the guard working, not a regression. The record is now the AGREEMENT, asserted
        the same way in both directions: each language must give the same answer for a value
        carrying a stray space.

        Compared against `value.strip()` rather than a literal, so a witness can be added to the
        tuple above without editing an expected answer beside it.
        """
        self.assertTrue(
            FORMERLY_UNTRIMMED,
            "no witnesses left in FORMERLY_UNTRIMMED, so this test compares nothing and would "
            "pass against any behaviour at all")
        for value in FORMERLY_UNTRIMMED:
            with self.subTest(value=value):
                expected = "on" if value.strip() == "1" else "off"
                self.assertEqual(
                    expected, _python_says("MATRIXARK_BACKFILL_FORCE", value, "0"),
                    "env_bool has stopped trimming %r" % value)
                self.assertEqual(
                    expected, _daemon_says(value, "0"),
                    "the shell copy has stopped trimming %r, so the daemon and env_bool disagree "
                    "about a value with a stray space again -- the divergence this file records "
                    "as CLOSED is open. Check every file in CARRIERS." % value)

    def test_no_switch_still_spells_its_own_word_list(self) -> None:
        """The shape that caused it, kept out by name.

        COMMENTS ARE STRIPPED FIRST. The first version of this test failed on the daemon's own
        comment, which records the four old conditions verbatim so a reader can see what was
        replaced. A guard that reads the note describing the defect as the defect is a guard
        that makes the tree stop explaining itself.
        """
        with open(os.path.join(TOOLS, DAEMON), encoding="utf-8", errors="replace") as handle:
            lines = handle.read().splitlines()
        code = [line for line in lines if not line.lstrip().startswith("#")]
        self.assertGreater(
            len(code), 100,
            "stripping comments left only %d lines of %s -- the check below would pass on "
            "almost nothing" % (len(code), DAEMON))
        body = "\n".join(code)
        for shape in ('"$FORCE" == "1"', '!= "false"', '!= "no"', '== "true"'):
            with self.subTest(shape=shape):
                self.assertNotIn(
                    shape, body,
                    "%s decides a switch against a literal word list again: %r"
                    % (DAEMON, shape))


    def test_no_switch_is_decided_by_any_boolean_literal_at_all(self) -> None:
        """The general form of the rule above, which the named list is only a sample of.

        Naming four shapes catches the four that were there. A fifth switch written tomorrow
        with a fifth spelling would pass a list and fail here: this asks whether ANY test in the
        file compares a variable against a boolean word, rather than whether it matches one of
        the spellings that happened to exist.
        """
        with open(os.path.join(TOOLS, DAEMON), encoding="utf-8", errors="replace") as handle:
            lines = handle.read().splitlines()
        code = [line for line in lines if not line.lstrip().startswith("#")]
        self.assertGreater(len(code), 100, "comment stripping left almost nothing")

        compares = re.compile(
            r'(?:\[\[|\[|test)[^\n]*?\$\{?[A-Za-z_][A-Za-z0-9_]*[^\n]*?'
            r'(?:==|!=)\s*"?(?:1|0|true|false|yes|no|on|off)"?(?:\s|$|\])', re.I)
        offenders = [(i + 1, line.strip()) for i, line in enumerate(code)
                     if compares.search(line)]
        self.assertEqual(
            [], offenders,
            "%s decides a switch by comparing against a boolean literal. Every boolean flag in "
            "this file goes through matrixark_flag_on; a comparison is a fifth vocabulary "
            "starting. Offending lines: %s" % (DAEMON, offenders))

    def test_the_switches_actually_go_through_the_shared_test(self) -> None:
        """The positive control for the rule above.

        A file with no flags at all would satisfy "no literal comparisons" perfectly. This
        counts the calls, so an empty answer cannot pass as a clean one.
        """
        with open(os.path.join(TOOLS, DAEMON), encoding="utf-8", errors="replace") as handle:
            lines = handle.read().splitlines()
        calls = [line.strip() for line in lines
                 if not line.lstrip().startswith("#") and "matrixark_flag_on " in line]
        self.assertGreaterEqual(
            len(calls), len(FLAGS),
            "%s makes only %d matrixark_flag_on calls for %d boolean flags, so the rule above "
            "may be passing because the switches left rather than because they were fixed: %s"
            % (DAEMON, len(calls), len(FLAGS), calls))


#: The launcher's own boolean switches, with the default each is exported with.
#: `DISK_FALLBACK`'s default is profile-dependent -- 1, or 0 under prod/production/benchmark/
#: bench -- which is why it is recorded as the expression rather than a literal.
LAUNCHER = "matrixark_mcp_rust_server.sh"
LAUNCHER_SWITCHES = ("MATRIXARK_MCP_AUTOSTART_NATIVE",
                     "MATRIXARK_TEMPORALSTORE_DISK_FALLBACK")


def _launcher_code_lines():
    with open(os.path.join(TOOLS, LAUNCHER), encoding="utf-8", errors="replace") as handle:
        return [line for line in handle.read().splitlines()
                if not line.lstrip().startswith("#")]


class TheDefaultLauncherDecidesBySharedVocabulary(unittest.TestCase):
    """tools/matrixark_mcp_rust_server.sh is matrixark_agent_config.DEFAULT_LAUNCHER.

    Its two switches were `== "1"` and `== "1" || == "true" || == "yes"`. Run out of the shipped
    file over 24 values, that is 8 and 6 answers respectively that disagreed with `env_bool` --
    every one of them a word written to turn something ON reading as off.

    This file's general "no boolean literal anywhere" rule cannot be applied here: the launcher
    legitimately compares MATRIXARK_LOCAL_MODE against `"1"` as an alias for embedded mode, and
    MATRIXARK_MCP_BACKEND against backend names. So the rule is scoped to the two switch NAMES,
    with a floor below that fails if either name stops appearing -- otherwise a rename would pass
    this silently, which is the failure mode a name list always has.
    """

    def test_both_switches_are_still_in_the_launcher(self) -> None:
        """The floor for the rule below. A renamed flag must fail, not vanish from the check."""
        code = "\n".join(_launcher_code_lines())
        self.assertGreater(len(code), 2000,
                           "comment stripping left almost nothing of %s" % LAUNCHER)
        for name in LAUNCHER_SWITCHES:
            with self.subTest(flag=name):
                self.assertIn(name, code,
                              "%s no longer mentions %s, so the rule below checks nothing about "
                              "it -- if it was renamed, rename it here too" % (LAUNCHER, name))

    def test_neither_switch_is_compared_against_a_boolean_literal(self) -> None:
        """The shape that was there, kept out by the switch's own name."""
        offenders = []
        for number, line in enumerate(_launcher_code_lines(), 1):
            for name in LAUNCHER_SWITCHES:
                if name not in line:
                    continue
                if re.search(r'(?:==|!=)\s*"?(?:1|0|true|false|yes|no|on|off)"?', line, re.I):
                    offenders.append((number, name, line.strip()[:90]))
        self.assertEqual(
            [], offenders,
            "%s decides a switch by comparing it against a boolean literal again. Every boolean "
            "flag in this launcher goes through matrixark_flag_on; a comparison is a second "
            "vocabulary starting in the file every agent integration exec's. %s"
            % (LAUNCHER, offenders))

    def test_both_switches_go_through_the_shared_test(self) -> None:
        """The positive control.

        A launcher that stopped reading its switches at all would satisfy the rule above
        perfectly, so the calls are counted rather than assumed.
        """
        code = _launcher_code_lines()
        for name in LAUNCHER_SWITCHES:
            calls = [line.strip() for line in code
                     if "matrixark_flag_on" in line and name in line]
            with self.subTest(flag=name):
                self.assertGreaterEqual(
                    len(calls), 1,
                    "%s makes no matrixark_flag_on call naming %s, so the rule above may be "
                    "passing because the switch left rather than because it was fixed"
                    % (LAUNCHER, name))

    def test_the_launcher_is_the_default_launcher(self) -> None:
        """Why this file cares. If the launcher stops being the default, say so deliberately."""
        path = os.path.join(TOOLS, "matrixark_agent_config.py")
        if not os.path.exists(path):
            self.skipTest("matrixark_agent_config.py is not in this checkout")
        with open(path, encoding="utf-8", errors="replace") as handle:
            body = handle.read()
        self.assertIn(
            'DEFAULT_LAUNCHER = "tools/%s"' % LAUNCHER, body,
            "%s is no longer matrixark_agent_config.DEFAULT_LAUNCHER; the docstring above "
            "explains this file's interest in it and would now be wrong" % LAUNCHER)


if __name__ == "__main__":
    unittest.main()
