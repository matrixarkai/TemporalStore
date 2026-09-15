#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The operator page states a rule for this control; every reader of it has to follow that rule.

`MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT` is one of six numeric settings out of seventy-nine
whose help text states what happens to the value, rather than only what the value is for:

    The value is floored at 1000 -- a smaller number is raised to it -- and a value that is not
    an integer falls back to 40000 without complaining.

That is a promise, and it had three readers. Two kept it. `bin/codex_context_hook.rs` applied no
floor at all, and read `0` as "not set" rather than as a number below the floor:

    written   the page, and both python readers   codex_context_hook.rs
    500       1000                                500
    100       1000                                100
    0         1000                                40000
    -1        1000                                40000

Which reader answers is not a deployment's choice. `tools/matrixark_claude_hook.sh` defaults to
`MATRIXARK_CLAUDE_HOOK_BACKEND=auto`, which runs `matrixark_agent_hook.py` when the rust proxy
binary is present and this binary when it is not -- so `=0` meant 1000 characters on one box and
40000 on another, decided by whether something unrelated had been built.

`test_the_context_limit_reaches_both_hooks` covers the two python readers, and covers them well:
it measures them in fresh processes because the codex reader binds its default at import. It has
"both" in its name because when it was written there were two. The third was never in scope.

ONE recorded difference, asserted in both directions rather than excluded. `matrixark_agent_hook`
falls back to 8000 rather than 40000 when the control is unset or unreadable, and says why beside
the constant: the two hooks build different blocks and have always had different budgets, and
raising it to match would quintuple what every Claude turn receives. That is a sizing decision. It
is a difference about the DEFAULT, not about the floor -- so this file pins it exactly, and fails
if it spreads to the floor or quietly goes away.

The rust side is read from the source rather than run, which is what `declared_floors()` in
`test_matrixark_clamped_settings_say_so` does for the engine's own floors: no CI job runs tests in
this binary (`cargo test` covers `--lib`, `--tests` and one other bin), so a test module next to
the reader would never execute and would be worse than nothing.
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfgmod  # noqa: E402

FLAG = "MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT"
RUST_READER = os.path.join(
    REPO, "crates", "temporalstore-rust", "src", "bin", "codex_context_hook.rs")

#: The reader whose default is deliberately its own, and the value it is.
RECORDED_OWN_BUDGET = {"agent": 8000}

#: Enough shapes to separate the three rules from each other: a value above the floor, values
#: below it, the two the old rust reader took as "not set", and whitespace.
CASES = (None, "40000", "20000", "2000", "500", "100", "0", "-1", "garbage", " 20000 ")

#: Both readers are called as FUNCTIONS, and that is the point rather than a style choice.
#:
#: This probe read `c.DEFAULT_ADDITIONAL_CONTEXT_CHAR_LIMIT`, a module-level constant, because that
#: was where the codex hook's answer lived. The change that removed the constant -- it was a default
#: ARGUMENT, evaluated once when the `def` ran, which froze a control the page calls live -- and the
#: change that added this file were each green on a main that did not have the other. Together they
#: are `AttributeError` in `setUpClass`, which reads as eight tests erroring about a char limit.
#:
#: A function call cannot go stale the same way: it is the reader, whatever the reader keeps.
PROBE = r"""
import json, sys
sys.path.insert(0, %r)
import matrixark_agent_hook as a
import matrixark_codex_hook as c
print(json.dumps({"agent": a._additional_context_char_limit(),
                  "codex": c._default_additional_context_char_limit()}))
"""


def _page_rule():
    """(floor, fallback) as the operator page states them, parsed rather than retyped."""
    setting = next((s for s in cfgmod.SETTINGS if s.env == FLAG), None)
    if setting is None:
        return None, None
    floor = re.search(r"floored at (\d+)", setting.help)
    fallback = re.search(r"falls back to (\d+)", setting.help)
    return (int(floor.group(1)) if floor else None,
            int(fallback.group(1)) if fallback else None)


def _read(path):
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _readers():
    """Every file that READS the control, as opposed to exporting it."""
    found = []
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_"):
            continue
        text = _read(os.path.join(TOOLS, name))
        if re.search(r"(?:environ\.get|getenv)\(\s*\n?\s*[\"']%s[\"']" % FLAG, text):
            found.append("tools/" + name)
    crates = os.path.join(REPO, "crates")
    for base, dirs, files in os.walk(crates):
        dirs[:] = [d for d in dirs if d != "target"]
        for name in sorted(files):
            if name.endswith(".rs") and FLAG in _read(os.path.join(base, name)):
                found.append(os.path.relpath(os.path.join(base, name), REPO)
                             .replace(os.sep, "/"))
    return found


def _rust_reader_numbers():
    """(default, floor) the rust read site applies, and the expression it applies them in."""
    text = _read(RUST_READER)
    start = text.find("fn additional_context_char_limit(")
    if start < 0:
        return None, None, ""
    depth, i = 0, text.index("{", start)
    j = i
    while j < len(text):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                break
        j += 1
    body = text[i:j + 1]
    flat = " ".join(body.split())
    default = re.search(r"(?:env_number\(\s*\"%s\",\s*|unwrap_or\(\s*)([0-9_]+)"
                        % FLAG, flat)
    floor = re.search(r"\.max\(\s*([0-9_]+)|max\(\s*([0-9_]+)\s*,", flat)
    return (int(default.group(1).replace("_", "")) if default else None,
            int((floor.group(1) or floor.group(2)).replace("_", "")) if floor else None,
            flat)


def _measure(value):
    env = {k: v for k, v in os.environ.items() if k != FLAG}
    if value is not None:
        env[FLAG] = value
    result = subprocess.run([sys.executable, "-B", "-c", PROBE % TOOLS],
                            capture_output=True, text=True, cwd=TOOLS, env=env)
    if result.returncode != 0:
        raise AssertionError("probe failed for %r:\n%s" % (value, result.stderr[-600:]))
    return json.loads(result.stdout.strip().splitlines()[-1])


def _page_answer(value, floor, fallback):
    if value is None:
        return fallback
    try:
        return max(floor, int(str(value).strip()))
    except ValueError:
        return fallback


class ThePageStatesARuleTest(unittest.TestCase):

    def test_the_rule_is_still_on_the_page(self) -> None:
        """Every assertion below is derived from this sentence. If the page stops stating the
        rule, they stop deciding anything and would pass on nothing at all."""
        floor, fallback = _page_rule()
        self.assertIsNotNone(floor, "the page no longer states a floor for %s" % FLAG)
        self.assertIsNotNone(fallback, "the page no longer states a fallback for %s" % FLAG)
        self.assertGreater(fallback, floor, "a fallback below the floor is not a rule")

    def test_the_scan_found_every_reader(self) -> None:
        readers = _readers()
        self.assertGreaterEqual(
            len(readers), 3,
            "found %d readers of %s (%s). This file exists because the third one was missed; "
            "a scan that finds fewer than three has stopped matching"
            % (len(readers), FLAG, readers))


class ThePythonReadersFollowThePageTest(unittest.TestCase):
    """Measured, in a fresh process per case: the codex reader binds its default at import."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.floor, cls.fallback = _page_rule()
        cls.answers = {value: _measure(value) for value in CASES}

    def test_every_case_returned(self) -> None:
        """A probe that raised and was read for a key would report agreement it never measured."""
        self.assertEqual(len(CASES), len(self.answers))
        for value, answer in self.answers.items():
            self.assertIn("codex", answer, "no answer for %r" % (value,))
            self.assertIn("agent", answer, "no answer for %r" % (value,))

    def test_the_codex_reader_matches_the_page_everywhere(self) -> None:
        wrong = []
        for value in CASES:
            expect = _page_answer(value, self.floor, self.fallback)
            got = self.answers[value]["codex"]
            if got != expect:
                wrong.append("%r: page %d, reader %d" % (value, expect, got))
        self.assertEqual([], wrong, "; ".join(wrong))

    def test_the_agent_reader_differs_only_where_it_is_recorded(self) -> None:
        """Both directions. The recorded difference is about the DEFAULT -- what an unset or
        unreadable control falls back to -- and nothing else. If it ever reaches the floor, or
        stops applying, this says so instead of leaving a note describing a tree that moved."""
        own = RECORDED_OWN_BUDGET["agent"]
        unexpected, missing = [], []
        for value in CASES:
            expect = _page_answer(value, self.floor, self.fallback)
            got = self.answers[value]["agent"]
            falls_back = expect == self.fallback and (
                value is None or not str(value).strip().lstrip("-").isdigit())
            if falls_back:
                if got != own:
                    missing.append("%r: recorded own budget %d, reader %d" % (value, own, got))
            elif got != expect:
                unexpected.append("%r: page %d, reader %d" % (value, expect, got))
        self.assertEqual([], missing,
                         "the agent hook's own budget is recorded here and it no longer applies "
                         "it -- strike the record rather than leaving it: " + "; ".join(missing))
        self.assertEqual([], unexpected,
                         "the agent hook's difference is recorded as being about the default "
                         "alone, and it has spread: " + "; ".join(unexpected))


class TheRustReaderFollowsThePageTest(unittest.TestCase):

    def setUp(self) -> None:
        self.floor, self.fallback = _page_rule()
        self.default, self.applied_floor, self.body = _rust_reader_numbers()

    def test_the_read_site_was_found(self) -> None:
        self.assertTrue(self.body,
                        "no read site found in %s; this file is deciding nothing about it"
                        % os.path.relpath(RUST_READER, REPO))

    def test_it_falls_back_to_the_number_the_page_states(self) -> None:
        self.assertEqual(
            self.fallback, self.default,
            "the page says an unreadable value falls back to %s and the rust hook uses %s: %s"
            % (self.fallback, self.default, self.body))

    def test_it_applies_the_floor_the_page_states(self) -> None:
        self.assertEqual(
            self.floor, self.applied_floor,
            "the page says a value below %s is raised to it and the rust hook applies %s. "
            "Measured before this was true: 500 reached the hook as 500, and 0 as %s: %s"
            % (self.floor, self.applied_floor, self.fallback, self.body))

    def test_it_names_only_the_numbers_the_page_states(self) -> None:
        """A third number in the reader is a third rule.

        `.filter(|value| *value > 0)` was that rule: it made 0 and every negative mean "not set"
        rather than "below the floor", which is where the 40000 came from.

        This asks for the numbers rather than for that spelling, because asking for the spelling
        is a detector asking IS where it means CONTAINS. It was written the other way first and a
        mutation walked straight past it: re-adding the same rule as `if written > 0 { .. } else
        { 40_000 }` has no `filter` in it anywhere, and the guard passed. The `0` is what both
        spellings have in common, and what neither sentence on the page contains.
        """
        literals = {int(m.replace("_", ""))
                    for m in re.findall(r"(?<![A-Za-z0-9_])\d[\d_]*", self.body)}
        self.assertEqual(
            {self.floor, self.fallback}, literals,
            "the page states two numbers for this control, %s and %s, and the rust reader names "
            "%s. A number the page does not state is a rule it does not state: %s"
            % (self.floor, self.fallback, sorted(literals), self.body))

    def test_it_does_not_branch_on_the_value(self) -> None:
        """The same rule again, caught by its other half. A floor is arithmetic; anything that
        chooses between two answers is deciding something the page has not described."""
        branching = [word for word in ("if ", "else", "match ", "filter", "map_or",
                                       "then_some", "unwrap_or_else", "&&", "||")
                     if word in self.body]
        self.assertEqual(
            [], branching,
            "the rust reader branches on the control's value (%s), so it applies a rule beyond "
            "the floor and the fallback the page states: %s" % (branching, self.body))

    def test_it_reads_the_value_through_the_crate_vocabulary(self) -> None:
        """One reader, one set of answers about whitespace and unreadable values."""
        self.assertIn(
            "env_number", self.body,
            "the rust reader parses the control itself instead of calling "
            "env_flag::env_number: %s" % self.body)


if __name__ == "__main__":
    unittest.main()
