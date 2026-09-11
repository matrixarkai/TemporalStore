#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A blank newer spelling falls through to the older one.

Several flags exist as a pair: a newer name and the older name it replaced, resolved as a chain so
a deployment that set the old one keeps working. Ten of those chains were written

    get(NEW, get(OLD, default))

which consults OLD only when NEW is ABSENT. A variable that is PRESENT AND BLANK -- what
`export NEW=$UNSET` leaves behind, and what clearing a field means -- resolved to "" and OLD was
never read, however correctly it was set.

Measured before the fix, with the older spelling set correctly every time:

    MATRIXARK_ANTHROPIC_TIMEOUT_SEC=    matrixark_mcp_core FAILED TO IMPORT
                                        ValueError: could not convert string to float: ''
    MATRIXARK_UNDERSTANDING_PROVIDER=   extraction ran the local rules engine while the portal
                                        probe reported the Anthropic endpoint working, and the
                                        portal routed the model to MATRIXARK_EXTRACTION_MODEL,
                                        which an Anthropic deployment ignores entirely
    MATRIXARK_TEMPORALSTORE_RUST_PROXY= the MATRIXARK_TEMPORALSTORE_RUST_CLI fallback was skipped

The numeric one is the sharp end: a blank value on the newer name takes the serving path down at
startup, and the traceback names `float`, not the variable.

`.strip()` as well as `or`, because `float("  5  ")` is 5.0 while `float("   ")` raises, so a
whitespace-only value fails exactly as an empty one does. The tree already held that principle one
layer up -- `test_an_empty_variable_is_not_an_override` asserts a whitespace-only
MATRIXARK_ANTHROPIC_TIMEOUT_SEC is not an override -- so the reporting layer called blank "not set"
while the consuming layer treated it as a value and crashed on it.

Two halves, because they fail independently. The SHAPE check is what a new chain trips over; the
BEHAVIOUR checks are what say the shape rule is about something. Each behaviour check is bracketed
by two controls -- the variable absent, and the variable set -- because a resolver that ignored the
newer name entirely would satisfy "blank behaves like absent" while being completely broken.

`precedence_pairs` below is exported. Three files ask which name wins over which, and they asked it
three ways: a regex here, an AST walk there, and this one. Rewriting the chains to `or` made the
other two blind at once -- both said so through their own floors, which is what floors are for --
and teaching the new spelling to three copies is how the third copy goes stale. The recognition is
shared; the file list and the filtering stay with each caller, because their scopes genuinely
differ.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
import unittest

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TOOLS_DIR)

#: Eleven pairs when this was written -- six between two flags this project owns, five falling
#: back to a variable it does not (OPENAI_BASE_URL, OPENAI_MODEL, USERNAME). A floor, so a scanner
#: that stops recognising the chain shape fails here rather than passing with nothing to look at.
EXPECTED_CHAIN_FLOOR = 10

_READERS = {"get", "getenv", "_env"}


def _name_of(node) -> str:
    func = node.func
    return func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")


def _reads_environment(node) -> bool:
    """Whether this call takes its value from the process environment.

    `_env` counts: matrixark_v1_gateway defines one, and it is
    `os.environ.get(name, default).strip()` -- the same two-argument semantics under another name,
    which a scan looking only for `os.environ.get` walks straight past.
    """
    name = _name_of(node)
    if name == "_env":
        return True
    if name not in ("get", "getenv"):
        return False
    owner = getattr(node.func, "value", None)
    return ((isinstance(owner, ast.Attribute) and owner.attr == "environ")
            or (isinstance(owner, ast.Name) and owner.id in ("os", "environ")))


def _first_string(node) -> str:
    if not isinstance(node, ast.Call) or not node.args:
        return ""
    first = node.args[0]
    return first.value if isinstance(first, ast.Constant) and isinstance(first.value, str) else ""


def precedence_pairs(paths, *, env_only: bool = False, prefixes: tuple = ()) -> list:
    """(winner, loser, module, line, form) for every fallback chain, in EITHER spelling.

    form is "two_arg" for `get(A, get(B, d))` and "or" for `get(A) or get(B) or d`. The two mean
    different things on a blank A -- that difference is this file's subject -- but both state the
    same precedence, which is what the other callers are asking about.

    `paths` are file paths relative to the repository root. Each caller passes its own list and
    does its own filtering: one asks about the whole repository, one about `tools/` alone, and this
    file about live modules only. Sharing the scope as well as the recognition would silently move
    three floors at once.
    """
    def wanted(winner: str, loser: str) -> bool:
        if not winner or not loser:
            return False
        if not prefixes:
            return True
        return winner.startswith(prefixes) and loser.startswith(prefixes)

    found = []
    for rel in paths:
        stem = os.path.basename(rel)
        try:
            with open(os.path.join(REPO_ROOT, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue

        for node in ast.walk(tree):
            # get(A, get(B, ...)) -- B is reached only when A is ABSENT.
            if isinstance(node, ast.Call) and len(node.args) > 1 \
                    and _name_of(node) in _READERS \
                    and (not env_only or _reads_environment(node)):
                winner = _first_string(node)
                for child in ast.walk(node.args[1]):
                    if not (isinstance(child, ast.Call) and _name_of(child) in _READERS):
                        continue
                    if env_only and not _reads_environment(child):
                        continue
                    loser = _first_string(child)
                    if wanted(winner, loser):
                        found.append((winner, loser, stem, node.lineno, "two_arg"))
                    break

            # get(A) or get(B) or d -- B is reached when A is absent OR blank.
            if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
                named = []
                for value in node.values:
                    for child in ast.walk(value):
                        if isinstance(child, ast.Call) and _name_of(child) in _READERS \
                                and (not env_only or _reads_environment(child)):
                            text = _first_string(child)
                            if text:
                                named.append(text)
                            break
                for index in range(len(named) - 1):
                    if wanted(named[index], named[index + 1]):
                        found.append((named[index], named[index + 1], stem, node.lineno, "or"))
    return found


def _unreachable() -> set:
    import importlib.util

    path = os.path.join(TOOLS_DIR, "test_a_module_only_tests_reach_is_not_live.py")
    spec = importlib.util.spec_from_file_location("_reach_for_blank_chain", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    out: set = set()
    for members in module.UNREACHABLE.values():
        out.update(members)
    return out


def live_sources() -> list:
    """Tracked non-test modules a request can actually reach."""
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO_ROOT,
                            capture_output=True, text=True, check=False).stdout.split()
    skip = _unreachable()
    return [rel for rel in listed
            if not os.path.basename(rel).startswith("test_")
            and os.path.basename(rel)[:-3] not in skip]


def _chains() -> tuple:
    """Chains whose OUTER name is a flag this project owns, whatever it falls back to.

    No prefix filter on the second name. A fall back to OPENAI_BASE_URL or USERNAME is the same
    arrangement -- "use mine if set, otherwise the one your deployment already exports" -- and a
    blank outer name cancels it identically. Filtering both names would have left seven of those
    unguarded, which is exactly where they were found.
    """
    rows = [row for row in precedence_pairs(live_sources(), env_only=True)
            if row[0].startswith(("MATRIXARK_", "TS_"))]
    return ([row for row in rows if row[4] == "two_arg"],
            [row for row in rows if row[4] == "or"])


def _probe(source: str, **environment) -> subprocess.CompletedProcess:
    """Run `source` in a fresh interpreter with `environment` applied.

    A fresh process because the values under test are resolved at MODULE SCOPE: setting the
    variable after the import would measure the import that already happened.

    A value of None removes the variable, so "absent" and "present and blank" are distinguishable
    -- which is the entire subject of this file.
    """
    env = {"PATH": "/usr/bin:/bin", "HOME": os.environ.get("HOME", "/root"),
           "PYTHONPATH": REPO_ROOT + os.pathsep + TOOLS_DIR}
    for name, value in environment.items():
        if value is not None:
            env[name] = value
    return subprocess.run([sys.executable, "-c", source], cwd=REPO_ROOT, capture_output=True,
                          text=True, timeout=300, env=env)


_NUMERIC = """
import sys; sys.path.insert(0, "tools")
import matrixark_mcp_core as core
print(repr(core.ANTHROPIC_LLM_TIMEOUT_SEC), repr(core.ANTHROPIC_LLM_MAX_TOKENS))
"""

_PROVIDER = """
import sys, importlib
core = importlib.import_module("tools.matrixark_mcp_core")
cfg = importlib.import_module("tools.matrixark_gateway_config")
print(repr(core.understanding_provider()),
      repr(cfg._configured_extraction_provider()),
      repr(cfg._env_name(cfg.SETTINGS_BY_KEY["extraction.model"], {})))
"""


class ABlankFlagFallsThroughToTheOlderNameTest(unittest.TestCase):

    def test_no_chain_is_written_with_the_two_argument_form(self) -> None:
        """The shape. `get(NEW, get(OLD, d))` reads OLD only when NEW is absent, so a blank NEW
        stops the chain -- which is never what a pair of spellings is for."""
        two_arg, _or_form = _chains()
        self.assertEqual(
            [], ["%s -> %s (%s:%d)" % row[:4] for row in sorted(two_arg)],
            "these chains consult the older spelling only when the newer one is ABSENT, so a "
            "blank newer variable resolves to \"\" and the older one is never read. Write each "
            "step with `.strip() or`.")

    def test_the_scan_still_finds_the_chains(self) -> None:
        """A floor. With the shape check above passing over nothing, it would pass forever."""
        _two_arg, or_form = _chains()
        pairs = {(row[0], row[1]) for row in or_form}
        self.assertGreaterEqual(
            len(pairs), EXPECTED_CHAIN_FLOOR,
            "the chain scan found %d flag pairs, below the floor of %d -- it has stopped "
            "recognising the shape, so the check above is passing over nothing"
            % (len(pairs), EXPECTED_CHAIN_FLOOR))

    def test_a_blank_numeric_flag_does_not_stop_the_process(self) -> None:
        """The one that is not a wrong value. Before this, a blank newer name handed float() the
        empty string at module scope and the whole serving path failed to import."""
        blank = _probe(_NUMERIC,
                       MATRIXARK_ANTHROPIC_TIMEOUT_SEC="", MATRIXARK_ANTHROPIC_MAX_TOKENS="",
                       MATRIXARK_EXTRACTION_TIMEOUT_SEC="77",
                       MATRIXARK_EXTRACTION_MAX_TOKENS="4321")
        self.assertEqual(
            0, blank.returncode,
            "a blank MATRIXARK_ANTHROPIC_* stopped the import even though the older spelling was "
            "set:\n" + (blank.stderr or "")[-800:])
        self.assertEqual("77.0 4321", blank.stdout.strip(),
                         "a blank newer name did not fall through to the older spelling")

    def test_a_whitespace_only_flag_falls_through_too(self) -> None:
        """`float("  5  ")` is 5.0 but `float("   ")` raises, so `or` alone does not cover this:
        a whitespace-only value is truthy. The override reporter already treats it as unset."""
        padded = _probe(_NUMERIC,
                        MATRIXARK_ANTHROPIC_TIMEOUT_SEC="   ",
                        MATRIXARK_ANTHROPIC_MAX_TOKENS="  ",
                        MATRIXARK_EXTRACTION_TIMEOUT_SEC="77",
                        MATRIXARK_EXTRACTION_MAX_TOKENS="4321")
        self.assertEqual(0, padded.returncode,
                         "a whitespace-only newer name stopped the import:\n"
                         + (padded.stderr or "")[-800:])
        self.assertEqual("77.0 4321", padded.stdout.strip(),
                         "a whitespace-only newer name did not fall through")

    def test_absent_and_blank_agree_and_an_explicit_value_still_wins(self) -> None:
        """Both controls. Without the second, a resolver that ignored the newer variable
        altogether would satisfy the first and be entirely broken."""
        absent = _probe(_NUMERIC, MATRIXARK_EXTRACTION_TIMEOUT_SEC="77",
                        MATRIXARK_EXTRACTION_MAX_TOKENS="4321")
        self.assertEqual("77.0 4321", absent.stdout.strip(),
                         "with the newer name absent the older spelling should be used")

        explicit = _probe(_NUMERIC,
                          MATRIXARK_ANTHROPIC_TIMEOUT_SEC="5", MATRIXARK_ANTHROPIC_MAX_TOKENS="9",
                          MATRIXARK_EXTRACTION_TIMEOUT_SEC="77",
                          MATRIXARK_EXTRACTION_MAX_TOKENS="4321")
        self.assertEqual("5.0 9", explicit.stdout.strip(),
                         "an explicitly set newer name must win over the older spelling")

    def test_a_blank_provider_resolves_as_an_absent_one_does(self) -> None:
        """The provider chain, through the resolvers the serving path and the portal actually
        call -- including `_env_name`, which decides WHICH variable the portal writes the model
        into, and whose own comment records that getting it wrong configures nothing silently."""
        blank = _probe(_PROVIDER, MATRIXARK_UNDERSTANDING_PROVIDER="",
                       MATRIXARK_EXTRACTION_PROVIDER="anthropic")
        absent = _probe(_PROVIDER, MATRIXARK_EXTRACTION_PROVIDER="anthropic")
        self.assertEqual(0, blank.returncode, (blank.stderr or "")[-800:])
        self.assertEqual(0, absent.returncode, (absent.stderr or "")[-800:])
        self.assertEqual(
            absent.stdout.strip(), blank.stdout.strip(),
            "a blank MATRIXARK_UNDERSTANDING_PROVIDER resolves differently from an absent one, so "
            "clearing the field changes the provider, the summary provider, and the variable the "
            "portal writes the extraction model into")
        self.assertIn("MATRIXARK_ANTHROPIC_MODEL", blank.stdout,
                      "on an Anthropic deployment the portal must write the model into the "
                      "variable Anthropic reads")

    def test_an_explicit_provider_still_wins(self) -> None:
        """The control for the check above."""
        explicit = _probe(_PROVIDER, MATRIXARK_UNDERSTANDING_PROVIDER="openai_compatible",
                          MATRIXARK_EXTRACTION_PROVIDER="anthropic")
        self.assertEqual(0, explicit.returncode, (explicit.stderr or "")[-800:])
        self.assertIn("openai_compatible", explicit.stdout,
                      "an explicitly set provider must win over the older spelling")


if __name__ == "__main__":
    unittest.main()
