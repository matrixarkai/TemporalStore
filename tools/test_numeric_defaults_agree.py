#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two readers of one variable should agree about its numeric default.

`test_flag_readers_agree` asks this of booleans, where a disagreement shows up as a setting that
half-applies. A NUMBER drifts the same way and shows less: two readers with different fallbacks
agree on every value anyone sets, and part company only for the deployment that leaves the variable
alone -- which is most of them, and the one nobody tests.

`MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS` was the worked example, and is now struck off. The backend
resolved an omitted budget to 500000 while the two agent hooks passed 128000, so an operator who set
nothing got a quarter of the documented window through one path and all of it through another. The
schema used to quote the wrong one of those two numbers at callers, which is what
`test_matrixark_the_schema_quotes_the_budget_it_applies` was written for -- the same pair of numbers,
one layer up. Both readers now agree, so the entry goes: the rule below is that a list of known
differences which is allowed to go stale is read as a description of the tree.

Nothing is now listed unexamined. All five entries this file was written with have been looked at:
three were made to agree and struck, and two are differences that are real and must stay, each with
its reason recorded here and at the code that reads it. A new entry may be parked here unexamined --
that is what the list is for, and better than a quiet disagreement -- but it should not stay that
way, and the two kinds are told apart by whether the note begins JUSTIFIED.

An entry is struck when the numbers are made to AGREE. One that has been examined and found
justified stays listed -- the literals still differ, and this scan cannot tell a sentinel from a
default -- but its note says so and begins JUSTIFIED, so the two kinds are not confused.

`MATRIXARK_RETRIEVAL_TIMEOUT_MS` is the first of those. Its 0 is a sentinel: `retrieval_deadline_ms`
answers "what deadline was THIS request given", 0 means none was, and `default_stage_budgets` reads
it that way and computes no budgets. The MCP server's 30000 is a different layer -- how long it
waits for the tool call before abandoning it. Resolving the sentinel to 30000 would silently turn
stage budgeting on for every unbudgeted request, so these must not be made to agree.

`MATRIXARK_HTTP_PORT` is the second JUSTIFIED entry, and its note had guessed wrong: the 0 is not an
ephemeral port. `if args.http_port` is what reads it, so 0 means "stay on stdio" and any non-zero
value means "serve the portal instead". The gateway's 8080 is a bind port. Reading a note rather
than the code is how a difference stays unexamined while looking examined.

`MATRIXARK_READER_MAX_TOKENS` is struck. It was not two reader paths that happened to differ:
they are the HTTP and local-transformers backends of ONE reader, built from the same evidence
bundle and the same prompts, defaulting to 160 and 64. With the variable unset the two wrote
answers of different lengths, while the shared-model contract -- which requires both sides to
use 'the same reader output-token budget' -- reported 160 for both. Both now call
`reader_max_tokens_from_env()`, the resolver whose value the contract publishes, so there is
one literal left and it belongs to the contract.

The two TemporalStore SDK timeouts are struck. They were not a client/server difference: one option
with one meaning, declared by three parsers, defaulting to 20000 in the backend resolver and 60000
in both agent hooks. Every launcher supplies 60000 -- the three installers, the codex hook wrapper,
the topology waiter, and `matrixark_mcp_rust_server.sh`, which starts the very server the 20000
belonged to and passes the value explicitly. So the short number was reached only by a server
started outside every shipped path, and the resolver now agrees at 60000.
"""
from __future__ import annotations

import ast
import io
import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

#: The variable names this guard covers, lifted out of the old read regex so the
#: parse below shares one definition with it.
def _or_fallbacks(tree) -> Dict[int, object]:
    """id(read call) -> the constant after the `or`, for a blank-safe read.

    `get(NAME, "").strip() or "256"` states its default on the right of the `or`. Reading only the
    second argument finds "" there, which is not a number, and the whole variable then drops out of
    the scan -- silently, which is what the floor below exists to catch.
    """
    found: Dict[int, object] = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or)
                and len(node.values) == 2):
            continue
        right = node.values[1]
        if not isinstance(right, ast.Constant):
            continue
        for inner in ast.walk(node.values[0]):
            if isinstance(inner, ast.Call):
                found[id(inner)] = right.value
    return found


_NAME = re.compile(r'(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+')

_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']\s*,\s*'
    r'["\']?(-?\d+)["\']?\s*\)')

# Known, unexamined. Each is a variable whose readers do not agree on the number to use when it is
# unset. The note says what the difference looks like, not that it is right.
KNOWN_DISAGREEMENTS: Dict[str, str] = {
    "MATRIXARK_HTTP_PORT":
        "JUSTIFIED: the 0 in the two MCP entry points is a MODE -- stay on stdio -- and not an "
        "ephemeral port as this note first guessed. The 8080 is a bind port for the gateway, "
        "which exists to bind one. Said in the code at both --http-port arguments.",
    "MATRIXARK_RETRIEVAL_TIMEOUT_MS":
        "JUSTIFIED: the 0 is a sentinel, not a default -- no deadline was supplied for this "
        "request -- and the MCP server's 30000 is a different layer, how long it waits for the "
        "tool call. Said in the code at retrieval_deadline_ms.",
}

# A floor on the SCAN, set far from both failure modes rather than near the count.
#
# It was 120 against 196 when written, and the population has since fallen to 157 -- not because
# the scan broke, but because matrixarkai#1540 folded flags nothing could set and matrixarkai#1587
# gave thirty-two duplicated constants a single definition. Consolidating the forty-three that
# remain would take it to roughly 114 and breach a floor of 120, which would be this file failing
# on work that makes disagreement impossible: the same shape that broke
# `test_flag_readers_agree` and this file's own shared-constant check earlier today.
#
# A read-shape scan that has stopped matching finds approximately NOTHING. Sixty is far below
# anything consolidation reaches one module at a time, and far above zero.
EXPECTED_NUMERIC_READ_FLOOR = 60

#: And a positive control the count cannot give: matrixark_mcp_runtime_config is where these
#: constants are being consolidated TO, so its own numeric reads only grow. 38 sites today. If the
#: scan stops seeing that module it has stopped working, whatever the global count says.
EXPECTED_RUNTIME_CONFIG_READ_FLOOR = 20


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


def _numeric_reads() -> Dict[str, List[Tuple[str, int, str]]]:
    """Every os.environ read of a prefixed variable with a NUMERIC default, keyed by variable.

    PARSED, not matched line by line, and not restricted to integers. The previous scan here was
    a regex run over one line at a time with the pattern ``(-?\\d+)``, so two whole classes of read
    were invisible to it:

        every FLOAT default                    MATRIXARK_CROSS_SESSION_MIN_SCORE, 0.20
        every call a formatter split           os.environ.get(
                                                   "MATRIXARK_...",
                                                   "0.15",
                                               )

    Measured before the change: 28 variables and 38 reads outside its view. None of them disagreed
    with anything the scan already saw, so nothing was being hidden at that moment -- but a guard
    whose purpose is to fail when a second default appears could not have failed for any of them.

    This is the same fault `test_string_defaults_agree` was rewritten to fix, and the same fix. It
    was not carried across at the time; a formatting choice should not decide what a guard can see.
    """
    found: Dict[str, List[Tuple[str, int, str]]] = collections.defaultdict(list)
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        or_fallbacks = _or_fallbacks(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) != 2:
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr not in ("get", "getenv"):
                continue
            owner = func.value
            if isinstance(owner, ast.Attribute):
                is_env = owner.attr == "environ"
            elif isinstance(owner, ast.Name):
                is_env = owner.id in ("os", "environ")
            else:
                is_env = False
            if not is_env:
                continue
            key, default = node.args
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                continue
            if not _NAME.fullmatch(key.value):
                continue
            if not isinstance(default, ast.Constant):
                continue
            value = default.value
            if value == "" and id(node) in or_fallbacks:
                value = or_fallbacks[id(node)]     # the default sits past the `or`
            if isinstance(value, bool):
                continue                      # a bool is not a number here; booleans have their
                                              # own guard, and True would render as "True"
            if isinstance(value, (int, float)):
                text = repr(value)
            elif isinstance(value, str):
                text = value.strip()
                try:
                    float(text)
                except ValueError:
                    continue                  # a non-numeric default is the string guard's
            else:
                continue
            found[key.value].append((path, key.lineno, _canonical(text)))
    return found


def _canonical(text: str) -> str:
    """One spelling per value, so `"30000"` and `30000` are not read as a disagreement.

    The old scan compared the matched TEXT, which meant the quotes around a default decided whether
    two readers agreed. `int(os.environ.get(X, "0"))` and `os.environ.get(X, 0)` are the same
    default written twice."""
    try:
        number = float(text)
    except ValueError:
        return text
    return repr(int(number)) if number == int(number) else repr(number)


def _disagreeing() -> Set[str]:
    return {name for name, entries in _numeric_reads().items()
            if len({value for _, _, value in entries}) > 1}


class NumericDefaultsAgreeTest(unittest.TestCase):

    def test_the_scan_still_finds_numeric_reads(self) -> None:
        reads = _numeric_reads()
        self.assertGreaterEqual(
            len(reads), EXPECTED_NUMERIC_READ_FLOOR,
            "found %d variables read with a numeric default, expected at least %d -- if the read "
            "shape changed, the assertions below pass on an empty set"
            % (len(reads), EXPECTED_NUMERIC_READ_FLOOR))
        # The named control. A global count falls when duplication is REMOVED as well as when the
        # scan breaks; this module's own reads only grow as constants consolidate into it.
        runtime_sites = sum(1 for entries in reads.values() for entry in entries
                            if entry[0].endswith("matrixark_mcp_runtime_config.py"))
        self.assertGreaterEqual(
            runtime_sites, EXPECTED_RUNTIME_CONFIG_READ_FLOOR,
            "only %d numeric reads found in matrixark_mcp_runtime_config, which is where these "
            "constants live; below %d the scan has stopped matching the read shape rather than "
            "the tree having changed" % (runtime_sites, EXPECTED_RUNTIME_CONFIG_READ_FLOOR))

    def test_the_list_has_not_emptied(self) -> None:
        self.assertTrue(
            KNOWN_DISAGREEMENTS,
            "the list is empty, so the check below cannot tell a clean tree from a broken scan.")

    def test_no_new_variable_disagrees_about_its_default(self) -> None:
        reads = _numeric_reads()
        new = sorted(_disagreeing() - set(KNOWN_DISAGREEMENTS))
        detail = []
        for name in new:
            values = sorted({value for _, _, value in reads[name]})
            detail.append("%s (%s)" % (name, ", ".join(values)))
        self.assertEqual(
            [], detail,
            "these are read with more than one numeric default, so a deployment that sets nothing "
            "gets a different number depending on which path asks: %s\nMake them agree, or list it "
            "with what the difference is." % detail)

    def test_a_listed_variable_that_now_agrees_is_struck_off(self) -> None:
        settled = sorted(set(KNOWN_DISAGREEMENTS) - _disagreeing())
        self.assertEqual(
            [], settled,
            "these are listed as disagreeing and no longer do: %s. Strike them off -- a list of "
            "known differences that is allowed to go stale is read as a description of the tree."
            % settled)


if __name__ == "__main__":
    unittest.main()


class TheRankingWeightsAreWrittenOnceTest(unittest.TestCase):
    """`DEFAULT_TIME_WEIGHT` and `DEFAULT_BUSINESS_WEIGHT` are written in four places.

    Two are module constants -- `matrixark_mcp_core` and `matrixark_mcp_runtime_config` -- which
    core's own comment says are deliberately defined in both. The other two are the keyword
    defaults in `matrixark_mcp_scoring.final_recall_score`, spelled 0.18 and 0.22.

    `test_no_new_variable_disagrees_about_its_default` above cannot see the last two: it compares
    the fallback of an ENVIRONMENT READ, and a signature default is not one. So four numbers that
    have to move together had a check over two of them.

    They agree today, and only because the single caller of that function passes the constants in
    explicitly. A new caller that omits them gets 0.18 and 0.22 whatever the constants say, for
    every deployment that configures no weights -- which is most of them, and the one nobody tests.
    That is the failure this file's own docstring describes for numbers, one layer further in.
    """

    NAMES = ("DEFAULT_TIME_WEIGHT", "DEFAULT_BUSINESS_WEIGHT")

    @staticmethod
    def _module_constants(stem, _resolve_imports=True):
        """Module-scope constants, INCLUDING ones this module gets by importing them.

        A name a module imports from the other is that module's constant as far as any reader
        is concerned, and it is the strongest form of agreement available: one definition, so
        the two cannot drift. Reading only literal assignments made consolidation look like the
        constant had vanished -- which is how this file failed on matrixarkai#1587, where
        thirty-two duplicated literals were each reduced to a single definition.

        The `differ` check below is unaffected: two INDEPENDENT literal assignments still have
        to match, and that is the disagreement this file exists for. An imported name agreeing
        with itself is not a weaker answer than agreeing by hand -- it is the answer the hand
        version was approximating."""
        with io.open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        found = {}
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and "matrixark_" in node.module:
                for alias in node.names:
                    if alias.name.isupper():
                        imported.add((node.module.rsplit(".", 1)[-1],
                                      alias.asname or alias.name, alias.name))
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                    and isinstance(node.targets[0], ast.Name) \
                    and isinstance(node.value, ast.Constant):
                found[node.targets[0].id] = node.value.value
        if _resolve_imports:
            for module, bound, original in sorted(imported):
                if bound in found:
                    continue
                source = TheRankingWeightsAreWrittenOnceTest._module_constants(module, False)
                if original in source:
                    found[bound] = source[original]
        return found

    @staticmethod
    def _signature_defaults(stem, function):
        with io.open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function:
                names = [a.arg for a in node.args.kwonlyargs]
                values = [d.value if isinstance(d, ast.Constant) else None
                          for d in node.args.kw_defaults]
                return dict(zip(names, values))
        return {}

    def test_the_two_modules_agree_about_the_weights(self) -> None:
        core = self._module_constants("matrixark_mcp_core")
        runtime = self._module_constants("matrixark_mcp_runtime_config")
        for name in self.NAMES:
            with self.subTest(constant=name):
                self.assertIn(name, core, "%s is no longer a plain constant in core" % name)
                self.assertIn(name, runtime, "%s is no longer a plain constant in runtime" % name)
                self.assertEqual(
                    core[name], runtime[name],
                    "%s is %r in matrixark_mcp_core and %r in matrixark_mcp_runtime_config. Both "
                    "are read as the default weight for a deployment that sets none, so which one "
                    "a request gets depends on which module served it."
                    % (name, core[name], runtime[name]))

    def test_the_two_modules_agree_about_EVERY_shared_constant(self) -> None:
        """Not only the two weights. Thirty bare literals are defined in both, pinned by nothing.

        `matrixark_mcp_core` and `matrixark_mcp_runtime_config` define **78** module constants
        under the same names. Forty-three of those read an environment variable, which is the shape
        `test_no_new_variable_disagrees_about_its_default` above compares, so those were covered.
        Thirty are bare literals and five are expressions, and nothing looked at either.

        That split moved recently and not by accident. matrixarkai#1540 folded fifty-seven flags
        that nothing sets into the value they already produced -- correct on its own terms, and
        every folded value was compared against pristine main before it shipped -- but a read like
        `int(os.environ.get("MATRIXARK_DIRECT_WRITE_THROTTLE_MS", "") or "0")` becoming `0` turns a
        duplication this file could see into one it could not. The values are identical today
        because the fold could not change them. Keeping them identical is what stopped having a
        check.

        The two modules are deliberately duplicated -- core says so in its own comment -- so the
        rule is not "there must not be two". It is that the two must agree, which is exactly what a
        deliberate duplication needs and the one thing nobody was asserting.
        """
        core = self._module_constants("matrixark_mcp_core")
        runtime = self._module_constants("matrixark_mcp_runtime_config")
        shared = sorted(set(core) & set(runtime))
        # A floor from what it is FOR, not from a count. `_module_constants` reads plain literal
        # assignments only, which is exactly the uncovered set -- the env reads above are the other
        # guard's business -- so this sees thirty of the seventy-eight names the two modules share.
        # An extractor that stopped matching returns approximately nothing; ten fails loudly on
        # that and does not move when a constant is added or folded.
        # The floor is on the EXTRACTOR, not on the shared set.
        #
        # It required more than ten names in BOTH modules, reasoning that an extractor which had
        # stopped matching returns approximately nothing. True while the two modules held thirty
        # duplicated literals; false as a floor, because the shared set also falls when the
        # duplication is REMOVED -- and it fell to zero in matrixarkai#1587, which gave each of
        # those literals a single definition. A floor that cannot tell "the scan broke" from
        # "there is nothing left to disagree" fails on the better outcome.
        #
        # So it asks what it was really asking: can this scan find constants at all.
        runtime_only = self._module_constants("matrixark_mcp_runtime_config", False)
        self.assertGreater(
            len(runtime_only), 10,
            "only %d literal constants were parsed out of matrixark_mcp_runtime_config; the "
            "extractor has stopped recognising them and everything below compares nothing"
            % len(runtime_only))
        differ = {name: (core[name], runtime[name])
                  for name in shared if core[name] != runtime[name]}
        self.assertEqual(
            {}, differ,
            "these constants are defined in BOTH matrixark_mcp_core and "
            "matrixark_mcp_runtime_config with different values. Both modules are read as the "
            "default for a deployment that sets nothing, so which one a request gets depends on "
            "which module served it -- the failure this file's own docstring describes, between "
            "two modules instead of two readers: %r" % (differ,))

    def test_the_signature_defaults_are_the_constants(self) -> None:
        core = self._module_constants("matrixark_mcp_core")
        signature = self._signature_defaults("matrixark_mcp_scoring", "final_recall_score")
        for name, keyword in zip(self.NAMES, ("default_time_weight", "default_business_weight")):
            with self.subTest(keyword=keyword):
                self.assertIn(keyword, signature,
                              "final_recall_score no longer takes %s" % keyword)
                self.assertEqual(
                    core[name], signature[keyword],
                    "final_recall_score defaults %s to %r while %s is %r. Every caller passes the "
                    "constant in today, so the two agree by habit rather than by construction -- "
                    "and a caller that omits it silently ranks on the older number."
                    % (keyword, signature[keyword], name, core[name]))

    def test_there_is_one_blend(self) -> None:
        """A delegation is an import and a return; a second implementation is not.

        matrixark_mcp_core_scoring held its own copy of the arithmetic until this was written.
        """
        implementations = []
        for entry in sorted(os.listdir(TOOLS)):
            if not entry.startswith("matrixark_") or not entry.endswith(".py"):
                continue
            try:
                with io.open(os.path.join(TOOLS, entry), encoding="utf-8",
                             errors="replace") as handle:
                    tree = ast.parse(handle.read())
            except (SyntaxError, OSError):
                continue
            for node in tree.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if node.name != "final_recall_score":
                    continue
                body = [s for s in node.body
                        if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))]
                if len(body) > 3:
                    implementations.append(entry[:-3])
        self.assertEqual(
            ["matrixark_mcp_scoring"], sorted(implementations),
            "the ranking blend has more than one implementation: %r. The rule is the strong form "
            "test_there_is_one_cosine states -- not that the copies must agree, but that there "
            "must not be two." % (sorted(implementations),))
