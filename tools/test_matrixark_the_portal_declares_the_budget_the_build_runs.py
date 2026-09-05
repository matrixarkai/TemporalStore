#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every setting declares the default the build actually runs.

The frozen retrieval knobs were fixed once: the registry's number was 10x to 156x what retrieval
uses, and `export_settings(include_defaults=True)` writes a declared default to the target as an
EXPLICIT value, so cloning a deployment raised its budgets by up to 156x. Four settings written by
hand had the same problem and were not covered:

    skills.shared_resource_budget_ratio        declared 0.10   build runs 0.25
    retrieval.cross_session_budget_ratio       declared ""     build runs 0.12
    retrieval.cross_session_max_sessions       declared ""     build runs 3
    ingestion.time_compression_window_events   declared ""     build runs 64

A blank is the worse of the two: the field reads as "nothing is in force" on a deployment that is
running a number, and a clone taking the blank as explicit gets an empty environment variable where
the source had a value.

The check below is not a list of those four. It walks EVERY setting, finds the literal fallback its
variable is read with in the tools tree, and requires the two to agree -- so a setting added later
with a default nobody checked fails here rather than being found the same way these were.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

try:
    from tools import matrixark_mcp_runtime_config as runtime  # type: ignore
except ImportError:
    import matrixark_mcp_runtime_config as runtime  # type: ignore

# Settings whose blank default MEANS something other than "nothing": it means "follow the provider",
# and the code's fallback is per provider rather than one value. Each is asserted below to have more
# than one fallback in the code, so nothing can be parked here to silence a genuine disagreement.
FOLLOWS_THE_PROVIDER = {
    "extraction.api_key_env", "embedding.api_key_env", "embedding.api_base",
}
# The extraction endpoint and model are a separate question -- the connection probe refuses to test
# a deployment that has not set them, while the extraction call would happily reach the build
# default -- so the two surfaces have to move together, in their own change.
DEFERRED = {"extraction.base_url", "extraction.model", "summary.provider", "summary.model"}


def literal_fallbacks() -> dict:
    """{variable: {literal, ...}} for every ``os.environ.get(VAR, "literal")`` under tools/."""
    found: dict = {}
    for name in sorted(os.listdir(TOOLS)):
        if (not name.endswith(".py") or name.startswith("test_")
                or name == "matrixark_gateway_config.py"):
            continue
        try:
            with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
                tree = ast.parse(handle.read(), filename=name)
        except SyntaxError:  # pragma: no cover - a module this build cannot parse
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) != 2:
                continue
            target = node.func
            if not (isinstance(target, ast.Attribute) and target.attr in {"get", "getenv"}):
                continue
            first, second = node.args
            if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
                continue
            if isinstance(second, ast.Constant) and isinstance(second.value, (str, int, float)):
                found.setdefault(first.value, set()).add(str(second.value))
            elif isinstance(second, ast.Call):
                # get(A, get(B, "literal")) -- the innermost literal decides when nothing is set.
                inner = [a for a in ast.walk(second)
                         if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                if inner:
                    found.setdefault(first.value, set()).add(inner[-1].value)
    return found


TRUE = {"1", "true", "yes", "on"}


def truth(value: str) -> bool:
    """The truthiness the code applies to a boolean variable: anything not in TRUE is off."""
    return str(value).strip().lower() in TRUE


def same_number(left: str, right: str) -> bool:
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def same_value(setting, left: str, right: str) -> bool:
    """Agreement in MEANING, not in spelling.

    A boolean is the case that makes the difference: `""` and `"0"` are both off, and reporting
    them as a disagreement would be a false alarm the sweep gets ignored for.
    """
    if setting.kind == "bool":
        return truth(left) == truth(right)
    return same_number(left, right)


class TheParseFoundSomethingTest(unittest.TestCase):
    """Everything below is a comparison against what this finds. If it finds nothing, the sweep
    passes by having nothing to say."""

    def test_it_reads_the_retrieval_modules(self) -> None:
        found = literal_fallbacks()
        self.assertIn("MATRIXARK_CROSS_SESSION_BUDGET_RATIO", found)
        self.assertIn("MATRIXARK_MAX_SELECTED_REFS", found)
        self.assertGreater(len(found), 100)

    def test_the_settings_registry_is_populated(self) -> None:
        self.assertGreater(len([s for s in cfg.SETTINGS if s.env]), 80)


class EverySettingDeclaresWhatTheBuildRunsTest(unittest.TestCase):

    def test_no_declared_default_disagrees_with_the_code(self) -> None:
        found = literal_fallbacks()
        wrong = []
        for setting in cfg.SETTINGS:
            if not setting.env or setting.key in DEFERRED or setting.key in FOLLOWS_THE_PROVIDER:
                continue
            literals = found.get(setting.env)
            if not literals:
                continue  # nothing to compare against
            # Several literals means the fallback is per provider (or two modules disagree about
            # it); matching ANY of them is the most this test can require. Skipping such variables
            # instead was a hole: a mutation that changed one module's fallback went unreported
            # because it made the variable multi-literal.
            if not any(same_value(setting, literal, str(setting.default))
                       for literal in literals):
                wrong.append("%s (%s): portal says %r, code falls back to %s"
                             % (setting.key, setting.env, setting.default,
                                " / ".join(sorted(repr(x) for x in literals))))
        self.assertEqual([], wrong, "\n  ".join([""] + wrong))

    def test_the_sweep_actually_compares_something(self) -> None:
        """The sweep above passes by finding no disagreement, so it has to be finding COMPARISONS.
        Without this it would go quiet the moment the parse stopped matching the code's shape, and
        read exactly the same as a clean result."""
        found = literal_fallbacks()
        compared = [s for s in cfg.SETTINGS
                    if s.env and s.key not in DEFERRED and s.key not in FOLLOWS_THE_PROVIDER
                    and found.get(s.env)]
        self.assertGreaterEqual(len(compared), 30,
                                "the sweep is comparing %d settings; it used to compare more"
                                % len(compared))

    def test_a_boolean_agrees_by_meaning_not_by_spelling(self) -> None:
        """The sweep found ingestion.embed_drainer declaring "0" against a code fallback of "" --
        both off. A sweep that reports that is a sweep people stop reading."""
        setting = cfg.SETTINGS_BY_KEY["ingestion.embed_drainer"]
        self.assertEqual("bool", setting.kind)
        self.assertTrue(same_value(setting, "", "0"))
        self.assertFalse(same_value(setting, "", "1"))
        self.assertFalse(same_value(cfg.SETTINGS_BY_KEY["skills.shared_resource_budget_ratio"],
                                    "0.10", "0.25"))

    def test_the_four_that_were_wrong_now_agree(self) -> None:
        """Named explicitly so the sweep above cannot pass by skipping them."""
        for key, constant in (
                ("retrieval.cross_session_budget_ratio", "DEFAULT_CROSS_SESSION_BUDGET_RATIO"),
                ("retrieval.cross_session_max_sessions", "DEFAULT_CROSS_SESSION_MAX_SESSIONS"),
                ("skills.shared_resource_budget_ratio", "DEFAULT_SHARED_RESOURCE_BUDGET_RATIO"),
                ("ingestion.time_compression_window_events", "TIME_COMPRESSION_WINDOW_EVENTS")):
            with self.subTest(setting=key):
                self.assertTrue(same_number(str(getattr(runtime, constant)),
                                            str(cfg.SETTINGS_BY_KEY[key].default)))

    def test_none_of_them_is_blank_any_more(self) -> None:
        """A blank reads as "nothing is in force" on a deployment that is running a number."""
        for key in cfg._EXPLICIT_BUILD_DEFAULT:
            with self.subTest(setting=key):
                self.assertNotEqual("", cfg.SETTINGS_BY_KEY[key].default)

    def test_the_help_says_what_the_deployment_runs(self) -> None:
        for key in cfg._EXPLICIT_BUILD_DEFAULT:
            with self.subTest(setting=key):
                setting = cfg.SETTINGS_BY_KEY[key]
                self.assertIn("With nothing set this deployment runs", setting.help)
                self.assertIn(str(setting.default), setting.help)

    def test_the_number_is_read_not_retyped(self) -> None:
        """The file's own rule: a table of numbers here is the second copy it refuses to keep."""
        with open(os.path.join(TOOLS, "matrixark_gateway_config.py"), encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("_EXPLICIT_BUILD_DEFAULT = {")
        block = source[start:source.index("_apply_build_defaults(SETTINGS)", start)]
        for constant in cfg._EXPLICIT_BUILD_DEFAULT.values():
            self.assertIn(constant, block)
        # The table AND the function that applies it: a mutation that re-typed the numbers inside
        # the function passed while only the table was inspected.
        for number in ("0.25", "0.12", "64", "3"):
            self.assertNotIn('"%s"' % number, block,
                             "%s is written here instead of read from the build" % number)


class TheExemptionsAreRealTest(unittest.TestCase):
    """A skip list is how a sweep quietly stops covering things, so each entry has to earn it."""

    def test_a_blank_that_follows_the_provider_really_has_several_fallbacks(self) -> None:
        found = literal_fallbacks()
        for key in FOLLOWS_THE_PROVIDER:
            with self.subTest(setting=key):
                setting = cfg.SETTINGS_BY_KEY[key]
                self.assertEqual("", setting.default, "no longer a follow-the-provider blank")
                self.assertGreater(len(found.get(setting.env, set())), 1,
                                   "one fallback, so this is a plain disagreement, not a blank "
                                   "that means 'follow the provider'")

    def test_the_deferred_ones_are_still_deferred_for_a_reason(self) -> None:
        """If one of these gains a matching default on its own, it should leave the list rather
        than sit here looking like an exemption that is still needed."""
        found = literal_fallbacks()
        still_disagreeing = [
            key for key in DEFERRED
            if len(found.get(cfg.SETTINGS_BY_KEY[key].env, set())) == 1
            and not same_number(next(iter(found[cfg.SETTINGS_BY_KEY[key].env])),
                                str(cfg.SETTINGS_BY_KEY[key].default))]
        self.assertTrue(still_disagreeing,
                        "nothing in DEFERRED disagrees any more; drop the list")

    def test_every_exempt_key_exists(self) -> None:
        for key in FOLLOWS_THE_PROVIDER | DEFERRED:
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)


if __name__ == "__main__":
    unittest.main()
