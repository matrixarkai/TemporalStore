#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every setting declares the default the build actually runs.

`export_settings(include_defaults=True)` writes a declared default to the target as an EXPLICIT
value, so a default that disagrees with the code does not merely mislead a reader -- it
reconfigures a clone. That was fixed once for the five frozen retrieval knobs, whose registry
numbers were 10x to 156x what retrieval uses. Six settings written by hand had the same problem:

    skills.shared_resource_budget_ratio        portal said 0.10   build runs 0.25
    retrieval.cross_session_budget_ratio       portal said ""     build runs 0.12
    retrieval.cross_session_max_sessions       portal said ""     build runs 3
    ingestion.time_compression_window_events   portal said ""     build runs 64
    extraction.base_url                        portal said ""     build calls http://127.0.0.1:8000/v1
    extraction.model                           portal said ""     build asks for qwen2.5:1.5b

The check is a sweep, not a list of six: it walks every setting, finds the literal fallback its
variable is read with anywhere under ``tools/``, and requires the two to agree.

**Every exemption is derived, not written down.** A hand-maintained skip list is how a sweep quietly
stops covering things, and the first version of this file had two of them. A blank default is
correct in exactly two shapes, and both are readable from the source:

* the variable is read as ``get(THIS, get(THAT, ...))`` where ``THAT`` is another setting's variable
  -- the setting FOLLOWS that one, and blank is what "follow it" looks like;
* the variable is read with several different literals, one per provider branch, so there is no
  single default the portal could honestly name.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

TRUE = {"1", "true", "yes", "on"}
SETTING_VARIABLES = {s.env for s in cfg.SETTINGS if s.env}


def _get_call(node: ast.AST):
    """The (variable, second argument) of an ``os.environ.get(VAR, ...)``, or None."""
    if not isinstance(node, ast.Call) or len(node.args) != 2:
        return None
    target = node.func
    if not (isinstance(target, ast.Attribute) and target.attr in {"get", "getenv"}):
        return None
    first, second = node.args
    if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
        return None
    return first.value, second


def read_shapes() -> tuple:
    """({variable: {literal, ...}}, {variable: {what it falls back to, ...}}).

    The second is what makes the exemptions derivable: a variable that falls back to ANOTHER
    setting's variable has no default of its own, and the literal at the end of the chain belongs
    to the setting at the end of the chain.
    """
    literals: dict = {}
    follows: dict = {}
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
            call = _get_call(node)
            if call is None:
                continue
            variable, second = call
            if isinstance(second, ast.Constant) and isinstance(second.value, (str, int, float)):
                literals.setdefault(variable, set()).add(str(second.value))
                continue
            inner = _get_call(second)
            if inner is not None:
                follows.setdefault(variable, set()).add(inner[0])
                deepest = [a.value for a in ast.walk(second)
                           if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                if deepest:
                    literals.setdefault(variable, set()).add(deepest[-1])
            elif isinstance(second, ast.Name):
                # An indirection through a module constant: this parser cannot say what it holds,
                # so it must not claim the setting has a wrong default either.
                follows.setdefault(variable, set()).add(second.id)
    return literals, follows


def truth(value: str) -> bool:
    return str(value).strip().lower() in TRUE


def same_value(setting, left: str, right: str) -> bool:
    """Agreement in MEANING, not spelling: `""` and `"0"` are both off for a boolean, and reporting
    that as a disagreement is how a sweep gets ignored."""
    if setting.kind == "bool":
        return truth(left) == truth(right)
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def classify() -> tuple:
    """(disagreements, settings compared, settings exempt) -- the exemptions derived, not listed."""
    literals, follows = read_shapes()
    wrong, compared, exempt = [], [], []
    for setting in cfg.SETTINGS:
        if not setting.env:
            continue
        chain = follows.get(setting.env, set())
        if chain & SETTING_VARIABLES:
            exempt.append(setting.key)  # follows another setting; blank is what that looks like
            continue
        seen = literals.get(setting.env)
        if not seen:
            continue  # nothing to compare against
        if len(seen) > 1 and str(setting.default) == "":
            exempt.append(setting.key)  # per-branch literals; no single default to name
            continue
        compared.append(setting.key)
        if not any(same_value(setting, literal, str(setting.default)) for literal in seen):
            wrong.append("%s (%s): portal says %r, code falls back to %s"
                         % (setting.key, setting.env, setting.default,
                            " / ".join(sorted(repr(x) for x in seen))))
    return wrong, compared, exempt


class TheParseFoundSomethingTest(unittest.TestCase):
    """Everything below compares against what this finds. If it finds nothing, the sweep passes by
    having nothing to say."""

    def test_it_reads_the_modules(self) -> None:
        literals, follows = read_shapes()
        self.assertIn("MATRIXARK_CROSS_SESSION_BUDGET_RATIO", literals)
        self.assertIn("MATRIXARK_MAX_SELECTED_REFS", literals)
        self.assertGreater(len(literals), 100)
        self.assertTrue(follows, "no fallback chains found, so every exemption would be a guess")

    def test_it_finds_the_chain_that_makes_a_summary_follow_extraction(self) -> None:
        """The shape the first exemption rests on, named once, so a parser change that stops seeing
        it fails here rather than turning into a false disagreement.

        It used to be the summary MODEL following the extraction model. There is no summary model
        any more -- a node summary is made by the extraction endpoint, so it uses the extraction
        model and nothing else -- and the summary PROVIDER follows the extraction provider the same
        way, which is the shape this rule is about.
        """
        _literals, follows = read_shapes()
        self.assertIn("MATRIXARK_UNDERSTANDING_PROVIDER",
                      follows.get("MATRIXARK_SUMMARY_PROVIDER", set()))

    def test_it_finds_a_variable_read_differently_per_provider(self) -> None:
        """The shape the second exemption rests on."""
        literals, _follows = read_shapes()
        self.assertGreater(len(literals.get("MATRIXARK_EMBEDDING_API_KEY_ENV", set())), 1)


class EverySettingDeclaresWhatTheBuildRunsTest(unittest.TestCase):

    def test_no_declared_default_disagrees_with_the_code(self) -> None:
        wrong, _compared, _exempt = classify()
        self.assertEqual([], wrong, "\n  ".join([""] + wrong))

    def test_the_sweep_actually_compares_something(self) -> None:
        """The sweep passes by finding no disagreement, so it has to be finding COMPARISONS.
        Without this it would go quiet the moment the parse stopped matching the code's shape, and
        read exactly the same as a clean result."""
        _wrong, compared, _exempt = classify()
        self.assertGreaterEqual(len(compared), 30,
                                "the sweep is comparing %d settings" % len(compared))

    def test_the_exemptions_are_the_minority(self) -> None:
        """An exemption rule that swallowed the registry would make the sweep vacuous with no list
        to notice it happening."""
        _wrong, compared, exempt = classify()
        self.assertLess(len(exempt), len(compared))

    def test_a_boolean_agrees_by_meaning_not_by_spelling(self) -> None:
        """The sweep found ingestion.embed_drainer declaring "0" against a code fallback of "" --
        both off. A sweep that reports that is a sweep people stop reading."""
        setting = cfg.SETTINGS_BY_KEY["ingestion.embed_drainer"]
        self.assertEqual("bool", setting.kind)
        self.assertTrue(same_value(setting, "", "0"))
        self.assertFalse(same_value(setting, "", "1"))
        self.assertFalse(same_value(cfg.SETTINGS_BY_KEY["skills.shared_resource_budget_ratio"],
                                    "0.10", "0.25"))

    def test_the_six_that_were_wrong_are_compared_and_agree(self) -> None:
        """Named explicitly so the sweep cannot pass by classifying them as exempt."""
        _wrong, compared, _exempt = classify()
        for key in cfg._EXPLICIT_BUILD_DEFAULT:
            with self.subTest(setting=key):
                self.assertIn(key, compared, "no longer compared, so no longer covered")
                self.assertNotEqual("", cfg.SETTINGS_BY_KEY[key].default)

    def test_the_help_says_what_the_deployment_runs(self) -> None:
        for key in cfg._EXPLICIT_BUILD_DEFAULT:
            with self.subTest(setting=key):
                setting = cfg.SETTINGS_BY_KEY[key]
                self.assertIn("With nothing set this deployment runs", setting.help)
                self.assertIn(str(setting.default), setting.help)

    def test_the_value_is_read_not_retyped(self) -> None:
        """The file's own rule: a table of values here is the second copy it refuses to keep."""
        with open(os.path.join(TOOLS, "matrixark_gateway_config.py"), encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("_EXPLICIT_BUILD_DEFAULT = {")
        block = source[start:source.index("_apply_build_defaults(SETTINGS)", start)]
        for value in ("0.25", "0.12", "64", "3", "qwen2.5:1.5b"):
            self.assertNotIn('"%s"' % value, block,
                             "%s is written here instead of read from the build" % value)


class TheExtractionEndpointIsTheOneTheCallReachesTest(unittest.TestCase):
    """Three surfaces described one endpoint three ways: the call posted to the build default, the
    panel showed an empty field, and the connection test refused to run at all."""

    VARIABLES = ("MATRIXARK_EXTRACTION_BASE_URL", "MATRIXARK_EXTRACTION_MODEL",
                 "MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
                 "OPENAI_BASE_URL", "OPENAI_MODEL", "OPENAI_API_KEY")

    def setUp(self) -> None:
        self._saved = {n: os.environ.get(n) for n in self.VARIABLES}
        for name in self.VARIABLES:
            os.environ.pop(name, None)
        self._post_json, self._load = cfg._post_json, cfg.load
        cfg.load = lambda: {"values": {}}   # never read the deployment's real settings file
        self.calls: list = []

        def recorder(url, payload, headers, timeout):
            self.calls.append({"url": url, "model": payload.get("model")})
            return 200, {"model": payload.get("model"),
                         "choices": [{"message": {"content": "pong"}}]}

        cfg._post_json = recorder

    def tearDown(self) -> None:
        cfg._post_json, cfg.load = self._post_json, self._load
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_the_probe_tests_the_endpoint_the_call_would_reach(self) -> None:
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "openai_compatible"
        result = cfg.probe(["extraction"], 5.0)
        self.assertEqual(1, len(self.calls), result)
        self.assertTrue(self.calls[0]["url"].startswith(
            str(cfg.SETTINGS_BY_KEY["extraction.base_url"].default)), self.calls)
        self.assertEqual(cfg.SETTINGS_BY_KEY["extraction.model"].default, self.calls[0]["model"])

    def test_it_no_longer_refuses_a_deployment_that_would_work(self) -> None:
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "openai_compatible"
        entry = cfg.probe(["extraction"], 5.0)["results"][0]
        self.assertNotEqual("incomplete_config", entry.get("error"))
        self.assertTrue(entry.get("ok"))

    def test_a_configured_endpoint_still_wins(self) -> None:
        """The floor: if the default overrode what was set, every deployment would be probed at
        localhost."""
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "openai_compatible"
        os.environ["MATRIXARK_EXTRACTION_BASE_URL"] = "https://api.example/v1"
        os.environ["MATRIXARK_EXTRACTION_MODEL"] = "gpt-4o-mini"
        cfg.probe(["extraction"], 5.0)
        self.assertEqual("https://api.example/v1/chat/completions", self.calls[0]["url"])
        self.assertEqual("gpt-4o-mini", self.calls[0]["model"])

    def test_a_provider_that_calls_nothing_is_still_skipped(self) -> None:
        os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = "deterministic"
        entry = cfg.probe(["extraction"], 5.0)["results"][0]
        self.assertEqual([], self.calls)
        self.assertTrue(entry["skipped"])


if __name__ == "__main__":
    unittest.main()
