#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The budget the agent gets is not the one the panel showed.

The portal's budget panel quoted `MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS` -- 500,000 -- and worked
every section's token count out of it. The agent hooks have never used that variable first. They
read `MATRIXARK_HOOK_MAX_CONTEXT_TOKENS`, fall back to the one the panel quotes, and fall back again
to a number of their own; and the installation manual sets the first to **10,000**.

So on a deployment installed by the book, the panel said skills were allowed 50,000 tokens and the
agent path allowed 1,000. Fifty times out, on the path that serves agents.

The expression lived inline in two hook scripts, which is why nothing could report it: a duplicated
literal in two standalone scripts is not something a panel can ask. It is one resolver now, the
panel reports both budgets, and neither is called *the* budget.

The safety property this suite exists for: the resolver must answer exactly what the inline
expression answered, on every shape of environment. These hooks run on live sessions.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_mcp_runtime_config as runtime  # noqa: E402
import matrixark_v1_gateway as gateway  # noqa: E402

HOOKS = ("matrixark_agent_hook.py", "matrixark_codex_hook.py")
VARIABLES = ("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS")

# What the two hooks used to compute inline. Kept here as the thing the resolver is checked
# against, so "unchanged behaviour" is a measurement rather than an assurance.
INLINE = ("int(os.environ.get('MATRIXARK_HOOK_MAX_CONTEXT_TOKENS', "
          "os.environ.get('MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS', '128000')))")


class Case(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = {name: os.environ.get(name) for name in VARIABLES}
        for name in VARIABLES:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


class TheResolverAnswersWhatTheInlineExpressionDidTest(unittest.TestCase):
    """These hooks run on live sessions, so the only acceptable behaviour change is none."""

    CASES = (
        ({}, "nothing set"),
        ({"MATRIXARK_HOOK_MAX_CONTEXT_TOKENS": "10000"}, "the manual's install"),
        ({"MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS": "500000"}, "only the API budget set"),
        ({"MATRIXARK_HOOK_MAX_CONTEXT_TOKENS": "10000",
          "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS": "500000"}, "both set"),
        ({"MATRIXARK_HOOK_MAX_CONTEXT_TOKENS": " 64000 "}, "padded"),
    )

    def probe(self, extra):
        script = ("import os, sys\n"
                  "sys.path.insert(0, %r)\n" % TOOLS +
                  "import matrixark_mcp_runtime_config as rc\n"
                  "print('%d %d' % (rc.hook_max_context_tokens(), " + INLINE + "))\n")
        env = {k: v for k, v in os.environ.items() if k not in VARIABLES}
        env.update(extra)
        out = subprocess.run([sys.executable, "-c", script],
                             capture_output=True, text=True, env=env, timeout=600)
        self.assertEqual(0, out.returncode, out.stderr)
        return out.stdout.split()

    def test_it_agrees_on_every_shape_of_environment(self) -> None:
        for extra, label in self.CASES:
            with self.subTest(case=label):
                resolver, inline = self.probe(extra)
                self.assertEqual(inline, resolver)

    def test_the_cases_are_not_all_the_same_answer(self) -> None:
        """The floor: five cases that all resolve to one number would agree trivially."""
        answers = {self.probe(extra)[0] for extra, _label in self.CASES}
        self.assertGreater(len(answers), 2, "the cases exercise only %d answers" % len(answers))


class NeitherHookRepeatsTheExpressionTest(unittest.TestCase):
    """A literal duplicated across two standalone scripts is not something a panel can ask."""

    def test_the_hooks_no_longer_carry_their_own_fallback(self) -> None:
        for hook in HOOKS:
            with open(os.path.join(TOOLS, hook), encoding="utf-8") as handle:
                source = handle.read()
            with self.subTest(hook=hook):
                self.assertNotIn('os.environ.get("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"', source)
                self.assertIn("_hook_max_context_tokens()", source)

    def test_the_build_has_exactly_one_number_for_it(self) -> None:
        """The floor: the rule above is satisfied by deleting the fallback altogether, which would
        be a behaviour change rather than a consolidation."""
        self.assertEqual(128000, runtime.DEFAULT_HOOK_MAX_CONTEXT_TOKENS)
        os.environ.pop("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", None)
        os.environ.pop("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", None)
        self.assertEqual(128000, runtime.hook_max_context_tokens())


class ABadValueDoesNotBecomeTheBudgetTest(Case):

    def test_nonsense_falls_through_to_the_next_source(self) -> None:
        for raw in ("", "   ", "not-a-number", "0", "-5"):
            with self.subTest(value=raw):
                os.environ["MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"] = raw
                os.environ["MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"] = "500000"
                self.assertEqual(500000, runtime.hook_max_context_tokens())

    def test_and_to_the_build_default_when_neither_is_usable(self) -> None:
        os.environ["MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"] = "nonsense"
        os.environ["MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"] = "also nonsense"
        self.assertEqual(runtime.DEFAULT_HOOK_MAX_CONTEXT_TOKENS,
                         runtime.hook_max_context_tokens())

    def test_a_good_value_is_still_taken(self) -> None:
        os.environ["MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"] = "24000"
        self.assertEqual(24000, runtime.hook_max_context_tokens())


class ThePanelReportsBothBudgetsTest(Case):

    def panel(self):
        return gateway._shared_budget_summary()

    def test_both_paths_are_named(self) -> None:
        paths = {row["path"] for row in self.panel()["paths"]}
        self.assertEqual({"api", "agent_hooks"}, paths)

    def test_the_manual_install_is_reported_as_it_is(self) -> None:
        """The case the panel was wrong about: 10,000 for the agent, 500,000 for an API caller."""
        os.environ["MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"] = "10000"
        os.environ["MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"] = "500000"
        rows = {row["path"]: row for row in self.panel()["paths"]}
        self.assertEqual(500000, rows["api"]["context_budget_tokens"])
        self.assertEqual(10000, rows["agent_hooks"]["context_budget_tokens"])
        # And the section figures follow the budget they belong to, rather than one of them being
        # quoted for both -- which is what the panel used to do.
        self.assertGreater(rows["api"]["sections"]["skills"],
                           rows["agent_hooks"]["sections"]["skills"] * 10)

    def test_it_says_when_they_differ(self) -> None:
        os.environ["MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"] = "10000"
        os.environ["MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"] = "500000"
        self.assertTrue(self.panel()["paths_differ"])

    def test_and_when_they_do_not(self) -> None:
        """The floor: a flag hard-coded to True would pass the test above."""
        os.environ["MATRIXARK_HOOK_MAX_CONTEXT_TOKENS"] = "500000"
        os.environ["MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"] = "500000"
        self.assertFalse(self.panel()["paths_differ"])

    def test_the_rows_the_panel_already_had_are_unchanged(self) -> None:
        """This is additive. Anything reading the old shape keeps reading it."""
        panel = self.panel()
        for key in ("context_budget_tokens", "skills", "resources"):
            self.assertIn(key, panel)
        self.assertIn("percent", panel["skills"])
        self.assertIn("bound_by", panel["skills"])


class ThePortalOffersItTest(unittest.TestCase):
    """The help for the budget beside it already told operators to set this one. A flag whose text
    advises an operator has to be reachable."""

    KEY = "retrieval.hook_max_context_tokens"

    def test_it_is_offered(self) -> None:
        self.assertIn(self.KEY, cfg.SETTINGS_BY_KEY)

    def test_it_takes_effect_without_a_restart(self) -> None:
        self.assertEqual("live", cfg.SETTINGS_BY_KEY[self.KEY].applies)

    def test_the_declared_default_is_the_one_the_build_runs(self) -> None:
        """The resolver reads its variables through a loop, so the declared-default sweep cannot
        pair them automatically. This is that comparison, made by hand because it has to be."""
        self.assertEqual(runtime.DEFAULT_HOOK_MAX_CONTEXT_TOKENS,
                         int(cfg.SETTINGS_BY_KEY[self.KEY].default))

    def test_the_neighbouring_setting_still_names_it(self) -> None:
        """The premise: the other budget's help is what advised operators to set this. If that text
        went away, this setting would need to explain itself rather than lean on it."""
        neighbour = cfg.SETTINGS_BY_KEY["retrieval.default_max_context_tokens"]
        self.assertIn("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", neighbour.help)


class TheHooksStillLoadTest(unittest.TestCase):
    """They are started per turn on live sessions. An import error here is a dead agent."""

    def test_each_hook_imports(self) -> None:
        for hook in HOOKS:
            module = hook[:-3]
            with self.subTest(hook=module):
                out = subprocess.run(
                    [sys.executable, "-c", "import sys; sys.path.insert(0, %r); import %s"
                     % (TOOLS, module)],
                    capture_output=True, text=True, timeout=600)
                self.assertEqual(0, out.returncode, out.stderr[-600:])


if __name__ == "__main__":
    unittest.main()
