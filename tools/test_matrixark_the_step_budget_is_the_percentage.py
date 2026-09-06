#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A percentage of the context budget decides each section of a pack, not a ceiling beside it.

Four sections of a pack are sized as a percentage of the context budget, and each has an absolute
ceiling as a backstop. At the shipped defaults the backstop was doing the sizing:

    context budget 500,000 tokens
    skills     10% -> 50,000    ceiling 8,192     ->  8,192 allowed
    resources  25% -> 125,000   ceiling 16,384    -> 16,384 allowed

So a customer setting a percentage got a sixth of it, and none of the four ceilings was reachable
from the portal to notice or change. The ceilings are raised to sit above what each percentage
yields at the default context budget -- asserted here rather than eyeballed -- and offered, so the
backstop is a decision rather than a surprise.

The panel used to report ``max_budget_tokens`` from ``MATRIXARK_MAX_BUDGET_TOKENS``, a variable
NOTHING reads: not the packer, not the engine, not the config file. It now reports what the packer
computes, and says which of the two limits bound.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

try:
    from tools import matrixark_v1_gateway as gw  # type: ignore
except ImportError:
    import matrixark_v1_gateway as gw  # type: ignore
try:
    from tools import matrixark_mcp_runtime_config as runtime  # type: ignore
    from tools import matrixark_mcp_budget_policies as policies  # type: ignore
    from tools import matrixark_mcp_core as core  # type: ignore
except ImportError:
    import matrixark_mcp_runtime_config as runtime  # type: ignore
    import matrixark_mcp_budget_policies as policies  # type: ignore
    import matrixark_mcp_core as core  # type: ignore

cfg = gw._gwconfig

# (ratio constant, ceiling constant) for each section sized this way.
SECTIONS = {
    "skills": ("DEFAULT_SHARED_SKILL_BUDGET_RATIO", "DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS"),
    "resources": ("DEFAULT_SHARED_RESOURCE_BUDGET_RATIO",
                  "DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS"),
    "cross-session": ("DEFAULT_CROSS_SESSION_BUDGET_RATIO",
                      "DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS"),
    "profile": ("DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO",
                "DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS"),
}
OFFERED = ("skills.shared_skill_max_budget_tokens", "skills.shared_resource_max_budget_tokens",
           "retrieval.cross_session_max_budget_tokens",
           "retrieval.cross_session_profile_max_budget_tokens")


class TheCeilingIsAboveWhatThePercentageYieldsTest(unittest.TestCase):
    """The property, not the four numbers. A ceiling below the percentage is the defect; asserting
    the numbers themselves would need editing every time either side moves."""

    def test_every_section_is_decided_by_its_percentage(self) -> None:
        total = runtime.DEFAULT_MAX_CONTEXT_TOKENS
        for name, (ratio_name, ceiling_name) in SECTIONS.items():
            with self.subTest(section=name):
                by_percentage = int(total * getattr(runtime, ratio_name))
                ceiling = getattr(runtime, ceiling_name)
                self.assertGreaterEqual(
                    ceiling, by_percentage,
                    "%s: %d%% of %d is %d, and the ceiling allows only %d -- the percentage is not "
                    "what decides" % (name, getattr(runtime, ratio_name) * 100, total,
                                      by_percentage, ceiling))

    def test_the_two_modules_agree_about_every_ceiling(self) -> None:
        """Each is defined twice, in runtime_config and in mcp_core. Raising one and not the other
        would give two answers depending on which module did the packing."""
        for _name, (_ratio, ceiling_name) in SECTIONS.items():
            with self.subTest(constant=ceiling_name):
                self.assertEqual(getattr(runtime, ceiling_name), getattr(core, ceiling_name))

    def test_the_ceilings_are_still_a_backstop_not_a_blank_cheque(self) -> None:
        """The floor for the rule above: setting them all to something enormous would satisfy it and
        remove the protection. Each stays within an order of magnitude of what its percentage
        yields."""
        total = runtime.DEFAULT_MAX_CONTEXT_TOKENS
        for name, (ratio_name, ceiling_name) in SECTIONS.items():
            with self.subTest(section=name):
                by_percentage = int(total * getattr(runtime, ratio_name))
                self.assertLess(getattr(runtime, ceiling_name), by_percentage * 10)


class TheCeilingsAreReachableTest(unittest.TestCase):
    """None of the four was offered, so the percentage was the only control a customer had -- and it
    was the one being overridden."""

    def test_each_one_is_offered(self) -> None:
        for key in OFFERED:
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)

    def test_each_declares_what_the_build_runs(self) -> None:
        for key in OFFERED:
            with self.subTest(setting=key):
                setting = cfg.SETTINGS_BY_KEY[key]
                self.assertEqual(str(os.environ.get(setting.env)
                                     or getattr(runtime, "DEFAULT_" + setting.env[len("MATRIXARK_"):])),
                                 setting.default)

    def test_each_says_it_is_a_backstop_and_not_the_way_to_size_a_section(self) -> None:
        """The mistake this fixes is reading a ceiling as the size control. The help has to say so
        where a customer is about to make it."""
        for key in OFFERED:
            with self.subTest(setting=key):
                help_text = cfg.SETTINGS_BY_KEY[key].help.lower()
                self.assertTrue("ceiling" in help_text or "backstop" in help_text, help_text)


class ThePanelReportsWhatThePackerComputesTest(unittest.TestCase):

    def test_nothing_reads_the_variable_that_was_reported(self) -> None:
        """A READ, not a mention: naming a retired variable in a comment is how the next reader
        finds out it was retired, and the comment above `_shared_budget_summary` does exactly that.
        Reporting a value nobody applies is the defect."""
        reads = []
        for name in sorted(os.listdir(TOOLS)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
                text = handle.read()
            for pattern in ('_env("MATRIXARK_MAX_BUDGET_TOKENS"',
                            'environ.get("MATRIXARK_MAX_BUDGET_TOKENS"',
                            'getenv("MATRIXARK_MAX_BUDGET_TOKENS"'):
                if pattern in text:
                    reads.append(name)
        self.assertEqual([], reads)

    def test_the_detector_would_find_a_read(self) -> None:
        """The floor: if the patterns above stopped matching how this codebase reads a variable,
        the assertion would pass on a build that had put the phantom budget straight back."""
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn('_env("MATRIXARK_SHARED_SKILL_BUDGET_RATIO"', text)

    def test_it_reports_the_packers_own_answer(self) -> None:
        budgets = gw._shared_budget_summary()
        total = runtime.DEFAULT_MAX_CONTEXT_TOKENS
        policy = policies.build_shared_context_policy({}, {}, remote_budget_tokens=total)
        self.assertEqual(total, budgets["context_budget_tokens"])
        self.assertEqual(policy["skill_budget_tokens"], budgets["skills"]["tokens"])
        self.assertEqual(policy["resource_budget_tokens"], budgets["resources"]["tokens"])

    def test_it_shows_the_percentage_and_what_it_works_out_to(self) -> None:
        """Both, because a percentage without its token count is not a decision anyone can make."""
        section = gw._shared_budget_summary()["skills"]
        self.assertEqual(round(runtime.DEFAULT_SHARED_SKILL_BUDGET_RATIO * 100, 2),
                         section["percent"])
        self.assertEqual(int(runtime.DEFAULT_MAX_CONTEXT_TOKENS
                             * runtime.DEFAULT_SHARED_SKILL_BUDGET_RATIO),
                         section["by_percentage_tokens"])

    def test_nothing_is_ceiling_bound_at_the_defaults(self) -> None:
        budgets = gw._shared_budget_summary()
        for name in ("skills", "resources"):
            with self.subTest(section=name):
                self.assertEqual("percentage", budgets[name]["bound_by"])

    def test_the_panel_carries_it(self) -> None:
        self.assertTrue(gw._model_config_snapshot()["skills"]["budgets"]["available"])


class ACeilingThatOverridesThePercentageIsReportedTest(unittest.TestCase):
    """The state this shipped in, and the one a customer can still create by lowering a ceiling."""

    def setUp(self) -> None:
        self._saved = os.environ.get("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS")

    def tearDown(self) -> None:
        if self._saved is None:
            os.environ.pop("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS", None)
        else:
            os.environ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS"] = self._saved

    def summary_with_ceiling(self, ceiling: int) -> dict:
        """The constants are bound at import, so the ceiling is applied the way a request can: under
        `shared_context`, which is where the packer reads its per-request policy from. Passing it at
        the top level instead silently changed nothing, and the assertion below caught that."""
        total = runtime.DEFAULT_MAX_CONTEXT_TOKENS
        return policies.build_shared_context_policy(
            {"shared_context": {"skill_max_budget_tokens": ceiling}}, {},
            remote_budget_tokens=total)

    def test_a_low_ceiling_really_does_override_the_percentage(self) -> None:
        """The premise of the warning, measured rather than assumed."""
        total = runtime.DEFAULT_MAX_CONTEXT_TOKENS
        by_percentage = int(total * runtime.DEFAULT_SHARED_SKILL_BUDGET_RATIO)
        policy = self.summary_with_ceiling(8192)
        self.assertLess(policy["skill_budget_tokens"], by_percentage)
        self.assertEqual(8192, policy["skill_budget_tokens"])

    PROBE = """
import json, sys
sys.path.insert(0, %r)
import matrixark_v1_gateway as gw
print(json.dumps([w for w in gw._model_config_snapshot()['warnings']
                  if 'share of a pack' in w]))
"""

    def warnings_with(self, **overrides) -> list:
        """The ceilings are module-scope constants bound at import, so a ceiling is applied the way
        a deployment applies one: in the environment of a process that has not started yet."""
        environment = dict(os.environ)
        for name in ("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS",
                     "MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS"):
            environment.pop(name, None)
        environment.update(overrides)
        result = subprocess.run([sys.executable, "-c", self.PROBE % TOOLS], capture_output=True,
                                text=True, timeout=600, env=environment, cwd=TOOLS)
        self.assertEqual(0, result.returncode, result.stderr[-400:])
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_the_warning_fires_when_a_ceiling_binds(self) -> None:
        """Measured, not read. Reading the source for the branch let a mutation that disabled it
        pass -- this is the state a customer can still create by lowering a ceiling."""
        warnings = self.warnings_with(MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS="8192")
        self.assertEqual(1, len(warnings), warnings)
        self.assertIn("8192", warnings[0])
        self.assertIn(str(int(runtime.DEFAULT_MAX_CONTEXT_TOKENS
                              * runtime.DEFAULT_SHARED_SKILL_BUDGET_RATIO)), warnings[0])

    def test_nothing_is_reported_when_no_ceiling_binds(self) -> None:
        """The floor: a warning that always fires says nothing."""
        self.assertEqual([], self.warnings_with())

    def test_the_warning_says_both_numbers(self) -> None:
        """A customer cannot act on "the ceiling bound" without knowing what it bound instead of."""
        source = open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8").read()
        warning = source[source.index('"The " + _name + " share of a pack'):]
        warning = warning[:warning.index("if _gwconfig.embedding_provider_effect")]
        self.assertIn("by_percentage_tokens", warning)
        self.assertIn("ceiling_tokens", warning)
        self.assertIn("raise the ceiling", warning)


if __name__ == "__main__":
    unittest.main()
