#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A budget ceiling applies to the next pack, not the next restart.

The four per-section ceilings were module-scope constants: read once when the process imported them,
so an operator who raised one watched the portal accept the value and the packer keep using the old
one until somebody restarted the deployment. The portal said ``restart`` and was telling the truth,
which is worse than it sounds -- a ceiling is a decision about the shape of the next pack, and the
next pack is when it is meant.

They are read per pack now. The constants are still there and are still the answer when nothing is
set, so a deployment that configures none of this does not move.

``live`` is derived here, not asserted: a test parses the modules and fails if the value goes back to
being bound at import, which is the only thing that would make the portal's promise false again.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_mcp_budget_policies as policies  # noqa: E402
# Reached through `matrixark_mcp_core`, which re-exports it: importing the split module directly
# hits a circular import, and it is the core module every caller uses anyway.
import matrixark_mcp_core as core  # noqa: E402
import matrixark_mcp_runtime_config as runtime  # noqa: E402

# setting key -> (variable, the constant it falls back to)
CEILINGS = {
    "skills.shared_skill_max_budget_tokens":
        ("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS", "DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS"),
    "skills.shared_resource_max_budget_tokens":
        ("MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS",
         "DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS"),
    "retrieval.cross_session_max_budget_tokens":
        ("MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS", "DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS"),
    "retrieval.cross_session_profile_max_budget_tokens":
        ("MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS",
         "DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS"),
}
VARIABLES = tuple(variable for variable, _constant in CEILINGS.values())


def module_scope_reads(filename: str) -> set:
    """Variables a module binds in a top-level assignment: read once, at import."""
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=filename)
    found = set()
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Call) or not sub.args:
                continue
            target = sub.func
            reads = ((isinstance(target, ast.Attribute) and target.attr in {"get", "getenv"})
                     or (isinstance(target, ast.Name) and target.id in {"getenv", "live_int"}))
            first = sub.args[0]
            if reads and isinstance(first, ast.Constant) and isinstance(first.value, str):
                found.add(first.value)
    return found


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

    # The shared-context policy exists in TWO modules. Driving one and not the other is how half a
    # deployment honours a setting -- and a mutation reverting only the second copy passed until
    # this took both.
    def copies(self):
        scoring = sys.modules["matrixark_mcp_core_scoring"]
        return {"budget_policies": policies.build_shared_context_policy,
                "core_scoring": scoring.build_shared_context_policy}

    def shared(self, section: str, which: str = "budget_policies") -> int:
        policy = self.copies()[which](
            {}, {}, remote_budget_tokens=runtime.DEFAULT_MAX_CONTEXT_TOKENS)
        return int(policy["%s_budget_tokens" % section])

    def cross_session(self, *, profile: bool) -> int:
        policy = core.build_cross_session_policy(
            {}, {}, question_type="broad", session_scope="cross_session",
            remote_budget_tokens=runtime.DEFAULT_MAX_CONTEXT_TOKENS)
        return int(policy.get("max_budget_tokens") or 0)


class TheCeilingIsReadPerPackTest(Case):

    def test_both_copies_of_the_shared_policy_are_live(self) -> None:
        """`matrixark_mcp_budget_policies` and `matrixark_mcp_core_scoring` each carry one. Which
        one runs depends on the caller, so a ceiling honoured by one of them is a setting that works
        on some requests."""
        for which in self.copies():
            for section, variable in (("skill", "MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS"),
                                      ("resource", "MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS")):
                with self.subTest(copy=which, section=section):
                    os.environ.pop(variable, None)
                    before = self.shared(section, which)
                    os.environ[variable] = "2048"
                    self.assertEqual(2048, self.shared(section, which))
                    self.assertNotEqual(before, 2048)
                    os.environ.pop(variable, None)

    def test_there_are_still_two_copies_to_check(self) -> None:
        """The floor: if they were ever consolidated, the loop above would silently cover one
        module twice, and the test would keep passing while covering less."""
        self.assertEqual(2, len({id(fn) for fn in self.copies().values()}))

    def test_lowering_one_takes_effect_without_a_restart(self) -> None:
        """The whole change, from the operator's side: set it, and the next pack is smaller."""
        before = self.shared("skill")
        os.environ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS"] = "4096"
        self.assertEqual(4096, self.shared("skill"))
        self.assertNotEqual(before, self.shared("skill"))

    def test_the_resource_ceiling_too(self) -> None:
        os.environ["MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS"] = "8192"
        self.assertEqual(8192, self.shared("resource"))

    def test_the_cross_session_ceiling_too(self) -> None:
        before = self.cross_session(profile=False)
        os.environ["MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS"] = "1024"
        self.assertNotEqual(before, self.cross_session(profile=False))

    def test_clearing_it_goes_back_to_the_build_default(self) -> None:
        """A live setting has to be un-settable, or it is a one-way door with extra steps."""
        os.environ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS"] = "4096"
        self.assertEqual(4096, self.shared("skill"))
        os.environ.pop("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS")
        self.assertEqual(
            min(runtime.DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS,
                int(runtime.DEFAULT_MAX_CONTEXT_TOKENS
                    * runtime.DEFAULT_SHARED_SKILL_BUDGET_RATIO)),
            self.shared("skill"))


class ABadValueDoesNotZeroASectionTest(Case):
    """A ceiling that resolved to nothing would cut its section to zero. Ignoring a bad value is the
    smaller error, and it is the same choice `explicit_int` makes for the retrieval budgets."""

    def test_nonsense_falls_back(self) -> None:
        for raw in ("", "   ", "not-a-number", "12.5", "-1", "0"):
            with self.subTest(value=raw):
                os.environ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS"] = raw
                self.assertEqual(runtime.DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS,
                                 runtime.live_int("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS",
                                                  runtime.DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS))

    def test_a_good_value_is_still_taken(self) -> None:
        """The floor: a resolver that ignored everything would satisfy the test above."""
        os.environ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS"] = " 4096 "
        self.assertEqual(4096, runtime.live_int("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS", 1))


class TheLiveClaimIsEarnedTest(unittest.TestCase):
    """`live` is a promise the portal makes on save. Here it is derived from where the read is."""

    def test_the_portal_says_live(self) -> None:
        for key in CEILINGS:
            with self.subTest(setting=key):
                self.assertEqual("live", cfg.SETTINGS_BY_KEY[key].applies)

    def test_no_ceiling_is_bound_at_import_where_it_is_used(self) -> None:
        """The constants are still defined at module scope -- that is the fallback. What must not
        happen is a CONSUMER binding one, which is what made these restart-scoped before."""
        for filename in ("matrixark_mcp_budget_policies.py", "matrixark_mcp_core_scoring.py"):
            bound = module_scope_reads(filename)
            for variable in VARIABLES:
                with self.subTest(module=filename, variable=variable):
                    self.assertNotIn(variable, bound)

    def test_the_detector_would_notice_an_import_time_read(self) -> None:
        """The floor for the rule above, on a variable that IS still captured at import.

        It used to name the four ceilings themselves, because they were captured in
        `runtime_config` -- and then this change stopped capturing them, which is the whole point,
        so the floor was asserting the defect it exists to detect. The RATIO beside each ceiling is
        still bound at import and still labelled restart, so it is the honest subject.
        """
        bound = module_scope_reads("matrixark_mcp_runtime_config.py")
        self.assertIn("MATRIXARK_SHARED_SKILL_BUDGET_RATIO", bound)
        self.assertIn("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", bound)

    def test_the_ceilings_are_no_longer_captured_anywhere(self) -> None:
        """What replaced it: the constants are plain build defaults now, so clearing a setting
        falls back to the number in the source rather than to whatever this process started with."""
        for filename in ("matrixark_mcp_runtime_config.py", "matrixark_mcp_core.py"):
            bound = module_scope_reads(filename)
            for variable in VARIABLES:
                with self.subTest(module=filename, variable=variable):
                    self.assertNotIn(variable, bound)


if __name__ == "__main__":
    unittest.main()
