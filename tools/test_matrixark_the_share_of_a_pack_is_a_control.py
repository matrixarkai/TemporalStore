#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The share of a pack a section gets is a control that can be raised.

The portal offered a percentage for skills and for shared resources, and neither could be moved. A
separate guard on the share -- offered nowhere -- sat at exactly the share's own default, and the
share resolves to ``min(share, guard)``, so 10, 20, 30 and 50 percent all produced ten percent with
nothing said about it. Behind that, the absolute token ceiling bound at 13.1 percent of the budget,
so even once the share moved the pack would not have.

Three limits decide a section's size: the share, the guard on the share, and the ceiling in tokens.
This suite pins the arrangement that keeps the first one in charge -- each limit clear of the one
below it -- and that the panel names whichever one actually decided.

The headroom is DERIVED here rather than written down: the test computes where each limit binds and
fails if a guard or a ceiling lands inside the range its share is allowed to ask for. Writing the
numbers in would only restate the source.
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
import matrixark_mcp_core as core  # noqa: E402
import matrixark_mcp_runtime_config as runtime  # noqa: E402
import matrixark_v1_gateway as gateway  # noqa: E402

# setting key -> (variable, share constant, guard variable, guard constant)
SHARES = {
    "skills.shared_skill_budget_ratio": (
        "MATRIXARK_SHARED_SKILL_BUDGET_RATIO", "DEFAULT_SHARED_SKILL_BUDGET_RATIO",
        "MATRIXARK_SHARED_SKILL_MAX_BUDGET_RATIO", "DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO",
        "DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS"),
    "skills.shared_resource_budget_ratio": (
        "MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO", "DEFAULT_SHARED_RESOURCE_BUDGET_RATIO",
        "MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_RATIO", "DEFAULT_SHARED_RESOURCE_MAX_BUDGET_RATIO",
        "DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS"),
    "retrieval.cross_session_budget_ratio": (
        "MATRIXARK_CROSS_SESSION_BUDGET_RATIO", "DEFAULT_CROSS_SESSION_BUDGET_RATIO",
        "MATRIXARK_CROSS_SESSION_MAX_BUDGET_RATIO", "DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO",
        "DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS"),
    "retrieval.cross_session_profile_budget_ratio": (
        "MATRIXARK_CROSS_SESSION_PROFILE_BUDGET_RATIO",
        "DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO",
        "MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO",
        "DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO",
        "DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS"),
}

# Every variable any of this touches. A sibling suite that sets one of these and does not clear it
# would otherwise decide the outcome here -- which has happened before, and only in CI, because
# running one suite alone is exactly the condition that hides it.
PARTICIPATING = tuple(
    [entry[0] for entry in SHARES.values()] + [entry[2] for entry in SHARES.values()]
    + ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS", "MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS",
       "MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS",
       "MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS", "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"])


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
                     or (isinstance(target, ast.Name)
                         and target.id in {"getenv", "live_int", "live_float"}))
            first = sub.args[0]
            if reads and isinstance(first, ast.Constant) and isinstance(first.value, str):
                found.add(first.value)
    return found


class Case(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = {name: os.environ.get(name) for name in PARTICIPATING}
        for name in PARTICIPATING:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    @property
    def total(self) -> int:
        return runtime.DEFAULT_MAX_CONTEXT_TOKENS

    # The shared-context policy exists in two modules and which one runs depends on the caller.
    def copies(self):
        scoring = sys.modules["matrixark_mcp_core_scoring"]
        return {"budget_policies": policies.build_shared_context_policy,
                "core_scoring": scoring.build_shared_context_policy}

    def shared(self, section: str, which: str = "budget_policies"):
        policy = self.copies()[which]({}, {}, remote_budget_tokens=self.total)
        return (float(policy["%s_budget_ratio" % section]),
                int(policy["%s_budget_tokens" % section]))

    def cross_session(self, *, profile: bool):
        # The profile share is chosen by `profile_budget_query`, which the scoring path derives from
        # the question type and the query text -- not from a caller-supplied flag. "profile_memory"
        # is the type that selects it; "profile" is not a value it recognises and silently lands on
        # the ordinary share, which is exactly how this test first told itself the feature worked.
        policy = core.build_cross_session_policy(
            {}, {}, question_type="profile_memory" if profile else "normal",
            session_scope="cross_session", remote_budget_tokens=self.total)
        return float(policy["budget_ratio"]), int(policy["budget_tokens"])


class TheShareCanBeRaisedTest(Case):
    """The defect, from the operator's side: set the percentage, get the percentage."""

    def test_the_skill_share_moves_and_so_do_the_tokens(self) -> None:
        for asked in (0.20, 0.30, 0.50):
            with self.subTest(asked=asked):
                os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = str(asked)
                ratio, tokens = self.shared("skill")
                self.assertAlmostEqual(asked, ratio, places=6)
                self.assertEqual(int(self.total * asked), tokens)

    def test_the_resource_share_too(self) -> None:
        for asked in (0.30, 0.40, 0.50):
            with self.subTest(asked=asked):
                os.environ["MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO"] = str(asked)
                ratio, tokens = self.shared("resource")
                self.assertAlmostEqual(asked, ratio, places=6)
                self.assertEqual(int(self.total * asked), tokens)

    def test_the_cross_session_share_too(self) -> None:
        os.environ["MATRIXARK_CROSS_SESSION_BUDGET_RATIO"] = "0.40"
        ratio, _tokens = self.cross_session(profile=False)
        self.assertAlmostEqual(0.40, ratio, places=6)

    def test_the_profile_share_too(self) -> None:
        os.environ["MATRIXARK_CROSS_SESSION_PROFILE_BUDGET_RATIO"] = "0.55"
        ratio, _tokens = self.cross_session(profile=True)
        self.assertAlmostEqual(0.55, ratio, places=6)

    def test_both_copies_of_the_shared_policy_honour_it(self) -> None:
        """Making one copy live and not the other is how a setting works on some requests. A
        mutation reverting only the second copy passed until a test drove both."""
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = "0.30"
        for which in self.copies():
            with self.subTest(copy=which):
                self.assertAlmostEqual(0.30, self.shared("skill", which)[0], places=6)

    def test_there_are_still_two_copies_to_check(self) -> None:
        """The floor: consolidating them would make the loop above cover one module twice and keep
        passing while covering less."""
        self.assertEqual(2, len({id(fn) for fn in self.copies().values()}))


class EveryLimitIsClearOfTheOneBelowItTest(Case):
    """The arrangement that keeps the share in charge, derived rather than written down.

    Three limits can decide a section: the share, the guard on the share, and the ceiling in
    tokens. The share is only a control if the other two sit outside the range it may ask for. Both
    of them used to sit inside it -- the guard at exactly the share's default, the ceiling at 13.1
    percent of the budget -- which is the whole defect, twice.
    """

    def test_the_guard_leaves_the_share_somewhere_to_go(self) -> None:
        for key, (_var, share_c, _gvar, guard_c, _ceil_c) in SHARES.items():
            with self.subTest(setting=key):
                share = getattr(runtime, share_c)
                guard = getattr(runtime, guard_c)
                self.assertGreater(
                    guard, share,
                    "%s is guarded at its own default, so raising it does nothing" % key)

    def test_the_ceiling_does_not_bind_inside_the_guard(self) -> None:
        """A share raised to the guard must still be what decides the token count. If the ceiling
        lands below `guard * total`, the percentage goes nominal somewhere inside its own range."""
        for key, (_var, _share_c, _gvar, guard_c, ceiling_c) in SHARES.items():
            with self.subTest(setting=key):
                guard = getattr(runtime, guard_c)
                ceiling = getattr(runtime, ceiling_c)
                self.assertGreaterEqual(
                    ceiling, int(runtime.DEFAULT_MAX_CONTEXT_TOKENS * guard),
                    "%s: the ceiling binds at %.1f%% of the budget, below its own %.0f%% guard"
                    % (key, 100.0 * ceiling / runtime.DEFAULT_MAX_CONTEXT_TOKENS, guard * 100))

    def test_the_guard_is_still_a_guard(self) -> None:
        """The floor for both rules above: they are satisfied trivially by removing the limits, so
        a share asked above its guard must still be held AT the guard."""
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = "0.95"
        ratio, _tokens = self.shared("skill")
        self.assertAlmostEqual(runtime.DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO, ratio, places=6)
        self.assertLess(ratio, 0.95)

    def test_the_guard_is_itself_a_control(self) -> None:
        """And it is raisable, so the arrangement is a default rather than a wall."""
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = "0.70"
        os.environ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_RATIO"] = "0.80"
        self.assertAlmostEqual(0.70, self.shared("skill")[0], places=6)


class NoDefaultPackMovesTest(Case):
    """Raising headroom must not hand anybody a different pack than they had yesterday."""

    def test_every_share_still_resolves_to_its_build_number(self) -> None:
        for section, constant in (("skill", "DEFAULT_SHARED_SKILL_BUDGET_RATIO"),
                                  ("resource", "DEFAULT_SHARED_RESOURCE_BUDGET_RATIO")):
            with self.subTest(section=section):
                ratio, tokens = self.shared(section)
                self.assertAlmostEqual(getattr(runtime, constant), ratio, places=6)
                self.assertEqual(int(self.total * getattr(runtime, constant)), tokens)

    def test_the_cross_session_shares_too(self) -> None:
        self.assertAlmostEqual(runtime.DEFAULT_CROSS_SESSION_BUDGET_RATIO,
                               self.cross_session(profile=False)[0], places=6)
        self.assertAlmostEqual(runtime.DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO,
                               self.cross_session(profile=True)[0], places=6)


class ABadValueFallsBackTest(Case):
    """A share outside 0.0-1.0 is not a share of anything. Falling back beats reinterpreting."""

    def test_nonsense_falls_back(self) -> None:
        for raw in ("", "   ", "not-a-number", "-0.1", "1.5", "100"):
            with self.subTest(value=raw):
                os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = raw
                self.assertEqual(
                    runtime.DEFAULT_SHARED_SKILL_BUDGET_RATIO,
                    runtime.live_float("MATRIXARK_SHARED_SKILL_BUDGET_RATIO",
                                       runtime.DEFAULT_SHARED_SKILL_BUDGET_RATIO))

    def test_a_good_value_is_still_taken(self) -> None:
        """The floor: a resolver that ignored everything would satisfy the test above."""
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = " 0.42 "
        self.assertAlmostEqual(
            0.42, runtime.live_float("MATRIXARK_SHARED_SKILL_BUDGET_RATIO", 0.1), places=6)

    def test_zero_is_a_decision_and_is_kept(self) -> None:
        """Unlike a ceiling of zero, which would cut a section to nothing by accident, a share of
        zero says "this section gets none of the pack" and there is no other way to say it."""
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = "0"
        self.assertEqual(0.0, runtime.live_float("MATRIXARK_SHARED_SKILL_BUDGET_RATIO", 0.1))
        self.assertEqual((0.0, 0), self.shared("skill"))


class ThePanelNamesWhatDecidedTest(Case):
    """Three limits can decide a section's size; the customer is owed the name of the one that did."""

    def skills(self):
        return gateway._shared_budget_summary()["skills"]

    def test_nothing_set_is_bound_by_the_percentage(self) -> None:
        row = self.skills()
        self.assertEqual("percentage", row["bound_by"])
        self.assertAlmostEqual(row["asked_percent"], row["percent"], places=6)

    def test_a_raise_within_the_guard_is_still_the_percentage(self) -> None:
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = "0.30"
        row = self.skills()
        self.assertEqual("percentage", row["bound_by"])
        self.assertEqual(30.0, row["percent"])

    def test_a_raise_past_the_guard_is_named_as_the_guard(self) -> None:
        """The state the panel could not previously describe: the share the deployment asked for is
        not the share it has, and the resolved policy alone cannot show that."""
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = "0.80"
        row = self.skills()
        self.assertEqual("share_guard", row["bound_by"])
        self.assertEqual(80.0, row["asked_percent"])
        self.assertEqual(row["guard_percent"], row["percent"])

    def test_a_ceiling_below_the_percentage_is_named_as_the_ceiling(self) -> None:
        os.environ["MATRIXARK_SHARED_SKILL_BUDGET_RATIO"] = "0.30"
        os.environ["MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS"] = "20000"
        row = self.skills()
        self.assertEqual("ceiling", row["bound_by"])
        self.assertEqual(20000, row["tokens"])


class TheLiveClaimIsEarnedTest(unittest.TestCase):
    """`live` is a promise the portal makes on save; here it is derived from where the read is."""

    def test_the_portal_says_live_for_every_share_and_guard(self) -> None:
        for key in SHARES:
            with self.subTest(setting=key):
                self.assertEqual("live", cfg.SETTINGS_BY_KEY[key].applies)
        for key in ("skills.shared_skill_max_budget_ratio",
                    "skills.shared_resource_max_budget_ratio",
                    "retrieval.cross_session_max_budget_ratio",
                    "retrieval.cross_session_profile_max_budget_ratio"):
            with self.subTest(setting=key):
                self.assertEqual("live", cfg.SETTINGS_BY_KEY[key].applies)

    def test_no_consumer_binds_a_share_at_import(self) -> None:
        """The constants stay at module scope -- that is the fallback. What must not happen is a
        CONSUMER binding one, which is what made these restart-scoped."""
        wanted = {entry[0] for entry in SHARES.values()} | {entry[2] for entry in SHARES.values()}
        for filename in ("matrixark_mcp_budget_policies.py", "matrixark_mcp_core_scoring.py"):
            bound = module_scope_reads(filename)
            for variable in sorted(wanted):
                with self.subTest(module=filename, variable=variable):
                    self.assertNotIn(variable, bound)

    def test_the_constants_no_longer_capture_the_environment(self) -> None:
        """A capture beside `live_float` can only shadow it: clearing the setting would fall back
        to what the process started with rather than to the number in the source."""
        wanted = {entry[0] for entry in SHARES.values()} | {entry[2] for entry in SHARES.values()}
        bound = module_scope_reads("matrixark_mcp_runtime_config.py")
        for variable in sorted(wanted):
            with self.subTest(variable=variable):
                self.assertNotIn(variable, bound)

    def test_the_detector_would_notice_an_import_time_read(self) -> None:
        """The floor for the two rules above, on a variable that IS still bound at import."""
        bound = module_scope_reads("matrixark_mcp_runtime_config.py")
        self.assertIn("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", bound)


class ThePortalDeclaresTheNumberTheBuildRunsTest(unittest.TestCase):
    """`export_settings(include_defaults=True)` writes a declared default into a clone as an
    explicit value, so a stale one does not merely mislead -- it reconfigures the next deployment.

    The pairs are DERIVED from the setting keys rather than listed, so a share added later is
    covered without anyone remembering to add it here.
    """

    def pairs(self):
        found = {}
        for setting in cfg.SETTINGS:
            if not setting.key.endswith("budget_ratio"):
                continue
            constant = "DEFAULT_" + setting.env[len("MATRIXARK_"):]
            if hasattr(runtime, constant):
                found[setting.key] = (setting, getattr(runtime, constant))
        return found

    def test_every_declared_share_matches_the_constant(self) -> None:
        for key, (setting, build) in sorted(self.pairs().items()):
            with self.subTest(setting=key):
                self.assertAlmostEqual(
                    build, float(setting.default), places=6,
                    msg="%s declares %r, the build runs %s" % (key, setting.default, build))

    def test_the_derivation_actually_found_them(self) -> None:
        """The floor: a naming change would empty the map above and the test would pass on
        nothing. Every share and guard this change offers has to be in it."""
        found = self.pairs()
        self.assertGreaterEqual(len(found), 8)
        for key in SHARES:
            self.assertIn(key, found)


if __name__ == "__main__":
    unittest.main()
