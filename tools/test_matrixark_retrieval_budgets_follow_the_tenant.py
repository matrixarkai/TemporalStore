#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Four retrieval budgets now read their tenant knob. They previously read nothing.

`top_k_per_layer`, `max_candidates_per_node`, `max_selected_refs` and `max_global_candidates` each
have a tenant knob in the registry, the portal offers all four, and retrieval consulted none of
them: it took a per-request `ranking` argument and fell back to a module constant captured from the
environment at import. A tenant setting any of these got exactly nothing.

`matrixark_gateway_config` asserted the opposite in a comment -- that a deployment-wide change waits
for a restart "even though a per-tenant policy record still applies immediately". The second half
was false, which is why the comment is corrected in the same change: a wrong comment about a broken
thing is what stops anyone looking.

**The trap this file exists to hold shut.** The obvious wiring is `resolve()`, and it is wrong.
`resolve()` returns the knob registry's default when nobody has set anything, and for these four the
registry disagrees with what retrieval actually uses by 10x to 156x:

    top_k_per_layer          registry 240    retrieval 8
    max_candidates_per_node  registry 10240  retrieval 1024
    max_global_candidates    registry 20480  retrieval 512
    max_selected_refs        registry 10000  retrieval 64

So wiring to `resolve()` reads as "the knob works now" and is really a silent multiplication of
every unconfigured deployment's budget. The precedence is therefore explicit-only, and
`test_an_unconfigured_deployment_does_not_move` is the assertion that keeps it that way.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter  # noqa: E402,F401  (establishes the package first)
import matrixark_tenant_policy as policy  # noqa: E402
import matrixark_local_adapter_retrieve as retrieve  # noqa: E402

BUDGETS = (
    ("top_k_per_layer", "MATRIXARK_TOP_K_PER_LAYER", 8),
    ("max_candidates_per_node", "MATRIXARK_MAX_CANDIDATES_PER_NODE", 1024),
    ("max_selected_refs", "MATRIXARK_MAX_SELECTED_REFS", 64),
    ("max_global_candidates", "MATRIXARK_MAX_GLOBAL_CANDIDATES", 512),
)


def _set_policy(tenant, knobs):
    """Set a tenant policy on EVERY loaded copy of the policy module.

    Under `unittest discover` the module is imported under two names in one process -- once as
    `matrixark_tenant_policy` and once through the `tools.` package path -- and each copy keeps its
    own `_RECORD_POLICIES`. Setting the policy on the copy the test imported while the production
    helper's lazy import binds the other makes the override vanish: the budget falls back to the
    build default and the failure reads as "the knob is not wired", which is exactly what it is not.

    These tests passed alone and failed in the full suite for that reason. Same root cause as the
    GatewayConfig identity failure in the readiness-sources tests: two definitions of one name, and
    which one you get depends on how you arrived.
    """
    for module in list(sys.modules.values()):
        if getattr(module, "__name__", "").endswith("matrixark_tenant_policy") and                 hasattr(module, "set_tenant_policy"):
            module.set_tenant_policy(tenant, knobs)


def _limit(name, tenant, build_default):
    return retrieve._tenant_retrieval_limit(name, {"tenant_id": tenant}, build_default)


class TheTenantSettingIsReadTest(unittest.TestCase):

    def test_an_explicit_tenant_value_is_used(self) -> None:
        for name, _env, build_default in BUDGETS:
            with self.subTest(knob=name):
                tenant = "explicit_%s" % name
                _set_policy(tenant, {name: 5})
                self.assertEqual(5, _limit(name, tenant, build_default),
                                 "%s ignored an explicit tenant setting" % name)

    def test_an_unconfigured_deployment_does_not_move(self) -> None:
        # The assertion that keeps the registry's much larger defaults out of the retrieval path.
        for name, env, build_default in BUDGETS:
            with self.subTest(knob=name):
                os.environ.pop(env, None)
                self.assertEqual(
                    build_default, _limit(name, "never_configured_%s" % name, build_default),
                    "%s changed for a deployment that configured nothing; wiring to resolve() "
                    "would do exactly this, because the registry default is far larger" % name)

    def test_the_registry_default_is_not_what_retrieval_uses(self) -> None:
        # Pins the disagreement itself. If someone reconciles the two numbers this test fails and
        # they can delete it deliberately -- rather than the reconciliation silently changing
        # retrieval through the back door.
        for name, _env, build_default in BUDGETS:
            with self.subTest(knob=name):
                knob = policy.KNOBS.get(name)
                self.assertIsNotNone(knob, "%s is not in the registry" % name)
                self.assertNotEqual(
                    knob.default, build_default,
                    "%s: registry and retrieval now agree (%s). Good -- but this test documented "
                    "the gap, so remove it on purpose rather than leaving it passing by accident."
                    % (name, build_default))


class BothExplicitLevelsApplyWithoutARestartTest(unittest.TestCase):

    def test_an_explicit_env_var_applies_mid_process(self) -> None:
        name, env, build_default = BUDGETS[2]  # max_selected_refs
        tenant = "env_live_case"
        os.environ.pop(env, None)
        self.assertEqual(build_default, _limit(name, tenant, build_default))
        try:
            os.environ[env] = "21"
            self.assertEqual(21, _limit(name, tenant, build_default),
                             "an env change needed a restart to take effect")
        finally:
            os.environ.pop(env, None)
        self.assertEqual(build_default, _limit(name, tenant, build_default),
                         "removing the env var did not take effect either")

    def test_a_tenant_override_beats_the_environment(self) -> None:
        name, env, build_default = BUDGETS[2]
        _set_policy("beats_env", {name: 7})
        try:
            os.environ[env] = "21"
            self.assertEqual(7, _limit(name, "beats_env", build_default),
                             "the deployment-wide value overrode a tenant's own setting")
        finally:
            os.environ.pop(env, None)


class NonsenseFallsBackTest(unittest.TestCase):
    """A budget that resolves to nothing returns nothing at all, which is worse than a bad setting."""

    def test_zero_and_junk_fall_back_to_the_build_default(self) -> None:
        name, env, build_default = BUDGETS[2]
        for bad in (0, -3, "abc", None, ""):
            with self.subTest(value=bad):
                tenant = "junk_%s" % str(bad)
                _set_policy(tenant, {name: bad})
                self.assertEqual(build_default, _limit(name, tenant, build_default),
                                 "a %r budget was accepted" % bad)
        for bad in ("0", "-1", "not-a-number"):
            with self.subTest(env_value=bad):
                os.environ[env] = bad
                try:
                    self.assertEqual(build_default,
                                     _limit(name, "junk_env", build_default))
                finally:
                    os.environ.pop(env, None)


class TheProfileCandidateCeilingFollowsTheTenantTest(unittest.TestCase):
    """The fifth budget, and the last tenant knob that had no caller at all.

    It could not be wired until there was one `build_cross_session_policy` to wire: the function
    existed twice, bound by different callers, so gating one would have left the other untouched --
    the guard-one-route-of-two mistake, which looks like it works.

    Asserted through the policy the builder returns rather than through the helper, because the
    helper returning the right number proves nothing about whether the builder consults it.
    """

    def _max_candidates(self, tenant, budget=8192):
        import matrixark_mcp_core_scoring as scoring
        args = {"scope": {"tenant_id": tenant},
                "query": "what is my standing rule about deploys"}
        policy_out = scoring.build_cross_session_policy(
            args, {}, question_type="profile_memory", session_scope="prefer",
            remote_budget_tokens=budget)
        return policy_out.get("max_candidates")

    def test_a_tenant_ceiling_is_applied(self) -> None:
        _set_policy("profile_ceiling", {"cross_session_profile_max_candidates": 6})
        self.assertEqual(6, self._max_candidates("profile_ceiling"))

    def test_an_unconfigured_tenant_does_not_move(self) -> None:
        # The registry default for this knob is 2000 and the value in use is 48. Wiring through
        # resolve() would have multiplied it forty-fold for everyone who set nothing.
        os.environ.pop("MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES", None)
        self.assertEqual(48, self._max_candidates("profile_unconfigured"))

    def test_an_explicit_env_var_applies_without_a_restart(self) -> None:
        os.environ.pop("MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES", None)
        self.assertEqual(48, self._max_candidates("profile_env"))
        try:
            os.environ["MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES"] = "11"
            self.assertEqual(11, self._max_candidates("profile_env"))
        finally:
            os.environ.pop("MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES", None)
        self.assertEqual(48, self._max_candidates("profile_env"))

    def test_it_does_not_touch_the_non_profile_budget(self) -> None:
        # The knob names the PROFILE ceiling. A non-profile query must keep its own default, or
        # this would be a much larger change than the name promises.
        import matrixark_mcp_core_scoring as scoring
        _set_policy("profile_only", {"cross_session_profile_max_candidates": 6})
        args = {"scope": {"tenant_id": "profile_only"}, "query": "what did we ship last week"}
        out = scoring.build_cross_session_policy(
            args, {}, question_type="latest", session_scope="prefer", remote_budget_tokens=8192)
        self.assertNotEqual(6, out.get("max_candidates"),
                            "the profile ceiling was applied to a non-profile query")


if __name__ == "__main__":
    unittest.main()
