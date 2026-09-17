#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A deployment-wide gauge says how many tenants it does not speak for.

``return_all_candidates`` decides whether ranking may drop anything, so it changes what every
retrieve returns -- and it resolves **per tenant**. The gauge publishes it for scope ``None``, the
deployment default.

Measured before this: with two tenants setting it on, retrieval honoured it for both and the gauge
still published ``0``. A dashboard read "return-all is off on this deployment" while that traffic
returned everything. Nothing was wrong with the number; it was answering a narrower question than
the one its name implies, and there was no way to tell from the series.

**The fix is not a per-tenant series.** A metric label taken from a tenant id is a cardinality
bomb, and the tenant set is exactly the open value that must never become one. What a reader needs
is narrower: *is the number above the whole story?* Zero means yes.

Same shape as ``matrixark_gateway_onebox_profile_readable`` beside it -- a series whose only job is
to say how far the series next to it can be trusted. Two of the five here are now that shape,
which is what a surface reporting on a system it cannot fully see ends up needing.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as gwm  # noqa: E402

NAME = "matrixark_gateway_retrieval_tenant_overrides"


def gauge(name: str):
    for line in gwm.onebox_lines():
        if line.startswith(name + " "):
            return line.split(" ", 1)[1].strip()
    return None


def set_policy(tenant: str, knobs: dict) -> None:
    """Set a tenant policy on EVERY loaded copy of the policy module.

    Under `unittest discover` it is imported under two names in one process -- plain and
    `tools.`-prefixed -- and each keeps its own record store. Setting it on the copy this file
    imported while the metric's lazy import binds the other makes the override vanish, and the
    failure reads as "the count is not wired", which is exactly what it is not.
    """
    for module in list(sys.modules.values()):
        if (getattr(module, "__name__", "").endswith("matrixark_tenant_policy")
                and hasattr(module, "set_tenant_policy")):
            module.set_tenant_policy(tenant, knobs)


def clear_policies() -> None:
    for module in list(sys.modules.values()):
        if getattr(module, "__name__", "").endswith("matrixark_tenant_policy"):
            for attr in ("_RECORD_POLICIES", "_FILE_POLICIES"):
                store = getattr(module, attr, None)
                if isinstance(store, dict):
                    store.clear()


class TheCountSaysWhoTheGaugesSpeakForTest(unittest.TestCase):

    def setUp(self) -> None:
        clear_policies()
        self.addCleanup(clear_policies)

    def test_nobody_overriding_reads_as_zero(self) -> None:
        """Zero is a real answer here -- the gauges above speak for everyone -- which is why a
        failed read must NOT be published as zero."""
        self.assertEqual("0", gauge(NAME))

    def test_a_tenant_that_differs_is_counted(self) -> None:
        set_policy("tenant-a", {"return_all_candidates": True})
        self.assertEqual("1", gauge(NAME))

    def test_each_tenant_counts_once_however_many_settings_it_sets(self) -> None:
        set_policy("tenant-a", {"return_all_candidates": True,
                                "return_all_candidate_threshold": 500})
        self.assertEqual("1", gauge(NAME),
                         "the count is of TENANTS the gauges do not speak for, not of overrides")

    def test_an_unrelated_override_does_not_move_it(self) -> None:
        """The question is "do the gauges above speak for everyone", not "has anyone configured
        anything". A count that answered the second would be at its maximum on every deployment
        that had ever been tuned, and would tell a reader nothing."""
        set_policy("tenant-c", {"top_k_per_layer": 24})
        self.assertEqual("0", gauge(NAME))

    def test_the_deployment_gauge_still_reports_the_default(self) -> None:
        """The count discloses; it does not replace. A reader still needs to know what a tenant
        with no override gets."""
        set_policy("tenant-a", {"return_all_candidates": True})
        self.assertEqual("0", gauge("matrixark_gateway_return_all_candidates"))
        self.assertEqual("1", gauge(NAME))


class AFailedReadIsNotNobodyDifferingTest(unittest.TestCase):
    """``0`` is a real answer -- "the gauges speak for everyone" -- so a failed read must not
    borrow it.

    This is the mistake the profile gauge made in its first version: an ImportError caught in an
    ``except`` became a confident ``0``, and every dashboard described the opposite of what was
    running. Here the stakes are the same shape: a deployment whose policy registry cannot be
    reached would look like a deployment where every tenant agrees.

    Staged rather than waited for. ``None`` in ``sys.modules`` is what CPython leaves behind for a
    module that failed to import, and importing it again raises ``ImportError`` exactly as a real
    failure would.
    """

    def setUp(self) -> None:
        clear_policies()
        self.addCleanup(clear_policies)

    def test_a_registry_that_cannot_be_asked_reads_as_minus_one(self) -> None:
        import matrixark_tenant_policy  # noqa: F401 - ensure a real entry to put back
        saved = sys.modules["matrixark_tenant_policy"]
        sys.modules["matrixark_tenant_policy"] = None
        try:
            value = gauge(NAME)
        finally:
            sys.modules["matrixark_tenant_policy"] = saved
        self.assertEqual("-1", value,
                         "a policy registry that could not be asked is being published as "
                         "'nobody has overridden anything', which is a different fact")

    def test_the_other_gauges_still_appear_when_it_fails(self) -> None:
        """A series that vanishes on failure cannot be alerted on, and its absence looks like a
        scrape problem rather than a deployment one."""
        import matrixark_tenant_policy  # noqa: F401
        saved = sys.modules["matrixark_tenant_policy"]
        sys.modules["matrixark_tenant_policy"] = None
        try:
            lines = gwm.onebox_lines()
        finally:
            sys.modules["matrixark_tenant_policy"] = saved
        self.assertEqual(5, len([l for l in lines if l.startswith("matrixark_gateway_")]))


class TheCountReadsTheSameSetTheGaugesReportTest(unittest.TestCase):
    """A control must read the same set as its subject.

    If the counted set and the reported set drift apart, the count answers a different question
    from the one the reader is asking -- and it does so silently, because both are just numbers.
    """

    def test_every_counted_setting_is_one_the_gauges_report(self) -> None:
        emitted = " ".join(gwm.onebox_lines())
        for name in gwm.TENANT_VARIABLE_RETRIEVAL_SETTINGS:
            with self.subTest(setting=name):
                self.assertIn("matrixark_gateway_%s " % name, emitted,
                              "%s is counted as something the gauges do not speak for, but no "
                              "gauge reports it" % name)

    def test_the_profile_is_not_counted_because_it_has_no_scope(self) -> None:
        """It reads the environment with no scope, so it is the same for every tenant and there is
        nothing for a tenant to differ about. Counting it would report a difference that cannot
        exist."""
        self.assertNotIn("onebox_embedding_first", gwm.TENANT_VARIABLE_RETRIEVAL_SETTINGS)
        import matrixark_retrieval_effective as eff
        import inspect
        self.assertNotIn("scope", inspect.signature(eff.onebox_embedding_first).parameters,
                         "the profile takes a scope now, so it CAN differ per tenant and belongs "
                         "in the counted set")

    def test_the_counted_set_is_not_empty(self) -> None:
        """The vacuity guard. Every assertion above passes over an empty tuple, and an empty tuple
        makes the count permanently zero -- a disclosure series that discloses nothing, for ever,
        while looking healthy."""
        self.assertTrue(gwm.TENANT_VARIABLE_RETRIEVAL_SETTINGS)


class NoTenantIdentityReachesAMetricLabelTest(unittest.TestCase):
    """The reason this is a count and not a per-tenant series.

    A label taken from an open value is a cardinality bomb: one series per tenant, for ever, in
    every scrape. The count exists precisely so nobody needs to reach for that.
    """

    def setUp(self) -> None:
        clear_policies()
        self.addCleanup(clear_policies)

    def test_a_tenant_name_never_appears_in_a_series(self) -> None:
        set_policy("acme-corporation", {"return_all_candidates": True})
        for line in gwm.onebox_lines():
            with self.subTest(line=line[:60]):
                self.assertNotIn("acme-corporation", line)

    def test_no_series_here_carries_a_label_at_all(self) -> None:
        """These five are single-valued. A label arriving on one is worth a second look, because
        the obvious label to reach for is the one that must never be used."""
        for line in gwm.onebox_lines():
            if line.startswith("matrixark_gateway_"):
                with self.subTest(line=line[:60]):
                    self.assertIsNone(re.search(r"\{[^}]*\}", line),
                                      "a labelled series appeared here; if the label is a tenant "
                                      "id this is a cardinality bomb")


if __name__ == "__main__":
    unittest.main()
