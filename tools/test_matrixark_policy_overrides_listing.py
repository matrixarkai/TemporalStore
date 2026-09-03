#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Who has a policy override, and only within one tenant.

The policy endpoints read and write one identity at a time and need its id first. So a tenant could
set a user override and then have no way to find it again, or to answer "who here has custom
settings" -- which is the first question anyone asks after setting the second one.

Two properties matter more than the listing itself:

* **Aliases fold.** A policy is stored under every identity it answers to, the id AND its scope
  hash, so walking the maps directly reports each tenant two or three times. That reads as more
  configuration than exists, which is worse than no listing.
* **Isolation holds.** The policy route derives the tenant from the API KEY and never from the
  request, precisely so a caller cannot read another tenant's settings by naming it. A listing is
  the easiest possible way to break that, so the served form is scoped and the unrestricted form is
  reachable in-process only.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_tenant_policy as policy  # noqa: E402


class ListingWhoHasAnOverrideTest(unittest.TestCase):

    def setUp(self) -> None:
        policy.clear_tenant_policy_cache()
        self.addCleanup(policy.clear_tenant_policy_cache)

    def test_nothing_set_lists_nothing(self) -> None:
        """A page that always shows rows teaches people the rows mean nothing."""
        out = policy.policy_overrides()
        self.assertEqual(0, out["tenant_count"], out["tenants"])

    def test_a_tenant_override_is_listed_with_its_settings(self) -> None:
        policy.set_tenant_policy("acme", {"top_k_per_layer": 24})
        out = policy.policy_overrides()
        names = [t["tenant"] for t in out["tenants"]]
        self.assertIn("acme", names)
        entry = next(t for t in out["tenants"] if t["tenant"] == "acme")
        self.assertEqual(24, entry["settings"].get("top_k_per_layer"))

    def test_aliases_fold_to_one_row(self) -> None:
        """The id and its hash are two keys for one tenant; two tenants must not read as four."""
        policy.set_tenant_policy("acme", {"top_k_per_layer": 24})
        policy.set_tenant_policy("globex", {"recall_reinforcement": False})
        out = policy.policy_overrides()
        self.assertEqual(2, out["tenant_count"],
                         "expected two tenants, got %r -- aliases are not folding"
                         % [t["tenant"] for t in out["tenants"]])

    def test_the_readable_id_is_preferred_over_its_hash(self) -> None:
        policy.set_tenant_policy("acme", {"top_k_per_layer": 24})
        out = policy.policy_overrides()
        names = [t["tenant"] for t in out["tenants"]]
        self.assertIn("acme", names,
                      "the listing reports %r rather than the id an operator typed" % names)

    def test_scoping_to_one_tenant_excludes_the_others(self) -> None:
        """The property the served route depends on."""
        policy.set_tenant_policy("acme", {"top_k_per_layer": 24})
        policy.set_tenant_policy("globex", {"recall_reinforcement": False})
        scoped = policy.policy_overrides(only_tenant="acme")
        names = [t["tenant"] for t in scoped["tenants"]]
        self.assertEqual(["acme"], names,
                         "a tenant-scoped listing returned another tenant: %r" % names)

    def test_scoping_to_a_tenant_with_nothing_set_is_empty(self) -> None:
        policy.set_tenant_policy("acme", {"top_k_per_layer": 24})
        self.assertEqual([], policy.policy_overrides(only_tenant="nobody")["tenants"])


class TheServedFormIsScopedTest(unittest.TestCase):
    """The route must pass the key's tenant, not one from the request."""

    def test_the_route_passes_only_tenant(self) -> None:
        import inspect

        import matrixark_v1_gateway as gateway

        source = inspect.getsource(gateway)
        self.assertIn("policy_overrides(only_tenant=tenant_id)", source,
                      "the overrides listing is served unscoped, so one tenant can read another "
                      "tenant's settings -- the exact thing the tenant-from-the-key rule prevents")

    def test_the_route_does_not_take_a_tenant_from_the_request(self) -> None:
        import inspect

        import matrixark_v1_gateway as gateway

        source = inspect.getsource(gateway)
        self.assertNotIn('policy_overrides(only_tenant=params', source)
        self.assertNotIn('policy_overrides(only_tenant=payload', source)


if __name__ == "__main__":
    unittest.main()
