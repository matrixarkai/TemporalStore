#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every policy accessor must agree with the registry it reads.

An accessor and its knob can disagree in two ways, and both were live:

* **In behaviour** — the accessor reads somewhere other than the registry, so a tenant sets a value
  and the accessor returns something else.
* **In description** — the accessor's docstring states a default the registry contradicts.
  `summarize_aggregation_only_nodes_enabled` said "(default OFF)" while the registry said `True`,
  because the default was deliberately reversed and the docstring was not. That reversal exists for
  a measured reason: skipping the spine nodes removed *every* L1 in the store, since `node_l1` is
  only generated where child summaries exist.

The second is not cosmetic. A confident wrong comment is what stops the next person checking — the
`matrixark_gateway_config` comment asserting that a per-tenant policy record "still applies
immediately" is why nobody noticed for however long that retrieval read none of those knobs.

This checks behaviour for every bool accessor, and checks the stated default for the ones whose
docstrings name one.
"""
from __future__ import annotations

import inspect
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_index_growth_bound as gates  # noqa: E402
import matrixark_tenant_policy as policy  # noqa: E402

DEFAULT_PHRASE = re.compile(r"\(default\s+(ON|OFF|True|False)\)", re.I)


def _bool_accessors():
    """(knob name, function) for every `<knob>_enabled` accessor with a bool knob."""
    out = []
    for name, func in vars(gates).items():
        if not name.endswith("_enabled") or not callable(func):
            continue
        knob_name = name[: -len("_enabled")]
        knob = policy.KNOBS.get(knob_name)
        if knob is None or getattr(knob, "kind", "") != "bool":
            continue
        out.append((knob_name, func))
    return sorted(out)


class AccessorsAgreeWithTheRegistryTest(unittest.TestCase):

    def setUp(self) -> None:
        self.accessors = _bool_accessors()
        self.assertGreater(len(self.accessors), 5,
                           "found almost no bool accessors, so these comparisons prove nothing")

    def test_the_resolved_default_matches_the_registry_default(self) -> None:
        for knob_name, func in self.accessors:
            with self.subTest(knob=knob_name):
                knob = policy.KNOBS[knob_name]
                os.environ.pop(getattr(knob, "env", "") or "_none_", None)
                try:
                    got = func({"tenant_id": "registry_check_%s" % knob_name})
                except TypeError:
                    got = func()          # a few take no scope: a deployment-wide flag
                self.assertEqual(
                    bool(knob.default), bool(got),
                    "%s resolves to %r for an unconfigured tenant while the registry default is "
                    "%r. One of them is lying to the portal." % (knob_name, got, knob.default))

    def test_a_stated_default_matches_the_registry(self) -> None:
        # Only accessors whose docstring actually names a default are checked; the rest are free
        # to say nothing.
        checked = 0
        for knob_name, func in self.accessors:
            doc = inspect.getdoc(func) or ""
            match = DEFAULT_PHRASE.search(doc)
            if not match:
                continue
            checked += 1
            stated = match.group(1).upper() in {"ON", "TRUE"}
            with self.subTest(knob=knob_name):
                self.assertEqual(
                    bool(policy.KNOBS[knob_name].default), stated,
                    "%s's docstring says (default %s) and the registry says %r. The docstring is "
                    "what the next person reads before deciding not to check."
                    % (knob_name, match.group(1), policy.KNOBS[knob_name].default))
        self.assertGreater(checked, 0,
                           "no accessor docstring names a default, so this test checked nothing")


if __name__ == "__main__":
    unittest.main()
