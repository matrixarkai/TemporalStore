#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every "applies live" the portal prints must be checked by something.

The audit next door reads `tools/*.py` and classifies each place a setting's env var appears as
import-time or per-call. It can only see the name written as a STRING CONSTANT, and the
tenant-policy knobs are not read that way -- `resolve()` does `os.environ.get(knob.env)`, where
the name is a variable. Those settings produce no site at all, and both of the audit's label
tests skip a setting that has no sites.

Measured when this was written: 77 settings carry an env var, 28 produce no site, and 24 of those
are advertised live. The label was not wrong -- reading through the registry at call time is
exactly what live means -- but nothing checked it, and a guard that skips what it cannot see
reports the same "no failures" whether the labels are right or a later change freezes one.

A setting is classified two ways here instead of one: a per-call site the audit found, or
ownership by the tenant-policy registry, whose resolvers are verified per-call in this file.
The second is only sound while those resolvers really do read the environment inside a function
body, so that is asserted rather than assumed.
"""
from __future__ import annotations

import ast
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402
import test_matrixark_gateway_config_audit as audit  # noqa: E402

RESOLVERS = ("resolve", "explicit_int", "explicit_bool")

# 49 settings had a site when this floor was set. It catches a scan that has stopped reaching the
# tree, not a release that moved a few readers.
SITE_COVERAGE_FLOOR = 40


def _registry_env_names() -> set:
    """Env vars the tenant-policy registry owns."""
    try:
        import matrixark_tenant_policy as policy
    except Exception:  # pragma: no cover - policy module absent
        return set()
    return {getattr(knob, "env", "") for knob in policy.KNOBS.values()
            if getattr(knob, "env", "")}


RUST_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "crates", "temporalstore-rust", "src")


def _rust_per_call_env_names() -> set:
    """Env vars the ENGINE reads per call, which the Python audit cannot see at all.

    `MATRIXARK_REQUIRE_MODEL_SUMMARIES` is the case that exposed this: the gateway offers it, the
    portal labels it live, and no Python module reads it. It is read in Rust, by
    `context_require_model_summaries()`, which calls `std::env::var` in its own body.

    The classification is a heuristic and worth stating plainly: for each `std::env::var("NAME")`
    the nearest preceding `fn ` and the nearest preceding `static `/`const ` are compared, and the
    read counts as per-call when the `fn` is nearer. That is right for a direct read in a function
    body and would be wrong for one buried in a lazily-initialised static -- so a name is only
    ever ADMITTED by this, never rejected. A setting this misjudges stays classified by the
    Python audit or by the registry, and if none of the three covers it the test fails.
    """
    names = set()
    if not os.path.isdir(RUST_SRC):
        return names
    pattern = re.compile(r'std::env::var\(\s*"([A-Z0-9_]+)"')
    for root, _dirs, files in os.walk(RUST_SRC):
        for entry in files:
            if not entry.endswith(".rs"):
                continue
            path = os.path.join(root, entry)
            try:
                with open(path, encoding="utf-8", errors="replace") as handle:
                    text = handle.read()
            except OSError:
                continue
            for match in pattern.finditer(text):
                head = text[:match.start()]
                fn_at = head.rfind("fn ")
                static_at = max(head.rfind("static "), head.rfind("const "))
                if fn_at > static_at:
                    names.add(match.group(1))
    return names

def _resolver_environ_scopes() -> dict:
    """resolver name -> True when its os.environ read happens inside the function body.

    An import-time read would mean the registry captured the value once, and every setting this
    file calls live on the registry's account would be wrong at the same moment.
    """
    import matrixark_tenant_policy as policy

    with open(policy.__file__, encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name not in RESOLVERS:
            continue
        reads = [d for d in ast.walk(node)
                 if isinstance(d, ast.Attribute) and d.attr == "environ"]
        found[node.name] = bool(reads)
    return found


class EveryLiveClaimIsCheckedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.live = [s for s in cfgmod.SETTINGS if s.env and s.applies == "live"]
        self.assertGreater(len(self.live), 10,
                           "almost no settings claim to apply live, so this file checks nothing")

    def test_the_registry_resolvers_read_the_environment_per_call(self) -> None:
        scopes = _resolver_environ_scopes()
        self.assertTrue(scopes, "found none of the resolvers, so registry ownership proves nothing")
        for name in RESOLVERS:
            with self.subTest(resolver=name):
                self.assertIn(name, scopes, "%s is gone; the classification here is stale" % name)
                self.assertTrue(scopes[name],
                                "%s does not read os.environ inside its body, so a registry knob "
                                "cannot be called live on its account" % name)

    def test_every_live_setting_is_classified_by_something(self) -> None:
        registry = _registry_env_names()
        engine = _rust_per_call_env_names()
        unchecked = []
        for setting in self.live:
            sites = audit.SITES.get(setting.env, [])
            per_call = any(scope == "per-call" for scope, _f, _n in sites)
            if not per_call and setting.env not in registry and setting.env not in engine:
                unchecked.append("%s (%s)" % (setting.key, setting.env))
        self.assertEqual([], unchecked,
                         "advertised live with nothing checking the claim -- no per-call reader "
                         "the audit can see, not owned by the tenant-policy registry, and not read per "
                         "call by the engine: %s"
                         % ", ".join(unchecked))

    def test_the_audit_still_reaches_the_tree(self) -> None:
        """Both label tests skip a setting with no sites, so shrinking coverage reads as success."""
        with_sites = sum(1 for name in audit.SITES if audit.SITES[name])
        self.assertGreaterEqual(
            with_sites, SITE_COVERAGE_FLOOR,
            "the audit found readers for only %d settings; it reached %d when this floor was set. "
            "A narrowed scan reports no failures for the settings it stopped seeing."
            % (with_sites, SITE_COVERAGE_FLOOR))

    def test_the_engine_scan_finds_something(self) -> None:
        """An empty engine scan would silently stop admitting the settings only Rust reads."""
        engine = _rust_per_call_env_names()
        self.assertGreater(
            len(engine), 5,
            "the engine scan found %d per-call env reads; it cannot be doing its job, and every "
            "setting it should admit would fall to the other two classifiers or fail"
            % len(engine))


if __name__ == "__main__":
    unittest.main()
