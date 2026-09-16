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
    # Two spellings, because the crate has two. `std::env::var("NAME")` is the direct read;
    # `env_flag::env_bool("NAME", default)` is the shared vocabulary helper, and a reader that
    # adopts it disappears from a scan that only knows the first. Measured on the crate: 118
    # names are read directly and 47 ONLY through the helper -- among them TS_RAFT_ALLOW_PLAINTEXT,
    # TS_SERVER_READONLY, TS_META_RAFT and TS_CACHE_DISK_TIER. Those pass today because another
    # classifier happens to cover them; the engine arm of this test could not see one of them.
    #
    # The bare `env_bool("NAME", ...)` form is here too: two modules define a local `env_bool`
    # that delegates to the crate one, so the call site is spelled without the path.
    pattern = re.compile(
        r'(?:std::env::var|(?:[A-Za-z_]+::)*env_flag::env_bool|(?<![:\w])env_bool)'
        r'\(\s*&?"([A-Z0-9_]+)"')
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
            # A second route this scan could not see at all. The storage-tuning family is read by
            # `StorageTuningConfig::from_getter`, which passes a name CONSTANT to a closure calling
            # `env::var(name)` -- so those names never appear beside `env::var`, exactly the blind
            # spot this file was written to close on the Python side.
            #
            # The name a constant HOLDS is the variable an operator sets, and it is not always the
            # identifier: `pub const TS_BLOCK_SLAB_TARGET_BYTES: &str =
            # "TS_BLOCK_SLAB_TARGET_BYTES"`. Resolve by value.
            #
            # Counted as per-call because the accessors call `StorageTuningConfig::from_env()` on
            # each use and nothing caches the result -- checked, not assumed. If that ever gains a
            # OnceLock, these become restart settings and this admission is wrong.
            names.update(re.findall(
                r'pub const TS_[A-Z0-9_]+\s*:\s*&(?:\'static\s+)?str\s*=\s*"(TS_[A-Z0-9_]+)"',
                text))
            # A THIRD route, and the same blind spot one more time. A knob the first milestone
            # renamed reads the current spelling first and each previous spelling after it,
            # through `env_flag::env_value_any` / `env_number_first` / `env_bool_first`. Those
            # names appear beside none of the shapes above, so a knob that had a per-call reader
            # before the rename would read as having none after it -- the reader is unchanged and
            # only its spelling moved. Every name in the list is read per call, current and
            # previous alike.
            for call in re.finditer(
                    r"env_(?:value_any|number_first|bool_first)\(\s*&\[([^\]]*)\]", text, re.S):
                names.update(re.findall(r'"([A-Z0-9_]+)"', call.group(1)))
    return names

def _resolver_environ_scopes() -> dict:
    """resolver name -> True when its os.environ read happens per call.

    An import-time read would mean the registry captured the value once, and every setting this
    file calls live on the registry's account would be wrong at the same moment.

    A resolver may delegate. `explicit_int` is the value half of `explicit_int_with_source`, so a
    surface can report which level supplied a budget without keeping a second copy of the
    precedence; its body is a call and contains no `os.environ`. A call executes per call, so the
    claim survives -- provided whatever it delegates to reads per call too.

    So the delegation is followed TRANSITIVELY, with a visited set and a depth cap. One hop would
    accept `a -> b -> captured at import` as live, which is this guard's own failure wearing one
    more layer. The chain has to END in a body that reads the environment.
    """
    import matrixark_tenant_policy as policy

    with open(policy.__file__, encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())

    bodies = {node.name: node for node in ast.walk(tree)
              if isinstance(node, ast.FunctionDef)}

    def reads_environ(node) -> bool:
        return any(isinstance(d, ast.Attribute) and d.attr == "environ"
                   for d in ast.walk(node))

    def delegates_to(node):
        """The function this one is a THIN WRAPPER for, or None.

        Deliberately narrow. Following every call instead accepts an `os.environ` read found
        anywhere in the call graph -- and every resolver here calls `tenant_policy`, which reads
        the environment for the POLICY FILE PATH, which has nothing to do with the knob being
        resolved. A version of this that followed all calls passed a mutated resolver whose knob
        value came from a dict captured at import, which is precisely what this test is for.

        A wrapper is one `return <call>`, optionally subscripted -- `return f(a, b)[0]`. Anything
        with branches, assignments or more than one statement is doing work of its own and must
        show the read in its own body.
        """
        statements = [s for s in node.body if not (isinstance(s, ast.Expr)
                                                   and isinstance(s.value, ast.Constant))]
        if len(statements) != 1 or not isinstance(statements[0], ast.Return):
            return None
        value = statements[0].value
        if isinstance(value, ast.Subscript):
            value = value.value
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
            return value.func.id if value.func.id in bodies else None
        return None

    def live(name: str, seen: frozenset = frozenset(), depth: int = 0) -> bool:
        """True when this resolver's own value is read from the environment per call.

        A wrapper is followed, transitively and with a visited set, because a call runs per call
        and the claim survives it. One hop would accept `a -> b -> captured at import`; the chain
        must END in a body that reads the environment itself.
        """
        if name in seen or depth > 8 or name not in bodies:
            return False
        node = bodies[name]
        if reads_environ(node):
            return True
        target = delegates_to(node)
        if target is None:
            return False
        return live(target, seen | {name}, depth + 1)

    return {name: live(name) for name in RESOLVERS if name in bodies}


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

    def test_the_delegation_follow_still_rejects_a_dead_chain(self) -> None:
        """The follow added above must not turn into a way to pass.

        A resolver that delegates to something which never reads the environment has to fail, or
        the loosening here is just the original bug with an extra function in front of it. Checked
        against a synthetic module rather than by reasoning about the real one, because the real
        one currently passes and would prove nothing either way.
        """
        import ast as _ast
        source = (
            "import os\n"
            "def captured_at_import():\n"
            "    return _FROZEN\n"
            "def middle():\n"
            "    return captured_at_import()\n"
            "def explicit_int():\n"
            "    return middle()\n"
            "def reads_live():\n"
            "    return os.environ.get('X')\n"
            "def explicit_bool():\n"
            "    return reads_live()\n"
            # The case that fooled the first version of the follow: a resolver doing real work,
            # whose own value is captured at import, but which happens to call something that
            # reads the environment for an unrelated reason.
            "def unrelated_env_read():\n"
            "    return os.environ.get('POLICY_FILE')\n"
            "def resolver_with_a_frozen_value():\n"
            "    other = unrelated_env_read()\n"
            "    if other:\n"
            "        pass\n"
            "    return _FROZEN.get('X')\n"
        )
        tree = _ast.parse(source)
        bodies = {n.name: n for n in _ast.walk(tree) if isinstance(n, _ast.FunctionDef)}

        def reads_environ(node):
            return any(isinstance(d, _ast.Attribute) and d.attr == "environ"
                       for d in _ast.walk(node))

        def delegates_to(node):
            statements = [s for s in node.body if not (isinstance(s, _ast.Expr)
                                                       and isinstance(s.value, _ast.Constant))]
            if len(statements) != 1 or not isinstance(statements[0], _ast.Return):
                return None
            value = statements[0].value
            if isinstance(value, _ast.Subscript):
                value = value.value
            if isinstance(value, _ast.Call) and isinstance(value.func, _ast.Name):
                return value.func.id if value.func.id in bodies else None
            return None

        def live(name, seen=frozenset(), depth=0):
            if name in seen or depth > 8 or name not in bodies:
                return False
            node = bodies[name]
            if reads_environ(node):
                return True
            target = delegates_to(node)
            return live(target, seen | {name}, depth + 1) if target else False

        self.assertFalse(live("explicit_int"),
                         "a chain ending in an import-time capture was accepted as live")
        self.assertTrue(live("explicit_bool"),
                        "a chain ending in a per-call read was rejected")
        self.assertFalse(
            live("resolver_with_a_frozen_value"),
            "a resolver whose own value is captured at import was accepted as live because it "
            "calls something that reads the environment for an unrelated reason -- this is what "
            "the first version of the follow did to the real module")

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

    def test_the_engine_scan_sees_both_ways_the_crate_reads_a_flag(self) -> None:
        """A floor on the engine arm, and a positive control for the second spelling.

        `test_every_live_setting_is_classified_by_something` passes when a setting is classified
        by ANY of the three arms, so the engine arm going blind does not fail it -- it just stops
        contributing. A named flag that is read ONLY through `env_flag::env_bool` is asserted
        here, so a scan that regresses to the direct spelling fails by name rather than quietly.
        """
        engine = _rust_per_call_env_names()
        self.assertGreaterEqual(
            len(engine), 120,
            "the engine scan found only %d names; it has stopped reaching the crate" % len(engine))
        for name in ("TS_RAFT_ALLOW_PLAINTEXT", "TS_CACHE_DISK_TIER", "TS_SERVER_READONLY"):
            with self.subTest(flag=name):
                self.assertIn(
                    name, engine,
                    "%s is read through env_flag::env_bool and nothing else. The engine arm of "
                    "this file cannot see it, so a reader that adopts the shared vocabulary "
                    "helper disappears from the scan that watches it." % name)

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
