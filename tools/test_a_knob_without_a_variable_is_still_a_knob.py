#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A knob with no environment variable is still a knob, and the variable is really gone.

Seven tenant knobs had an environment variable nothing set: no launcher, no document, no config
file, no test. The variable and the portal row it generates were the only deployment-wide route
to them; the per-tenant route through the policy file was always there and is where a knob of
this kind belongs.

So the variable was taken off the `Knob(...)` and nothing else was touched. Two mechanisms were
already written for that and neither had ever run -- no knob in the registry had an empty `env`:

    _knob_settings   `if not env or env in taken or name in INTERNAL_KNOBS: continue`
    resolve          user -> tenant -> env -> default, and an empty env simply drops the third

This file is what makes them exercised rather than merely present. Both halves are measured in a
FRESH SUBPROCESS, because the policy module caches the file it reads and this process may have
loaded one already.

Measured before and after the change, with all seven variables set to the opposite of their
built-in default in the second arm:

    knob                               before: variable set   after: variable set
    generate_l1_summaries              False  (it moved)      True   (no effect)
    audit_payload_retain_per_scope     119    (it moved)      20     (no effect)
    ... and the other five the same

and the policy file still carries all seven, before and after.

The list below is FIXED rather than derived from the registry. Deriving it asks the changed tree
which knobs have no variable, which is the set this file is about -- the first version of the
probe did exactly that, came back with an empty list, and reported "0 knobs the variable still
moves" for a tree where it still moved all seven.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_tenant_policy as policy  # noqa: E402
import matrixark_gateway_config as gateway  # noqa: E402

#: The seven, by knob name. Fixed on purpose -- see the note above.
RETIRED = (
    "audit_payload_retain_per_scope",
    "node_path_embeddings",
    "traverse_sibling_sessions",
)

#: What each one's variable used to be, so "setting it does nothing" can be tested by setting it.
RETIRED_VARIABLE = {
    "audit_payload_retain_per_scope": "MATRIXARK_AUDIT_PAYLOAD_RETAIN_PER_SCOPE",
    "generate_l1_summaries": "MATRIXARK_GENERATE_L1_SUMMARIES",
    "node_path_embeddings": "MATRIXARK_NODE_PATH_EMBEDDINGS",
    "traverse_sibling_sessions": "MATRIXARK_TRAVERSE_SIBLING_SESSIONS",
}

PROBE = r"""
import json, os, sys
sys.path.insert(0, %r)
import matrixark_tenant_policy as policy
print(json.dumps({k: policy.resolve(k, {}) for k in %r}))
"""


def _clean_env(extra=None):
    env = {n: v for n, v in os.environ.items()
           if not n.startswith(("MATRIXARK_", "TS_", "TEMPORALSTORE_"))}
    env.update(extra or {})
    return env


def _resolve_in_subprocess(extra=None):
    result = subprocess.run(
        [sys.executable, "-B", "-c", PROBE % (TOOLS, list(RETIRED))],
        capture_output=True, text=True, cwd=TOOLS, env=_clean_env(extra))
    if result.returncode != 0:
        raise AssertionError("probe failed:\n%s" % result.stderr[-800:])
    return json.loads(result.stdout.strip().splitlines()[-1])


def _opposite(knob):
    if knob.kind == "bool":
        return "0" if knob.default else "1"
    return str(int(knob.default) + 99)


class AKnobWithoutAVariableTest(unittest.TestCase):

    def test_the_registry_still_holds_them(self) -> None:
        """Retiring the variable must not retire the knob."""
        for name in RETIRED:
            with self.subTest(knob=name):
                self.assertIn(name, policy.KNOBS)
                self.assertEqual("", policy.KNOBS[name].env,
                                 "%s still names an environment variable" % name)

    def test_most_knobs_still_have_one(self) -> None:
        """The floor. If the registry ever went all-empty this file would pass on a tree where
        `_knob_settings` offers nothing at all."""
        with_env = [n for n, k in policy.KNOBS.items() if getattr(k, "env", "")]
        self.assertGreaterEqual(
            len(with_env), 20,
            "only %d knobs still name a variable; the empty-env path is no longer the exception "
            "and this file is not describing what it says it describes" % len(with_env))

    def test_none_of_them_is_offered_on_the_page(self) -> None:
        """`_knob_settings` skips a knob with no env, so the generated `behaviour.` row goes with
        the variable. That is the half of this change that moves the configurable count."""
        offered = {s.key for s in gateway.SETTINGS}
        for name in RETIRED:
            with self.subTest(knob=name):
                self.assertNotIn("behaviour." + name, offered)

    def test_the_tenant_policy_file_still_carries_them(self) -> None:
        """The route that has to survive, measured rather than asserted."""
        wanted = {}
        for name in RETIRED:
            knob = policy.KNOBS[name]
            wanted[name] = (not knob.default) if knob.kind == "bool" else int(knob.default) + 7
        directory = tempfile.mkdtemp(prefix="matrixark-knob-route-")
        path = os.path.join(directory, "tenant_policy.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"defaults": wanted}, handle)
        got = _resolve_in_subprocess({"MATRIXARK_TENANT_POLICY_PATH": path})
        for name in RETIRED:
            with self.subTest(knob=name):
                self.assertEqual(wanted[name], got[name],
                                 "%s is no longer reachable through the tenant policy file, so "
                                 "taking its variable away removed the knob rather than one "
                                 "route to it" % name)

    def test_setting_the_old_variable_does_nothing(self) -> None:
        """The other direction. Without this, a knob that quietly kept reading its variable would
        satisfy every assertion above."""
        plain = _resolve_in_subprocess()
        extra = {RETIRED_VARIABLE[n]: _opposite(policy.KNOBS[n]) for n in RETIRED}
        self.assertEqual(len(RETIRED), len(extra), "every retired variable must be set")
        with_variable = _resolve_in_subprocess(extra)
        moved = [n for n in RETIRED if plain[n] != with_variable[n]]
        self.assertEqual(
            [], moved,
            "these still follow the variable that was supposed to be gone: %s" % moved)
        for name in RETIRED:
            self.assertEqual(policy.KNOBS[name].default, plain[name],
                             "%s no longer falls back to its own default" % name)


if __name__ == "__main__":
    unittest.main()
