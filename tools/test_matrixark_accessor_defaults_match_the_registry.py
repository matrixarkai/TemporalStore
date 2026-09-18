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
NUMERIC_DEFAULT = re.compile(r"\(default\s+(\d+)\)", re.I)

#: An int accessor cannot be found by its NAME. `index_hard_ceiling` resolves
#: `max_secondary_index_records_per_tenant`, which the name does not contain, so the knob is
#: read out of the accessor's own `resolve_tenant_policy("...")` call instead of a list here.
_RESOLVES = re.compile(r'resolve_tenant_policy\(\s*["\']([a-z0-9_]+)["\']')


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


def _int_accessors():
    """(knob name, function) for every accessor that resolves exactly one INT knob.

    Derived from the call, not the name, and an accessor that resolves more than one knob is
    skipped rather than guessed at: the pair it should be compared against is ambiguous.
    """
    out = []
    for name, func in vars(gates).items():
        if name.startswith("_") or not callable(func):
            continue
        if getattr(func, "__module__", None) != gates.__name__:
            continue
        try:
            source = inspect.getsource(func)
        except (OSError, TypeError):
            continue
        resolved = set(_RESOLVES.findall(source))
        if len(resolved) != 1:
            continue
        knob_name = resolved.pop()
        knob = policy.KNOBS.get(knob_name)
        if knob is None or getattr(knob, "kind", "") != "int":
            continue
        # An accessor that REQUIRES a caller-supplied default has no registry-resolved default to
        # compare against -- `max_summary_text_chars(*, default)` is one. Skipped on the signature
        # rather than by name, so a second one is skipped for the same stated reason.
        required = [p for p in inspect.signature(func).parameters.values()
                    if p.default is inspect.Parameter.empty
                    and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)]
        if required:
            continue
        out.append((knob_name, func))
    return sorted(out, key=lambda pair: (pair[0], pair[1].__name__))


class AccessorsAgreeWithTheRegistryTest(unittest.TestCase):

    def setUp(self) -> None:
        # Restore the ENTIRE environment afterwards. These tests ask what an accessor returns for an
        # UNCONFIGURED tenant, which means unsetting each knob's env var -- and `unittest discover`
        # runs the whole suite in ONE process, so unsetting without restoring silently reconfigures
        # every test that comes after. It did: 35 unrelated tests failed, none of them near this
        # file. Same defect as the ~180 set_var sites that make the Rust suite look flaky.
        self._saved_environ = dict(os.environ)
        self.addCleanup(self._restore_environ)
        self.accessors = _bool_accessors()
        self.assertGreater(len(self.accessors), 5,
                           "found almost no bool accessors, so these comparisons prove nothing")
        self.int_accessors = _int_accessors()
        self.assertGreater(
            len(self.int_accessors), 1,
            "found almost no int accessors, so the numeric comparisons below prove nothing. They "
            "are collected from each accessor's own resolve_tenant_policy call, so this drops to "
            "zero if that call is renamed or wrapped rather than if the accessors disappear.")

    def _restore_environ(self) -> None:
        os.environ.clear()
        os.environ.update(self._saved_environ)

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

    def test_an_int_accessor_resolves_the_registry_default(self) -> None:
        for knob_name, func in self.int_accessors:
            with self.subTest(knob=knob_name, accessor=func.__name__):
                knob = policy.KNOBS[knob_name]
                os.environ.pop(getattr(knob, "env", "") or "_none_", None)
                for alias in getattr(knob, "env_aliases", ()) or ():
                    os.environ.pop(alias, None)
                try:
                    got = func({"tenant_id": "registry_check_%s" % func.__name__})
                except TypeError:
                    got = func()
                self.assertEqual(
                    int(knob.default), int(got),
                    "%s resolves to %r for an unconfigured tenant while the registry default for "
                    "%s is %r." % (func.__name__, got, knob_name, knob.default))

    def test_an_int_accessor_states_the_registry_default(self) -> None:
        """The half that was missing. `index_hard_ceiling` said "(default 2048)" and returned 1024,
        because the regex above only ever matched ON/OFF/True/False and the collector only ever took
        names ending `_enabled`. A stated default is what the next person reads instead of checking,
        and a memory-bounding lever stated at twice its value sizes an operator's expectations
        wrong."""
        checked = 0
        for knob_name, func in self.int_accessors:
            match = NUMERIC_DEFAULT.search(inspect.getdoc(func) or "")
            if not match:
                continue
            checked += 1
            with self.subTest(knob=knob_name, accessor=func.__name__):
                self.assertEqual(
                    int(policy.KNOBS[knob_name].default), int(match.group(1)),
                    "%s's docstring says (default %s) and the registry default for %s is %r."
                    % (func.__name__, match.group(1), knob_name,
                       policy.KNOBS[knob_name].default))
        self.assertGreater(
            checked, 0,
            "no int accessor docstring names a default, so this checked nothing. It is allowed for "
            "an accessor to say nothing; it is not allowed for ALL of them to, because that is "
            "also what a broken NUMERIC_DEFAULT looks like.")


if __name__ == "__main__":
    unittest.main()
