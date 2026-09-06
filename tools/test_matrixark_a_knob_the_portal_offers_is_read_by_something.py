#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A knob the portal offers should be read by something.

Every ``behaviour.*`` setting on the portal is a tenant-policy knob. Setting one writes a value the
resolver returns and the history records -- and for eleven of them, that is the end of it. Their only
accessor lives in ``matrixark_index_growth_bound``, in a function no production code calls, and no
other module reads the variable. The operator gets a control, a stored value and a confirmation,
and the deployment behaves identically either way.

The live deployment this was found on had ``behaviour.summary_levels``,
``behaviour.recall_reinforcement`` and ``behaviour.generate_l1_summaries`` all set. None of them
did anything.

**Two ways a knob is read, and both count.** Either something calls its accessor -- which requires
that accessor to be reachable from a production entry point, not merely to exist -- or some module
reads the environment variable directly. Seven of the unreachable accessors are in the second
group: ``top_k_per_layer``'s accessor is dead while ``matrixark_mcp_core`` reads
MATRIXARK_TOP_K_PER_LAYER on its own. Those knobs work. Counting them as dead would be wrong, and
grepping for the accessor name alone would have done exactly that.

**The list below may only shrink.** It is not a skip list: a name in it that turns out to BE read
fails ``test_the_list_holds_nobody_who_is_read``, so wiring one up forces its removal. What the
list prevents is the set growing quietly.
"""
from __future__ import annotations

import ast
import io
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as _cfg  # noqa: E402
import matrixark_tenant_policy as tp  # noqa: E402

GROWTH_BOUND = "matrixark_index_growth_bound.py"

# Where a knob is DECLARED rather than used. A mention here is not a consumer.
DECLARING = {GROWTH_BOUND, "matrixark_tenant_policy.py", "matrixark_gateway_config.py"}

# The registry's own list, not a second copy of it. The portal badges a field with this set, so
# a list here that drifted from it would fail the deployment's screen while passing its tests --
# and a test holding its own copy of the answer is not checking the answer.
#
# Measured 2026-09-06. `write_secondary_index` is the one worth reading twice: it appears EXACTLY
# ONCE in the repository, the line that declares it, and defaults to on, so an operator turning
# secondary index writes off to hold down index growth gets a stored value, a confirmation, and
# the same number of records as before.
OFFERED_AND_UNREAD = _cfg.KNOBS_READ_BY_NOTHING


_SOURCES_CACHE = {}
_REACHABLE_CACHE = set()
_UNREAD_CACHE = set()


def _production_sources() -> dict:
    if _SOURCES_CACHE:
        return _SOURCES_CACHE
    out = {}
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_") or name in DECLARING:
            continue
        try:
            out[name] = io.open(os.path.join(TOOLS, name), encoding="utf-8").read()
        except OSError:
            continue
    _SOURCES_CACHE.update(out)
    return out


def _reads_the_variable(sources: dict, variable: str) -> list:
    pattern = re.compile(r'["\']%s["\']' % re.escape(variable))
    return sorted(name for name, src in sources.items() if pattern.search(src))


def _reachable_accessors() -> set:
    """Growth-bound functions reachable from a call made outside that module.

    Reachability, not existence: ``summary_levels`` IS called -- by ``node_summary_plan``, which
    nothing calls. A caller count would have reported it live.
    """
    if _REACHABLE_CACHE:
        return _REACHABLE_CACHE
    tree = ast.parse(io.open(os.path.join(TOOLS, GROWTH_BOUND), encoding="utf-8").read())
    defs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}

    graph = {}
    for name, node in defs.items():
        inner = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call):
                called = getattr(sub.func, "id", None) or getattr(sub.func, "attr", None)
                if called in defs:
                    inner.add(called)
        graph[name] = inner

    entries = set()
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_") or name == GROWTH_BOUND:
            continue
        try:
            outer = ast.parse(io.open(os.path.join(TOOLS, name), encoding="utf-8").read())
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(outer):
            if isinstance(node, ast.Call):
                called = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
                if called in defs:
                    entries.add(called)

    reachable, stack = set(entries), list(entries)
    while stack:
        for nxt in graph.get(stack.pop(), ()):
            if nxt not in reachable:
                reachable.add(nxt)
                stack.append(nxt)
    _REACHABLE_CACHE.update(reachable)
    return reachable


def _resolved_by_name() -> set:
    """Knob names passed as a string to the policy resolver.

    The third way a knob is read, and the one this file first missed: `enforce_secondary_index_bounds`
    is reachable and asks for `max_secondary_index_records_per_tenant` by NAME, never touching the
    variable and never calling an accessor of that name. Four knobs were reported dead on that
    omission alone.
    """
    asked = set()
    for name, src in _production_sources().items():
        for match in re.finditer(r'resolve(?:_tenant_policy)?\(\s*["\']([a-z0-9_]+)["\']', src):
            asked.add(match.group(1))
    # ...and from the reachable half of the growth-bound module itself.
    reachable = _reachable_accessors()
    tree = ast.parse(io.open(os.path.join(TOOLS, GROWTH_BOUND), encoding="utf-8").read())
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef) or node.name not in reachable:
            continue
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Call):
                continue
            called = getattr(sub.func, "id", None) or getattr(sub.func, "attr", None)
            if called in ("resolve", "resolve_tenant_policy") and sub.args:
                first = sub.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    asked.add(first.value)
    return asked


def _unread_knobs() -> set:
    if _UNREAD_CACHE:
        return _UNREAD_CACHE
    sources = _production_sources()
    reachable = _reachable_accessors()
    by_name = _resolved_by_name()
    unread = set()
    for name, knob in tp.KNOBS.items():
        if not knob.env:
            continue
        if _reads_the_variable(sources, knob.env):
            continue
        if name in reachable or (name + "_enabled") in reachable:
            continue
        if name in by_name:
            continue
        unread.add(name)
    _UNREAD_CACHE.update(unread)
    return unread


class TheKnobsThePortalOffersAreReadBySomething(unittest.TestCase):

    def test_the_set_has_not_grown(self) -> None:
        new = sorted(_unread_knobs() - OFFERED_AND_UNREAD)
        self.assertEqual([], new,
                         "%d knob(s) became unread: the portal offers them, the policy resolves "
                         "them, and nothing reads the result" % len(new))

    def test_the_list_holds_nobody_who_is_read(self) -> None:
        """So the list can only shrink. Wire one up and this fails until it is removed."""
        wired = sorted(OFFERED_AND_UNREAD - _unread_knobs())
        self.assertEqual([], wired,
                         "%s is read now -- take it out of OFFERED_AND_UNREAD" % (wired,))

    def test_every_name_in_the_list_is_a_real_knob(self) -> None:
        self.assertEqual([], sorted(n for n in OFFERED_AND_UNREAD if n not in tp.KNOBS))


class TheDetectorWorks(unittest.TestCase):
    """Every assertion above is an equality against an empty list, which is also what a detector
    that finds nothing produces."""

    def test_it_finds_the_knobs_that_are_read(self) -> None:
        read = {name for name in tp.KNOBS if name not in _unread_knobs()}
        self.assertGreater(len(read), len(OFFERED_AND_UNREAD), sorted(read)[:5])

    def test_a_variable_nothing_mentions_is_reported_unread(self) -> None:
        sources = _production_sources()
        self.assertGreater(len(sources), 100, len(sources))
        self.assertEqual([], _reads_the_variable(sources, "MATRIXARK_NO_SUCH_VARIABLE_AT_ALL"))

    def test_a_variable_that_is_read_is_reported_read(self) -> None:
        self.assertTrue(_reads_the_variable(_production_sources(), "MATRIXARK_TOP_K_PER_LAYER"),
                        "the reader check found nothing for a variable known to be read")

    def test_reachability_is_narrower_than_existence(self) -> None:
        """The distinction the whole file turns on: ``summary_levels`` exists and is called, and is
        still unreachable, because its only caller is unreachable too."""
        reachable = _reachable_accessors()
        self.assertGreater(len(reachable), 5, sorted(reachable))
        self.assertNotIn("summary_levels", reachable)
        self.assertNotIn("node_summary_plan", reachable)


class TheScreenSaysSoTest(unittest.TestCase):
    """A test that only reports the number in CI leaves the operator looking at eleven controls
    that accept a value, confirm it, and do nothing. The field carries the answer now."""

    @staticmethod
    def _fields() -> list:
        snapshot = _cfg.snapshot()
        groups = snapshot["groups"]
        if isinstance(groups, list):
            return [field for group in groups for field in group["fields"]]
        return [field for fields in groups.values() for field in fields]

    def test_every_unread_knob_is_marked(self) -> None:
        marked = {f["key"] for f in self._fields() if f.get("read_by_nothing")}
        self.assertEqual(len(_cfg.KNOBS_READ_BY_NOTHING), len(marked),
                         "%d knobs, %d marked: %s" % (len(_cfg.KNOBS_READ_BY_NOTHING),
                                                      len(marked), sorted(marked)))

    def test_the_match_is_by_variable_not_by_key_prefix(self) -> None:
        """The discriminator. Ten of the eleven surface as `behaviour.<knob>`; this one does not,
        and a prefix rule marked ten and left it looking like a working control."""
        marked = {f["key"] for f in self._fields() if f.get("read_by_nothing")}
        self.assertIn("skills.description_always", marked)
        self.assertNotIn("behaviour.skill_description_always",
                         {f["key"] for f in self._fields()})

    def test_nothing_else_is_marked(self) -> None:
        """A badge on every field is a badge on none."""
        fields = self._fields()
        marked = [f["key"] for f in fields if f.get("read_by_nothing")]
        self.assertGreater(len(fields), len(marked) * 5, len(fields))
        for field in fields:
            self.assertIn("read_by_nothing", field, field["key"])

    def test_the_control_is_still_offered(self) -> None:
        """Not hidden. A deployment may already have one set, and a field that vanishes takes its
        value out of view while leaving it in the file."""
        keys = {f["key"] for f in self._fields()}
        for name in _cfg.KNOBS_READ_BY_NOTHING:
            self.assertTrue(
                any(f.get("env") and f["env"].endswith(name.upper()) for f in self._fields())
                or ("behaviour." + name) in keys,
                "%s is no longer offered at all" % name)


if __name__ == "__main__":
    unittest.main()
