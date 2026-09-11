#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Some published values are absolute times, and moving them to a monotonic clock breaks them.

`time.time()` counts from 1970 and can step, forwards or backwards, whenever something adjusts the
clock. `time.monotonic()` never steps, which is why a DURATION should be measured with it -- and
its origin is arbitrary (boot, here), which is why an ABSOLUTE time must not be.

Most of this tree's `time.time()` reads are stored and then subtracted, and those could move. These
are the ones that cannot, because the stored value is also published as an absolute time -- one and
two hops from where it was read, which is why a sweep does not see it:

    matrixark_gateway_metrics     self._start -> start -> "matrixark_gateway_start_time_seconds"
                                  a Prometheus gauge documented as a Unix time. A dashboard
                                  computing `time() - start_time_seconds` would read the worker's
                                  age as fifty-odd years.

    matrixark_gateway_metrics     _sample_locked(time.time()) -> self._series -> "at"
                                  the portal chart's x-axis, which the page renders against the
                                  browser's clock. test_matrixark_the_ages_are_measured_against_
                                  the_deployment pins the ages measured from it.

    matrixark_google_oauth        now_s vs the token's `exp` and `iat`
                                  claims minted by GOOGLE's clock. A monotonic `now_s` is a small
                                  number, so `now_s > exp` is never true and every expired token
                                  verifies. This one fails OPEN.

    openai_compatible_*           every `"created"` field
                                  the OpenAI API defines it as a Unix timestamp and clients render
                                  it as a date.

    matrixark_v1_gateway          "first_used_at_ms" / "last_used_at_ms"
                                  per-key usage times, read back as wall-clock milliseconds.

Written because a static sweep for "which of these can move" got it wrong four times running. A
check keyed on the NAME missed `start = self._start`; one keyed on the ASSIGNMENT missed a value
arriving as a PARAMETER; one asking only about dict fields and format strings missed the OAuth
COMPARISON entirely, because a comparison publishes nothing and looks local; and one seeded on
`self.loaded_at_unix` missed the same value reached as `self.server.model_state.loaded_at_unix`.
Each version found the previous version's blind spot, so what is worth leaving behind is not the
sweep but these sites, named.

Two things that resolution has to get right, both learned by getting them wrong here:

* A BARE NAME is resolved inside its own function. `matrixark_v1_gateway` assigns `now` four times
  -- `int(time.time() * 1000)` in one class and `time.monotonic()` in another -- and a module-wide
  match reports both clocks for every one of them. (The module already keeps them apart by calling
  the monotonic one `now_m`.)
* A DOTTED ATTRIBUTE is resolved module-wide, and falls back to the attribute LEAF, because
  `self.loaded_at_unix = int(time.time())` and the read `self.server.model_state.loaded_at_unix`
  name one value and share no path.

The readers are asked by FIELD rather than by attribute, deliberately: there are three `"created"`
fields across two reader modules fed by three different expressions, and pinning the one attribute
I happened to read first would have covered one of them and reported the dimension done.

`AMonotonicCopyIsRejectedTest` runs every check against a copy with the clock swapped, and
`test_every_check_finds_a_clock_at_all` is separate from it: a check that resolves NO clock
satisfies `assertNotIn("mono", ...)` too, so "not monotonic" is evidence only once the check is
known to resolve something.
"""
from __future__ import annotations

import ast
import glob
import io
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

WALL = {"time.time", "time.time_ns"}
MONO = {"time.monotonic", "time.monotonic_ns", "time.perf_counter", "time.perf_counter_ns"}

#: Three today, across two reader modules. A floor, not a count -- a new reader should raise it,
#: and a check that silently stops finding them fails here rather than passing quietly.
CREATED_FIELDS_FLOOR = 3


def source_of(stem: str) -> str:
    with io.open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8") as handle:
        return handle.read()


def reader_stems() -> list:
    """Every OpenAI-compatible reader, found rather than listed."""
    return sorted(os.path.basename(path)[:-3]
                  for path in glob.glob(os.path.join(TOOLS, "openai_compatible_*.py"))
                  if not os.path.basename(path).startswith("test_"))


def parsed(source: str):
    tree = ast.parse(source)
    parent = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parent[child] = node
    return tree, parent


def enclosing_function(node, parent):
    seen = node
    while seen is not None:
        if isinstance(seen, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return seen
        seen = parent.get(seen)
    return None


def clocks_read_by(node) -> set:
    """Every clock family this subtree reads. CONTAINS, not IS -- the read is rarely at the top."""
    found = set()
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call):
            continue
        name = ast.unparse(sub.func)
        if name in WALL:
            found.add("wall")
        elif name in MONO:
            found.add("mono")
    return found


def _assignments_to(tree, scope, dotted: str) -> list:
    """Values assigned to `dotted`, resolved in the scope that name actually lives in."""
    is_attribute = "." in dotted
    leaf = dotted.rsplit(".", 1)[-1]
    searched = tree if is_attribute else (scope if scope is not None else tree)

    exact, by_leaf = [], []
    for node in ast.walk(searched):
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        else:
            continue
        for target in targets:
            text = ast.unparse(target)
            if text == dotted:
                exact.append(value)
            elif is_attribute and text.rsplit(".", 1)[-1] == leaf:
                by_leaf.append(value)
    return exact or by_leaf


def clock_behind(source: str, expression, *, scope=None, hops: int = 4) -> set:
    """Which clock the value of `expression` came from, following simple copies.

    `start = self._start` and `self._start = time.time()` is two hops -- the chain the first
    version of this check could not see.
    """
    tree, _parent = parsed(source)
    if isinstance(expression, str):
        try:
            seed = ast.parse(expression, mode="eval").body
        except SyntaxError:  # pragma: no cover - the callers pass expressions
            return set()
    else:
        seed = expression

    found = clocks_read_by(seed)
    frontier = {ast.unparse(n) for n in ast.walk(seed)
                if isinstance(n, (ast.Name, ast.Attribute))}
    seen = set()
    for _ in range(hops):
        following = set()
        for name in frontier - seen:
            seen.add(name)
            for value in _assignments_to(tree, scope, name):
                found |= clocks_read_by(value)
                for sub in ast.walk(value):
                    if isinstance(sub, (ast.Name, ast.Attribute)):
                        following.add(ast.unparse(sub))
        frontier = following - seen
        if not frontier:
            break
    return found


def dict_fields_named(source: str, field: str) -> list:
    """Every `{"<field>": <expression>}`, with the function it is written in."""
    tree, parent = parsed(source)
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant) and key.value == field:
                out.append((getattr(value, "lineno", node.lineno), value,
                            enclosing_function(node, parent)))
    return out


def clocks_behind_field(source: str, field: str) -> set:
    found = set()
    for _line, value, scope in dict_fields_named(source, field):
        found |= clock_behind(source, value, scope=scope)
    return found


def _scope_containing(source: str, marker: str):
    """The function whose text contains `marker` -- how the bare-name checks find their scope."""
    tree, _parent = parsed(source)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        try:
            if marker in ast.unparse(node):
                return node
        except Exception:  # pragma: no cover - unparse is total for functions
            continue
    return None


# --------------------------------------------------------------------------------------------
# The checks. Each is a function of source text so the swapped-clock control can re-run it.
# --------------------------------------------------------------------------------------------

def check_prometheus_start_gauge(source: str) -> set:
    assert "matrixark_gateway_start_time_seconds" in source, "the gauge is gone; re-aim this check"
    scope = _scope_containing(source, "matrixark_gateway_start_time_seconds %g")
    assert scope is not None, "the gauge is not built in a function any more; re-aim this check"
    return clock_behind(source, "start", scope=scope)


def check_portal_series_timestamp(source: str) -> set:
    """The sample is timestamped by what `_sample_locked` is CALLED with, not by anything inside
    it -- the value arrives as a parameter."""
    tree, _parent = parsed(source)
    found, calls = set(), 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "_sample_locked":
            continue
        if not node.args:
            continue
        calls += 1
        found |= clocks_read_by(node.args[0])
    assert calls, "nothing calls _sample_locked any more; re-aim this check"
    return found


def check_oauth_expiry_comparison(source: str) -> set:
    assert "now_s > exp" in source, "the expiry comparison changed shape; re-aim this check"
    return clock_behind(source, "now_s", scope=_scope_containing(source, "now_s > exp"))


def check_reader_created_fields(source: str) -> set:
    return clocks_behind_field(source, "created")


def check_gateway_key_usage_times(source: str) -> set:
    return (clocks_behind_field(source, "first_used_at_ms")
            | clocks_behind_field(source, "last_used_at_ms"))


def every_check() -> list:
    """(stem, check, label) for every site, readers found rather than listed."""
    checks = [
        ("matrixark_gateway_metrics", check_prometheus_start_gauge,
         "the Prometheus start-time gauge"),
        ("matrixark_gateway_metrics", check_portal_series_timestamp,
         "the portal chart's sample timestamp"),
        ("matrixark_google_oauth", check_oauth_expiry_comparison,
         "the Google token expiry comparison"),
        ("matrixark_v1_gateway", check_gateway_key_usage_times,
         "the per-key usage times"),
    ]
    for stem in reader_stems():
        if dict_fields_named(source_of(stem), "created"):
            checks.append((stem, check_reader_created_fields, "%s: the created field" % stem))
    return checks


class AnAbsoluteTimeReadsTheWallClockTest(unittest.TestCase):

    def test_the_prometheus_start_gauge_is_a_unix_time(self) -> None:
        clocks = check_prometheus_start_gauge(source_of("matrixark_gateway_metrics"))
        self.assertIn("wall", clocks, "the gauge is documented as a Unix time")
        self.assertNotIn("mono", clocks,
                         "a monotonic origin would publish the worker's start as 1970")

    def test_the_portal_series_timestamp_is_a_unix_time(self) -> None:
        clocks = check_portal_series_timestamp(source_of("matrixark_gateway_metrics"))
        self.assertIn("wall", clocks)
        self.assertNotIn("mono", clocks, "the page measures its ages against this value")

    def test_the_token_expiry_is_compared_against_a_unix_time(self) -> None:
        """The one that fails OPEN: a monotonic `now_s` never exceeds a Google `exp`."""
        clocks = check_oauth_expiry_comparison(source_of("matrixark_google_oauth"))
        self.assertIn("wall", clocks)
        self.assertNotIn("mono", clocks, "every expired token would verify")

    def test_the_per_key_usage_times_are_unix_times(self) -> None:
        clocks = check_gateway_key_usage_times(source_of("matrixark_v1_gateway"))
        self.assertIn("wall", clocks)
        self.assertNotIn("mono", clocks)

    def test_every_reader_created_field_is_a_unix_time(self) -> None:
        """By FIELD, not by attribute. Three fields, three different expressions feeding them."""
        for stem in reader_stems():
            source = source_of(stem)
            if not dict_fields_named(source, "created"):
                continue
            with self.subTest(reader=stem):
                clocks = check_reader_created_fields(source)
                self.assertIn("wall", clocks, "the OpenAI created field is a Unix timestamp")
                self.assertNotIn("mono", clocks)

    def test_the_readers_still_publish_the_fields_this_is_about(self) -> None:
        """The floor. A check that stops FINDING the fields passes every assertion above."""
        total = sum(len(dict_fields_named(source_of(stem), "created"))
                    for stem in reader_stems())
        self.assertGreaterEqual(
            total, CREATED_FIELDS_FLOOR,
            "found %d created fields, expected at least %d -- this check has gone blind, or the "
            "field moved and the guard needs re-aiming" % (total, CREATED_FIELDS_FLOOR))


class AMonotonicCopyIsRejectedTest(unittest.TestCase):
    """The floor for the whole file: every check is run against a copy with the clock swapped."""

    def test_every_check_notices_a_monotonic_clock(self) -> None:
        for stem, check, label in every_check():
            with self.subTest(check=label):
                swapped = source_of(stem).replace("time.time(", "time.monotonic(")
                self.assertIn("mono", check(swapped),
                              "%s: the check cannot see a monotonic clock, so its verdict on the "
                              "real source is not evidence" % label)

    def test_every_check_finds_a_clock_at_all(self) -> None:
        """Separate from which clock: a check resolving NOTHING satisfies assertNotIn too."""
        for stem, check, label in every_check():
            with self.subTest(check=label):
                self.assertTrue(check(source_of(stem)),
                                "%s: resolved no clock, so it is not testing anything" % label)


if __name__ == "__main__":
    unittest.main()
