#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Both languages honour the same spellings of `TS_STORAGE_BACKEND`.

`matrixark_deployment_plan.resolve_backend` says of itself:

    Mirrors `StorageBackendConfig::resolve_decision`: raft is forced; matrixobject is forced
    when compiled; shared is forced only when a directory is configured ...

Nothing checked that it does. The engine accepts **ten** spellings across three backends, and the
plan has its own copy of all ten:

    matrixobject | matrix_object | object
    shared | shared_path | shared_store | path
    raft | raft_replication | replication

A spelling one side honours and the other does not is not a cosmetic difference. The plan is what
tells an operator which backend a deployment will get; the engine is what they actually get. They
diverge silently, because a request that is not recognised does not fail — it falls through to
auto-detection, which `resolve_backend`'s own docstring calls out as "from the outside ...
indistinguishable from having been honoured".

The same shape already cost a production incident on the neighbouring rule: `TS_META_ADDR=local`
was a sentinel to one implementation and a literal socket address to another, and every write on
a one-box failed. See `test_both_languages_read_the_same_sentinels.py`.

Normalisation is part of the rule on both sides: rust matches on
`value.trim().to_ascii_lowercase()`, python on `_clean(...).lower()`, so `" MatrixObject "` is
honoured by both.
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
RUST = os.path.join(REPO, "crates", "temporalstore-rust", "src", "storage_backend.rs")

sys.path.insert(0, TOOLS)

import matrixark_deployment_plan as plan  # noqa: E402

#: One match arm of `BackendOverride::parse`: a run of `Some("...")` alternatives, then the
#: variant it selects — either `Self::Name,` or a braced block holding `Self::Name`.
_ARM = re.compile(
    r'(?P<spellings>Some\("[a-z_]+"\)(?:\s*\|\s*Some\("[a-z_]+"\))*)\s*=>\s*'
    r'(?:Self::(?P<direct>\w+)|\{\s*Self::(?P<braced>\w+)\s*\})')
_SPELLING = re.compile(r'Some\("([a-z_]+)"\)')

#: The python side of the same rule: `if requested in ("a", "b"):` followed, within the branch,
#: by the backend it returns. Parsed rather than probed, so the comparison below can be an
#: EQUALITY -- probing only ever shows that python honours everything rust does, never that it
#: honours something rust has since dropped.
_PY_GROUP = re.compile(
    r'if\s+requested\s+in\s*\((?P<spellings>[^)]*)\)\s*:'
    r'(?P<body>(?:.|\n){0,400}?)"backend"\s*:\s*"(?P<backend>[a-z_]+)"')
_PY_SPELLING = re.compile(r'"([a-z_]+)"')

#: The one correspondence that cannot be derived: the rust variant names and the strings the
#: python plan reports are simply spelled differently. Exhaustiveness is asserted below, so a
#: new variant fails this file rather than being skipped by it.
VARIANT_TO_PLAN_BACKEND = {
    "MatrixObject": "matrixobject",
    "SharedPath": "shared_path",
    "Raft": "raft",
}

#: What each backend needs before the plan will call the request honoured. Without these the
#: engine forces the backend but the plan reports the auto fall-through, and the comparison
#: would be measuring the precondition rather than the spelling.
PRECONDITIONS = {
    "matrixobject": ({}, {"matrixobject_available": True}),
    "shared_path": ({"TS_SHARED_STORE_DIR": "/srv/shared"}, {}),
    "raft": ({}, {}),
}


def _rust_source():
    with io.open(RUST, encoding="utf-8") as handle:
        return handle.read()


def plan_overrides():
    """{backend: {spelling, ...}} read from resolve_backend."""
    with io.open(plan.__file__, encoding="utf-8") as handle:
        source = handle.read()
    found = {}
    for match in _PY_GROUP.finditer(source):
        found.setdefault(match.group("backend"), set()).update(
            _PY_SPELLING.findall(match.group("spellings")))
    return found


def rust_overrides():
    """{variant: {spelling, ...}} read from BackendOverride::parse."""
    found = {}
    for match in _ARM.finditer(_rust_source()):
        variant = match.group("direct") or match.group("braced")
        found[variant] = set(_SPELLING.findall(match.group("spellings")))
    return found


class BothLanguagesHonourTheSameBackendSpellingsTest(unittest.TestCase):

    def test_the_rust_arms_are_still_readable(self) -> None:
        """Everything below iterates what this returns, so an empty scan would pass silently."""
        found = rust_overrides()
        self.assertGreaterEqual(
            len(found), 3,
            "read %d BackendOverride arms out of storage_backend.rs; if parse() changed shape, "
            "move this check with it rather than deleting it" % len(found))
        total = sum(len(v) for v in found.values())
        self.assertGreaterEqual(total, 8, "only %d spellings in total" % total)

    def test_every_rust_variant_is_mapped(self) -> None:
        """The mapping below is hand-written; this is what stops a new backend being skipped."""
        self.assertEqual(
            sorted(rust_overrides()), sorted(VARIANT_TO_PLAN_BACKEND),
            "BackendOverride variants and the plan mapping have diverged")

    def test_the_two_spelling_sets_are_equal(self) -> None:
        """Both directions. A spelling only RUST knows is one the plan will call auto while the
        engine forces it; a spelling only PYTHON knows is one the plan promises and the engine
        sends to auto-detection. The driven check below cannot see the second kind."""
        mine = plan_overrides()
        self.assertGreaterEqual(len(mine), 3,
                                "read %d groups out of resolve_backend" % len(mine))
        for variant, spellings in sorted(rust_overrides().items()):
            backend = VARIANT_TO_PLAN_BACKEND[variant]
            with self.subTest(backend=backend):
                self.assertEqual(
                    sorted(spellings), sorted(mine.get(backend, ())),
                    "the engine and the plan accept different spellings for %s" % backend)

    def test_the_plan_honours_every_spelling_the_engine_does(self) -> None:
        for variant, spellings in sorted(rust_overrides().items()):
            backend = VARIANT_TO_PLAN_BACKEND[variant]
            extra_env, kwargs = PRECONDITIONS[backend]
            for spelling in sorted(spellings):
                with self.subTest(variant=variant, spelling=spelling):
                    env = dict(extra_env, TS_STORAGE_BACKEND=spelling)
                    got = plan.resolve_backend(env, **kwargs)
                    self.assertEqual(
                        backend, got.get("backend"),
                        "the engine reads %r as %s; the plan reports %r"
                        % (spelling, variant, got.get("backend")))
                    self.assertTrue(
                        got.get("honoured"),
                        "the plan does not recognise %r, so it reports the auto fall-through "
                        "while the engine forces %s" % (spelling, variant))

    def test_both_sides_normalise_the_request(self) -> None:
        normalises = re.search(
            r"raw\.map\(\|value\|\s*value\.trim\(\)\.to_ascii_lowercase\(\)\)", _rust_source())
        self.assertTrue(
            normalises,
            "BackendOverride::parse no longer trims and lowercases, so ` Raft ` stops being "
            "honoured by the engine while the plan still reports it as forced")
        for written in (" raft ", "RAFT", "\tRaft\n"):
            with self.subTest(written=written):
                got = plan.resolve_backend({"TS_STORAGE_BACKEND": written})
                self.assertEqual("raft", got.get("backend"))
                self.assertTrue(got.get("honoured"), "the plan did not honour %r" % written)

    def test_an_unknown_request_falls_through_on_both_sides(self) -> None:
        """The positive control, and the half that is easy to lose: an unrecognised value must
        reach auto-detection rather than being treated as a forced backend."""
        self.assertRegex(_rust_source(), r"_\s*=>\s*Self::Auto",
                         "BackendOverride::parse no longer falls back to Auto")
        # `honoured` answers "did you get the backend you asked for", so an absent request is
        # trivially honoured -- it is not the signal for this. The signal is that an
        # unrecognised value lands where NO request lands: on auto-detection.
        auto = plan.resolve_backend({}).get("backend")
        for written in ("wat", "auto", ""):
            with self.subTest(written=written):
                self.assertEqual(
                    auto, plan.resolve_backend({"TS_STORAGE_BACKEND": written}).get("backend"),
                    "the plan treated %r as a forced backend; the engine sends it to Auto"
                    % written)


if __name__ == "__main__":
    unittest.main()
