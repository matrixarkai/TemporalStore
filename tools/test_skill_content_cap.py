#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Skill-content size cap. Skills SHARE the resource inline/text limit (5 MiB) as a
single source of truth, so a skill and a resource of the same size gate IDENTICALLY.
The cap stays configurable via env overrides and the per-request ``max_text_chars``
arg. Tests hit the smallest unit that owns the cap directly — no gateway needed."""
import os
import unittest

import matrixark_skill_parser as skill_parser
from matrixark_skill_parser import (
    DEFAULT_MAX_SKILL_BYTES,
    parse_skill,
    resolve_max_skill_bytes,
)
from matrixark_resource_parser import (
    DEFAULT_MAX_INLINE_TEXT_CHARS,
    ResourceParserError,
    parse_resource,
)


_TWO_MIB = 2 * 1024 * 1024
_FIVE_MIB = 5 * 1024 * 1024

# Env vars that influence the effective (default) cap; cleared so tests are deterministic.
_CAP_ENV = ("MATRIXARK_MAX_SKILL_BYTES", "MATRIXARK_SKILL_MAX_TEXT_CHARS",
            "MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS")


class _EnvGuard:
    """Context manager that sets/clears env vars and restores them on exit."""

    def __init__(self, **values):
        self._values = values
        self._saved: dict[str, str | None] = {}

    def __enter__(self):
        for key, value in self._values.items():
            self._saved[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return self

    def __exit__(self, *exc):
        for key, prev in self._saved.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev


def _defaults_cleared() -> _EnvGuard:
    return _EnvGuard(**{k: None for k in _CAP_ENV})


class SkillCapTest(unittest.TestCase):
    def test_default_cap_is_the_shared_resource_limit(self):
        # Skills now share the resource inline/text cap (5 MiB) as a single source of truth.
        self.assertEqual(_FIVE_MIB, DEFAULT_MAX_INLINE_TEXT_CHARS)
        self.assertEqual(DEFAULT_MAX_INLINE_TEXT_CHARS, DEFAULT_MAX_SKILL_BYTES)
        with _defaults_cleared():
            self.assertEqual(_FIVE_MIB, resolve_max_skill_bytes())

    def test_oversized_skill_rejected_with_clear_error(self):
        big = "x" * (_FIVE_MIB + 10)
        with _defaults_cleared():
            with self.assertRaises(ValueError) as ctx:
                parse_skill("mem://big-skill", text=big)
        msg = str(ctx.exception)
        self.assertIn("too large", msg)
        self.assertIn(str(_FIVE_MIB), msg)   # effective (shared) limit is in the message

    def test_env_override_sets_the_gate(self):
        # A skill-specific env override changes the effective cap (here LOWERED to 1 MiB);
        # a body over the override is rejected with the effective limit in the message.
        one_mib = 1024 * 1024
        body = "x" * (one_mib + 10)     # size-gate rejects before chunking
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=str(one_mib),
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None,
                       MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS=None):
            self.assertEqual(one_mib, resolve_max_skill_bytes())
            with self.assertRaises(ValueError) as ctx:
                parse_skill("mem://big-skill", text=body)
            self.assertIn(str(one_mib), str(ctx.exception))

    def test_legacy_env_alias_still_works(self):
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=str(_TWO_MIB * 3),
                       MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS=None):
            self.assertEqual(_TWO_MIB * 3, resolve_max_skill_bytes())

    def test_preferred_env_wins_over_legacy(self):
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES="123456",
                       MATRIXARK_SKILL_MAX_TEXT_CHARS="999",
                       MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS=None):
            self.assertEqual(123456, resolve_max_skill_bytes())

    def test_shared_resource_env_knob_moves_the_skill_default(self):
        # With no skill-specific override, the shared resource knob sets the skill cap too.
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None,
                       MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS=str(_TWO_MIB)):
            self.assertEqual(_TWO_MIB, resolve_max_skill_bytes())

    def test_arg_override_beats_env(self):
        # env would REJECT (500 KiB cap), but the explicit arg raises the gate -> accepted.
        body = "x" * 800_000
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES="500000",
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None,
                       MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS=None):
            with self.assertRaises(ValueError):
                parse_skill("mem://big-skill", text=body)             # env cap rejects
            parsed = parse_skill("mem://big-skill", text=body, max_text_chars=2_000_000)
            self.assertTrue(parsed.text)                              # arg override wins

    def test_non_positive_override_raises(self):
        with self.assertRaises(ValueError):
            resolve_max_skill_bytes(0)
        with self.assertRaises(ValueError):
            parse_skill("mem://s", text="hi", max_text_chars=-1)

    def test_skill_between_two_and_five_mib_now_passes(self):
        # Regression proof: content > 2 MiB (the OLD skill cap) but within the shared 5 MiB
        # cap used to be REJECTED by the size gate and must now PARSE fine as a skill.
        mid = "y" * int(2.3 * 1024 * 1024)   # 2.3 MiB: > 2 MiB old cap, < 5 MiB shared cap
        self.assertGreater(len(mid), _TWO_MIB)
        self.assertLess(len(mid), _FIVE_MIB)
        with _defaults_cleared():
            parsed = parse_skill("mem://mid-skill", text=mid)   # no raise (was ValueError before)
            self.assertTrue(parsed.text)


class SkillAndResourceGateIdenticallyTest(unittest.TestCase):
    """A skill and a resource of the SAME size are accepted/rejected identically —
    they share ONE inline/text cap. Uses a small controlled cap so the SIZE gate (not
    the orthogonal, pre-existing max_total_chunks limit) is the deciding factor."""

    # Small shared cap so bodies stay well under the chunk ceiling; the SIZE gate decides.
    _CAP = 200_000

    def _guard(self) -> _EnvGuard:
        return _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                         MATRIXARK_SKILL_MAX_TEXT_CHARS=None,
                         MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS=str(self._CAP))

    def _skill_ok(self, text: str) -> bool:
        try:
            parse_skill("mem://s.md", text=text)
            return True
        except (ValueError, ResourceParserError):
            return False

    def _resource_ok(self, text: str) -> bool:
        try:
            parse_resource("mem://r.md", resource_type="md", text=text)
            return True
        except (ValueError, ResourceParserError):
            return False

    def test_same_size_under_limit_both_accepted(self):
        text = "z" * (self._CAP - 5_000)
        with self._guard():
            self.assertTrue(self._skill_ok(text))
            self.assertTrue(self._resource_ok(text))

    def test_same_size_over_limit_both_rejected(self):
        text = "z" * (self._CAP + 5_000)
        with self._guard():
            self.assertFalse(self._skill_ok(text))
            self.assertFalse(self._resource_ok(text))

    def test_identical_verdict_across_sizes(self):
        with self._guard():
            for size in (1_000, self._CAP - 1, self._CAP, self._CAP + 1):
                text = "q" * size
                self.assertEqual(self._skill_ok(text), self._resource_ok(text),
                                 f"skill/resource verdict diverged at size={size}")

    def test_shared_knob_moves_both_gates_together(self):
        # Lowering the shared resource knob moves BOTH the skill and resource gate together.
        with self._guard():
            over = "w" * (self._CAP + 10)
            self.assertFalse(self._skill_ok(over))
            self.assertFalse(self._resource_ok(over))
            under = "w" * (self._CAP - 10)
            self.assertTrue(self._skill_ok(under))
            self.assertTrue(self._resource_ok(under))


if __name__ == "__main__":
    unittest.main(verbosity=2)
