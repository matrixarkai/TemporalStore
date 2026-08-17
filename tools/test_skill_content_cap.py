#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Skill-content size cap: default 2 MiB, configurable via env override and the
per-request ``max_text_chars`` arg. Also proves resources are NOT subject to the
skill cap (they carry their own, higher limit). Tests hit the smallest unit that
owns the cap directly — no gateway needed."""
import os
import unittest

import matrixark_skill_parser as skill_parser
from matrixark_skill_parser import (
    DEFAULT_MAX_SKILL_BYTES,
    parse_skill,
    resolve_max_skill_bytes,
)
from matrixark_resource_parser import parse_resource


_TWO_MIB = 2 * 1024 * 1024


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


class SkillCapTest(unittest.TestCase):
    def test_default_cap_is_two_mib(self):
        self.assertEqual(_TWO_MIB, DEFAULT_MAX_SKILL_BYTES)
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None):
            self.assertEqual(_TWO_MIB, resolve_max_skill_bytes())

    def test_oversized_skill_rejected_with_clear_error(self):
        big = "x" * (_TWO_MIB + 10)
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None):
            with self.assertRaises(ValueError) as ctx:
                parse_skill("mem://big-skill", text=big)
        msg = str(ctx.exception)
        self.assertIn("too large", msg)
        self.assertIn(str(_TWO_MIB), msg)   # effective limit is in the message

    def test_env_override_raises_the_gate(self):
        big = "x" * (_TWO_MIB + 10)
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=str(_TWO_MIB * 4),
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None):
            self.assertEqual(_TWO_MIB * 4, resolve_max_skill_bytes())
            parsed = parse_skill("mem://big-skill", text=big)   # no raise
            self.assertTrue(parsed.text)

    def test_legacy_env_alias_still_works(self):
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=str(_TWO_MIB * 3)):
            self.assertEqual(_TWO_MIB * 3, resolve_max_skill_bytes())

    def test_preferred_env_wins_over_legacy(self):
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES="123456",
                       MATRIXARK_SKILL_MAX_TEXT_CHARS="999"):
            self.assertEqual(123456, resolve_max_skill_bytes())

    def test_arg_override_beats_env(self):
        big = "x" * (_TWO_MIB + 10)
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None):
            parsed = parse_skill("mem://big-skill", text=big, max_text_chars=_TWO_MIB * 8)
            self.assertTrue(parsed.text)

    def test_non_positive_override_raises(self):
        with self.assertRaises(ValueError):
            resolve_max_skill_bytes(0)
        with self.assertRaises(ValueError):
            parse_skill("mem://s", text="hi", max_text_chars=-1)


class ResourceNotSubjectToSkillCapTest(unittest.TestCase):
    def test_large_resource_not_rejected_by_skill_cap(self):
        # A body well over the 2 MiB skill cap but within the resource inline limit
        # (5 MiB default) must parse fine as a resource.
        big = "word " * ((_TWO_MIB) // 5 + 1000)   # ~2 MiB+ of text
        self.assertGreater(len(big), _TWO_MIB)
        with _EnvGuard(MATRIXARK_MAX_SKILL_BYTES=None,
                       MATRIXARK_SKILL_MAX_TEXT_CHARS=None):
            chunks = parse_resource("mem://big-resource.md", resource_type="md", text=big)
        self.assertTrue(chunks)   # produced chunks, not rejected by any 2 MiB cap


if __name__ == "__main__":
    unittest.main(verbosity=2)
