#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Freed arenas go back to the OS after a large payload, and only then.

The proxy daemon stores nothing yet sat at 2.9 GB RSS while the engine it fronts held 1.1 GB.
Every request and response is encoded and decoded whole in this process, and CPython keeps the
arenas after freeing them -- measured here at 124 MB left resident from a 105 MB document, falling
to 12 MB after malloc_trim(0).

Trimming walks the arenas, so doing it per request would spend CPU on the hot path to reclaim
nothing. These tests pin the bound rather than the byte count: they assert WHEN the trim runs,
which is deterministic, instead of asserting an RSS number, which is not.
"""
import os
import unittest

import matrixark_rust_proxy_daemon as daemon


class _FakeLibc:
    def __init__(self):
        self.calls = 0

    def malloc_trim(self, _pad):
        self.calls += 1
        return 1


class ArenaTrimTest(unittest.TestCase):
    def setUp(self):
        self._real_libc = daemon._LIBC_FOR_TRIM
        self._real_looked_up = daemon._LIBC_LOOKED_UP
        self._real_threshold = daemon._ARENA_TRIM_THRESHOLD_BYTES
        self.fake = _FakeLibc()
        daemon._LIBC_FOR_TRIM = self.fake
        daemon._LIBC_LOOKED_UP = True
        daemon._ARENA_TRIM_THRESHOLD_BYTES = 1024

    def tearDown(self):
        daemon._LIBC_FOR_TRIM = self._real_libc
        daemon._LIBC_LOOKED_UP = self._real_looked_up
        daemon._ARENA_TRIM_THRESHOLD_BYTES = self._real_threshold

    def test_a_large_payload_returns_its_arenas(self):
        self.assertTrue(daemon.release_arenas_after_large_payload(4096))
        self.assertEqual(self.fake.calls, 1)

    def test_a_small_payload_does_not_pay_for_a_trim(self):
        """The control. Trimming per request would put arena walking on the hot path."""
        self.assertFalse(daemon.release_arenas_after_large_payload(64))
        self.assertEqual(self.fake.calls, 0, "a small payload must not trigger a trim")

    def test_the_boundary_is_inclusive_upward(self):
        self.assertFalse(daemon.release_arenas_after_large_payload(1023))
        self.assertTrue(daemon.release_arenas_after_large_payload(1024))

    def test_a_zero_threshold_disables_trimming_entirely(self):
        """An operator must be able to turn it off without patching the daemon."""
        daemon._ARENA_TRIM_THRESHOLD_BYTES = 0
        self.assertFalse(daemon.release_arenas_after_large_payload(10 * 1024 * 1024))
        self.assertEqual(self.fake.calls, 0)

    def test_no_glibc_is_survivable(self):
        """Housekeeping must never break a served request on a platform without malloc_trim."""
        daemon._LIBC_FOR_TRIM = None
        self.assertFalse(daemon.release_arenas_after_large_payload(10 * 1024 * 1024))

    def test_a_raising_libc_is_survivable(self):
        class _Angry:
            def malloc_trim(self, _pad):
                raise OSError("no")

        daemon._LIBC_FOR_TRIM = _Angry()
        self.assertFalse(daemon.release_arenas_after_large_payload(10 * 1024 * 1024))


if __name__ == "__main__":
    unittest.main()
