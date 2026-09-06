#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A turn that cannot build a pack serves the last one that loaded, and says so.

Measured on a live one-box under its normal load: agent turns returned NO context at all -- the
retrieve completed, reported `deadline_exceeded: false`, and selected zero refs. The hook then
emitted `{}`, which tells the agent it has no history. That is both wrong and silent, and it is a
worse answer than context that is a turn or two old.

Both hooks share this module rather than keeping a copy each, because Claude and Codex run
entirely separate entry points: a fix applied to one leaves the other silently broken (measured:
Claude 0 empty, Codex 3 of 4 empty, same store and same minute), and two copies of a cache-key
scheme are free to drift until one agent serves the other's pack. These tests cover the cache
directly -- the failure they guard against is not "the store was slow" but "the hook had something
true and served nothing".
"""
import os
import tempfile
import time
import unittest
from pathlib import Path

import matrixark_hook_pack_cache as pack_cache


class LastGoodPackTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._prev = os.environ.get("MATRIXARK_HOOK_PACK_CACHE_DIR")
        os.environ["MATRIXARK_HOOK_PACK_CACHE_DIR"] = self._dir.name

    def tearDown(self):
        if self._prev is None:
            os.environ.pop("MATRIXARK_HOOK_PACK_CACHE_DIR", None)
        else:
            os.environ["MATRIXARK_HOOK_PACK_CACHE_DIR"] = self._prev
        self._dir.cleanup()

    def test_a_remembered_pack_comes_back(self):
        path = pack_cache.context_pack_cache_path("claude", "/work/project")
        pack_cache.remember_context_pack(path, "the pack that loaded")
        text, age = pack_cache.recover_context_pack(path, max_age_s=3600)
        self.assertEqual(text, "the pack that loaded")
        self.assertLess(age, 60, "a pack just written should read back as fresh")

    def test_nothing_is_served_when_nothing_was_ever_stored(self):
        """The fail-open contract: with no cache, the hook must emit nothing rather than invent
        context."""
        path = pack_cache.context_pack_cache_path("claude", "/never/seen")
        self.assertEqual(pack_cache.recover_context_pack(path, max_age_s=3600), ("", 0.0))

    def test_a_stale_pack_is_refused(self):
        """Past the age bound, nothing is better than stale."""
        path = pack_cache.context_pack_cache_path("claude", "/work/project")
        pack_cache.remember_context_pack(path, "yesterday's pack")
        old = time.time() - 7200
        os.utime(path, (old, old))
        self.assertEqual(
            pack_cache.recover_context_pack(path, max_age_s=3600)[0],
            "",
            "a pack older than the bound must not be served",
        )
        self.assertTrue(
            pack_cache.recover_context_pack(path, max_age_s=10800)[0],
            "the same pack inside a wider bound must still be served",
        )

    def test_one_workspace_cannot_serve_another_its_context(self):
        """A stale pack is a small error; the WRONG PROJECT's pack is a large one."""
        a = pack_cache.context_pack_cache_path("claude", "/work/alpha")
        b = pack_cache.context_pack_cache_path("claude", "/work/beta")
        self.assertNotEqual(a, b, "different workspaces must not share a cache file")
        pack_cache.remember_context_pack(a, "alpha context")
        self.assertEqual(pack_cache.recover_context_pack(b, max_age_s=3600)[0], "")

    def test_one_agent_cannot_serve_another_its_context(self):
        """The two hooks share this module; they must not share a FILE."""
        claude = pack_cache.context_pack_cache_path("claude", "/work/project")
        codex = pack_cache.context_pack_cache_path("codex", "/work/project")
        self.assertNotEqual(claude, codex, "agents must not share a cache file")
        pack_cache.remember_context_pack(claude, "claude context")
        self.assertEqual(pack_cache.recover_context_pack(codex, max_age_s=3600)[0], "")

    def test_a_torn_write_cannot_be_served(self):
        """Written via rename, so a turn that dies mid-write leaves the previous pack intact
        rather than a truncated one."""
        path = pack_cache.context_pack_cache_path("claude", "/work/project")
        pack_cache.remember_context_pack(path, "first pack")
        pack_cache.remember_context_pack(path, "second pack, longer than the first")
        self.assertEqual(
            pack_cache.recover_context_pack(path, max_age_s=3600)[0],
            "second pack, longer than the first",
        )
        self.assertEqual(
            list(Path(self._dir.name).glob("*.tmp")),
            [],
            "no temporary file should survive a completed write",
        )

    def test_the_age_bound_is_configurable_and_survives_nonsense(self):
        prev = os.environ.get("MATRIXARK_HOOK_PACK_CACHE_MAX_AGE_S")
        try:
            os.environ["MATRIXARK_HOOK_PACK_CACHE_MAX_AGE_S"] = "120"
            self.assertEqual(pack_cache.pack_cache_max_age_s(), 120.0)
            os.environ["MATRIXARK_HOOK_PACK_CACHE_MAX_AGE_S"] = "not-a-number"
            self.assertEqual(
                pack_cache.pack_cache_max_age_s(),
                pack_cache.DEFAULT_MAX_AGE_S,
                "an unparseable bound must fall back, not crash the hook",
            )
        finally:
            if prev is None:
                os.environ.pop("MATRIXARK_HOOK_PACK_CACHE_MAX_AGE_S", None)
            else:
                os.environ["MATRIXARK_HOOK_PACK_CACHE_MAX_AGE_S"] = prev

    def test_a_served_pack_is_labelled_as_prior_context(self):
        """Unlabelled, the model cannot tell a stale pack from a fresh one -- and silently passing
        off old context as current is the failure this path exists to avoid."""
        labelled = pack_cache.label_previous_pack("BODY", 372.0)
        self.assertIn("prior context", labelled)
        self.assertIn("6 min ago", labelled)
        self.assertTrue(labelled.endswith("BODY"), "the pack itself must survive the label")


if __name__ == "__main__":
    unittest.main()
