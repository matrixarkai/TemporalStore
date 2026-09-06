#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A failure notice is never remembered as a context pack.

When retrieval errors, the Codex hook puts a plain-language notice in the same variable a pack
would occupy:

    "MatrixArk/TemporalStore retrieval was attempted for this prompt but failed. Use visible local
     Codex context as authoritative for this turn. Failure: ..."

The fallback remembered whatever was non-empty, so it stored those. Found on a live box as a cache
file holding that sentence and nothing else. A later turn that could not reach the store would then
serve "retrieval failed" back as PRIOR CONTEXT -- worse than serving nothing, because it reaches the
agent labelled as remembered history.

These tests cover the cache module's own guard, so both hooks get it and neither can drift.
"""
import os
import tempfile
import unittest

import matrixark_hook_pack_cache as pack_cache


class FailureNoticeIsNotAPackTest(unittest.TestCase):
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

    def test_a_failure_notice_is_recognised(self):
        notice = (
            "MatrixArk/TemporalStore retrieval was attempted for this prompt but failed. Use "
            "visible local Codex context as authoritative for this turn. Failure: "
            "matrixark_retrieve timed out at the Codex hook boundary after 300000ms"
        )
        self.assertTrue(
            pack_cache.looks_like_failure_notice(notice),
            "the notice the hook emits on a retrieval error must be recognised as such",
        )

    def test_a_real_pack_is_not_mistaken_for_one(self):
        """The control: over-eager matching would throw away real context."""
        pack = (
            "Relevant context from earlier turns (MatrixArk memory):\\n"
            "- user: why did the retrieval fail last night\\n"
            "- assistant: the store did not answer within its deadline"
        )
        self.assertFalse(
            pack_cache.looks_like_failure_notice(pack),
            "a pack that merely mentions failure is still a pack",
        )

    def test_remembering_a_failure_notice_is_refused(self):
        path = pack_cache.context_pack_cache_path("codex", "/work/project")
        pack_cache.remember_context_pack(path, "a real pack worth keeping")
        pack_cache.remember_context_pack(
            path,
            "MatrixArk/TemporalStore retrieval was attempted for this prompt but failed. "
            "Use visible local Codex context as authoritative for this turn.",
        )
        self.assertEqual(
            pack_cache.recover_context_pack(path, max_age_s=3600)[0],
            "a real pack worth keeping",
            "a failure notice must not overwrite the last pack that actually loaded",
        )

    def test_nothing_is_stored_when_the_first_write_is_a_notice(self):
        path = pack_cache.context_pack_cache_path("codex", "/work/fresh")
        pack_cache.remember_context_pack(
            path, "MatrixArk/TemporalStore retrieval was attempted for this prompt but failed."
        )
        self.assertEqual(
            pack_cache.recover_context_pack(path, max_age_s=3600)[0],
            "",
            "with only a notice to store, the cache must stay empty rather than serve it later",
        )


if __name__ == "__main__":
    unittest.main()
