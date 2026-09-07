# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A retrieve that RAISED is not an answer, and must not suppress the previous-pack fallback.

Observed live under load: a turn took 344.8 s and served the agent 0 bytes, with

    warnings    = ["retrieval_deadline_exceeded:request_deadline_exception", "request_deadline_exception"]
    pack_source = None

`request_deadline_exception` is the raise path — `adapter.retrieve` threw and the dispatcher
substituted an empty deadline fallback pack, which is right, because there is no pack to serve
there. What was wrong is that the hook's last-good-pack fallback did not fire: the fallback pack
carries a `context_pack_id` like any other, so `store_answered()` read a failure as an answer. A
7,886-byte pack was in the cache at that moment, 18.7 minutes old against a 1,440-minute ceiling.

The boundary is the whole point of these tests. `request_deadline_after_retrieve` means the retrieve
FINISHED and was merely late; mx#1214 stopped those packs being discarded, so they carry real refs
and must keep suppressing the stale fallback. Serving stale context over fresh context would be
worse than the bug being fixed.
"""
from __future__ import annotations

import unittest

try:  # package path
    from tools import matrixark_hook_pack_cache as pack_cache
except ImportError:  # run from tools/
    import matrixark_hook_pack_cache as pack_cache  # type: ignore


class StoreAnsweredTest(unittest.TestCase):
    def test_a_raised_retrieve_did_not_answer(self) -> None:
        pack = {
            "context_pack_id": "deadline-fallback",
            "warnings": ["retrieval_deadline_exceeded:request_deadline_exception",
                         "request_deadline_exception"],
        }
        self.assertFalse(
            pack_cache.store_answered(pack),
            "a raised retrieve read as an answer, suppressing the previous-pack fallback",
        )

    def test_the_full_pack_spelling_is_recognised_too(self) -> None:
        # The compact serving pack says `warnings`; the uncompacted one says `quality_warnings`.
        pack = {"context_pack_id": "x", "quality_warnings": ["request_deadline_exception"]}
        self.assertFalse(pack_cache.store_answered(pack))

    def test_a_late_but_complete_pack_IS_an_answer(self) -> None:
        # The boundary. This pack finished and carries refs; serving a stale pack instead would be
        # a regression, not a fix.
        pack = {
            "context_pack_id": "rust-native-1-2",
            "warnings": ["request_deadline_after_retrieve"],
            "selected_refs": [{"text": "a real ref"}],
        }
        self.assertTrue(
            pack_cache.store_answered(pack),
            "a late but complete pack was treated as a failure",
        )

    def test_an_ordinary_pack_is_still_an_answer(self) -> None:
        self.assertTrue(pack_cache.store_answered({"context_pack_id": "plain"}))

    def test_a_deliberately_empty_answer_is_still_an_answer(self) -> None:
        # A turn whose pack renders to nothing on purpose must not get stale context injected.
        self.assertTrue(pack_cache.store_answered({"context_pack_id": "empty", "warnings": []}))

    def test_a_tool_timeout_is_still_not_an_answer(self) -> None:
        self.assertFalse(pack_cache.store_answered({"context_pack_id": "x", "_hook_tool_timeout": True}))

    def test_a_non_dict_is_not_an_answer(self) -> None:
        self.assertFalse(pack_cache.store_answered(None))


if __name__ == "__main__":
    unittest.main()
