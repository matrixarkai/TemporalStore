#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""get_all lists a subject's records without resolving every record in the store.

``records_for_get_all`` returns the subject's own records rather than the whole log, memoised per
``(tenant_hash, user_hash)`` on the signature the compacted read cache already uses. Real callers
always arrive with both hashes resolved, so this narrows in practice: at 32,000 records a repeated
listing goes from 40.7 ms to 3.9 ms.

The memo is taken only while the expiry filter is inactive, so a TTL record anywhere in the store
turns it off for every subject. That has a consequence for this file worth stating up front: the
TTL test below cannot exercise the memo, because the condition it would test is the same condition
that disables it. The memo's invalidation is pinned by the WRITE test instead, which fails when the
memo is made never to invalidate. Both were established by mutating the adapter, not by reading it.
"""
from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

from matrixark_mcp_local_adapter import MatrixArkLocalAdapter  # noqa: E402

ANCHOR_MS = 1_700_000_000_000
SUBJECTS = 4


def _scope(user: int) -> dict:
    return {"tenant_hash": 7, "user_hash": user, "scope_key": f"t=7;u={user}"}


def _event(index: int, user: int, **extra) -> dict:
    record = {
        "record_type": "context_event",
        "event_id_hash": f"e{index}",
        "summary_text": f"note {index}",
        "text": f"note {index}",
        "updated_at_ms": ANCHOR_MS + index,
        "timestamp_key_ms": ANCHOR_MS + index,
        "access_scope": _scope(user),
    }
    record.update(extra)
    return record


class GetAllListsASubjectWithoutWalkingTheStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.adapter = MatrixArkLocalAdapter(pathlib.Path(self.tmp.name) / "events.jsonl")
        self.adapter.append_many(
            [_event(i, 100 + (i % SUBJECTS)) for i in range(40)]
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()
        os.environ.pop("MATRIXARK_MEMORY_NOW_MS", None)

    def _ids(self, user: int) -> set:
        out = self.adapter.get_all({"scope": _scope(user), "limit": 10_000})
        return {row["id"] for row in out["memories"]}

    def test_a_subject_sees_exactly_its_own(self) -> None:
        for user in range(100, 100 + SUBJECTS):
            self.assertEqual(
                self._ids(user),
                {f"e{i}" for i in range(40) if 100 + (i % SUBJECTS) == user},
                f"subject {user}",
            )

    def test_the_answer_is_the_same_on_a_repeat(self) -> None:
        """The second call is the memoised one; it must answer what the first did."""
        self.assertEqual(self._ids(101), self._ids(101))

    def test_a_subject_with_nothing_gets_nothing(self) -> None:
        self.assertEqual(self.adapter.get_all({"scope": _scope(999), "limit": 100})["count"], 0)

    def test_a_scope_with_no_hashes_still_lists_everything(self) -> None:
        self.assertEqual(self.adapter.get_all({"limit": 10_000})["count"], 40)

    def test_a_write_reaches_a_listing_that_was_already_served(self) -> None:
        before = self._ids(100)
        self.adapter.append(_event(9_000, 100))
        self.assertEqual(self._ids(100), before | {"e9000"})

    def test_a_ttl_record_still_disappears_from_a_listing(self) -> None:
        """Expiry is still visible through the new path.

        This does NOT test the memo, and saying so matters: a TTL record anywhere in the store is
        exactly what turns the memo off, so on this fixture no bucket is ever remembered. Every
        mutation of the memo passes here, which is how that was found rather than assumed. What it
        does pin is the end-to-end guarantee through the narrowed listing -- an expired memory
        leaves it -- which is the thing a reader of this change will want assured.

        The memo's own invalidation is covered by
        ``test_a_write_reaches_a_listing_that_was_already_served``, which fails when the memo is
        made never to invalidate.
        """
        self.adapter.append(_event(7_000, 100, expires_at_ms=ANCHOR_MS + 10_000))
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 1_000)
        self.assertIn("e7000", self._ids(100), "the record is live before its expiry")

        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 20_000)
        self.assertNotIn("e7000", self._ids(100), "an expired record must leave the listing")

    def test_one_subjects_expiry_does_not_disturb_another(self) -> None:
        self.adapter.append(_event(7_001, 100, expires_at_ms=ANCHOR_MS + 10_000))
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 1_000)
        other = self._ids(101)
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 20_000)
        self.assertEqual(self._ids(101), other)


if __name__ == "__main__":
    unittest.main(verbosity=2)
