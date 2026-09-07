#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""get_all resolves a record's scope only when it has one to compare it against.

`_resolve_subject_hashes` returns ``(0, 0)`` for any request scope that was not identity-enriched
upstream, and the resolved pair is read by exactly two comparisons, both guarded on those hashes.
So on such a call the resolution ran once per record in the store and was dropped -- 16 ms per
32,000 records when the records carry their hashes, 57 ms when they do not, on a listing returning
ten rows.

Skipping work is only correct if the work was not needed, so the filtering half is asserted here
too, in the same file: an enriched scope must still see ONLY its own subject's memories. A test
that asserted just the fast path would pass just as well on a get_all that had stopped filtering.
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


def _event(index: int, tenant: int, user: int) -> dict:
    return {
        "record_type": "context_event",
        "event_id_hash": f"e{index}",
        "summary_text": f"note {index}",
        "text": f"note {index}",
        "updated_at_ms": 1_700_000_000_000 + index,
        "timestamp_key_ms": 1_700_000_000_000 + index,
        "access_scope": {"tenant_hash": tenant, "user_hash": user,
                         "scope_key": f"t={tenant};u={user}"},
    }


class GetAllResolvesAScopeOnlyWhenItComparesOneTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.adapter = MatrixArkLocalAdapter(pathlib.Path(self.tmp.name) / "events.jsonl")
        # Two subjects under one tenant, interleaved, so an off-by-one in the filter shows up as
        # the wrong subject rather than as a short list.
        self.adapter.append_many(
            [_event(i, 7, 11 if i % 2 == 0 else 22) for i in range(20)]
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_an_enriched_scope_sees_only_its_own_subject(self) -> None:
        """The positive control. This is the branch the guard skips, and it must still filter."""
        out = self.adapter.get_all({"scope": {"tenant_hash": 7, "user_hash": 11}, "limit": 100})
        got = {row["id"] for row in out["memories"]}
        self.assertEqual(got, {f"e{i}" for i in range(0, 20, 2)})
        self.assertEqual(out["count"], 10)

    def test_the_other_subject_sees_the_other_half(self) -> None:
        out = self.adapter.get_all({"scope": {"tenant_hash": 7, "user_hash": 22}, "limit": 100})
        self.assertEqual({row["id"] for row in out["memories"]},
                         {f"e{i}" for i in range(1, 20, 2)})

    def test_a_tenant_hash_alone_still_narrows(self) -> None:
        """Only one of the two hashes set -- the guard must run the branch for either, not both."""
        self.assertEqual(self.adapter.get_all({"scope": {"tenant_hash": 7}, "limit": 100})["count"], 20)
        self.assertEqual(self.adapter.get_all({"scope": {"tenant_hash": 999}, "limit": 100})["count"], 0)

    def test_a_user_hash_alone_still_narrows(self) -> None:
        self.assertEqual(self.adapter.get_all({"scope": {"user_hash": 11}, "limit": 100})["count"], 10)
        self.assertEqual(self.adapter.get_all({"scope": {"user_hash": 999}, "limit": 100})["count"], 0)

    def test_an_unenriched_scope_lists_everything(self) -> None:
        """The branch the guard skips entirely: neither hash, so nothing to compare, so no
        resolution. The answer is the same one the unguarded code gave."""
        self.assertEqual(self.adapter.get_all({"limit": 100})["count"], 20)

    def test_the_newest_are_the_ones_a_limit_keeps(self) -> None:
        """Guarding the filter must not disturb which records a limit selects."""
        out = self.adapter.get_all({"scope": {"tenant_hash": 7, "user_hash": 11}, "limit": 3})
        self.assertEqual([row["id"] for row in out["memories"]], ["e14", "e16", "e18"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
