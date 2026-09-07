#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The expiry guard re-asks only about records it has not already seen.

The guard answers "does anything here carry a TTL, or is there a cutoff marker" -- a monotone OR
over a per-record property. It was memoised on the compacted cache's signature, which every write
moves, so every write sent it back over the whole store: at 32,000 records the read after one append
cost 14.34 ms against 0.333 ms warm, and 97% of that was this walk.

It now keeps the list object it walked. ``_read_cache_records`` is extended in place by appends and
REPLACED by anything that removes, so the same object proves the walked prefix is intact and only
the tail is new.

Two ways that can go wrong, and both are tested here rather than argued:

* a TTL record appended AFTER a clean answer must still be noticed -- otherwise the guard keeps
  saying "nothing expires here" and an expired memory never leaves any listing;
* a removal must force a full re-ask, since the prefix is no longer what was walked.
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


def _event(index: int, **extra) -> dict:
    record = {"record_type": "context_event", "event_id_hash": f"e{index}",
              "summary_text": f"note {index}", "text": f"note {index}",
              "updated_at_ms": ANCHOR_MS + index, "timestamp_key_ms": ANCHOR_MS + index}
    record.update(extra)
    return record


class TheExpiryGuardReAsksOnlyAboutWhatIsNewTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.adapter = MatrixArkLocalAdapter(pathlib.Path(self.tmp.name) / "events.jsonl")
        self.adapter.append_many([_event(i) for i in range(50)])
        self.adapter.read_all()          # the clean answer the fast path will try to reuse

    def tearDown(self) -> None:
        self.tmp.cleanup()
        os.environ.pop("MATRIXARK_MEMORY_NOW_MS", None)

    def _ids(self) -> set:
        return {str(r.get("event_id_hash")) for r in self.adapter.read_all()
                if r.get("event_id_hash") not in (None, "")}

    def test_a_ttl_appended_after_a_clean_answer_is_still_noticed(self) -> None:
        """The one the fast path can get wrong: the guard has already answered "nothing expires"
        for everything before this record, and this record is the whole reason to say otherwise."""
        self.adapter.append(_event(900, expires_at_ms=ANCHOR_MS + 10_000))
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 1_000)
        self.assertIn("e900", self._ids(), "live before its expiry")
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 20_000)
        self.assertNotIn("e900", self._ids(), "an expired record must leave the live view")

    def test_an_ephemeral_appended_later_is_noticed_too(self) -> None:
        """`ephemeral` is the guard's other trigger, and a tail check that only looked at
        expires_at_ms would pass the test above and fail here."""
        self.adapter.read_all()
        self.adapter.append(_event(901, ephemeral=True, expires_at_ms=ANCHOR_MS + 5_000))
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 60_000)
        self.assertNotIn("e901", self._ids())

    def test_many_appends_between_the_answer_and_the_ttl(self) -> None:
        """The prefix grows several times before the TTL arrives, so the memo is advanced on each
        read; the record must still be seen when it finally lands."""
        for batch in range(4):
            self.adapter.append_many([_event(1_000 + batch * 10 + k) for k in range(10)])
            self.adapter.read_all()
        self.adapter.append(_event(902, expires_at_ms=ANCHOR_MS + 10_000))
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 20_000)
        self.assertNotIn("e902", self._ids())

    def test_a_removal_forces_the_question_again(self) -> None:
        """A delete replaces the cache list, so the walked prefix is no longer what is there."""
        self.adapter.append(_event(903, expires_at_ms=ANCHOR_MS + 10_000))
        self.adapter.read_all()
        self.adapter.delete_memory({"memory_id": "e3"})
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 20_000)
        ids = self._ids()
        self.assertNotIn("e903", ids, "the TTL record must still expire after an unrelated delete")
        self.assertNotIn("e3", ids, "and the deleted record must be gone")

    def test_a_removal_that_shifts_an_unwalked_record_into_the_prefix(self) -> None:
        """The case a prefix check gets wrong if it does not verify the list is the same one.

        Append a TTL record and do NOT read: the guard has walked 50 records and this one sits at
        index 50, unwalked. Now delete something. The list is rebuilt one shorter, and the TTL record
        lands at index 49 -- INSIDE the walked prefix. A guard that trusted "the first 50 are already
        answered" would never look at it, keep saying nothing expires here, and the record would
        outlive its own expiry with nothing to make it leave.

        This is why the memo holds the list OBJECT and not just a length: a delete hands back a
        different list, so the prefix claim is dropped rather than re-used.
        """
        self.adapter.append(_event(906, expires_at_ms=ANCHOR_MS + 10_000))
        self.adapter.delete_memory({"memory_id": "e7"})     # no read between the two
        os.environ["MATRIXARK_MEMORY_NOW_MS"] = str(ANCHOR_MS + 20_000)
        ids = self._ids()
        self.assertNotIn("e906", ids, "the TTL record was shifted into the walked prefix and missed")
        self.assertNotIn("e7", ids)

    def test_the_answer_is_the_same_as_asking_from_scratch(self) -> None:
        """Differential: whatever the guard says, a full walk of the same records must agree."""
        import matrixark_mcp_local_adapter as module
        for extra in (None, _event(904), _event(905, expires_at_ms=ANCHOR_MS + 99_000)):
            if extra is not None:
                self.adapter.append(extra)
            records = self.adapter._read_all_compacted()
            self.assertEqual(
                self.adapter._records_need_expiry_filter_incrementally(records),
                module._memory_records_need_expiry_filter(records),
                "the incremental answer differs from a full walk")

    def test_random_operation_sequences_never_disagree_with_a_full_walk(self) -> None:
        """The gate that actually pins the identity check.

        Hand-written sequences did not distinguish it: reusing the walked prefix across a REPLACED
        list passes every other test in this file, because ``delete_memory`` reads before it writes
        and so a new record is usually seen anyway. Random sequences over SMALL, FRESH stores do
        distinguish it -- 0 disagreements as written, 10 with the identity check removed. Fresh and
        small matters: the sequences that disagree are the early ones, where a delete removes a
        large fraction of a handful of records.

        After every operation the incremental answer is compared against a full walk of the same
        records. The seed is fixed, so a disagreement is reproducible rather than a story about a run
        that once failed.
        """
        import random
        import matrixark_mcp_local_adapter as module

        rng = random.Random(20260907)
        comparisons = 0
        for trial in range(12):
            with tempfile.TemporaryDirectory() as room:
                adapter = MatrixArkLocalAdapter(pathlib.Path(room) / "events.jsonl")
                live: list[str] = []
                counter = 0
                for _ in range(40):
                    counter += 1
                    record = _event(7_000 + trial * 100 + counter)
                    operation = rng.choice(["append", "append_many", "append_ttl",
                                            "append_ephemeral", "delete", "read"])
                    if operation in ("append_ttl", "append_ephemeral"):
                        record["expires_at_ms"] = ANCHOR_MS + 10_000 + counter
                    if operation == "append_ephemeral":
                        record["ephemeral"] = True
                    try:
                        if operation == "append_many":
                            batch = [dict(record, event_id_hash=f"{record['event_id_hash']}_{k}")
                                     for k in range(rng.randint(1, 5))]
                            adapter.append_many(batch)
                            live += [row["event_id_hash"] for row in batch]
                        elif operation == "delete" and live:
                            adapter.delete_memory({"memory_id": live.pop(rng.randrange(len(live)))})
                        elif operation == "read":
                            adapter.read_all()
                        else:
                            adapter.append(record)
                            live.append(record["event_id_hash"])
                    except Exception:
                        continue
                    records = adapter._read_all_compacted()
                    comparisons += 1
                    self.assertEqual(
                        adapter._records_need_expiry_filter_incrementally(records),
                        module._memory_records_need_expiry_filter(records),
                        f"trial {trial}: the incremental answer differs from a full walk "
                        f"after {operation}")
        self.assertGreater(comparisons, 400, "the sequences must actually exercise the guard")

    def test_a_store_with_no_ttl_still_lists_everything(self) -> None:
        self.assertEqual(len(self._ids()), 50)


if __name__ == "__main__":
    unittest.main(verbosity=2)
