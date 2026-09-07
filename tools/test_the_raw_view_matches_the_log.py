#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The raw view is kept current by the write, not re-derived by the read.

``_read_raw_records`` is cached on (total size, newest mtime), so any append moved the key and the
next call re-read and re-parsed every line of the durable log -- 60,033 ``json.loads`` to answer one
``history`` over 20,000 records. Measured there: history is 0.06 ms warm and was **418.68 ms after a
single append**, and in a realistic read/write mix it was 51% of all time spent.

The compacted cache never had that problem, because the write path extends it in place. The raw view
now takes the same records at the same site.

The risk is the whole point of the tests below: the raw view IS the durable history, so if the
in-memory copy can drift from the file, ``history`` answers something the log does not say. The main
test opens a SECOND adapter over the same directory that never writes -- so its raw view always
comes off disk -- and requires the two to agree after every operation.
"""
from __future__ import annotations

import os
import pathlib
import random
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


class TheRawViewMatchesTheLogTest(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.log = pathlib.Path(self.tmp.name) / "events.jsonl"
        self.adapter = MatrixArkLocalAdapter(self.log)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _from_disk(self) -> list:
        """A second adapter that never writes, so its raw view is always parsed from the file."""
        return MatrixArkLocalAdapter(self.log)._read_raw_records()

    def test_random_sequences_never_drift_from_the_file(self) -> None:
        """The one that matters. An in-memory stand-in for the durable log has to be identical to
        it, record for record and in order, after anything."""
        rng = random.Random(31337)
        comparisons = 0
        for trial in range(6):
            with tempfile.TemporaryDirectory() as room:
                log = pathlib.Path(room) / "events.jsonl"
                adapter = MatrixArkLocalAdapter(log)
                live: list[str] = []
                counter = 0
                for _ in range(40):
                    counter += 1
                    operation = rng.choice(["append", "append_many", "delete", "history",
                                            "read_all", "ttl"])
                    try:
                        if operation == "append_many":
                            batch = [_event(trial * 1_000 + counter * 10 + k)
                                     for k in range(rng.randint(1, 4))]
                            adapter.append_many(batch)
                            live += [row["event_id_hash"] for row in batch]
                        elif operation == "delete" and live:
                            adapter.delete_memory(
                                {"memory_id": live.pop(rng.randrange(len(live)))})
                        elif operation == "history" and live:
                            adapter.history({"memory_id": rng.choice(live)})
                        elif operation == "read_all":
                            adapter.read_all()
                        else:
                            record = _event(trial * 1_000 + counter * 10)
                            if operation == "ttl":
                                record["expires_at_ms"] = ANCHOR_MS + 900_000 + counter
                            adapter.append(record)
                            live.append(record["event_id_hash"])
                    except Exception:
                        continue
                    mine = adapter._read_raw_records()
                    fresh = MatrixArkLocalAdapter(log)._read_raw_records()
                    comparisons += 1
                    self.assertEqual(len(mine), len(fresh),
                                     "the in-memory raw view has a different number of records "
                                     "than the file")
                    self.assertEqual(mine, fresh,
                                     "the in-memory raw view differs from a fresh parse")
        self.assertGreater(comparisons, 150, "the sequences must actually exercise the view")

    def test_an_append_is_visible_without_re_reading_the_file(self) -> None:
        self.adapter.append_many([_event(i) for i in range(20)])
        before = len(self.adapter._read_raw_records())
        self.adapter.append(_event(900))
        self.assertEqual(len(self.adapter._read_raw_records()), before + 1)
        self.assertEqual(self.adapter._read_raw_records(), self._from_disk())

    def test_history_sees_a_record_appended_after_it_was_served(self) -> None:
        self.adapter.append_many([_event(i) for i in range(20)])
        self.adapter.history({"memory_id": "e5"})
        self.adapter.append({"record_type": self.adapter.MEMORY_FEEDBACK_RECORD_TYPE,
                             "target_memory_id": "e5", "feedback": "up",
                             "updated_at_ms": ANCHOR_MS + 5_000})
        events = self.adapter.history({"memory_id": "e5"})["history"]
        self.assertIn("feedback", [row["event"] for row in events])

    def test_a_delete_still_reaches_the_raw_view(self) -> None:
        """A delete writes a tombstone, and the raw view is the un-tombstoned history, so the
        tombstone itself must appear in it."""
        self.adapter.append_many([_event(i) for i in range(20)])
        self.adapter.history({"memory_id": "e5"})
        self.adapter.delete_memory({"memory_id": "e5"})
        self.assertEqual(self.adapter._read_raw_records(), self._from_disk())
        self.assertIn("deleted",
                      [row["event"] for row in self.adapter.history({"memory_id": "e5"})["history"]])

    def test_an_append_does_not_send_the_view_back_to_the_file(self) -> None:
        """The performance claim, pinned rather than left to a benchmark.

        Correctness alone cannot see this: forgetting to advance the cache key still ANSWERS
        correctly, because the next read finds a signature mismatch and re-parses -- which is exactly
        the 418 ms this change exists to remove. So the test is that the view is the SAME list
        object afterwards, which is only true if it was extended rather than rebuilt.
        """
        self.adapter.append_many([_event(i) for i in range(20)])
        self.adapter._read_raw_records()
        before = self.adapter._raw_records_cache[1]
        self.adapter.append(_event(901))
        after_read = self.adapter._read_raw_records()
        after = self.adapter._raw_records_cache[1]
        # The CACHED list, not the returned one: the reader hands out snapshots on purpose, so
        # comparing what it returned would prove nothing either way.
        self.assertIs(after, before, "the raw view was rebuilt from the file, not extended")
        self.assertEqual(len(after_read), len(before))
        self.assertEqual(after_read, self._from_disk())

    def test_a_history_after_an_append_does_not_rebuild_the_index(self) -> None:
        """Same shape one layer up: the index over the raw view is extended, not rebuilt."""
        self.adapter.append_many([_event(i) for i in range(20)])
        self.adapter.history({"memory_id": "e1"})
        first = getattr(self.adapter, "_raw_history_index_memo", None)
        self.assertIsNotNone(first)
        self.adapter.append(_event(902))
        self.adapter.history({"memory_id": "e1"})
        second = getattr(self.adapter, "_raw_history_index_memo", None)
        self.assertIs(second[1], first[1], "the history index was rebuilt rather than extended")
        self.assertGreater(second[3], first[3], "and it must have taken in the new record")

    def test_another_writer_is_not_lost_from_the_view(self) -> None:
        """The guard that makes extending safe at all, and the one thing here that could lose
        durable history rather than merely be slow.

        Two adapters share one log. The first reads the raw view, so it holds a list as of that
        moment. The second appends -- the first knows nothing about it. Now the first appends: if it
        extended its own stale list and stamped the new signature on it, the second writer's record
        would be absent from a view that claims to be current, and `history` would answer something
        the log does not say.

        The guard is that the view must have been current as of immediately before this write. It is
        not, so it is dropped and the next read re-derives from the file.
        """
        first = MatrixArkLocalAdapter(self.log)
        second = MatrixArkLocalAdapter(self.log)
        first.append_many([_event(i) for i in range(10)])
        first._read_raw_records()                       # first now holds a view

        second.append(_event(500))                      # the other writer, unseen by first
        first.append(_event(501))                       # first extends or drops

        view = first._read_raw_records()
        ids = [str(row.get("event_id_hash")) for row in view]
        self.assertIn("e500", ids, "the other writer's record is missing from the raw view")
        self.assertIn("e501", ids)
        self.assertEqual(view, self._from_disk(), "the raw view disagrees with the file")

    def test_a_held_result_does_not_change_underneath_its_caller(self) -> None:
        """The defect this change introduced, and the reason the reader hands back a snapshot.

        Extending the cache in place is what makes a write cheap. It is also what makes the cached
        list unsafe to hand out: a caller holding an earlier result would watch it grow, because it
        IS the list being extended. `test_raw_records_served_from_memory` catches it as "49 not
        greater than 49" -- its `first` and `after` had become the same object.

        Both ways out of the reader are covered here, because only one of them was fixed first and
        the test still failed: the cache-HIT path, and the parse path that populates the cache and
        returns what it just built.
        """
        self.adapter.append_many([_event(i) for i in range(20)])

        parsed = self.adapter._read_raw_records()        # the parse path populates the cache
        held_after_parse = len(parsed)
        self.adapter.append(_event(910))
        self.assertEqual(len(parsed), held_after_parse,
                         "a result from the parse path grew under its caller")

        served = self.adapter._read_raw_records()        # now the cache-hit path
        held_after_hit = len(served)
        self.adapter.append(_event(911))
        self.assertEqual(len(served), held_after_hit,
                         "a result from the cache-hit path grew under its caller")

        # And the reader still reports the new records to anyone who asks again.
        self.assertEqual(len(self.adapter._read_raw_records()), held_after_hit + 1)
        self.assertEqual(self.adapter._read_raw_records(), self._from_disk())

    def test_the_view_is_only_kept_current_once_something_asks_for_it(self) -> None:
        """Nothing pays for the raw view until it is wanted: an adapter that never calls history
        holds no raw records, and the first ask still reads the file."""
        self.adapter.append_many([_event(i) for i in range(20)])
        self.assertIsNone(getattr(self.adapter, "_raw_records_cache", None))
        self.adapter.history({"memory_id": "e1"})
        self.assertIsNotNone(getattr(self.adapter, "_raw_records_cache", None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
