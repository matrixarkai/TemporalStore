# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A record cache that failed to take an append must not keep serving reads.

`materialize_appended_records_locked` refreshes process-local views after the durable write. The
write has already happened by then, so a failure here loses nothing -- which is why it is caught.

But the records cache is not a mere accelerator. `read_all_without_disk_fallback_recovery` returns
it verbatim while the hot cache is on:

    if hot_cache_enabled and self._records_cache is not None:
        return list(self._records_cache)

so a cache that is known to be missing the records just appended answers reads that omit a write
the store holds, and nothing reports it. The safe response to a failed cache update is to drop the
cache -- the next read reloads from the store -- not to keep the incomplete one.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matrixark_mcp_temporal_append as append_module


class _RefusingList(list):
    """Extends fine until armed, then fails the way a cache update can fail."""

    armed = False

    def extend(self, items):
        if self.armed:
            raise RuntimeError("cache update failed")
        return super().extend(items)


class _Target:
    def __init__(self, cache):
        self._records_cache = cache
        self._entry_count_cache = 0
        self.direct_cache_puts = 0

    def _put_direct_record_cache(self, count, records):
        self.direct_cache_puts += 1

    def _prune_retrieval_candidate_cache(self, count):
        pass

    def _update_latest_entity_cache(self, records):
        pass


NEW_RECORDS = [{"record_type": "context_event", "event_id_hash": 1},
               {"record_type": "context_event", "event_id_hash": 2}]


class FailedCacheUpdateDropsTheCache(unittest.TestCase):
    def _materialize(self, target):
        append_module.materialize_appended_records_locked(
            target, prior_entry_count=0, new_entry_count=len(NEW_RECORDS), records=NEW_RECORDS)

    def test_the_cache_is_dropped_when_it_cannot_take_the_records(self):
        cache = _RefusingList([{"record_type": "context_event", "event_id_hash": 0}])
        cache.armed = True
        target = _Target(cache)

        self._materialize(target)

        self.assertIsNone(
            target._records_cache,
            "the cache could not take the appended records and was kept anyway; read_all returns "
            "it verbatim, so reads would omit a write the store holds",
        )

    def test_a_working_cache_is_kept_and_extended(self):
        """The control. Without it this passes on a change that drops the cache unconditionally."""
        cache = _RefusingList([{"record_type": "context_event", "event_id_hash": 0}])
        target = _Target(cache)

        self._materialize(target)

        self.assertIsNotNone(target._records_cache, "a healthy cache must survive an append")
        self.assertEqual(3, len(target._records_cache))
        self.assertEqual(1, target.direct_cache_puts)

    def test_the_append_itself_still_does_not_raise(self):
        """The reason the failure is caught at all: the durable write is already done."""
        cache = _RefusingList()
        cache.armed = True
        self._materialize(_Target(cache))       # must not raise


if __name__ == "__main__":
    unittest.main()
