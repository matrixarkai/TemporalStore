# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A record cache that failed to take an append must not keep serving reads -- in BOTH copies.

`materialize_appended_records_locked` refreshes process-local views after the durable write. The
write has already happened by then, so a failure here loses nothing -- which is why it is caught.

But the records cache is not a mere accelerator. `read_all_without_disk_fallback_recovery` returns
it verbatim while the hot cache is on:

    if hot_cache_enabled and self._records_cache is not None:
        return list(self._records_cache)

so a cache that is known to be missing the records just appended answers reads that omit a write
the store holds, and nothing reports it. The safe response to a failed cache update is to drop the
cache -- the next read reloads from the store -- not to keep the incomplete one.

THIS RULE HAS TWO IMPLEMENTATIONS, and they are both live:

    matrixark_mcp_temporal_append.materialize_appended_records_locked        <- append_many_materialized
    _TemporalDirectWriteMixin._materialize_appended_records_locked           <- _append_many_materialized

`MatrixArkTemporalStoreDirectAdapter` inherits the write mixin and `_TemporalDirectReadMixin`
together, so the mixin's `self._records_cache` is the same list the read side returns verbatim.
The first copy was fixed and the second was not, because the guard that recorded the rule only
ever called one of them. Every assertion below now runs against both, so a fix applied to one
copy cannot leave the other behind.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matrixark_mcp_temporal_append as append_module
# Reached through the parent on purpose: matrixark_temporal_direct_write and
# matrixark_mcp_temporal_adapters import each other, so importing the mixin module first
# raises ImportError. Production binds the mixin through the adapter, and so does this.
import matrixark_mcp_temporal_adapters as adapters_module


def _materialize_through_append_module(target, **kwargs):
    append_module.materialize_appended_records_locked(target, **kwargs)


def _materialize_through_write_mixin(target, **kwargs):
    """The mixin copy, called the way a bound method would be.

    `_TemporalDirectWriteMixin` is a mixin: it is never instantiated on its own, so the function is
    taken off the class and handed the same stand-in `target` the other copy gets.
    """
    adapters_module._TemporalDirectWriteMixin._materialize_appended_records_locked(
        target, **kwargs)


#: Every live implementation of the rule this file records.
MATERIALIZERS = {
    "matrixark_mcp_temporal_append.materialize_appended_records_locked":
        _materialize_through_append_module,
    "_TemporalDirectWriteMixin._materialize_appended_records_locked":
        _materialize_through_write_mixin,
}


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

    def test_both_copies_of_the_rule_are_really_there(self) -> None:
        """A floor. Every subTest below is skipped silently if a name moved."""
        self.assertEqual(
            2, len(MATERIALIZERS),
            "this file asserts the rule against every copy of it; %d were wired"
            % len(MATERIALIZERS))
        self.assertTrue(
            callable(getattr(append_module, "materialize_appended_records_locked", None)),
            "matrixark_mcp_temporal_append no longer defines materialize_appended_records_locked")
        self.assertTrue(
            callable(getattr(adapters_module._TemporalDirectWriteMixin,
                             "_materialize_appended_records_locked", None)),
            "_TemporalDirectWriteMixin no longer defines _materialize_appended_records_locked; if "
            "it now delegates to the other copy, drop it from MATERIALIZERS and say so")

    def test_the_cache_is_dropped_when_it_cannot_take_the_records(self) -> None:
        for label, materialize in sorted(MATERIALIZERS.items()):
            with self.subTest(copy=label):
                cache = _RefusingList([{"record_type": "context_event", "event_id_hash": 0}])
                cache.armed = True
                target = _Target(cache)

                materialize(target, prior_entry_count=0, new_entry_count=len(NEW_RECORDS),
                            records=NEW_RECORDS)

                self.assertIsNone(
                    target._records_cache,
                    "%s left a cache that could not take the appended records; read_all returns it "
                    "verbatim, so reads would omit a write the store holds" % label)

    def test_a_working_cache_is_kept_and_extended(self) -> None:
        """The control. Without it this passes on a change that drops the cache unconditionally."""
        for label, materialize in sorted(MATERIALIZERS.items()):
            with self.subTest(copy=label):
                cache = _RefusingList([{"record_type": "context_event", "event_id_hash": 0}])
                target = _Target(cache)

                materialize(target, prior_entry_count=0, new_entry_count=len(NEW_RECORDS),
                            records=NEW_RECORDS)

                self.assertIsNotNone(target._records_cache,
                                     "%s dropped a healthy cache" % label)
                self.assertEqual(3, len(target._records_cache), label)
                self.assertEqual(1, target.direct_cache_puts, label)

    def test_the_append_itself_still_does_not_raise(self) -> None:
        """The reason the failure is caught at all: the durable write is already done."""
        for label, materialize in sorted(MATERIALIZERS.items()):
            with self.subTest(copy=label):
                cache = _RefusingList()
                cache.armed = True
                materialize(_Target(cache), prior_entry_count=0,
                            new_entry_count=len(NEW_RECORDS), records=NEW_RECORDS)


if __name__ == "__main__":
    unittest.main()
