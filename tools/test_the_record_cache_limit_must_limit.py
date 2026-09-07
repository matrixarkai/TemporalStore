# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The record-cache size limit has to actually limit something.

`MATRIXARK_DIRECT_RECORD_HOT_CACHE_MAX_RECORDS` (default 20,000) is defined in two modules, carried
through `matrixark_load_config`, and offered in the portal — `test_matrixark_the_portal_offers_what
_the_config_file_does` asserts it is presented as a live setting. Nothing read it. The only bound
applied was `_DIRECT_RECORD_CACHE_MAX_PREFIXES`, which caps how many STORES are cached, not how many
records an entry holds.

An operator setting it to bound memory therefore got no bound, and the entry retained the whole
decoded record list for a store of any size. Harmless while the cache was off for native backends;
not harmless once it is on.

These pin the limit from both sides, because only enforcing it is half a setting: under the limit
the cache must still serve (or the fix is just a slower cache), and over it the cache must decline
AND say so — a cache that stops serving looks exactly like one that is working, only slower.
"""
from __future__ import annotations

import os
import unittest

try:  # package path
    from tools import matrixark_mcp_direct_cache as cache
except ImportError:  # run from tools/
    import matrixark_mcp_direct_cache as cache  # type: ignore


ENV = "MATRIXARK_DIRECT_RECORD_HOT_CACHE_MAX_RECORDS"


class _Target:
    """The handful of attributes the cache helpers touch."""

    def __init__(self, namespace="ns1", table="t1", prefix="matrixark:mcp"):
        import threading
        self._namespace = namespace
        self._table = table
        self._storage_prefix = prefix
        self._entry_count_cache = None
        self._records_cache = None
        self._index_cache = None
        self._retrieval_candidate_cache = {}
        self._retrieval_candidate_cache_lock = threading.RLock()

    def python_hot_cache_enabled(self) -> bool:
        return True


def _records(n):
    return [{"record_type": "context_event", "i": i} for i in range(n)]


class RecordCacheLimitTest(unittest.TestCase):
    def setUp(self) -> None:
        self._previous = os.environ.get(ENV)
        with cache._DIRECT_RECORD_CACHE_LOCK:
            cache._DIRECT_RECORD_CACHE.clear()
        cache._RECORD_CACHE_DECLINE_REPORTED = False

    def tearDown(self) -> None:
        if self._previous is None:
            os.environ.pop(ENV, None)
        else:
            os.environ[ENV] = self._previous
        with cache._DIRECT_RECORD_CACHE_LOCK:
            cache._DIRECT_RECORD_CACHE.clear()
        cache._RECORD_CACHE_DECLINE_REPORTED = False

    def test_a_record_set_over_the_limit_is_not_cached(self) -> None:
        os.environ[ENV] = "10"
        target = _Target()
        cache.put_direct_record_cache(target, 25, _records(25))
        self.assertIsNone(
            cache.get_direct_record_cache(target, 25),
            "the documented record limit did not bound the cache",
        )

    def test_under_the_limit_the_cache_still_serves(self) -> None:
        # Without this the "fix" could simply be a cache that never caches.
        os.environ[ENV] = "10"
        target = _Target()
        cache.put_direct_record_cache(target, 5, _records(5))
        self.assertEqual(_records(5), cache.get_direct_record_cache(target, 5))

    def test_declining_is_reported(self) -> None:
        os.environ[ENV] = "10"
        seen = []
        target = _Target()
        original = cache._log_record_cache_declined
        cache._log_record_cache_declined = lambda n, limit: seen.append((n, limit))
        try:
            cache.put_direct_record_cache(target, 25, _records(25))
        finally:
            cache._log_record_cache_declined = original
        self.assertEqual([(25, 10)], seen, "the cache went quiet instead of saying it declined")

    def test_zero_means_no_ceiling(self) -> None:
        os.environ[ENV] = "0"
        target = _Target()
        cache.put_direct_record_cache(target, 50, _records(50))
        self.assertEqual(50, len(cache.get_direct_record_cache(target, 50) or []))

    def test_the_limit_is_read_at_call_time(self) -> None:
        # Captured at import, a deployment raising the limit would have to restart every hook.
        os.environ[ENV] = "10"
        self.assertEqual(10, cache.direct_record_cache_max_records())
        os.environ[ENV] = "40"
        self.assertEqual(40, cache.direct_record_cache_max_records())

    def test_an_unparseable_limit_falls_back_to_the_documented_default(self) -> None:
        os.environ[ENV] = "not-a-number"
        self.assertEqual(20000, cache.direct_record_cache_max_records())


if __name__ == "__main__":
    unittest.main()
