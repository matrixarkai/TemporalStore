#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The direct record cache: what makes it safe to serve a read from.

It is validated by the record COUNT, read fresh from the store on every call. The record log is
append-only — a delete is a tombstone append — so any write moves the count and the entry misses.
That is the whole safety argument, and these pin it, because a cache that serves one stale read is
worse than no cache at all.

The second thing pinned here is the cache KEY. `_storage_prefix` defaults to "matrixark:mcp" for
every adapter, while the store a client actually talks to is separated by namespace and table, so
keying on the prefix alone lets two adapters over different stores share an entry and serve each
other's records. That was latent while the cache was off for native backends; it is reachable now
that it is on.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_direct_cache as cache
except ImportError:  # run from tools/ dir
    import matrixark_mcp_direct_cache as cache


class _Target:
    """The handful of attributes the cache helpers touch."""

    def __init__(self, namespace="ns1", table="t1", prefix="matrixark:mcp", enabled=True):
        self._namespace = namespace
        self._table = table
        self._storage_prefix = prefix
        self._enabled = enabled
        self._entry_count_cache = None
        self._records_cache = None
        self._index_cache = None
        import threading
        self._retrieval_candidate_cache = {}
        self._retrieval_candidate_cache_lock = threading.RLock()

    def python_hot_cache_enabled(self) -> bool:
        return self._enabled


class DirectRecordCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        # The cache is process-global; start each test from empty.
        with cache._DIRECT_RECORD_CACHE_LOCK:
            cache._DIRECT_RECORD_CACHE.clear()

    def test_a_hit_requires_the_same_count(self) -> None:
        t = _Target()
        cache.put_direct_record_cache(t, 10, [{"r": 1}])
        self.assertEqual([{"r": 1}], cache.get_direct_record_cache(t, 10))

    def test_an_append_invalidates_it(self) -> None:
        """This is read-after-write: one more record means a different count, so no hit."""
        t = _Target()
        cache.put_direct_record_cache(t, 10, [{"r": 1}])
        self.assertIsNone(cache.get_direct_record_cache(t, 11),
                          "a changed record count must not serve the old records")

    def test_a_shrinking_count_also_misses(self) -> None:
        t = _Target()
        cache.put_direct_record_cache(t, 10, [{"r": 1}])
        self.assertIsNone(cache.get_direct_record_cache(t, 9))

    def test_returns_a_copy_not_the_cached_list(self) -> None:
        """A caller mutating its result must not corrupt what everyone else reads."""
        t = _Target()
        cache.put_direct_record_cache(t, 10, [{"r": 1}])
        got = cache.get_direct_record_cache(t, 10)
        got.append({"r": 2})
        self.assertEqual([{"r": 1}], cache.get_direct_record_cache(t, 10))

    # ---- the key names the store -------------------------------------------------------

    def test_different_namespaces_do_not_share_an_entry(self) -> None:
        a = _Target(namespace="ns_a")
        b = _Target(namespace="ns_b")
        cache.put_direct_record_cache(a, 10, [{"store": "a"}])
        self.assertIsNone(cache.get_direct_record_cache(b, 10),
                          "a different namespace is a different store")

    def test_different_tables_do_not_share_an_entry(self) -> None:
        a = _Target(table="t_a")
        b = _Target(table="t_b")
        cache.put_direct_record_cache(a, 10, [{"store": "a"}])
        self.assertIsNone(cache.get_direct_record_cache(b, 10))

    def test_same_store_does_share(self) -> None:
        a = _Target(namespace="ns", table="t")
        b = _Target(namespace="ns", table="t")
        cache.put_direct_record_cache(a, 10, [{"store": "shared"}])
        self.assertEqual([{"store": "shared"}], cache.get_direct_record_cache(b, 10))

    def test_scope_includes_all_three_parts(self) -> None:
        scope = cache.direct_cache_scope(_Target(namespace="N", table="T", prefix="P"))
        for part in ("N", "T", "P"):
            self.assertIn(part, scope)

    # ---- the switch still works ---------------------------------------------------------

    def test_disabled_never_serves(self) -> None:
        t = _Target(enabled=True)
        cache.put_direct_record_cache(t, 10, [{"r": 1}])
        t._enabled = False
        self.assertIsNone(cache.get_direct_record_cache(t, 10))

    def test_disabled_never_stores(self) -> None:
        t = _Target(enabled=False)
        cache.put_direct_record_cache(t, 10, [{"r": 1}])
        t._enabled = True
        self.assertIsNone(cache.get_direct_record_cache(t, 10))

    def test_drop_clears_the_entry(self) -> None:
        t = _Target()
        cache.put_direct_record_cache(t, 10, [{"r": 1}])
        cache.drop_direct_record_cache(t)
        self.assertIsNone(cache.get_direct_record_cache(t, 10))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
