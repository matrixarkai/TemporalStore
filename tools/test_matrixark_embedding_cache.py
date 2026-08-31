# SPDX-License-Identifier: Apache-2.0
"""The embedding vector cache: bounded, least-recently-used, and observable.

The policy this replaces cleared the WHOLE cache on overflow. That is worse than it sounds on an
ingest run: the cache fills during the first documents, and the clear then discards every warm entry
at the moment repeated text would start paying off, so a steady hit rate becomes a sawtooth. The
capacity also matters as memory in its own right -- an entry holds a 384-float vector, so the
default bound is roughly 25 MB per worker and needs to be configurable.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_embeddings as emb  # noqa: E402


class EmbeddingCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = os.environ.get("MATRIXARK_EMBEDDING_CACHE_ENTRIES")
        with emb._EMBEDDING_VECTOR_CACHE_LOCK:
            emb._EMBEDDING_VECTOR_CACHE.clear()
            for key in emb._EMBEDDING_CACHE_STATS:
                emb._EMBEDDING_CACHE_STATS[key] = 0

    def tearDown(self) -> None:
        if self._saved is None:
            os.environ.pop("MATRIXARK_EMBEDDING_CACHE_ENTRIES", None)
        else:
            os.environ["MATRIXARK_EMBEDDING_CACHE_ENTRIES"] = self._saved

    def _put(self, name: str) -> None:
        with emb._EMBEDDING_VECTOR_CACHE_LOCK:
            emb._cache_put(("m", name), [1.0, 2.0])

    def _get(self, name):
        with emb._EMBEDDING_VECTOR_CACHE_LOCK:
            return emb._cache_get(("m", name))

    def test_capacity_is_honoured_and_configurable(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_CACHE_ENTRIES"] = "3"
        for name in "abcde":
            self._put(name)
        self.assertEqual(emb.embedding_cache_stats()["entries"], 3)

    def test_eviction_drops_the_least_recently_used_not_everything(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_CACHE_ENTRIES"] = "3"
        for name in "abc":
            self._put(name)
        self._get("a")          # 'a' becomes most recent, so 'b' is now coldest
        self._put("d")          # evicts exactly one entry
        stats = emb.embedding_cache_stats()
        self.assertEqual(stats["entries"], 3)
        self.assertEqual(stats["evictions"], 1)
        self.assertIsNotNone(self._get("a"), "recently used entry must survive")
        self.assertIsNone(self._get("b"), "least recently used entry must be the one evicted")
        self.assertIsNotNone(self._get("c"))
        self.assertIsNotNone(self._get("d"))

    def test_overflow_keeps_serving_warm_entries(self) -> None:
        """The regression the old clear-on-full policy caused: a warm key must not vanish
        merely because unrelated keys pushed the cache past its bound."""
        os.environ["MATRIXARK_EMBEDDING_CACHE_ENTRIES"] = "4"
        self._put("hot")
        for index in range(20):
            self._put("cold%d" % index)
            self._get("hot")          # kept warm throughout
        self.assertIsNotNone(self._get("hot"), "a continuously used entry must never be evicted")

    def test_stats_report_hits_misses_and_hit_rate(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_CACHE_ENTRIES"] = "8"
        self._put("x")
        self._get("x")
        self._get("missing")
        stats = emb.embedding_cache_stats()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hit_rate"], 0.5)
        self.assertEqual(stats["capacity"], 8)

    def test_a_zero_capacity_disables_caching_without_error(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_CACHE_ENTRIES"] = "0"
        self._put("x")
        self.assertEqual(emb.embedding_cache_stats()["entries"], 0)
        self.assertIsNone(self._get("x"))

    def test_a_malformed_capacity_falls_back_to_the_default(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_CACHE_ENTRIES"] = "not-a-number"
        self.assertEqual(emb.embedding_cache_capacity(), 8192)


if __name__ == "__main__":
    unittest.main()
