# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
import unittest

from tools.matrixark_mcp_rust_proxy_cache import (
    context_pack_response_cache_key,
    mark_context_pack_response_cache_hit,
)


class MatrixArkRustProxyCacheTest(unittest.TestCase):
    def test_context_pack_response_cache_hit_preserves_candidate_cache_miss(self) -> None:
        response = {
            "cache_hit": False,
            "retrieval_metrics": {
                "candidate_cache_hit": False,
                "serving_memory_promoted": True,
            },
            "context_pack": {
                "retrieval_metrics": {
                    "candidate_cache_hit": False,
                    "serving_memory_promoted": True,
                }
            },
        }

        cached = mark_context_pack_response_cache_hit(response)

        self.assertTrue(cached["cache_hit"])
        self.assertTrue(cached["context_pack_response_cache_hit"])
        self.assertTrue(cached["retrieval_metrics"]["cache_hit"])
        self.assertTrue(cached["retrieval_metrics"]["context_pack_response_cache_hit"])
        self.assertFalse(cached["retrieval_metrics"]["candidate_cache_hit"])
        self.assertTrue(cached["context_pack"]["retrieval_metrics"]["cache_hit"])
        self.assertTrue(cached["context_pack"]["retrieval_metrics"]["context_pack_response_cache_hit"])
        self.assertFalse(cached["context_pack"]["retrieval_metrics"]["candidate_cache_hit"])
        self.assertFalse(response["cache_hit"])
        self.assertFalse(response["retrieval_metrics"]["candidate_cache_hit"])

    def test_context_pack_response_cache_key_includes_record_count_watermark(self) -> None:
        request = {
            "scope": {"tenant_id": "tenant_a"},
            "query": "reload context",
            "ranking": {"max_selected_refs": 8},
        }

        first = context_pack_response_cache_key(
            count_key="matrixark:test:record_count",
            record_hash_key="matrixark:test:records",
            shard_size=256,
            request=request,
            record_count_watermark="10",
        )
        second = context_pack_response_cache_key(
            count_key="matrixark:test:record_count",
            record_hash_key="matrixark:test:records",
            shard_size=256,
            request=request,
            record_count_watermark="11",
        )

        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
