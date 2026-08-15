# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
import unittest

from tools.matrixark_mcp_rust_proxy_cache import mark_context_pack_response_cache_hit


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


if __name__ == "__main__":
    unittest.main()
