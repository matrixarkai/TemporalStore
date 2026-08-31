# SPDX-License-Identifier: Apache-2.0
"""Instruction prefixes for encoder families that expect them, and the cache key that follows.

The e5 family is trained with "query: " and "passage: " on the input and scores materially worse
without them: on a 298-pair benchmark, adding the prefixes moved hit@1 from 68.8% to 74.8%.

The prefix differs by side, so identical text embeds differently as a query than as a passage. That
makes the role part of the cache identity -- caching them together would hand back a query vector
where a passage vector was asked for, silently, with no dimension change to notice.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_embeddings as emb  # noqa: E402


class PrefixSelectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = dict(os.environ)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._saved)

    def _model(self, name: str) -> None:
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "openai_compatible"
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = name

    def test_e5_models_take_the_query_and_passage_prefixes(self) -> None:
        self._model("intfloat/multilingual-e5-small")
        self.assertEqual(emb.embedding_input_prefix("query"), "query: ")
        self.assertEqual(emb.embedding_input_prefix("passage"), "passage: ")

    def test_the_prefix_applies_regardless_of_the_org_path(self) -> None:
        self._model("intfloat/multilingual-e5-large")
        self.assertEqual(emb.embedding_input_prefix("query"), "query: ")

    def test_models_that_do_not_expect_a_prefix_get_none(self) -> None:
        for name in ("sentence-transformers/all-MiniLM-L6-v2",
                     "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                     "BAAI/bge-m3"):
            with self.subTest(model=name):
                self._model(name)
                self.assertEqual(emb.embedding_input_prefix("query"), "")
                self.assertEqual(emb.embedding_input_prefix("passage"), "")

    def test_an_unknown_role_is_treated_as_a_passage(self) -> None:
        """Document text is the overwhelmingly common case, so it is the safe default."""
        self._model("intfloat/multilingual-e5-small")
        self.assertEqual(emb.embedding_input_prefix("something-else"), "passage: ")


class CacheIdentityTest(unittest.TestCase):
    """The role has to reach the cache key, not just the request.

    These pin the behaviour with the model name forced, because `embedding_model_name()` reports the
    provider's model and the deterministic provider has none -- so a fixture that merely sets
    MATRIXARK_EMBEDDING_MODEL while running deterministically exercises no prefix at all.
    """

    def setUp(self) -> None:
        self._saved = dict(os.environ)
        self._real_name = emb.embedding_model_name
        emb.embedding_model_name = lambda: "intfloat/multilingual-e5-small"
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "deterministic"
        with emb._EMBEDDING_VECTOR_CACHE_LOCK:
            emb._EMBEDDING_VECTOR_CACHE.clear()

    def tearDown(self) -> None:
        emb.embedding_model_name = self._real_name
        os.environ.clear()
        os.environ.update(self._saved)

    def test_the_two_roles_produce_different_inputs(self) -> None:
        text = "how do I rotate an API key"
        self.assertEqual(emb._with_prefix(text, "query"), "query: " + text)
        self.assertEqual(emb._with_prefix(text, "passage"), "passage: " + text)

    def test_the_same_text_as_query_and_passage_does_not_share_a_cache_entry(self) -> None:
        text = "how do I rotate an API key"
        emb.embedding_for_text(text, role="query")
        emb.embedding_for_text(text, role="passage")
        with emb._EMBEDDING_VECTOR_CACHE_LOCK:
            keys = list(emb._EMBEDDING_VECTOR_CACHE)
        self.assertEqual(len(keys), 2, "each role needs its own cache entry")
        cached = {key[1] for key in keys}
        self.assertIn("query: " + text, cached)
        self.assertIn("passage: " + text, cached)

    def test_batch_and_single_agree_for_the_same_role(self) -> None:
        text = "apply the coupon at checkout"
        single = emb.embedding_for_text(text, role="passage")
        batched = emb.embeddings_for_texts([text], role="passage")[0]
        self.assertEqual(single, batched)

    def test_passage_is_the_default_role(self) -> None:
        text = "confirm the total before paying"
        self.assertEqual(emb.embedding_for_text(text),
                         emb.embedding_for_text(text, role="passage"))


if __name__ == "__main__":
    unittest.main()
