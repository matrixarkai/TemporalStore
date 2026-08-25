#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A backend that cannot answer yet must never serve as an empty store.

A populated store whose shard was still loading (or whose load had been refused) answered
every read as a clean empty -- ``_get_count`` swallowed the backend error into 0 and
``_get_index`` into [], so ``get_all`` returned ``{"memories": [], "count": 0}`` with a 200
while thousands of records sat durably on disk. Worse, a write path that derives its append
position from the count would derive it from the lie. These tests pin the corrected
contract: a RETRYABLE backend failure (shard not loaded / recovering / timeout) raises; only
an ABSENT key on a reachable backend is an authoritative zero. They also pin the retry
classifier knowing the engine's load-window statuses.
"""
import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
    from tools.matrixark_mcp_errors import MatrixArkError, is_retryable_temporalstore_error
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters
    from matrixark_mcp_errors import MatrixArkError, is_retryable_temporalstore_error


class _LoadingClient:
    """A client whose shard is mid-load: every read fails with the engine's status."""

    def get_string(self, key):
        raise MatrixArkError(
            "Rust TemporalStore get_string failed: shard_not_loaded: "
            "shard is not loaded on this server"
        )


class _BrokenClient:
    """A client failing with a NON-retryable error: best-effort answers stay best-effort."""

    def get_string(self, key):
        raise MatrixArkError("stored value is not UTF-8: invalid byte")


class _EmptyStoreClient:
    """A reachable backend with no count/index keys: the real empty store."""

    def get_string(self, key):
        return ""


class _Adapter(adapters.MatrixArkTemporalStoreDirectAdapter):
    def __init__(self, client):
        self._client = client
        self._count_key = "p:record_count"
        self._index_key = "p:record_index"


class BackendUnavailableReadTests(unittest.TestCase):
    def test_count_raises_while_the_shard_loads(self):
        with self.assertRaises(MatrixArkError):
            _Adapter(_LoadingClient())._get_count()

    def test_index_raises_while_the_shard_loads(self):
        with self.assertRaises(MatrixArkError):
            _Adapter(_LoadingClient())._get_index()

    def test_non_retryable_failures_keep_the_best_effort_answer(self):
        self.assertEqual(0, _Adapter(_BrokenClient())._get_count())
        self.assertEqual([], _Adapter(_BrokenClient())._get_index())

    def test_an_absent_key_on_a_reachable_backend_is_zero(self):
        self.assertEqual(0, _Adapter(_EmptyStoreClient())._get_count())
        self.assertEqual([], _Adapter(_EmptyStoreClient())._get_index())


class LoadWindowRetryClassificationTests(unittest.TestCase):
    def test_not_loaded_is_retryable(self):
        self.assertTrue(is_retryable_temporalstore_error(
            "shard_not_loaded: shard is not loaded on this server"))

    def test_recovering_is_retryable(self):
        self.assertTrue(is_retryable_temporalstore_error(
            "shard_not_loaded: shard is recovering (WAL replay in progress)"))

    def test_plain_data_errors_stay_terminal(self):
        self.assertFalse(is_retryable_temporalstore_error("stored value is not UTF-8"))


if __name__ == "__main__":
    unittest.main()
