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


class _StreamProc:
    """A lane process whose stdout already holds the given response lines."""

    def __init__(self, lines):
        import os
        read_fd, write_fd = os.pipe()
        self.stdout = os.fdopen(read_fd, "r")
        with os.fdopen(write_fd, "w") as writer:
            for line in lines:
                writer.write(line + "\n")

    def poll(self):
        return None


def _lane_reader():
    reader = object.__new__(adapters.MatrixArkRustProxyClient)
    reader.request_timeout_ms = 2000
    reader.cli_path = "matrixark_rust_proxy"
    return reader


class LaneResponseCorrelationTests(unittest.TestCase):
    """The proxy answers strictly in order on one stdout, so the late response of a request an
    earlier caller abandoned (its own timeout) sits first in the stream. Without correlation the
    next caller reads it as ITS answer and every later reply shifts one back -- observed live as
    one scope's scan answered with another scope's records. The reader must discard responses
    tagged for a different request and accept untagged ones (older proxy binaries)."""

    def test_a_stale_tagged_response_is_discarded(self):
        import json as _json
        stale = _json.dumps({"ok": True, "value": "stale", "client_request_id": "abandoned-1"})
        mine = _json.dumps({"ok": True, "value": "mine", "client_request_id": "current-2"})
        proc = _StreamProc([stale, mine])
        response = _lane_reader()._read_json_line(proc, "get_string", None,
                                                  expected_request_id="current-2")
        self.assertEqual("mine", response.get("value"))

    def test_an_untagged_response_still_answers(self):
        import json as _json
        proc = _StreamProc([_json.dumps({"ok": True, "value": "legacy"})])
        response = _lane_reader()._read_json_line(proc, "get_string", None,
                                                  expected_request_id="current-9")
        self.assertEqual("legacy", response.get("value"))


if __name__ == "__main__":
    unittest.main()
