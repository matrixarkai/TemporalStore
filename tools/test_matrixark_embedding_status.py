#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Encoding state: how much of the store is actually searchable yet.

Ingest can defer encoding — chunking is synchronous and the vector is filled in behind it. Between
the write and the drainer catching up, a chunk exists and cannot be matched on meaning, so a
retrieve over that window returns less than it should and says nothing. That reads as "retrieval is
bad" rather than "retrieval is not finished", and the difference is a number nobody was counting.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg, drive  # noqa: E402

ADMIN = {"Authorization": "Bearer k-acme"}


def _adapter(records):
    """A dashboard mixin bound to a fixed record list, without a store behind it."""
    import matrixark_mcp_local_adapter as adapter_module

    class _Fixed(adapter_module.MatrixArkLocalAdapter):  # type: ignore[misc]
        def __init__(self, rows):
            self._rows = rows

        def read_all(self):
            return self._rows

    return _Fixed(records)


def _embedding(**fields):
    row = {"record_type": "context_embedding", "vector": [0.1, 0.2], "dim": 2,
           "model": "minilm", "updated_at_ms": 1_700_000_000_000}
    row.update(fields)
    return row


class EmbeddingStatusTest(unittest.TestCase):
    def test_encoded_and_pending_are_counted_separately(self) -> None:
        status = _adapter([
            _embedding(),
            _embedding(),
            _embedding(extraction_phase="pending_async"),
        ]).embedding_status({})
        self.assertEqual(3, status["total"])
        self.assertEqual(2, status["encoded"])
        self.assertEqual(1, status["pending"])
        self.assertEqual(66.7, status["percent_encoded"])

    def test_a_record_with_no_vector_counts_as_waiting(self) -> None:
        # The marker and the missing vector are two spellings of the same state; a record can carry
        # either, and counting only the marker would under-report the backlog.
        status = _adapter([_embedding(vector=[]), _embedding(vector=None)]).embedding_status({})
        self.assertEqual(2, status["pending"])
        self.assertEqual(2, status["without_vector"])
        self.assertEqual(0, status["encoded"])

    def test_every_spelling_of_the_pending_marker_is_recognised(self) -> None:
        for field, value in (("extraction_phase", "pending_async"),
                             ("event_type", "pending_async"),
                             ("classification", "PENDING_ASYNC_EXTRACTION")):
            with self.subTest(marker=field):
                status = _adapter([_embedding(**{field: value})]).embedding_status({})
                self.assertEqual(1, status["pending"], "%s=%s was not read as pending"
                                 % (field, value))

    def test_mixed_vector_widths_are_called_out(self) -> None:
        # Vectors of different widths cannot be compared, so some memories can never match a
        # query. It happens the moment the embedding model changes without a backfill, and looks
        # exactly like ordinary poor recall.
        status = _adapter([
            _embedding(dim=384, vector=[0.0] * 384),
            _embedding(dim=768, vector=[0.0] * 768),
        ]).embedding_status({})
        self.assertTrue(status["mixed_dimensions"])
        self.assertEqual({384, 768}, {d["dim"] for d in status["dimensions"]})

    def test_one_width_is_not_called_out(self) -> None:
        status = _adapter([_embedding(), _embedding()]).embedding_status({})
        self.assertFalse(status["mixed_dimensions"])

    def test_the_models_in_use_are_reported(self) -> None:
        status = _adapter([
            _embedding(model="minilm"), _embedding(model="minilm"), _embedding(model="bge-m3"),
        ]).embedding_status({})
        self.assertEqual([{"model": "minilm", "count": 2}, {"model": "bge-m3", "count": 1}],
                         status["models"])

    def test_deferred_pipeline_work_is_counted(self) -> None:
        status = _adapter([
            {"record_type": "matrixark_async_pipeline_task", "remaining_stages": ["a", "b"]},
            {"record_type": "matrixark_async_pipeline_task", "remaining_stages": []},
        ]).embedding_status({})
        self.assertEqual(1, status["deferred_tasks"])
        self.assertEqual(2, status["deferred_stages"])

    def test_an_empty_store_is_complete_rather_than_zero_percent(self) -> None:
        # 0/0 is "nothing to do", and reporting 0% would put a red bar on a healthy new deployment.
        status = _adapter([]).embedding_status({})
        self.assertEqual(0, status["total"])
        self.assertEqual(100.0, status["percent_encoded"])

    def test_records_that_are_not_embeddings_are_ignored(self) -> None:
        status = _adapter([
            _embedding(),
            {"record_type": "context_event", "vector": [0.1]},
            {"record_type": "resource_manifest"},
        ]).embedding_status({})
        self.assertEqual(1, status["total"])


class EmbeddingRouteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _FakeServer()
        self.app = gw.make_v1_app(self.server, _cfg())
        self._saved = dict(os.environ)
        # The encoder summary reads the process environment; a value left by another test would
        # make the "no encoder configured" half of this pass or fail for the wrong reason.
        for name in list(os.environ):
            if name.startswith("MATRIXARK_EMBEDDING"):
                del os.environ[name]

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._saved)

    def test_it_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="GET", path="/v1/admin/embeddings")
        self.assertEqual(401, status)
        self.assertEqual([], self.server.calls)

    def test_it_reaches_the_status_tool_with_the_scope(self) -> None:
        status, _, _ = drive(self.app, method="GET",
                             path="/v1/admin/embeddings?user_id=alice", headers=ADMIN)
        self.assertEqual(200, status)
        name, args = self.server.calls[0]
        self.assertEqual("matrixark_embedding_status", name)
        self.assertEqual("alice", args["scope"]["user_id"])
        self.assertEqual("acme", args["scope"]["tenant_id"])

    def test_the_configured_encoder_travels_with_the_counts(self) -> None:
        # A backlog means something different with no encoder configured: nothing is waiting
        # because nothing will ever be encoded, and a bare "0 pending" would read as "all done".
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/embeddings", headers=ADMIN)
        encoder = json.loads(body)["encoder"]
        self.assertFalse(encoder["semantic"])
        self.assertIn("hash fallback", encoder["note"])

        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "openai_compatible"
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = "minilm"
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/embeddings", headers=ADMIN)
        encoder = json.loads(body)["encoder"]
        self.assertTrue(encoder["semantic"])
        self.assertEqual("minilm", encoder["model"])
        self.assertEqual("", encoder["note"])

    def test_a_backend_that_cannot_answer_is_an_error_not_an_empty_backlog(self) -> None:
        # "Nothing is pending" and "I could not find out" must not look the same: the first says
        # retrieval is ready and the second says nobody knows.
        def explode(_name, _args):
            raise RuntimeError("backend down")

        self.server.call_tool = explode  # type: ignore[assignment]
        status, _, body = drive(self.app, method="GET", path="/v1/admin/embeddings", headers=ADMIN)
        self.assertGreaterEqual(status, 500)
        self.assertEqual("backend_error", json.loads(body)["error"])


class EmbeddingToolRegistrationTest(unittest.TestCase):
    def test_the_tool_is_gated_like_any_other_read_of_the_store(self) -> None:
        from matrixark_mcp_core import MATRIXARK_TOOL_SCOPES
        self.assertEqual({"context:retrieve"},
                         MATRIXARK_TOOL_SCOPES["matrixark_embedding_status"])

    def test_both_copies_of_the_scope_map_agree(self) -> None:
        # The map is duplicated in matrixark_mcp_core and matrixark_mcp_identity; a tool added to
        # one and not the other is gated differently depending on which path serves the request.
        from matrixark_mcp_core import MATRIXARK_TOOL_SCOPES as core_map
        from matrixark_mcp_identity import MATRIXARK_TOOL_SCOPES as identity_map
        self.assertEqual(core_map.get("matrixark_embedding_status"),
                         identity_map.get("matrixark_embedding_status"))

    def test_it_is_advertised_in_the_tool_schemas(self) -> None:
        from matrixark_mcp_schemas import TOOLS
        names = {tool["name"] for tool in TOOLS}
        self.assertIn("matrixark_embedding_status", names)


if __name__ == "__main__":
    unittest.main()
