#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Batch and synchronous-ingest contracts for the LLM-context Python client."""

from __future__ import annotations

import unittest

from temporalstore.llm_context import mem0


class _Recorder:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, base_url, api_key, path, body, timeout):
        self.calls.append((path, body))
        if path.endswith("update"):
            return {"updated": len(body["memories"]), "failed": []}
        if path.endswith("delete"):
            return {"deleted": len(body["memories"]), "failed": []}
        return {"status": "accepted", "event_id_hash": 17}


class MemoryBatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original = mem0._post_json
        self.addCleanup(lambda: setattr(mem0, "_post_json", self.original))
        self.recorder = _Recorder()
        mem0._post_json = self.recorder
        self.client = object.__new__(mem0.Memory)
        self.client._base_url = "http://store"
        self.client._api_key = None
        self.client._timeout = 30

    def test_batch_update_uses_one_bounded_request(self) -> None:
        entries = [
            {"memory_id": "m1", "text": "Updated"},
            {"memory_id": "m2", "metadata": {"verified": True}},
        ]
        result = self.client.batch_update(entries)
        self.assertEqual(2, result["updated"])
        self.assertEqual([("/memories/batch/update", {"memories": entries})],
                         self.recorder.calls)

    def test_batch_delete_uses_one_bounded_request(self) -> None:
        entries = [{"memory_id": "m1"}, "m2"]
        result = self.client.batch_delete(entries)
        self.assertEqual(2, result["deleted"])
        self.assertEqual([("/memories/batch/delete", {"memories": entries})],
                         self.recorder.calls)

    def test_update_forwards_provider_and_metadata(self) -> None:
        original = mem0._put_json
        self.addCleanup(lambda: setattr(mem0, "_put_json", original))
        mem0._put_json = self.recorder
        provider = {"provider_kind": "ollama", "embedding_model": "all-minilm"}
        self.client.update("m1", "Updated", provider=provider, metadata={"verified": True})
        self.assertEqual([("/memories/m1", {
            "memory_id": "m1", "data": "Updated", "provider": provider,
            "metadata": {"verified": True},
        })], self.recorder.calls)

    def test_batch_update_forwards_provider_without_rewriting_entries(self) -> None:
        provider = {"provider_kind": "ollama", "embedding_model": "all-minilm"}
        override = {"provider_kind": "ollama", "embedding_model": "second-encoder"}
        entries = [{"memory_id": "m1", "text": "Updated", "provider": override}]
        self.client.batch_update(entries, provider=provider)
        self.assertEqual([("/memories/batch/update", {
            "memories": entries, "provider": provider,
        })], self.recorder.calls)
        self.assertIs(entries[0]["provider"], override)

    def test_batches_refuse_more_than_the_limit(self) -> None:
        entries = [{"memory_id": str(i)} for i in range(mem0.MEM0_BATCH_LIMIT + 1)]
        with self.assertRaises(ValueError):
            self.client.batch_update(entries)
        with self.assertRaises(ValueError):
            self.client.batch_delete(entries)

    def test_add_finalizes_by_default(self) -> None:
        result = self.client.add("Remember this", user_id="u1")
        path, body = self.recorder.calls[0]
        self.assertEqual("/memories", path)
        self.assertTrue(body["finalize"])
        self.assertEqual("17", result["results"][0]["id"])


if __name__ == "__main__":
    unittest.main()
