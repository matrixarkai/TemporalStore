# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
import tempfile
import unittest
from pathlib import Path

from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter, _LOCAL_READ_CACHE, _LOCAL_READ_CACHE_LOCK


def clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


class MatrixArkLocalDurableReadCacheTest(unittest.TestCase):
    def test_restart_read_uses_durable_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            event_log = Path(directory) / "events.jsonl"
            adapter = MatrixArkLocalAdapter(event_log)
            adapter.append_many(
                [
                    {"record_type": "context_event", "event_id": "event-1", "content": "restart cache event"},
                    {"record_type": "context_summary", "summary_id": "summary-1", "content": "restart cache summary"},
                ]
            )

            self.assertEqual(len(adapter.read_all()), 2)
            self.assertTrue(adapter._durable_read_cache_snapshot_path().exists())

            clear_process_read_cache()
            restarted = MatrixArkLocalAdapter(event_log)

            self.assertEqual(len(restarted.read_all()), 2)
            self.assertEqual(restarted._read_cache_source, "durable")

    def test_warm_append_refreshes_durable_snapshot_for_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            event_log = Path(directory) / "events.jsonl"
            adapter = MatrixArkLocalAdapter(event_log)
            adapter.append({"record_type": "context_event", "event_id": "event-1", "content": "first event"})
            self.assertEqual(len(adapter.read_all()), 1)

            adapter.append({"record_type": "context_event", "event_id": "event-2", "content": "second event"})

            clear_process_read_cache()
            restarted = MatrixArkLocalAdapter(event_log)
            records = restarted.read_all()

            self.assertEqual(restarted._read_cache_source, "durable")
            self.assertEqual({record.get("event_id") for record in records}, {"event-1", "event-2"})


if __name__ == "__main__":
    unittest.main()
