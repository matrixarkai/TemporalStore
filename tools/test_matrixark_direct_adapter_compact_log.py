#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from matrixark_mcp_server import MatrixArkTemporalStoreDirectAdapter


class FakeTemporalStoreClient:
    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.hsets: list[tuple[str, str, str]] = []
        self.puts: list[tuple[str, str]] = []
        self.fail_next_hsets = 0

    def get_string(self, key: str) -> str:
        if key not in self.strings:
            raise KeyError(key)
        return self.strings[key]

    def put_string(self, key: str, value: str) -> None:
        self.strings[key] = value
        self.puts.append((key, value))

    def hset(self, key: str, field: str, value: str) -> None:
        if self.fail_next_hsets:
            self.fail_next_hsets -= 1
            raise RuntimeError("transient hset failure")
        self.hashes.setdefault(key, {})[field] = value
        self.hsets.append((key, field, value))

    def hget(self, key: str, field: str) -> str:
        self.hget_count = getattr(self, "hget_count", 0) + 1
        return self.hashes.get(key, {}).get(field, "")


def make_adapter(client: FakeTemporalStoreClient, prefix: str) -> MatrixArkTemporalStoreDirectAdapter:
    adapter = MatrixArkTemporalStoreDirectAdapter.__new__(MatrixArkTemporalStoreDirectAdapter)
    adapter.event_log = Path("/tmp/unused-matrixark-direct-adapter-test.jsonl")
    adapter._client = client
    adapter._storage_prefix = prefix
    adapter._record_hash_key = f"{prefix}:records"
    adapter._index_key = f"{prefix}:record_index"
    adapter._count_key = f"{prefix}:record_count"
    adapter._shard_size = 2
    adapter._index_cache = None
    adapter._records_cache = None
    adapter._legacy_index_mode = False
    import threading
    adapter._records_lock = threading.RLock()
    adapter._audit_lock = threading.RLock()
    adapter._audit_buffer = []
    adapter._audit_flusher_started = False
    adapter._audit_flush_failures = 0
    adapter._audit_mode = "deferred"
    adapter._audit_buffer_max_records = 128
    adapter._audit_flush_interval_s = 1.0
    adapter._write_retries = 3
    adapter._write_backoff_s = 0.0
    adapter._write_throttle_s = 0.0
    return adapter


def test_compact_count_log() -> None:
    client = FakeTemporalStoreClient()
    adapter = make_adapter(client, "matrixark:test")
    adapter.append({"record_type": "context_event", "text": "one"})
    adapter.append({"record_type": "context_summary", "text": "two"})

    assert client.strings["matrixark:test:record_count"] == "2"
    assert "matrixark:test:record_index" not in client.strings
    assert sorted(client.hashes["matrixark:test:records:000000"]) == [
        "00000000000000000000",
        "00000000000000000001",
    ]
    assert [record["text"] for record in adapter.read_all()] == ["one", "two"]


def test_compact_count_log_shards_large_runs() -> None:
    client = FakeTemporalStoreClient()
    adapter = make_adapter(client, "matrixark:sharded")
    for index in range(5):
        adapter.append({"record_type": "context_event", "text": f"event-{index}"})

    assert client.strings["matrixark:sharded:record_count"] == "5"
    assert sorted(client.hashes) == [
        "matrixark:sharded:records:000000",
        "matrixark:sharded:records:000001",
        "matrixark:sharded:records:000002",
    ]
    assert [record["text"] for record in adapter.read_all()] == [
        "event-0",
        "event-1",
        "event-2",
        "event-3",
        "event-4",
    ]


def test_legacy_index_log_remains_readable_and_appendable() -> None:
    client = FakeTemporalStoreClient()
    prefix = "matrixark:legacy"
    index_key = f"{prefix}:record_index"
    records_key = f"{prefix}:records"
    client.strings[index_key] = '["00000000000000000000:context_event:7"]'
    client.hashes[records_key] = {
        "00000000000000000000:context_event:7": '{"record_type":"context_event","text":"old"}'
    }
    adapter = make_adapter(client, prefix)

    assert [record["text"] for record in adapter.read_all()] == ["old"]
    adapter.append({"record_type": "context_event", "text": "new"})
    assert "record_count" not in "".join(client.strings)
    assert len(client.strings[index_key]) > 40
    assert [record["text"] for record in adapter.read_all()] == ["old", "new"]


def test_read_all_singleflight_cache_avoids_duplicate_prefix_loads() -> None:
    client = FakeTemporalStoreClient()
    adapter = make_adapter(client, "matrixark:singleflight")
    for index in range(4):
        adapter.append({"record_type": "context_event", "text": f"event-{index}"})

    first_reader = make_adapter(client, "matrixark:singleflight")
    second_reader = make_adapter(client, "matrixark:singleflight")

    assert [record["text"] for record in first_reader.read_all()] == [
        "event-0",
        "event-1",
        "event-2",
        "event-3",
    ]
    hget_count_after_first = getattr(client, "hget_count", 0)
    assert [record["text"] for record in second_reader.read_all()] == [
        "event-0",
        "event-1",
        "event-2",
        "event-3",
    ]
    assert getattr(client, "hget_count", 0) == hget_count_after_first


def test_append_many_bundles_expand_without_logical_side_effects() -> None:
    client = FakeTemporalStoreClient()
    adapter = make_adapter(client, "matrixark:bundle")
    adapter._shard_size = 32
    records = [{"record_type": "context_event", "text": f"event-{index}"} for index in range(8)]

    adapter.append_many(records)

    assert client.strings["matrixark:bundle:record_count"] == "1"
    assert len(client.hsets) == 1
    assert [record["text"] for record in adapter.read_all()] == [record["text"] for record in records]
    fresh_reader = make_adapter(client, "matrixark:bundle")
    assert [record["text"] for record in fresh_reader.read_all()] == [record["text"] for record in records]


def test_transient_hset_failure_retries_before_count_update() -> None:
    client = FakeTemporalStoreClient()
    client.fail_next_hsets = 2
    adapter = make_adapter(client, "matrixark:retry")

    adapter.append({"record_type": "context_event", "text": "after retry"})

    assert client.strings["matrixark:retry:record_count"] == "1"
    assert [record["text"] for record in adapter.read_all()] == ["after retry"]


def test_deferred_audit_does_not_write_until_flush() -> None:
    client = FakeTemporalStoreClient()
    adapter = make_adapter(client, "matrixark:audit")

    adapter.append_audit({"record_type": "context_pack_audit", "context_pack_id": "audit-1"})

    assert client.hsets == []
    assert adapter._audit_buffer
    adapter.flush_audits()
    assert client.hsets
    assert adapter.read_all()[0]["context_pack_id"] == "audit-1"


def main() -> int:
    test_compact_count_log()
    test_compact_count_log_shards_large_runs()
    test_legacy_index_log_remains_readable_and_appendable()
    test_read_all_singleflight_cache_avoids_duplicate_prefix_loads()
    test_append_many_bundles_expand_without_logical_side_effects()
    test_transient_hset_failure_retries_before_count_update()
    test_deferred_audit_does_not_write_until_flush()
    print("PASS matrixark_direct_adapter_compact_log")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
