#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The memory API on a native (record-log) backend: forget removes, history has a log, get_all
lists one entry per memory, and the fields the caller supplied on an ingest survive the rewrite
extraction performs when it commits.

Each test here fails on the unfixed tree. The adapter is driven through a stub record log rather
than a live datanode, because every defect these cover is in the Python read/write path, not in
the engine: the native branch skipped two of the three serving-pipeline stages, and three memory
methods called a JSONL-only reader that returns `[]` the moment the JSONL log is disabled -- which
is exactly what a native backend does.
"""
import json
import os
import tempfile
import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
    from tools import matrixark_mcp_local_adapter as local_mod
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters
    import matrixark_mcp_local_adapter as local_mod


TENANT = 4848243343181226175
USER = 4473490034483841169
SCOPE_KEY = f"t={TENANT}|u={USER}|s=1|"


def _event(event_id, text, *, phase="hot_path", updated_at_ms=1000, **extra):
    """A `context_event` row shaped like the ones the ingest pipeline writes."""
    record = {
        "record_type": "context_event",
        "event_id_hash": event_id,
        "text": text,
        "scope_key": SCOPE_KEY,
        "access_scope": {"tenant_hash": TENANT, "user_hash": USER, "scope_key": SCOPE_KEY},
        "extraction_phase": phase,
        "status": "observed" if phase == "hot_path" else "extraction_committed",
        "updated_at_ms": updated_at_ms,
        "timestamp_key_ms": updated_at_ms,
    }
    record.update(extra)
    return record


def _forget_tombstone(created_at_ms=9000):
    return {
        "record_type": local_mod.MEMORY_TOMBSTONE_RECORD_TYPE,
        "tombstone_kind": "forget",
        "target_tenant_hash": TENANT,
        "target_user_hash": USER,
        "target_scope_key": SCOPE_KEY,
        "removed_count": 2,
        "created_at_ms": created_at_ms,
    }


class _StubNativeAdapter:
    """Build a direct adapter whose native record log is a plain list.

    `object.__new__` skips __init__ (which would dial a metaserver / spawn a CLI); everything the
    read path touches is stubbed. This is the same construction the membership-index tests use.
    """

    @staticmethod
    def build(records):
        adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
        adapter._storage_prefix = "matrixark:mcp"
        adapter._local_jsonl_enabled = False
        adapter._native_log = list(records)
        adapter._recover_serving_from_disk_fallback_if_needed = lambda *, reason="": None
        adapter._load_latest_context_state_records = lambda: []
        # The native read exactly as the retrieval path sees it: the latest-state fold and the
        # state collapse, and nothing else. The latest-value collapse and the tombstone sweep
        # the memory API needs are applied above this, in `_read_all_compacted`, and these tests
        # go through `read_all` / `_read_all_compacted` so they exercise that seam for real.
        adapter.read_all_without_disk_fallback_recovery = (
            lambda: adapter._with_latest_context_state_records(list(adapter._native_log))
        )
        return adapter

    @staticmethod
    def retrieval_view(records):
        """What the retrieval hot path gets -- deliberately WITHOUT the memory-API stages."""
        adapter = _StubNativeAdapter.build(records)
        return adapter.read_all_without_disk_fallback_recovery()


class NativeReadPathTests(unittest.TestCase):
    def test_forget_tombstone_is_applied_on_the_native_read(self):
        """A forget wrote a durable tombstone and reported an accurate removed_count, then served
        every one of those records straight back: the native read ran only the last of the three
        serving-pipeline stages, so `apply_memory_tombstones` never ran."""
        adapter = _StubNativeAdapter.build([
            _event(11, "user: I am vegetarian.", updated_at_ms=1000),
            _event(22, "user: My favourite language is Rust.", updated_at_ms=2000),
            _forget_tombstone(),
        ])
        live = adapter.read_all()
        self.assertEqual([], [r for r in live if r.get("record_type") == "context_event"])
        # The tombstone marker itself is internal and must not surface either.
        self.assertEqual([], [r for r in live
                              if r.get("record_type") == local_mod.MEMORY_TOMBSTONE_RECORD_TYPE])

    def test_forget_does_not_remove_a_later_re_ingest(self):
        """The sweep is order-aware: re-ingesting after a forget produces live memories again."""
        adapter = _StubNativeAdapter.build([
            _event(11, "user: I am vegetarian.", updated_at_ms=1000),
            _forget_tombstone(),
            _event(33, "user: I am vegetarian.", updated_at_ms=11000),
        ])
        live_ids = [r["event_id_hash"] for r in adapter.read_all()
                    if r.get("record_type") == "context_event"]
        self.assertEqual([33], live_ids)

    def test_one_memory_serves_as_one_record_not_two(self):
        """The ingest pipeline persists an event row twice -- once on the hot path and again when
        extraction commits -- and both rows served, so get_all reported 2 ingests as 4 memories.
        Latest-value compaction collapses them to the committed row."""
        adapter = _StubNativeAdapter.build([
            _event(11, "user: I am vegetarian.", phase="hot_path", updated_at_ms=1000),
            _event(11, "user: I am vegetarian.", phase="final", updated_at_ms=1500),
            _event(22, "user: My favourite language is Rust.", phase="hot_path", updated_at_ms=2000),
            _event(22, "user: My favourite language is Rust.", phase="final", updated_at_ms=2500),
        ])
        events = [r for r in adapter.read_all() if r.get("record_type") == "context_event"]
        self.assertEqual(2, len(events))
        self.assertEqual([11, 22], sorted(r["event_id_hash"] for r in events))
        self.assertEqual({"final"}, {r["extraction_phase"] for r in events})

    def test_the_memory_stages_stay_off_the_retrieval_hot_path(self):
        """`_with_latest_context_state_records` runs on retrieval too, so the latest-value
        collapse and the tombstone sweep are applied one level up instead.

        Applying them at that choke point changes the candidate set retrieval hands the proxy,
        and measured against a 16-memory store the proxy then wedged -- the sixth retrieve stopped
        responding for 120s and every later one was rejected on the pack lane after 40s, taking
        the gateway's whole data path with it. Split, the same store answers 12/12 retrieves in
        165-322ms. If someone moves the stages back down, this fails."""
        records = [
            _event(11, "user: I am vegetarian.", phase="hot_path", updated_at_ms=1000),
            _event(11, "user: I am vegetarian.", phase="final", updated_at_ms=1500),
            _forget_tombstone(),
        ]
        hot_path = _StubNativeAdapter.retrieval_view(records)
        # Untouched by the memory stages: both event rows, and the tombstone, still present.
        self.assertEqual(2, len([r for r in hot_path if r.get("record_type") == "context_event"]))
        self.assertEqual(1, len([r for r in hot_path
                                 if r.get("record_type") == local_mod.MEMORY_TOMBSTONE_RECORD_TYPE]))
        # The memory API, one level up, still sees the swept view.
        self.assertEqual([], [r for r in _StubNativeAdapter.build(records).read_all()
                              if r.get("record_type") == "context_event"])

    def test_compacted_view_keeps_expired_records_for_the_sweep(self):
        """`_read_all_compacted` is the seam below the live view: the expiry sweep has to SEE an
        expired record to tombstone it, so only `read_all` may filter by expiry."""
        expired = _event(11, "user: Use gate B12 today.", expires_at_ms=1, ephemeral=True)
        adapter = _StubNativeAdapter.build([expired])
        self.assertEqual(
            [11], [r["event_id_hash"] for r in adapter._read_all_compacted()
                   if r.get("record_type") == "context_event"])
        self.assertEqual(
            [], [r for r in adapter.read_all() if r.get("record_type") == "context_event"])


class NativeRawLogReadTests(unittest.TestCase):
    """`history` reads the RAW log on purpose -- a memory's change history IS the tombstones and
    superseded rows the live view exists to hide. The inherited reader is JSONL-only, so on a
    native backend history reported an empty log for a memory that plainly had one."""

    @staticmethod
    def _adapter(records):
        adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
        adapter._local_jsonl_enabled = False
        adapter._recover_serving_from_disk_fallback_if_needed = lambda *, reason="": None
        adapter._records_lock = __import__("threading").RLock()
        adapter._get_count = lambda: len(records)
        adapter._load_records_by_count = lambda count: list(records[:count])
        adapter._load_records = lambda index: list(records)
        adapter._get_index = lambda: []
        return adapter

    def test_raw_records_read_the_native_log(self):
        records = [_event(11, "user: I am vegetarian."), _forget_tombstone()]
        self.assertEqual(2, len(self._adapter(records)._read_raw_records()))

    def test_raw_records_expand_a_bundled_append(self):
        """A bundled append stores several records as ONE hash field; the wrapper carries no
        record_type, which is what every reader filters on."""
        bundled = [{"record_bundle": [_event(11, "user: a"), _event(22, "user: b")]}]
        raw = self._adapter(bundled)._read_raw_records()
        self.assertEqual([11, 22], [r["event_id_hash"] for r in raw])

    def test_history_reports_ingest_and_supersede_from_the_raw_log(self):
        adapter = self._adapter([
            _event(11, "user: I am vegetarian."),
            {
                "record_type": local_mod.MEMORY_TOMBSTONE_RECORD_TYPE,
                "tombstone_kind": "delete",
                "target_memory_id": "11",
                "tombstone_reason": "supersede",
                "superseded_by": 22,
                "created_at_ms": 5000,
            },
        ])
        history = adapter.history({"memory_id": "11"})
        self.assertEqual(2, history["count"])
        self.assertEqual(["ingested", "superseded"], [entry["event"] for entry in history["history"]])
        self.assertEqual(22, history["history"][1]["superseded_by"])


class CallerSuppliedFieldsSurviveExtractionTests(unittest.TestCase):
    """`identity_key` / TTL come from the CALLER; extraction cannot rebuild them. When extraction
    commits it rewrites the event row for an id that already exists and latest-value compaction
    serves that newer row, so anything the extractor does not reproduce was silently dropped at
    read time -- keyed recall 404'd and a TTL record never expired. They survived only when
    extraction happened to run inside the ingest call, which is why an in-process sync ingest
    looked correct while the same request through the gateway did not."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.adapter = local_mod.MatrixArkLocalAdapter(
            __import__("pathlib").Path(self._dir.name) / "events.jsonl")

    def tearDown(self):
        self._dir.cleanup()

    def _scope(self):
        return {"tenant_id": "t1", "user_id": "u1", "session_id": "s1"}

    def _live_event(self, event_id):
        for record in self.adapter.read_all():
            if (str(record.get("record_type") or "") == "context_event"
                    and str(record.get("event_id_hash")) == str(event_id)):
                return record
        return None

    def test_identity_key_survives_a_separate_extraction_commit(self):
        result = self.adapter.ingest({
            "scope": self._scope(),
            "messages": [{"role": "user", "content": "Preferred seat: aisle"}],
            "identity_key": "pref.seat",
            "truth_class": "asserted",
        })
        event_id = result["event_id_hash"]
        self.assertEqual("pref.seat", self._live_event(event_id).get("identity_key"))
        # Commit extraction OUTSIDE the ingest call -- what a finalize through the gateway does.
        self.adapter.batch_extract({
            "scope": self._scope(),
            "messages": [{"role": "user", "content": "Preferred seat: aisle"}],
            "derive_from_existing_events": True,
            "source_event_ids": [event_id],
            "extraction_phase": "final",
            "force": True,
        })
        served = self._live_event(event_id)
        self.assertIsNotNone(served, "the event must still serve after extraction commits")
        self.assertEqual("pref.seat", served.get("identity_key"))
        self.assertEqual("asserted", served.get("truth_class"))

    def test_ttl_survives_a_separate_extraction_commit(self):
        result = self.adapter.ingest({
            "scope": self._scope(),
            "messages": [{"role": "user", "content": "Use gate B12 today"}],
            "ttl_seconds": 3600,
        })
        event_id = result["event_id_hash"]
        expires_at_ms = self._live_event(event_id).get("expires_at_ms")
        self.assertTrue(expires_at_ms, "the ingest must stamp an expiry")
        self.adapter.batch_extract({
            "scope": self._scope(),
            "messages": [{"role": "user", "content": "Use gate B12 today"}],
            "derive_from_existing_events": True,
            "source_event_ids": [event_id],
            "extraction_phase": "final",
            "force": True,
        })
        served = self._live_event(event_id)
        self.assertIsNotNone(served)
        self.assertEqual(expires_at_ms, served.get("expires_at_ms"))
        self.assertTrue(served.get("ephemeral"))


class NativeWritePathStampTests(unittest.TestCase):
    """The native writer built its own record batch and skipped the per-ingestion stamp the
    JSONL writer applies, so those fields never reached the store at all."""

    def test_append_many_stamps_like_the_jsonl_writer(self):
        adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
        adapter._local_jsonl_enabled = False
        written = []
        adapter._append_many_materialized = lambda records, allow_queue=True: written.extend(records)
        adapter._queue_batched_records = lambda records: False
        adapter._update_latest_entity_cache = lambda records: None
        adapter._maintain_event_membership_after_append = lambda records: None
        adapter._push_ingest_stamp({"identity_key": "pref.seat", "truth_class": "asserted"})
        try:
            adapter.append_many([_event(11, "user: Preferred seat: aisle")])
        finally:
            adapter._pop_ingest_stamp()
        events = [r for r in written if r.get("record_type") == "context_event"]
        self.assertEqual(1, len(events))
        self.assertEqual("pref.seat", events[0].get("identity_key"))
        self.assertEqual("asserted", events[0].get("truth_class"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
