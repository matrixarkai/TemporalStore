#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Event-membership index -> complete (orphan-free) delete/update, O(1)/O(k) fast path.

Covers the delete-completeness gap: after ``delete(anchor_event_id)`` the served memory was removed,
but the deleted records' OWN embeddings (``context_embedding`` ref_type entity/summary/segment) and
secondary-index postings (``context_index`` any ref_type -- including the anchor's own event postings)
survived as orphans, permanently bloating the vector/index space (they were not tombstoned, so purge
could not reclaim them either).

The fix builds an event-membership index ``event_id_hash -> {member identity hashes}`` (the transitive
closure event -> derivatives -> their embeddings/postings, the last covered by ref_hash membership).
That member set is carried on the delete tombstone (``closure_ref_ids``) and swept by
``_tombstone_kills_record`` regardless of ref_type; the serving pipeline applies tombstones BEFORE the
posting-compaction rebuild so the order-aware sweep is not defeated by rebuilt postings relocating past
the tombstone. The index is the authoritative O(1) member enumeration (in-memory locally, a durable
engine hash on the backend); a provenance scan is the correctness fallback.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp
import matrixark_mcp_local_adapter as A


def _scope(user: str = "carol", *, tenant: str = "tenant_mem", session: str = "s1") -> dict:
    return {"account_id": "acct_local", "tenant_id": tenant, "user_id": user,
            "session_id": session, "agent_name": "t"}


_SECONDARY_TYPES = ("context_embedding", "context_index")


def _referenced_ids(record: dict) -> set[str]:
    """Every id an embedding / index posting points at (ref_hash + ref_hashes elements)."""
    ids: set[str] = set()
    ref_hash = record.get("ref_hash")
    if ref_hash not in (None, ""):
        ids.add(str(ref_hash))
    ref_hashes = record.get("ref_hashes")
    if isinstance(ref_hashes, list):
        ids.update(str(x) for x in ref_hashes)
    return ids


def _deleted_identity_set(raw_before: list[dict], surviving: list[dict], anchor: str) -> set[str]:
    """The set of identity hashes (event + closure-removed derivatives) that the delete removed --
    computed as ground truth from what disappeared between the pre-delete raw log and the surviving
    view, mirroring the orphan-detection logic."""
    def key(r: dict) -> tuple:
        return (str(r.get("record_type")), str(r.get("event_id_hash")), str(r.get("entity_hash")),
                str(r.get("summary_hash")), str(r.get("segment_hash")), str(r.get("ref_type")),
                str(r.get("ref_hash")), str(r.get("ref_hashes")))
    surv_keys = {key(r) for r in surviving}
    deleted: set[str] = {str(anchor)}
    for r in raw_before:
        if str(r.get("record_type")) == A.MEMORY_TOMBSTONE_RECORD_TYPE:
            continue
        if key(r) in surv_keys:
            continue
        for field in ("event_id_hash", "entity_hash", "summary_hash", "segment_hash"):
            v = r.get(field)
            if v not in (None, ""):
                deleted.add(str(v))
    return deleted


def _orphans(surviving: list[dict], deleted_ids: set[str]) -> list[dict]:
    """Surviving embeddings / index postings whose ref target is a deleted id."""
    out = []
    for r in surviving:
        if str(r.get("record_type")) in _SECONDARY_TYPES and (_referenced_ids(r) & deleted_ids):
            out.append(r)
    return out


class MembershipIndexCase(unittest.TestCase):
    def _server(self, tmp: str):
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        self.addCleanup(server.close, timeout_s=1.0)
        return adapter, server

    def _ingest(self, server, content: str, user: str = "carol") -> str:
        res = server.call_tool("matrixark_ingest",
                               {"messages": [{"role": "user", "content": content}], "scope": _scope(user)})
        return res.get("event_id_hash")

    # -- 1. member set matches a ground-truth scan ------------------------------------------------
    def test_member_set_matches_ground_truth(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = self._ingest(server, "Carol loves espresso and hiking in the Alps with dog Rex")
            index = adapter._ensure_event_member_index()
            members = index.get(str(anchor), set())
            # Ground truth: event id + every derivative identity whose provenance includes the anchor.
            ground = {str(anchor)}
            anchor_int = int(anchor)
            for r in adapter.read_all():
                prov = A._record_provenance_source_ids(r)
                if prov and anchor_int in prov:
                    ground |= A._record_derivative_identity_ids(r)
            self.assertEqual(members, ground)
            self.assertIn(str(anchor), members)
            # Every surviving embedding/posting for this event references a member id (coverage).
            for r in adapter.read_all():
                if str(r.get("record_type")) in _SECONDARY_TYPES:
                    refs = _referenced_ids(r)
                    if str(anchor) in refs:
                        self.assertTrue(refs & members)

    # -- 2. delete -> zero orphans ----------------------------------------------------------------
    def test_delete_leaves_zero_orphans(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = self._ingest(server, "Carol loves espresso and hiking in the Alps with dog Rex")
            raw_before = A.apply_memory_tombstones(adapter._read_raw_records())
            res = server.call_tool("matrixark_delete", {"memory_id": anchor, "scope": _scope()})
            self.assertTrue(res["deleted"])
            surviving = adapter.read_all()
            deleted_ids = _deleted_identity_set(raw_before, surviving, anchor)
            self.assertEqual([], _orphans(surviving, deleted_ids),
                             f"orphan embeddings/index survived: {_orphans(surviving, deleted_ids)}")
            # get_all is empty; no context_embedding/context_index references the deleted anchor.
            self.assertEqual(0, server.call_tool("matrixark_get_all", {"scope": _scope()})["count"])
            for r in surviving:
                if str(r.get("record_type")) in _SECONDARY_TYPES:
                    self.assertNotIn(str(anchor), _referenced_ids(r))

    # -- 3. delete takes the index fast path (no scan) --------------------------------------------
    def test_delete_uses_index_fast_path(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = self._ingest(server, "Carol loves espresso and hiking")
            hits_before = adapter._event_member_index_hits
            res = server.call_tool("matrixark_delete", {"memory_id": anchor, "scope": _scope()})
            self.assertEqual("index_memory", res["member_source"])
            self.assertGreater(adapter._event_member_index_hits, hits_before)

    # -- 4. multi-source derivative: demote on one delete, gone on both ---------------------------
    def test_multi_source_entity_demotes_then_deletes(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            a = self._ingest(server, "Alice met Bob at the conference in Berlin")
            b = self._ingest(server, "Bob presented the keynote in Berlin")
            a_int, b_int = int(a), int(b)
            shared_entity = 999000111  # synthetic shared entity hash
            scope_key = None
            for r in adapter.read_all():
                if str(r.get("record_type")) == "context_event" and str(r.get("event_id_hash")) == a:
                    scope_key = r.get("scope_key")
                    break
            # A multi-source entity built from BOTH events, plus its own embedding + index posting.
            adapter.append_many([
                {"record_type": "context_entity", "entity_hash": shared_entity,
                 "source_event_ids": [a_int, b_int], "scope_key": scope_key,
                 "entity_name": "Bob", "updated_at_ms": A.now_ms()},
                {"record_type": "context_embedding", "ref_type": "entity", "ref_hash": shared_entity,
                 "embedding_type": "entity", "scope_key": scope_key, "updated_at_ms": A.now_ms()},
                {"record_type": "context_index", "ref_type": "entity", "ref_hash": shared_entity,
                 "ref_hashes": [shared_entity], "index_name": "entity_name", "scope_key": scope_key,
                 "data_model": "entity", "updated_at_ms": A.now_ms()},
            ])

            def entity_alive() -> bool:
                return any(str(r.get("record_type")) == "context_entity"
                           and str(r.get("entity_hash")) == str(shared_entity) for r in adapter.read_all())

            def entity_secondary_alive() -> bool:
                return any(str(r.get("record_type")) in _SECONDARY_TYPES
                           and str(shared_entity) in _referenced_ids(r) for r in adapter.read_all())

            self.assertTrue(entity_alive())
            # Delete A -> entity DEMOTED (survives, A trimmed), its embedding/posting survive.
            server.call_tool("matrixark_delete", {"memory_id": a, "scope": _scope()})
            self.assertTrue(entity_alive(), "shared entity must survive (demote) while B is live")
            self.assertTrue(entity_secondary_alive(), "shared entity's embedding/posting must survive")
            surviving = adapter.read_all()
            demoted = [r for r in surviving if str(r.get("record_type")) == "context_entity"
                       and str(r.get("entity_hash")) == str(shared_entity)]
            self.assertEqual(1, len(demoted))
            self.assertNotIn(a_int, demoted[0].get("source_event_ids", []))
            self.assertIn(b_int, demoted[0].get("source_event_ids", []))
            # Delete B -> entity now single-source, fully gone; its embedding/posting swept.
            server.call_tool("matrixark_delete", {"memory_id": b, "scope": _scope()})
            self.assertFalse(entity_alive(), "entity must be gone once its last source event is deleted")
            self.assertFalse(entity_secondary_alive(), "entity's embedding/posting must be swept with it")

    # -- 5. index + delete survive an adapter reload ----------------------------------------------
    def test_delete_survives_reload(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(path)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            anchor = self._ingest(server, "Carol loves espresso and hiking in the Alps")
            server.call_tool("matrixark_delete", {"memory_id": anchor, "scope": _scope()})
            server.close(timeout_s=1.0)
            # Fresh adapter over the same log: index rebuilds from the durable tombstoned view.
            adapter2 = mcp.MatrixArkLocalAdapter(path)
            server2 = mcp.MatrixArkMcpServer(adapter2, access_mode="dev")
            self.addCleanup(server2.close, timeout_s=1.0)
            surviving = adapter2.read_all()
            for r in surviving:
                if str(r.get("record_type")) in _SECONDARY_TYPES:
                    self.assertNotIn(str(anchor), _referenced_ids(r))
            self.assertEqual(0, server2.call_tool("matrixark_get_all", {"scope": _scope()})["count"])

    # -- 6. no retrieval / vector leak of the deleted content -------------------------------------
    def test_no_retrieval_leak_after_delete(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = self._ingest(server, "Carol's secret passphrase is orange-elephant-42")
            server.call_tool("matrixark_delete", {"memory_id": anchor, "scope": _scope()})
            retrieved = server.call_tool("matrixark_retrieve",
                                         {"query": "orange elephant passphrase", "scope": _scope()})
            self.assertTrue(retrieved.get("insufficient_context", False) or not retrieved.get("groups"))
            # No surviving embedding/index posting references the deleted anchor (no vector leak surface).
            for r in adapter.read_all():
                if str(r.get("record_type")) in _SECONDARY_TYPES:
                    self.assertNotIn(str(anchor), _referenced_ids(r))

    # -- 7. purge reclaims the newly-tombstoned members -------------------------------------------
    def test_purge_reclaims_orphans(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = self._ingest(server, "Carol loves espresso and hiking in the Alps with dog Rex")
            server.call_tool("matrixark_delete", {"memory_id": anchor, "scope": _scope()})
            before = len(adapter._read_raw_records())
            purge = adapter.purge_tombstones(force=True)
            self.assertTrue(purge["purged"])
            after_raw = adapter._read_raw_records()
            self.assertLess(len(after_raw), before)  # log shrank
            # No tombstone markers remain, and zero orphans / anchor refs remain post-purge.
            self.assertFalse(any(str(r.get("record_type")) == A.MEMORY_TOMBSTONE_RECORD_TYPE for r in after_raw))
            for r in adapter.read_all():
                if str(r.get("record_type")) in _SECONDARY_TYPES:
                    self.assertNotIn(str(anchor), _referenced_ids(r))

    # -- 8. delete reads the durable persisted member set when present (engine wiring) ------------
    def test_delete_prefers_persisted_members(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = self._ingest(server, "Carol loves espresso and hiking")
            index_members = adapter._ensure_event_member_index().get(str(anchor), set())
            # Simulate the engine's durable hash by overriding the persistence-lookup seam.
            seen = {}

            def fake_lookup(event_id, _members=index_members, _seen=seen):
                _seen["called"] = event_id
                return set(_members)

            adapter._lookup_persisted_event_members = fake_lookup  # type: ignore[assignment]
            res = server.call_tool("matrixark_delete", {"memory_id": anchor, "scope": _scope()})
            self.assertEqual("index_persisted", res["member_source"])
            self.assertEqual(str(anchor), seen.get("called"))
            surviving = adapter.read_all()
            for r in surviving:
                if str(r.get("record_type")) in _SECONDARY_TYPES:
                    self.assertNotIn(str(anchor), _referenced_ids(r))


class EnginePersistenceMethodsCase(unittest.TestCase):
    """Unit-test the engine adapter's event_members hash read/write/forget seam directly against a
    fake client (a live TemporalStore backend is not runnable in CI -- documented E2E gap)."""

    def _engine_stub(self):
        import matrixark_mcp_temporal_adapters as eng

        class FakeClient:
            def __init__(self):
                self.store: dict[tuple[str, str], str] = {}

            def hset(self, key, field, value):
                self.store[(key, field)] = value

            def hget(self, key, field):
                return self.store.get((key, field), "")

        obj = object.__new__(eng.MatrixArkTemporalStoreDirectAdapter)
        obj._storage_prefix = "matrixark:mcp"
        obj._client = FakeClient()
        # Bind a direct hset (bypass the backoff/throttle machinery not initialized on the stub).
        obj._hset_with_backoff = lambda key, field, value: obj._client.hset(key, field, value)
        return obj, eng

    def test_persist_lookup_forget_roundtrip(self):
        obj, _eng = self._engine_stub()
        self.assertIsNone(obj._lookup_persisted_event_members("42"))
        obj._persist_event_members("42", {"42", "1001", "2002"})
        self.assertEqual({"42", "1001", "2002"}, obj._lookup_persisted_event_members("42"))
        # Key/field shape is the documented {prefix}:event_members hash.
        self.assertIn(("matrixark:mcp:event_members", "42"), obj._client.store)
        obj._forget_persisted_event_members("42")
        self.assertIsNone(obj._lookup_persisted_event_members("42"))

    def test_maintain_writes_hash_on_append(self):
        obj, eng = self._engine_stub()
        # Stub the base (in-memory) maintenance so we exercise only the durable write-through.
        obj._invalidate_event_member_index = lambda: None
        anchor, entity = 700, 800
        obj._maintain_event_membership_after_append([
            {"record_type": "context_event", "event_id_hash": anchor},
            {"record_type": "context_entity", "entity_hash": entity, "source_event_ids": [anchor]},
        ])
        members = obj._lookup_persisted_event_members(str(anchor))
        self.assertEqual({str(anchor), str(entity)}, members)


class BatchedMembershipWriteThroughCase(unittest.TestCase):
    """The batched read/write path, which the FakeClient above never reaches.

    That client exposes only hget/hset, so `batch_hget`/`batch_hset` raise AttributeError,
    the adapter falls back to per-key access, and the batched code is never executed. A
    client that DOES batch is therefore the only way these lines are covered.
    """

    def _engine_stub(self, answer_fields=None):
        import matrixark_mcp_temporal_adapters as eng

        class BatchingFakeClient:
            def __init__(self):
                self.store: dict[tuple[str, str], str] = {}
                self.batch_gets = 0
                self.batch_sets = 0
                self.single_gets = 0
                self.single_sets = 0

            def hset(self, key, field, value):
                self.single_sets += 1
                self.store[(key, field)] = value

            def hget(self, key, field):
                self.single_gets += 1
                return self.store.get((key, field), "")

            def batch_hget(self, entries):
                self.batch_gets += 1
                rows = []
                for entry in entries:
                    key, field = str(entry.get("key")), str(entry.get("field"))
                    # answer_fields=None means answer everything (the normal case).
                    if answer_fields is not None and field not in answer_fields:
                        continue
                    rows.append({"key": key, "field": field,
                                 "value": self.store.get((key, field), "")})
                return rows

            def batch_hset(self, entries):
                self.batch_sets += 1
                for entry in entries:
                    self.store[(str(entry.get("key")), str(entry.get("field")))] = str(
                        entry.get("value"))

        obj = object.__new__(eng.MatrixArkTemporalStoreDirectAdapter)
        obj._storage_prefix = "matrixark:mcp"
        obj._client = BatchingFakeClient()
        obj._hset_with_backoff = lambda key, field, value: obj._client.hset(key, field, value)
        obj._invalidate_event_member_index = lambda: None
        return obj, eng

    def _records(self, anchor, entity):
        return [
            {"record_type": "context_event", "event_id_hash": anchor},
            {"record_type": "context_entity", "entity_hash": entity,
             "source_event_ids": [anchor]},
        ]

    def test_batch_path_is_taken_and_membership_matches(self):
        obj, _eng = self._engine_stub()
        anchor, entity = 700, 800
        obj._maintain_event_membership_after_append(self._records(anchor, entity))
        self.assertEqual({str(anchor), str(entity)},
                         obj._lookup_persisted_event_members(str(anchor)))
        # The whole point of the change: batched, not per-key.
        self.assertEqual(1, obj._client.batch_gets)
        self.assertEqual(1, obj._client.batch_sets)
        self.assertEqual(0, obj._client.single_sets)

    def test_one_round_trip_each_regardless_of_event_count(self):
        obj, _eng = self._engine_stub()
        records = []
        for i in range(25):
            records.extend(self._records(1000 + i, 2000 + i))
        obj._maintain_event_membership_after_append(records)
        self.assertEqual(1, obj._client.batch_gets)
        self.assertEqual(1, obj._client.batch_sets)
        for i in range(25):
            self.assertEqual({str(1000 + i), str(2000 + i)},
                             obj._lookup_persisted_event_members(str(1000 + i)))

    def test_membership_is_cumulative_across_appends(self):
        obj, _eng = self._engine_stub()
        anchor = 700
        obj._maintain_event_membership_after_append(self._records(anchor, 800))
        obj._maintain_event_membership_after_append([
            {"record_type": "context_entity", "entity_hash": 900,
             "source_event_ids": [anchor]},
        ])
        self.assertEqual({"700", "800", "900"},
                         obj._lookup_persisted_event_members(str(anchor)))

    def test_partial_batch_response_cannot_shrink_a_member_set(self):
        """A field the batch does not answer for must be read singly, not assumed absent.

        Assuming absent would union new members onto an EMPTY set and write back a smaller
        set than what is stored -- silently losing membership, which a later delete needs.
        """
        obj, _eng = self._engine_stub(answer_fields=set())  # batch answers nothing
        anchor = 700
        obj._maintain_event_membership_after_append(self._records(anchor, 800))
        before = obj._lookup_persisted_event_members(str(anchor))
        self.assertEqual({"700", "800"}, before)
        # Second append, with the batch still refusing to answer for this field.
        obj._maintain_event_membership_after_append([
            {"record_type": "context_entity", "entity_hash": 900,
             "source_event_ids": [anchor]},
        ])
        self.assertEqual({"700", "800", "900"},
                         obj._lookup_persisted_event_members(str(anchor)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
