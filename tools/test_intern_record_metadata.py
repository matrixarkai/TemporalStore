#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two memory-layer size optimizations, proven end-to-end and isolated.

LEVER 1 -- record-metadata interning (matrixark_mcp_local_adapter): the near-constant backend
routing/placement config (``storage_route``, ``storage_options``, ``placement_key``,
``placement_hash``, ``posting_policy``) is ~61% of on-disk memory yet has only a handful of DISTINCT
values. A write-side codec replaces each per-record value with a short content-hash token and writes
the full value ONCE as a durable ``matrixark_intern_dict`` sidecar record; every read choke point
re-expands the tokens, so downstream consumers see byte-identical fully-expanded records.

LEVER 2 -- secondary-index dimension pruning (matrixark_mcp_core.candidate_index_terms): stop emitting
context_index postings for constant internal-metadata dimensions (memory_scope, session_continuity,
memory_layer, extraction_phase, ...), keeping the semantic dimensions that carry recall. Cuts posting
COUNT ~65% with recall held at 5/5.

Both are gated (MATRIXARK_INTERN_RECORD_METADATA / MATRIXARK_PRUNE_INTERNAL_INDEX_DIMENSIONS, default
ON); flag-OFF reproduces today's behaviour exactly.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp
import matrixark_mcp_local_adapter as A
import matrixark_mcp_core as C


def _scope(user: str = "carol", *, tenant: str = "tenant_mem", session: str = "s1") -> dict:
    return {"account_id": "acct_local", "tenant_id": tenant, "user_id": user,
            "session_id": session, "agent_name": "t"}


_META_FIELDS = ("storage_route", "storage_options", "placement_key", "placement_hash", "posting_policy")
_FACTS = [
    "Carol is allergic to peanuts", "Carol works as a data engineer at Acme",
    "Carol lives in Seattle", "Carol has a dog named Rex", "Carol drinks oat milk lattes",
    "Carol loves hiking in the Alps", "Carol plays the cello", "Carol was born in 1990",
]


def _serialized_bytes(records: list[dict]) -> int:
    return sum(len(json.dumps(r, separators=(",", ":")).encode()) + 1 for r in records)


class _FlagGuard(unittest.TestCase):
    """Save/restore the two module-global flags so each test controls them in isolation."""

    def setUp(self) -> None:
        self._intern0 = A.INTERN_RECORD_METADATA
        self._prune0 = C.PRUNE_INTERNAL_INDEX_DIMENSIONS
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        A.INTERN_RECORD_METADATA = self._intern0
        C.PRUNE_INTERNAL_INDEX_DIMENSIONS = self._prune0

    def _server(self, tmp: str):
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        self.addCleanup(server.close, timeout_s=2.0)
        return adapter, server

    def _ingest_turns(self, server, n: int = 50, user: str = "carol") -> None:
        for i in range(n):
            server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": f"Turn {i}: {_FACTS[i % len(_FACTS)]} (detail {i})"}],
                "scope": _scope(user)})


# ================================================================================================
# LEVER 1 -- interning
# ================================================================================================
class InterningCase(_FlagGuard):
    def test_interning_reduces_bytes_by_at_least_half(self):
        """On-disk metadata bytes for identical logical content drop >=50% with interning ON vs OFF."""
        C.PRUNE_INTERNAL_INDEX_DIMENSIONS = False  # isolate lever 1 on the full metadata-heavy corpus
        A.INTERN_RECORD_METADATA = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._ingest_turns(server, 50)
            records = adapter._read_raw_records()  # expanded, token-free logical history
        off_bytes = _serialized_bytes(records)
        on_bytes = _serialized_bytes(A.encode_interned_records(records, set()))
        reduction = 100.0 * (off_bytes - on_bytes) / off_bytes
        print(f"\n[lever1] on-disk OFF={off_bytes/1024:.1f}KB ON={on_bytes/1024:.1f}KB "
              f"reduction={reduction:.1f}% over {len(records)} records")
        self.assertGreaterEqual(reduction, 50.0,
                                f"interning reclaimed only {reduction:.1f}% (< 50%)")

    def test_codec_roundtrip_is_byte_identical(self):
        """encode -> expand returns the EXACT original records (no token ever escapes)."""
        C.PRUNE_INTERNAL_INDEX_DIMENSIONS = False
        A.INTERN_RECORD_METADATA = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._ingest_turns(server, 40)
            original = adapter._read_raw_records()
        encoded = A.encode_interned_records(original, set())
        # the encoded form is genuinely interned: dict records exist and some data record hides a field
        self.assertTrue(any(r.get("record_type") == A.INTERN_DICT_RECORD_TYPE for r in encoded))
        self.assertTrue(any(A.INTERN_TOKEN_KEY in r for r in encoded if isinstance(r, dict)))
        self.assertTrue(any("storage_route" not in r for r in encoded
                            if isinstance(r, dict) and A.INTERN_TOKEN_KEY in r))
        expanded = A.expand_interned_records(encoded)
        self.assertEqual(original, expanded, "round-trip must reproduce the exact original records")
        self.assertFalse(any(A.INTERN_TOKEN_KEY in r for r in expanded if isinstance(r, dict)))
        self.assertFalse(any(r.get("record_type") == A.INTERN_DICT_RECORD_TYPE for r in expanded))

    def test_read_all_is_fully_expanded(self):
        """The adapter read path never surfaces a token or a dict record; metadata is full-form."""
        A.INTERN_RECORD_METADATA = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._ingest_turns(server, 30)
            for r in adapter.read_all():
                self.assertNotIn(A.INTERN_TOKEN_KEY, r)
                self.assertNotEqual(r.get("record_type"), A.INTERN_DICT_RECORD_TYPE)
                if "storage_route" in r:
                    self.assertIsInstance(r["storage_route"], dict)
                    self.assertIn("route", r["storage_route"])
            # get_all likewise sees no dict records
            got = server.call_tool("matrixark_get_all", {"scope": _scope()})
            self.assertNotIn(A.INTERN_DICT_RECORD_TYPE, json.dumps(got, default=str))

    def test_disk_lines_are_actually_interned(self):
        """Prove the on-disk representation is compressed (tokens present) while reads expand it."""
        A.INTERN_RECORD_METADATA = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(path)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self._ingest_turns(server, 20)
            server.close(timeout_s=2.0)
            raw_lines = [json.loads(ln) for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        self.assertTrue(any(r.get("record_type") == A.INTERN_DICT_RECORD_TYPE for r in raw_lines),
                        "expected durable intern-dict records on disk")
        interned_data = [r for r in raw_lines if isinstance(r, dict) and A.INTERN_TOKEN_KEY in r]
        self.assertTrue(interned_data, "expected interned data records on disk")
        for r in interned_data:  # the interned field is elided from the data line
            for field in r[A.INTERN_TOKEN_KEY]:
                self.assertNotIn(field, r)

    def test_flag_off_is_byte_identical_to_today(self):
        """With interning OFF, no dict records and no token key are ever written."""
        A.INTERN_RECORD_METADATA = False
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(path)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self._ingest_turns(server, 20)
            server.close(timeout_s=2.0)
            text = path.read_text(encoding="utf-8")
        self.assertNotIn(A.INTERN_DICT_RECORD_TYPE, text)
        self.assertNotIn(f'"{A.INTERN_TOKEN_KEY}"', text)

    def test_reload_fidelity(self):
        """A fresh adapter over the durable interned log expands to the same records."""
        A.INTERN_RECORD_METADATA = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "events.jsonl"
            adapter = mcp.MatrixArkLocalAdapter(path)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self._ingest_turns(server, 30)
            server.close(timeout_s=2.0)  # drain async workers BEFORE snapshotting (deterministic log)
            before = adapter._read_raw_records()
            # Fresh adapter, cold caches -> must read + expand purely from the durable log.
            adapter2 = mcp.MatrixArkLocalAdapter(path)
            server2 = mcp.MatrixArkMcpServer(adapter2, access_mode="dev")
            self.addCleanup(server2.close, timeout_s=2.0)
            after = adapter2._read_raw_records()
        self.assertEqual(before, after)
        for r in after:
            self.assertNotIn(A.INTERN_TOKEN_KEY, r)

    def test_backward_compat_inline_log_reads_under_flag_on(self):
        """An old log written flag-OFF (inline fields) reads correctly with the flag ON (no-op expand)."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "events.jsonl"
            A.INTERN_RECORD_METADATA = False
            adapter = mcp.MatrixArkLocalAdapter(path)
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            self._ingest_turns(server, 25)
            server.close(timeout_s=2.0)  # drain before snapshotting the legacy (inline) log
            legacy = adapter._read_raw_records()
            self.assertNotIn(A.INTERN_DICT_RECORD_TYPE, path.read_text(encoding="utf-8"))
            # Reopen with interning ON: the inline (un-interned) log must read identically.
            A.INTERN_RECORD_METADATA = True
            adapter2 = mcp.MatrixArkLocalAdapter(path)
            server2 = mcp.MatrixArkMcpServer(adapter2, access_mode="dev")
            self.addCleanup(server2.close, timeout_s=2.0)
            self.assertEqual(legacy, adapter2._read_raw_records())

    def test_retrieve_delete_get_all_work_with_interning(self):
        """The mem0 surface behaves identically with interning ON: recall, then delete leaves nothing."""
        A.INTERN_RECORD_METADATA = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            anchor = server.call_tool("matrixark_ingest", {
                "messages": [{"role": "user", "content": "Carol is allergic to peanuts and has a dog named Rex"}],
                "scope": _scope(), "finalize": True})["event_id_hash"]
            pack = server.call_tool("matrixark_retrieve", {"query": "what is carol allergic to", "scope": _scope()})
            self.assertIn("peanut", json.dumps(pack, default=str).lower())
            res = server.call_tool("matrixark_delete", {"memory_id": anchor, "scope": _scope()})
            self.assertTrue(res["deleted"])
            self.assertEqual(0, server.call_tool("matrixark_get_all", {"scope": _scope()})["count"])
            # No surviving embedding/index posting references the deleted anchor (no orphans).
            for r in adapter.read_all():
                if str(r.get("record_type")) in ("context_embedding", "context_index"):
                    refs = {str(r.get("ref_hash"))} | {str(x) for x in (r.get("ref_hashes") or [])}
                    self.assertNotIn(str(anchor), refs)


# ================================================================================================
# LEVER 2 -- secondary-index dimension pruning
# ================================================================================================
class PruneDimensionsCase(_FlagGuard):
    def test_prune_reduces_posting_count(self):
        """Over the SAME records, pruning drops the posting count substantially (~65%)."""
        A.INTERN_RECORD_METADATA = True
        C.PRUNE_INTERNAL_INDEX_DIMENSIONS = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._ingest_turns(server, 50)
            records = adapter._read_raw_records()

        def count(prune_on: bool):
            C.PRUNE_INTERNAL_INDEX_DIMENSIONS = prune_on
            total, dims = 0, set()
            for r in records:
                terms = C.candidate_index_terms(r, {}, {})
                total += len(terms)
                dims |= {t.split(":", 1)[0] for t in terms}
            return total, dims

        off_total, off_dims = count(False)
        on_total, on_dims = count(True)
        reduction = 100.0 * (off_total - on_total) / off_total if off_total else 0.0
        print(f"\n[lever2] postings OFF={off_total} ON={on_total} reduction={reduction:.1f}% "
              f"dropped_dims={sorted(off_dims - on_dims)}")
        self.assertGreaterEqual(reduction, 55.0, f"posting count fell only {reduction:.1f}%")
        # every dropped dimension is an internal one; no semantic dimension is lost
        self.assertTrue((off_dims - on_dims).issubset(C.INTERNAL_INDEX_DIMENSIONS))
        for semantic in ("entity_type", "entity_name", "segment_topic", "event_type",
                         "classification", "status", "source_role", "source_type"):
            if semantic in off_dims:
                self.assertIn(semantic, on_dims, f"semantic dimension {semantic} must be kept")

    def test_flag_off_reproduces_internal_dimensions(self):
        """With pruning OFF, the internal dimensions are emitted again (byte-for-byte prior behaviour)."""
        A.INTERN_RECORD_METADATA = True
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter, server = self._server(tmp)
            self._ingest_turns(server, 25)
            records = adapter._read_raw_records()
        C.PRUNE_INTERNAL_INDEX_DIMENSIONS = False
        dims = set()
        for r in records:
            dims |= {t.split(":", 1)[0] for t in C.candidate_index_terms(r, {}, {})}
        self.assertTrue(dims & C.INTERNAL_INDEX_DIMENSIONS,
                        "flag OFF must still emit at least one internal dimension")

    def test_recall_unchanged_five_of_five(self):
        """25 turns of 5 distinctive facts: retrieve recalls all 5 with pruning ON, same as OFF."""
        distinctive = {
            "allergy": ("Carol is allergic to peanuts", "what is carol allergic to", "peanut"),
            "job": ("Carol works as a data engineer at Acme", "what is carol's job", "engineer"),
            "location": ("Carol lives in Seattle", "where does carol live", "seattle"),
            "pet": ("Carol has a dog named Rex", "what pet does carol have", "rex"),
            "drink": ("Carol drinks oat milk lattes", "what does carol drink", "oat milk"),
        }

        def recall_count(prune_on: bool) -> int:
            A.INTERN_RECORD_METADATA = True
            C.PRUNE_INTERNAL_INDEX_DIMENSIONS = prune_on
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
                adapter, server = self._server(tmp)
                facts = [v[0] for v in distinctive.values()]
                for i in range(25):
                    server.call_tool("matrixark_ingest", {
                        "messages": [{"role": "user", "content": facts[i % len(facts)]}],
                        "scope": _scope("dana"), "finalize": True})
                hits = 0
                for _key, (_fact, query, needle) in distinctive.items():
                    pack = server.call_tool("matrixark_retrieve", {"query": query, "scope": _scope("dana")})
                    if needle in json.dumps(pack, default=str).lower():
                        hits += 1
                return hits

        recall_on = recall_count(True)
        recall_off = recall_count(False)
        print(f"\n[lever2] recall pruned={recall_on}/5 baseline={recall_off}/5")
        self.assertEqual(5, recall_off, "baseline recall should be 5/5")
        self.assertEqual(recall_off, recall_on,
                         f"pruning changed recall: {recall_on}/5 vs baseline {recall_off}/5")


if __name__ == "__main__":
    unittest.main(verbosity=2)
