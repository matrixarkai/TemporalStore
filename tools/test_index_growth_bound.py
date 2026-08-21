#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Secondary-index growth bound: compaction is recall-preserving, caps are deterministic.

The three levers (see ``matrixark_index_growth_bound``) each get a gate here:

* Lever 1 -- an ``index_compact`` tombstone drops ONLY per-event postings whose every ref was rolled
  up into a summary, and the one-pass sweep is equivalent to the per-tombstone predicate.
* Lever 2 -- the per-scope cap evicts oldest-first, deterministically, per scope.
* Lever 3 -- the store-wide ceiling applies AFTER the cap and is never exceeded.
* Every lever is flag-reversible: with the gates off the input list is returned unchanged (identity).
* End to end -- after a rollup the event postings are gone and the facts are still retrievable.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_index_growth_bound as bound


def posting(ref_type: str, refs: list, *, scope_key: str = "s1", ts: int = 0, index_hash: str = "") -> dict:
    return {
        "record_type": "context_index",
        "index_name": "keyword:x",
        "ref_type": ref_type,
        "ref_hashes": list(refs),
        "scope_key": scope_key,
        "timestamp_key_ms": ts,
        "index_hash": index_hash or f"h{ts}-{refs}",
    }


class IndexCompactionTombstoneTest(unittest.TestCase):
    def test_tombstone_is_none_when_nothing_was_rolled_up(self):
        self.assertIsNone(bound.index_compaction_tombstone(source_event_ids=[]))

    def test_kills_only_fully_covered_event_postings(self):
        tombstone = bound.index_compaction_tombstone(source_event_ids=[1, 2, 3], scope={"scope_key": "s1"})
        kills = lambda record: bound.index_compact_tombstone_kills_record(tombstone, record)
        self.assertTrue(kills(posting("event", [1, 2])), "fully covered event posting must compact")
        self.assertTrue(kills(posting("event", [3])))
        self.assertFalse(kills(posting("event", [1, 99])), "partially covered posting must survive intact")
        self.assertFalse(kills(posting("summary", [1])), "summary postings are the surviving recall path")
        self.assertFalse(kills(posting("entity", [1])))
        self.assertFalse(kills(posting("event", [1], scope_key="other")), "must not cross scopes")
        self.assertFalse(kills({"record_type": "context_event", "event_id_hash": 1}), "never touches events")

    def test_legacy_single_ref_field_is_matched(self):
        tombstone = bound.index_compaction_tombstone(source_event_ids=[7])
        legacy = {"record_type": "context_index", "ref_type": "event", "ref_hash": 7}
        self.assertTrue(bound.index_compact_tombstone_kills_record(tombstone, legacy))


class SweepEquivalenceTest(unittest.TestCase):
    """The O(n) reverse pass must agree with applying the predicate tombstone-by-tombstone."""

    def _reference_sweep(self, records: list[dict]) -> list[dict]:
        live: list[dict] = []
        for record in records:
            if str(record.get("tombstone_kind") or "") == bound.INDEX_COMPACT_TOMBSTONE_KIND:
                live = [kept for kept in live if not bound.index_compact_tombstone_kills_record(record, kept)]
                continue
            live.append(record)
        return live

    def test_matches_reference_and_is_order_aware(self):
        log = [
            posting("event", [1], ts=1),
            posting("event", [2], ts=2),
            posting("summary", [900], ts=3),
            bound.index_compaction_tombstone(source_event_ids=[1, 2], scope={"scope_key": "s1"}),
            posting("event", [3], ts=4),  # ingested AFTER the tombstone -> must survive
        ]
        swept = bound.sweep_index_compaction(log)
        self.assertEqual(swept, self._reference_sweep(log))
        self.assertEqual([str(r.get("ref_hashes")) for r in swept if r.get("ref_type") == "event"], ["[3]"])
        self.assertFalse(any(r.get("tombstone_kind") for r in swept), "tombstones must not serve")

    def test_multi_scope_targets_stay_isolated(self):
        log = [
            posting("event", [1], scope_key="a", ts=1),
            posting("event", [1], scope_key="b", ts=1),
            bound.index_compaction_tombstone(source_event_ids=[1], scope={"scope_key": "a"}),
        ]
        swept = bound.sweep_index_compaction(log)
        self.assertEqual(swept, self._reference_sweep(log))
        self.assertEqual([r["scope_key"] for r in swept], ["b"])

    def test_no_tombstone_returns_input_identity(self):
        log = [posting("event", [1]), posting("summary", [2])]
        self.assertIs(bound.sweep_index_compaction(log), log)


class BoundEnforcementTest(unittest.TestCase):
    def setUp(self):
        self._saved = {
            key: os.environ.get(key)
            # Every env var these tests touch, including the legacy aliases -- an unrestored budget
            # leaks into later tests and silently evicts their postings.
            for key in (
                "MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SESSION",
                "MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_TENANT",
                "MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE",
                "MATRIXARK_SECONDARY_INDEX_HARD_CEILING",
                "MATRIXARK_INDEX_COMPACT_ON_SUMMARY",
                "MATRIXARK_DEDUPE_INDEX_POSTINGS",
            )
        }

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_per_scope_cap_evicts_oldest_first_per_scope(self):
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE"] = "2"
        os.environ["MATRIXARK_SECONDARY_INDEX_HARD_CEILING"] = "0"
        records = [posting("event", [i], scope_key=scope, ts=i) for scope in ("a", "b") for i in (1, 2, 3)]
        records.append({"record_type": "context_event", "event_id_hash": 1})
        kept = bound.enforce_secondary_index_bounds(records)
        by_scope = {}
        for record in kept:
            if record.get("record_type") == "context_index":
                by_scope.setdefault(record["scope_key"], []).append(record["timestamp_key_ms"])
        self.assertEqual(by_scope, {"a": [2, 3], "b": [2, 3]}, "each scope keeps its newest 2")
        self.assertTrue(any(r.get("record_type") == "context_event" for r in kept), "non-index records untouched")

    def test_tenant_budget_applies_after_the_session_budget(self):
        """Per-session budget first, then the tenant's own budget across its sessions.

        Deliberately NOT a store-wide total: the sessions below belong to one tenant, and the tenant
        budget bounds their sum. A different tenant's sessions are accounted separately (covered in
        test_tenant_policy), because a global total would let one tenant evict another."""
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SESSION"] = "3"
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_TENANT"] = "4"
        records = [
            posting("event", [f"{session}{i}"], scope_key=f"t=42|u=1|s={session}", ts=i)
            for session in ("a", "b")
            for i in (1, 2, 3, 4)
        ]
        kept = [r for r in bound.enforce_secondary_index_bounds(records) if r.get("record_type") == "context_index"]
        self.assertEqual(len(kept), 4, "the tenant budget bounds the sum of its sessions")
        self.assertEqual(sorted(r["timestamp_key_ms"] for r in kept), [3, 3, 4, 4], "newest survive")

    def test_eviction_is_deterministic_across_repeats(self):
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE"] = "2"
        os.environ["MATRIXARK_SECONDARY_INDEX_HARD_CEILING"] = "0"
        records = [posting("event", [i], ts=0, index_hash=f"h{i}") for i in range(6)]
        first = [r["index_hash"] for r in bound.enforce_secondary_index_bounds(list(records))]
        for _ in range(5):
            self.assertEqual([r["index_hash"] for r in bound.enforce_secondary_index_bounds(list(records))], first)

    def test_eviction_gives_up_recall_paths_last(self):
        """A binding cap must spend the cheap postings first: events before segments before
        entities before summaries -- summaries are what lever 1 leaves as the route to compacted
        old content, so evicting them by age alone is what turns a cap into a recall loss."""
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE"] = "2"
        os.environ["MATRIXARK_SECONDARY_INDEX_HARD_CEILING"] = "0"
        # The summary posting is the OLDEST, so a pure oldest-first rule would evict it first.
        records = [
            posting("summary", [1], ts=1),
            posting("entity", [2], ts=2),
            posting("segment", [3], ts=3),
            posting("event", [4], ts=4),
        ]
        kept = [record["ref_type"] for record in bound.enforce_secondary_index_bounds(records)]
        self.assertEqual(kept, ["summary", "entity"], "recall paths survive, raw postings go first")

    def test_defaults_are_small_and_enabled(self):
        for key in ("MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE", "MATRIXARK_SECONDARY_INDEX_HARD_CEILING"):
            os.environ.pop(key, None)
        self.assertEqual(bound.max_index_records_per_scope(), 128)
        self.assertEqual(bound.index_hard_ceiling(), 1024)

    def test_disabled_bounds_return_input_identity(self):
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE"] = "0"
        os.environ["MATRIXARK_SECONDARY_INDEX_HARD_CEILING"] = "0"
        records = [posting("event", [i], ts=i) for i in range(10)]
        self.assertIs(bound.enforce_secondary_index_bounds(records), records)

    def test_under_budget_returns_input_identity(self):
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE"] = "100"
        os.environ["MATRIXARK_SECONDARY_INDEX_HARD_CEILING"] = "100"
        records = [posting("event", [i], ts=i) for i in range(10)]
        self.assertIs(bound.enforce_secondary_index_bounds(records), records)

    def test_drop_callback_reports_every_eviction(self):
        os.environ["MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE"] = "1"
        os.environ["MATRIXARK_SECONDARY_INDEX_HARD_CEILING"] = "0"
        seen: list[dict] = []
        records = [posting("event", [i], ts=i) for i in range(4)]
        kept = bound.enforce_secondary_index_bounds(records, on_drop=seen.append)
        self.assertEqual(len(seen), 3, "no silent truncation")
        self.assertEqual(len(kept), 1)


class EndToEndCompactionTest(unittest.TestCase):
    """Ingest -> roll up -> the per-event postings are gone and the facts still come back."""

    FACTS = ["I am allergic to peanuts.", "I live in Kyoto.", "My favorite drink is matcha."]

    def _run(self, *, compact: str) -> dict:
        import matrixark_mcp_server as mcp

        saved = os.environ.get("MATRIXARK_INDEX_COMPACT_ON_SUMMARY")
        os.environ["MATRIXARK_INDEX_COMPACT_ON_SUMMARY"] = compact
        try:
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
                adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl")
                server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
                scope = {"tenant_id": "acme", "user_id": "alice", "session_id": "s1"}
                call = lambda name, args: server.call_tool(name, {**args, "scope": scope})
                for turn, fact in enumerate(self.FACTS * 3):
                    call("matrixark_ingest", {
                        "messages": [{"role": "user", "content": f"turn {turn}: {fact}"}],
                        "finalize": True,
                    })
                    call("matrixark_session_commit", {})
                call("matrixark_refresh_summaries", {"limit": 200})
                stats = bound.secondary_index_bound_stats(adapter.read_all())
                import json as _json

                found = {
                    fact: fact.split()[-1].strip(".").lower()
                    in _json.dumps(call("matrixark_retrieve", {"query": fact}), default=str).lower()
                    for fact in self.FACTS
                }
                return {"stats": stats, "recall": found}
        finally:
            if saved is None:
                os.environ.pop("MATRIXARK_INDEX_COMPACT_ON_SUMMARY", None)
            else:
                os.environ["MATRIXARK_INDEX_COMPACT_ON_SUMMARY"] = saved

    def test_rollup_prunes_event_postings_without_losing_recall(self):
        off = self._run(compact="0")
        on = self._run(compact="1")
        off_events = off["stats"]["by_ref_type"].get("event", 0)
        on_events = on["stats"]["by_ref_type"].get("event", 0)
        self.assertGreater(off_events, 0, "baseline must carry per-event postings to compact")
        self.assertEqual(on_events, 0, "every rolled-up event's postings must be compacted")
        self.assertGreater(on["stats"]["by_ref_type"].get("summary", 0), 0, "summary recall path survives")
        # Recall is the whole point of Lever 1: compaction must not cost a single fact.
        self.assertEqual(on["recall"], off["recall"])
        self.assertTrue(all(on["recall"].values()), f"facts lost after compaction: {on['recall']}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
