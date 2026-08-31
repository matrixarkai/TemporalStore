#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Collapsing re-stamped pipeline-task rows must not change a single consumer's answer."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_pipeline_task_slim as slim
from matrixark_mcp_async_readiness import latest_async_pipeline_rows as readiness_latest
from matrixark_mcp_dashboard import latest_async_pipeline_rows as dashboard_latest


def task(task_hash: int, status: str, *, updated: int, detail: bool = True) -> dict:
    record = {
        "record_type": "matrixark_async_pipeline_task",
        "task_hash": task_hash,
        "event_id_hash": 900 + task_hash,
        "scope": {"scope_key": "s1"},
        "scope_key": "s1",
        "status": status,
        "idle_commit_deadline_ms": 5,
        "updated_at_ms": updated,
    }
    if detail:
        record["memory_layers_written"] = {"context_events": 1, "secondary_indexes": 6}
        record["stages"] = ["extraction", "summary"]
    return record


def positional_latest(rows: list[dict]) -> list[dict]:
    latest: dict[str, dict] = {}
    for row in rows:
        latest[str(row.get("task_hash"))] = row
    return list(latest.values())


def fingerprint(rows: list[dict]) -> dict:
    ident = lambda rs: sorted((str(r.get("task_hash")), str(r.get("status")), int(r.get("updated_at_ms") or 0))
                              for r in rs)
    return {
        "readiness": ident(readiness_latest(rows)),
        "dashboard": ident(dashboard_latest(rows)),
        "positional": ident(positional_latest(rows)),
        "extraction_committed": sorted({int(r.get("event_id_hash") or 0)
                                        for r in rows if r.get("status") == "extraction_committed"}),
        "states": sorted({(str(r.get("task_hash")), str(r.get("status"))) for r in rows}),
    }


class CollapseTest(unittest.TestCase):
    def setUp(self):
        self._saved = os.environ.get("MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS")
        os.environ["MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS"] = "1"

    def tearDown(self):
        if self._saved is None:
            os.environ.pop("MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS", None)
        else:
            os.environ["MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS"] = self._saved

    def test_restamps_collapse_but_every_state_survives(self):
        rows = [task(1, "extraction_committed", updated=10)] + [
            task(1, "summary_completed", updated=20 + stamp) for stamp in range(5)
        ]
        out = slim.collapse_pipeline_task_rows(rows)
        self.assertEqual(len(out), 2, "one row per (task, status)")
        self.assertEqual({r["status"] for r in out}, {"extraction_committed", "summary_completed"})
        newest = [r for r in out if r["status"] == "summary_completed"][0]
        self.assertEqual(newest["updated_at_ms"], 24, "the newest re-stamp is the one kept")

    def test_every_consumer_rule_gives_the_same_answer(self):
        rows = []
        for task_hash in (1, 2, 3):
            rows.append(task(task_hash, "pending", updated=1))
            rows.append(task(task_hash, "extraction_committed", updated=5))
            rows.extend(task(task_hash, "summary_completed", updated=10 + stamp) for stamp in range(4))
        # a task whose extraction_committed lands AFTER its summary_completed (the ordering that
        # makes the rank rules and the positional rule disagree)
        rows.append(task(4, "summary_completed", updated=3))
        rows.append(task(4, "extraction_committed", updated=9))
        before = fingerprint(rows)
        out = slim.collapse_pipeline_task_rows(rows)
        self.assertLess(len(out), len(rows), "something must actually collapse")
        self.assertEqual(fingerprint(out), before, "a consumer answer changed")

    def test_rows_without_an_identity_are_never_dropped(self):
        rows = [
            {"record_type": "matrixark_async_pipeline_task", "status": "summary_completed", "updated_at_ms": 1},
            {"record_type": "matrixark_async_pipeline_task", "status": "summary_completed", "updated_at_ms": 2},
            task(1, "summary_completed", updated=1),
            task(1, "summary_completed", updated=2),
        ]
        out = slim.collapse_pipeline_task_rows(rows)
        unkeyed = [r for r in out if r.get("task_hash") is None]
        self.assertEqual(len(unkeyed), 2, "unkeyable rows pass through untouched")

    def test_other_record_types_and_order_are_preserved(self):
        rows = [
            {"record_type": "context_event", "event_id_hash": 1},
            task(1, "summary_completed", updated=1),
            {"record_type": "context_summary", "summary_hash": 2},
            task(1, "summary_completed", updated=2),
        ]
        out = slim.collapse_pipeline_task_rows(rows)
        self.assertEqual([r["record_type"] for r in out],
                         ["context_event", "context_summary", "matrixark_async_pipeline_task"])

    def test_collapse_is_idempotent(self):
        rows = [task(1, "summary_completed", updated=stamp) for stamp in range(4)]
        once = slim.collapse_pipeline_task_rows(rows)
        self.assertIs(slim.collapse_pipeline_task_rows(once), once)

    def test_nothing_duplicated_returns_input_identity(self):
        rows = [task(1, "pending", updated=1), task(2, "summary_completed", updated=2)]
        self.assertIs(slim.collapse_pipeline_task_rows(rows), rows)

    def test_disabled_flag_returns_input_identity(self):
        os.environ["MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS"] = "0"
        rows = [task(1, "summary_completed", updated=stamp) for stamp in range(4)]
        self.assertIs(slim.collapse_pipeline_task_rows(rows), rows)


class SlimLeverTest(unittest.TestCase):
    """Lever B is the small one and is opt-in; it must still be correct when switched on."""

    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in
                       ("MATRIXARK_SLIM_TERMINAL_PIPELINE_TASKS", "MATRIXARK_PIPELINE_TASK_DETAIL_RETAIN_PER_SCOPE")}

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_off_by_default(self):
        os.environ.pop("MATRIXARK_SLIM_TERMINAL_PIPELINE_TASKS", None)
        self.assertFalse(slim.slim_terminal_pipeline_tasks_enabled())
        rows = [task(1, "summary_completed", updated=1)]
        self.assertIs(slim.slim_terminal_pipeline_tasks(rows), rows)

    def test_when_enabled_consumer_fields_survive(self):
        os.environ["MATRIXARK_SLIM_TERMINAL_PIPELINE_TASKS"] = "1"
        os.environ["MATRIXARK_PIPELINE_TASK_DETAIL_RETAIN_PER_SCOPE"] = "0"
        rows = [task(1, "summary_completed", updated=1)]
        aged = slim.slim_terminal_pipeline_tasks(rows)[0]
        self.assertTrue(aged["detail_slimmed"])
        self.assertNotIn("memory_layers_written", aged)
        for field in ("task_hash", "event_id_hash", "scope", "status", "idle_commit_deadline_ms", "updated_at_ms"):
            self.assertIn(field, aged)


class EndToEndTest(unittest.TestCase):
    def _run(self, *, collapse: str) -> dict:
        import matrixark_mcp_server as mcp

        saved = os.environ.get("MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS")
        os.environ["MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS"] = collapse
        try:
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
                adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl")
                server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
                scope = {"tenant_id": "acme", "user_id": "alice", "session_id": "s1"}
                call = lambda name, args: server.call_tool(name, {**args, "scope": scope})
                facts = ["I am allergic to peanuts.", "I live in Kyoto.", "My favorite drink is matcha."]
                for turn, fact in enumerate(facts * 4):
                    call("matrixark_ingest", {"messages": [{"role": "user", "content": f"turn {turn}: {fact}"}],
                                              "finalize": True})
                    call("matrixark_session_commit", {})
                call("matrixark_refresh_summaries", {"limit": 500})
                records = adapter.read_all()
                tasks = [r for r in records if r.get("record_type") == "matrixark_async_pipeline_task"]
                return {
                    "stats": slim.pipeline_task_footprint_stats(records),
                    "records": records,
                    "fingerprint": fingerprint(tasks),
                    "recall": {
                        fact: fact.split()[-1].strip(".").lower()
                        in json.dumps(call("matrixark_retrieve", {"query": fact}), default=str).lower()
                        for fact in facts
                    },
                }
        finally:
            if saved is None:
                os.environ.pop("MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS", None)
            else:
                os.environ["MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS"] = saved

    def test_serving_view_collapses_without_changing_behavior(self):
        off = self._run(collapse="0")
        on = self._run(collapse="1")
        self.assertEqual(on["recall"], off["recall"], "recall must not move")
        # Counts across two ingest runs are not comparable -- async pipeline timing decides how many
        # task rows each run produces -- so the survival claim is checked on ONE record set below.
        # Two ingest runs mint different event ids, so the ids themselves are not comparable across
        # arms -- the equivalence that matters is on ONE record set: collapsing the un-collapsed
        # store must leave every consumer rule's answer bit-identical.
        os.environ["MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS"] = "1"
        try:
            source = list(off["records"])
            # An ingest no longer mints duplicate stampings on its own: a task's row is now keyed
            # by task_hash in the latest-state view, so repeated stampings of one task collapse at
            # WRITE time and never reach the log. The collapse still has to work -- stores written
            # before that change are full of them -- so the duplicate it removes is supplied here
            # rather than depended on. Without this the assertion below passes or fails on whether
            # the async pipeline happened to stamp twice during the run, which is timing, not
            # behaviour.
            existing = [r for r in source
                        if r.get("record_type") == "matrixark_async_pipeline_task"]
            if existing:
                source.append(dict(existing[-1]))
            raw_tasks = [r for r in source if r.get("record_type") == "matrixark_async_pipeline_task"]
            collapsed = slim.collapse_pipeline_task_rows(source)
            collapsed_tasks = [r for r in collapsed if r.get("record_type") == "matrixark_async_pipeline_task"]
        finally:
            os.environ.pop("MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS", None)
        self.assertLess(len(collapsed_tasks), len(raw_tasks), "duplicate stampings must be removed")
        states = lambda rows: {(str(r.get("task_hash")), str(r.get("status"))) for r in rows}
        self.assertEqual(states(collapsed_tasks), states(raw_tasks),
                         "every (task, status) the pipeline reached must survive the collapse")
        self.assertEqual(fingerprint(collapsed_tasks), fingerprint(raw_tasks),
                         "readiness / dashboard / positional / extraction-signal answers must be identical")


if __name__ == "__main__":
    unittest.main(verbosity=2)
