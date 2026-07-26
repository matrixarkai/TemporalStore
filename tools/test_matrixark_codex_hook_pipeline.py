#!/usr/bin/env python3

from __future__ import annotations

from argparse import Namespace
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from pathlib import Path

import matrixark_codex_hook
from matrixark_mcp_core import packing_sort_key
from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer


class CountingLocalAdapter(MatrixArkLocalAdapter):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.flushed_batch_sizes: list[int] = []
        self.retrieval_call_count = 0

    def append_many(self, records: list[dict]) -> None:
        active_batch = self._current_write_batch()
        super().append_many(records)
        if active_batch is None and records:
            self.flushed_batch_sizes.append(len(records))

    def retrieval_records(self, **kwargs):
        self.retrieval_call_count += 1
        return super().retrieval_records(**kwargs)


class FastHookLocalAdapter(MatrixArkLocalAdapter):
    def enqueue_raw_ingestion_records(self, records: list[dict]) -> None:
        self.append_many(records)

    def _enqueue_direct_write(self, records: list[dict]) -> None:
        self.append_many(records)


class MatrixArkCodexHookPipelineTest(unittest.TestCase):
    def test_pending_async_events_are_demoted_for_budget_packing(self) -> None:
        pending_event = {
            "ref_type": "event",
            "event_type": "pending_async",
            "classification": "PENDING_ASYNC_EXTRACTION",
            "extraction_status": "pending",
            "extraction_mode": "async_pending",
            "score": 0.92,
            "text": "assistant: Commit d0152479 pushed and hook pipeline tests passed.",
        }
        ordinary_event = {
            **pending_event,
            "event_type": "assistant_decision",
            "classification": "ANSWER",
            "extraction_status": "observed",
            "extraction_mode": "one_pass",
        }
        extracted_entity = {
            "ref_type": "entity",
            "entity_type": "assistant_decision",
            "score": 0.76,
            "text": "assistant_decision Commit d0152479 pushed and hook pipeline tests passed.",
        }

        self.assertGreater(packing_sort_key(ordinary_event, "fact"), packing_sort_key(pending_event, "fact"))
        self.assertGreater(packing_sort_key(extracted_entity, "fact"), packing_sort_key(pending_event, "fact"))

    def test_fast_hook_dirty_markers_refresh_into_retrievable_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-summary.jsonl")

            class Server:
                def __init__(self) -> None:
                    self.adapter = adapter

            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_fast",
                tenant_id="tenant_fast",
                user_id="user_fast",
                session_id="session_fast_summary",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            result = matrixark_codex_hook.fast_async_hook_ingest(
                Server(),
                args=args,
                text="Remember that the fast hook summary refresh path must turn dirty markers into retrievable summaries.",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )
            self.assertEqual("accepted", result["status"])
            self.assertGreaterEqual(result["summary_dirty_count"], 1)

            refresh = adapter.refresh_summaries(
                {
                    "scope": {
                        "account_id": "acct_fast",
                        "tenant_id": "tenant_fast",
                        "user_id": "user_fast",
                        "session_id": "session_fast_summary",
                    },
                    "limit": 16,
                    "refreshed_at_ms": int(time.time() * 1000) + 1000,
                }
            )
            self.assertGreaterEqual(refresh.get("refreshed_count", 0), 1)
            records = adapter.read_all()
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("status") == "completed"
                    for record in records
                )
            )
            self.assertTrue(any(record.get("record_type") == "context_summary" for record in records))

            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_fast",
                        "tenant_id": "tenant_fast",
                        "user_id": "user_fast",
                        "session_id": "session_fast_summary",
                    },
                    "query": "Summarize the fast hook summary refresh path.",
                    "max_context_tokens": 100,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 2},
                }
            )
            self.assertLessEqual(pack["used_context_tokens"], 100)
            self.assertTrue(any(ref.get("ref_type") == "summary" for ref in pack["selected_refs"]))

    def test_retrieval_metrics_expose_shared_local_remote_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-retrieval-budget.jsonl")
            scope = {
                "account_id": "acct_budget",
                "tenant_id": "tenant_budget",
                "user_id": "user_budget",
                "session_id": "session_budget",
            }
            adapter.ingest(
                {
                    "scope": scope,
                    "messages": [
                        {
                            "role": "user",
                            "content": "Budget fact: Alice approved the GPU checklist after Bob confirmed the owner.",
                        }
                    ],
                }
            )
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Who approved the GPU checklist?",
                    "max_context_tokens": 100,
                    "local_context": [{"ref": "open-file:notes.md", "text": "Visible local context already mentions Bob."}],
                    "local_context_tokens": 30,
                    "local_context_safety_margin_tokens": 10,
                    "include_retrieval_metrics": True,
                    "audit_mode": "off",
                }
            )
            metrics = pack["retrieval_metrics"]
            self.assertEqual(100, metrics["requested_max_context_tokens"])
            self.assertEqual(30, metrics["used_local_context_tokens"])
            self.assertEqual(10, metrics["local_context_safety_margin_tokens"])
            self.assertEqual(60, metrics["remote_context_budget_tokens"])
            self.assertLessEqual(metrics["used_remote_context_tokens"], metrics["remote_context_budget_tokens"])
            self.assertEqual(
                metrics["used_local_context_tokens"] + metrics["used_remote_context_tokens"],
                metrics["total_prompt_context_tokens"],
            )
            self.assertTrue(metrics["remote_is_additive_only_within_remaining_budget"])

    def run_hook(self, repo: Path, event_log: Path, *, event: str, payload: dict, query: str = "") -> dict:
        cmd = [
            sys.executable,
            str(repo / "tools" / "matrixark_codex_hook.py"),
            "--backend",
            "local",
            "--event-log",
            str(event_log),
            "--event",
            event,
            "--account-id",
            "acct_hook",
            "--tenant-id",
            "tenant_hook",
            "--user-id",
            "codex_user",
            "--session-id",
            "codex_session_1",
            "--team",
            "agent",
            "--project",
            "context",
        ]
        if query:
            cmd.extend(["--query", query])
        proc = subprocess.run(
            cmd,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            cwd=repo,
            timeout=30,
            env={**os.environ, "MATRIXARK_ALLOW_LOCAL_BACKEND": "1"},
        )
        if proc.returncode != 0:
            raise AssertionError(f"hook failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return json.loads(proc.stdout)

    def test_message_ingest_batches_hot_path_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "matrixark-message-batch.jsonl"
            adapter = CountingLocalAdapter(event_log)
            server = MatrixArkMcpServer(adapter, line_json=True, access_mode="dev")
            result = server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": "Alice approved the GPU request after finance reviewed the budget.",
                        }
                    ],
                    "scope": {
                        "account_id": "acct_batch",
                        "tenant_id": "tenant_batch",
                        "user_id": "user_batch",
                        "session_id": "session_batch",
                    },
                    "metadata": {"node_path": ["tenant:tenant_batch", "user:user_batch", "session:session_batch"]},
                },
            )
            self.assertEqual("accepted", result["status"])
            self.assertTrue(any(size >= 7 for size in adapter.flushed_batch_sizes), adapter.flushed_batch_sizes)
            records = adapter.read_all()
            record_types = {record.get("record_type") for record in records}
            self.assertIn("context_event", record_types)
            self.assertIn("context_embedding", record_types)
            self.assertIn("context_index", record_types)
            self.assertIn("session_buffer_event", record_types)
            self.assertIn("context_summary_dirty", record_types)
            index_records = [record for record in records if record.get("record_type") == "context_index"]
            self.assertTrue(index_records)
            self.assertTrue(all("timestamp_key_ms" in record for record in index_records))
            self.assertTrue(all(isinstance(record.get("ref_hashes"), list) for record in index_records))
            self.assertTrue(all(isinstance(record.get("index_hash"), int) for record in index_records))
            self.assertEqual(len(index_records), len({record.get("index_hash") for record in index_records}))

    def test_lightweight_async_ingest_threshold_and_idle_commit_flush_session_buffer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-async-threshold-idle.jsonl")
            scope = {
                "account_id": "acct_async",
                "tenant_id": "tenant_async",
                "user_id": "user_async",
                "session_id": "session_async",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": True,
                "session_buffer_threshold": 2,
                "skip_prior_context": True,
            }
            first = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "user", "content": "Plan to extract Codex prompts in batches."}],
                }
            )
            self.assertEqual("accepted", first["status"])
            self.assertIsNone(first["auto_batch_extract_result"])
            self.assertEqual(1, first["session_buffer"]["pending_event_count"])

            second = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "assistant", "content": "Decision: threshold commits should flush without waiting for Stop."}],
                }
            )
            self.assertEqual("committed", second["auto_batch_extract_result"]["status"])
            self.assertEqual("threshold", second["auto_batch_extract_result"]["trigger_policy"])
            self.assertEqual("provisional", second["auto_batch_extract_result"]["extraction_phase"])
            self.assertFalse(second["auto_batch_extract_result"]["final_session_boundary"])
            self.assertEqual(2, second["auto_batch_extract_result"]["committed_event_count"])
            threshold_evidence = second["auto_batch_extract_result"]["trigger_evidence"]
            self.assertTrue(threshold_evidence["threshold_ready"])
            self.assertFalse(threshold_evidence["idle_ready"])
            self.assertEqual(2, threshold_evidence["pending_event_count"])
            self.assertEqual(2, threshold_evidence["threshold_messages"])

            third = adapter.ingest(
                {
                    **base_args,
                    "auto_batch_extract": False,
                    "messages": [{"role": "tool", "content": "Exit code: 0\nRan 3 tests in 0.01s\nOK"}],
                }
            )
            self.assertEqual(1, third["session_buffer"]["pending_event_count"])
            idle = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 20,
                    "force": False,
                    "idle_timeout_ms": 0,
                    "commit_reason": "idle_timeout",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("committed", idle["status"])
            self.assertEqual("idle_timeout", idle["trigger_policy"])
            self.assertEqual("provisional", idle["extraction_phase"])
            self.assertFalse(idle["final_session_boundary"])
            self.assertEqual(1, idle["committed_event_count"])
            self.assertEqual(2, idle["extraction_context_event_count"])
            self.assertEqual(
                [int(event_id) for event_id in second["auto_batch_extract_result"]["source_event_ids"]],
                [int(event_id) for event_id in idle["extraction_context_event_ids"]],
            )
            idle_evidence = idle["trigger_evidence"]
            self.assertFalse(idle_evidence["threshold_ready"])
            self.assertTrue(idle_evidence["idle_ready"])
            self.assertEqual(1, idle_evidence["pending_event_count"])
            self.assertEqual(0, idle_evidence["idle_timeout_ms"])

            records = adapter.read_all()
            commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
            self.assertEqual(2, len(commits))
            self.assertTrue(all(commit.get("extraction_phase") == "provisional" for commit in commits))
            self.assertTrue(all(commit.get("final_session_boundary") is False for commit in commits))
            self.assertTrue(all(isinstance(commit.get("trigger_evidence"), dict) for commit in commits))
            committed_ids = {
                int(event_id)
                for commit in commits
                for event_id in commit.get("source_event_ids", [])
            }
            pending_ids = {
                int(record.get("event_id_hash"))
                for record in records
                if record.get("record_type") == "session_buffer_event"
            }
            self.assertEqual(pending_ids, committed_ids)
            self.assertFalse(adapter.pending_session_events(scope))
            idle_commit = next(commit for commit in commits if commit.get("trigger_policy") == "idle_timeout")
            self.assertEqual(2, idle_commit["extraction_context_event_count"])
            extraction_audits = [
                record
                for record in records
                if record.get("record_type") == "context_extraction_audit"
                and record.get("batch_id_hash") == idle["batch_id_hash"]
            ]
            self.assertEqual(1, len(extraction_audits))
            self.assertEqual(
                [int(event_id) for event_id in idle["extraction_context_event_ids"]],
                [int(event_id) for event_id in extraction_audits[0]["extraction_context_event_ids"]],
            )
            profile_tool_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("entity_type") == "tool_evidence"
            ]
            self.assertTrue(profile_tool_entities)
            self.assertTrue(any("Exit code: 0" in str(record.get("state") or "") for record in profile_tool_entities))
            pack = adapter.retrieve(
                {
                    "scope": {**scope, "session_id": "later_async_session"},
                    "session_scope": "prefer",
                    "query": "What tool evidence proved the async threshold and idle extraction path?",
                    "max_context_tokens": 240,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 4},
                }
            )
            selected_refs = pack["selected_refs"]
            selected_tool_refs = [
                ref
                for ref in selected_refs
                if ref.get("ref_type") == "entity"
                and ref.get("memory_scope") == "user_profile"
                and ref.get("session_continuity") == "cross_session"
                and ref.get("entity_type") == "tool_evidence"
            ]
            self.assertTrue(selected_tool_refs, selected_refs)
            self.assertTrue(any("Exit code: 0" in str(ref.get("text") or "") for ref in selected_tool_refs))
            self.assertTrue(any(scope["session_id"] in ref.get("source_session_ids", []) for ref in selected_tool_refs))
            budget = pack["memory_layer_budget"]
            self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(budget["by_extraction_phase"]["provisional"]["refs"], 1)

    def test_stop_boundary_force_commits_full_live_conversation_tail_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-stop-boundary.jsonl")
            scope = {
                "account_id": "acct_stop",
                "tenant_id": "tenant_stop",
                "user_id": "user_stop",
                "session_id": "session_stop_boundary",
            }
            base_args = {
                "scope": scope,
                "async_processing": True,
                "auto_batch_extract": False,
                "session_buffer_threshold": 20,
                "skip_prior_context": True,
            }
            user_ingest = adapter.ingest(
                {
                    **base_args,
                    "messages": [{"role": "user", "content": "Should memory wait for Stop or commit on thresholds?"}],
                }
            )
            assistant_ingest = adapter.ingest(
                {
                    **base_args,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": (
                                "Decision: threshold and idle commits are provisional checkpoints; "
                                "Stop force-drains the remaining conversation tail."
                            ),
                        }
                    ],
                }
            )
            self.assertEqual(1, user_ingest["session_buffer"]["pending_event_count"])
            self.assertEqual(2, assistant_ingest["session_buffer"]["pending_event_count"])

            pending_before_stop = adapter.pending_session_events(scope)
            stop = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 20,
                    "force": True,
                    "commit_reason": "hook_boundary",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("committed", stop["status"])
            self.assertEqual("force", stop["trigger_policy"])
            self.assertEqual("final", stop["extraction_phase"])
            self.assertTrue(stop["final_session_boundary"])
            self.assertEqual(2, stop["committed_event_count"])
            self.assertEqual(
                [int(record["event_id_hash"]) for record in pending_before_stop],
                [int(event_id) for event_id in stop["source_event_ids"]],
            )
            self.assertFalse(adapter.pending_session_events(scope))

            second_stop = adapter.session_commit(
                {
                    "scope": scope,
                    "threshold_messages": 20,
                    "force": True,
                    "commit_reason": "hook_boundary",
                    "skip_prior_context": True,
                }
            )
            self.assertEqual("empty", second_stop["status"])

            records = adapter.read_all()
            commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
            self.assertEqual(1, len(commits))
            self.assertEqual(2, commits[0]["committed_event_count"])
            self.assertEqual("final", commits[0]["extraction_phase"])
            self.assertTrue(commits[0]["final_session_boundary"])
            assistant_decisions = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("entity_type") == "assistant_decision"
            ]
            self.assertTrue(assistant_decisions)
            self.assertTrue(
                any("Stop force-drains" in str(record.get("state") or "") for record in assistant_decisions)
            )

    def test_compact_hot_prefix_preserves_boundary_session_commits(self) -> None:
        with mock.patch.object(matrixark_codex_hook, "HOOK_COMPACT_HOT_PREFIX_ONLY", True):
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("IdleTimeout", ""))
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("SessionIdle", ""))
            self.assertTrue(matrixark_codex_hook.should_run_session_commit_after_ingest("Stop", ""))
            self.assertFalse(matrixark_codex_hook.should_run_session_commit_after_ingest("UserPromptSubmit", ""))
            self.assertFalse(matrixark_codex_hook.should_run_session_commit_after_ingest("IdleTimeout", "timeout"))

    def test_batch_extract_events_are_timestamp_keyed_under_segment_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-batch-segment-time.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_seg",
                        "tenant_id": "tenant_seg",
                        "user_id": "user_seg",
                        "session_id": "session_seg",
                    },
                    "messages": [
                        {"role": "user", "content": "Alice discussed recursion base cases."},
                        {"role": "assistant", "content": "Use a base case to stop recursion."},
                    ],
                    "force": True,
                }
            )
            self.assertGreaterEqual(result.get("segments_written", 0), 1)
            records = adapter.read_all()
            segments = [row for row in records if row.get("record_type") == "context_segment"]
            events = [row for row in records if row.get("record_type") == "context_event"]
            self.assertTrue(segments)
            self.assertTrue(events)
            segment_hashes = {row.get("segment_hash") for row in segments}
            self.assertTrue(all(row.get("context_event_parent_type") == "context_segment" for row in events))
            self.assertTrue(all(row.get("context_event_parent_hash") in segment_hashes for row in events))
            self.assertTrue(all(str(row.get("event_time_key", "")).startswith(str(row.get("event_time_ms", 0)).zfill(20)) for row in events))

    def test_batch_extract_promotes_session_entities_to_cross_session_profile_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-memory.jsonl")
            result = adapter.batch_extract(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "session_codex_1",
                    },
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                "I prefer no external Codex hook logs outside TemporalStore. "
                                "We will batch user prompts and assistant decisions for extraction."
                            ),
                        },
                        {
                            "role": "assistant",
                            "content": "Done. Commit d0152479 pushed; the Rust hook daemon log is /dev/null.",
                        },
                        {
                            "role": "tool",
                            "content": (
                                "Exit code: 0\n"
                                "Ran 32 tests in 0.508s\n"
                                "OK\n"
                                "002fbd45034c69ce4487a64ab40d90135a55ae1a refs/heads/main"
                            ),
                        },
                    ],
                    "metadata": {
                        "hook_type": "hook_boundary",
                        "codex_event": "Stop",
                    },
                    "force": True,
                }
            )

            self.assertGreaterEqual(result.get("entities_written", 0), 1)
            self.assertEqual(result.get("entities_written"), result.get("profile_entities_written"))
            records = adapter.read_all()
            session_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "session"
                and record.get("session_continuity") == "same_session"
            ]
            profile_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(session_entities)
            self.assertEqual(len(session_entities), len(profile_entities))
            profile_entity = profile_entities[0]
            self.assertEqual(
                {"account_id": "acct_profile", "tenant_id": "tenant_profile", "user_id": "user_profile"},
                profile_entity["access_scope"],
            )
            self.assertEqual(["session_codex_1"], profile_entity["source_session_ids"])
            self.assertTrue(profile_entity["source_entity_hashes"])
            self.assertTrue(profile_entity["source_refs"])
            self.assertIn("assistant", profile_entity["source_roles"])
            self.assertIn("tool", profile_entity["source_roles"])
            self.assertIn("hook_boundary", profile_entity["source_hook_types"])
            self.assertIn("Stop", profile_entity["source_codex_events"])
            index_names = {
                record.get("index_name")
                for record in records
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_profile_entity"
            }
            self.assertIn("memory_scope:user_profile", index_names)
            self.assertIn("session_continuity:cross_session", index_names)
            self.assertIn("source_role:assistant", index_names)
            self.assertIn("source_role:tool", index_names)
            self.assertIn("hook_type:hook_boundary", index_names)
            self.assertIn("codex_event:Stop", index_names)
            self.assertTrue(any(str(name).startswith("entity_type:") for name in index_names))
            self.assertIn("entity_type:assistant_decision", index_names)
            self.assertIn("entity_type:tool_evidence", index_names)
            self.assertTrue(
                any(
                    record.get("entity_type") == "assistant_decision"
                    and "Commit d0152479 pushed" in str(record.get("state") or "")
                    for record in profile_entities
                )
            )
            self.assertTrue(
                any(
                    record.get("entity_type") == "tool_evidence"
                    and "Exit code: 0" in str(record.get("state") or "")
                    for record in profile_entities
                )
            )
            retrieved = adapter.retrieval_records(
                scope={
                    "account_id": "acct_profile",
                    "tenant_id": "tenant_profile",
                    "user_id": "user_profile",
                    "session_id": "later_session",
                    "session_scope": "prefer",
                },
                record_types={"context_entity"},
            )
            retrieved_profile_entities = [
                record
                for record in retrieved["records"]
                if record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
            ]
            self.assertTrue(retrieved_profile_entities)
            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "prefer",
                    "query": "What tool evidence proves the hook extraction fix was pushed?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 2},
                }
            )
            selected = pack["selected_refs"]
            self.assertLessEqual(pack["used_context_tokens"], 120)
            self.assertLessEqual(len(selected), 2)
            self.assertEqual(len(selected), pack["retrieval_metrics"]["selected_refs"])
            layer_budget = pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertEqual(len(selected), layer_budget["total_selected_refs"])
            self.assertLessEqual(layer_budget["total_selected_tokens"], pack["retrieval_metrics"]["remote_context_budget_tokens"])
            self.assertGreaterEqual(layer_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_extraction_phase"]["final"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_entity_type"]["tool_evidence"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_source_role"]["tool"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_hook_type"]["hook_boundary"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["final_session_boundary_ref_count"], 1)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("session_continuity") == "cross_session"
                    and "Exit code: 0" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in selected
                )
            )
            decision_pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "prefer",
                    "query": "What did the assistant decide was done?",
                    "max_context_tokens": 120,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 2},
                }
            )
            self.assertLessEqual(decision_pack["used_context_tokens"], 120)
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("entity_type") == "assistant_decision"
                    and "Commit d0152479 pushed" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in decision_pack["selected_refs"]
                )
            )
            self.assertTrue(
                any(
                    record.get("record_type") == "context_summary_dirty"
                    and record.get("dirty_reason") == "profile_entity_promoted"
                    for record in records
                )
            )
            self.assertFalse(
                any(
                    record.get("record_type") == "context_summary"
                    and record.get("node_path") == ["tenant:tenant_profile", "user:user_profile", "profile:long_term_memory"]
                    for record in records
                )
            )
            refresh = adapter.refresh_summaries(
                {
                    "scope": {"account_id": "acct_profile", "tenant_id": "tenant_profile", "user_id": "user_profile"},
                    "limit": 16,
                    "refreshed_at_ms": 1234567890,
                }
            )
            self.assertGreaterEqual(refresh.get("refreshed_count", 0), 1)
            refreshed_profile = next(
                item
                for item in refresh["refreshed"]
                if item.get("node_path") == ["tenant:tenant_profile", "user:user_profile", "profile:long_term_memory"]
            )
            self.assertIn("assistant", refreshed_profile["source_roles"])
            self.assertIn("tool", refreshed_profile["source_roles"])
            self.assertIn("hook_boundary", refreshed_profile["source_hook_types"])
            self.assertIn("Stop", refreshed_profile["source_codex_events"])
            self.assertIn("user_profile", refreshed_profile["source_memory_scopes"])
            self.assertIn("cross_session", refreshed_profile["source_session_continuities"])
            self.assertIn("final", refreshed_profile["source_extraction_phases"])
            self.assertGreaterEqual(refreshed_profile["source_final_session_boundary_count"], 1)
            records = adapter.read_all()
            profile_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_path") == ["tenant:tenant_profile", "user:user_profile", "profile:long_term_memory"]
            ]
            self.assertTrue(profile_summaries)
            self.assertTrue(any(record.get("summary_type") == "node_l0" for record in profile_summaries))
            self.assertTrue(any("assistant" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any("tool" in record.get("source_roles", []) for record in profile_summaries))
            self.assertTrue(any("hook_boundary" in record.get("source_hook_types", []) for record in profile_summaries))
            self.assertTrue(any("Stop" in record.get("source_codex_events", []) for record in profile_summaries))
            self.assertTrue(any("user_profile" in record.get("source_memory_scopes", []) for record in profile_summaries))
            self.assertTrue(any("cross_session" in record.get("source_session_continuities", []) for record in profile_summaries))
            self.assertTrue(any("final" in record.get("source_extraction_phases", []) for record in profile_summaries))
            self.assertTrue(any(record.get("source_final_session_boundary_count", 0) >= 1 for record in profile_summaries))
            profile_entity_hashes_by_type = {
                record.get("entity_type"): record.get("entity_hash")
                for record in profile_entities
                if record.get("entity_hash") is not None
            }
            self.assertTrue(
                any(
                    profile_entity_hashes_by_type.get("assistant_decision") in record.get("source_entity_hashes", [])
                    and profile_entity_hashes_by_type.get("tool_evidence") in record.get("source_entity_hashes", [])
                    and "assistant_decision" in record.get("source_entity_types", [])
                    and "tool_evidence" in record.get("source_entity_types", [])
                    for record in profile_summaries
                )
            )
            summary_pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_profile",
                        "tenant_id": "tenant_profile",
                        "user_id": "user_profile",
                        "session_id": "later_session",
                    },
                    "session_scope": "prefer",
                    "query": "Summarize long term memory about assistant decisions and tool evidence.",
                    "max_context_tokens": 90,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 3},
                }
            )
            self.assertLessEqual(summary_pack["used_context_tokens"], 90)
            summary_layer_budget = summary_pack["retrieval_metrics"]["memory_layer_budget"]
            self.assertGreaterEqual(summary_layer_budget["by_memory_scope"]["user_profile"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_session_continuity"]["cross_session"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_extraction_phase"]["final"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_entity_type"]["assistant_decision"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_source_role"]["assistant"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["by_hook_type"]["hook_boundary"]["refs"], 1)
            self.assertGreaterEqual(summary_layer_budget["final_session_boundary_ref_count"], 1)

    def test_profile_entity_updates_preserve_cross_session_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-entity-update.jsonl")
            base_scope = {
                "account_id": "acct_profile_update",
                "tenant_id": "tenant_profile_update",
                "user_id": "user_profile_update",
            }
            first = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_1"},
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Done. Commit aaa111 pushed and profile memory update tests passed.",
                        }
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )
            second = adapter.batch_extract(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_2"},
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Done. Commit bbb222 pushed and cross-session lineage was preserved.",
                        }
                    ],
                    "metadata": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "force": True,
                }
            )

            self.assertGreaterEqual(first.get("profile_entities_written", 0), 1)
            self.assertGreaterEqual(second.get("profile_entities_written", 0), 1)
            records = adapter.read_all()
            profile_decisions = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and record.get("memory_scope") == "user_profile"
                and record.get("session_continuity") == "cross_session"
                and record.get("entity_type") == "assistant_decision"
            ]
            self.assertEqual(1, len(profile_decisions))
            profile_decision = profile_decisions[0]
            self.assertEqual(
                ["session_profile_update_1", "session_profile_update_2"],
                profile_decision["source_session_ids"],
            )
            self.assertGreaterEqual(len(profile_decision["source_entity_hashes"]), 2)
            self.assertIn("assistant", profile_decision["source_roles"])
            self.assertIn("hook_boundary", profile_decision["source_hook_types"])
            self.assertIn("Stop", profile_decision["source_codex_events"])
            self.assertIn("aaa111", profile_decision["state"])
            self.assertIn("bbb222", profile_decision["state"])

            pack = adapter.retrieve(
                {
                    "scope": {**base_scope, "session_id": "session_profile_update_3"},
                    "session_scope": "prefer",
                    "query": "aaa111 bbb222 assistant",
                    "max_context_tokens": 500,
                    "audit_mode": "off",
                    "ranking": {"max_selected_refs": 10},
                }
            )
            self.assertTrue(
                any(
                    ref.get("ref_type") == "entity"
                    and ref.get("memory_scope") == "user_profile"
                    and ref.get("session_continuity") == "cross_session"
                    and "bbb222" in str(ref.get("text") or ref.get("summary_text") or "")
                    for ref in pack["selected_refs"]
                )
            )

    def test_async_resource_import_uses_bounded_worker_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, mock.patch.dict(
            os.environ,
            {
                "MATRIXARK_RESOURCE_IMPORT_WORKERS": "1",
                "MATRIXARK_RESOURCE_IMPORT_QUEUE_MAX": "2",
            },
        ):
            event_log = Path(tmp_dir) / "matrixark-resource-queue.jsonl"
            adapter = MatrixArkLocalAdapter(event_log)
            server = MatrixArkMcpServer(adapter, line_json=True, access_mode="dev")
            result = server.call_tool(
                "matrixark_ingest",
                {
                    "kind": "resource",
                    "raw_uri": "inline://resource-queue.md",
                    "resource_type": "md",
                    "wait": False,
                    "messages": [
                        {
                            "role": "user",
                            "content": "# GPU Runbook\n\nAlice owns the GPU approval checklist. Finance review is required before purchase.",
                        }
                    ],
                    "scope": {
                        "account_id": "acct_queue",
                        "tenant_id": "tenant_queue",
                        "user_id": "user_queue",
                        "session_id": "session_queue",
                    },
                    "metadata": {"node_path": ["users", "user_queue", "resources", "runbooks"]},
                },
            )
            self.assertEqual("queued", result["status"])
            task = result["resource_import_task"]
            self.assertFalse(task["wait"])
            self.assertTrue(task["worker_pool"]["bounded"])
            self.assertEqual(1, task["worker_pool"]["worker_count"])
            self.assertEqual(2, task["worker_pool"]["queue_max"])

            task_hash = task["task_hash"]
            deadline = time.time() + 5.0
            records: list[dict] = []
            while time.time() < deadline:
                records = adapter.read_all()
                if any(
                    record.get("record_type") == "resource_import_task"
                    and record.get("task_hash") == task_hash
                    and record.get("status") == "completed"
                    for record in records
                ):
                    break
                time.sleep(0.05)
            else:
                self.fail("background resource import did not complete")

            self.assertTrue(any(record.get("record_type") == "resource_chunk" for record in records))
            self.assertTrue(any(record.get("record_type") == "context_embedding" for record in records))
            self.assertTrue(any(record.get("record_type") == "context_summary" for record in records))

    def test_tenant_shared_resource_and_skill_live_outside_session_and_retrieve_with_quota(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-shared-context.jsonl"
            resource = tmp / "tenant_policy.md"
            resource.write_text(
                "# Tenant GPU Policy\n\n"
                "Decision: shared GPU purchases require finance approval.\n"
                "Owner: Alice finance.\n",
                encoding="utf-8",
            )
            skill = tmp / "SKILL.md"
            skill.write_text(
                "---\n"
                "name: shared-context-debugger\n"
                "description: Inspect MatrixArk replay and selected refs.\n"
                "triggers:\n  - replay\n"
                "allowed_tools:\n  - matrixark_replay\n"
                "status: active\n"
                "---\n\n"
                "# Shared Context Debugger\n\nUse this skill to inspect replay evidence.\n",
                encoding="utf-8",
            )
            adapter = MatrixArkLocalAdapter(event_log)
            shared_scope = {"account_id": "acct_shared", "tenant_id": "tenant_shared"}
            adapter.ingest(
                {
                    "kind": "resource",
                    "raw_uri": str(resource),
                    "resource_type": "md",
                    "scope": shared_scope,
                    "messages": [{"role": "user", "content": "Import tenant shared resource."}],
                    "wait": True,
                }
            )
            adapter.ingest(
                {
                    "kind": "skill",
                    "raw_uri": str(skill),
                    "resource_type": "skill",
                    "scope": shared_scope,
                    "messages": [{"role": "user", "content": "Import tenant shared skill."}],
                    "wait": True,
                }
            )
            records = adapter.read_all()
            resource_chunks = [row for row in records if row.get("record_type") == "resource_chunk" and row.get("resource_type") != "skill"]
            skill_sections = [row for row in records if row.get("record_type") == "skill_section"]
            self.assertTrue(resource_chunks)
            self.assertTrue(skill_sections)
            resource_manifests = [row for row in records if row.get("record_type") == "resource_manifest"]
            skill_manifests = [row for row in records if row.get("record_type") == "skill_manifest"]
            self.assertTrue(any(row.get("node_path")[:3] == ["tenant:tenant_shared", "shared", "resources"] for row in resource_manifests))
            self.assertTrue(any(row.get("node_path")[:3] == ["tenant:tenant_shared", "shared", "skills"] for row in skill_manifests))
            self.assertTrue(all(row.get("access_scope", {}).get("sharing_scope") == "tenant_shared" for row in resource_chunks + skill_sections))

            pack = adapter.retrieve(
                {
                    "scope": {
                        "account_id": "acct_shared",
                        "tenant_id": "tenant_shared",
                        "user_id": "user_123",
                        "session_id": "active_session",
                    },
                    "query": "Which shared policy requires finance approval and which skill inspects replay evidence?",
                    "max_context_tokens": 1600,
                    "shared_context": {"resource_budget_tokens": 120, "skill_budget_tokens": 120},
                    "audit_mode": "off",
                    "include_retrieval_debug": True,
                }
            )
            ref_types = {ref.get("ref_type") for ref in pack["selected_refs"]}
            self.assertIn("resource_chunk", ref_types)
            self.assertIn("skill_section", ref_types)
            self.assertEqual("tenant_shared", next(ref for ref in pack["selected_refs"] if ref.get("ref_type") == "resource_chunk")["sharing_scope"])
            shared_policy = pack["recall_policy"]["shared_context"]
            self.assertGreaterEqual(shared_policy["resource_selected_ref_count"], 1)
            self.assertGreaterEqual(shared_policy["skill_selected_ref_count"], 1)

    def test_codex_hook_ingests_conversation_resource_and_skill_then_retrieves_all(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "matrixark-codex-hook.jsonl"
            resource = tmp / "gpu_policy.md"
            resource.write_text(
                "# GPU Approval Policy\n\n"
                "Decision: GPU budget requests require finance approval before purchase.\n"
                "Owner: Alice from finance.\n"
                "Deadline: requests must be reviewed within 3 business days.\n",
                encoding="utf-8",
            )
            skill = tmp / "SKILL.md"
            skill.write_text(
                "---\n"
                "name: context-debugger\n"
                "description: Inspect MatrixArk selected refs, dropped refs, and replay evidence.\n"
                "triggers:\n"
                "  - contextpack\n"
                "  - replay evidence\n"
                "allowed_tools:\n"
                "  - matrixark_replay\n"
                "status: active\n"
                "version: '1'\n"
                "---\n\n"
                "# Context Debugger\n\n"
                "Use this skill to inspect selected ContextPack refs and replay evidence.\n",
                encoding="utf-8",
            )

            msg = self.run_hook(
                repo,
                event_log,
                event="UserPromptSubmit",
                payload={
                    "prompt": "Alice asked Codex to verify GPU budget approval workflow.",
                    "thread_id": "codex-thread-1",
                },
            )
            self.assertEqual("ok", msg["status"])
            self.assertTrue(msg["ingest"].get("event_id_hash"))
            self.assertTrue(msg["retrieve"]["context_pack_id"])

            resource_result = self.run_hook(
                repo,
                event_log,
                event="ResourceAdded",
                payload={"raw_uri": str(resource), "resource_type": "md", "thread_id": "codex-thread-1", "wait": True},
            )
            self.assertEqual("ok", resource_result["status"])
            self.assertEqual(str(resource), resource_result["resource_uri"])
            self.assertTrue(resource_result["ingest"].get("resource_chunks"))
            self.assertGreaterEqual(resource_result["ingest"].get("resource_fact_event_count", 0), 1)
            self.assertLessEqual(resource_result["ingest"].get("resource_fact_event_count", 0), 8)

            skill_result = self.run_hook(
                repo,
                event_log,
                event="ResourceAdded",
                payload={"raw_uri": str(skill), "thread_id": "codex-thread-1", "wait": True},
            )
            self.assertEqual("ok", skill_result["status"])
            self.assertEqual("skill", skill_result["resource_type"])
            self.assertIsInstance(skill_result["ingest"].get("skill_hash"), int)

            records = MatrixArkLocalAdapter(event_log).read_all()
            context_tree_records = [
                record
                for record in records
                if record.get("record_type") in {"context_node", "context_child_ref"}
            ]
            self.assertTrue(context_tree_records)
            self.assertFalse(any("status" in record for record in context_tree_records))
            resource_chunks = [
                record
                for record in records
                if record.get("record_type") == "resource_chunk"
                and record.get("resource_type") != "skill"
            ]
            self.assertTrue(resource_chunks)
            self.assertTrue(all(record.get("resource_hash") for record in resource_chunks))
            self.assertFalse(any(record.get("raw_uri") or record.get("source_ref") for record in resource_chunks))
            resource_entities = [
                record
                for record in records
                if record.get("record_type") == "context_entity"
                and str(record.get("entity_type") or "").startswith("resource_")
            ]
            self.assertTrue(resource_entities)
            self.assertFalse(
                any(
                    "/" in str(record.get("entity_name") or "")
                    or "\\" in str(record.get("entity_name") or "")
                    for record in resource_entities
                )
            )
            self.assertFalse(
                any(
                    record.get("record_type") == "context_index"
                    and str(record.get("index_name") or "").lower() == "classification:new_event"
                    for record in records
                )
            )

            server = MatrixArkMcpServer(MatrixArkLocalAdapter(event_log), line_json=True, access_mode="dev")
            scope = {
                "account_id": "acct_hook",
                "tenant_id": "tenant_hook",
                "user_id": "codex_user",
                "session_id": "codex_session_1",
            }
            pack = server.call_tool(
                "matrixark_retrieve",
                {
                    "scope": scope,
                    "query": "Which policy requires finance approval and which skill inspects replay evidence?",
                    "max_context_tokens": 1200,
                    "audit_mode": "full",
                },
            )
            ref_types = {str(ref.get("ref_type")) for ref in pack["selected_refs"]}
            self.assertIn("event", ref_types)
            self.assertIn("resource_chunk", ref_types)
            self.assertIn("skill_section", ref_types)
            self.assertGreaterEqual(sum(1 for ref in pack["selected_refs"] if ref.get("ref_type") == "resource_chunk"), 1)
            self.assertGreaterEqual(sum(1 for ref in pack["selected_refs"] if ref.get("ref_type") == "skill_section"), 1)
            self.assertNotIn("context_assembly_policy", pack)
            self.assertNotIn("recall_policy", pack)
            audit = next(
                record
                for record in reversed(server.adapter.read_all())
                if record.get("record_type") == "context_pack_audit"
                and isinstance(record.get("backend_retrieval_pushdown"), dict)
            )
            pushdown = audit["backend_retrieval_pushdown"]
            self.assertIn(pushdown["execution_mode"], {"adapter_prefilter", "adapter_prefilter_cached"})
            self.assertGreater(pushdown["dropped_by_type"], 0)
            context_pack_id = pack.get("context_pack_id") or pack.get("pack_id")
            replay = server.call_tool("matrixark_replay", {"scope": scope, "context_pack_id": context_pack_id, "enable_replay": True})
            audits = [row for row in replay["events"] if row.get("record_type") == "context_pack_audit"]
            self.assertTrue(any(row.get("context_pack_id") == context_pack_id for row in audits))


if __name__ == "__main__":
    unittest.main()
