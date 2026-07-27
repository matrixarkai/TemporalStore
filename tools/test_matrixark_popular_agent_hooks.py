#!/usr/bin/env python3
"""Regression tests for supported Codex hook plus planned-agent TODOs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import matrixark_agent_config
import matrixark_agent_hook


class MatrixArkPopularAgentHooksTest(unittest.TestCase):
    def run_agent_hook(self, *, agent: str, event: str, payload: dict, rust_root: Path, extra: list[str] | None = None) -> dict:
        repo = Path(__file__).resolve().parents[1]
        cmd = [
            sys.executable,
            str(repo / "tools" / "matrixark_agent_hook.py"),
            "--agent",
            agent,
            "--event",
            event,
            "--backend",
            "temporalstore-rust",
            "--metaserver",
            "local",
            "--namespace",
            "deploy_ns",
            "--table",
            "deploy_table",
            "--rust-proxy",
            os.environ.get(
                "MATRIXARK_TEST_RUST_PROXY",
                "/root/src/github-services/TemporalStore/target/release/matrixark_rust_proxy",
            ),
            "--storage-prefix",
            f"matrixark:test-agent-hook:{agent}",
            "--account-id",
            "acct_agents",
            "--tenant-id",
            "tenant_agents",
            "--user-id",
            "agent_user",
            "--team",
            "agent",
            "--project",
            "integration",
        ]
        if extra:
            cmd.extend(extra)
        env = dict(os.environ)
        env.update(
            {
                "MATRIXARK_MCP_BACKEND": "temporalstore-rust",
                "MATRIXARK_LOCAL_MODE": "no-metaserver",
                "MATRIXARK_TEMPORALSTORE_METASERVER": "local",
                "MATRIXARK_TEMPORALSTORE_RUST_ROOT": str(rust_root),
                "MATRIXARK_RUST_PROXY_ASYNC_STORAGE": "true",
                "MATRIXARK_HOOK_STORAGE_ROUTE": "shared_store_async",
            }
        )
        proc = subprocess.run(
            cmd,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            cwd=repo,
            env=env,
            timeout=30,
        )
        if proc.returncode != 0:
            raise AssertionError(f"agent hook failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return json.loads(proc.stdout)

    def test_codex_prompt_hook_ingests_retrieves_and_preserves_visible_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rust_root = Path(tmp_dir) / "rust-store"
            result = self.run_agent_hook(
                agent="codex",
                event="UserPromptSubmit",
                rust_root=rust_root,
                payload={
                    "prompt": "Remember that Codex owns the GPU release checklist.",
                    "conversation_id": "codex-thread-42",
                    "workspace_root": "/repo/aurora",
                    "local_context": [
                        {"ref": "open-file:docs/release.md", "text": "Visible release checklist notes."}
                    ],
                    "local_context_tokens": 12,
                    "max_context_tokens": 512,
                },
            )
            self.assertEqual("ok", result["status"])
            self.assertEqual("codex", result["agent"])
            self.assertEqual("codex:codex-thread-42", result["session_id"])
            self.assertEqual("payload_field", result["session_id_source"])
            self.assertEqual(1, result["agent_context_refs"])
            self.assertTrue(result["ingested"])
            self.assertGreaterEqual(result["retrieved"]["selected_ref_count"], 0)
            self.assertEqual("payload_field", result["retrieved"]["session_identity"]["session_id_source"])
            self.assertTrue(result["retrieved"]["session_identity"]["strong_session_identity"])
            self.assertFalse(result["retrieved"]["session_identity"]["fallback_session_identity"])
            self.assertNotIn("session_identity_fallback:payload_field", result["retrieved"]["quality_warnings"])
            self.assertIn("memory_layer_budget", result["retrieved"])
            self.assertIn("layer_summary", result["retrieved"])
            self.assertEqual(
                result["retrieved"]["memory_layer_budget"],
                result["retrieved"]["layer_summary"].get("memory_layer_budget", {}),
            )
            self.assertEqual(
                "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains bounded",
                result["retrieved"]["memory_hierarchy_contract"]["retrieval_strategy"],
            )

    def test_generic_agent_retrieval_summary_exposes_memory_layer_pressure(self) -> None:
        retrieve = {
            "context_pack_id": "pack-pressure",
            "selected_refs": [{"ref_type": "entity", "text": "assistant decision memory"}],
            "retrieval_metrics": {
                "memory_layer_budget": {
                    "total_selected_refs": 1,
                    "total_selected_tokens": 8,
                    "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 8}},
                },
                "memory_layer_pressure": {
                    "selected_refs": 1,
                    "selected_tokens": 8,
                    "dropped_refs": 2,
                    "dropped_tokens": 19,
                    "pressure_dimensions": ["by_memory_scope", "by_source_role"],
                    "dropped_dimensions": ["by_memory_scope", "by_source_role"],
                    "profile_memory_pressure": True,
                    "assistant_memory_pressure": True,
                    "pressure_bucket_count": 1,
                    "dropped_bucket_count": 2,
                },
            },
            "recall_policy": {
                "session_identity": {
                    "session_id_source": "payload_field",
                    "strong_session_identity": True,
                    "fallback_session_identity": False,
                },
                "cross_session": {
                    "enabled": True,
                    "budget_tokens": 18,
                    "remote_budget_tokens": 90,
                    "computed_budget_tokens": 18,
                    "budget_floor_tokens": 256,
                    "budget_floor_applied": False,
                    "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                    "max_sessions": 3,
                    "max_candidates": 24,
                },
            },
        }

        summary = matrixark_agent_hook.agent_retrieval_summary(
            retrieve,
            session_id_source="payload_field",
        )

        self.assertEqual("pack-pressure", summary["context_pack_id"])
        self.assertEqual(2, summary["memory_layer_pressure"]["dropped_refs"])
        self.assertTrue(summary["memory_layer_pressure"]["profile_memory_pressure"])
        self.assertEqual(
            summary["layer_summary"]["memory_layer_pressure"],
            summary["memory_layer_pressure"],
        )
        hierarchy = summary["memory_hierarchy_contract"]
        self.assertTrue(hierarchy["cross_session_enabled"])
        self.assertEqual(18, hierarchy["cross_session_budget_tokens"])
        self.assertEqual(90, hierarchy["cross_session_remote_budget_tokens"])
        self.assertEqual(18, hierarchy["cross_session_computed_budget_tokens"])
        self.assertEqual(256, hierarchy["cross_session_budget_floor_tokens"])
        self.assertFalse(hierarchy["cross_session_budget_floor_applied"])
        self.assertEqual(
            "remote_budget_too_small_for_profile_floor",
            hierarchy["cross_session_budget_floor_status"],
        )
        self.assertEqual(3, hierarchy["cross_session_max_sessions"])
        self.assertEqual(24, hierarchy["cross_session_max_candidates"])

    def test_generic_hook_idle_commit_is_reported_as_auto_batch_decision(self) -> None:
        ingest = {
            "session_buffer": {
                "pending_event_count": 1,
                "pending_message_count": 1,
                "threshold_messages": 20,
                "threshold_ready": False,
                "idle_ready": True,
                "auto_batch_extract": True,
            },
            "auto_batch_extract_result": {},
            "idle_commit_result": {
                "status": "committed",
                "trigger_policy": "idle_timeout",
                "commit_reason": "idle_timeout",
                "source_roles": ["user"],
                "source_hook_types": ["user_prompt_submit"],
                "source_codex_events": ["UserPromptSubmit"],
                "profile_promotion_summary": [{"entity_name": "repo location"}],
                "trigger_evidence": {
                    "pending_event_count": 1,
                    "threshold_ready": False,
                    "idle_ready": True,
                    "idle_timeout_ms": 1,
                },
            },
        }

        session_buffer = matrixark_agent_hook.normalized_session_buffer_from_ingest(ingest)
        decision = matrixark_agent_hook.auto_batch_decision_summary(ingest)

        self.assertTrue(session_buffer["idle_ready"])
        self.assertFalse(session_buffer["threshold_ready"])
        self.assertEqual("idle_commit", decision["decision"])
        self.assertEqual("committed", decision["auto_batch_extract_status"])
        self.assertEqual("idle_timeout", decision["reason"])
        self.assertEqual(["user"], decision["source_roles"])

    def test_generic_hook_threshold_extracts_user_and_assistant_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rust_root = Path(tmp_dir) / "rust-store"
            threshold_args = ["--session-commit-threshold", "2"]
            first = self.run_agent_hook(
                agent="codex",
                event="UserPromptSubmit",
                rust_root=rust_root,
                extra=threshold_args,
                payload={
                    "prompt": "Should live memory wait for Stop before extracting?",
                    "conversation_id": "codex-threshold-session",
                    "workspace_root": "/repo/memory",
                },
            )
            self.assertTrue(first["ingested"])
            self.assertEqual(1, first["ingest"]["session_buffer"]["pending_event_count"])
            self.assertTrue(first["ingest"]["session_buffer"]["auto_batch_extract"])
            self.assertFalse(first["ingest"]["session_buffer"]["threshold_ready"])
            self.assertEqual({}, first["auto_batch_extract"])

            second = self.run_agent_hook(
                agent="codex",
                event="response",
                rust_root=rust_root,
                extra=threshold_args,
                payload={
                    "messages": [
                        {
                            "role": "user",
                            "content": "Should live memory wait for Stop before extracting?",
                        },
                        {
                            "role": "assistant",
                            "content": (
                                "Decision: live memory should use threshold and idle provisional extraction; "
                                "Stop remains the final session boundary."
                            ),
                        }
                    ],
                    "conversation_id": "codex-threshold-session",
                    "workspace_root": "/repo/memory",
                },
            )
            self.assertTrue(second["ingested"])
            self.assertGreaterEqual(second["ingest"]["session_buffer"]["pending_event_count"], 1)
            self.assertEqual(2, second["ingest"]["session_buffer"]["pending_message_count"])
            self.assertTrue(second["ingest"]["session_buffer"]["threshold_ready"])
            self.assertEqual("committed", second["auto_batch_extract"]["status"])
            self.assertEqual("threshold", second["auto_batch_extract"]["trigger_policy"])
            self.assertEqual("provisional", second["auto_batch_extract"]["extraction_phase"])
            self.assertFalse(second["auto_batch_extract"]["final_session_boundary"])
            self.assertGreaterEqual(second["auto_batch_extract"]["source_event_count"], 1)
            self.assertEqual(2, second["auto_batch_extract"]["trigger_evidence"]["pending_message_count"])
            self.assertEqual(["assistant", "user"], second["auto_batch_extract"]["source_roles"])
            self.assertGreaterEqual(second["auto_batch_extract"]["session_entities_written"], 1)
            self.assertGreaterEqual(second["auto_batch_extract"]["profile_entities_written"], 1)

    def test_generic_hook_post_tool_use_extracts_selected_tool_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rust_root = Path(tmp_dir) / "rust-store"
            noisy_output = "\n".join(
                [
                    "Compiling generated module with verbose progress that should not be durable.",
                    "warning: unused import in unrelated generated file",
                    "Exit code: 0",
                    "Ran 89 tests in 4.51s",
                    "OK",
                    "To https://github.com/bjmeetsfo/TemporalStore.git",
                    "ca8f8a96 HEAD -> main",
                    "More streaming output that should be ignored.",
                ]
            )
            result = self.run_agent_hook(
                agent="codex",
                event="PostToolUse",
                rust_root=rust_root,
                extra=["--session-commit-threshold", "1"],
                payload={
                    "conversation_id": "codex-tool-session",
                    "workspace_root": "/repo/memory",
                    "tool_name": "shell_command",
                    "tool_status": "ok",
                    "terminal_output": noisy_output,
                },
            )

            self.assertTrue(result["ingested"])
            self.assertEqual(1, result["ingest"]["session_buffer"]["pending_message_count"])
            self.assertEqual("committed", result["auto_batch_extract"]["status"])
            self.assertEqual("threshold", result["auto_batch_extract"]["trigger_policy"])
            self.assertEqual(["tool"], result["auto_batch_extract"]["source_roles"])
            self.assertEqual(["tool_result"], result["auto_batch_extract"]["source_hook_types"])
            self.assertGreaterEqual(result["auto_batch_extract"]["entity_type_counts"]["tool_evidence"], 1)
            self.assertGreaterEqual(result["auto_batch_extract"]["session_entities_written"], 1)
            self.assertGreaterEqual(result["auto_batch_extract"]["profile_entities_written"], 1)

            retrieval = self.run_agent_hook(
                agent="codex",
                event="UserPromptSubmit",
                rust_root=rust_root,
                extra=["--query", "ca8f8a96 HEAD -> main Exit code 0 OK"],
                payload={
                    "prompt": "Continue with retrieved tool evidence.",
                    "conversation_id": "codex-tool-session",
                    "workspace_root": "/repo/memory",
                    "max_context_tokens": 512,
                },
            )
            self.assertGreaterEqual(retrieval["retrieved"]["selected_ref_count"], 1)
            layer_budget = retrieval["retrieved"]["memory_layer_budget"]
            self.assertGreaterEqual(
                layer_budget.get("by_entity_type", {}).get("tool_evidence", {}).get("refs", 0),
                1,
                layer_budget,
            )
            self.assertGreaterEqual(layer_budget["by_source_role"]["tool"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_hook_type"]["tool_result"]["refs"], 1)
            self.assertGreaterEqual(layer_budget["by_memory_scope"]["session"]["refs"], 1)
            self.assertGreaterEqual(
                layer_budget["by_session_continuity"]["same_session"]["refs"],
                1,
            )

    def test_planned_agent_configs_are_marked_todo_not_supported_hooks(self) -> None:
        snippet = json.loads(matrixark_agent_config.openclaw_json(".", "tools/matrixark_mcp_rust_server.sh"))
        self.assertEqual("openclaw", snippet["agent"])
        self.assertEqual("todo_planned", snippet["hook_status"])
        self.assertNotIn("recommended_hook_command", snippet)
        self.assertEqual("temporalstore-rust", snippet["server"]["env"]["MATRIXARK_MCP_BACKEND"])

        for agent in ("opencode", "aider", "continue", "cline", "roo"):
            planned = json.loads(matrixark_agent_config.named_agent_json(agent, ".", "tools/matrixark_mcp_cpp_server.sh"))
            self.assertEqual(snippet["envelope"]["schema"], "matrixark_agent_envelope_v1")
            self.assertEqual("todo_planned", planned["hook_status"])
            self.assertNotIn("recommended_hook_command", planned)

    def test_agent_config_exposes_codex_supported_hook_and_todo_agents(self) -> None:
        self.assertEqual(matrixark_agent_config.SUPPORTED_AGENT_CLIENTS, ["codex"])
        self.assertEqual(matrixark_agent_config.SUPPORTED_HOOK_CLIENTS, ["codex"])
        self.assertIn("claude", matrixark_agent_config.TODO_AGENT_CLIENTS)
        self.assertIn("openclaw", matrixark_agent_config.TODO_AGENT_CLIENTS)
        envelope = matrixark_agent_config.agent_envelope_schema()
        self.assertTrue(envelope["visible_local_context_only"])
        self.assertIn("query", envelope["fields"])
        self.assertIn("scope", envelope["fields"])
        self.assertIn("local_context_tokens", envelope["fields"])
        self.assertIn("max_context_tokens", envelope["fields"])
        self.assertIn("lifecycle_event_type", envelope["fields"])
        self.assertIn("file_refs", envelope["optional_fields"])
        self.assertIn("resource_refs", envelope["optional_fields"])
        self.assertEqual(envelope["required_fields_by_lifecycle"]["before_llm"], ["query"])
        self.assertEqual(envelope["required_fields_by_lifecycle"]["after_answer"], ["messages"])
        self.assertEqual(envelope["lifecycle_tools"]["before_llm"], "matrixark_retrieve")
        self.assertEqual(envelope["lifecycle_tools"]["after_answer"], "matrixark_ingest")
        self.assertEqual(envelope["lifecycle_tools"]["session_boundary"], "matrixark_session_commit")
        memory_policy = envelope["memory_extraction_policy"]
        self.assertEqual("raw", memory_policy["live_ingest"]["phase"])
        self.assertEqual("provisional", memory_policy["threshold_checkpoint"]["extraction_phase"])
        self.assertFalse(memory_policy["threshold_checkpoint"]["final_session_boundary"])
        self.assertEqual("provisional", memory_policy["idle_checkpoint"]["extraction_phase"])
        self.assertFalse(memory_policy["idle_checkpoint"]["final_session_boundary"])
        self.assertEqual("final", memory_policy["final_boundary"]["extraction_phase"])
        self.assertTrue(memory_policy["final_boundary"]["final_session_boundary"])
        self.assertIn("Stop", memory_policy["final_boundary"]["events"])
        self.assertIn("SubagentStop", memory_policy["final_boundary"]["events"])
        self.assertIn("PostCompact", memory_policy["final_boundary"]["events"])
        retrieval_policy = envelope["retrieval_budget_policy"]
        self.assertTrue(retrieval_policy["local_context_first"])
        self.assertTrue(retrieval_policy["remote_fills_remaining_budget"])
        self.assertEqual("lower_than_final", retrieval_policy["provisional_memory_confidence"])
        self.assertEqual("off", retrieval_policy["debug_default"])
        self.assertIn("include_retrieval_metrics", retrieval_policy["debug_fields"])
        self.assertIn("ContextEvent", envelope["agent_internal_model_hidden"])
        self.assertIn("ContextSummary", envelope["agent_internal_model_hidden"])
        self.assertIn("hidden prompt", envelope["do_not_send"])

    def test_agent_policy_text_documents_provisional_and_final_extraction(self) -> None:
        policy = matrixark_agent_config.agent_policy_text()
        required = [
            "do not wait only for Stop",
            "thresholds and idle timeouts call matrixark_session_commit as provisional",
            "checkpoints with extraction_phase=provisional",
            "extraction_phase=provisional",
            "final_session_boundary=false",
            "Stop, SubagentStop, and PostCompact call matrixark_session_commit as the final",
            "session boundary with extraction_phase=final",
            "extraction_phase=final",
            "final_session_boundary=true",
            "Retrieval keeps visible",
            "local context plus a safety margin first",
            "Retrieval metrics and debug ContextPacks are opt-in audit fields",
        ]
        for snippet in required:
            self.assertIn(snippet, policy)

    def test_hook_examples_only_emit_codex_commands(self) -> None:
        examples = matrixark_agent_config.hook_examples_text(".")
        self.assertIn("--agent codex --event UserPromptSubmit", examples)
        self.assertIn("TODO/planned", examples)
        self.assertNotIn("--agent claude --event UserPromptSubmit", examples)
        self.assertNotIn("--agent openclaw --event UserPromptSubmit", examples)


if __name__ == "__main__":
    unittest.main()
