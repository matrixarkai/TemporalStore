#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Regression tests for supported Codex hook plus planned-agent TODOs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

import matrixark_agent_config
import matrixark_agent_hook
import matrixark_mcp_local_adapter

DEFAULT_RUST_PROXY = "/opt/github-services/TemporalStore/target/release/matrixark_rust_proxy"


def require_rust_proxy() -> str:
    """The native proxy these hook tests drive, or a SKIP if it has not been built.

    These tests run an agent hook end to end against the native backend, which refuses to fall back
    to the Python path when it is serving. Without the binary the hook cannot start, and the test
    reported a failure that said nothing about the code -- on a suite gate that is already red.

    The path is a machine-specific absolute one and the job that runs this suite builds no Rust, so
    the honest report there is "not exercised", not "broken". Set MATRIXARK_TEST_RUST_PROXY to run
    them against a proxy kept elsewhere.
    """
    path = os.environ.get("MATRIXARK_TEST_RUST_PROXY", DEFAULT_RUST_PROXY)
    if not os.path.exists(path):
        raise unittest.SkipTest(
            "no native proxy at %s, so an end-to-end agent hook cannot be driven; set "
            "MATRIXARK_TEST_RUST_PROXY to point at one" % path)
    return path


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
            require_rust_proxy(),
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
                "MATRIXARK_DISABLE_IDLE_COMMIT_WORKER": "1",
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

    def test_claude_prompt_hook_ingests_retrieves_and_preserves_visible_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rust_root = Path(tmp_dir) / "rust-store"
            result = self.run_agent_hook(
                agent="claude",
                event="UserPromptSubmit",
                rust_root=rust_root,
                payload={
                    "prompt": "Remember that Claude owns the GPU release checklist.",
                    "conversation_id": "claude-thread-42",
                    "workspace_root": "/repo/aurora",
                    "local_context": [
                        {"ref": "open-file:docs/release.md", "text": "Visible release checklist notes."}
                    ],
                    "local_context_tokens": 12,
                    "max_context_tokens": 512,
                },
            )
            self.assertEqual("ok", result["status"])
            self.assertEqual("claude", result["agent"])
            self.assertEqual("claude:claude-thread-42", result["session_id"])
            self.assertEqual("payload_field", result["session_id_source"])
            self.assertEqual(1, result["agent_context_refs"])
            self.assertTrue(result["ingested"])
            self.assertGreaterEqual(result["retrieved"]["selected_ref_count"], 0)

    def test_codex_and_claude_hooks_have_ingestion_extraction_retrieval_parity(self) -> None:
        """Codex and Claude hooks run the same ingest/extract/retrieve pipeline,
        capturing user prompts and LLM responses, differing only by agent identity
        and session scope."""
        agents = ("codex", "claude")
        results: dict[str, dict] = {}
        with tempfile.TemporaryDirectory() as tmp_dir:
            for agent in agents:
                rr = Path(tmp_dir) / f"{agent}-store"
                ingest = self.run_agent_hook(
                    agent=agent, event="UserPromptSubmit", rust_root=rr,
                    payload={"prompt": f"Remember that {agent} owns the GPU release checklist.",
                             "conversation_id": f"{agent}-parity"},
                )
                retrieve = self.run_agent_hook(
                    agent=agent, event="UserPromptSubmit", rust_root=rr,
                    payload={"prompt": "Who owns the GPU release checklist?",
                             "conversation_id": f"{agent}-parity"},
                )
                # Stop carries the LLM response via last_assistant_message (Claude Code's key).
                commit = self.run_agent_hook(
                    agent=agent, event="Stop", rust_root=rr,
                    payload={"last_assistant_message": f"{agent} confirmed the owner is {agent}.",
                             "conversation_id": f"{agent}-parity"},
                )
                results[agent] = {"ingest": ingest, "retrieve": retrieve, "commit": commit}

        for agent in agents:
            s = results[agent]
            # Ingestion conformance.
            self.assertEqual("ok", s["ingest"]["status"], agent)
            self.assertTrue(s["ingest"]["ingested"], agent)
            self.assertEqual(agent, s["ingest"]["agent"], agent)
            self.assertEqual(f"{agent}:{agent}-parity", s["ingest"]["session_id"], agent)
            # Retrieval conformance.
            self.assertEqual("ok", s["retrieve"]["status"], agent)
            self.assertGreaterEqual(s["retrieve"]["retrieved"]["selected_ref_count"], 0, agent)
            # LLM-response ingestion conformance (assistant message on Stop).
            self.assertEqual("ok", s["commit"]["status"], agent)
            self.assertTrue(s["commit"]["ingested"], agent)

        # Structural conformance: identical result shape per stage across the two agents.
        for stage in ("ingest", "retrieve", "commit"):
            self.assertEqual(
                set(results["codex"][stage].keys()),
                set(results["claude"][stage].keys()),
                f"{stage} result-shape parity",
            )

        # Decision conformance: query/config-derived retrieval decisions are identical
        # across agents (only identity/scope differ). This is substantive
        # ingestion/extraction/retrieval conformance beyond result-shape conformance.
        codex_ret = results["codex"]["retrieve"]["retrieved"]
        claude_ret = results["claude"]["retrieve"]["retrieved"]
        self.assertEqual(
            codex_ret["memory_hierarchy_contract"]["retrieval_strategy"],
            claude_ret["memory_hierarchy_contract"]["retrieval_strategy"],
            "retrieval strategy parity",
        )
        self.assertEqual(
            codex_ret["session_identity"]["strong_session_identity"],
            claude_ret["session_identity"]["strong_session_identity"],
            "session identity strength parity",
        )
        self.assertTrue(
            codex_ret["session_identity"]["strong_session_identity"],
            "both agents resolve a strong session identity from the payload",
        )

    def test_claude_hook_default_auto_routes_through_shared_pipeline(self) -> None:
        """With the shared proxy present, the Claude hook's default (auto) backend
        routes through tools/matrixark_agent_hook.py -- the SAME ingest/extract/
        retrieve pipeline the Codex hook uses -- not the separate offline rust engine.
        The shared pipeline writes to MATRIXARK_TEMPORALSTORE_RUST_ROOT; the offline
        engine uses a different root, so that store's creation proves auto->shared."""
        repo = Path(__file__).resolve().parents[1]
        proxy = os.environ.get(
            "MATRIXARK_TEST_RUST_PROXY",
            str(repo / "target" / "debug" / "matrixark_rust_proxy"),
        )
        if not os.access(proxy, os.X_OK):
            self.skipTest("shared rust proxy not built")
        hook = repo / "tools" / "matrixark_claude_hook.sh"
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared-store"
            env = dict(os.environ)
            env.pop("MATRIXARK_CLAUDE_HOOK_BACKEND", None)  # exercise the default (auto)
            env.update(
                {
                    "MATRIXARK_TEST_RUST_PROXY": proxy,
                    "MATRIXARK_TEMPORALSTORE_RUST_ROOT": str(shared_root),
                }
            )
            proc = subprocess.run(
                ["bash", str(hook), "--event", "UserPromptSubmit"],
                input=json.dumps({"prompt": "auto routing probe", "session_id": "claude-auto"}),
                text=True,
                capture_output=True,
                cwd=repo,
                env=env,
                timeout=60,
            )
            self.assertEqual(0, proc.returncode, proc.stderr)
            json.loads(proc.stdout)  # valid Claude Code hook contract JSON
            self.assertTrue(
                shared_root.exists(),
                "default (auto) backend did not route through the shared matrixark_agent_hook.py pipeline",
            )

    def test_generic_agent_retrieval_summary_exposes_memory_layer_pressure(self) -> None:
        retrieve = {
            "context_pack_id": "pack-pressure",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "text": "assistant decision memory",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                }
            ],
            "retrieval_metrics": {
                "memory_layer_budget": {
                    "total_selected_refs": 1,
                    "total_selected_tokens": 8,
                    "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 8}},
                    "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 8}},
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
        self.assertEqual("ok", summary["memory_layer_coverage"]["status"])
        self.assertEqual([], summary["memory_layer_coverage"]["gaps"])
        self.assertEqual(1, summary["memory_layer_coverage"]["profile_memory_refs"])
        self.assertEqual(1, summary["memory_layer_coverage"]["cross_session_refs"])
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

    def test_generic_agent_retrieval_summary_flags_missing_memory_layer_coverage(self) -> None:
        summary = matrixark_agent_hook.agent_retrieval_summary(
            {
                "context_pack_id": "pack-empty",
                "selected_refs": [],
                "retrieval_metrics": {"memory_layer_budget": {}},
            },
            session_id_source="payload_field",
        )

        coverage = summary["memory_layer_coverage"]
        self.assertEqual("gap", coverage["status"])
        self.assertIn("retrieval:no_remote_refs_selected", coverage["gaps"])
        self.assertIn("retrieval:no_session_or_profile_memory_selected", coverage["gaps"])
        self.assertIn("retrieval:no_session_continuity_refs_selected", coverage["gaps"])
        self.assertIn("retrieval:memory_layer_budget_missing_selected_refs", coverage["gaps"])

    def test_generic_hook_can_fail_closed_on_retrieval_memory_coverage_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo = Path(__file__).resolve().parents[1]
            rust_root = Path(tmp_dir) / "rust-store"
            cmd = [
                sys.executable,
                str(repo / "tools" / "matrixark_agent_hook.py"),
                "--agent",
                "codex",
                "--event",
                "UserPromptSubmit",
                "--backend",
                "temporalstore-rust",
                "--metaserver",
                "local",
                "--namespace",
                "deploy_ns",
                "--table",
                "deploy_table",
                "--rust-proxy",
                require_rust_proxy(),
                "--storage-prefix",
                "matrixark:test-retrieval-coverage-gap",
                "--account-id",
                "acct_agents",
                "--tenant-id",
                "tenant_agents",
                "--user-id",
                "agent_user",
                "--require-retrieval-memory-coverage",
            ]
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
                input=json.dumps(
                    {
                        "prompt": "Query should fail closed when no session/profile memory is retrievable.",
                        "conversation_id": "codex-empty-retrieval",
                    }
                ),
                text=True,
                capture_output=True,
                cwd=repo,
                env=env,
                timeout=30,
            )

        self.assertEqual(3, proc.returncode, proc.stderr)
        output = json.loads(proc.stdout)
        self.assertEqual("retrieval_memory_coverage_gap", output["status"])
        self.assertTrue(output["retrieval_memory_coverage_required"])
        self.assertEqual("gap", output["retrieved"]["memory_layer_coverage"]["status"])
        self.assertIn(
            "retrieval:no_session_or_profile_memory_selected",
            output["retrieved"]["memory_layer_coverage"]["gaps"],
        )

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

    def test_generic_agent_idle_commit_worker_child_spawns_due_session_commit(self) -> None:
        launched: list[dict] = []
        original_popen = matrixark_agent_hook.subprocess.Popen
        original_time = matrixark_agent_hook.time.time

        class FakePopen:
            def __init__(self, cmd, **kwargs):
                launched.append({"cmd": cmd, "kwargs": kwargs})

        try:
            matrixark_agent_hook.subprocess.Popen = FakePopen
            matrixark_agent_hook.time.time = lambda: 2000.0
            with tempfile.TemporaryDirectory() as tmp_dir:
                args = Namespace(
                    agent="generic",
                    event="response",
                    backend="temporalstore-rust",
                    api_key="",
                    account_id="acct_agent_idle_worker",
                    tenant_id="tenant_agent_idle_worker",
                    user_id="user_agent_idle_worker",
                    session_id="generic:session-agent-idle-worker",
                    team="agent",
                    project="memory",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=300,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                    request_timeout_ms=60000,
                    io_timeout_ms=60000,
                    repo_root=Path(tmp_dir),
                    metaserver="local",
                    namespace="deploy_ns",
                    table="deploy_table",
                    temporalstore_lib="",
                    rust_proxy="/tmp/matrixark_rust_proxy",
                    rust_direct_sdk="",
                    rust_cli="",
                    storage_prefix="matrixark:agent-hook-test",
                    session_state_dir=Path(tmp_dir) / "sessions",
                    idle_commit_worker_only=False,
                )

                result = matrixark_agent_hook.spawn_idle_commit_worker_child(
                    args,
                    ingest={
                        "session_buffer": {
                            "idle_commit_scheduled": True,
                            "idle_commit_deadline_ms": 2000300,
                            "idle_commit_cutoff_ms": 2000000,
                        },
                        "auto_batch_extract_result": {
                            "trigger_policy": "idle_timeout",
                        },
                    },
                    session_id_source="payload_field",
                )
        finally:
            matrixark_agent_hook.subprocess.Popen = original_popen
            matrixark_agent_hook.time.time = original_time

        self.assertEqual("spawned", result["status"])
        self.assertEqual(300, result["delay_ms"])
        self.assertEqual(1, len(launched))
        cmd = launched[0]["cmd"]
        self.assertIn("--idle-commit-worker-only", cmd)
        self.assertIn("--agent", cmd)
        self.assertIn("generic", cmd)
        self.assertIn("--event", cmd)
        self.assertIn("IdleTimeout", cmd)
        self.assertIn("--session-id", cmd)
        self.assertIn("generic:session-agent-idle-worker", cmd)
        self.assertIn("--idle-commit-cutoff-ms", cmd)
        self.assertIn("2000000", cmd)
        env = launched[0]["kwargs"]["env"]
        self.assertEqual("300", env["MATRIXARK_IDLE_COMMIT_WORKER_DELAY_MS"])
        self.assertEqual("2000000", env["MATRIXARK_IDLE_COMMIT_CUTOFF_MS"])
        self.assertEqual("response", env["MATRIXARK_IDLE_COMMIT_WORKER_PARENT_EVENT"])

    def test_generic_agent_hook_compacts_assistant_response_memory(self) -> None:
        raw_assistant = "\n".join(
            ["background implementation detail " * 80]
            + [
                "Implemented generic agent assistant selection and pushed commit 123abcd to origin/main.",
                "Validation ran 9 tests passed.",
                "Next: continue profile retrieval budget tuning.",
            ]
        )
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "content": raw_assistant,
                }
            ]
        }

        messages = matrixark_agent_hook.hook_messages_from_payload(
            payload,
            event="response",
            text=raw_assistant,
        )
        metadata = matrixark_agent_hook.agent_memory_selection_metadata(
            payload,
            event="response",
            text=raw_assistant,
            messages=messages,
        )

        self.assertEqual(1, len(messages))
        self.assertEqual("assistant", messages[0]["role"])
        self.assertIn("Outcome: pushed commit 123abcd to origin/main", messages[0]["content"])
        self.assertIn("Validation: 9 tests passed", messages[0]["content"])
        self.assertIn("Next: continue profile retrieval budget tuning", messages[0]["content"])
        self.assertNotIn("background implementation detail", messages[0]["content"])
        # The assistant turn is both a decision outcome and a feature/profile fact
        # ("Next: continue profile retrieval budget tuning"), so it is budgeted under
        # both policies, and feature-focused assistant memory prefers the profile policy
        # as its primary selection (assistant profile facts are budgeted separately).
        self.assertEqual(
            {
                "selected_assistant_decision_outcome_only": 1,
                "selected_assistant_profile_fact": 1,
            },
            metadata["source_memory_selection_policy_counts"],
        )
        self.assertEqual(
            "selected_assistant_profile_fact",
            metadata["codex_memory_selection"]["policy"],
        )
        self.assertTrue(metadata["codex_memory_selection"]["selection_lossy"])
        self.assertFalse(metadata["codex_memory_selection"]["large_payload_verbatim_stored"])

    def test_generic_agent_hook_compacts_mixed_user_assistant_memory_policies(self) -> None:
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": "Please implement profile entities and keep cross-session retrieval bounded.",
                },
                {
                    "role": "assistant",
                    "content": (
                        "Done. Implemented profile entity promotion. "
                        "Validation ran 12 tests passed. "
                        + ("Verbose explanation " * 120)
                    ),
                },
            ]
        }

        messages = matrixark_agent_hook.hook_messages_from_payload(
            payload,
            event="response",
            text="",
        )
        metadata = matrixark_agent_hook.agent_memory_selection_metadata(
            payload,
            event="response",
            text="",
            messages=messages,
        )

        self.assertEqual(2, len(messages))
        self.assertIn("Please implement profile entities", messages[0]["content"])
        self.assertIn("Validation: 12 tests passed", messages[1]["content"])
        self.assertNotIn("Verbose explanation Verbose explanation", messages[1]["content"])
        self.assertEqual(
            {
                "selected_assistant_decision_outcome_only": 1,
                "selected_user_prompt": 1,
            },
            metadata["source_memory_selection_policy_counts"],
        )
        self.assertNotIn("codex_memory_selection", metadata)

    def test_generic_agent_user_prompt_preserves_profile_fact_policy(self) -> None:
        raw_prompt = "Always use the Ubuntu repo for TemporalStore work; never use Windows folders."
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": raw_prompt,
                }
            ]
        }

        messages = matrixark_agent_hook.hook_messages_from_payload(
            payload,
            event="UserPromptSubmit",
            text=raw_prompt,
        )
        metadata = matrixark_agent_hook.agent_memory_selection_metadata(
            payload,
            event="UserPromptSubmit",
            text=raw_prompt,
            messages=messages,
        )

        self.assertEqual(1, len(messages))
        self.assertEqual("user", messages[0]["role"])
        self.assertEqual(
            ["selected_user_profile_fact", "selected_user_prompt"],
            metadata["source_memory_selection_policies"],
        )
        self.assertEqual(
            {"selected_user_prompt": 1, "selected_user_profile_fact": 1},
            metadata["source_memory_selection_policy_counts"],
        )
        self.assertEqual(
            ["selected_user_prompt", "selected_user_profile_fact"],
            metadata["codex_memory_selection"]["policies"],
        )

    def test_generic_agent_messages_carry_per_message_memory_selection(self) -> None:
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": "Always use Ubuntu folders for TemporalStore and never use Windows repos.",
                },
                {
                    "role": "assistant",
                    "content": "Outcome: pushed commit 123abcd to origin/main.",
                },
            ]
        }

        messages = matrixark_agent_hook.hook_messages_from_payload(
            payload,
            event="response",
            text="",
        )

        user_counts = matrixark_mcp_local_adapter.memory_selection_policy_counts_for_message(messages[0])
        assistant_counts = matrixark_mcp_local_adapter.memory_selection_policy_counts_for_message(messages[1])
        self.assertEqual(
            {"selected_user_prompt": 1, "selected_user_profile_fact": 1},
            user_counts,
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 1},
            assistant_counts,
        )
        self.assertNotIn("selected_user_profile_fact", assistant_counts)

    def test_generic_agent_hook_normalizes_assistant_and_tool_role_aliases(self) -> None:
        payload = {
            "messages": [
                {
                    "role": "assistant_response",
                    "content": "Implemented profile memory extraction and pushed commit abc1234 to origin/main.",
                },
                {
                    "role": "tool_result",
                    "content": "verbose output\nExit code: 0\nRan 5 tests\nOK",
                },
            ]
        }

        messages = matrixark_agent_hook.hook_messages_from_payload(
            payload,
            event="response",
            text="",
        )
        metadata = matrixark_agent_hook.agent_memory_selection_metadata(
            payload,
            event="response",
            text="",
            messages=messages,
        )

        self.assertEqual(["assistant", "tool"], [message["role"] for message in messages])
        self.assertIn("Outcome: pushed commit abc1234 to origin/main", messages[0]["content"])
        self.assertIn("Exit code: 0", messages[1]["content"])
        self.assertNotIn("verbose output", messages[1]["content"])
        self.assertEqual(
            {
                "selected_assistant_decision_outcome_only": 1,
                "selected_tool_evidence_only": 1,
            },
            metadata["source_memory_selection_policy_counts"],
        )

    def test_generic_hook_threshold_extracts_user_and_assistant_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rust_root = Path(tmp_dir) / "rust-store"
            threshold_args = ["--session-commit-threshold", "2", "--idle-commit-timeout-ms", "0"]
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
                    "To https://github.com/matrixarkai/TemporalStore.git",
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
            # Event-level provenance (source_role="tool", hook_type="tool_result") is
            # captured and asserted at extraction time (auto_batch_extract above). The
            # retrieved record is the promoted tool_evidence *entity*, budgeted by
            # entity_type / memory_scope / session_continuity rather than by the raw
            # event's source_role/hook_type (which are event-level, not entity-level).
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
            planned = json.loads(matrixark_agent_config.named_agent_json(agent, ".", "tools/matrixark_mcp_rust_server.sh"))
            self.assertEqual(snippet["envelope"]["schema"], "matrixark_agent_envelope_v1")
            self.assertEqual("todo_planned", planned["hook_status"])
            self.assertNotIn("recommended_hook_command", planned)

    def test_agent_config_exposes_codex_and_claude_supported_hook_and_todo_agents(self) -> None:
        self.assertEqual(matrixark_agent_config.SUPPORTED_AGENT_CLIENTS, ["codex", "claude"])
        self.assertEqual(matrixark_agent_config.SUPPORTED_HOOK_CLIENTS, ["codex", "claude"])
        self.assertIn("claude", matrixark_agent_config.SUPPORTED_HOOK_CLIENTS)
        self.assertNotIn("claude", matrixark_agent_config.TODO_AGENT_CLIENTS)
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

    def test_hook_examples_emit_codex_and_claude_commands(self) -> None:
        examples = matrixark_agent_config.hook_examples_text(".")
        self.assertIn("--agent codex --event UserPromptSubmit", examples)
        self.assertIn("--agent claude --event UserPromptSubmit", examples)
        self.assertIn("TODO/planned", examples)
        self.assertNotIn("--agent openclaw --event UserPromptSubmit", examples)

    def test_installed_codex_wrappers_forward_actual_hook_event(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        for script_name in ["install_linux_temporalstore.sh", "install_macos_temporalstore.sh"]:
            with self.subTest(script_name=script_name):
                script = (repo / "tools" / script_name).read_text()
                self.assertIn('event="\\${MATRIXARK_AGENT_EVENT:-\\${CODEX_HOOK_EVENT:-UserPromptSubmit}}"', script)
                self.assertIn('if [[ "\\${1:-}" != "" && "\\${1:-}" != --* ]]; then', script)
                self.assertIn('event="\\$1"', script)
                self.assertIn("  shift", script)
                self.assertIn('--event "\\$event" --backend temporalstore-rust "\\$@"', script)
                self.assertNotIn("--event UserPromptSubmit --backend temporalstore-rust", script)

    def test_agent_hook_main_extracts_payload_text_with_event_context(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "tools" / "matrixark_agent_hook.py").read_text()
        self.assertIn("text = payload_text(payload, event=args.event) or args.query", source)

    def test_generic_agent_stop_payload_accepts_llm_response_alias(self) -> None:
        text = matrixark_agent_hook.payload_text(
            {"llm_response": "Done. Implemented profile extraction and pushed commit abc1234 to origin/main."},
            event="Stop",
        )
        messages = matrixark_agent_hook.hook_messages_from_payload(
            {},
            event="Stop",
            text=text,
        )

        self.assertEqual("assistant", messages[0]["role"])
        self.assertIn("Outcome: pushed commit abc1234 to origin/main", messages[0]["content"])


if __name__ == "__main__":
    unittest.main()
