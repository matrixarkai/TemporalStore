#!/usr/bin/env python3
"""Regression tests for MatrixArk universal popular-agent hooks."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import matrixark_agent_config


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
            self.assertGreaterEqual(result["retrieved"]["selected_ref_count"], 1)

    def test_openclaw_resource_hook_is_configured_as_async_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            resource = tmp / "openclaw_gpu_runbook.md"
            resource.write_text(
                "# OpenClaw GPU Runbook\n\nDecision: OpenClaw agents must attach finance approval before vendor selection.\n",
                encoding="utf-8",
            )
            snippet = json.loads(matrixark_agent_config.openclaw_json(".", "tools/matrixark_mcp_rust_server.sh"))
            self.assertEqual("openclaw", snippet["agent"])
            self.assertIn("ResourceAdded", snippet["lifecycle_events"])
            self.assertEqual("temporalstore-rust", snippet["server"]["env"]["MATRIXARK_MCP_BACKEND"])
            self.assertEqual("true", snippet["server"]["env"]["MATRIXARK_RUST_PROXY_ASYNC_STORAGE"])

    def test_openclaw_stop_hook_commits_session_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rust_root = Path(tmp_dir) / "rust-store"
            self.run_agent_hook(
                agent="openclaw",
                event="UserPromptSubmit",
                rust_root=rust_root,
                payload={"prompt": "OpenClaw should remember Alice approved the GPU request.", "thread_id": "openclaw-thread-commit"},
            )
            result = self.run_agent_hook(
                agent="openclaw",
                event="Stop",
                rust_root=rust_root,
                payload={"message": "OpenClaw task completed.", "thread_id": "openclaw-thread-commit"},
            )
            self.assertEqual("ok", result["status"])
            self.assertEqual("hook_boundary", result["committed"]["commit_reason"])

    def test_agent_config_exposes_one_envelope_for_popular_agents(self) -> None:
        self.assertEqual(
            matrixark_agent_config.SUPPORTED_AGENT_CLIENTS,
            ["codex", "claude", "cursor", "openclaw", "opencode", "aider", "continue", "cline", "roo", "generic"],
        )
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
        self.assertEqual(envelope["required_fields_by_lifecycle"]["resource_added"], ["file_refs or resource_refs or raw_uri"])
        self.assertEqual(envelope["required_fields_by_lifecycle"]["skill_added"], ["file_refs or resource_refs or raw_uri"])
        self.assertTrue(envelope["file_ref_examples"])
        self.assertTrue(envelope["resource_ref_examples"])
        self.assertEqual(envelope["lifecycle_tools"]["before_llm"], "matrixark_retrieve")
        self.assertEqual(envelope["lifecycle_tools"]["after_answer"], "matrixark_ingest")
        self.assertEqual(envelope["lifecycle_tools"]["after_tool"], "matrixark_ingest")
        self.assertEqual(envelope["lifecycle_tools"]["session_boundary"], "matrixark_session_commit")
        self.assertEqual(envelope["lifecycle_actions"]["before_llm"], "retrieve")
        self.assertEqual(envelope["lifecycle_actions"]["after_answer"], "ingest_durable_outcome")
        self.assertEqual(envelope["lifecycle_actions"]["resource_added"], "import_resource_or_skill")
        self.assertEqual(envelope["lifecycle_actions"]["feedback"], "record_accepted_rejected_refs")
        self.assertEqual(envelope["lifecycle_actions"]["session_boundary"], "commit_batch_extract")
        self.assertIn("ContextEvent", envelope["agent_internal_model_hidden"])
        self.assertIn("ContextSummary", envelope["agent_internal_model_hidden"])
        self.assertIn("hidden prompt", envelope["do_not_send"])

        for agent in ("opencode", "aider", "continue", "cline", "roo"):
            snippet = json.loads(matrixark_agent_config.named_agent_json(agent, ".", "tools/matrixark_mcp_cpp_server.sh"))
            self.assertEqual(snippet["agent"], agent)
            self.assertEqual(snippet["envelope"]["schema"], "matrixark_agent_envelope_v1")
            self.assertIn("matrixark_retrieve", snippet["required_tools"])


if __name__ == "__main__":
    unittest.main()
