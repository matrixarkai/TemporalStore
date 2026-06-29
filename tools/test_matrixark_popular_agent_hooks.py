#!/usr/bin/env python3
"""Regression tests for MatrixArk universal popular-agent hooks."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_server import MatrixArkLocalAdapter


class MatrixArkPopularAgentHooksTest(unittest.TestCase):
    def run_agent_hook(self, *, agent: str, event: str, payload: dict, event_log: Path, extra: list[str] | None = None) -> dict:
        repo = Path(__file__).resolve().parents[1]
        cmd = [
            sys.executable,
            str(repo / "tools" / "matrixark_agent_hook.py"),
            "--agent",
            agent,
            "--event",
            event,
            "--backend",
            "local",
            "--event-log",
            str(event_log),
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
        proc = subprocess.run(
            cmd,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            cwd=repo,
            timeout=30,
        )
        if proc.returncode != 0:
            raise AssertionError(f"agent hook failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return json.loads(proc.stdout)

    def test_claude_prompt_hook_ingests_retrieves_and_preserves_visible_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "claude-agent-hook.jsonl"
            result = self.run_agent_hook(
                agent="claude",
                event="UserPromptSubmit",
                event_log=event_log,
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
            self.assertTrue(result["retrieved"]["context_pack_id"])

            records = MatrixArkLocalAdapter(event_log).read_all()
            event = next(record for record in records if record.get("record_type") == "context_event")
            self.assertIn("Claude owns the GPU release checklist", event.get("text", ""))
            self.assertIn("scope_key", event)
            self.assertTrue(any(record.get("record_type") == "context_summary_dirty" for record in records))
            self.assertTrue(any(record.get("record_type") == "session_buffer_event" for record in records))

    def test_openclaw_resource_hook_imports_resource_with_agent_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            event_log = tmp / "openclaw-agent-hook.jsonl"
            resource = tmp / "openclaw_gpu_runbook.md"
            resource.write_text(
                "# OpenClaw GPU Runbook\n\nDecision: OpenClaw agents must attach finance approval before vendor selection.\n",
                encoding="utf-8",
            )
            result = self.run_agent_hook(
                agent="openclaw",
                event="ResourceAdded",
                event_log=event_log,
                payload={
                    "path": str(resource),
                    "resource_type": "md",
                    "thread_id": "openclaw-thread-7",
                    "message": "OpenClaw added a GPU runbook.",
                },
            )
            self.assertEqual("ok", result["status"])
            self.assertEqual("openclaw", result["agent"])
            self.assertEqual("openclaw:openclaw-thread-7", result["session_id"])
            self.assertEqual(str(resource), result["resource_uri"])
            self.assertEqual("md", result["resource_type"])
            self.assertTrue(result["ingested"])

            records = MatrixArkLocalAdapter(event_log).read_all()
            self.assertTrue(any(record.get("record_type") == "resource_manifest" for record in records))
            self.assertTrue(any(record.get("record_type") == "resource_chunk" for record in records))
            manifest = next(record for record in records if record.get("record_type") == "resource_manifest")
            self.assertEqual("md", manifest.get("resource_type"))
            self.assertIn("scope_key", manifest.get("access_scope", {}))

    def test_openclaw_stop_hook_commits_session_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            event_log = Path(tmp_dir) / "openclaw-stop-hook.jsonl"
            self.run_agent_hook(
                agent="openclaw",
                event="UserPromptSubmit",
                event_log=event_log,
                payload={"prompt": "OpenClaw should remember Alice approved the GPU request.", "thread_id": "openclaw-thread-commit"},
            )
            result = self.run_agent_hook(
                agent="openclaw",
                event="Stop",
                event_log=event_log,
                payload={"message": "OpenClaw task completed.", "thread_id": "openclaw-thread-commit"},
            )
            self.assertEqual("ok", result["status"])
            self.assertEqual("hook_boundary", result["committed"]["commit_reason"])
            records = MatrixArkLocalAdapter(event_log).read_all()
            self.assertTrue(any(record.get("record_type") == "context_batch_commit" for record in records))


if __name__ == "__main__":
    unittest.main()
