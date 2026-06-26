#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer


class MatrixArkCodexHookPipelineTest(unittest.TestCase):
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
        )
        if proc.returncode != 0:
            raise AssertionError(f"hook failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return json.loads(proc.stdout)

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
                payload={"raw_uri": str(resource), "resource_type": "md", "thread_id": "codex-thread-1"},
            )
            self.assertEqual("ok", resource_result["status"])
            self.assertEqual(str(resource), resource_result["resource_uri"])
            self.assertTrue(resource_result["ingest"].get("resource_chunks"))
            self.assertGreaterEqual(resource_result["ingest"].get("resource_fact_event_count", 0), 1)

            skill_result = self.run_hook(
                repo,
                event_log,
                event="ResourceAdded",
                payload={"raw_uri": str(skill), "thread_id": "codex-thread-1"},
            )
            self.assertEqual("ok", skill_result["status"])
            self.assertEqual("skill", skill_result["resource_type"])
            self.assertIsInstance(skill_result["ingest"].get("skill_hash"), int)

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
                },
            )
            ref_types = {str(ref.get("ref_type")) for ref in pack["selected_refs"]}
            self.assertIn("event", ref_types)
            self.assertIn("resource_chunk", ref_types)
            self.assertIn("skill_section", ref_types)
            self.assertGreaterEqual(pack["selected_ref_counts"].get("resource_chunk", 0), 1)
            self.assertGreaterEqual(pack["selected_ref_counts"].get("skill_section", 0), 1)
            self.assertTrue(pack["context_assembly_policy"]["skill_selection"], "skill_section_only")
            replay = server.call_tool("matrixark_replay", {"scope": scope, "context_pack_id": pack["context_pack_id"]})
            audits = [row for row in replay["events"] if row.get("record_type") == "context_pack_audit"]
            self.assertTrue(any(row.get("context_pack_id") == pack["context_pack_id"] for row in audits))


if __name__ == "__main__":
    unittest.main()
