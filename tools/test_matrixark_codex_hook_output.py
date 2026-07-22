#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from argparse import Namespace
from pathlib import Path

import matrixark_codex_hook as hook


class MatrixArkCodexHookOutputTest(unittest.TestCase):
    def test_hook_trace_records_output_summary(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.records = []

            def append(self, record):
                self.records.append(record)

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="UserPromptSubmit",
            backend="temporalstore-rust",
            storage_prefix="matrixark:codex-hook:rust-live-v2",
            session_id="codex-session-1",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="deeproute",
            team="codex",
            project="temporalstore",
        )
        trace = hook.begin_hook_trace(
            args=args,
            payload={"prompt": "trace me", "session_id": "codex-session-1"},
            text="trace me",
            session_id_source="payload_field",
        )
        server = Server()
        hook.append_hook_trace(
            server,
            trace,
            output={
                "hookSpecificOutput": {"additionalContext": "remote memory"},
                "retrieve": {"context_pack_id": "pack-1", "selected_ref_count": 2},
                "ingest": {"status": "accepted"},
                "session_commit": {"status": "deferred"},
            },
        )

        self.assertEqual(1, len(server.adapter.records))
        record = server.adapter.records[0]
        self.assertEqual("codex_hook_trace", record["record_type"])
        self.assertEqual("temporalstore-rust", record["backend"])
        self.assertEqual("UserPromptSubmit", record["event"])
        self.assertEqual("payload_field", record["session_id_source"])
        self.assertEqual(["prompt", "session_id"], record["payload_keys"])
        self.assertEqual("ok", record["status"])
        self.assertEqual("pack-1", record["output_summary"]["context_pack_id"])
        self.assertTrue(record["output_summary"]["strict_additional_context_emitted"])

    def test_user_prompt_emit_codex_additional_context_from_selected_refs(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="payload_field",
            agent_context={"local_context": [{"ref": "src/main.cc", "text": "local code"}], "workspace_root": "/repo"},
            ingest={"status": "accepted", "event_id_hash": 123},
            retrieve={
                "context_pack_id": "pack-1",
                "used_context_tokens": 42,
                "selected_refs": [
                    {
                        "ref_type": "context_event",
                        "citation": "session:abc#turn=2",
                        "score": 0.91,
                        "token_estimate": 12,
                        "summary_text": "Alice asked Codex to keep TemporalStore production readiness context.",
                    }
                ],
            },
            query="TemporalStore hook status",
        )

        self.assertEqual("ok", output["status"])
        self.assertTrue(output["retrieve"]["additional_context_emitted"])
        self.assertEqual(1, output["retrieve"]["selected_ref_count"])
        hook_output = output["hookSpecificOutput"]
        self.assertEqual("UserPromptSubmit", hook_output["hookEventName"])
        additional = hook_output["additionalContext"]
        self.assertIn("MatrixArk/TemporalStore retrieved context for Codex", additional)
        self.assertIn("Merge this remote memory with the visible local Codex context", additional)
        self.assertIn("session:abc#turn=2", additional)
        self.assertIn("Alice asked Codex", additional)
        self.assertIn("local_context_refs_seen=1", additional)

    def test_grouped_refs_count_and_format_as_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-grouped",
                "tokens": {"remote": 9},
                "selected_ref_groups": [
                    {
                        "count": 2,
                        "refs": [
                            {"ref_type": "summary", "source_locator": "node/a", "text": "Node A summary."},
                            {"ref_type": "event", "source_locator": "node/b", "body": "Node B event."},
                        ],
                    }
                ],
            },
            query="grouped refs",
        )
        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(2, output["retrieve"]["selected_ref_count"])
        self.assertEqual(9, output["retrieve"]["used_context_tokens"])
        self.assertIn("node/a", additional)
        self.assertIn("Node B event", additional)

    def test_codex_hook_heartbeat_is_filtered_from_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: C++ TemporalStore is live and accepting MatrixArk hook writes."
        self.assertTrue(hook.is_codex_hook_heartbeat_text(heartbeat))
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-heartbeat",
                "selected_refs": [
                    {"ref_type": "event", "text": heartbeat},
                    {"ref_type": "event", "text": "user: real TemporalStore question"},
                ],
            },
            query="real question",
        )
        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertNotIn("Codex hook heartbeat", additional)
        self.assertIn("real TemporalStore question", additional)

    def test_non_prompt_event_keeps_audit_json_without_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="PostToolUse",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            ingest={"status": "accepted"},
        )
        self.assertNotIn("hookSpecificOutput", output)
        self.assertFalse(output["retrieve"]["additional_context_emitted"])
        self.assertTrue(output["lifecycle_stage"]["after_llm_ingest_only"])

    def test_strict_codex_stdout_removes_rich_audit_fields(self) -> None:
        prompt_output = {
            "status": "ok",
            "event": "UserPromptSubmit",
            "ingest": {"status": "accepted"},
            "retrieve": {"additional_context_emitted": True},
            "hookSpecificOutput": {
                "hookEventName": "UserPromptSubmit",
                "additionalContext": "remote memory",
            },
        }
        self.assertEqual(
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": "remote memory",
                }
            },
            hook.strict_codex_stdout(prompt_output),
        )
        self.assertEqual({}, hook.strict_codex_stdout({"status": "ok", "event": "Stop"}))

    def test_fail_open_backend_error_is_visible_to_codex_user_prompt(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cmd = [
            sys.executable,
            str(repo / "tools" / "matrixark_codex_hook.py"),
            "--event",
            "UserPromptSubmit",
            "--backend",
            "temporalstore-direct",
            "--metaserver",
            "127.0.0.1:1",
            "--namespace",
            "missing_ns",
            "--table",
            "missing_table",
            "--temporalstore-lib",
            "/tmp/definitely-missing-libbcache2.so",
            "--request-timeout-ms",
            "1",
            "--io-timeout-ms",
            "1",
        ]
        proc = subprocess.run(
            cmd,
            input=json.dumps({"prompt": "Will this hook fail open?", "thread_id": "codex-fail-open"}),
            cwd=repo,
            text=True,
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(0, proc.returncode, proc.stderr)
        output = json.loads(proc.stdout)
        self.assertEqual("warning", output["status"])
        self.assertEqual("hook_failed_fail_open", output["reason"])
        self.assertIn("hookSpecificOutput", output)
        self.assertIn("retrieval was attempted", output["hookSpecificOutput"]["additionalContext"])


if __name__ == "__main__":
    unittest.main()
