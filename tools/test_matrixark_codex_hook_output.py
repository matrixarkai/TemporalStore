#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from argparse import Namespace
from pathlib import Path

import matrixark_codex_hook as hook
from matrixark_codex_hook_payload import decode_payload, extract_identity, extract_prompt


class MatrixArkCodexHookOutputTest(unittest.TestCase):
    def test_loose_stop_payload_extracts_current_input_message_and_thread_identity(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019f8cb5-b4d5-77f2-8c82-0499440da36f,'
            'turn-id:019f8cd9-7669-7891-ab92-7353efe82e4d,'
            'cwd:C:\\Users\\Deeproute\\Documents\\Codex,client:Codex Desktop,'
            'input-messages:[older prompt,'
            '<codex_delegation>\\n <input>fresh old task prompt should win</input>\\n</codex_delegation>]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual("fresh old task prompt should win", extract_prompt(payload, event="Stop"))
        identity = extract_identity(payload)
        self.assertEqual("codex:019f8cb5-b4d5-77f2-8c82-0499440da36f", identity["session_id"])
        self.assertEqual("019f8cb5-b4d5-77f2-8c82-0499440da36f", identity["thread_id"])
        self.assertEqual("019f8cd9-7669-7891-ab92-7353efe82e4d", identity["turn_id"])

    def test_json_user_prompt_payload_extracts_prompt_and_session_identity(self) -> None:
        payload = decode_payload(
            json.dumps(
                {
                    "hook_event_name": "UserPromptSubmit",
                    "session_id": "019efad7-2f87-77e1-b082-c294fcb5e731",
                    "turn_id": "019f-turn",
                    "prompt": "natural current prompt",
                }
            ).encode("utf-8")
        )

        self.assertEqual("natural current prompt", extract_prompt(payload, event="UserPromptSubmit"))
        identity = extract_identity(payload)
        self.assertEqual("codex:019efad7-2f87-77e1-b082-c294fcb5e731", identity["session_id"])
        self.assertEqual("019f-turn", identity["turn_id"])

    def test_loose_stop_payload_preserves_single_prompt_with_commas(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019f8d12-86c6-7100-9a44-7537cdd30aec,'
            'input-messages:[QUERY TOP 10 MESSAGES FROM C++ AND RUST TEMPORALSTORE WITH DETAILS TO COMPARE, '
            'MAKE SURE CURRENT THREAD IS FETCHED]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertIn(
            "QUERY TOP 10 MESSAGES FROM C++ AND RUST TEMPORALSTORE",
            extract_prompt(payload, event="Stop"),
        )
        identity = extract_identity(payload)
        self.assertEqual("codex:019f8d12-86c6-7100-9a44-7537cdd30aec", identity["session_id"])

    def test_env_thread_identity_fallback_when_payload_has_no_session(self) -> None:
        identity = extract_identity(
            {"hook_event_name": "Stop", "transcript_path": "ignored.jsonl"},
            env={"CODEX_THREAD_ID": "019f8d12-86c6-7100-9a44-7537cdd30aec"},
        )

        self.assertEqual("codex:019f8d12-86c6-7100-9a44-7537cdd30aec", identity["session_id"])
        self.assertEqual("019f8d12-86c6-7100-9a44-7537cdd30aec", identity["thread_id"])
        self.assertEqual("env", identity["session_id_source"])

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


    def test_fast_async_hook_ingest_writes_raw_and_serving_scope(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="UserPromptSubmit",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="deeproute",
            session_id="codex-cpp-session-1",
            team="codex",
            project="temporalstore",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="real hooked Codex message",
            role="user",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        self.assertEqual("accepted", result["status"])
        self.assertEqual("accepted", result["raw_ingestion_status"])
        self.assertEqual(1, len(server.adapter.raw_records))
        self.assertEqual(1, len(server.adapter.serving_records))
        raw = server.adapter.raw_records[0]
        serving = server.adapter.serving_records[0]
        self.assertEqual("agent_message", raw["record_type"])
        self.assertEqual("real hooked Codex message", raw["messages"][0]["content"])
        self.assertEqual("codex-cpp-session-1", raw["scope"]["session_id"])
        self.assertEqual("context_event", serving["record_type"])
        self.assertEqual("codex-cpp-session-1", serving["session_id"])
        self.assertEqual("codex-cpp-session-1", serving["scope"]["session_id"])
        self.assertEqual("UserPromptSubmit", serving["metadata"]["codex_event"])
        self.assertIn("real hooked Codex message", serving["text"])

if __name__ == "__main__":
    unittest.main()
