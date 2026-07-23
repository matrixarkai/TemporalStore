#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from argparse import Namespace
from pathlib import Path

import matrixark_codex_hook as hook
import matrixark_http
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
        self.assertEqual("019efad7-2f87-77e1-b082-c294fcb5e731", identity["thread_id"])
        self.assertEqual("019f-turn", identity["turn_id"])

    def test_user_prompt_payload_preserves_explicit_thread_identity(self) -> None:
        payload = decode_payload(
            json.dumps(
                {
                    "hook_event_name": "UserPromptSubmit",
                    "session_id": "019efad7-2f87-77e1-b082-c294fcb5e731",
                    "thread_id": "019f8d12-86c6-7100-9a44-7537cdd30aec",
                    "turn_id": "019f-turn",
                    "prompt": "delegated prompt",
                }
            ).encode("utf-8")
        )

        identity = extract_identity(payload)
        self.assertEqual("codex:019efad7-2f87-77e1-b082-c294fcb5e731", identity["session_id"])
        self.assertEqual("019f8d12-86c6-7100-9a44-7537cdd30aec", identity["thread_id"])

    def test_user_prompt_payload_unwraps_delegation_input(self) -> None:
        payload = decode_payload(
            json.dumps(
                {
                    "hook_event_name": "UserPromptSubmit",
                    "session_id": "019efad7-2f87-77e1-b082-c294fcb5e731",
                    "turn_id": "019f-turn",
                    "prompt": (
                        "<codex_delegation>\n"
                        "  <source_thread_id>019f8d12-86c6-7100-9a44-7537cdd30aec</source_thread_id>\n"
                        "  <input>clean delegated prompt</input>\n"
                        "</codex_delegation>"
                    ),
                }
            ).encode("utf-8")
        )

        self.assertEqual("clean delegated prompt", extract_prompt(payload, event="UserPromptSubmit"))

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

    def test_loose_stop_payload_extracts_latest_newline_separated_input_message(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019efad7-2f87-77e1-b082-c294fcb5e731,'
            'input-messages:[inference for llm\\n,'
            'vllm, sglang, others for comparison\\n,'
            'MatrixArk natural live acceptance after launcher bash fix]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual(
            "MatrixArk natural live acceptance after launcher bash fix",
            extract_prompt(payload, event="Stop"),
        )

    def test_loose_stop_payload_keeps_delegations_after_bracketed_tail(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019efad7-2f87-77e1-b082-c294fcb5e731,'
            'input-messages:[inference for llm\\n,vllm, sglang\\n,deep dive into vllm and sglang\\n,]\\n,'
            '<codex_delegation>\\n'
            ' <source_thread_id>019f8d12-86c6-7100-9a44-7537cdd30aec</source_thread_id>\\n'
            ' <input>MatrixArk trace live add-llm payload capture 1784782700: reply with one concise sentence about hook payloads.</input>\\n'
            '</codex_delegation>]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual(
            "MatrixArk trace live add-llm payload capture 1784782700: reply with one concise sentence about hook payloads.",
            extract_prompt(payload, event="Stop"),
        )

    def test_loose_stop_payload_prefers_plain_prompt_after_initial_delegation(self) -> None:
        raw = (
            '-- {"type:agent-turn-complete,thread-id:019f8d12-86c6-7100-9a44-7537cdd30aec,'
            'input-messages:[<codex_delegation>\\n'
            ' <source_thread_id>019ea4c9-a88c-71e1-baca-df7ff879e020</source_thread_id>\\n'
            ' <input>initial delegated task prompt</input>\\n'
            '</codex_delegation>\\n,'
            'REBASE ALL CHANGES\\n,'
            'COMMIT ALL CHANGES TO REMOTE MAIN\\n,'
            'test again in current session\\n,]}"'
        )
        payload = decode_payload(raw.encode("utf-8"))

        self.assertEqual("test again in current session", extract_prompt(payload, event="Stop"))

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

    def test_retention_keeps_acceptance_prompt_that_mentions_synthetic_rows(self) -> None:
        fields = hook.hook_retention_fields(
            text=(
                "Fix MatrixArk Codex realtime hook ingestion/query. "
                "Query top K with validation/probe/synthetic rows hidden by default."
            ),
            role="user",
            now_ms=123,
        )

        self.assertFalse(fields["synthetic"])
        self.assertEqual("normal", fields["retention_class"])

    def test_retention_marks_explicit_synthetic_probe_debug(self) -> None:
        fields = hook.hook_retention_fields(
            text="MatrixArk synthetic probe global cmd bash 1784781133",
            role="user",
            now_ms=123,
        )

        self.assertTrue(fields["synthetic"])
        self.assertEqual("debug", fields["retention_class"])

    def test_query_filter_keeps_natural_prompt_with_stale_synthetic_flag(self) -> None:
        text = (
            "Fix MatrixArk Codex realtime hook ingestion/query. "
            "Query top K with validation/probe/synthetic rows hidden by default."
        )

        self.assertTrue(
            matrixark_http._hook_is_real_user(
                {"synthetic": True, "record_type": "agent_message"},
                "user",
                text,
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": True, "record_type": "agent_message"},
                "user",
                "MatrixArk synthetic probe global cmd bash 1784781133",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "manual validation loose payload parser row 1784778866123",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "You are a helpful assistant. You will be presented with a user prompt, and your job is to provide a short title for a task.",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "context_event"},
                "user",
                "user: You are a helpful assistant. You will be presented with a user prompt, and your job is to provide a short title for a task.",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "MatrixArk cmd wrapper direct smoke 1784771237422",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "matrixark plain string prompt hook proof 1784770203",
                real_user_only=True,
                include_synthetic=False,
            )
        )

    def test_hook_dedupe_prefers_raw_over_context_event_user_prefix(self) -> None:
        rows = matrixark_http._hook_dedupe_rows(
            [
                {
                    "backend": "c++",
                    "session_id": "codex:session",
                    "text": "user: same prompt",
                    "timestamp_ms": 100,
                    "sequence": 1,
                    "projection": "records",
                },
                {
                    "backend": "c++",
                    "session_id": "codex:session",
                    "text": "same prompt",
                    "timestamp_ms": 100,
                    "sequence": 2,
                    "projection": "raw_ingestion",
                },
            ]
        )

        self.assertEqual(1, len(rows))
        self.assertEqual("raw_ingestion", rows[0]["projection"])

    def test_query_effective_synthetic_status_uses_text_classifier(self) -> None:
        self.assertTrue(matrixark_http._hook_text_is_synthetic("matrixark plain string prompt hook proof 1784770203"))
        self.assertFalse(
            matrixark_http._hook_text_is_synthetic(
                "Fix MatrixArk query with validation/probe/synthetic rows hidden by default"
            )
        )

if __name__ == "__main__":
    unittest.main()
