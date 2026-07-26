#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
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

    def test_payload_text_flattens_structured_assistant_content_parts(self) -> None:
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "output_text", "text": "Decision: ingest assistant responses."},
                        {"type": "text", "text": "Next: extract profile entities on idle timeout."},
                    ],
                }
            ]
        }

        self.assertEqual(
            "Decision: ingest assistant responses.\nNext: extract profile entities on idle timeout.",
            hook.payload_text(payload),
        )

    def test_rollout_assistant_extraction_flattens_structured_content_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rollout = Path(tmp_dir) / "rollout-test.jsonl"
            rollout.write_text(
                json.dumps(
                    {
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [
                                {"type": "output_text", "text": "Committed hook batching fix."},
                                {"type": "text", "text": "Tests passed and origin/main was pushed."},
                            ],
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            self.assertEqual(
                "Committed hook batching fix.\nTests passed and origin/main was pushed.",
                hook._extract_assistant_text_from_rollout(rollout),
            )

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
                "retrieve": {
                    "context_pack_id": "pack-1",
                    "selected_ref_count": 2,
                    "budget": {"remote_context_budget_tokens": 100, "used_remote_context_tokens": 12},
                    "layers": {
                        "selected_ref_counts": {"event": 1, "entity": 1},
                        "same_session_refs": 1,
                        "cross_session_refs": 1,
                        "entity_bridge_refs": 1,
                        "profile_memory_refs": 1,
                    },
                    "rendered_context_chars": 37,
                },
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
        self.assertEqual(100, record["output_summary"]["retrieval_budget"]["remote_context_budget_tokens"])
        self.assertEqual({"event": 1, "entity": 1}, record["output_summary"]["retrieval_layers"]["selected_ref_counts"])
        self.assertEqual(1, record["output_summary"]["retrieval_layers"]["cross_session_refs"])
        self.assertEqual(1, record["output_summary"]["retrieval_layers"]["profile_memory_refs"])
        self.assertEqual(37, record["output_summary"]["rendered_context_chars"])
        self.assertTrue(record["output_summary"]["strict_additional_context_emitted"])

    def test_retrieve_tool_call_trace_records_budget_and_layers(self) -> None:
        class Server:
            def handle(self, request):
                self.request = request
                result = {
                    "context_pack_id": "pack-tool",
                    "used_context_tokens": 33,
                    "remote_context_budget_tokens": 90,
                    "context": "user: rendered profile decision",
                    "selected_ref_counts": {"event": 2, "entity": 1},
                    "selected_refs": [
                        {
                            "ref_type": "event",
                            "memory_scope": "session",
                            "session_continuity": "same_session",
                            "text": (
                                "user: Codex hook heartbeat 2026-07-15T13:32:00Z: "
                                "C++ TemporalStore is live and accepting MatrixArk hook writes."
                            ),
                        },
                        {
                            "ref_type": "event",
                            "memory_scope": "session",
                            "session_continuity": "same_session",
                            "text": "current turn evidence",
                        },
                        {
                            "ref_type": "entity",
                            "memory_scope": "user_profile",
                            "session_continuity": "cross_session",
                            "text": "profile decision",
                        },
                    ],
                }
                return {"result": {"content": [{"text": json.dumps(result)}]}}

        trace = {"tool_calls": []}
        result = hook.trace_tool_call(Server(), "matrixark_retrieve", {"query": "profile decision"}, trace)

        self.assertEqual("pack-tool", result["context_pack_id"])
        self.assertEqual(1, len(trace["tool_calls"]))
        item = trace["tool_calls"][0]
        self.assertEqual("ok", item["status"])
        self.assertEqual(2, item["result"]["selected_ref_count"])
        self.assertEqual(90, item["result"]["retrieval_budget"]["remote_context_budget_tokens"])
        self.assertEqual(57, item["result"]["retrieval_budget"]["remote_budget_remaining_tokens"])
        self.assertEqual({"event": 1, "entity": 1}, item["result"]["retrieval_layers"]["selected_ref_counts"])
        self.assertEqual(1, item["result"]["retrieval_layers"]["same_session_refs"])
        self.assertEqual(1, item["result"]["retrieval_layers"]["cross_session_refs"])
        self.assertEqual(1, item["result"]["retrieval_layers"]["profile_memory_refs"])
        self.assertEqual(len("user: rendered profile decision"), item["result"]["rendered_context_chars"])

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
                "used_remote_context_tokens": 42,
                "used_local_context_tokens": 8,
                "total_prompt_context_tokens": 50,
                "remote_context_budget_tokens": 100,
                "requested_max_context_tokens": 160,
                "local_context_safety_margin_tokens": 4,
                "budget_source": "agent_provided_max_context_tokens",
                "selected_ref_counts": {"event": 1, "entity": 1, "summary": 1},
                "recall_policy": {
                    "session_continuity": {
                        "same_session_selected_ref_count": 1,
                        "cross_session_selected_ref_count": 2,
                        "entity_bridge_selected_ref_count": 1,
                    }
                },
                "local_context_policy": {"local_context_count": 1},
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
        self.assertEqual(100, output["retrieve"]["budget"]["remote_context_budget_tokens"])
        self.assertEqual(58, output["retrieve"]["budget"]["remote_budget_remaining_tokens"])
        self.assertFalse(output["retrieve"]["budget"]["remote_budget_overrun"])
        hook_output = output["hookSpecificOutput"]
        self.assertEqual("UserPromptSubmit", hook_output["hookEventName"])
        additional = hook_output["additionalContext"]
        self.assertIn("MatrixArk/TemporalStore retrieved context for Codex", additional)
        self.assertIn("Merge this remote memory with the visible local Codex context", additional)
        self.assertIn("session:abc#turn=2", additional)
        self.assertIn("Alice asked Codex", additional)
        self.assertIn("local_context_refs_seen=1", additional)
        self.assertIn("Budget summary:", additional)
        self.assertIn("remote_budget=100", additional)
        self.assertIn("remote_remaining=58", additional)
        self.assertIn("budget_source=agent_provided_max_context_tokens", additional)
        self.assertIn("Layer summary:", additional)
        self.assertIn("event=1", additional)
        self.assertIn("entity=1", additional)
        self.assertIn("summary=1", additional)
        self.assertIn("same_session_refs=1", additional)
        self.assertIn("cross_session_refs=2", additional)
        self.assertIn("entity_bridge_refs=1", additional)
        self.assertIn("local_context_refs=1", additional)

    def test_additional_context_layer_summary_falls_back_to_serving_refs(self) -> None:
        args = Namespace(session_id="codex-session-1")
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-layer-fallback",
                "selected_refs": [
                    {
                        "ref_type": "event",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "text": "current session prompt",
                    },
                    {
                        "ref_type": "entity",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "text": "profile decision",
                    },
                    {"context_class": "summary", "text": "compressed profile summary"},
                ],
            },
            query="memory layers",
        )

        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertIn("Layer summary:", additional)
        self.assertIn("event=1", additional)
        self.assertIn("entity=1", additional)
        self.assertIn("summary=1", additional)
        self.assertIn("same_session_refs=1", additional)
        self.assertIn("cross_session_refs=1", additional)
        self.assertIn("entity_bridge_refs=1", additional)
        self.assertIn("session_memory_refs=1", additional)
        self.assertIn("profile_memory_refs=1", additional)

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
        self.assertEqual(1, output["retrieve"]["selected_ref_count"])
        self.assertEqual({"event": 1}, output["retrieve"]["layers"]["selected_ref_counts"])
        self.assertNotIn("Codex hook heartbeat", additional)
        self.assertIn("real TemporalStore question", additional)

    def test_codex_hook_heartbeat_line_is_filtered_from_rendered_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: C++ TemporalStore is live and accepting MatrixArk hook writes."
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={
                "pack_id": "pack-rendered-heartbeat",
                "context": heartbeat + "\nuser: real rendered TemporalStore memory",
            },
            query="real rendered memory",
        )

        additional = output["hookSpecificOutput"]["additionalContext"]
        self.assertNotIn("Codex hook heartbeat", additional)
        self.assertIn("real rendered TemporalStore memory", additional)
        self.assertEqual(
            len("user: real rendered TemporalStore memory"),
            output["retrieve"]["rendered_context_chars"],
        )

    def test_heartbeat_only_rendered_context_does_not_emit_additional_context(self) -> None:
        args = Namespace(session_id="codex-session-1")
        heartbeat = "user: Codex hook heartbeat 2026-07-15T13:32:00Z: C++ TemporalStore is live and accepting MatrixArk hook writes."
        output = hook.codex_hook_output(
            args=args,
            status="ok",
            event="UserPromptSubmit",
            session_id_source="explicit",
            agent_context={"local_context": [], "workspace_root": "/repo"},
            retrieve={"pack_id": "pack-heartbeat-only", "context": heartbeat},
            query="real memory",
        )

        self.assertNotIn("hookSpecificOutput", output)
        self.assertFalse(output["retrieve"]["additional_context_emitted"])
        self.assertEqual(0, output["retrieve"]["selected_ref_count"])
        self.assertEqual(0, output["retrieve"]["rendered_context_chars"])

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

    def test_selected_tool_evidence_filters_large_stdout(self) -> None:
        raw = "\n".join(
            [f"noise line {index}" for index in range(40)]
            + [
                "Exit code: 0",
                "Ran 32 tests in 0.508s",
                "OK",
                "To https://github.com/bjmeetsfo/TemporalStore.git",
                "002fbd45034c69ce4487a64ab40d90135a55ae1a refs/heads/main",
                "another verbose blob " * 200,
            ]
        )
        evidence = hook.selected_tool_evidence_text(raw, max_chars=1000)
        self.assertIn("Exit code: 0", evidence)
        self.assertIn("Ran 32 tests", evidence)
        self.assertIn("refs/heads/main", evidence)
        self.assertNotIn("noise line 0", evidence)
        self.assertLess(len(evidence), 1000)

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
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return []

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
        self.assertEqual(2, len(server.adapter.serving_records))
        raw = server.adapter.raw_records[0]
        serving = server.adapter.serving_records[0]
        task = server.adapter.serving_records[1]
        self.assertEqual("agent_message", raw["record_type"])
        self.assertEqual("real hooked Codex message", raw["messages"][0]["content"])
        self.assertEqual("codex-cpp-session-1", raw["scope"]["session_id"])
        self.assertEqual("context_event", serving["record_type"])
        self.assertEqual("codex-cpp-session-1", serving["session_id"])
        self.assertEqual("codex-cpp-session-1", serving["scope"]["session_id"])
        self.assertEqual("UserPromptSubmit", serving["metadata"]["codex_event"])
        self.assertEqual("PENDING_ASYNC_EXTRACTION", serving["classification"])
        self.assertEqual("pending_async", serving["event_type"])
        self.assertEqual("pending", serving["status"])
        self.assertEqual("async_pending", serving["internal_extraction"]["mode"])
        self.assertIn("real hooked Codex message", serving["summary_text"])
        self.assertIn("real hooked Codex message", serving["text"])
        self.assertEqual("matrixark_async_pipeline_task", task["record_type"])
        self.assertEqual(serving["event_id_hash"], task["event_id_hash"])
        self.assertEqual("pending", task["status"])
        self.assertEqual(["extraction", "summary", "compression", "embedding"], task["stages"])
        self.assertEqual(task["task_hash"], result["async_pipeline_task_hash"])
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertTrue(result["session_buffer"]["registered"])

    def test_fast_async_hook_ingest_runs_threshold_batch_commit(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        hook.HOOK_AUTO_BATCH_EXTRACT = True

        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}, {"event_id_hash": 2}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {"status": "accepted", "entities_written": 2, "indexes_written": 3}

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="deeproute",
                session_id="codex-session-threshold",
                team="codex",
                project="temporalstore",
                session_commit_threshold=2,
                idle_commit_timeout_ms=300000,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="batch me into entities",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("accepted", result["auto_batch_extract_result"]["status"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("threshold", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(2, commit_args["threshold_messages"])
        self.assertEqual(2, commit_args["max_messages"])
        self.assertEqual("session_commit", commit_hook["hook_type"])

    def test_fast_async_hook_ingest_commits_assistant_stop_boundary(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {"status": "accepted", "entities_written": 1}

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="Stop",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="deeproute",
            session_id="codex-session-stop",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=300000,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Done. Commit d0152479 pushed and tests passed.",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        self.assertEqual("accepted", result["session_commit"]["status"])
        self.assertEqual(1, len(server.adapter.raw_records))
        self.assertEqual("assistant", server.adapter.raw_records[0]["messages"][0]["role"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, _commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("hook_boundary", commit_args["commit_reason"])
        self.assertTrue(commit_args["force"])

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

    def test_dual_hook_has_no_persistent_hook_logs(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('CPP_HOOK_STDOUT="/dev/null"', script)
        self.assertIn('RUST_PUBLISH_STDERR="/dev/null"', script)
        self.assertIn('CPP_PUBLISH_STDERR="/dev/null"', script)
        self.assertIn('export MATRIXARK_CODEX_HOOK_DIAG_LOG=""', script)
        self.assertNotIn('MATRIXARK_CODEX_HOOK_LOG_DIR', script)
        self.assertNotIn('dispatch-diagnostics.jsonl', script)
        self.assertNotIn('with open(os.environ.get("MATRIXARK_CODEX_HOOK_DIAG_LOG"', script)
        self.assertNotIn('cpp-$EVENT.out', script)
        self.assertNotIn('rust-service-publish.err', script)
        self.assertNotIn('cpp-direct-publish.err', script)

    def test_cpp_and_rust_hooks_do_not_persist_external_logs(self) -> None:
        tools_dir = Path(__file__).resolve().parents[1] / "tools"
        cpp_script = (tools_dir / "matrixark_codex_cpp_hook.sh").read_text()
        rust_script = (tools_dir / "matrixark_codex_rust_hook.sh").read_text()
        dual_script = (tools_dir / "matrixark_codex_dual_hook.sh").read_text()
        combined = "\n".join([cpp_script, rust_script, dual_script])

        self.assertIn('export MATRIXARK_RUST_PROXY_DAEMON_LOG="/dev/null"', rust_script)
        self.assertNotIn("daemon.log", rust_script)
        self.assertNotIn('mkdir -p "$(dirname "$MATRIXARK_RUST_PROXY_DAEMON_LOG")"', rust_script)
        self.assertNotIn("MATRIXARK_CODEX_HOOK_LOG_DIR", combined)
        self.assertNotIn("dispatch-diagnostics.jsonl", combined)
        self.assertNotIn("cpp-$EVENT.out", combined)
        self.assertNotIn("rust-service-publish.err", combined)
        self.assertNotIn("cpp-direct-publish.err", combined)

    def test_live_codex_hooks_enable_assistant_capture_and_auto_batch_extraction(self) -> None:
        tools_dir = Path(__file__).resolve().parents[1] / "tools"
        cpp_script = (tools_dir / "matrixark_codex_cpp_hook.sh").read_text()
        rust_script = (tools_dir / "matrixark_codex_rust_hook.sh").read_text()
        dual_script = (tools_dir / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('MATRIXARK_CODEX_CPP_USER_PROMPTS_ONLY="${MATRIXARK_CODEX_CPP_USER_PROMPTS_ONLY:-0}"', cpp_script)
        self.assertIn('MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"', cpp_script)
        self.assertIn('MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"', rust_script)
        self.assertIn('MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"', dual_script)

    def test_dual_hook_keeps_derived_context_out_of_raw_ingestion(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('f"{prefix}:raw_ingestion:records", raw_record', script)
        self.assertNotIn('for extracted_record in rust_live_extraction_records()', script)
        self.assertNotIn('for extracted_record in cpp_live_extraction_records()', script)


    def test_codex_hook_messages_both_skips_local_proxy_debug_reader(self) -> None:
        original_cpp = matrixark_http._CppHookStoreReader
        original_service = matrixark_http._RustServiceHookStoreReader
        original_local = matrixark_http._RustLocalHookStoreReader
        calls = []

        class EmptyReader:
            name = "empty"

            def get_string(self, key):
                return "0"

            def hget(self, key, field):
                return None

        try:
            matrixark_http._CppHookStoreReader = lambda args: calls.append("c++") or EmptyReader()
            matrixark_http._RustServiceHookStoreReader = lambda args: calls.append("rust-service") or EmptyReader()

            def fail_local(args):
                raise AssertionError("rust-local proxy should be explicit-only")

            matrixark_http._RustLocalHookStoreReader = fail_local
            matrixark_http.query_codex_hook_messages({"backend": "both", "top_k": 1})
        finally:
            matrixark_http._CppHookStoreReader = original_cpp
            matrixark_http._RustServiceHookStoreReader = original_service
            matrixark_http._RustLocalHookStoreReader = original_local

        self.assertEqual(["c++", "rust-service"], calls)

    def test_query_effective_synthetic_status_uses_text_classifier(self) -> None:
        self.assertTrue(matrixark_http._hook_text_is_synthetic("matrixark plain string prompt hook proof 1784770203"))
        self.assertFalse(
            matrixark_http._hook_text_is_synthetic(
                "Fix MatrixArk query with validation/probe/synthetic rows hidden by default"
            )
        )

if __name__ == "__main__":
    unittest.main()
