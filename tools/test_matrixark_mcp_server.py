import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tools.matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer


class MatrixArkMcpServerTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.event_log = Path(self.tmpdir.name) / "events.jsonl"
        self.server = MatrixArkMcpServer(MatrixArkLocalAdapter(self.event_log))

    def tearDown(self):
        self.tmpdir.cleanup()

    def call_tool(self, name, arguments):
        response = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        self.assertNotIn("error", response)
        text = response["result"]["content"][0]["text"]
        return json.loads(text)

    def test_lists_matrixark_tools(self):
        response = self.server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        names = {tool["name"] for tool in response["result"]["tools"]}
        self.assertEqual(
            names,
            {
                "matrixark_ingest",
                "matrixark_retrieve",
                "matrixark_feedback",
                "matrixark_replay",
            },
        )
        ingest = next(tool for tool in response["result"]["tools"] if tool["name"] == "matrixark_ingest")
        message_schema = ingest["inputSchema"]["properties"]["messages"]["items"]
        self.assertEqual(message_schema["required"], ["role", "content"])
        self.assertEqual(
            message_schema["properties"]["role"]["enum"],
            ["user", "assistant", "tool", "system"],
        )
        hook_schema = ingest["inputSchema"]["properties"]["agent_hook"]
        self.assertIn("hook_type", hook_schema["required"])
        scope_schema = ingest["inputSchema"]["properties"]["scope"]
        self.assertIn("session_id", scope_schema["properties"])
        self.assertIn("user_id or session_id", scope_schema["description"])
        self.assertIn("Useful alone", scope_schema["properties"]["user_id"]["description"])
        self.assertIn("Useful alone", scope_schema["properties"]["session_id"]["description"])
        self.assertIn("The only required top-level field", ingest["inputSchema"]["properties"]["messages"]["description"])
        self.assertNotIn("infer", ingest["inputSchema"]["properties"])
        feedback = next(tool for tool in response["result"]["tools"] if tool["name"] == "matrixark_feedback")
        self.assertEqual(feedback["inputSchema"]["required"], ["messages"])
        self.assertIn("strongly recommended", feedback["inputSchema"]["properties"]["context_pack_id"]["description"])
        self.assertNotIn("infer", feedback["inputSchema"]["properties"])

    def test_hook_captured_ingest_then_retrieve(self):
        ingest = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "user", "content": "Alice approved the GPU request."}
                ],
                "scope": {"team": "infra_team", "project": "project_1"},
                "metadata": {"node_path": ["company_a", "infra_team", "project_1", "approvals"]},
                "agent_hook": {
                    "source": "matrixark-sdk",
                    "hook_type": "before_llm",
                    "hook_id": "hook-before-gpu",
                    "observed_at_ms": 1781500000000,
                    "idempotency_key": "gpu-approval-1",
                    "trigger": "user_message",
                    "auto_captured": True,
                },
            },
        )
        self.assertEqual(ingest["status"], "accepted")
        self.assertTrue(ingest["hook_captured"])
        self.assertNotIn("infer", ingest)
        self.assertEqual(ingest["extraction_mode"], "matrixark_internal")
        self.assertEqual(ingest["classification"], "NEW_EVENT")

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "approved GPU request",
                "scope": {"team": "infra_team", "project": "project_1"},
                "max_context_tokens": 8,
            },
        )
        self.assertFalse(pack["insufficient_context"])
        self.assertEqual(pack["selected_refs"][0]["ref_hash"], ingest["event_id_hash"])
        self.assertEqual(pack["query_embedding_model"], "matrixark-local-token-hash-v1")
        self.assertTrue(pack["layer_scores"])
        self.assertGreater(pack["selected_refs"][0]["node_score"], 0)

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        record_types = [record["record_type"] for record in replay["events"]]
        self.assertIn("context_event", record_types)
        self.assertIn("context_summary", record_types)
        self.assertIn("context_embedding", record_types)
        self.assertTrue(any(record.get("summary_type") == "session_l0" for record in replay["events"]))
        event = next(record for record in replay["events"] if record.get("event_id_hash") == ingest["event_id_hash"])
        self.assertTrue(event["summary_text"])
        self.assertEqual(len(event["summary_embedding"]), 32)

    def test_agent_direct_ingest_generates_summary_embedding_and_retrieves_by_layer_score(self):
        ingest = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "user", "content": "The rollback runbook says restart the stream proxy first."}
                ],
                "scope": {"user_id": "alice", "session_id": "thread-runbook"},
                "metadata": {"node_path": ["company_a", "infra_team", "runbooks", "stream_proxy"]},
            },
        )
        self.assertEqual(ingest["classification"], "NEW_EVENT")
        self.assertFalse(ingest["hook_captured"])

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "stream proxy rollback restart",
                "scope": {"session_id": "thread-runbook"},
                "max_context_tokens": 4,
            },
        )
        self.assertFalse(pack["insufficient_context"])
        self.assertEqual(pack["selected_refs"][0]["ref_hash"], ingest["event_id_hash"])
        self.assertGreater(pack["selected_refs"][0]["embedding_score"], 0)
        self.assertTrue(any(score["depth"] >= 3 for score in pack["layer_scores"]))

    def test_non_feedback_extraction_uses_same_session_prior_context(self):
        prior = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "assistant", "content": "Project Phoenix uses stream proxy rollback runbook A."}
                ],
                "scope": {"user_id": "alice", "session_id": "thread-phoenix"},
            },
        )

        event = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "user", "content": "For this project, restart the proxy before draining queues."}
                ],
                "scope": {"user_id": "alice", "session_id": "thread-phoenix"},
            },
        )
        self.assertEqual(event["classification"], "NEW_EVENT")
        self.assertEqual(event["prior_context"], "session")
        self.assertEqual(event["prior_message_count"], 1)
        self.assertGreaterEqual(event["prior_summary_count"], 1)
        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        latest = replay["events"][-1]
        self.assertEqual(latest["prior_context"]["summaries"][0]["ref_type"], "session_summary")
        self.assertIn(prior["event_id_hash"], {ref["ref_hash"] for ref in event["prior_refs"] if ref["ref_type"] == "event"})

    def test_feedback_without_prior_context_is_ambiguous(self):
        result = self.call_tool(
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "yes"}],
                "scope": {"team": "infra_team"},
                "agent_hook": {
                    "source": "matrixark-sdk",
                    "hook_type": "after_llm",
                    "hook_id": "hook-after-ambiguous",
                    "observed_at_ms": 1781500000100,
                    "idempotency_key": "ambiguous-yes",
                    "trigger": "final_answer_feedback",
                    "auto_captured": True,
                },
            },
        )
        self.assertEqual(result["classification"], "AMBIGUOUS")
        self.assertIn("lacks prior context", result["quality_warning"])

    def test_feedback_with_same_session_prior_confirms(self):
        self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "assistant", "content": "The GPU approval is ready for Alice to confirm."}
                ],
                "scope": {
                    "user_id": "alice",
                    "session_id": "thread-gpu",
                    "team": "infra_team",
                },
            },
        )

        result = self.call_tool(
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "yes, approved"}],
                "scope": {
                    "user_id": "alice",
                    "session_id": "thread-gpu",
                    "team": "infra_team",
                },
            },
        )
        self.assertEqual(result["extraction_mode"], "matrixark_internal")
        self.assertEqual(result["classification"], "CONFIRMATION")
        self.assertEqual(result["prior_context"], "session")
        self.assertEqual(result["prior_message_count"], 1)
        self.assertGreaterEqual(result["prior_summary_count"], 1)
        self.assertIn("event", {ref["ref_type"] for ref in result["prior_refs"]})
        self.assertEqual(result["quality_warning"], "")

    def test_feedback_with_session_id_only_confirms(self):
        self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "assistant", "content": "The GPU approval is ready for this thread to confirm."}
                ],
                "scope": {"session_id": "thread-gpu", "team": "infra_team"},
            },
        )

        result = self.call_tool(
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "yes, approved"}],
                "scope": {"session_id": "thread-gpu", "team": "infra_team"},
            },
        )
        self.assertEqual(result["classification"], "CONFIRMATION")
        self.assertEqual(result["prior_context"], "session")
        self.assertEqual(result["prior_message_count"], 1)
        self.assertGreaterEqual(result["prior_summary_count"], 1)
        self.assertIn("event", {ref["ref_type"] for ref in result["prior_refs"]})
        self.assertEqual(result["quality_warning"], "")

    def test_feedback_prior_context_window_is_bounded_and_replayable(self):
        for index in range(12):
            self.call_tool(
                "matrixark_ingest",
                {
                    "messages": [
                        {"role": "assistant", "content": f"Prior GPU approval context turn {index}."}
                    ],
                    "scope": {"user_id": "alice", "session_id": "thread-gpu"},
                },
            )

        result = self.call_tool(
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "yes, approved"}],
                "scope": {"user_id": "alice", "session_id": "thread-gpu"},
            },
        )
        self.assertEqual(result["classification"], "CONFIRMATION")
        self.assertEqual(result["prior_context"], "session")
        self.assertEqual(result["prior_message_count"], 8)
        self.assertGreaterEqual(len(result["prior_refs"]), 8)
        self.assertGreaterEqual(result["prior_summary_count"], 1)

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        feedback_record = replay["events"][-1]
        self.assertEqual(feedback_record["prior_context"]["level"], "session")
        self.assertEqual(len(feedback_record["prior_context"]["messages"]), 8)
        self.assertIn("Prior GPU approval context turn 11", feedback_record["prior_context"]["messages"][0]["text"])

    def test_feedback_with_context_pack_uses_pack_summary_not_raw_messages(self):
        ingest = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "assistant", "content": "Alice approved the GPU budget after finance review."}
                ],
                "scope": {"user_id": "alice", "session_id": "thread-pack"},
            },
        )
        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "GPU budget approval",
                "scope": {"session_id": "thread-pack"},
                "max_context_tokens": 4,
            },
        )
        self.assertEqual(pack["selected_refs"][0]["ref_hash"], ingest["event_id_hash"])

        result = self.call_tool(
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "yes, correct"}],
                "scope": {"user_id": "alice", "session_id": "thread-pack"},
                "context_pack_id": pack["context_pack_id"],
            },
        )
        self.assertEqual(result["classification"], "CONFIRMATION")
        self.assertEqual(result["prior_context"], "explicit")
        self.assertEqual(result["prior_message_count"], 0)
        self.assertEqual(result["prior_summary_count"], 1)
        self.assertEqual(result["prior_refs"][0]["ref_hash"], ingest["event_id_hash"])

        replay = self.call_tool("matrixark_replay", {"context_pack_id": pack["context_pack_id"]})
        feedback_record = replay["events"][-1]
        self.assertEqual(feedback_record["prior_context"]["summaries"][0]["ref_type"], "context_pack")
        self.assertEqual(feedback_record["prior_context"]["messages"], [])

    def test_feedback_without_session_id_uses_user_fallback_with_warning(self):
        self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "assistant", "content": "The GPU approval is ready for Alice to confirm."}
                ],
                "scope": {"user_id": "alice", "team": "infra_team"},
            },
        )

        result = self.call_tool(
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "yes, approved"}],
                "scope": {"user_id": "alice", "team": "infra_team"},
            },
        )
        self.assertEqual(result["classification"], "CONFIRMATION")
        self.assertEqual(result["prior_context"], "user")
        self.assertEqual(result["prior_message_count"], 1)
        self.assertGreaterEqual(result["prior_summary_count"], 1)
        self.assertIn("event", {ref["ref_type"] for ref in result["prior_refs"]})
        self.assertIn("user_id fallback", result["quality_warning"])

    def test_legacy_infer_false_feedback_is_ignored_and_still_extracts(self):
        result = self.call_tool(
            "matrixark_feedback",
            {
                "messages": [{"role": "user", "content": "yes"}],
                "scope": {"user_id": "alice", "session_id": "thread-gpu"},
                "infer": False,
            },
        )
        self.assertNotIn("infer", result)
        self.assertEqual(result["extraction_mode"], "matrixark_internal")
        self.assertEqual(result["classification"], "AMBIGUOUS")

    def test_initialize_protocol_shape(self):
        response = self.server.handle({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
        self.assertEqual(response["result"]["serverInfo"]["name"], "matrixark-context")
        self.assertIn("tools", response["result"]["capabilities"])

    def test_stdio_content_length_framing(self):
        message = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode()
        framed = b"Content-Length: " + str(len(message)).encode() + b"\r\n\r\n" + message
        proc = subprocess.run(
            [
                sys.executable,
                "tools/matrixark_mcp_server.py",
                "--event-log",
                str(self.event_log),
            ],
            input=framed,
            stdout=subprocess.PIPE,
            check=True,
        )
        self.assertTrue(proc.stdout.startswith(b"Content-Length:"))
        body = proc.stdout.split(b"\r\n\r\n", 1)[1]
        response = json.loads(body)
        names = [tool["name"] for tool in response["result"]["tools"]]
        self.assertIn("matrixark_ingest", names)


if __name__ == "__main__":
    unittest.main()
