import json
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from tools.matrixark_mcp_server import (
    MatrixArkLocalAdapter,
    MatrixArkMcpServer,
    apply_statistical_operator,
    latest_record,
    score_recall_candidate,
)


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
                "matrixark_batch_extract",
                "matrixark_session_commit",
                "matrixark_refresh_summaries",
                "matrixark_retrieve",
                "matrixark_feedback",
                "matrixark_replay",
                "matrixark_admin_create_account",
                "matrixark_admin_update_account",
                "matrixark_admin_list_accounts",
                "matrixark_admin_create_user",
                "matrixark_admin_update_user",
                "matrixark_admin_list_users",
                "matrixark_admin_create_api_key",
                "matrixark_admin_list_api_keys",
                "matrixark_admin_rotate_api_key",
                "matrixark_admin_revoke_api_key",
                "matrixark_admin_map_sso_user",
                "matrixark_admin_audit",
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
        refresh = next(tool for tool in response["result"]["tools"] if tool["name"] == "matrixark_refresh_summaries")
        self.assertIn("limit", refresh["inputSchema"]["properties"])
        retrieve = next(tool for tool in response["result"]["tools"] if tool["name"] == "matrixark_retrieve")
        self.assertIn("local_context", retrieve["inputSchema"]["properties"])
        self.assertIn("local_context_tokens", retrieve["inputSchema"]["properties"])
        self.assertIn("shared prompt context budget", retrieve["inputSchema"]["properties"]["max_context_tokens"]["description"])
        session_commit = next(tool for tool in response["result"]["tools"] if tool["name"] == "matrixark_session_commit")
        self.assertIn("commit_reason", session_commit["inputSchema"]["properties"])
        self.assertIn("idle_timeout_ms", session_commit["inputSchema"]["properties"])
        self.assertIn("idle_commit_timeout_ms", ingest["inputSchema"]["properties"])
        create_key = next(tool for tool in response["result"]["tools"] if tool["name"] == "matrixark_admin_create_api_key")
        self.assertIn("api_key", create_key["inputSchema"]["properties"])
        self.assertIn("allowed_user_ids", create_key["inputSchema"]["properties"])
        self.assertIn("allowed_session_ids", create_key["inputSchema"]["properties"])
        create_user = next(tool for tool in response["result"]["tools"] if tool["name"] == "matrixark_admin_create_user")
        self.assertEqual(create_user["inputSchema"]["required"], ["user_id"])
        self.assertIn("matrixark_admin_update_account", names)
        self.assertIn("matrixark_admin_list_accounts", names)
        self.assertIn("matrixark_admin_update_user", names)
        self.assertIn("matrixark_admin_list_users", names)
        self.assertIn("matrixark_admin_list_api_keys", names)

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
        self.assertFalse(pack["layer_scores"])
        self.assertTrue(pack["recall_policy"]["tree_traversal"]["fallback_to_flat"])
        self.assertEqual(pack["recall_policy"]["tree_traversal"]["fallback_reason"], "missing_or_stale_summary_embeddings")

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        record_types = [record["record_type"] for record in replay["events"]]
        self.assertIn("context_node", record_types)
        self.assertIn("context_child_ref", record_types)
        self.assertIn("context_event", record_types)
        self.assertIn("context_summary", record_types)
        self.assertIn("context_embedding", record_types)
        self.assertIn("context_summary_dirty", record_types)
        self.assertTrue(any(record.get("summary_type") == "session_l0" for record in replay["events"]))
        self.assertFalse(any(record.get("summary_type") == "node_l0" for record in replay["events"]))
        self.assertFalse(any(record.get("summary_type") == "node_l1" for record in replay["events"]))
        self.assertFalse(any(record.get("embedding_type") == "node_l0" for record in replay["events"]))
        self.assertFalse(any(record.get("embedding_type") == "node_l1" for record in replay["events"]))
        event = next(record for record in replay["events"] if record.get("event_id_hash") == ingest["event_id_hash"])
        self.assertTrue(event["summary_text"])
        self.assertEqual(len(event["summary_embedding"]), 32)
        self.assertEqual(ingest["node_materialization"]["nodes_created"], 4)
        self.assertEqual(ingest["node_materialization"]["child_refs_created"], 3)
        nodes = [record for record in replay["events"] if record.get("record_type") == "context_node"]
        child_refs = [record for record in replay["events"] if record.get("record_type") == "context_child_ref"]
        self.assertEqual(len(nodes), 4)
        self.assertEqual(len(child_refs), 3)
        self.assertTrue(any(record.get("node_path") == ["company_a", "infra_team", "project_1", "approvals"] for record in nodes))

        second_ingest = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Bob confirmed the same GPU request."}],
                "scope": {"team": "infra_team", "project": "project_1"},
                "metadata": {"node_path": ["company_a", "infra_team", "project_1", "approvals"]},
            },
        )
        self.assertEqual(second_ingest["node_materialization"]["nodes_created"], 0)
        self.assertEqual(second_ingest["node_materialization"]["child_refs_created"], 0)

    def test_ingest_marks_dirty_and_async_refresh_writes_versioned_node_summaries(self):
        ingest = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "user", "content": "Alice approved the GPU request after finance review."}
                ],
                "scope": {"user_id": "alice", "session_id": "sess-summary"},
                "metadata": {"node_path": ["user:alice", "topic:approvals", "entity:gpu_request"]},
            },
        )
        self.assertEqual(ingest["status"], "accepted")
        self.assertEqual(ingest["summary_refresh"]["status"], "dirty_marked")
        self.assertIsNone(ingest["summary_refresh"]["refresh_result"])

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        dirty_records = [
            record
            for record in replay["events"]
            if record.get("record_type") == "context_summary_dirty"
        ]
        self.assertTrue(dirty_records)
        self.assertTrue(all(record.get("status") == "pending" for record in dirty_records))
        self.assertFalse(any(record.get("record_type") == "context_summary_refresh_audit" for record in replay["events"]))
        self.assertFalse(any(record.get("summary_type") in {"node_l0", "node_l1"} for record in replay["events"]))

        refresh = self.call_tool("matrixark_refresh_summaries", {"scope": {"user_id": "alice", "session_id": "sess-summary"}})
        self.assertGreaterEqual(refresh["refreshed_count"], 1)
        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})

        refresh_audits = [
            record
            for record in replay["events"]
            if record.get("record_type") == "context_summary_refresh_audit"
        ]
        self.assertTrue(refresh_audits)
        self.assertTrue(all(record.get("status") == "refreshed" for record in refresh_audits))

        refreshed_version_hashes = {
            record["summary_version_hash"]
            for record in refresh_audits
            if record.get("summary_version_hash")
        }
        versioned_summaries = [
            record
            for record in replay["events"]
            if record.get("record_type") == "context_summary"
            and record.get("summary_type") in {"node_l0", "node_l1"}
            and record.get("summary_version_hash") in refreshed_version_hashes
        ]
        self.assertTrue(versioned_summaries)
        self.assertTrue(
            any(ingest["event_id_hash"] in record.get("source_event_ids", []) for record in versioned_summaries)
        )

        versioned_embeddings = [
            record
            for record in replay["events"]
            if record.get("record_type") == "context_embedding"
            and record.get("embedding_type") in {"node_l0", "node_l1"}
            and record.get("summary_version_hash") in refreshed_version_hashes
        ]
        self.assertTrue(versioned_embeddings)
        self.assertTrue(all(len(record.get("vector", [])) == 32 for record in versioned_embeddings))

    def test_retrieve_dedupes_remote_context_against_local_context_budget(self):
        duplicate_text = "The rollout checklist is already in the open file."
        unique_text = "Priya owns the rollout launch plan."
        duplicate = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": duplicate_text}],
                "scope": {"user_id": "alice", "session_id": "sess-local-remote"},
                "metadata": {"node_path": ["user:alice", "topic:gpu"]},
            },
        )
        unique = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": unique_text}],
                "scope": {"user_id": "alice", "session_id": "sess-local-remote"},
                "metadata": {"node_path": ["user:alice", "topic:gpu"]},
            },
        )

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "rollout",
                "scope": {"user_id": "alice", "session_id": "sess-local-remote"},
                "max_context_tokens": 24,
                "local_context": [
                    {
                        "ref_type": "file_snippet",
                        "source": "codex:open-buffer",
                        "text": duplicate_text,
                    }
                ],
                "local_context_tokens": 5,
            },
        )

        selected_hashes = {ref["ref_hash"] for ref in pack["selected_refs"]}
        self.assertNotIn(duplicate["event_id_hash"], selected_hashes)
        self.assertIn(unique["event_id_hash"], selected_hashes)
        self.assertGreaterEqual(pack["used_local_context_tokens"], 5)
        self.assertLessEqual(pack["used_remote_context_tokens"], pack["remote_context_budget_tokens"])
        self.assertLessEqual(pack["total_prompt_context_tokens"], 24)
        self.assertEqual(pack["local_context_policy"]["mode"], "shared_budget_dedupe")
        self.assertGreaterEqual(pack["dropped_refs"]["duplicate"], 1)

    def test_current_state_entity_update_prefers_latest_location(self):
        scope = {"user_id": "locomo-user", "session_id": "locomo-location"}
        for text in [
            "I moved to Seattle today, please remember this location.",
            "Actually I moved to Austin now for the new infra project.",
        ]:
            self.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": text}],
                    "scope": scope,
                },
            )

        commit = self.call_tool(
            "matrixark_session_commit",
            {
                "scope": scope,
                "force": True,
                "commit_reason": "hook_boundary",
            },
        )
        self.assertEqual(commit["status"], "committed")
        self.call_tool("matrixark_refresh_summaries", {"scope": scope})

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "Where is the user currently located?",
                "scope": scope,
                "max_context_tokens": 80,
            },
        )
        location_refs = [
            ref
            for ref in pack["selected_refs"]
            if ref.get("ref_type") == "entity" and ref.get("entity_type") == "location"
        ]
        self.assertTrue(location_refs)
        self.assertIn("Austin", location_refs[0]["text"])
        self.assertNotIn("Seattle", location_refs[0]["text"])

    def test_temporal_before_query_keeps_raw_dated_event_evidence(self):
        scope = {"user_id": "locomo-user", "session_id": "locomo-temporal"}
        for text in [
            "On March 2 I lived in Seattle.",
            "On April 10 I moved to Austin.",
        ]:
            self.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": text}],
                    "scope": scope,
                },
            )
        self.call_tool(
            "matrixark_session_commit",
            {"scope": scope, "force": True, "commit_reason": "hook_boundary"},
        )
        self.call_tool("matrixark_refresh_summaries", {"scope": scope})

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "Where was the user before April 10?",
                "scope": scope,
                "max_context_tokens": 80,
            },
        )
        raw_event_text = "\n".join(
            ref.get("text", "")
            for ref in pack["selected_refs"]
            if ref.get("ref_type") == "event"
        )
        self.assertEqual(pack["question_type"], "date")
        self.assertIn("March 2", raw_event_text)
        self.assertIn("Seattle", raw_event_text)

    def test_access_management_key_lifecycle_and_session_isolation(self):
        account = self.call_tool(
            "matrixark_admin_create_account",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "account_name": "Acme",
                "tenant_name": "Engineering",
            },
        )
        self.assertEqual(account["status"], "created")

        created_user = self.call_tool(
            "matrixark_admin_create_user",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "user_id": "alice",
                "display_name": "Alice",
                "external_subject": "okta:alice@acme.com",
            },
        )
        self.assertEqual(created_user["status"], "created")
        self.assertEqual(created_user["user_id"], "alice")
        self.assertTrue(created_user["user_hash"])

        created_key = self.call_tool(
            "matrixark_admin_create_api_key",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "scopes": ["context:ingest", "context:retrieve", "context:replay"],
                "role": "agent_service",
                "display_name": "alice codex hook",
                "allowed_user_ids": ["alice"],
                "allowed_session_ids": ["sess-a"],
                "expires_at_ms": 4102444800000,
            },
        )
        api_key = created_key["api_key"]
        self.assertTrue(api_key.startswith("mk_test_"))
        self.assertEqual(created_key["allowed_user_ids"], ["alice"])
        self.assertEqual(created_key["allowed_session_ids"], ["sess-a"])
        self.assertEqual(created_key["expires_at_ms"], 4102444800000)
        self.assertNotIn(api_key, self.event_log.read_text())

        ingest = self.call_tool(
            "matrixark_ingest",
            {
                "api_key": api_key,
                "messages": [{"role": "user", "content": "Alice approved the GPU purchase for Project Orion."}],
                "scope": {"user_id": "alice", "session_id": "sess-a", "team": "infra"},
            },
        )
        self.assertEqual(ingest["access"]["account_id"], "acct_acme")
        self.assertEqual(ingest["access"]["tenant_id"], "tenant_eng")
        replay = self.call_tool("matrixark_replay", {"api_key": api_key, "context_pack_id": "debug", "scope": {"user_id": "alice", "session_id": "sess-a"}})
        event = next(record for record in replay["events"] if record.get("event_id_hash") == ingest["event_id_hash"])
        self.assertEqual(event["envelope"]["scope"]["account_id"], "acct_acme")
        self.assertEqual(event["envelope"]["scope"]["tenant_id"], "tenant_eng")
        self.assertTrue(event["envelope"]["scope"]["tenant_hash"])
        self.assertTrue(event["envelope"]["scope"]["user_hash"])
        self.assertTrue(event["envelope"]["scope"]["session_hash"])

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "api_key": api_key,
                "query": "GPU Project Orion approval",
                "scope": {"user_id": "alice", "session_id": "sess-a"},
            },
        )
        self.assertFalse(pack["insufficient_context"])
        missing_user = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 96,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {
                        "api_key": api_key,
                        "query": "GPU Project Orion approval",
                        "scope": {"session_id": "sess-a"},
                    },
                },
            }
        )
        self.assertIn("error", missing_user)
        self.assertIn("scope.user_id is required", missing_user["error"]["message"])
        wrong_user = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 97,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {
                        "api_key": api_key,
                        "query": "GPU Project Orion approval",
                        "scope": {"user_id": "bob", "session_id": "sess-a"},
                    },
                },
            }
        )
        self.assertIn("error", wrong_user)
        self.assertIn("scope.user_id is not allowed", wrong_user["error"]["message"])
        wrong_session = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 98,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {
                        "api_key": api_key,
                        "query": "GPU Project Orion approval",
                        "scope": {"user_id": "alice", "session_id": "sess-b"},
                    },
                },
            }
        )
        self.assertIn("error", wrong_session)
        self.assertIn("scope.session_id is not allowed", wrong_session["error"]["message"])

        users = self.call_tool("matrixark_admin_list_users", {"account_id": "acct_acme", "tenant_id": "tenant_eng"})
        self.assertEqual(users["count"], 1)
        self.assertEqual(users["users"][0]["user_id"], "alice")

        keys = self.call_tool("matrixark_admin_list_api_keys", {"account_id": "acct_acme", "tenant_id": "tenant_eng"})
        self.assertEqual(keys["count"], 1)
        self.assertEqual(keys["api_keys"][0]["api_key_id"], created_key["api_key_id"])
        self.assertNotIn("api_key", keys["api_keys"][0])
        self.assertNotIn("api_key_hash", keys["api_keys"][0])

        audit = self.call_tool("matrixark_admin_audit", {"account_id": "acct_acme", "tenant_id": "tenant_eng"})
        self.assertGreaterEqual(audit["count"], 3)
        usage_rows = [row for row in audit["audit_logs"] if row.get("record_type") == "matrixark_api_key_usage"]
        self.assertTrue(usage_rows)
        self.assertTrue(any(row.get("action") == "matrixark_ingest" for row in usage_rows))
        self.assertTrue(any(row.get("action") == "matrixark_retrieve" for row in usage_rows))
        self.assertTrue(all(row.get("user_id") == "alice" for row in usage_rows))

        revoked = self.call_tool("matrixark_admin_revoke_api_key", {"api_key_id": created_key["api_key_id"]})
        self.assertEqual(revoked["status"], "revoked")
        error_response = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 99,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {"api_key": api_key, "query": "GPU", "scope": {"user_id": "alice"}},
                },
            }
        )
        self.assertIn("error", error_response)
        self.assertIn("invalid or revoked", error_response["error"]["message"])

    def test_account_tenant_status_and_scope_validation(self):
        self.call_tool(
            "matrixark_admin_create_account",
            {"account_id": "acct_acme", "tenant_id": "tenant_eng", "account_name": "Acme", "tenant_name": "Engineering"},
        )
        accounts = self.call_tool("matrixark_admin_list_accounts", {"account_id": "acct_acme"})
        self.assertEqual(accounts["count"], 1)
        self.assertEqual(accounts["accounts"][0]["account_status"], "active")
        self.assertEqual(accounts["accounts"][0]["tenant_status"], "active")

        unknown_scope = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 49,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_admin_create_api_key",
                    "arguments": {
                        "account_id": "acct_acme",
                        "tenant_id": "tenant_eng",
                        "scopes": ["context:retrieve", "context:delete_everything"],
                    },
                },
            }
        )
        self.assertIn("error", unknown_scope)
        self.assertIn("unknown MatrixArk scope", unknown_scope["error"]["message"])

        api_key = self.call_tool(
            "matrixark_admin_create_api_key",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "scopes": ["context:retrieve"],
            },
        )["api_key"]
        disabled = self.call_tool(
            "matrixark_admin_update_account",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "tenant_status": "disabled",
            },
        )
        self.assertEqual(disabled["tenant_status"], "disabled")
        blocked = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 48,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {"api_key": api_key, "query": "anything", "scope": {"user_id": "alice"}},
                },
            }
        )
        self.assertIn("error", blocked)
        self.assertIn("tenant is disabled", blocked["error"]["message"])

        reenabled = self.call_tool(
            "matrixark_admin_update_account",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "tenant_status": "active",
                "account_status": "disabled",
            },
        )
        self.assertEqual(reenabled["account_status"], "disabled")
        blocked_account = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 47,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {"api_key": api_key, "query": "anything", "scope": {"user_id": "alice"}},
                },
            }
        )
        self.assertIn("error", blocked_account)
        self.assertIn("account is disabled", blocked_account["error"]["message"])

    def test_disabled_user_blocks_api_key_context_access(self):
        self.call_tool(
            "matrixark_admin_create_account",
            {"account_id": "acct_acme", "tenant_id": "tenant_eng"},
        )
        self.call_tool(
            "matrixark_admin_create_user",
            {"account_id": "acct_acme", "tenant_id": "tenant_eng", "user_id": "alice"},
        )
        created_key = self.call_tool(
            "matrixark_admin_create_api_key",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "scopes": ["context:retrieve"],
                "allowed_user_ids": ["alice"],
            },
        )
        disabled = self.call_tool(
            "matrixark_admin_update_user",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "user_id": "alice",
                "status": "disabled",
            },
        )
        self.assertEqual(disabled["user_status"], "disabled")
        users = self.call_tool(
            "matrixark_admin_list_users",
            {"account_id": "acct_acme", "tenant_id": "tenant_eng", "status": "disabled"},
        )
        self.assertEqual(users["count"], 1)

        response = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 50,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {
                        "api_key": created_key["api_key"],
                        "query": "GPU",
                        "scope": {"user_id": "alice"},
                    },
                },
            }
        )
        self.assertIn("error", response)
        self.assertIn("scope.user_id is disabled", response["error"]["message"])

    def test_api_key_expiry_and_admin_account_boundaries(self):
        self.call_tool(
            "matrixark_admin_create_account",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
            },
        )
        expired_response = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 51,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_admin_create_api_key",
                    "arguments": {
                        "account_id": "acct_acme",
                        "tenant_id": "tenant_eng",
                        "scopes": ["context:ingest"],
                        "expires_at_ms": 1,
                    },
                },
            }
        )
        self.assertIn("error", expired_response)
        self.assertIn("expires_at_ms must be a future", expired_response["error"]["message"])

        admin_key = self.call_tool(
            "matrixark_admin_create_api_key",
            {
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
                "scopes": ["admin:api_key", "admin:user", "admin:audit"],
                "role": "tenant_admin",
            },
        )["api_key"]
        create_user = self.call_tool(
            "matrixark_admin_create_user",
            {
                "api_key": admin_key,
                "user_id": "alice",
                "display_name": "Alice",
                "scope": {"account_id": "acct_acme", "tenant_id": "tenant_eng"},
            },
        )
        self.assertEqual(create_user["status"], "created")

        cross_account_response = self.server.handle(
            {
                "jsonrpc": "2.0",
                "id": 52,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_admin_create_api_key",
                    "arguments": {
                        "api_key": admin_key,
                        "account_id": "acct_other",
                        "tenant_id": "tenant_eng",
                        "scopes": ["context:retrieve"],
                    },
                },
            }
        )
        self.assertIn("error", cross_account_response)
        self.assertIn("admin operation account/tenant does not match API key", cross_account_response["error"]["message"])

        api_keys = self.call_tool(
            "matrixark_admin_list_api_keys",
            {"api_key": admin_key, "scope": {"account_id": "acct_acme", "tenant_id": "tenant_eng"}, "include_revoked": True},
        )
        self.assertGreaterEqual(api_keys["count"], 1)
        self.assertTrue(all("api_key_hash" not in key for key in api_keys["api_keys"]))

    def test_sso_mapping_and_enforced_mode_requires_api_key(self):
        mapped = self.call_tool(
            "matrixark_admin_map_sso_user",
            {
                "provider": "okta",
                "external_user_id": "alice@acme.com",
                "account_id": "acct_acme",
                "tenant_id": "tenant_eng",
            },
        )
        self.assertEqual(mapped["status"], "mapped")
        self.assertTrue(mapped["matrixark_user_id"].startswith("mu_"))

        enforced = MatrixArkMcpServer(MatrixArkLocalAdapter(self.event_log), access_mode="enforced")
        response = enforced.handle(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "matrixark_retrieve",
                    "arguments": {"query": "anything", "scope": {"user_id": "alice"}},
                },
            }
        )
        self.assertIn("error", response)
        self.assertIn("API key is required", response["error"]["message"])

    def test_session_commit_derives_segments_entities_without_duplicate_events(self):
        scope = {"user_id": "alice", "session_id": "thread-session-buffer"}
        first = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "I moved to Seattle today."}],
                "scope": scope,
            },
        )
        second = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Actually I moved to Austin now."}],
                "scope": scope,
            },
        )
        self.assertEqual(first["session_buffer"]["pending_event_count"], 1)
        self.assertEqual(second["session_buffer"]["pending_event_count"], 2)

        committed = self.call_tool(
            "matrixark_session_commit",
            {
                "scope": scope,
                "threshold_messages": 20,
                "force": True,
                "agent_hook": {
                    "source": "codex",
                    "hook_type": "session_commit",
                    "hook_id": "commit-location",
                    "observed_at_ms": 1781500000000,
                    "auto_captured": True,
                },
            },
        )
        self.assertEqual(committed["status"], "committed")
        self.assertEqual(committed["events_written"], 0)
        self.assertFalse(committed["raw_events_duplicated"])
        self.assertEqual(committed["commit_reason"], "manual_api")
        self.assertEqual(committed["trigger_policy"], "force")
        self.assertEqual(set(committed["source_event_ids"]), {first["event_id_hash"], second["event_id_hash"]})
        self.assertGreaterEqual(committed["segments_written"], 1)
        self.assertGreaterEqual(committed["entities_written"], 1)

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        events = [record for record in replay["events"] if record.get("record_type") == "context_event"]
        self.assertEqual(len(events), 2)
        segments = [record for record in replay["events"] if record.get("record_type") == "context_segment"]
        self.assertTrue(segments)
        self.assertTrue(any(set(segment.get("source_event_ids", [])) for segment in segments))
        entities = [record for record in replay["events"] if record.get("record_type") == "context_entity"]
        self.assertTrue(any(record.get("entity_type") == "location" for record in entities))
        self.assertTrue(all(record.get("source_event_ids") for record in entities))
        commits = [record for record in replay["events"] if record.get("record_type") == "context_batch_commit"]
        self.assertEqual(len(commits), 1)

    def test_single_message_online_path_then_later_batch_extraction_contract(self):
        scope = {"user_id": "online-user", "session_id": "online-then-batch"}
        first = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Alice approved the GPU rollout checklist."}],
                "scope": scope,
                "metadata": {"node_path": ["principal:online-user", "collection:sessions", "session:online-then-batch"]},
            },
        )
        replay_after_one = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        record_types_after_one = [record.get("record_type") for record in replay_after_one["events"]]

        self.assertIn("context_node", record_types_after_one)
        self.assertIn("context_child_ref", record_types_after_one)
        self.assertIn("context_event", record_types_after_one)
        self.assertIn("context_embedding", record_types_after_one)
        self.assertIn("session_buffer_event", record_types_after_one)
        self.assertIn("context_summary_dirty", record_types_after_one)
        self.assertIn("context_summary", record_types_after_one)
        self.assertTrue(any(record.get("embedding_type") == "event_text" for record in replay_after_one["events"]))
        self.assertTrue(any(record.get("summary_type") == "session_l0" for record in replay_after_one["events"]))
        self.assertNotIn("context_summary_refresh_audit", record_types_after_one)
        self.assertFalse(any(record.get("summary_type") == "node_l0" for record in replay_after_one["events"]))
        self.assertFalse(any(record.get("summary_type") == "node_l1" for record in replay_after_one["events"]))

        self.assertNotIn("context_segment", record_types_after_one)
        self.assertNotIn("context_entity", record_types_after_one)
        self.assertNotIn("context_index", record_types_after_one)
        self.assertFalse(any(record.get("summary_type") == "batch_l0" for record in replay_after_one["events"]))
        self.assertEqual(first["session_buffer"]["pending_event_count"], 1)

        second = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "assistant", "content": "Finance also confirmed the rollout budget."}],
                "scope": scope,
                "metadata": {"node_path": ["principal:online-user", "collection:sessions", "session:online-then-batch"]},
            },
        )
        self.assertEqual(second["session_buffer"]["pending_event_count"], 2)
        committed = self.call_tool(
            "matrixark_session_commit",
            {
                "scope": scope,
                "threshold_messages": 20,
                "force": True,
                "metadata": {"node_path": ["principal:online-user", "collection:sessions", "session:online-then-batch"]},
            },
        )
        self.assertEqual(committed["status"], "committed")
        self.assertEqual(committed["events_written"], 0)
        self.assertFalse(committed["raw_events_duplicated"])
        self.assertEqual(set(committed["source_event_ids"]), {first["event_id_hash"], second["event_id_hash"]})

        replay_after_commit = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        record_types_after_commit = [record.get("record_type") for record in replay_after_commit["events"]]
        self.assertEqual(record_types_after_commit.count("context_event"), 2)
        self.assertIn("context_segment", record_types_after_commit)
        self.assertIn("context_entity", record_types_after_commit)
        self.assertIn("context_index", record_types_after_commit)
        self.assertTrue(any(record.get("summary_type") == "batch_l0" for record in replay_after_commit["events"]))
        self.assertEqual(committed["node_materialization"]["nodes_created"], 0)
        self.assertEqual(committed["node_materialization"]["child_refs_created"], 0)

    def test_ingest_auto_batch_extract_commits_at_threshold(self):
        scope = {"user_id": "bob", "session_id": "thread-auto-buffer"}
        first = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "I prefer Rust for storage engines."}],
                "scope": scope,
                "auto_batch_extract": True,
                "session_buffer_threshold": 2,
            },
        )
        self.assertIsNone(first["auto_batch_extract_result"])
        second = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "assistant", "content": "Noted that Rust is the current storage preference."}],
                "scope": scope,
                "auto_batch_extract": True,
                "session_buffer_threshold": 2,
            },
        )
        auto = second["auto_batch_extract_result"]
        self.assertIsNotNone(auto)
        self.assertEqual(auto["status"], "committed")
        self.assertEqual(auto["commit_reason"], "threshold")
        self.assertEqual(auto["trigger_policy"], "threshold")
        self.assertEqual(auto["events_written"], 0)
        self.assertFalse(auto["raw_events_duplicated"])

    def test_idle_timeout_commit_runs_before_new_message(self):
        scope = {"user_id": "idle-user", "session_id": "thread-idle-buffer"}
        first = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Remember that I prefer Rust for services."}],
                "scope": scope,
            },
        )
        self.assertEqual(first["session_buffer"]["pending_event_count"], 1)
        second = self.call_tool(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "New turn after the idle boundary."}],
                "scope": scope,
                "idle_commit_timeout_ms": 0,
            },
        )
        idle = second["idle_commit_result"]
        self.assertIsNotNone(idle)
        self.assertEqual(idle["status"], "committed")
        self.assertEqual(idle["commit_reason"], "idle_timeout")
        self.assertEqual(idle["trigger_policy"], "idle_timeout")
        self.assertEqual(idle["committed_event_count"], 1)
        self.assertEqual(idle["source_event_ids"], [first["event_id_hash"]])
        self.assertEqual(second["session_buffer"]["pending_event_count"], 1)

    def test_rolling_threshold_commit_extracts_only_next_window(self):
        scope = {"user_id": "rolling-user", "session_id": "thread-rolling-buffer"}
        ingested = []
        for index in range(5):
            ingested.append(
                self.call_tool(
                    "matrixark_ingest",
                    {
                        "messages": [{"role": "user", "content": f"Rolling memory turn {index} about Rust services."}],
                        "scope": scope,
                    },
                )
            )
        first_window = self.call_tool(
            "matrixark_session_commit",
            {
                "scope": scope,
                "threshold_messages": 2,
                "force": False,
                "commit_reason": "threshold",
            },
        )
        self.assertEqual(first_window["status"], "committed")
        self.assertEqual(first_window["committed_event_count"], 2)
        self.assertEqual(first_window["source_event_ids"], [item["event_id_hash"] for item in ingested[:2]])
        second_window = self.call_tool(
            "matrixark_session_commit",
            {
                "scope": scope,
                "threshold_messages": 2,
                "force": False,
                "commit_reason": "threshold",
            },
        )
        self.assertEqual(second_window["status"], "committed")
        self.assertEqual(second_window["committed_event_count"], 2)
        self.assertEqual(second_window["source_event_ids"], [item["event_id_hash"] for item in ingested[2:4]])
        deferred = self.call_tool(
            "matrixark_session_commit",
            {
                "scope": scope,
                "threshold_messages": 2,
                "force": False,
                "commit_reason": "threshold",
            },
        )
        self.assertEqual(deferred["status"], "deferred")
        self.assertEqual(deferred["pending_event_count"], 1)

    def test_oss_segment_provider_emits_model_boundaries(self):
        scope = {"user_id": "segment-user", "session_id": "oss-segment-session"}
        model_output = {
            "segments": [
                {
                    "topic": "recursion_learning",
                    "coordinate_tuples": [[1, 1], [3, 3]],
                    "message_indexes": [1, 3],
                    "saliency_score": 0.97,
                    "summary_text": "Recursion definition and merge sort usage.",
                }
            ]
        }
        with patch("tools.matrixark_mcp_server.oss_model_memory_segments", return_value=model_output) as mocked:
            result = self.call_tool(
                "matrixark_batch_extract",
                {
                    "messages": [
                        {"role": "user", "content": "Hi"},
                        {"role": "user", "content": "Recursion means solving a smaller version of the same problem."},
                        {"role": "assistant", "content": "A game algorithm note is unrelated."},
                        {"role": "user", "content": "Merge sort uses recursion to split and combine arrays."},
                    ],
                    "scope": scope,
                    "threshold_messages": 1,
                    "segment_provider": "oss",
                    "segment_model": "local-test-model",
                },
            )
        mocked.assert_called_once()
        self.assertEqual(result["segment_provider"]["provider"], "oss")
        self.assertEqual(result["segment_provider"]["execution_mode"], "oss_model")
        self.assertEqual(result["segment_provider"]["model"], "local-test-model")
        self.assertFalse(result["segment_provider"]["fallback_used"])

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        segments = [record for record in replay["events"] if record.get("record_type") == "context_segment"]
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["topic"], "recursion_learning")
        self.assertTrue(segments[0]["non_contiguous"])
        self.assertEqual(segments[0]["message_indexes"], [1, 3])
        self.assertEqual(len(segments[0]["source_event_ids"]), 2)

    def test_oss_segment_provider_can_fallback_to_rules(self):
        scope = {"user_id": "segment-user", "session_id": "oss-fallback-session"}
        with patch("tools.matrixark_mcp_server.oss_model_memory_segments", side_effect=RuntimeError("model unavailable")):
            result = self.call_tool(
                "matrixark_batch_extract",
                {
                    "messages": [
                        {"role": "user", "content": "The rollback runbook says restart stream proxy first."},
                        {"role": "assistant", "content": "Thanks"},
                    ],
                    "scope": scope,
                    "threshold_messages": 1,
                    "segment_provider": "oss-fallback",
                    "segment_model": "missing-local-model",
                },
            )
        self.assertEqual(result["segment_provider"]["provider"], "oss")
        self.assertEqual(result["segment_provider"]["execution_mode"], "rules_fallback")
        self.assertTrue(result["segment_provider"]["fallback_used"])
        self.assertGreaterEqual(result["segments_written"], 1)

    def test_segment_detection_keeps_generic_business_facts(self):
        scope = {"user_id": "segment-user", "session_id": "business-segment-session"}
        result = self.call_tool(
            "matrixark_batch_extract",
            {
                "messages": [
                    {"role": "user", "content": "Hi"},
                    {"role": "user", "content": "The launch checklist requires two reviewers before release."},
                    {"role": "assistant", "content": "For the game algorithm, minimax scores moves for an opponent."},
                    {"role": "user", "content": "Priya owns the launch decision and the deadline is Friday."},
                    {"role": "assistant", "content": "Thanks"},
                ],
                "scope": scope,
                "threshold_messages": 1,
            },
        )
        self.assertIn(result["status"], {"accepted", "committed"})
        self.assertGreaterEqual(result["segments_written"], 2)

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        segments = [record for record in replay["events"] if record.get("record_type") == "context_segment"]
        task_segments = [segment for segment in segments if segment.get("topic") == "task_decision"]
        self.assertTrue(task_segments)
        task_text = " ".join(segment.get("text", "") for segment in task_segments)
        self.assertIn("requires two reviewers", task_text)
        self.assertIn("deadline is Friday", task_text)
        self.assertTrue(all(segment.get("source_event_ids") for segment in task_segments))

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "Who owns the launch checklist and what review is required?",
                "scope": scope,
                "max_context_tokens": 80,
            },
        )
        self.assertFalse(pack["insufficient_context"])
        selected_topics = {ref.get("topic") for ref in pack["selected_refs"] if ref.get("ref_type") == "segment"}
        self.assertIn("task_decision", selected_topics)

    def test_vikingmem_style_twenty_message_session_window_auto_extracts_once(self):
        scope = {"user_id": "batch-user", "session_id": "vikingmem-threshold-session"}
        messages = []
        for index in range(20):
            if index % 5 == 0:
                text = f"Message {index}: I prefer Rust for storage services and low latency systems."
            elif index % 5 == 1:
                text = f"Message {index}: Alice approved GPU budget item {index} after finance review."
            elif index % 5 == 2:
                text = f"Message {index}: I moved to Austin for the infrastructure project."
            elif index % 5 == 3:
                text = f"Message {index}: My manager Priya tracks the launch status."
            else:
                text = f"Message {index}: The current plan is to finish the benchmark report."
            messages.append(text)

        results = []
        for index, text in enumerate(messages):
            results.append(
                self.call_tool(
                    "matrixark_ingest",
                    {
                        "messages": [{"role": "user", "content": text}],
                        "scope": scope,
                        "auto_batch_extract": True,
                        "session_buffer_threshold": 20,
                    },
                )
            )
        for result in results[:-1]:
            self.assertIsNone(result["auto_batch_extract_result"])
            self.assertLess(result["session_buffer"]["pending_event_count"], 20)
        auto = results[-1]["auto_batch_extract_result"]
        self.assertIsNotNone(auto)
        self.assertEqual(auto["status"], "committed")
        self.assertEqual(auto["threshold_messages"], 20)
        self.assertEqual(auto["events_written"], 0)
        self.assertEqual(auto["source_event_count"], 20)
        self.assertFalse(auto["raw_events_duplicated"])
        self.assertGreaterEqual(auto["entities_written"], 4)
        self.assertGreaterEqual(auto["segments_written"], 4)
        self.assertGreaterEqual(auto["indexes_written"], 4)

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        raw_events = [record for record in replay["events"] if record.get("record_type") == "context_event"]
        commits = [record for record in replay["events"] if record.get("record_type") == "context_batch_commit"]
        segments = [record for record in replay["events"] if record.get("record_type") == "context_segment"]
        entities = [record for record in replay["events"] if record.get("record_type") == "context_entity"]
        summaries = [record for record in replay["events"] if record.get("record_type") == "context_summary"]
        indexes = [record for record in replay["events"] if record.get("record_type") == "context_index"]
        self.assertEqual(len(raw_events), 20)
        self.assertEqual(len(commits), 1)
        self.assertTrue(all(segment.get("source_event_ids") for segment in segments))
        self.assertTrue(all(entity.get("source_event_ids") for entity in entities))
        self.assertTrue(any(summary.get("summary_type") == "batch_l0" for summary in summaries))
        self.assertTrue(indexes)

        refresh = self.call_tool("matrixark_refresh_summaries", {"scope": scope})
        self.assertGreaterEqual(refresh["refreshed_count"], 1)
        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "What storage language preference and GPU approval should I remember?",
                "scope": scope,
                "max_context_tokens": 80,
            },
        )
        self.assertFalse(pack["insufficient_context"])
        self.assertTrue({"entity", "segment", "event"}.intersection({ref["ref_type"] for ref in pack["selected_refs"]}))
        self.assertFalse(pack["recall_policy"]["tree_traversal"]["fallback_to_flat"])

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
        self.assertTrue(pack["recall_policy"]["tree_traversal"]["fallback_to_flat"])

        refresh = self.call_tool("matrixark_refresh_summaries", {"scope": {"session_id": "thread-runbook"}})
        self.assertGreaterEqual(refresh["refreshed_count"], 1)
        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "stream proxy rollback restart",
                "scope": {"session_id": "thread-runbook"},
                "max_context_tokens": 4,
            },
        )
        self.assertTrue(any(score["depth"] >= 3 for score in pack["layer_scores"]))
        traversal = pack["recall_policy"]["tree_traversal"]
        self.assertTrue(traversal["enabled"])
        self.assertFalse(traversal["fallback_to_flat"])
        self.assertGreaterEqual(traversal["selected_path_count"], 1)

    def test_batch_extract_writes_node_l0_and_retrieve_uses_tree_first_traversal(self):
        relevant = self.call_tool(
            "matrixark_batch_extract",
            {
                "messages": [
                    {"role": "user", "content": "The stream proxy rollback runbook says restart the proxy before draining queues."},
                    {"role": "assistant", "content": "Use the infra runbook for Project Phoenix rollback evidence."},
                ],
                "scope": {"user_id": "alice", "session_id": "bench-tree", "team": "infra_team"},
                "metadata": {"node_path": ["company_a", "infra_team", "runbooks", "stream_proxy"]},
                "threshold_messages": 20,
                "force": True,
            },
        )
        self.assertEqual(relevant["status"], "accepted")
        self.assertTrue(relevant["one_pass"])

        decoy = self.call_tool(
            "matrixark_batch_extract",
            {
                "messages": [
                    {"role": "user", "content": "The unrelated finance archive mentions stream proxy rollback as a decoy."},
                    {"role": "assistant", "content": "This archive is not the infra runbook path."},
                ],
                "scope": {"user_id": "alice", "session_id": "bench-tree", "team": "infra_team"},
                "metadata": {"node_path": ["company_b", "finance_team", "archives", "decoys"]},
                "threshold_messages": 20,
                "force": True,
            },
        )
        self.assertEqual(decoy["status"], "accepted")

        refresh = self.call_tool(
            "matrixark_refresh_summaries",
            {"scope": {"user_id": "alice", "session_id": "bench-tree", "team": "infra_team"}},
        )
        self.assertGreaterEqual(refresh["refreshed_count"], 1)
        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "company_a infra_team stream proxy rollback runbook",
                "scope": {"user_id": "alice", "session_id": "bench-tree", "team": "infra_team"},
                "max_context_tokens": 20,
                "ranking": {"top_k_per_layer": 1, "max_children_scored_per_parent": 32},
            },
        )
        self.assertFalse(pack["insufficient_context"])
        traversal = pack["recall_policy"]["tree_traversal"]
        self.assertTrue(traversal["enabled"])
        self.assertEqual(traversal["top_k_per_layer"], 1)
        self.assertFalse(traversal["fallback_to_flat"])
        self.assertTrue(pack["layer_scores"])
        selected_paths = [ref.get("node_path", []) for ref in pack["selected_refs"]]
        self.assertTrue(selected_paths)
        self.assertTrue(all(path[:1] == ["company_a"] for path in selected_paths), selected_paths)
        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        expected_prefixes = [
            ["company_a"],
            ["company_a", "infra_team"],
            ["company_a", "infra_team", "runbooks"],
            ["company_a", "infra_team", "runbooks", "stream_proxy"],
        ]
        summary_pairs = {
            (tuple(record.get("node_path", [])), record.get("summary_type"))
            for record in replay["events"]
            if record.get("record_type") == "context_summary"
            and record.get("summary_type") in {"node_l0", "node_l1"}
        }
        embedding_pairs = {
            (tuple(record.get("node_path", [])), record.get("embedding_type"))
            for record in replay["events"]
            if record.get("record_type") == "context_embedding"
            and record.get("embedding_type") in {"node_l0", "node_l1"}
        }
        for prefix in expected_prefixes:
            self.assertIn((tuple(prefix), "node_l0"), summary_pairs)
            self.assertIn((tuple(prefix), "node_l1"), summary_pairs)
            self.assertIn((tuple(prefix), "node_l0"), embedding_pairs)
            self.assertIn((tuple(prefix), "node_l1"), embedding_pairs)
        self.assertTrue(
            any(
                record.get("record_type") == "context_embedding"
                and record.get("embedding_type") == "node_l0"
                and record.get("node_path") == ["company_a", "infra_team", "runbooks", "stream_proxy"]
                for record in replay["events"]
            )
        )
        self.assertTrue(
            any(
                record.get("record_type") == "context_embedding"
                and record.get("embedding_type") == "node_l1"
                and record.get("node_path") == ["company_a", "infra_team", "runbooks", "stream_proxy"]
                for record in replay["events"]
            )
        )
        self.assertTrue(
            any(
                record.get("record_type") == "context_summary"
                and record.get("summary_type") == "node_l1"
                and "tree-first retrieval" in record.get("summary_text", "")
                for record in replay["events"]
            )
        )

    def test_secondary_index_prefilter_uses_general_indexes_before_scoring(self):
        approval = self.call_tool(
            "matrixark_batch_extract",
            {
                "messages": [
                    {"role": "user", "content": "Alice approved the GPU purchase after finance reviewed the cost."},
                    {"role": "assistant", "content": "The approval is valid for Project Phoenix."},
                ],
                "scope": {"user_id": "alice", "session_id": "bench-index", "team": "infra_team", "project": "project_1"},
                "metadata": {"node_path": ["company_a", "infra_team", "project_1", "approvals"]},
                "threshold_messages": 20,
                "force": True,
            },
        )
        self.assertEqual(approval["status"], "accepted")

        location = self.call_tool(
            "matrixark_batch_extract",
            {
                "messages": [
                    {"role": "user", "content": "Alice moved to Seattle and is staying near Lake Union."},
                    {"role": "assistant", "content": "Remember Alice's current location as Seattle."},
                ],
                "scope": {"user_id": "alice", "session_id": "bench-index", "team": "infra_team", "project": "project_1"},
                "metadata": {"node_path": ["company_a", "people", "alice", "location"]},
                "threshold_messages": 20,
                "force": True,
            },
        )
        self.assertEqual(location["status"], "accepted")

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        index_names = [record.get("index_name") for record in replay["events"] if record.get("record_type") == "context_index"]
        self.assertIn("entity_type:location", index_names)
        self.assertIn("event_type:confirmation", index_names)
        self.assertNotIn("infra_team", index_names)
        self.assertNotIn("project_1", index_names)

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "where is Alice located now?",
                "scope": {"user_id": "alice", "session_id": "bench-index", "team": "infra_team", "project": "project_1"},
                "max_context_tokens": 40,
                "ranking": {"top_k_per_layer": 8, "max_children_scored_per_parent": 64},
            },
        )
        self.assertFalse(pack["insufficient_context"])
        secondary = pack["recall_policy"]["secondary_index_filter"]
        self.assertTrue(secondary["enabled"])
        self.assertIn(["entity_type:location"], secondary["required_groups"])
        self.assertGreater(secondary["dropped_candidate_count"], 0)
        selected_text = " ".join(ref.get("text", "") for ref in pack["selected_refs"])
        self.assertIn("Seattle", selected_text)
        self.assertNotIn("GPU purchase", selected_text)

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
        latest = next(record for record in replay["events"] if record.get("event_id_hash") == event["event_id_hash"])
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
        feedback_record = next(record for record in replay["events"] if record.get("event_id_hash") == result["event_id_hash"])
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
        feedback_record = next(record for record in replay["events"] if record.get("event_id_hash") == result["event_id_hash"])
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

    def test_context_statistical_and_latest_operators(self):
        records = [
            {"metadata": {"value": 10}, "updated_at_ms": 1000, "state": "old"},
            {"metadata": {"value": 25}, "updated_at_ms": 3000, "state": "new"},
            {"metadata": {"value": 5}, "updated_at_ms": 2000, "state": "middle"},
        ]
        self.assertEqual(apply_statistical_operator("COUNT", records), 3)
        self.assertEqual(apply_statistical_operator("SUM", records), 40)
        self.assertEqual(apply_statistical_operator("AVG", records), 13.333333)
        self.assertEqual(apply_statistical_operator("MAX", records), 25)
        self.assertEqual(latest_record(records)["state"], "new")

    def test_decay_score_and_business_weight_operator(self):
        scored = score_recall_candidate(
            {
                "origin_score": 0.4,
                "updated_at_ms": 1_000,
                "event_type": "approval",
                "metadata": {"business_weight": 1.0},
            },
            {
                "freshness_tolerance_ms": 0,
                "half_life_ms": 1_000,
                "weights": {"time": 0.25, "business": 0.35},
            },
            reference_time_ms=5_000,
        )
        self.assertLess(scored["time_score"], 0.2)
        self.assertEqual(scored["business_score"], 1.0)
        self.assertEqual(scored["ranking_formula"], "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi")
        self.assertGreater(scored["final_score"], 0.4)

    def test_time_compression_is_retrievable_and_non_destructive(self):
        scope = {"user_id": "alice", "session_id": "operator-window", "team": "infra"}
        node_path = ["account:acct_dev", "tenant:tenant_dev", "principal:user:alice", "collection:sessions", "session:operator-window"]
        ingested = []
        for text in [
            "Alice approved the old GPU purchase after finance reviewed it.",
            "The GPU approval budget was 42000 dollars.",
            "The approval was confirmed by infra lead Sam.",
        ]:
            ingested.append(
                self.call_tool(
                    "matrixark_ingest",
                    {
                        "messages": [{"role": "user", "content": text}],
                        "scope": scope,
                        "metadata": {"node_path": node_path, "importance": 0.95, "business_weight": 0.95},
                    },
                )
            )
        records = self.server.adapter.read_all()
        event_records = [record for record in records if record.get("record_type") == "context_event"]
        self.assertEqual(len(event_records), 3)
        node_hash = ingested[0]["node_hash"]
        times = [record["envelope"]["ingestion_time_ms"] for record in event_records]
        compression = self.server.adapter.write_time_compression(
            scope=scope,
            node_hash=node_hash,
            node_path=node_path,
            source_start_ms=min(times),
            source_end_ms=max(times),
            compressed_time_ms=max(times) + 10_000,
            max_source_events=2,
            min_importance=0.9,
        )
        self.assertEqual(compression["record_type"], "context_compression_event")
        self.assertEqual(compression["operator"], "TIME_COMPRESS")
        self.assertEqual(compression["source_event_count"], 2)
        self.assertTrue(compression["truncated_source_events"])
        self.assertEqual(len(compression["source_event_ids"]), 2)

        queried = self.server.adapter.query_time_compressions(
            scope=scope,
            node_hashes={node_hash},
            start_time_ms=min(times),
            end_time_ms=max(times),
        )
        self.assertEqual([item["compression_id_hash"] for item in queried], [compression["compression_id_hash"]])

        pack = self.call_tool(
            "matrixark_retrieve",
            {
                "query": "old GPU approval budget finance",
                "scope": scope,
                "max_context_tokens": 20,
                "ranking": {"weights": {"time": 0.05, "business": 0.25}, "auxiliary_quota": 2},
            },
        )
        compression_refs = [ref for ref in pack["selected_refs"] if ref.get("ref_type") == "compression"]
        self.assertTrue(compression_refs, pack["selected_refs"])
        self.assertEqual(compression_refs[0]["operator"], "TIME_COMPRESS")
        self.assertIn("source_event_ids", compression_refs[0])

        replay = self.call_tool("matrixark_replay", {"context_pack_id": "debug"})
        replay_types = [record.get("record_type") for record in replay["events"]]
        self.assertEqual(replay_types.count("context_event"), 3)
        self.assertIn("context_compression_event", replay_types)

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
