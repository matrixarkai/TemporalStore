#!/usr/bin/env python3

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkError


class MatrixArkAccessGovernanceTest(unittest.TestCase):
    def make_server(self) -> MatrixArkMcpServer:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return MatrixArkMcpServer(MatrixArkLocalAdapter(Path(tmp.name) / "events.jsonl"), line_json=True, access_mode="dev")

    def test_api_key_hash_only_and_role_limited_viewer(self) -> None:
        server = self.make_server()
        admin_key = server.call_tool(
            "matrixark_admin_create_api_key",
            {
                "scope": {"account_id": "acct_gov", "tenant_id": "tenant_gov"},
                "account_id": "acct_gov",
                "tenant_id": "tenant_gov",
                "role": "owner",
                "scopes": ["admin:account", "admin:user", "admin:api_key", "admin:audit", "portal:read"],
            },
        )["api_key"]
        records = server.adapter.read_all()
        key_records = [r for r in records if r.get("record_type") == "matrixark_api_key"]
        self.assertTrue(key_records)
        self.assertIn("api_key_hash", key_records[-1])
        self.assertNotIn("api_key", key_records[-1])
        self.assertNotIn(admin_key, str(records))

        viewer = server.call_tool(
            "matrixark_admin_create_api_key",
            {
                "api_key": admin_key,
                "account_id": "acct_gov",
                "tenant_id": "tenant_gov",
                "role": "viewer",
                "scopes": ["portal:read", "context:retrieve", "context:replay", "resource:read", "skill:read"],
                "allowed_user_ids": ["alice"],
            },
        )["api_key"]
        portal = server.call_tool(
            "matrixark_management_portal",
            {"api_key": viewer, "scope": {"account_id": "acct_gov", "tenant_id": "tenant_gov", "user_id": "alice"}},
        )
        self.assertEqual("ok", portal["status"])
        self.assertEqual([], portal["api_keys"])
        with self.assertRaises(MatrixArkError):
            server.call_tool(
                "matrixark_admin_list_api_keys",
                {"api_key": viewer, "account_id": "acct_gov", "tenant_id": "tenant_gov"},
            )

    def test_denied_and_portal_and_replay_are_audited(self) -> None:
        server = self.make_server()
        admin_key = server.call_tool(
            "matrixark_admin_create_api_key",
            {
                "scope": {"account_id": "acct_audit", "tenant_id": "tenant_audit"},
                "account_id": "acct_audit",
                "tenant_id": "tenant_audit",
                "role": "owner",
                "scopes": ["admin:account", "admin:user", "admin:api_key", "admin:audit", "portal:read", "context:ingest", "context:retrieve", "context:replay"],
            },
        )["api_key"]
        with self.assertRaises(MatrixArkError):
            server.call_tool(
                "matrixark_management_portal",
                {"api_key": "mk_bad", "scope": {"account_id": "acct_audit", "tenant_id": "tenant_audit"}},
            )
        server.call_tool(
            "matrixark_ingest",
            {
                "api_key": admin_key,
                "scope": {"account_id": "acct_audit", "tenant_id": "tenant_audit", "user_id": "alice", "session_id": "s1"},
                "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
            },
        )
        pack = server.call_tool(
            "matrixark_retrieve",
            {
                "api_key": admin_key,
                "scope": {"account_id": "acct_audit", "tenant_id": "tenant_audit", "user_id": "alice", "session_id": "s1"},
                "query": "what did Alice approve?",
            },
        )
        server.call_tool("matrixark_replay", {"api_key": admin_key, "context_pack_id": pack["context_pack_id"]})
        portal = server.call_tool(
            "matrixark_management_portal",
            {"api_key": admin_key, "scope": {"account_id": "acct_audit", "tenant_id": "tenant_audit", "user_id": "alice"}},
        )
        self.assertIn("users", portal["dashboard"])
        self.assertIn("api_keys", portal["dashboard"])
        self.assertIn("audit_logs", portal["dashboard"])
        self.assertIn("context_summaries", portal["topology"]["records"])
        self.assertIn("context_embeddings", portal["topology"]["records"])
        self.assertIn("dirty_summaries", portal["topology"]["records"])
        self.assertEqual(pack["context_pack_id"], portal["context_pack_debugger"]["context_pack_id"])
        self.assertIn("selected_refs", portal["context_pack_debugger"])
        self.assertIn("dropped_refs", portal["context_pack_debugger"])
        self.assertIn("replay_link", portal["context_pack_debugger"])
        self.assertIn("used_local_context_tokens", portal["context_pack_debugger"])
        self.assertIn("used_remote_context_tokens", portal["context_pack_debugger"])
        audits = server.call_tool("matrixark_admin_audit", {"api_key": admin_key, "account_id": "acct_audit", "tenant_id": "tenant_audit"})["audit_logs"]
        actions = [row.get("action") for row in audits]
        self.assertIn("matrixark_management_portal", actions)
        self.assertIn("context.replay", actions)
        self.assertIn("admin.management_portal", actions)
        self.assertTrue(any(row.get("status") == "denied" and row.get("api_key_hash_prefix") for row in audits))
        self.assertTrue(all("api_key" not in row for row in audits))


if __name__ == "__main__":
    unittest.main()
