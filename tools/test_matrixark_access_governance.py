#!/usr/bin/env python3

from __future__ import annotations

import json
from http.server import ThreadingHTTPServer
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import Request, urlopen

from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkError, make_matrixark_http_handler


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

    def test_signup_sso_callback_and_redacted_key_usage_inventory(self) -> None:
        server = self.make_server()
        signup = server.call_tool(
            "matrixark_auth_signup",
            {
                "trusted_gateway": True,
                "provider": "google",
                "email": "alice@example.com",
                "external_user_id": "google-sub-123",
                "account_id": "acct_signup",
                "tenant_id": "tenant_signup",
                "user_id": "alice",
                "display_name": "Alice",
                "allowed_user_ids": ["alice"],
                "first_key_scopes": [
                    "admin:account",
                    "admin:user",
                    "admin:api_key",
                    "admin:sso",
                    "admin:audit",
                    "portal:read",
                    "context:ingest",
                    "context:retrieve",
                    "context:replay",
                    "resource:read",
                    "skill:read",
                ],
            },
        )
        self.assertEqual("signed_up", signup["status"])
        self.assertEqual("account_tenant_user_first_scoped_key", signup["signup_contract"])
        self.assertTrue(signup["key_inventory_redacted"])
        first_key = signup["api_key"]
        self.assertTrue(first_key.startswith("mk_live_"))

        server.call_tool(
            "matrixark_retrieve",
            {
                "api_key": first_key,
                "scope": {"account_id": "acct_signup", "tenant_id": "tenant_signup", "user_id": "alice"},
                "query": "what does Alice know?",
            },
        )
        inventory = server.call_tool(
            "matrixark_admin_list_api_keys",
            {"api_key": first_key, "account_id": "acct_signup", "tenant_id": "tenant_signup", "include_revoked": True},
        )
        self.assertEqual(1, inventory["count"])
        row = inventory["api_keys"][0]
        self.assertTrue(row["redacted"])
        self.assertGreaterEqual(row["usage_count"], 1)
        self.assertGreater(row["last_used_at_ms"], 0)
        self.assertIn(row["last_used_action"], {"matrixark_retrieve", "matrixark_admin_list_api_keys"})
        self.assertNotIn("api_key", row)
        self.assertNotIn("api_key_hash", row)

        callback = server.call_tool(
            "matrixark_auth_sso_callback",
            {
                "trusted_gateway": True,
                "id_token_verified": True,
                "provider": "github",
                "email": "alice@users.noreply.github.com",
                "external_user_id": "github-456",
                "account_id": "acct_signup",
                "tenant_id": "tenant_signup",
                "matrixark_user_id": "alice",
            },
        )
        self.assertEqual("sso_callback_mapped", callback["status"])
        self.assertFalse(callback["stored_oauth_tokens"])
        self.assertEqual("trusted_gateway_oidc_oauth_callback", callback["callback_contract"])
        records = server.adapter.read_all()
        record_dump = str(records)
        self.assertNotIn("id_token", record_dump)
        self.assertNotIn("access_token", record_dump)
        self.assertNotIn(first_key, record_dump)
        audits = server.call_tool("matrixark_admin_audit", {"api_key": first_key, "account_id": "acct_signup", "tenant_id": "tenant_signup"})["audit_logs"]
        actions = [row.get("action") for row in audits]
        self.assertIn("auth.signup", actions)
        self.assertIn("auth.sso_callback", actions)


    def test_http_json_portal_facade_calls_live_mcp_admin_routes(self) -> None:
        server = self.make_server()
        handler = make_matrixark_http_handler(
            server,
            Path(__file__).resolve().parents[1] / "tools" / "temporalstore-monitoring-ui",
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{httpd.server_address[1]}"

        def post(path: str, payload: dict) -> dict:
            body = json.dumps(payload).encode("utf-8")
            req = Request(
                base_url + path,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode("utf-8"))

        def get(path: str, api_key: str | None = None) -> dict:
            headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
            req = Request(base_url + path, headers=headers, method="GET")
            with urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode("utf-8"))

        try:
            signup = post(
                "/api/auth/signup",
                {
                    "arguments": {
                        "trusted_gateway": True,
                        "provider": "github",
                        "email": "portal@example.com",
                        "external_user_id": "github-portal-1",
                        "account_id": "acct_http",
                        "tenant_id": "tenant_http",
                        "user_id": "portal_user",
                        "display_name": "Portal User",
                        "allowed_user_ids": ["portal_user"],
                        "first_key_scopes": [
                            "admin:account",
                            "admin:user",
                            "admin:api_key",
                            "admin:sso",
                            "admin:audit",
                            "portal:read",
                            "context:ingest",
                            "context:retrieve",
                            "context:replay",
                            "resource:read",
                            "skill:read",
                        ],
                    }
                },
            )
            self.assertEqual("ok", signup["status"])
            api_key = signup["result"]["api_key"]
            scope = {"account_id": "acct_http", "tenant_id": "tenant_http", "user_id": "portal_user", "session_id": "s_http"}
            ingest = post(
                "/api/tools/call",
                {
                    "tool": "matrixark_ingest",
                    "arguments": {
                        "api_key": api_key,
                        "scope": scope,
                        "messages": [{"role": "user", "content": "HTTP portal user approved the live facade test."}],
                    },
                },
            )
            self.assertEqual("ok", ingest["status"])
            retrieve = post(
                "/api/tools/call",
                {
                    "tool": "matrixark_retrieve",
                    "arguments": {"api_key": api_key, "scope": scope, "query": "what did the portal user approve?"},
                },
            )
            pack_id = retrieve["result"]["context_pack_id"]
            portal = post(
                "/api/management_portal",
                {"arguments": {"api_key": api_key, "scope": scope, "page_size": 5, "include_revoked": True}},
            )
            self.assertEqual("matrixark_management_portal", portal["tool"])
            self.assertEqual("ok", portal["result"]["status"])
            self.assertIn("dashboard", portal["result"])
            self.assertIn("topology", portal["result"])
            metrics = post("/api/backend_metrics", {"arguments": {}})
            self.assertEqual("matrixark_backend_metrics", metrics["tool"])
            self.assertEqual("ok", metrics["status"])
            dashboard = get(
                "/api/ingestion_dashboard?account_id=acct_http&tenant_id=tenant_http&user_id=portal_user&table=messages&page_size=5",
                api_key,
            )
            self.assertEqual("matrixark_ingestion_dashboard", dashboard["tool"])
            self.assertEqual("ok", dashboard["status"])
            self.assertIn("rows", dashboard["result"])
            replay = post("/api/replay", {"arguments": {"api_key": api_key, "context_pack_id": pack_id}})
            self.assertEqual("matrixark_replay", replay["tool"])
            self.assertEqual(pack_id, replay["result"]["context_pack_id"])
            audit = post(
                "/api/audit",
                {"arguments": {"api_key": api_key, "account_id": "acct_http", "tenant_id": "tenant_http"}},
            )
            self.assertEqual("matrixark_admin_audit", audit["tool"])
            self.assertTrue(audit["result"]["audit_logs"])
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
