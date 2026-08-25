#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI

from __future__ import annotations

import json
import os
from http.server import ThreadingHTTPServer
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkError, make_matrixark_http_handler
from matrixark_access import MatrixArkSqlMetadataStore


class MatrixArkAccessGovernanceTest(unittest.TestCase):
    def make_server(self) -> MatrixArkMcpServer:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        # Auditing is off by default; these tests verify what auditing records when it is on.
        previous = os.environ.get("MATRIXARK_AUDIT_MODE")
        os.environ["MATRIXARK_AUDIT_MODE"] = "async"
        def restore() -> None:
            if previous is None:
                os.environ.pop("MATRIXARK_AUDIT_MODE", None)
            else:
                os.environ["MATRIXARK_AUDIT_MODE"] = previous
        self.addCleanup(restore)
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

    def test_sql_metadata_store_populates_normalized_portal_tables(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = MatrixArkSqlMetadataStore(backend="sqlite", dsn=str(Path(tmp.name) / "metadata.sqlite3"), auto_init=True)
        store.append({"record_type": "matrixark_account", "account_id": "acct_sql", "account_name": "SQL Account", "status": "active", "created_at_ms": 1})
        store.append({"record_type": "matrixark_tenant", "account_id": "acct_sql", "tenant_id": "tenant_sql", "tenant_name": "SQL Tenant", "status": "active", "tenant_hash": 7, "created_at_ms": 2})
        store.append({"record_type": "matrixark_user", "account_id": "acct_sql", "tenant_id": "tenant_sql", "user_id": "alice", "display_name": "Alice", "status": "active", "created_at_ms": 3})
        store.append({"record_type": "matrixark_api_key", "api_key_id": "key_sql", "account_id": "acct_sql", "tenant_id": "tenant_sql", "role": "owner", "status": "active", "api_key_hash": "abcdef1234567890", "created_at_ms": 4})
        store.append({"record_type": "matrixark_api_key_usage", "usage_id_hash": 5, "api_key_id": "key_sql", "account_id": "acct_sql", "tenant_id": "tenant_sql", "user_id": "alice", "action": "matrixark_retrieve", "used_at_ms": 5})
        store.append({"record_type": "matrixark_sso_user_mapping", "provider": "github", "external_user_id": "gh_1", "account_id": "acct_sql", "tenant_id": "tenant_sql", "matrixark_user_id": "alice", "email": "alice@example.com", "created_at_ms": 6})
        store.append({"record_type": "matrixark_audit_log", "audit_id_hash": 8, "account_id": "acct_sql", "tenant_id": "tenant_sql", "user_id": "alice", "api_key_id": "key_sql", "action": "admin.test", "status": "ok", "created_at_ms": 7})

        counts = store.normalized_counts()
        self.assertEqual(7, counts[store.TABLE])
        self.assertEqual(1, counts[store.ACCOUNT_TABLE])
        self.assertEqual(1, counts[store.TENANT_TABLE])
        self.assertEqual(1, counts[store.USER_TABLE])
        self.assertEqual(1, counts[store.API_KEY_TABLE])
        self.assertEqual(1, counts[store.API_KEY_USAGE_TABLE])
        self.assertEqual(1, counts[store.SSO_TABLE])
        self.assertEqual(1, counts[store.AUDIT_TABLE])
        ready = store.check_ready()
        self.assertIn(store.USER_TABLE, ready["normalized_tables"])
        self.assertEqual("ok", ready["status"])

    def test_production_deployment_requires_live_sql_metadata(self) -> None:
        saved = {
            key: os.environ.get(key)
            for key in [
                "MATRIXARK_METADATA_BACKEND",
                "MATRIXARK_METADATA_DSN",
                "MATRIXARK_METADATA_AUTO_INIT",
                "MATRIXARK_REQUIRE_SQL_METADATA",
                "MATRIXARK_METADATA_REQUIRE_SQL",
                "MATRIXARK_METADATA_REQUIRE_LIVE",
            ]
        }
        try:
            os.environ["MATRIXARK_REQUIRE_SQL_METADATA"] = "1"
            os.environ["MATRIXARK_METADATA_BACKEND"] = "record_log"
            with self.assertRaises(MatrixArkError):
                self.make_server()

            os.environ["MATRIXARK_METADATA_BACKEND"] = "sqlite"
            os.environ["MATRIXARK_METADATA_DSN"] = ":memory:"
            with self.assertRaises(MatrixArkError):
                self.make_server()
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_matrixkv_sql_backend_reports_mysql_compatible_metadata(self) -> None:
        store = MatrixArkSqlMetadataStore(
            backend="matrixkv_sql",
            dsn="matrixkv+mysql://matrixark:password@matrixkv-sql:3306/matrixark",
            auto_init=False,
        )
        info = store.backend_info()
        self.assertEqual("matrixkv_sql", info["backend"])
        self.assertEqual("mysql", info["sql_compatible_with"])
        self.assertEqual("matrixkv", info["product_family"])
        self.assertTrue(info["dsn_configured"])

    def test_local_api_key_application_gets_resource_skill_management_scopes(self) -> None:
        server = self.make_server()
        applied = server.call_tool(
            "matrixark_admin_apply_api_key",
            {
                "account_id": "acct_local",
                "agent_name": "codex",
                "user_id": "local_user",
                "scope": {"session_id": "local_session"},
            },
        )
        scopes = set(applied["scopes"])
        self.assertIn("resource:ingest", scopes)
        self.assertIn("resource:manage", scopes)
        self.assertIn("skill:manage", scopes)
        self.assertEqual("tenant_codex", applied["tenant_id"])
        self.assertEqual("local_user", applied["local_scope"]["user_id"])
        self.assertTrue(applied["api_key"].startswith("mk_local_"))

    def test_scoped_read_tools_are_checked_before_output(self) -> None:
        server = self.make_server()
        session_key = server.call_tool(
            "matrixark_admin_create_api_key",
            {
                "scope": {"account_id": "acct_read", "tenant_id": "tenant_read"},
                "account_id": "acct_read",
                "tenant_id": "tenant_read",
                "role": "viewer",
                "scopes": [
                    "portal:read",
                    "context:retrieve",
                    "context:replay",
                    "resource:read",
                    "skill:read",
                ],
                "allowed_session_ids": ["session_allowed"],
            },
        )["api_key"]
        allowed_scope = {"account_id": "acct_read", "tenant_id": "tenant_read", "session_id": "session_allowed"}
        self.assertEqual(
            "ok",
            server.call_tool("matrixark_ingestion_dashboard", {"api_key": session_key, "scope": allowed_scope})["status"],
        )
        with self.assertRaises(MatrixArkError):
            server.call_tool(
                "matrixark_retrieve",
                {
                    "api_key": session_key,
                    "scope": {"account_id": "acct_read", "tenant_id": "tenant_read", "session_id": "session_denied"},
                    "query": "should not leave the server",
                },
            )
        with self.assertRaises(MatrixArkError):
            server.call_tool(
                "matrixark_ingestion_dashboard",
                {
                    "api_key": session_key,
                    "scope": {"account_id": "acct_read", "tenant_id": "tenant_read", "session_id": "session_denied"},
                },
            )

    def test_disabled_user_and_tenant_block_portal_and_context_access(self) -> None:
        server = self.make_server()
        admin_key = server.call_tool(
            "matrixark_admin_create_api_key",
            {
                "scope": {"account_id": "acct_disable", "tenant_id": "tenant_disable"},
                "account_id": "acct_disable",
                "tenant_id": "tenant_disable",
                "role": "owner",
                "scopes": ["admin:account", "admin:user", "admin:api_key", "admin:audit", "portal:read", "context:retrieve"],
            },
        )["api_key"]
        server.call_tool(
            "matrixark_admin_create_account",
            {"api_key": admin_key, "account_id": "acct_disable", "tenant_id": "tenant_disable"},
        )
        server.call_tool(
            "matrixark_admin_create_user",
            {"api_key": admin_key, "account_id": "acct_disable", "tenant_id": "tenant_disable", "user_id": "alice"},
        )
        viewer_key = server.call_tool(
            "matrixark_admin_create_api_key",
            {
                "api_key": admin_key,
                "account_id": "acct_disable",
                "tenant_id": "tenant_disable",
                "role": "viewer",
                "scopes": ["portal:read", "context:retrieve", "context:replay"],
                "allowed_user_ids": ["alice"],
            },
        )["api_key"]
        scope = {"account_id": "acct_disable", "tenant_id": "tenant_disable", "user_id": "alice"}
        self.assertEqual("ok", server.call_tool("matrixark_management_portal", {"api_key": viewer_key, "scope": scope})["status"])

        server.call_tool(
            "matrixark_admin_update_user",
            {"api_key": admin_key, "account_id": "acct_disable", "tenant_id": "tenant_disable", "user_id": "alice", "status": "disabled"},
        )
        with self.assertRaises(MatrixArkError):
            server.call_tool("matrixark_management_portal", {"api_key": viewer_key, "scope": scope})

        server.call_tool(
            "matrixark_admin_update_user",
            {"api_key": admin_key, "account_id": "acct_disable", "tenant_id": "tenant_disable", "user_id": "alice", "status": "active"},
        )
        server.call_tool(
            "matrixark_admin_update_account",
            {"api_key": admin_key, "account_id": "acct_disable", "tenant_id": "tenant_disable", "tenant_status": "disabled"},
        )
        with self.assertRaises(MatrixArkError):
            server.call_tool("matrixark_retrieve", {"api_key": viewer_key, "scope": scope, "query": "what is visible?"})
        with self.assertRaises(MatrixArkError):
            server.call_tool("matrixark_management_portal", {"api_key": viewer_key, "scope": scope})

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
                "audit_mode": "full",
            },
        )
        pack_id = pack.get("context_pack_id") or pack.get("pack_id")
        self.assertTrue(pack_id)
        server.call_tool("matrixark_replay", {"api_key": admin_key, "context_pack_id": pack_id, "enable_replay": True, "audit_mode": "full"})
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
        self.assertEqual(pack_id, portal["context_pack_debugger"]["context_pack_id"])
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
                "access_token": "secret-access-token-value",
                "refresh_token": "secret-refresh-token-value",
                "id_token": "secret-id-token-value",
            },
        )
        self.assertEqual("sso_callback_mapped", callback["status"])
        self.assertFalse(callback["stored_oauth_tokens"])
        self.assertEqual("trusted_gateway_oidc_oauth_callback", callback["callback_contract"])
        records = server.adapter.read_all()
        record_dump = str(records)
        self.assertNotIn("id_token", record_dump)
        self.assertNotIn("access_token", record_dump)
        self.assertNotIn("secret-access-token-value", record_dump)
        self.assertNotIn("secret-refresh-token-value", record_dump)
        self.assertNotIn("secret-id-token-value", record_dump)
        self.assertNotIn("secret-access-token-value", str(callback))
        self.assertNotIn("secret-refresh-token-value", str(callback))
        self.assertNotIn("secret-id-token-value", str(callback))
        self.assertNotIn(first_key, record_dump)
        audits = server.call_tool("matrixark_admin_audit", {"api_key": first_key, "account_id": "acct_signup", "tenant_id": "tenant_signup"})["audit_logs"]
        actions = [row.get("action") for row in audits]
        self.assertIn("auth.signup", actions)
        self.assertIn("auth.sso_callback", actions)

    def test_email_password_signup_login_and_never_stores_plaintext(self) -> None:
        server = self.make_server()
        secret_password = "correct-horse-battery-staple-42"
        signup = server.call_tool(
            "matrixark_auth_signup",
            {
                "trusted_gateway": True,
                "provider": "password",
                "email": "bob@gmail.com",
                "password": secret_password,
                "account_id": "acct_pw",
                "tenant_id": "tenant_pw",
                "user_id": "bob",
                "display_name": "Bob",
                "allowed_user_ids": ["bob"],
                "first_key_scopes": ["portal:read", "context:retrieve"],
            },
        )
        self.assertEqual("signed_up", signup["status"])
        self.assertTrue(signup["password_login_enabled"])

        # Plaintext password must never be persisted or echoed back.
        record_dump = str(server.adapter.read_all())
        self.assertNotIn(secret_password, record_dump)
        self.assertNotIn(secret_password, str(signup))
        credential_records = [r for r in server.adapter.read_all() if r.get("record_type") == "matrixark_user_credential"]
        self.assertEqual(1, len(credential_records))
        self.assertEqual("pbkdf2_sha256", credential_records[0]["algo"])
        self.assertNotIn("password", credential_records[0])

        # Correct email + password logs in and resolves the MatrixArk user.
        login = server.call_tool(
            "matrixark_auth_login",
            {"email": "bob@gmail.com", "password": secret_password, "account_id": "acct_pw", "tenant_id": "tenant_pw"},
        )
        self.assertEqual("logged_in", login["status"])
        self.assertEqual("bob", login["matrixark_user_id"])
        self.assertEqual("password", login["auth_method"])
        self.assertIn("apply_api_key", login["next_actions"])

        # Wrong password and unknown email both fail with a single generic error.
        with self.assertRaises(MatrixArkError):
            server.call_tool(
                "matrixark_auth_login",
                {"email": "bob@gmail.com", "password": "wrong-password", "account_id": "acct_pw", "tenant_id": "tenant_pw"},
            )
        with self.assertRaises(MatrixArkError):
            server.call_tool(
                "matrixark_auth_login",
                {"email": "nobody@gmail.com", "password": secret_password, "account_id": "acct_pw", "tenant_id": "tenant_pw"},
            )

        admin_key = server.call_tool(
            "matrixark_admin_create_api_key",
            {
                "account_id": "acct_pw",
                "tenant_id": "tenant_pw",
                "role": "owner",
                "scopes": ["admin:audit", "portal:read"],
            },
        )["api_key"]
        audit_actions = [
            row.get("action")
            for row in server.call_tool("matrixark_admin_audit", {"api_key": admin_key, "account_id": "acct_pw", "tenant_id": "tenant_pw"})["audit_logs"]
        ]
        self.assertIn("auth.login", audit_actions)
        self.assertNotIn(secret_password, str(server.adapter.read_all()))


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
            tools = get("/api/tools", api_key)
            self.assertIn("matrixark_agent_hook", tools["tools"])
            dashboard = get(
                "/api/ingestion_dashboard?account_id=acct_http&tenant_id=tenant_http&user_id=portal_user&table=messages&page_size=5",
                api_key,
            )
            self.assertEqual("matrixark_ingestion_dashboard", dashboard["tool"])
            self.assertEqual("ok", dashboard["status"])
            self.assertIn("rows", dashboard["result"])
            replay = post("/api/replay", {"arguments": {"api_key": api_key, "context_pack_id": pack_id, "enable_replay": True}})
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

    def test_http_agent_hook_accepts_remote_normalized_lifecycle_events(self) -> None:
        server = self.make_server()
        handler = make_matrixark_http_handler(
            server,
            Path(__file__).resolve().parents[1] / "tools" / "temporalstore-monitoring-ui",
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{httpd.server_address[1]}"

        def post(payload: dict) -> dict:
            body = json.dumps(payload).encode("utf-8")
            req = Request(
                base_url + "/api/agent/hook",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode("utf-8"))

        scope = {
            "account_id": "acct_http_hook",
            "tenant_id": "tenant_http_hook",
            "user_id": "hook_user",
            "session_id": "codex:remote-thread-1",
        }
        try:
            prompt = post(
                {
                    "scope": scope,
                    "session_buffer_threshold": 2,
                    "normalized_event": {
                        "agent": "codex",
                        "event": "UserPromptSubmit",
                        "hook_type": "before_llm",
                        "lifecycle_stage": "before_llm_retrieve",
                        "should_retrieve": True,
                        "should_commit": False,
                        "extraction_phase": "provisional",
                        "final_session_boundary": False,
                        "conversation_id": "remote-thread-1",
                        "session_id": "codex:remote-thread-1",
                        "session_id_source": "payload.conversation_id",
                        "role": "user",
                        "text": "Remote hook should retrieve memory and keep lifecycle metadata.",
                        "timestamp_ms": 123,
                    },
                    "raw_payload": {"conversation_id": "remote-thread-1"},
                }
            )
            self.assertEqual("ok", prompt["status"])
            prompt_result = prompt["result"]
            self.assertEqual("ok", prompt_result["status"])
            self.assertEqual("accepted", prompt_result["ingested"]["status"])
            self.assertTrue(prompt_result["retrieved"]["context_pack_id"])

            assistant = post(
                {
                    "scope": scope,
                    "session_buffer_threshold": 2,
                    "normalized_event": {
                        "agent": "codex",
                        "event": "AssistantResponse",
                        "hook_type": "after_llm",
                        "lifecycle_stage": "after_llm_ingest",
                        "should_retrieve": False,
                        "should_commit": False,
                        "extraction_phase": "provisional",
                        "final_session_boundary": False,
                        "conversation_id": "remote-thread-1",
                        "session_id": "codex:remote-thread-1",
                        "session_id_source": "payload.conversation_id",
                        "role": "assistant",
                        "text": "Decision: remote assistant response should threshold extract without retrieval.",
                        "timestamp_ms": 234,
                    },
                    "raw_payload": {"conversation_id": "remote-thread-1"},
                }
            )
            self.assertEqual("ok", assistant["status"])
            assistant_result = assistant["result"]
            self.assertEqual("accepted", assistant_result["ingested"]["status"])
            self.assertEqual({}, assistant_result["retrieved"])
            self.assertEqual(
                "committed",
                assistant_result["ingested"]["auto_batch_extract_result"]["status"],
            )
            self.assertEqual(
                "threshold",
                assistant_result["ingested"]["auto_batch_extract_result"]["trigger_policy"],
            )

            string_false = post(
                {
                    "scope": {**scope, "session_id": "codex:remote-thread-string-false"},
                    "normalized_event": {
                        "agent": "codex",
                        "event": "AssistantResponse",
                        "hook_type": "after_llm",
                        "lifecycle_stage": "after_llm_ingest",
                        "should_retrieve": "false",
                        "should_commit": "false",
                        "auto_batch_extract": "false",
                        "final_session_boundary": "false",
                        "conversation_id": "remote-thread-string-false",
                        "session_id": "codex:remote-thread-string-false",
                        "session_id_source": "payload.conversation_id",
                        "role": "assistant",
                        "text": "String false lifecycle flags must not retrieve or commit.",
                        "timestamp_ms": 345,
                    },
                    "raw_payload": {"conversation_id": "remote-thread-string-false"},
                }
            )
            self.assertEqual("ok", string_false["status"])
            string_false_result = string_false["result"]
            self.assertEqual("accepted", string_false_result["ingested"]["status"])
            self.assertFalse(string_false_result["ingested"]["session_buffer"]["auto_batch_extract"])
            self.assertEqual({}, string_false_result["retrieved"])
            self.assertEqual({}, string_false_result["committed"])

            idle = post(
                {
                    "scope": {**scope, "session_id": "codex:remote-thread-string-false"},
                    "idle_commit_timeout_ms": 0,
                    "normalized_event": {
                        "agent": "codex",
                        "event": "IdleTimeout",
                        "hook_type": "session_commit",
                        "lifecycle_stage": "idle_timeout_commit",
                        "should_retrieve": False,
                        "should_commit": True,
                        "conversation_id": "remote-thread-string-false",
                        "session_id": "codex:remote-thread-string-false",
                        "session_id_source": "payload.conversation_id",
                        "role": "system",
                        "timestamp_ms": 346,
                    },
                    "raw_payload": {"conversation_id": "remote-thread-string-false"},
                }
            )
            self.assertEqual("ok", idle["status"])
            idle_commit = idle["result"]["committed"]
            self.assertEqual("committed", idle_commit["status"])
            self.assertEqual("idle_timeout", idle_commit["commit_reason"])
            self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
            self.assertEqual("provisional", idle_commit["extraction_phase"])
            self.assertFalse(idle_commit["final_session_boundary"])
            self.assertEqual(1, idle_commit["committed_event_count"])

            stop = post(
                {
                    "scope": scope,
                    "normalized_event": {
                        "agent": "codex",
                        "event": "Stop",
                        "hook_type": "session_commit",
                        "lifecycle_stage": "session_boundary_commit",
                        "should_retrieve": False,
                        "should_commit": True,
                        "extraction_phase": "final",
                        "final_session_boundary": True,
                        "conversation_id": "remote-thread-1",
                        "session_id": "codex:remote-thread-1",
                        "session_id_source": "payload.conversation_id",
                        "role": "assistant",
                        "text": "Final answer: remote hook commit should extract session memory.",
                        "timestamp_ms": 456,
                    },
                    "raw_payload": {"conversation_id": "remote-thread-1"},
                }
            )
            self.assertEqual("ok", stop["status"])
            stop_result = stop["result"]
            self.assertEqual("accepted", stop_result["ingested"]["status"])
            self.assertEqual("committed", stop_result["committed"]["status"])
            self.assertEqual("final", stop_result["committed"]["extraction_phase"])
            self.assertTrue(stop_result["committed"]["final_session_boundary"])

            records = server.adapter.read_all()
            prompt_pack_rows = [
                record
                for record in records
                if record.get("record_type") in {"context_pack_audit", "context_pack_telemetry"}
                and record.get("context_pack_id") == prompt_result["retrieved"]["context_pack_id"]
            ]
            self.assertTrue(prompt_pack_rows, records)
            prompt_pack_row = prompt_pack_rows[-1]
            self.assertEqual(
                "payload.conversation_id",
                prompt_pack_row["retrieval_request_metadata"]["session_id_source"],
            )
            self.assertEqual(
                "remote_agent_hook",
                prompt_pack_row["retrieval_request_metadata"]["retrieval_source"],
            )
            prompt_telemetry_rows = [
                record
                for record in prompt_pack_rows
                if record.get("record_type") == "context_pack_telemetry"
            ]
            self.assertTrue(prompt_telemetry_rows)
            prompt_session_identity = prompt_telemetry_rows[-1]["session_identity"]
            self.assertEqual("payload.conversation_id", prompt_session_identity["session_id_source"])
            self.assertTrue(prompt_session_identity["strong_session_identity"])
            self.assertFalse(prompt_session_identity["fallback_session_identity"])
            self.assertNotIn("session_identity_fallback:payload.conversation_id", prompt_telemetry_rows[-1]["quality_warnings"])
            dashboard = server.adapter.ingestion_dashboard({"scope": scope, "table": "context_packs", "page_size": 10})
            dashboard_pack_rows = [
                row
                for row in dashboard["rows"]
                if row.get("context_pack_id") == prompt_result["retrieved"]["context_pack_id"]
                and row.get("row_type") == "context_pack_telemetry"
            ]
            self.assertTrue(dashboard_pack_rows)
            self.assertTrue(dashboard_pack_rows[-1]["session_identity"]["strong_session_identity"])
            commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
            self.assertTrue(commits)
            self.assertTrue(any(record.get("final_session_boundary") is True for record in commits))
            self.assertTrue(any(record.get("trigger_policy") == "threshold" for record in commits))
            buffer_events = [record for record in records if record.get("record_type") == "session_buffer_event"]
            self.assertTrue(
                any(
                    record.get("envelope", {}).get("metadata", {}).get("lifecycle_stage") == "before_llm_retrieve"
                    for record in buffer_events
                )
            )
            self.assertTrue(any("session_commit" in record.get("source_hook_types", []) for record in commits))
            self.assertTrue(any("AssistantResponse" in record.get("source_codex_events", []) for record in commits))
            self.assertTrue(any("Stop" in record.get("source_codex_events", []) for record in commits))
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=5)

    def test_http_cloud_mode_requires_auth_or_trusted_gateway_and_restricts_cors(self) -> None:
        saved = {key: os.environ.get(key) for key in ["MATRIXARK_HTTP_MODE", "MATRIXARK_HTTP_ALLOWED_ORIGIN"]}
        os.environ["MATRIXARK_HTTP_MODE"] = "cloud"
        os.environ["MATRIXARK_HTTP_ALLOWED_ORIGIN"] = "https://console.matrixark.test"
        try:
            server = self.make_server()
            handler = make_matrixark_http_handler(
                server,
                Path(__file__).resolve().parents[1] / "tools" / "temporalstore-monitoring-ui",
            )
            httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=httpd.serve_forever, daemon=True)
            thread.start()
            base_url = f"http://127.0.0.1:{httpd.server_address[1]}"

            def post(path: str, payload: dict, headers: dict | None = None) -> tuple[int, dict, str]:
                body = json.dumps(payload).encode("utf-8")
                req = Request(
                    base_url + path,
                    data=body,
                    headers={"Content-Type": "application/json", **(headers or {})},
                    method="POST",
                )
                try:
                    with urlopen(req, timeout=10) as response:
                        return response.status, json.loads(response.read().decode("utf-8")), response.headers.get("Access-Control-Allow-Origin", "")
                except HTTPError as exc:
                    return exc.code, json.loads(exc.read().decode("utf-8")), exc.headers.get("Access-Control-Allow-Origin", "")

            def get(path: str, headers: dict | None = None) -> tuple[int, dict, str]:
                req = Request(base_url + path, headers=headers or {}, method="GET")
                try:
                    with urlopen(req, timeout=10) as response:
                        return response.status, json.loads(response.read().decode("utf-8")), response.headers.get("Access-Control-Allow-Origin", "")
                except HTTPError as exc:
                    return exc.code, json.loads(exc.read().decode("utf-8")), exc.headers.get("Access-Control-Allow-Origin", "")

            try:
                status, body, origin = get("/api/tools")
                self.assertEqual(401, status)
                self.assertEqual("https://console.matrixark.test", origin)

                status, body, origin = post("/api/management_portal", {"arguments": {}})
                self.assertEqual(401, status)
                self.assertEqual("https://console.matrixark.test", origin)
                self.assertIn("requires bearer API key", body["error"])

                status, body, origin = post(
                    "/api/agent/hook",
                    {
                        "normalized_event": {
                            "agent": "codex",
                            "event": "UserPromptSubmit",
                            "hook_type": "before_llm",
                            "lifecycle_stage": "before_llm_retrieve",
                            "should_retrieve": True,
                            "role": "user",
                            "text": "Cloud agent hook must require auth.",
                        }
                    },
                )
                self.assertEqual(401, status)
                self.assertEqual("https://console.matrixark.test", origin)
                self.assertIn("requires bearer API key", body["error"])

                status, body, origin = post(
                    "/api/auth/sso_callback",
                    {
                        "arguments": {
                            "trusted_gateway": True,
                            "provider": "github",
                            "external_user_id": "gh-cloud",
                            "account_id": "acct_cloud",
                            "tenant_id": "tenant_cloud",
                            "matrixark_user_id": "alice",
                        }
                    },
                    headers={"X-MatrixArk-Trusted-Gateway": "true"},
                )
                self.assertEqual(200, status)
                self.assertEqual("ok", body["status"])
                self.assertEqual("https://console.matrixark.test", origin)
            finally:
                httpd.shutdown()
                httpd.server_close()
                thread.join(timeout=5)
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
