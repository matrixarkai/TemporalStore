#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Admin, auth, and portal MCP tool schemas for MatrixArk."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_schema_common import SCOPE_SCHEMA
    from tools.matrixark_mcp_auth_schemas import ADMIN_ACCOUNT_PROPERTIES, API_KEY_SCHEMA
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_schema_common import SCOPE_SCHEMA
    from matrixark_mcp_auth_schemas import ADMIN_ACCOUNT_PROPERTIES, API_KEY_SCHEMA


ADMIN_TOOLS: list[Json] = [
    {
        "name": "matrixark_auth_signup",
        "description": "Production signup: create account, tenant, user, first scoped API key, and audit record. In enforced mode call from a trusted gateway or an admin API key.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "trusted_gateway": {"type": "boolean", "default": False},
                "provider": {"type": "string", "description": "local, google, github, okta, azure_ad, or oidc."},
                "external_user_id": {"type": "string"},
                "email": {"type": "string"},
                "account_id": {"type": "string"},
                "account_name": {"type": "string"},
                "tenant_id": {"type": "string"},
                "tenant_name": {"type": "string"},
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "first_key_scopes": {"type": "array", "items": {"type": "string"}},
                "first_key_role": {"type": "string", "default": "owner"},
                "key_display_name": {"type": "string"},
                "allowed_user_ids": {"type": "array", "items": {"type": "string"}},
                "allowed_session_ids": {"type": "array", "items": {"type": "string"}},
                "allow_all_users": {"type": "boolean", "default": False},
                "expires_at_ms": {"type": "integer"},
                "password": {"type": "string", "description": "Optional password for email/password login; stored only as a salted PBKDF2-SHA256 hash. Omit for SSO-only users."},
                "key_prefix": {"type": "string", "default": "mk_live"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_auth_sso_callback",
        "description": "Trusted OAuth/OIDC gateway callback for Google/Gmail, GitHub, Okta, and Azure AD. MatrixArk stores mapped identity metadata only, never raw OAuth tokens.",
        "inputSchema": {
            "type": "object",
            "required": ["provider"],
            "properties": {
                "provider": {"type": "string", "enum": ["google", "gmail", "github", "okta", "azure_ad", "azuread", "oidc"]},
                "id_token": {"type": "string", "description": "Google OIDC ID token. When provided for provider google/gmail without trusted_gateway, MatrixArk verifies RS256 + claims against Google's JWKS in process and never stores it."},
                "google_client_id": {"type": "string", "description": "Google OAuth client id used as the expected token audience; falls back to the MATRIXARK_GOOGLE_CLIENT_ID environment variable."},
                "external_user_id": {"type": "string", "description": "Stable IdP subject, such as OIDC sub or GitHub id."},
                "email": {"type": "string"},
                "matrixark_user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "id_token_verified": {"type": "boolean", "default": False},
                "trusted_gateway": {"type": "boolean", "default": False},
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_auth_sso_login",
        "description": "Map verified Google/Okta/Azure AD/OIDC login claims into a MatrixArk user scope. In enforced mode the gateway must pass id_token_verified=true or trusted_gateway=true.",
        "inputSchema": {
            "type": "object",
            "required": ["provider"],
            "properties": {
                "provider": {"type": "string", "description": "google, okta, azure_ad, github, or another trusted IdP."},
                "external_user_id": {"type": "string", "description": "Stable IdP subject. For Google this is the OIDC sub claim when available."},
                "email": {"type": "string", "description": "Email claim, e.g. a Gmail or Google Workspace address."},
                "matrixark_user_id": {"type": "string", "description": "Optional explicit MatrixArk user id; otherwise derived from provider subject."},
                "display_name": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "id_token_verified": {"type": "boolean", "default": False, "description": "Set by the trusted portal/gateway after OAuth/OIDC token validation."},
                "trusted_gateway": {"type": "boolean", "default": False},
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_auth_login",
        "description": "Email/password login for users who registered without SSO. Verifies a salted PBKDF2-SHA256 hash; MatrixArk never stores or logs the plaintext password. Gmail/Google and GitHub users should use matrixark_auth_sso_callback instead.",
        "inputSchema": {
            "type": "object",
            "required": ["password"],
            "properties": {
                "email": {"type": "string", "description": "Registered email, e.g. a Gmail address used at signup."},
                "password": {"type": "string", "description": "Plaintext password, verified against a salted PBKDF2-SHA256 hash and never stored."},
                "user_id": {"type": "string", "description": "Optional explicit MatrixArk user id; otherwise resolved from the email."},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "provider": {"type": "string", "default": "password"},
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_management_portal",
        "description": "Return one backend portal payload for registration, API-key management, ingestion history, topology, metrics, and audit.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "page_size": {"type": "integer", "default": 10, "minimum": 1, "maximum": 50},
                "page_token": {"type": ["integer", "string"], "default": 0, "description": "Offset for live paged portal tables."},
                "include_revoked": {"type": "boolean", "default": False},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_account",
        "description": "Create a MatrixArk account and default tenant.",
        "inputSchema": {
            "type": "object",
            "properties": ADMIN_ACCOUNT_PROPERTIES,
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_update_account",
        "description": "Update account or tenant metadata and active/disabled status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "account_status": {"type": "string", "enum": ["active", "disabled"]},
                "tenant_status": {"type": "string", "enum": ["active", "disabled"]},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_accounts",
        "description": "List account and tenant metadata visible to the caller.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_user",
        "description": "Create or register a MatrixArk user under an account/tenant.",
        "inputSchema": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "status": {"type": "string", "enum": ["active", "disabled"], "default": "active"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_update_user",
        "description": "Update, enable, or disable a MatrixArk user under an account/tenant.",
        "inputSchema": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "status": {"type": "string", "enum": ["active", "disabled"]},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_users",
        "description": "List MatrixArk users for an account/tenant without exposing context data.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "status": {"type": "string", "enum": ["active", "disabled"]},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_api_key",
        "description": "Create a MatrixArk API key for an account/tenant. The raw key is returned once.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowed scopes such as context:ingest, context:retrieve, admin:api_key.",
                },
                "role": {"type": "string", "default": "service"},
                "display_name": {"type": "string"},
                "allowed_user_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional user allow-list. Empty means any user in the key tenant.",
                },
                "allowed_session_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional session allow-list. Empty means any session in the key tenant.",
                },
                "expires_at_ms": {
                    "type": "integer",
                    "description": "Optional future unix timestamp in milliseconds when this key expires.",
                },
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_apply_api_key",
        "description": "One-call local agent onboarding: create or reuse account, agent-derived tenant, local user, and return a scoped MatrixArk API key.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "account_id": {"type": "string", "description": "Optional account/customer id. Defaults to acct_local in local mode."},
                "tenant_id": {"type": "string", "description": "Optional tenant/workspace id. Defaults to tenant_<agent_name> in local mode."},
                "agent_name": {"type": "string", "description": "Agent name used for the local tenant, e.g. codex, claude, cursor."},
                "user_id": {"type": "string", "description": "Optional MatrixArk user id. Defaults to the local OS account."},
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "display_name": {"type": "string", "description": "Display name for the local MatrixArk user."},
                "external_subject": {"type": "string", "description": "Optional external subject such as local:<user>, okta:<id>, google:<id>."},
                "key_display_name": {"type": "string"},
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowed scopes for the new key. Defaults to context ingest/retrieve/feedback/replay plus resource and skill read.",
                },
                "role": {"type": "string", "default": "local_agent"},
                "allowed_user_ids": {"type": "array", "items": {"type": "string"}},
                "allowed_session_ids": {"type": "array", "items": {"type": "string"}},
                "allow_all_users": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, do not restrict the key to the derived local user.",
                },
                "expires_at_ms": {"type": "integer"},
                "key_prefix": {"type": "string", "default": "mk_local"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_api_keys",
        "description": "List MatrixArk API key metadata for an account/tenant. Raw keys and hashes are never returned.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "include_revoked": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_rotate_api_key",
        "description": "Revoke an active MatrixArk API key and create a replacement with the same scopes.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "api_key_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_revoke_api_key",
        "description": "Revoke a MatrixArk API key.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {"api_key": API_KEY_SCHEMA, "api_key_id": {"type": "string"}, "scope": SCOPE_SCHEMA},
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_map_sso_user",
        "description": "Map an external Okta/Google/Azure AD user id to a MatrixArk user id.",
        "inputSchema": {
            "type": "object",
            "required": ["provider", "external_user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "provider": {"type": "string", "description": "okta, google, azure_ad, or another IdP name."},
                "external_user_id": {"type": "string"},
                "matrixark_user_id": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_audit",
        "description": "List MatrixArk access-management audit records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    }
]
