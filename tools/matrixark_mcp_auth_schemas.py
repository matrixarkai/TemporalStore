#!/usr/bin/env python3
"""Shared auth and admin schema fragments for MatrixArk MCP tools."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_schema_common import SCOPE_SCHEMA
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_schema_common import SCOPE_SCHEMA


API_KEY_SCHEMA: Json = {
    "type": "string",
    "description": "Optional MatrixArk API key. Required when access mode is enforced; dev mode allows omitted keys for local testing.",
}

IDEMPOTENCY_KEY_SCHEMA: Json = {
    "type": "string",
    "description": "Optional caller-generated idempotency key for write APIs. Retries with the same key in the same account/tenant/user/session scope return the original response instead of writing duplicates.",
}

ADMIN_ACCOUNT_PROPERTIES: Json = {
    "api_key": API_KEY_SCHEMA,
    "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
    "account_id": {"type": "string", "description": "MatrixArk account/customer id. Generated or defaulted when omitted in dev mode."},
    "account_name": {"type": "string"},
    "tenant_id": {"type": "string", "description": "MatrixArk tenant/workspace id. Defaults to tenant_default for account creation."},
    "tenant_name": {"type": "string"},
    "scope": SCOPE_SCHEMA,
}
