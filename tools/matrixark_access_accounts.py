# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_AccessAccountsMixin methods split from matrixark_access.MatrixArkAccessManager (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403


class _AccessAccountsMixin:
    def create_account(self, args: Json, identity: Json) -> Json:
        account_id = canonical_account_id(optional_string(args, "account_id") or f"acct_{stable_hash(optional_string(args, 'account_name', 'account'))}")
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or "tenant_default")
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        account_name = optional_string(args, "account_name", account_id)
        tenant_name = optional_string(args, "tenant_name", tenant_id)
        self.metadata.append(
            {
                "record_type": "matrixark_account",
                "account_id": account_id,
                "account_name": account_name,
                "status": "active",
                "created_by_api_key_id": identity.get("api_key_id", ""),
                "created_at_ms": now_ms(),
            }
        )
        self.metadata.append(
            {
                "record_type": "matrixark_tenant",
                "account_id": account_id,
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                **identity_hashes(account_id, tenant_id),
                "status": "active",
                "created_by_api_key_id": identity.get("api_key_id", ""),
                "created_at_ms": now_ms(),
            }
        )
        self.append_audit("admin.create_account", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id})
        return {"status": "created", "account_id": account_id, "tenant_id": tenant_id}

    def update_account(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        current_account = self.latest_account_record(account_id) or {}
        current_tenant = self.latest_tenant_record(account_id, tenant_id) or {}
        account_status = optional_string(args, "account_status", str(current_account.get("status") or "active"))
        tenant_status = optional_string(args, "tenant_status", str(current_tenant.get("status") or "active"))
        if account_status not in {"active", "disabled"}:
            raise MatrixArkError("account_status must be active or disabled")
        if tenant_status not in {"active", "disabled"}:
            raise MatrixArkError("tenant_status must be active or disabled")
        account_name = optional_string(args, "account_name", str(current_account.get("account_name") or account_id))
        tenant_name = optional_string(args, "tenant_name", str(current_tenant.get("tenant_name") or tenant_id))
        account_record = {
            "record_type": "matrixark_account",
            "account_id": account_id,
            "account_name": account_name,
            "status": account_status,
            "created_by_api_key_id": current_account.get("created_by_api_key_id", identity.get("api_key_id", "")),
            "created_at_ms": current_account.get("created_at_ms", now_ms()),
            "updated_by_api_key_id": identity.get("api_key_id", ""),
            "updated_at_ms": now_ms(),
        }
        tenant_record = {
            "record_type": "matrixark_tenant",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            **identity_hashes(account_id, tenant_id),
            "status": tenant_status,
            "created_by_api_key_id": current_tenant.get("created_by_api_key_id", identity.get("api_key_id", "")),
            "created_at_ms": current_tenant.get("created_at_ms", now_ms()),
            "updated_by_api_key_id": identity.get("api_key_id", ""),
            "updated_at_ms": now_ms(),
        }
        self.metadata.append(account_record)
        self.metadata.append(tenant_record)
        self.append_audit(
            "admin.update_account",
            identity,
            status="ok",
            details={"account_id": account_id, "tenant_id": tenant_id, "account_status": account_status, "tenant_status": tenant_status},
        )
        return {
            "status": "updated",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "account_status": account_status,
            "tenant_status": tenant_status,
            "tenant_hash": tenant_record["tenant_hash"],
        }

    def list_accounts(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        requested_account = optional_string(args, "account_id", "")
        requested_tenant = optional_string(args, "tenant_id", "")
        if identity.get("mode") != "dev":
            requested_account = identity["account_id"]
            requested_tenant = requested_tenant or identity["tenant_id"]
        latest_accounts: dict[str, Json] = {}
        latest_tenants: dict[tuple[str, str], Json] = {}
        for record in reversed(self.metadata.read_all()):
            if record.get("record_type") == "matrixark_account":
                account_id = str(record.get("account_id", ""))
                if not account_id or account_id in latest_accounts:
                    continue
                if requested_account and account_id != requested_account:
                    continue
                latest_accounts[account_id] = record
            elif record.get("record_type") == "matrixark_tenant":
                account_id = str(record.get("account_id", ""))
                tenant_id = str(record.get("tenant_id", ""))
                key = (account_id, tenant_id)
                if not account_id or not tenant_id or key in latest_tenants:
                    continue
                if requested_account and account_id != requested_account:
                    continue
                if requested_tenant and tenant_id != requested_tenant:
                    continue
                latest_tenants[key] = record
        rows = []
        for (account_id, tenant_id), tenant in latest_tenants.items():
            account = latest_accounts.get(account_id) or self.latest_account_record(account_id) or {}
            rows.append(
                {
                    "account_id": account_id,
                    "account_name": account.get("account_name", ""),
                    "account_status": account.get("status", ""),
                    "tenant_id": tenant_id,
                    "tenant_name": tenant.get("tenant_name", ""),
                    "tenant_status": tenant.get("status", ""),
                    "tenant_hash": tenant.get("tenant_hash", 0),
                    "created_at_ms": tenant.get("created_at_ms", 0),
                    "updated_at_ms": tenant.get("updated_at_ms", 0),
                }
            )
            if len(rows) >= limit:
                break
        self.append_audit("admin.list_accounts", identity, status="ok", details={"account_id": requested_account, "tenant_id": requested_tenant, "count": len(rows)})
        return {"status": "ok", "accounts": rows, "count": len(rows)}

    def create_user(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        user_id = require_string(args, "user_id")
        display_name = optional_string(args, "display_name", user_id)
        external_subject = optional_string(args, "external_subject", "")
        status = optional_string(args, "status", "active")
        if status not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        record = {
            "record_type": "matrixark_user",
            "user_record_hash": stable_hash(f"{account_id}:{tenant_id}:user:{user_id}"),
            "account_id": account_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "display_name": display_name,
            "external_subject": external_subject,
            **identity_hashes(account_id, tenant_id, user_id),
            "status": status,
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        self.metadata.append(record)
        self.append_audit("admin.create_user", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id})
        return {
            "status": "created",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "user_hash": record["user_hash"],
        }

    def update_user(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        user_id = require_string(args, "user_id")
        current = self.latest_user_record(account_id, tenant_id, user_id) or {}
        status = optional_string(args, "status", str(current.get("status") or "active"))
        if status not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        display_name = optional_string(args, "display_name", str(current.get("display_name") or user_id))
        external_subject = optional_string(args, "external_subject", str(current.get("external_subject") or ""))
        record = {
            "record_type": "matrixark_user",
            "user_record_hash": stable_hash(f"{account_id}:{tenant_id}:user:{user_id}"),
            "account_id": account_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "display_name": display_name,
            "external_subject": external_subject,
            **identity_hashes(account_id, tenant_id, user_id),
            "status": status,
            "created_by_api_key_id": current.get("created_by_api_key_id", identity.get("api_key_id", "")),
            "created_at_ms": current.get("created_at_ms", now_ms()),
            "updated_by_api_key_id": identity.get("api_key_id", ""),
            "updated_at_ms": now_ms(),
        }
        self.metadata.append(record)
        self.append_audit("admin.update_user", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "user_status": status})
        return {"status": "updated", "account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "user_status": status, "user_hash": record["user_hash"]}

    def list_users(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        status_filter = optional_string(args, "status", "")
        if status_filter and status_filter not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        latest: dict[str, Json] = {}
        for record in reversed(self.metadata.read_all()):
            if record.get("record_type") != "matrixark_user":
                continue
            if record.get("account_id") != account_id or record.get("tenant_id") != tenant_id:
                continue
            user_id = str(record.get("user_id", ""))
            if not user_id or user_id in latest:
                continue
            if status_filter and record.get("status") != status_filter:
                continue
            latest[user_id] = {
                "user_id": user_id,
                "display_name": record.get("display_name", ""),
                "external_subject": record.get("external_subject", ""),
                "status": record.get("status", ""),
                "user_hash": record.get("user_hash", 0),
                "created_at_ms": record.get("created_at_ms", 0),
                "updated_at_ms": record.get("updated_at_ms", 0),
            }
            if len(latest) >= limit:
                break
        self.append_audit("admin.list_users", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "count": len(latest)})
        return {"status": "ok", "account_id": account_id, "tenant_id": tenant_id, "users": list(latest.values()), "count": len(latest)}

