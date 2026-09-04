# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_AccessApiKeyMixin methods split from matrixark_access.MatrixArkAccessManager (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403


class _AccessApiKeyMixin:
    def create_api_key(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        scopes = optional_string_list(args, "scopes", ["context:ingest", "context:retrieve", "context:feedback", "context:replay"])
        if not scopes:
            raise MatrixArkError("scopes must not be empty")
        unknown_scopes = sorted(set(scopes) - MATRIXARK_ALL_SCOPES)
        if unknown_scopes:
            raise MatrixArkError(f"unknown MatrixArk scope(s): {unknown_scopes}")
        role = normalize_matrixark_role(optional_string(args, "role", "service"))
        if not role_allows_scopes(role, set(scopes)):
            raise MatrixArkError(f"role {role!r} cannot be granted requested scope(s): {sorted(scopes)}")
        display_name = optional_string(args, "display_name", role)
        allowed_user_ids = sorted(set(optional_string_list(args, "allowed_user_ids", [])))
        allowed_session_ids = sorted(set(optional_string_list(args, "allowed_session_ids", [])))
        expires_at_ms = args.get("expires_at_ms")
        if expires_at_ms is not None:
            if not isinstance(expires_at_ms, int) or expires_at_ms <= now_ms():
                raise MatrixArkError("expires_at_ms must be a future unix timestamp in milliseconds")
        # Optional per-key request QUOTA enforced at the gateway edge (observe + limit; not billing).
        # None/absent -> UNLIMITED (backward compatible; record shape unchanged for un-quota'd keys).
        request_quota = args.get("request_quota")
        if request_quota is not None and (not isinstance(request_quota, int) or isinstance(request_quota, bool) or request_quota < 0):
            raise MatrixArkError("request_quota must be a non-negative integer (requests per window)")
        quota_window = args.get("quota_window")
        if quota_window is not None and (not isinstance(quota_window, (int, float)) or isinstance(quota_window, bool) or quota_window < 0):
            raise MatrixArkError("quota_window must be a non-negative number of seconds")
        key_prefix = optional_string(args, "key_prefix", "mk_test")
        api_key = make_api_key(key_prefix)
        api_key_id = f"key_{stable_hash(api_key)}"
        record = {
            "record_type": "matrixark_api_key",
            "api_key_id": api_key_id,
            "api_key_hash": secret_hash(api_key),
            "account_id": account_id,
            "tenant_id": tenant_id,
            **identity_hashes(account_id, tenant_id),
            "scopes": sorted(set(scopes)),
            "role": role,
            "display_name": display_name,
            "allowed_user_ids": allowed_user_ids,
            "allowed_session_ids": allowed_session_ids,
            "expires_at_ms": expires_at_ms,
            "status": "active",
            # Stored so rotation can mint the replacement with the same prefix. Without it rotate
            # has nothing to carry and falls back to its default, turning an sk_live key into an
            # mk_test one.
            "key_prefix": key_prefix,
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        if request_quota is not None and request_quota > 0:
            record["request_quota"] = int(request_quota)
            if quota_window is not None and quota_window > 0:
                record["quota_window"] = float(quota_window)
        self.metadata.append(record)
        self.append_audit(
            "admin.create_api_key",
            identity,
            status="ok",
            details={
                "api_key_id": api_key_id,
                "account_id": account_id,
                "tenant_id": tenant_id,
                "allowed_user_count": len(allowed_user_ids),
                "allowed_session_count": len(allowed_session_ids),
                "expires_at_ms": expires_at_ms,
            },
        )
        return {
            "status": "created",
            "api_key": api_key,
            "api_key_id": api_key_id,
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scopes": record["scopes"],
            "role": role,
            "allowed_user_ids": allowed_user_ids,
            "allowed_session_ids": allowed_session_ids,
            "expires_at_ms": expires_at_ms,
            "request_quota": record.get("request_quota"),
            "quota_window": record.get("quota_window"),
            "warning": "Store api_key now. MatrixArk only stores its hash.",
        }

    def apply_api_key(self, args: Json, identity: Json) -> Json:
        """One-call local application flow for agent/API-key setup.

        In local/dev mode this lets Codex, Claude, Cursor, or another host agent
        ask for a usable MatrixArk key without first hand-creating account,
        tenant, and user records. Enforced deployments still require an admin
        key because this tool is protected by admin scopes.
        """

        scope = optional_object(args, "scope")
        defaults = local_identity_defaults(args, scope)
        account_id = canonical_account_id(optional_string(args, "account_id") or str(defaults["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(defaults["tenant_id"]))
        user_id = optional_string(args, "user_id") or str(scope.get("user_id") or defaults["user_id"])
        agent_name = safe_identifier(optional_string(args, "agent_name") or str(defaults["agent_name"]), default="local_agent")
        self.ensure_identity_can_manage(identity, account_id, tenant_id)

        created_records: list[str] = []
        if self.latest_account_record(account_id) is None:
            self.metadata.append(
                {
                    "record_type": "matrixark_account",
                    "account_id": account_id,
                    "account_name": optional_string(args, "account_name", account_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", ""),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("account")
        if self.latest_tenant_record(account_id, tenant_id) is None:
            self.metadata.append(
                {
                    "record_type": "matrixark_tenant",
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "tenant_name": optional_string(args, "tenant_name", agent_name),
                    "agent_name": agent_name,
                    **identity_hashes(account_id, tenant_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", ""),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("tenant")
        if user_id and self.latest_user_record(account_id, tenant_id, user_id) is None:
            self.metadata.append(
                {
                    "record_type": "matrixark_user",
                    "user_record_hash": stable_hash(f"{account_id}:{tenant_id}:user:{user_id}"),
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "display_name": optional_string(args, "display_name", user_id),
                    "external_subject": optional_string(args, "external_subject", f"local:{user_id}"),
                    **identity_hashes(account_id, tenant_id, user_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", ""),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("user")

        allow_all_users = bool(args.get("allow_all_users", False))
        key_args: Json = {
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scopes": optional_string_list(
                args,
                "scopes",
                [
                    "context:ingest",
                    "context:retrieve",
                    "context:feedback",
                    "context:replay",
                    "resource:ingest",
                    "resource:read",
                    "resource:manage",
                    "skill:read",
                    "skill:manage",
                    "portal:read",
                ],
            ),
            "role": normalize_matrixark_role(optional_string(args, "role", "local_agent")),
            "display_name": optional_string(args, "key_display_name", f"{agent_name} local key"),
            "allowed_user_ids": []
            if allow_all_users
            else sorted(set(optional_string_list(args, "allowed_user_ids", [user_id] if user_id else []))),
            "allowed_session_ids": sorted(set(optional_string_list(args, "allowed_session_ids", []))),
            "expires_at_ms": args.get("expires_at_ms"),
            "key_prefix": optional_string(args, "key_prefix", "mk_local"),
        }
        created_key = self.create_api_key(key_args, identity)
        local_scope = enrich_scope_with_identity(
            {
                **scope,
                "agent_name": agent_name,
                "user_id": user_id,
            },
            {
                "account_id": account_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "session_id": str(scope.get("session_id") or ""),
                "agent_name": agent_name,
            },
        )
        self.append_audit(
            "admin.apply_api_key",
            identity,
            status="ok",
            details={
                "api_key_id": created_key["api_key_id"],
                "account_id": account_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "agent_name": agent_name,
                "created_records": created_records,
            },
        )
        return {
            **created_key,
            "status": "applied",
            "created_records": created_records,
            "local_scope": local_scope,
            "default_node_path": self.adapter.default_session_node_path(local_scope),
        }

    def signup(self, args: Json, identity: Json) -> Json:
        """Production signup/onboarding flow for hosted MatrixArk.

        A trusted product gateway may call this after validating a human login;
        an admin key may also call it directly. MatrixArk stores only account,
        tenant, user, SSO metadata, a hashed first API key, and audit records.
        """

        trusted_gateway = bool(args.get("trusted_gateway", False))
        identity_scopes = set(identity.get("scopes", []))
        if self.mode == "enforced" and identity.get("mode") != "api_key" and not trusted_gateway:
            raise MatrixArkError("signup requires a trusted gateway or MatrixArk admin API key in enforced mode")
        if identity.get("mode") == "api_key" and not trusted_gateway and not {"admin:account", "admin:user", "admin:api_key"}.issubset(identity_scopes):
            raise MatrixArkError("signup with an API key requires account, user, and api-key admin scopes")

        provider = safe_identifier(optional_string(args, "provider", "local"), default="local")
        email = optional_string(args, "email", "")
        external_user_id = optional_string(args, "external_user_id", email or optional_string(args, "external_subject", ""))
        external_subject = optional_string(args, "external_subject", f"{provider}:{external_user_id}" if external_user_id else "")
        first_key_scopes = optional_string_list(args, "first_key_scopes", sorted(MATRIXARK_ALL_SCOPES))
        password = optional_string(args, "password", "")
        apply_args: Json = {
            **args,
            "scopes": first_key_scopes,
            "role": normalize_matrixark_role(optional_string(args, "first_key_role", optional_string(args, "role", "owner"))),
            "external_subject": external_subject or optional_string(args, "external_subject", ""),
            "key_display_name": optional_string(args, "key_display_name", "MatrixArk owner key"),
            "key_prefix": optional_string(args, "key_prefix", "mk_live"),
        }
        # Never let a plaintext password flow into API-key creation or audit records.
        apply_args.pop("password", None)
        result = self.apply_api_key(apply_args, identity)
        resolved_user_id = result.get("local_scope", {}).get("user_id", optional_string(args, "user_id", ""))
        password_set = False
        if password and resolved_user_id:
            self.set_user_password(
                result.get("account_id", identity.get("account_id", "")),
                result.get("tenant_id", identity.get("tenant_id", "")),
                resolved_user_id,
                password,
                email=email,
                identity=identity,
            )
            password_set = True
        signup_identity = {
            **identity,
            "account_id": result.get("account_id", identity.get("account_id", "")),
            "tenant_id": result.get("tenant_id", identity.get("tenant_id", "")),
            "user_id": result.get("local_scope", {}).get("user_id", optional_string(args, "user_id", "")),
        }
        self.append_audit(
            "auth.signup",
            signup_identity,
            status="ok",
            details={
                "api_key_id": result.get("api_key_id"),
                "provider": provider,
                "email_present": bool(email),
                "external_user_id_present": bool(external_user_id),
                "trusted_gateway": trusted_gateway,
                "created_records": result.get("created_records", []),
            },
        )
        return {
            **result,
            "status": "signed_up",
            "signup_contract": "account_tenant_user_first_scoped_key",
            "identity_metadata_stored": {
                "provider": provider,
                "email": email,
                "external_subject": external_subject,
                "matrixark_user_id": result.get("local_scope", {}).get("user_id", ""),
            },
            "password_login_enabled": password_set,
            "key_inventory_redacted": True,
        }

    def revoke_api_key(self, args: Json, identity: Json, *, action: str = "admin.revoke_api_key") -> Json:
        api_key_id = require_string(args, "api_key_id")
        record = self.latest_api_key_record(api_key_id)
        if not record or record.get("status") != "active":
            raise MatrixArkError("active api_key_id not found")
        # Creating a key checks this; revoking one did not, so an admin key for one tenant could
        # revoke another tenant's key given its id. Authorization must not rest on an identifier
        # being hard to guess.
        self.ensure_identity_can_manage(identity, record["account_id"], record["tenant_id"])
        revoked = {
            **record,
            "record_type": "matrixark_api_key",
            "status": "revoked",
            "revoked_by_api_key_id": identity.get("api_key_id", ""),
            "revoked_at_ms": now_ms(),
        }
        self.metadata.append(revoked)
        self.append_audit(action, identity, status="ok", details={"api_key_id": api_key_id})
        return {"status": "revoked", "api_key_id": api_key_id}

    def rotate_api_key(self, args: Json, identity: Json) -> Json:
        old_api_key_id = require_string(args, "api_key_id")
        old_record = self.latest_api_key_record(old_api_key_id)
        if old_record is None or old_record.get("status") != "active":
            raise MatrixArkError("active api_key_id not found")
        # Before either half runs. This used to be reached only inside the create below, by
        # which point the old key had already been revoked -- so a refused rotation destroyed the
        # key it was refused permission to touch.
        self.ensure_identity_can_manage(identity, old_record["account_id"], old_record["tenant_id"])
        # Mint first, revoke second. Whatever the create rejects -- a scope retired since the key
        # was made, a role that no longer carries it -- the caller keeps a working key. The old
        # order left them with none, and returned an error that read like nothing had happened.
        created = self.create_api_key(
            {
                "account_id": old_record["account_id"],
                "tenant_id": old_record["tenant_id"],
                "scopes": list(old_record.get("scopes", [])),
                "role": old_record.get("role", "service"),
                "display_name": old_record.get("display_name", old_record.get("role", "service")),
                "allowed_user_ids": list(old_record.get("allowed_user_ids", [])),
                "allowed_session_ids": list(old_record.get("allowed_session_ids", [])),
                "expires_at_ms": old_record.get("expires_at_ms"),
                "request_quota": old_record.get("request_quota"),
                "quota_window": old_record.get("quota_window"),
                # The replacement keeps the prefix of the key it replaces. Records written
                # before the prefix was stored have nothing to carry, so they keep the old default.
                "key_prefix": optional_string(args, "key_prefix",
                                              old_record.get("key_prefix") or "mk_test"),
            },
            identity,
        )
        try:
            self.revoke_api_key({"api_key_id": old_api_key_id}, identity,
                                action="admin.rotate_api_key.revoke_old")
        except MatrixArkError as exc:
            # The replacement exists and works. Say so rather than letting a bare failure suggest
            # the rotation did nothing -- the caller needs to know the new key is live and the old
            # one still is too.
            raise MatrixArkError(
                "rotation minted %s but could not revoke %s (%s); the new key is active and the "
                "old one is still active" % (created["api_key_id"], old_api_key_id, exc)
            ) from exc
        self.append_audit("admin.rotate_api_key", identity, status="ok", details={"old_api_key_id": old_api_key_id, "new_api_key_id": created["api_key_id"]})
        # `**created` last would overwrite the status with the "created" that create_api_key
        # returns, so every rotation reported itself as a creation.
        return {**created, "status": "rotated", "old_api_key_id": old_api_key_id}

    def list_api_keys(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        include_revoked = bool(args.get("include_revoked", False))
        metadata_records = self.metadata.read_all()
        usage_by_key: dict[str, Json] = {}
        for usage in metadata_records:
            if usage.get("record_type") != "matrixark_api_key_usage":
                continue
            if usage.get("account_id") != account_id or usage.get("tenant_id") != tenant_id:
                continue
            usage_key_id = str(usage.get("api_key_id", ""))
            if not usage_key_id:
                continue
            stats = usage_by_key.setdefault(usage_key_id, {"usage_count": 0, "last_used_at_ms": 0, "last_used_action": ""})
            stats["usage_count"] = int(stats.get("usage_count") or 0) + 1
            used_at_ms = int(usage.get("used_at_ms") or 0)
            if used_at_ms >= int(stats.get("last_used_at_ms") or 0):
                stats["last_used_at_ms"] = used_at_ms
                stats["last_used_action"] = usage.get("action", "")
        latest: dict[str, Json] = {}
        for record in reversed(metadata_records):
            if record.get("record_type") != "matrixark_api_key":
                continue
            if record.get("account_id") != account_id or record.get("tenant_id") != tenant_id:
                continue
            api_key_id = str(record.get("api_key_id", ""))
            if not api_key_id or api_key_id in latest:
                continue
            if record.get("status") == "revoked" and not include_revoked:
                continue
            expires_at_ms = record.get("expires_at_ms")
            effective_status = record.get("status", "")
            if effective_status == "active" and isinstance(expires_at_ms, int) and expires_at_ms <= now_ms():
                effective_status = "expired"
            latest[api_key_id] = {
                "api_key_id": api_key_id,
                "status": effective_status,
                "role": record.get("role", ""),
                "display_name": record.get("display_name", ""),
                "scopes": record.get("scopes", []),
                "allowed_user_ids": record.get("allowed_user_ids", []),
                "allowed_session_ids": record.get("allowed_session_ids", []),
                "request_quota": record.get("request_quota"),
                "quota_window": record.get("quota_window"),
                "expires_at_ms": expires_at_ms,
                "created_at_ms": record.get("created_at_ms", 0),
                "revoked_at_ms": record.get("revoked_at_ms", 0),
                "last_used_at_ms": usage_by_key.get(api_key_id, {}).get("last_used_at_ms", 0),
                "last_used_action": usage_by_key.get(api_key_id, {}).get("last_used_action", ""),
                "usage_count": usage_by_key.get(api_key_id, {}).get("usage_count", 0),
                "redacted": True,
            }
            if len(latest) >= limit:
                break
        self.append_audit("admin.list_api_keys", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "count": len(latest), "include_revoked": include_revoked})
        return {"status": "ok", "account_id": account_id, "tenant_id": tenant_id, "api_keys": list(latest.values()), "count": len(latest)}

