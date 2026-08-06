"""_AccessSsoMixin methods split from matrixark_access.MatrixArkAccessManager (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.matrixark_access import (
    strip_oauth_token_fields,
)
except ImportError:
    from matrixark_access import (
    strip_oauth_token_fields,
)


class _AccessSsoMixin:
    def latest_sso_mapping(self, account_id: str, tenant_id: str, provider: str, external_user_id: str) -> Json | None:
        for record in reversed(self.metadata.read_all()):
            if (
                record.get("record_type") == "matrixark_sso_user_mapping"
                and record.get("account_id") == account_id
                and record.get("tenant_id") == tenant_id
                and record.get("provider") == provider
                and record.get("external_user_id") == external_user_id
            ):
                return record
        return None

    def sso_login(self, args: Json, identity: Json) -> Json:
        """Map a verified external login into a MatrixArk user scope.

        MatrixArk does not act as the OAuth provider in this MVP. A product
        gateway, hosted portal, or enterprise IdP verifies Google/Okta/Azure AD
        first, then passes the verified subject here for MatrixArk account/user
        mapping and context-scope creation.
        """
        args = strip_oauth_token_fields(args)

        provider = safe_identifier(require_string(args, "provider"), default="sso")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        external_user_id = optional_string(args, "external_user_id") or optional_string(args, "email")
        if not external_user_id:
            raise MatrixArkError("external_user_id or email is required")
        email = optional_string(args, "email", external_user_id if "@" in external_user_id else "")
        id_token_verified = bool(args.get("id_token_verified", False))
        trusted_gateway = bool(args.get("trusted_gateway", False))
        if self.mode == "enforced" and not (id_token_verified or trusted_gateway):
            raise MatrixArkError("SSO login requires verified OAuth/OIDC claims in enforced mode")
        self.ensure_account_tenant_active(account_id, tenant_id)
        existing_mapping = self.latest_sso_mapping(account_id, tenant_id, provider, external_user_id)
        matrixark_user_id = optional_string(args, "matrixark_user_id") or str(
            (existing_mapping or {}).get("matrixark_user_id") or f"mu_{stable_hash(f'{account_id}:{tenant_id}:{provider}:{external_user_id}') }"
        )
        display_name = optional_string(args, "display_name", email or matrixark_user_id)
        external_subject = f"{provider}:{external_user_id}"
        created_records: list[str] = []
        if self.latest_account_record(account_id) is None:
            self.metadata.append(
                {
                    "record_type": "matrixark_account",
                    "account_id": account_id,
                    "account_name": optional_string(args, "account_name", account_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", "sso_login"),
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
                    "tenant_name": optional_string(args, "tenant_name", tenant_id),
                    **identity_hashes(account_id, tenant_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", "sso_login"),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("tenant")
        if self.latest_user_record(account_id, tenant_id, matrixark_user_id) is None:
            self.metadata.append(
                {
                    "record_type": "matrixark_user",
                    "user_record_hash": stable_hash(f"{account_id}:{tenant_id}:user:{matrixark_user_id}"),
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "user_id": matrixark_user_id,
                    "display_name": display_name,
                    "external_subject": external_subject,
                    **identity_hashes(account_id, tenant_id, matrixark_user_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", "sso_login"),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("user")
        mapping_created = False
        if existing_mapping is None:
            self.metadata.append(
                {
                    "record_type": "matrixark_sso_user_mapping",
                    "mapping_id_hash": stable_hash(f"{account_id}:{tenant_id}:{provider}:{external_user_id}"),
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "provider": provider,
                    "external_user_id": external_user_id,
                    "email": email,
                    "matrixark_user_id": matrixark_user_id,
                    **identity_hashes(account_id, tenant_id, matrixark_user_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", "sso_login"),
                    "created_at_ms": now_ms(),
                }
            )
            mapping_created = True
            created_records.append("sso_mapping")
        login_scope = enrich_scope_with_identity(
            {**scope, "account_id": account_id, "tenant_id": tenant_id, "user_id": matrixark_user_id},
            {"account_id": account_id, "tenant_id": tenant_id, "user_id": matrixark_user_id, "session_id": str(scope.get("session_id") or "")},
        )
        self.append_audit(
            "auth.sso_login",
            {**identity, "account_id": account_id, "tenant_id": tenant_id, "user_id": matrixark_user_id},
            status="ok",
            details={"provider": provider, "external_user_id": external_user_id, "mapping_created": mapping_created},
        )
        return {
            "status": "logged_in",
            "provider": provider,
            "external_user_id": external_user_id,
            "email": email,
            "matrixark_user_id": matrixark_user_id,
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scope": login_scope,
            "created_records": created_records,
            "mapping_created": mapping_created,
            "next_actions": {
                "apply_api_key": {
                    "tool": "matrixark_admin_apply_api_key",
                    "arguments": {"account_id": account_id, "tenant_id": tenant_id, "user_id": matrixark_user_id},
                },
                "open_portal": {
                    "tool": "matrixark_management_portal",
                    "arguments": {"scope": login_scope, "include_revoked": True},
                },
            },
        }

    def sso_callback(self, args: Json, identity: Json) -> Json:
        """Trusted gateway callback contract for OAuth/OIDC providers.

        The gateway verifies Google/Gmail, GitHub, Okta, or Azure AD tokens.
        MatrixArk receives only stable identity metadata and never stores raw
        OAuth access tokens, refresh tokens, or ID-token bytes.
        """
        args = strip_oauth_token_fields(args)

        provider = safe_identifier(require_string(args, "provider"), default="sso")
        allowed_providers = {"google", "gmail", "github", "okta", "azure_ad", "azuread", "oidc"}
        if provider not in allowed_providers:
            raise MatrixArkError(f"unsupported SSO provider {provider!r}; expected google, github, okta, azure_ad, or oidc")
        if self.mode == "enforced" and not (bool(args.get("id_token_verified", False)) or bool(args.get("trusted_gateway", False))):
            raise MatrixArkError("SSO callback requires trusted gateway verification in enforced mode")
        login = self.sso_login(args, identity)
        callback_identity = {
            **identity,
            "account_id": login.get("account_id", identity.get("account_id", "")),
            "tenant_id": login.get("tenant_id", identity.get("tenant_id", "")),
            "user_id": login.get("matrixark_user_id", ""),
        }
        self.append_audit(
            "auth.sso_callback",
            callback_identity,
            status="ok",
            details={
                "provider": provider,
                "external_user_id_present": bool(login.get("external_user_id")),
                "email_present": bool(login.get("email")),
                "matrixark_user_id": login.get("matrixark_user_id", ""),
                "stored_tokens": False,
            },
        )
        return {
            **login,
            "status": "sso_callback_mapped",
            "callback_contract": "trusted_gateway_oidc_oauth_callback",
            "stored_identity_metadata": {
                "provider": provider,
                "external_user_id": login.get("external_user_id", ""),
                "email": login.get("email", ""),
                "matrixark_user_id": login.get("matrixark_user_id", ""),
            },
            "stored_oauth_tokens": False,
        }

    def login(self, args: Json, identity: Json) -> Json:
        """Email/password login for users who registered without SSO.

        MatrixArk verifies a salted PBKDF2 hash and never stores or logs the
        plaintext password. Gmail/Google and GitHub users should use
        sso_callback instead. Failures return a single generic error and are
        audited without revealing whether the email or the password was wrong.
        """
        provider = safe_identifier(optional_string(args, "provider", "password"), default="password")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        email = optional_string(args, "email", "")
        password = optional_string(args, "password", "")
        user_id = optional_string(args, "user_id", "") or optional_string(args, "matrixark_user_id", "")
        if not password or not (email or user_id):
            self.append_denied_audit("auth.login", args, reason="email/user_id and password are required")
            raise MatrixArkError("email and password are required")
        if not user_id:
            user_id = self.find_credential_user_id_by_email(account_id, tenant_id, email)
        credential = self.latest_user_credential(account_id, tenant_id, user_id) if user_id else None
        if not credential or not self.verify_user_password(password, credential):
            self.append_denied_audit("auth.login", args, reason="invalid email or password")
            raise MatrixArkError("invalid email or password")
        self.ensure_account_tenant_active(account_id, tenant_id)
        self.ensure_user_active(account_id, tenant_id, user_id)
        login_scope = enrich_scope_with_identity(
            {**scope, "account_id": account_id, "tenant_id": tenant_id, "user_id": user_id},
            {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "session_id": str(scope.get("session_id") or "")},
        )
        self.append_audit(
            "auth.login",
            {**identity, "account_id": account_id, "tenant_id": tenant_id, "user_id": user_id},
            status="ok",
            details={"provider": provider, "email_present": bool(email), "matrixark_user_id": user_id},
        )
        return {
            "status": "logged_in",
            "provider": provider,
            "email": email or str(credential.get("email", "")),
            "matrixark_user_id": user_id,
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scope": login_scope,
            "auth_method": "password",
            "next_actions": {
                "apply_api_key": {
                    "tool": "matrixark_admin_apply_api_key",
                    "arguments": {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id},
                },
                "open_portal": {
                    "tool": "matrixark_management_portal",
                    "arguments": {"scope": login_scope, "include_revoked": True},
                },
            },
        }

    def map_sso_user(self, args: Json, identity: Json) -> Json:
        provider = require_string(args, "provider")
        external_user_id = require_string(args, "external_user_id")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        matrixark_user_id = optional_string(args, "matrixark_user_id") or f"mu_{stable_hash(f'{account_id}:{tenant_id}:{provider}:{external_user_id}')}"
        record = {
            "record_type": "matrixark_sso_user_mapping",
            "mapping_id_hash": stable_hash(f"{account_id}:{tenant_id}:{provider}:{external_user_id}"),
            "account_id": account_id,
            "tenant_id": tenant_id,
            "provider": provider,
            "external_user_id": external_user_id,
            "matrixark_user_id": matrixark_user_id,
            **identity_hashes(account_id, tenant_id, matrixark_user_id),
            "status": "active",
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        self.metadata.append(record)
        self.append_audit("admin.map_sso_user", identity, status="ok", details={"provider": provider, "matrixark_user_id": matrixark_user_id})
        return {"status": "mapped", "matrixark_user_id": matrixark_user_id, "provider": provider, "external_user_id": external_user_id}

