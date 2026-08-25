#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk access-management and metadata-store support for the MCP server."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import *
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *

import hashlib
import hmac
import secrets


# Password login is intentionally separate from OAuth/SSO. MatrixArk stores only
# a salted PBKDF2-SHA256 hash; the plaintext password is never persisted or
# logged. SSO/Gmail auto-login continues to flow through sso_callback/sso_login.
MATRIXARK_PASSWORD_ITERATIONS = 210_000


def hash_matrixark_password(
    password: str, *, salt: bytes | None = None, iterations: int = MATRIXARK_PASSWORD_ITERATIONS
) -> Json:
    if not password:
        raise MatrixArkError("password must not be empty")
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return {
        "algo": "pbkdf2_sha256",
        "iterations": iterations,
        "salt_hex": salt.hex(),
        "password_hash": digest.hex(),
    }


def verify_matrixark_password(password: str, credential: Json | None) -> bool:
    if not password or not credential:
        return False
    try:
        salt = bytes.fromhex(str(credential.get("salt_hex", "")))
        iterations = int(credential.get("iterations") or MATRIXARK_PASSWORD_ITERATIONS)
    except (ValueError, TypeError):
        return False
    expected = str(credential.get("password_hash", ""))
    if not salt or not expected:
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations).hex()
    return hmac.compare_digest(digest, expected)


class MatrixArkMetadataStore:
    """Admin/control-plane metadata boundary for MatrixArk.

    Context records still live in TemporalStore. This store is for account,
    tenant, user, SSO, API-key, usage, and admin audit metadata that a portal
    needs to query transactionally. MySQL, MatrixKV SQL, and MatrixKV SQL share
    the same schema.
    """

    backend_name = "record_log"

    def append(self, record: Json) -> None:
        raise NotImplementedError

    def read_all(self) -> list[Json]:
        raise NotImplementedError

    def backend_info(self) -> Json:
        return {"backend": self.backend_name, "status": "ok"}


class MatrixArkRecordLogMetadataStore(MatrixArkMetadataStore):
    backend_name = "record_log"

    def __init__(self, adapter: "MatrixArkLocalAdapter") -> None:
        self.adapter = adapter

    def append(self, record: Json) -> None:
        self.adapter.append(record)

    def read_all(self) -> list[Json]:
        return [record for record in self.adapter.read_all() if str(record.get("record_type", "")).startswith("matrixark_")]


def _matrixark_env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS = {"mysql", "matrixkv_sql", "matrixkv_sql"}
MATRIXARK_OAUTH_TOKEN_FIELDS = {
    "access_token",
    "refresh_token",
    "id_token",
    "raw_id_token",
    "oauth_token",
    "oauth_refresh_token",
    "token",
}


def strip_oauth_token_fields(args: Json) -> Json:
    return {key: value for key, value in args.items() if key not in MATRIXARK_OAUTH_TOKEN_FIELDS}


class MatrixArkSqlMetadataStore(MatrixArkMetadataStore):
    """Small SQL metadata store.

    Supported backends:
    - sqlite: local/dev smoke tests, no extra dependency.
    - mysql: PyMySQL or mysql-connector-python DB-API connection.
    - matrixkv_sql: MySQL-compatible MatrixKV SQL endpoint, same table shape.
    - matrixkv_sql: MySQL-compatible MatrixKV SQL endpoint, same table shape.
    """

    TABLE = "matrixark_metadata_records"
    ACCOUNT_TABLE = "matrixark_accounts"
    TENANT_TABLE = "matrixark_tenants"
    USER_TABLE = "matrixark_users"
    API_KEY_TABLE = "matrixark_api_keys"
    API_KEY_USAGE_TABLE = "matrixark_api_key_usage"
    SSO_TABLE = "matrixark_sso_mappings"
    CREDENTIAL_TABLE = "matrixark_user_credentials"
    AUDIT_TABLE = "matrixark_audit_logs"
    NORMALIZED_TABLES = [
        ACCOUNT_TABLE,
        TENANT_TABLE,
        USER_TABLE,
        API_KEY_TABLE,
        API_KEY_USAGE_TABLE,
        SSO_TABLE,
        CREDENTIAL_TABLE,
        AUDIT_TABLE,
    ]

    def __init__(self, *, backend: str, dsn: str, auto_init: bool = True) -> None:
        self.backend_name = backend
        self.dsn = dsn
        self.auto_init = auto_init
        if auto_init:
            self.ensure_schema()

    def _connect(self):
        if self.backend_name == "sqlite":
            import sqlite3

            path = self.dsn
            if path.startswith("sqlite:///"):
                path = path[len("sqlite:///") :]
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            return conn
        if self.backend_name in MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS:
            from urllib.parse import urlparse, parse_qs, unquote

            parsed = urlparse(self.dsn)
            if parsed.scheme not in {"mysql", "matrixkv", "matrixkv+mysql", "matrixkv", "matrixkv+mysql"}:
                raise MatrixArkError(
                    "MATRIXARK_METADATA_DSN must be mysql://, matrixkv+mysql://, or matrixkv+mysql:// for SQL metadata"
                )
            params = {
                "host": parsed.hostname or "127.0.0.1",
                "port": parsed.port or 3306,
                "user": unquote(parsed.username or "root"),
                "password": unquote(parsed.password or ""),
                "database": parsed.path.lstrip("/") or os.environ.get("MATRIXARK_METADATA_DB", "matrixark"),
                "charset": "utf8mb4",
                "autocommit": True,
            }
            query = parse_qs(parsed.query)
            if "ssl_disabled" in query:
                params["ssl_disabled"] = query["ssl_disabled"][-1].lower() in {"1", "true", "yes"}
            try:
                import pymysql  # type: ignore

                return pymysql.connect(**params)
            except ModuleNotFoundError:
                try:
                    import mysql.connector  # type: ignore

                    return mysql.connector.connect(**params)
                except ModuleNotFoundError as exc:
                    raise MatrixArkError("SQL metadata backend requires pymysql or mysql-connector-python") from exc
        raise MatrixArkError(f"unsupported metadata backend: {self.backend_name}")

    def ensure_schema(self) -> None:
        if self.backend_name == "sqlite":
            statements = [
                f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE} (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  record_type TEXT NOT NULL,
                  account_id TEXT NOT NULL DEFAULT '',
                  tenant_id TEXT NOT NULL DEFAULT '',
                  user_id TEXT NOT NULL DEFAULT '',
                  api_key_id TEXT NOT NULL DEFAULT '',
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL
                )
                """,
                f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE}_scope ON {self.TABLE}(account_id, tenant_id, user_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE}_type_time ON {self.TABLE}(record_type, created_at_ms)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE}_api_key ON {self.TABLE}(api_key_id)",
                f"""
                CREATE TABLE IF NOT EXISTS {self.ACCOUNT_TABLE} (
                  account_id TEXT PRIMARY KEY,
                  account_name TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  updated_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL
                )
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.TENANT_TABLE} (
                  account_id TEXT NOT NULL,
                  tenant_id TEXT NOT NULL,
                  tenant_name TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  tenant_hash INTEGER NOT NULL DEFAULT 0,
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  updated_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL,
                  PRIMARY KEY (account_id, tenant_id)
                )
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.USER_TABLE} (
                  account_id TEXT NOT NULL,
                  tenant_id TEXT NOT NULL,
                  user_id TEXT NOT NULL,
                  display_name TEXT NOT NULL DEFAULT '',
                  external_subject TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  updated_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL,
                  PRIMARY KEY (account_id, tenant_id, user_id)
                )
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.API_KEY_TABLE} (
                  api_key_id TEXT PRIMARY KEY,
                  account_id TEXT NOT NULL DEFAULT '',
                  tenant_id TEXT NOT NULL DEFAULT '',
                  role TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  key_prefix TEXT NOT NULL DEFAULT '',
                  api_key_hash_prefix TEXT NOT NULL DEFAULT '',
                  expires_at_ms INTEGER NOT NULL DEFAULT 0,
                  last_used_at_ms INTEGER NOT NULL DEFAULT 0,
                  usage_count INTEGER NOT NULL DEFAULT 0,
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  updated_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL
                )
                """,
                f"CREATE INDEX IF NOT EXISTS idx_{self.API_KEY_TABLE}_scope ON {self.API_KEY_TABLE}(account_id, tenant_id, status)",
                f"""
                CREATE TABLE IF NOT EXISTS {self.API_KEY_USAGE_TABLE} (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  usage_id_hash INTEGER NOT NULL DEFAULT 0,
                  api_key_id TEXT NOT NULL DEFAULT '',
                  account_id TEXT NOT NULL DEFAULT '',
                  tenant_id TEXT NOT NULL DEFAULT '',
                  user_id TEXT NOT NULL DEFAULT '',
                  session_id TEXT NOT NULL DEFAULT '',
                  action TEXT NOT NULL DEFAULT '',
                  used_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL
                )
                """,
                f"CREATE INDEX IF NOT EXISTS idx_{self.API_KEY_USAGE_TABLE}_key_time ON {self.API_KEY_USAGE_TABLE}(api_key_id, used_at_ms)",
                f"""
                CREATE TABLE IF NOT EXISTS {self.SSO_TABLE} (
                  provider TEXT NOT NULL,
                  external_user_id TEXT NOT NULL,
                  account_id TEXT NOT NULL DEFAULT '',
                  tenant_id TEXT NOT NULL DEFAULT '',
                  user_id TEXT NOT NULL DEFAULT '',
                  email TEXT NOT NULL DEFAULT '',
                  display_name TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  updated_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL,
                  PRIMARY KEY (provider, external_user_id)
                )
                """,
                f"CREATE INDEX IF NOT EXISTS idx_{self.SSO_TABLE}_user ON {self.SSO_TABLE}(account_id, tenant_id, user_id)",
                f"""
                CREATE TABLE IF NOT EXISTS {self.CREDENTIAL_TABLE} (
                  account_id TEXT NOT NULL,
                  tenant_id TEXT NOT NULL,
                  user_id TEXT NOT NULL,
                  email TEXT NOT NULL DEFAULT '',
                  algo TEXT NOT NULL DEFAULT 'pbkdf2_sha256',
                  iterations INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL DEFAULT 'active',
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  updated_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL,
                  PRIMARY KEY (account_id, tenant_id, user_id)
                )
                """,
                f"CREATE INDEX IF NOT EXISTS idx_{self.CREDENTIAL_TABLE}_email ON {self.CREDENTIAL_TABLE}(account_id, tenant_id, email)",
                f"""
                CREATE TABLE IF NOT EXISTS {self.AUDIT_TABLE} (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  audit_id_hash INTEGER NOT NULL DEFAULT 0,
                  account_id TEXT NOT NULL DEFAULT '',
                  tenant_id TEXT NOT NULL DEFAULT '',
                  user_id TEXT NOT NULL DEFAULT '',
                  session_id TEXT NOT NULL DEFAULT '',
                  api_key_id TEXT NOT NULL DEFAULT '',
                  action TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT '',
                  role TEXT NOT NULL DEFAULT '',
                  created_at_ms INTEGER NOT NULL DEFAULT 0,
                  payload_json TEXT NOT NULL
                )
                """,
                f"CREATE INDEX IF NOT EXISTS idx_{self.AUDIT_TABLE}_scope_time ON {self.AUDIT_TABLE}(account_id, tenant_id, user_id, created_at_ms)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.AUDIT_TABLE}_action_time ON {self.AUDIT_TABLE}(action, created_at_ms)",
            ]
        else:
            statements = [
                f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE} (
                  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  record_type VARCHAR(96) NOT NULL,
                  account_id VARCHAR(128) NOT NULL DEFAULT '',
                  tenant_id VARCHAR(128) NOT NULL DEFAULT '',
                  user_id VARCHAR(256) NOT NULL DEFAULT '',
                  api_key_id VARCHAR(128) NOT NULL DEFAULT '',
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  KEY idx_scope (account_id, tenant_id, user_id),
                  KEY idx_type_time (record_type, created_at_ms),
                  KEY idx_api_key (api_key_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.ACCOUNT_TABLE} (
                  account_id VARCHAR(128) NOT NULL PRIMARY KEY,
                  account_name VARCHAR(256) NOT NULL DEFAULT '',
                  status VARCHAR(32) NOT NULL DEFAULT 'active',
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  updated_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.TENANT_TABLE} (
                  account_id VARCHAR(128) NOT NULL,
                  tenant_id VARCHAR(128) NOT NULL,
                  tenant_name VARCHAR(256) NOT NULL DEFAULT '',
                  status VARCHAR(32) NOT NULL DEFAULT 'active',
                  tenant_hash BIGINT NOT NULL DEFAULT 0,
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  updated_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  PRIMARY KEY (account_id, tenant_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.USER_TABLE} (
                  account_id VARCHAR(128) NOT NULL,
                  tenant_id VARCHAR(128) NOT NULL,
                  user_id VARCHAR(256) NOT NULL,
                  display_name VARCHAR(256) NOT NULL DEFAULT '',
                  external_subject VARCHAR(512) NOT NULL DEFAULT '',
                  status VARCHAR(32) NOT NULL DEFAULT 'active',
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  updated_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  PRIMARY KEY (account_id, tenant_id, user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.API_KEY_TABLE} (
                  api_key_id VARCHAR(128) NOT NULL PRIMARY KEY,
                  account_id VARCHAR(128) NOT NULL DEFAULT '',
                  tenant_id VARCHAR(128) NOT NULL DEFAULT '',
                  role VARCHAR(64) NOT NULL DEFAULT '',
                  status VARCHAR(32) NOT NULL DEFAULT 'active',
                  key_prefix VARCHAR(64) NOT NULL DEFAULT '',
                  api_key_hash_prefix VARCHAR(24) NOT NULL DEFAULT '',
                  expires_at_ms BIGINT NOT NULL DEFAULT 0,
                  last_used_at_ms BIGINT NOT NULL DEFAULT 0,
                  usage_count BIGINT NOT NULL DEFAULT 0,
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  updated_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  KEY idx_scope (account_id, tenant_id, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.API_KEY_USAGE_TABLE} (
                  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  usage_id_hash BIGINT NOT NULL DEFAULT 0,
                  api_key_id VARCHAR(128) NOT NULL DEFAULT '',
                  account_id VARCHAR(128) NOT NULL DEFAULT '',
                  tenant_id VARCHAR(128) NOT NULL DEFAULT '',
                  user_id VARCHAR(256) NOT NULL DEFAULT '',
                  session_id VARCHAR(256) NOT NULL DEFAULT '',
                  action VARCHAR(128) NOT NULL DEFAULT '',
                  used_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  KEY idx_key_time (api_key_id, used_at_ms)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.SSO_TABLE} (
                  provider VARCHAR(64) NOT NULL,
                  external_user_id VARCHAR(256) NOT NULL,
                  account_id VARCHAR(128) NOT NULL DEFAULT '',
                  tenant_id VARCHAR(128) NOT NULL DEFAULT '',
                  user_id VARCHAR(256) NOT NULL DEFAULT '',
                  email VARCHAR(320) NOT NULL DEFAULT '',
                  display_name VARCHAR(256) NOT NULL DEFAULT '',
                  status VARCHAR(32) NOT NULL DEFAULT 'active',
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  updated_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  PRIMARY KEY (provider, external_user_id),
                  KEY idx_user (account_id, tenant_id, user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.CREDENTIAL_TABLE} (
                  account_id VARCHAR(128) NOT NULL,
                  tenant_id VARCHAR(128) NOT NULL,
                  user_id VARCHAR(256) NOT NULL,
                  email VARCHAR(320) NOT NULL DEFAULT '',
                  algo VARCHAR(32) NOT NULL DEFAULT 'pbkdf2_sha256',
                  iterations BIGINT NOT NULL DEFAULT 0,
                  status VARCHAR(32) NOT NULL DEFAULT 'active',
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  updated_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  PRIMARY KEY (account_id, tenant_id, user_id),
                  KEY idx_email (account_id, tenant_id, email)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
                f"""
                CREATE TABLE IF NOT EXISTS {self.AUDIT_TABLE} (
                  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  audit_id_hash BIGINT NOT NULL DEFAULT 0,
                  account_id VARCHAR(128) NOT NULL DEFAULT '',
                  tenant_id VARCHAR(128) NOT NULL DEFAULT '',
                  user_id VARCHAR(256) NOT NULL DEFAULT '',
                  session_id VARCHAR(256) NOT NULL DEFAULT '',
                  api_key_id VARCHAR(128) NOT NULL DEFAULT '',
                  action VARCHAR(128) NOT NULL DEFAULT '',
                  status VARCHAR(32) NOT NULL DEFAULT '',
                  role VARCHAR(64) NOT NULL DEFAULT '',
                  created_at_ms BIGINT NOT NULL DEFAULT 0,
                  payload_json LONGTEXT NOT NULL,
                  KEY idx_scope_time (account_id, tenant_id, user_id, created_at_ms),
                  KEY idx_action_time (action, created_at_ms)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """,
            ]
        with self._connect() as conn:
            cur = conn.cursor()
            for statement in statements:
                cur.execute(statement)
            if self.backend_name == "sqlite":
                conn.commit()

    def _placeholder(self) -> str:
        return "?" if self.backend_name == "sqlite" else "%s"

    def _execute_insert(self, cur, table: str, columns: list[str], row: tuple[object, ...], *, conflict_columns: list[str] | None = None) -> None:
        ph = self._placeholder()
        column_sql = ", ".join(columns)
        value_sql = ", ".join([ph] * len(columns))
        sql = f"INSERT INTO {table} ({column_sql}) VALUES ({value_sql})"
        if conflict_columns:
            update_columns = [col for col in columns if col not in conflict_columns]
            if self.backend_name == "sqlite":
                update_sql = ", ".join([f"{col}=excluded.{col}" for col in update_columns])
                sql += f" ON CONFLICT({', '.join(conflict_columns)}) DO UPDATE SET {update_sql}"
            else:
                update_sql = ", ".join([f"{col}=VALUES({col})" for col in update_columns])
                sql += f" ON DUPLICATE KEY UPDATE {update_sql}"
        cur.execute(sql, row)

    def _append_normalized(self, cur, record: Json, payload: str) -> None:
        record_type = str(record.get("record_type", ""))
        created_at_ms = int(record.get("created_at_ms") or record.get("updated_at_ms") or record.get("used_at_ms") or now_ms())
        if record_type == "matrixark_account":
            self._execute_insert(
                cur,
                self.ACCOUNT_TABLE,
                ["account_id", "account_name", "status", "created_at_ms", "updated_at_ms", "payload_json"],
                (
                    str(record.get("account_id", "")),
                    str(record.get("account_name", "")),
                    str(record.get("status", "active")),
                    int(record.get("created_at_ms") or created_at_ms),
                    int(record.get("updated_at_ms") or created_at_ms),
                    payload,
                ),
                conflict_columns=["account_id"],
            )
        elif record_type == "matrixark_tenant":
            self._execute_insert(
                cur,
                self.TENANT_TABLE,
                ["account_id", "tenant_id", "tenant_name", "status", "tenant_hash", "created_at_ms", "updated_at_ms", "payload_json"],
                (
                    str(record.get("account_id", "")),
                    str(record.get("tenant_id", "")),
                    str(record.get("tenant_name", "")),
                    str(record.get("status", "active")),
                    int(record.get("tenant_hash") or 0),
                    int(record.get("created_at_ms") or created_at_ms),
                    int(record.get("updated_at_ms") or created_at_ms),
                    payload,
                ),
                conflict_columns=["account_id", "tenant_id"],
            )
        elif record_type == "matrixark_user":
            self._execute_insert(
                cur,
                self.USER_TABLE,
                ["account_id", "tenant_id", "user_id", "display_name", "external_subject", "status", "created_at_ms", "updated_at_ms", "payload_json"],
                (
                    str(record.get("account_id", "")),
                    str(record.get("tenant_id", "")),
                    str(record.get("user_id", "")),
                    str(record.get("display_name", "")),
                    str(record.get("external_subject", "")),
                    str(record.get("status", "active")),
                    int(record.get("created_at_ms") or created_at_ms),
                    int(record.get("updated_at_ms") or created_at_ms),
                    payload,
                ),
                conflict_columns=["account_id", "tenant_id", "user_id"],
            )
        elif record_type == "matrixark_api_key":
            hash_prefix = str(record.get("api_key_hash", ""))[:12]
            self._execute_insert(
                cur,
                self.API_KEY_TABLE,
                [
                    "api_key_id",
                    "account_id",
                    "tenant_id",
                    "role",
                    "status",
                    "key_prefix",
                    "api_key_hash_prefix",
                    "expires_at_ms",
                    "last_used_at_ms",
                    "usage_count",
                    "created_at_ms",
                    "updated_at_ms",
                    "payload_json",
                ],
                (
                    str(record.get("api_key_id", "")),
                    str(record.get("account_id", "")),
                    str(record.get("tenant_id", "")),
                    str(record.get("role", "")),
                    str(record.get("status", "active")),
                    str(record.get("key_prefix", "")),
                    hash_prefix,
                    int(record.get("expires_at_ms") or 0),
                    int(record.get("last_used_at_ms") or 0),
                    int(record.get("usage_count") or 0),
                    int(record.get("created_at_ms") or created_at_ms),
                    int(record.get("updated_at_ms") or created_at_ms),
                    payload,
                ),
                conflict_columns=["api_key_id"],
            )
        elif record_type == "matrixark_api_key_usage":
            self._execute_insert(
                cur,
                self.API_KEY_USAGE_TABLE,
                ["usage_id_hash", "api_key_id", "account_id", "tenant_id", "user_id", "session_id", "action", "used_at_ms", "payload_json"],
                (
                    int(record.get("usage_id_hash") or 0),
                    str(record.get("api_key_id", "")),
                    str(record.get("account_id", "")),
                    str(record.get("tenant_id", "")),
                    str(record.get("user_id", "")),
                    str(record.get("session_id", "")),
                    str(record.get("action", "")),
                    int(record.get("used_at_ms") or created_at_ms),
                    payload,
                ),
            )
        elif record_type == "matrixark_sso_user_mapping":
            self._execute_insert(
                cur,
                self.SSO_TABLE,
                ["provider", "external_user_id", "account_id", "tenant_id", "user_id", "email", "display_name", "status", "created_at_ms", "updated_at_ms", "payload_json"],
                (
                    str(record.get("provider", "")),
                    str(record.get("external_user_id", "")),
                    str(record.get("account_id", "")),
                    str(record.get("tenant_id", "")),
                    str(record.get("matrixark_user_id") or record.get("user_id") or ""),
                    str(record.get("email", "")),
                    str(record.get("display_name", "")),
                    str(record.get("status", "active")),
                    int(record.get("created_at_ms") or created_at_ms),
                    int(record.get("updated_at_ms") or created_at_ms),
                    payload,
                ),
                conflict_columns=["provider", "external_user_id"],
            )
        elif record_type == "matrixark_audit_log":
            self._execute_insert(
                cur,
                self.AUDIT_TABLE,
                ["audit_id_hash", "account_id", "tenant_id", "user_id", "session_id", "api_key_id", "action", "status", "role", "created_at_ms", "payload_json"],
                (
                    int(record.get("audit_id_hash") or 0),
                    str(record.get("account_id", "")),
                    str(record.get("tenant_id", "")),
                    str(record.get("user_id", "")),
                    str(record.get("session_id", "")),
                    str(record.get("api_key_id", "")),
                    str(record.get("action", "")),
                    str(record.get("status", "")),
                    str(record.get("role", "")),
                    int(record.get("created_at_ms") or created_at_ms),
                    payload,
                ),
            )

    def _placeholder(self) -> str:
        return "?" if self.backend_name == "sqlite" else "%s"

    def _execute_insert(self, cur, table: str, columns: list[str], row: tuple[object, ...], *, conflict_columns: list[str] | None = None) -> None:
        ph = self._placeholder()
        column_sql = ", ".join(columns)
        value_sql = ", ".join([ph] * len(columns))
        sql = f"INSERT INTO {table} ({column_sql}) VALUES ({value_sql})"
        if conflict_columns:
            update_columns = [col for col in columns if col not in conflict_columns]
            if self.backend_name == "sqlite":
                update_sql = ", ".join([f"{col}=excluded.{col}" for col in update_columns])
                sql += f" ON CONFLICT({', '.join(conflict_columns)}) DO UPDATE SET {update_sql}"
            else:
                update_sql = ", ".join([f"{col}=VALUES({col})" for col in update_columns])
                sql += f" ON DUPLICATE KEY UPDATE {update_sql}"
        cur.execute(sql, row)

    def _append_normalized(self, cur, record: Json, payload: str) -> None:
        record_type = str(record.get("record_type", ""))
        created_at_ms = int(record.get("created_at_ms") or record.get("updated_at_ms") or record.get("used_at_ms") or now_ms())
        if record_type == "matrixark_account":
            self._execute_insert(
                cur,
                self.ACCOUNT_TABLE,
                ["account_id", "account_name", "status", "created_at_ms", "updated_at_ms", "payload_json"],
                (str(record.get("account_id", "")), str(record.get("account_name", "")), str(record.get("status", "active")), int(record.get("created_at_ms") or created_at_ms), int(record.get("updated_at_ms") or created_at_ms), payload),
                conflict_columns=["account_id"],
            )
        elif record_type == "matrixark_tenant":
            self._execute_insert(
                cur,
                self.TENANT_TABLE,
                ["account_id", "tenant_id", "tenant_name", "status", "tenant_hash", "created_at_ms", "updated_at_ms", "payload_json"],
                (str(record.get("account_id", "")), str(record.get("tenant_id", "")), str(record.get("tenant_name", "")), str(record.get("status", "active")), int(record.get("tenant_hash") or 0), int(record.get("created_at_ms") or created_at_ms), int(record.get("updated_at_ms") or created_at_ms), payload),
                conflict_columns=["account_id", "tenant_id"],
            )
        elif record_type == "matrixark_user":
            self._execute_insert(
                cur,
                self.USER_TABLE,
                ["account_id", "tenant_id", "user_id", "display_name", "external_subject", "status", "created_at_ms", "updated_at_ms", "payload_json"],
                (str(record.get("account_id", "")), str(record.get("tenant_id", "")), str(record.get("user_id", "")), str(record.get("display_name", "")), str(record.get("external_subject", "")), str(record.get("status", "active")), int(record.get("created_at_ms") or created_at_ms), int(record.get("updated_at_ms") or created_at_ms), payload),
                conflict_columns=["account_id", "tenant_id", "user_id"],
            )
        elif record_type == "matrixark_api_key":
            self._execute_insert(
                cur,
                self.API_KEY_TABLE,
                ["api_key_id", "account_id", "tenant_id", "role", "status", "key_prefix", "api_key_hash_prefix", "expires_at_ms", "last_used_at_ms", "usage_count", "created_at_ms", "updated_at_ms", "payload_json"],
                (str(record.get("api_key_id", "")), str(record.get("account_id", "")), str(record.get("tenant_id", "")), str(record.get("role", "")), str(record.get("status", "active")), str(record.get("key_prefix", "")), str(record.get("api_key_hash", ""))[:12], int(record.get("expires_at_ms") or 0), int(record.get("last_used_at_ms") or 0), int(record.get("usage_count") or 0), int(record.get("created_at_ms") or created_at_ms), int(record.get("updated_at_ms") or created_at_ms), payload),
                conflict_columns=["api_key_id"],
            )
        elif record_type == "matrixark_api_key_usage":
            self._execute_insert(
                cur,
                self.API_KEY_USAGE_TABLE,
                ["usage_id_hash", "api_key_id", "account_id", "tenant_id", "user_id", "session_id", "action", "used_at_ms", "payload_json"],
                (int(record.get("usage_id_hash") or 0), str(record.get("api_key_id", "")), str(record.get("account_id", "")), str(record.get("tenant_id", "")), str(record.get("user_id", "")), str(record.get("session_id", "")), str(record.get("action", "")), int(record.get("used_at_ms") or created_at_ms), payload),
            )
        elif record_type == "matrixark_sso_user_mapping":
            self._execute_insert(
                cur,
                self.SSO_TABLE,
                ["provider", "external_user_id", "account_id", "tenant_id", "user_id", "email", "display_name", "status", "created_at_ms", "updated_at_ms", "payload_json"],
                (str(record.get("provider", "")), str(record.get("external_user_id", "")), str(record.get("account_id", "")), str(record.get("tenant_id", "")), str(record.get("matrixark_user_id") or record.get("user_id") or ""), str(record.get("email", "")), str(record.get("display_name", "")), str(record.get("status", "active")), int(record.get("created_at_ms") or created_at_ms), int(record.get("updated_at_ms") or created_at_ms), payload),
                conflict_columns=["provider", "external_user_id"],
            )
        elif record_type == "matrixark_user_credential":
            self._execute_insert(
                cur,
                self.CREDENTIAL_TABLE,
                ["account_id", "tenant_id", "user_id", "email", "algo", "iterations", "status", "created_at_ms", "updated_at_ms", "payload_json"],
                (str(record.get("account_id", "")), str(record.get("tenant_id", "")), str(record.get("user_id", "")), str(record.get("email", "")), str(record.get("algo", "pbkdf2_sha256")), int(record.get("iterations") or 0), str(record.get("status", "active")), int(record.get("created_at_ms") or created_at_ms), int(record.get("updated_at_ms") or created_at_ms), payload),
                conflict_columns=["account_id", "tenant_id", "user_id"],
            )
        elif record_type == "matrixark_audit_log":
            self._execute_insert(
                cur,
                self.AUDIT_TABLE,
                ["audit_id_hash", "account_id", "tenant_id", "user_id", "session_id", "api_key_id", "action", "status", "role", "created_at_ms", "payload_json"],
                (int(record.get("audit_id_hash") or 0), str(record.get("account_id", "")), str(record.get("tenant_id", "")), str(record.get("user_id", "")), str(record.get("session_id", "")), str(record.get("api_key_id", "")), str(record.get("action", "")), str(record.get("status", "")), str(record.get("role", "")), int(record.get("created_at_ms") or created_at_ms), payload),
            )

    def append(self, record: Json) -> None:
        payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
        row = (
            str(record.get("record_type", "")),
            str(record.get("account_id", "")),
            str(record.get("tenant_id", "")),
            str(record.get("user_id", "")),
            str(record.get("api_key_id", "")),
            int(record.get("created_at_ms") or record.get("updated_at_ms") or record.get("used_at_ms") or now_ms()),
            payload,
        )
        ph = self._placeholder()
        sql = f"INSERT INTO {self.TABLE} (record_type, account_id, tenant_id, user_id, api_key_id, created_at_ms, payload_json) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, row)
            self._append_normalized(cur, record, payload)
            if self.backend_name == "sqlite":
                conn.commit()

    def read_all(self) -> list[Json]:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT payload_json FROM {self.TABLE} ORDER BY id ASC")
            rows = cur.fetchall()
        records: list[Json] = []
        for row in rows:
            payload = row[0] if not isinstance(row, dict) else row.get("payload_json")
            try:
                records.append(json.loads(payload))
            except Exception:
                continue
        return records

    def normalized_counts(self) -> Json:
        counts: Json = {}
        with self._connect() as conn:
            cur = conn.cursor()
            for table in [self.TABLE, *self.NORMALIZED_TABLES]:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    row = cur.fetchone()
                    counts[table] = int(row[0] if row else 0)
                except Exception:
                    counts[table] = None
        return counts

    def check_ready(self) -> Json:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            row = cur.fetchone()
            for table in [self.TABLE, *self.NORMALIZED_TABLES]:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                cur.fetchone()
        return {
            "backend": self.backend_name,
            "status": "ok",
            "table": self.TABLE,
            "normalized_tables": list(self.NORMALIZED_TABLES),
            "probe": row[0] if row else 1,
            "sql_compatible_with": "mysql"
            if self.backend_name in MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS
            else self.backend_name,
            "product_family": "matrixkv"
            if self.backend_name == "matrixkv_sql"
            else ("matrixkv" if self.backend_name == "matrixkv_sql" else self.backend_name),
        }

    def backend_info(self) -> Json:
        return {
            "backend": self.backend_name,
            "dsn_configured": bool(self.dsn),
            "table": self.TABLE,
            "normalized_tables": list(self.NORMALIZED_TABLES),
            "auto_init": self.auto_init,
            "sql_compatible_with": "mysql"
            if self.backend_name in MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS
            else self.backend_name,
            "product_family": "matrixkv"
            if self.backend_name == "matrixkv_sql"
            else ("matrixkv" if self.backend_name == "matrixkv_sql" else self.backend_name),
        }


def build_matrixark_metadata_store(adapter: "MatrixArkLocalAdapter") -> MatrixArkMetadataStore:
    backend = os.environ.get("MATRIXARK_METADATA_BACKEND", "record_log").strip().lower()
    require_sql = _matrixark_env_truthy("MATRIXARK_REQUIRE_SQL_METADATA") or _matrixark_env_truthy("MATRIXARK_METADATA_REQUIRE_SQL")
    require_live = require_sql or _matrixark_env_truthy("MATRIXARK_METADATA_REQUIRE_LIVE")
    if backend in {"", "record_log", "temporalstore", "adapter"}:
        if require_sql:
            raise MatrixArkError(
                "MATRIXARK_REQUIRE_SQL_METADATA=1 requires MATRIXARK_METADATA_BACKEND=mysql, matrixkv_sql, or matrixkv_sql and a live MATRIXARK_METADATA_DSN"
            )
        return MatrixArkRecordLogMetadataStore(adapter)
    if backend == "sqlite" and require_sql:
        raise MatrixArkError(
            "MATRIXARK_REQUIRE_SQL_METADATA=1 requires MATRIXARK_METADATA_BACKEND=mysql, matrixkv_sql, or matrixkv_sql; sqlite is local-test only"
        )
    if backend in {"sqlite", *MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS}:
        dsn = os.environ.get("MATRIXARK_METADATA_DSN", "").strip()
        if backend == "sqlite" and not dsn:
            dsn = "/tmp/matrixark_metadata.sqlite3"
        if backend in MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS and not dsn:
            raise MatrixArkError("MATRIXARK_METADATA_DSN is required for mysql/matrixkv_sql/matrixkv_sql metadata backend")
        auto_init = os.environ.get("MATRIXARK_METADATA_AUTO_INIT", "1").strip().lower() in {"1", "true", "yes"}
        store = MatrixArkSqlMetadataStore(backend=backend, dsn=dsn, auto_init=auto_init)
        if require_live:
            store.check_ready()
        return store
    raise MatrixArkError("MATRIXARK_METADATA_BACKEND must be record_log, sqlite, mysql, matrixkv_sql, or matrixkv_sql")

try:  # mixin
    from tools.matrixark_access_portal import _AccessPortalMixin
except ImportError:
    from matrixark_access_portal import _AccessPortalMixin

try:  # mixin
    from tools.matrixark_access_sso import _AccessSsoMixin
except ImportError:
    from matrixark_access_sso import _AccessSsoMixin

try:  # mixin
    from tools.matrixark_access_apikey import _AccessApiKeyMixin
except ImportError:
    from matrixark_access_apikey import _AccessApiKeyMixin

try:  # mixin
    from tools.matrixark_access_accounts import _AccessAccountsMixin
except ImportError:
    from matrixark_access_accounts import _AccessAccountsMixin

class MatrixArkAccessManager(_AccessPortalMixin, _AccessSsoMixin, _AccessApiKeyMixin, _AccessAccountsMixin):
    """Small MatrixArk product access layer over the same storage adapter.

    It is deliberately simple: API keys authenticate the calling app/service;
    account_id + tenant_id + user_id + session_id isolate context records.
    """

    def __init__(self, adapter: MatrixArkLocalAdapter, *, mode: str = "dev") -> None:
        if mode not in {"dev", "enforced"}:
            raise MatrixArkError("access mode must be dev or enforced")
        self.adapter = adapter
        self.metadata = build_matrixark_metadata_store(adapter)
        self.mode = mode

    def authenticate(self, tool_name: str, args: Json) -> Json:
        api_key = optional_string(args, "api_key")
        scope = optional_object(args, "scope")
        required_scopes = MATRIXARK_TOOL_SCOPES.get(tool_name, set())
        if api_key:
            key_record = self.find_active_api_key(api_key)
            if not key_record:
                raise MatrixArkError("invalid or revoked MatrixArk API key")
            scopes = set(key_record.get("scopes", []))
            if not required_scopes.issubset(scopes):
                raise MatrixArkError(f"API key lacks required scope(s): {sorted(required_scopes)}")
            role = normalize_matrixark_role(str(key_record.get("role", "service")))
            if not role_allows_scopes(role, required_scopes):
                raise MatrixArkError(f"role {role!r} is not allowed to use scope(s): {sorted(required_scopes)}")
            account_id = str(key_record["account_id"])
            tenant_id = str(key_record["tenant_id"])
            requested_account = str(scope.get("account_id", ""))
            requested_tenant = str(scope.get("tenant_id", ""))
            if requested_account and requested_account != account_id:
                raise MatrixArkError("scope.account_id does not match API key account")
            if requested_tenant and requested_tenant != tenant_id:
                raise MatrixArkError("scope.tenant_id does not match API key tenant")
            if required_scopes.intersection(MATRIXARK_CONTEXT_SCOPES | {"portal:read"}):
                self.ensure_account_tenant_active(account_id, tenant_id)
            allowed_user_ids = set(key_record.get("allowed_user_ids", []))
            allowed_session_ids = set(key_record.get("allowed_session_ids", []))
            requested_user = str(scope.get("user_id") or (next(iter(allowed_user_ids)) if len(allowed_user_ids) == 1 else ""))
            requested_session = str(scope.get("session_id") or (next(iter(allowed_session_ids)) if len(allowed_session_ids) == 1 else ""))
            if allowed_user_ids and not requested_user:
                raise MatrixArkError("scope.user_id is required by API key")
            if allowed_session_ids and not requested_session:
                raise MatrixArkError("scope.session_id is required by API key")
            if allowed_user_ids and requested_user not in allowed_user_ids:
                raise MatrixArkError("scope.user_id is not allowed by API key")
            if allowed_session_ids and requested_session not in allowed_session_ids:
                raise MatrixArkError("scope.session_id is not allowed by API key")
            self.ensure_user_active(account_id, tenant_id, requested_user)
            hashes = identity_hashes(account_id, tenant_id, requested_user, requested_session)
            return {
                "mode": "api_key",
                "api_key_id": key_record["api_key_id"],
                "account_id": account_id,
                "tenant_id": tenant_id,
                **hashes,
                "scopes": sorted(scopes),
                "role": role,
                "user_id": requested_user,
                "session_id": requested_session,
                "allowed_user_ids": sorted(allowed_user_ids),
                "allowed_session_ids": sorted(allowed_session_ids),
            }
        if self.mode == "enforced" and required_scopes:
            raise MatrixArkError("MatrixArk API key is required")
        defaults = local_identity_defaults(args, scope)
        account_id = str(defaults["account_id"])
        tenant_id = str(defaults["tenant_id"])
        hashes = identity_hashes(account_id, tenant_id, str(defaults["user_id"]), str(defaults["session_id"]))
        return {
            "mode": "dev",
            "api_key_id": "dev",
            "account_id": account_id,
            "tenant_id": tenant_id,
            **hashes,
            "scopes": sorted(MATRIXARK_ALL_SCOPES),
            "role": "dev_admin",
            "user_id": str(defaults["user_id"]),
            "session_id": str(defaults["session_id"]),
            "agent_name": str(defaults["agent_name"]),
        }

    def authorize_and_enrich(self, tool_name: str, args: Json) -> Json:
        try:
            identity = self.authenticate(tool_name, args)
        except Exception as exc:
            self.append_denied_audit(tool_name, args, reason=str(exc))
            raise
        scope = optional_object(args, "scope")
        args["scope"] = enrich_scope_with_identity(scope, identity)
        auth_summary = {
            "mode": identity["mode"],
            "api_key_id": identity["api_key_id"],
            "account_id": identity["account_id"],
            "tenant_id": identity["tenant_id"],
            "role": identity["role"],
            "tenant_hash": identity.get("tenant_hash", 0),
            "user_hash": identity.get("user_hash", 0),
            "session_hash": identity.get("session_hash", 0),
            "scope_key": args["scope"].get("scope_key", identity.get("scope_key", "")),
        }
        if identity.get("user_id"):
            auth_summary["user_id"] = identity["user_id"]
        if identity.get("session_id"):
            auth_summary["session_id"] = identity["session_id"]
        if identity.get("agent_name"):
            auth_summary["agent_name"] = identity["agent_name"]
        args["_matrixark_auth"] = auth_summary
        if identity["mode"] == "api_key":
            self.append_api_key_usage(tool_name, identity, args["scope"])
        return identity

    def find_active_api_key(self, api_key: str) -> Json | None:
        hashed = secret_hash(api_key)
        for record in reversed(self.metadata.read_all()):
            if record.get("record_type") != "matrixark_api_key":
                continue
            if record.get("api_key_hash") == hashed:
                if record.get("status") != "active":
                    return None
                expires_at_ms = record.get("expires_at_ms")
                if isinstance(expires_at_ms, int) and expires_at_ms <= now_ms():
                    return None
                return record
        return None

    def latest_account_record(self, account_id: str) -> Json | None:
        for record in reversed(self.metadata.read_all()):
            if record.get("record_type") == "matrixark_account" and record.get("account_id") == account_id:
                return record
        return None

    def latest_tenant_record(self, account_id: str, tenant_id: str) -> Json | None:
        for record in reversed(self.metadata.read_all()):
            if (
                record.get("record_type") == "matrixark_tenant"
                and record.get("account_id") == account_id
                and record.get("tenant_id") == tenant_id
            ):
                return record
        return None

    def ensure_account_tenant_active(self, account_id: str, tenant_id: str) -> None:
        account = self.latest_account_record(account_id)
        if account and account.get("status") != "active":
            raise MatrixArkError("account is disabled")
        tenant = self.latest_tenant_record(account_id, tenant_id)
        if tenant and tenant.get("status") != "active":
            raise MatrixArkError("tenant is disabled")

    def latest_api_key_record(self, api_key_id: str) -> Json | None:
        for record in reversed(self.metadata.read_all()):
            if record.get("record_type") == "matrixark_api_key" and record.get("api_key_id") == api_key_id:
                return record
        return None

    def latest_user_record(self, account_id: str, tenant_id: str, user_id: str) -> Json | None:
        if not user_id:
            return None
        for record in reversed(self.metadata.read_all()):
            if (
                record.get("record_type") == "matrixark_user"
                and record.get("account_id") == account_id
                and record.get("tenant_id") == tenant_id
                and record.get("user_id") == user_id
            ):
                return record
        return None

    def ensure_user_active(self, account_id: str, tenant_id: str, user_id: str) -> None:
        record = self.latest_user_record(account_id, tenant_id, user_id)
        if record and record.get("status") != "active":
            raise MatrixArkError("scope.user_id is disabled")

    def latest_user_credential(self, account_id: str, tenant_id: str, user_id: str) -> Json | None:
        if not user_id:
            return None
        for record in reversed(self.metadata.read_all()):
            if (
                record.get("record_type") == "matrixark_user_credential"
                and record.get("account_id") == account_id
                and record.get("tenant_id") == tenant_id
                and record.get("user_id") == user_id
                and record.get("status", "active") == "active"
            ):
                return record
        return None

    def find_credential_user_id_by_email(self, account_id: str, tenant_id: str, email: str) -> str:
        email_normalized = email.strip().lower()
        if not email_normalized:
            return ""
        for record in reversed(self.metadata.read_all()):
            if (
                record.get("record_type") == "matrixark_user_credential"
                and record.get("account_id") == account_id
                and record.get("tenant_id") == tenant_id
                and str(record.get("email", "")).strip().lower() == email_normalized
                and record.get("status", "active") == "active"
            ):
                return str(record.get("user_id", ""))
        return ""

    def set_user_password(
        self, account_id: str, tenant_id: str, user_id: str, password: str, *, email: str = "", identity: Json | None = None
    ) -> None:
        """Store a salted PBKDF2 hash for email/password login. Plaintext is never persisted."""
        if not user_id:
            raise MatrixArkError("user_id is required to set a password")
        hashed = hash_matrixark_password(password)
        self.metadata.append(
            {
                "record_type": "matrixark_user_credential",
                "credential_id_hash": stable_hash(f"{account_id}:{tenant_id}:credential:{user_id}"),
                "account_id": account_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "email": email,
                **hashed,
                **identity_hashes(account_id, tenant_id, user_id),
                "status": "active",
                "created_by_api_key_id": (identity or {}).get("api_key_id", ""),
                "created_at_ms": now_ms(),
                "updated_at_ms": now_ms(),
            }
        )

    def verify_user_password(self, password: str, credential: Json | None) -> bool:
        """Constant-time PBKDF2 check for email/password login."""
        return verify_matrixark_password(password, credential)

    def _append_audit_record(self, record: Json) -> None:
        """Route one audit record to storage -- or drop it when auditing is off (the default).

        Audit records live in the main record log, so with auditing on every audited call grows
        the store forever; MATRIXARK_AUDIT_MODE (default off) is the one knob, shared with the
        server's audit policy. When on and the metadata backend is the record log, the write goes
        through the adapter's buffered audit path (MATRIXARK_DIRECT_AUDIT_MODE governs it;
        "buffered" coalesces into one durable append_many per flush interval). Any other metadata
        backend (SQL) keeps its own append -- audits live in its tables, not the record log.
        """
        mode = os.environ.get("MATRIXARK_AUDIT_MODE", "off").strip().lower() or "off"
        if mode in {"off", "none", "disabled"}:
            return
        appender = getattr(self.adapter, "append_audit", None)
        if callable(appender) and getattr(self.metadata, "backend_name", "") == "record_log":
            appender(record)
            return
        self.metadata.append(record)

    def append_audit(self, action: str, identity: Json, *, status: str, details: Json | None = None) -> None:
        self._append_audit_record(
            {
                "record_type": "matrixark_audit_log",
                "audit_id_hash": stable_hash(f"{action}:{identity.get('api_key_id')}:{now_ms()}"),
                "action": action,
                "status": status,
                "account_id": identity.get("account_id", ""),
                "tenant_id": identity.get("tenant_id", ""),
                "user_id": identity.get("user_id", ""),
                "session_id": identity.get("session_id", ""),
                "tenant_hash": identity.get("tenant_hash", 0),
                "user_hash": identity.get("user_hash", 0),
                "session_hash": identity.get("session_hash", 0),
                "scope_key": identity.get("scope_key", ""),
                "api_key_id": identity.get("api_key_id", ""),
                "role": normalize_matrixark_role(str(identity.get("role", ""))),
                "details": details or {},
                "created_at_ms": now_ms(),
            }
        )

    def append_denied_audit(self, action: str, args: Json, *, reason: str) -> None:
        scope = optional_object(args, "scope")
        api_key = optional_string(args, "api_key", "")
        account_id = canonical_account_id(str(scope.get("account_id") or args.get("account_id") or "")) if (scope.get("account_id") or args.get("account_id")) else ""
        tenant_id = canonical_tenant_id(str(scope.get("tenant_id") or args.get("tenant_id") or "")) if (scope.get("tenant_id") or args.get("tenant_id")) else ""
        hashes = identity_hashes(account_id, tenant_id, str(scope.get("user_id") or ""), str(scope.get("session_id") or "")) if account_id and tenant_id else {}
        self._append_audit_record(
            {
                "record_type": "matrixark_audit_log",
                "audit_id_hash": stable_hash(f"denied:{action}:{secret_hash(api_key) if api_key else 'no_key'}:{now_ms()}"),
                "action": action,
                "status": "denied",
                "account_id": account_id,
                "tenant_id": tenant_id,
                "user_id": str(scope.get("user_id") or ""),
                "session_id": str(scope.get("session_id") or ""),
                "tenant_hash": hashes.get("tenant_hash", 0),
                "user_hash": hashes.get("user_hash", 0),
                "session_hash": hashes.get("session_hash", 0),
                "scope_key": hashes.get("scope_key", ""),
                "api_key_id": "unknown",
                "api_key_hash_prefix": secret_hash(api_key)[:12] if api_key else "",
                "role": "unknown",
                "details": {"reason": reason, "scope_keys": sorted(scope.keys())},
                "created_at_ms": now_ms(),
            }
        )

    def append_api_key_usage(self, action: str, identity: Json, scope: Json) -> None:
        self.metadata.append(
            {
                "record_type": "matrixark_api_key_usage",
                "usage_id_hash": stable_hash(
                    f"{identity.get('api_key_id')}:{action}:{scope.get('user_id', '')}:{scope.get('session_id', '')}:{now_ms()}"
                ),
                "action": action,
                "api_key_id": identity.get("api_key_id", ""),
                "account_id": identity.get("account_id", ""),
                "tenant_id": identity.get("tenant_id", ""),
                "role": identity.get("role", ""),
                "user_id": scope.get("user_id", ""),
                "session_id": scope.get("session_id", ""),
                "tenant_hash": scope.get("tenant_hash", 0),
                "user_hash": scope.get("user_hash", 0),
                "session_hash": scope.get("session_hash", 0),
                "scope_key": scope.get("scope_key", ""),
                "used_at_ms": now_ms(),
            }
        )

    def ensure_identity_can_manage(self, identity: Json, account_id: str, tenant_id: str) -> None:
        if identity.get("mode") == "dev":
            return
        if identity.get("account_id") != account_id or identity.get("tenant_id") != tenant_id:
            raise MatrixArkError("admin operation account/tenant does not match API key")

    def ensure_identity_can_read_scope(self, identity: Json, account_id: str, tenant_id: str, scope: Json | None = None) -> None:
        if identity.get("mode") == "dev":
            return
        if identity.get("account_id") != account_id or identity.get("tenant_id") != tenant_id:
            raise MatrixArkError("portal scope account/tenant does not match API key")
        self.ensure_account_tenant_active(account_id, tenant_id)
        scope = scope or {}
        requested_user = str(scope.get("user_id") or identity.get("user_id") or "")
        requested_session = str(scope.get("session_id") or "")
        allowed_users = set(identity.get("allowed_user_ids") or [])
        allowed_sessions = set(identity.get("allowed_session_ids") or [])
        if requested_user:
            self.ensure_user_active(account_id, tenant_id, requested_user)
        if allowed_users and requested_user and requested_user not in allowed_users:
            raise MatrixArkError("portal scope.user_id is not allowed by API key")
        if allowed_sessions and requested_session and requested_session not in allowed_sessions:
            raise MatrixArkError("portal scope.session_id is not allowed by API key")










