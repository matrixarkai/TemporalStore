#!/usr/bin/env python3
"""MatrixArk access-management and metadata-store support for the MCP server."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import *
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *


class MatrixArkMetadataStore:
    """Admin/control-plane metadata boundary for MatrixArk.

    Context records still live in TemporalStore. This store is for account,
    tenant, user, SSO, API-key, usage, and admin audit metadata that a portal
    needs to query transactionally. MySQL, MatrixKV SQL, and ByteKV SQL share
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


MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS = {"mysql", "matrixkv_sql", "bytekv_sql"}
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
    - bytekv_sql: MySQL-compatible ByteKV SQL endpoint, same table shape.
    """

    TABLE = "matrixark_metadata_records"
    ACCOUNT_TABLE = "matrixark_accounts"
    TENANT_TABLE = "matrixark_tenants"
    USER_TABLE = "matrixark_users"
    API_KEY_TABLE = "matrixark_api_keys"
    API_KEY_USAGE_TABLE = "matrixark_api_key_usage"
    SSO_TABLE = "matrixark_sso_mappings"
    AUDIT_TABLE = "matrixark_audit_logs"
    NORMALIZED_TABLES = [
        ACCOUNT_TABLE,
        TENANT_TABLE,
        USER_TABLE,
        API_KEY_TABLE,
        API_KEY_USAGE_TABLE,
        SSO_TABLE,
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
            if parsed.scheme not in {"mysql", "matrixkv", "matrixkv+mysql", "bytekv", "bytekv+mysql"}:
                raise MatrixArkError(
                    "MATRIXARK_METADATA_DSN must be mysql://, matrixkv+mysql://, or bytekv+mysql:// for SQL metadata"
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
            else ("bytekv" if self.backend_name == "bytekv_sql" else self.backend_name),
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
            else ("bytekv" if self.backend_name == "bytekv_sql" else self.backend_name),
        }


def build_matrixark_metadata_store(adapter: "MatrixArkLocalAdapter") -> MatrixArkMetadataStore:
    backend = os.environ.get("MATRIXARK_METADATA_BACKEND", "record_log").strip().lower()
    require_sql = _matrixark_env_truthy("MATRIXARK_REQUIRE_SQL_METADATA") or _matrixark_env_truthy("MATRIXARK_METADATA_REQUIRE_SQL")
    require_live = require_sql or _matrixark_env_truthy("MATRIXARK_METADATA_REQUIRE_LIVE")
    if backend in {"", "record_log", "temporalstore", "adapter"}:
        if require_sql:
            raise MatrixArkError(
                "MATRIXARK_REQUIRE_SQL_METADATA=1 requires MATRIXARK_METADATA_BACKEND=mysql, matrixkv_sql, or bytekv_sql and a live MATRIXARK_METADATA_DSN"
            )
        return MatrixArkRecordLogMetadataStore(adapter)
    if backend == "sqlite" and require_sql:
        raise MatrixArkError(
            "MATRIXARK_REQUIRE_SQL_METADATA=1 requires MATRIXARK_METADATA_BACKEND=mysql, matrixkv_sql, or bytekv_sql; sqlite is local-test only"
        )
    if backend in {"sqlite", *MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS}:
        dsn = os.environ.get("MATRIXARK_METADATA_DSN", "").strip()
        if backend == "sqlite" and not dsn:
            dsn = "/tmp/matrixark_metadata.sqlite3"
        if backend in MATRIXARK_MYSQL_COMPATIBLE_METADATA_BACKENDS and not dsn:
            raise MatrixArkError("MATRIXARK_METADATA_DSN is required for mysql/matrixkv_sql/bytekv_sql metadata backend")
        auto_init = os.environ.get("MATRIXARK_METADATA_AUTO_INIT", "1").strip().lower() in {"1", "true", "yes"}
        store = MatrixArkSqlMetadataStore(backend=backend, dsn=dsn, auto_init=auto_init)
        if require_live:
            store.check_ready()
        return store
    raise MatrixArkError("MATRIXARK_METADATA_BACKEND must be record_log, sqlite, mysql, matrixkv_sql, or bytekv_sql")

class MatrixArkAccessManager:
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

    def append_audit(self, action: str, identity: Json, *, status: str, details: Json | None = None) -> None:
        self.metadata.append(
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
        self.metadata.append(
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
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
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
        apply_args: Json = {
            **args,
            "scopes": first_key_scopes,
            "role": normalize_matrixark_role(optional_string(args, "first_key_role", optional_string(args, "role", "owner"))),
            "external_subject": external_subject or optional_string(args, "external_subject", ""),
            "key_display_name": optional_string(args, "key_display_name", "MatrixArk owner key"),
            "key_prefix": optional_string(args, "key_prefix", "mk_live"),
        }
        result = self.apply_api_key(apply_args, identity)
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
            "key_inventory_redacted": True,
        }

    def revoke_api_key(self, args: Json, identity: Json, *, action: str = "admin.revoke_api_key") -> Json:
        api_key_id = require_string(args, "api_key_id")
        record = self.latest_api_key_record(api_key_id)
        if not record or record.get("status") != "active":
            raise MatrixArkError("active api_key_id not found")
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
        self.revoke_api_key({"api_key_id": old_api_key_id}, identity, action="admin.rotate_api_key.revoke_old")
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
                "key_prefix": optional_string(args, "key_prefix", "mk_test"),
            },
            identity,
        )
        self.append_audit("admin.rotate_api_key", identity, status="ok", details={"old_api_key_id": old_api_key_id, "new_api_key_id": created["api_key_id"]})
        return {"status": "rotated", "old_api_key_id": old_api_key_id, **created}

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


    def management_portal(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_read_scope(identity, account_id, tenant_id, scope)
        effective_scope = enrich_scope_with_identity({**scope, "account_id": account_id, "tenant_id": tenant_id}, identity)
        page_size = args.get("page_size", 10)
        if not isinstance(page_size, int) or page_size <= 0 or page_size > 50:
            raise MatrixArkError("page_size must be an integer between 1 and 50")
        page_token = args.get("page_token", 0)
        if isinstance(page_token, str) and page_token.isdigit():
            page_token = int(page_token)
        if not isinstance(page_token, int) or page_token < 0:
            raise MatrixArkError("page_token must be a non-negative integer offset")

        def page_table_rows(rows: list[Json]) -> Json:
            page = rows[page_token : page_token + page_size]
            next_token = page_token + page_size if page_token + page_size < len(rows) else None
            return {"total": len(rows), "rows": page, "next_page_token": next_token, "page_token": page_token, "page_size": page_size}
        include_revoked = bool(args.get("include_revoked", False))
        records = self.adapter.read_all() + self.metadata.read_all()
        tables = ["messages", "resources", "skills", "events", "entities", "context_packs"]
        dashboard = {}
        totals = {}
        for table in tables:
            rows = self.adapter._dashboard_rows_for_table(records, table, effective_scope)
            totals[table] = len(rows)
            dashboard[table] = page_table_rows(rows)
        identity_scopes = set(identity.get("scopes", []))
        is_dev_identity = identity.get("mode") == "dev"
        if is_dev_identity or "admin:account" in identity_scopes:
            account_rows = self.list_accounts({"account_id": account_id, "tenant_id": tenant_id, "limit": 50}, identity)
        else:
            account_rows = {
                "status": "ok",
                "accounts": [{"account_id": account_id, "tenant_id": tenant_id, "account_status": "scoped", "tenant_status": "scoped"}],
                "count": 1,
            }
        if is_dev_identity or "admin:user" in identity_scopes:
            user_rows = self.list_users({"account_id": account_id, "tenant_id": tenant_id, "limit": 50}, identity)
        else:
            user_rows = {
                "status": "ok",
                "account_id": account_id,
                "tenant_id": tenant_id,
                "users": ([{"user_id": effective_scope.get("user_id", ""), "status": "scoped"}] if effective_scope.get("user_id") else []),
                "count": 1 if effective_scope.get("user_id") else 0,
            }
        if is_dev_identity or "admin:api_key" in identity_scopes:
            api_key_rows = self.list_api_keys(
                {"account_id": account_id, "tenant_id": tenant_id, "limit": 50, "include_revoked": include_revoked},
                identity,
            )
        else:
            api_key_rows = {"status": "ok", "account_id": account_id, "tenant_id": tenant_id, "api_keys": [], "count": 0}
        if is_dev_identity or "admin:audit" in identity_scopes:
            audit_rows = self.audit({"account_id": account_id, "tenant_id": tenant_id, "limit": 50}, identity)
        else:
            audit_rows = {"status": "ok", "audit_logs": [], "count": 0}
        dashboard["users"] = page_table_rows(user_rows.get("users", []))
        dashboard["api_keys"] = page_table_rows(api_key_rows.get("api_keys", []))
        dashboard["audit_logs"] = page_table_rows(audit_rows.get("audit_logs", []))
        scoped_records = [record for record in records if scope_matches(self.adapter._dashboard_record_scope(record), effective_scope)]
        nodes = [record for record in scoped_records if record.get("record_type") == "context_node"]
        summaries = [record for record in scoped_records if record.get("record_type") == "context_summary"]
        embeddings = [record for record in scoped_records if record.get("record_type") == "context_embedding"]
        dirty = [record for record in scoped_records if record.get("record_type") == "context_summary_dirty"]
        resource_records = [record for record in scoped_records if str(record.get("record_type", "")).startswith("resource_")]
        skill_records = [record for record in scoped_records if str(record.get("record_type", "")).startswith("skill_")]

        def count_by_node(rows: list[Json]) -> dict[Any, int]:
            counts: dict[Any, int] = {}
            for row in rows:
                node_hash = row.get("node_hash")
                if node_hash is None:
                    continue
                counts[node_hash] = counts.get(node_hash, 0) + 1
            return counts

        summary_counts = count_by_node(summaries)
        embedding_counts = count_by_node(embeddings)
        dirty_counts = count_by_node(dirty)
        resource_counts = count_by_node(resource_records)
        skill_counts = count_by_node(skill_records)
        topology_nodes = sorted(
            [
                {
                    "node_hash": record.get("node_hash", 0),
                    "parent_hash": record.get("parent_hash", 0),
                    "node_name": record.get("node_name", (record.get("node_path") or [""])[-1] if isinstance(record.get("node_path"), list) and record.get("node_path") else ""),
                    "node_path": record.get("node_path", []),
                    "depth": record.get("depth", 0),
                    "updated_at_ms": record.get("updated_at_ms", 0),
                    "summary_count": summary_counts.get(record.get("node_hash"), 0),
                    "embedding_count": embedding_counts.get(record.get("node_hash"), 0),
                    "dirty_summary_count": dirty_counts.get(record.get("node_hash"), 0),
                    "resource_record_count": resource_counts.get(record.get("node_hash"), 0),
                    "skill_record_count": skill_counts.get(record.get("node_hash"), 0),
                }
                for record in nodes
            ],
            key=lambda row: (int(row.get("depth") or 0), str(row.get("node_path"))),
        )[:100]
        topology_records = {
            "context_nodes": page_table_rows(nodes),
            "context_summaries": page_table_rows(summaries),
            "context_embeddings": page_table_rows(embeddings),
            "dirty_summaries": page_table_rows(dirty),
            "resources": page_table_rows(resource_records),
            "skills": page_table_rows(skill_records),
        }
        metrics = {
            "record_count": len(records),
            "scoped_record_count": len(scoped_records),
            "context_node_count": len(nodes),
            "context_summary_count": len(summaries),
            "context_embedding_count": len(embeddings),
            "dirty_summary_count": len(dirty),
            "api_key_count": api_key_rows.get("count", 0),
            "user_count": user_rows.get("count", 0),
            "message_count": totals.get("messages", 0),
            "resource_count": totals.get("resources", 0),
            "skill_count": totals.get("skills", 0),
            "event_count": totals.get("events", 0),
            "entity_count": totals.get("entities", 0),
            "context_pack_count": totals.get("context_packs", 0),
            "metadata_backend": self.metadata.backend_info(),
        }
        try:
            backend_metrics = self.adapter.backend_metrics()
        except Exception as exc:
            backend_metrics = {"backend": "unknown", "health": {"ok": False, "error": str(exc)}, "readiness": {"ok": False, "error": str(exc)}, "metrics": {}}
        backend_identity = {
            "backend": backend_metrics.get("backend", "unknown"),
            "storage_mode": backend_metrics.get("gateway_mode") or backend_metrics.get("production_path") or backend_metrics.get("backend", "unknown"),
            "metrics_format": backend_metrics.get("metrics_format", "prometheus"),
            "readiness_status": (backend_metrics.get("readiness") or {}).get("status") or ("ready" if (backend_metrics.get("readiness") or {}).get("ok") else "unknown"),
            "health_ok": bool((backend_metrics.get("health") or {}).get("ok", True)),
        }
        backend_inner_metrics = backend_metrics.get("metrics") if isinstance(backend_metrics.get("metrics"), dict) else {}
        rust_client_metrics = backend_inner_metrics.get("rust_client") if isinstance(backend_inner_metrics.get("rust_client"), dict) else {}
        context_pack_audits = sorted(
            [record for record in scoped_records if record.get("record_type") == "context_pack_audit"],
            key=lambda row: int(row.get("created_at_ms") or 0),
            reverse=True,
        )
        latest_pack_audit = context_pack_audits[0] if context_pack_audits else {}
        context_pack_debugger = {
            "context_pack_id": latest_pack_audit.get("context_pack_id", ""),
            "query": latest_pack_audit.get("query", ""),
            "selected_refs": latest_pack_audit.get("selected_refs", []),
            "dropped_refs": latest_pack_audit.get("dropped_refs", {}),
            "used_context_tokens": latest_pack_audit.get("used_context_tokens", 0),
            "used_local_context_tokens": latest_pack_audit.get("used_local_context_tokens", 0),
            "used_remote_context_tokens": latest_pack_audit.get("used_remote_context_tokens", 0),
            "total_prompt_context_tokens": latest_pack_audit.get("total_prompt_context_tokens", 0),
            "remote_context_budget_tokens": latest_pack_audit.get("remote_context_budget_tokens", 0),
            "local_context_policy": latest_pack_audit.get("local_context_policy", {}),
            "quality_warnings": latest_pack_audit.get("quality_warnings", []),
            "recall_policy": latest_pack_audit.get("recall_policy", {}),
            "replay_link": {
                "tool": "matrixark_replay",
                "arguments": {"context_pack_id": latest_pack_audit.get("context_pack_id", ""), "scope": effective_scope},
            } if latest_pack_audit else {},
        }
        import_tasks = [record for record in scoped_records if record.get("record_type") == "resource_import_task"]
        import_lag_count = len([record for record in import_tasks if record.get("status") not in {"completed", "failed"}])
        audit_write_failures = int(backend_inner_metrics.get("audit_flush_failures") or rust_client_metrics.get("commands_failed_total") or 0)
        retrieve_count = len(context_pack_audits)
        used_tokens = sum(int(record.get("used_context_tokens") or 0) for record in context_pack_audits)
        max_tokens = sum(int(record.get("max_context_tokens") or 0) for record in context_pack_audits) or max(1, retrieve_count * 10000)
        token_pressure_pct = round(min(100.0, used_tokens * 100.0 / max_tokens), 2)
        fallback_count = sum(1 for record in context_pack_audits if (record.get("recall_policy") or {}).get("tree_traversal", {}).get("fallback_to_flat"))
        fallback_rate_pct = round(fallback_count * 100.0 / max(1, retrieve_count), 2)
        revoked_key_usage = len([row for row in audit_rows.get("audit_logs", []) if row.get("status") == "denied" and "revoked" in json.dumps(row).lower()])
        model_fallback_flags = {
            "embedding_fallback_used": any("hashing" in json.dumps(record).lower() or "fallback" in json.dumps(record).lower() for record in scoped_records if record.get("record_type") in {"context_embedding", "matrixark_metric"}),
            "oss_model_fallback_used": any("oss" in json.dumps(record).lower() and "fallback" in json.dumps(record).lower() for record in scoped_records if record.get("record_type") == "matrixark_metric"),
        }
        observability = {
            "backend_identity": backend_identity,
            "prometheus_source": "matrixark_backend_metrics.prometheus plus MatrixArk record-log counters",
            "prometheus_panels": [
                {"name": "Ingest QPS", "metric": "matrixark_ingest_qps", "cpp_value": backend_inner_metrics.get("ingest_qps", 0), "rust_value": rust_client_metrics.get("qps", 0), "unit": "ops/s"},
                {"name": "Retrieve QPS", "metric": "matrixark_retrieve_qps", "cpp_value": backend_inner_metrics.get("retrieve_qps", retrieve_count), "rust_value": rust_client_metrics.get("qps", 0), "unit": "ops/s"},
                {"name": "p50 latency", "metric": "matrixark_request_latency_ms_p50", "cpp_value": backend_inner_metrics.get("p50_latency_ms", 0), "rust_value": rust_client_metrics.get("p50_latency_ms", 0), "unit": "ms"},
                {"name": "p95 latency", "metric": "matrixark_request_latency_ms_p95", "cpp_value": backend_inner_metrics.get("p95_latency_ms", 0), "rust_value": rust_client_metrics.get("p95_latency_ms", 0), "unit": "ms"},
                {"name": "p99 latency", "metric": "matrixark_request_latency_ms_p99", "cpp_value": backend_inner_metrics.get("p99_latency_ms", 0), "rust_value": rust_client_metrics.get("p99_latency_ms", 0), "unit": "ms"},
                {"name": "Errors", "metric": "matrixark_errors_total", "cpp_value": backend_inner_metrics.get("errors_total", 0), "rust_value": rust_client_metrics.get("commands_failed_total", 0), "unit": "count"},
                {"name": "Timeouts", "metric": "matrixark_timeouts_total", "cpp_value": backend_inner_metrics.get("timeouts_total", 0), "rust_value": rust_client_metrics.get("timeouts_total", 0), "unit": "count"},
                {"name": "Token pressure", "metric": "matrixark_token_pressure_ratio", "cpp_value": token_pressure_pct, "rust_value": token_pressure_pct, "unit": "%"},
                {"name": "Dirty summary lag", "metric": "matrixark_dirty_summary_lag_count", "cpp_value": len(dirty), "rust_value": len(dirty), "unit": "nodes"},
                {"name": "Resource/skill import lag", "metric": "matrixark_resource_skill_import_lag_count", "cpp_value": import_lag_count, "rust_value": import_lag_count, "unit": "tasks"},
                {"name": "Audit write failures", "metric": "matrixark_audit_write_failures_total", "cpp_value": audit_write_failures, "rust_value": audit_write_failures, "unit": "count"},
                {"name": "Backend readiness", "metric": "matrixark_backend_ready", "cpp_value": 1 if backend_identity["readiness_status"] in {"ready", "ok"} else 0, "rust_value": 1 if backend_identity["readiness_status"] in {"ready", "ok"} else 0, "unit": "bool"},
            ],
            "model_fallback_flags": model_fallback_flags,
            "alert_posture": [
                {"name": "unhealthy_backend", "status": "alert" if not backend_identity["health_ok"] else "ok", "detail": backend_identity["backend"]},
                {"name": "topology_not_ready", "status": "alert" if backend_identity["readiness_status"] not in {"ready", "ok"} else "ok", "detail": backend_identity["readiness_status"]},
                {"name": "revoked_key_usage", "status": "alert" if revoked_key_usage else "ok", "detail": revoked_key_usage},
                {"name": "high_token_pressure", "status": "warning" if token_pressure_pct >= 85 else "ok", "detail": f"{token_pressure_pct}%"},
                {"name": "stale_summary_lag", "status": "warning" if len(dirty) else "ok", "detail": len(dirty)},
                {"name": "retrieval_fallback_rate", "status": "warning" if fallback_rate_pct >= 5 else "ok", "detail": f"{fallback_rate_pct}%"},
                {"name": "audit_gaps", "status": "alert" if audit_write_failures else "ok", "detail": audit_write_failures},
            ],
            "backend_metrics": backend_metrics,
        }
        security_governance = {
            "account_id": account_id,
            "tenant_id": tenant_id,
            "effective_user_id": effective_scope.get("user_id", ""),
            "effective_session_id": effective_scope.get("session_id", ""),
            "role": identity.get("role", ""),
            "scopes": sorted(identity.get("scopes", [])),
            "allowed_user_ids": sorted(identity.get("allowed_user_ids", [])),
            "allowed_session_ids": sorted(identity.get("allowed_session_ids", [])),
            "api_key_id": identity.get("api_key_id", ""),
            "api_keys_redacted": True,
            "raw_oauth_tokens_stored": False,
            "access_enforcement": {
                "scope_filter_before_portal_rows": True,
                "account_tenant_active_required_for_portal": True,
                "disabled_user_blocked": bool(effective_scope.get("user_id")),
                "audit_every_portal_read": True,
                "audit_every_context_replay": True,
                "secret_storage": "api_key_hash_only",
            },
            "metadata_backend": self.metadata.backend_info(),
        }
        portal_actions = {
            "register_user": {
                "tool": "matrixark_admin_create_user",
                "arguments": {
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "user_id": effective_scope.get("user_id", "new_user"),
                    "display_name": effective_scope.get("user_id", "New User"),
                    "external_subject": "local:" + str(effective_scope.get("user_id", "new_user")),
                },
            },
            "apply_api_key": {
                "tool": "matrixark_admin_apply_api_key",
                "arguments": {
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "agent_name": effective_scope.get("agent_name", "local_agent"),
                    "user_id": effective_scope.get("user_id", ""),
                    "scopes": ["context:ingest", "context:retrieve", "context:feedback", "context:replay", "resource:read", "skill:read"],
                },
            },
            "sso_login": {
                "tool": "matrixark_auth_sso_login",
                "arguments": {
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "provider": "google",
                    "email": str(effective_scope.get("user_id", "user")) + "@example.com",
                    "id_token_verified": False,
                },
            },
            "open_ingestion_history": {
                "tool": "matrixark_ingestion_dashboard",
                "arguments": {"scope": effective_scope, "table": "messages", "page_size": page_size},
            },
        }
        return {
            "status": "ok",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scope": effective_scope,
            "accounts": account_rows.get("accounts", []),
            "users": user_rows.get("users", []),
            "api_keys": api_key_rows.get("api_keys", []),
            "audit_logs": audit_rows.get("audit_logs", []),
            "dashboard": dashboard,
            "topology": {"nodes": topology_nodes, "count": len(nodes), "records": topology_records},
            "context_pack_debugger": context_pack_debugger,
            "metrics": metrics,
            "observability": observability,
            "security_governance": security_governance,
            "metadata_store": self.metadata.backend_info(),
            "portal_actions": portal_actions,
        }

    def audit(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        account_id = optional_string(args, "account_id", identity["account_id"])
        tenant_id = optional_string(args, "tenant_id", identity["tenant_id"])
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        rows = [
            record
            for record in reversed(self.metadata.read_all())
            if record.get("record_type") in {"matrixark_audit_log", "matrixark_api_key_usage"}
            and (not account_id or record.get("account_id") == account_id)
            and (not tenant_id or record.get("tenant_id") == tenant_id)
        ][:limit]
        self.append_audit("admin.audit", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "count": len(rows)})
        return {"status": "ok", "audit_logs": rows, "count": len(rows)}
