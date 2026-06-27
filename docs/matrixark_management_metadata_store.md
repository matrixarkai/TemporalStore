# MatrixArk Management Metadata Store

MatrixArk separates **context serving data** from **control-plane metadata**.

- TemporalStore stores ContextNode, ContextEvent, ContextEntity, ContextSummary, ContextEmbedding, resources, skills, ContextPacks, and replay audit.
- The metadata store can store accounts, tenants, users, SSO mappings, API-key metadata, usage rows, and admin audit.

This lets the backend portal use a normal transactional SQL store for user management while keeping context retrieval TemporalStore-native. MatrixKV can be used here the same way as MySQL when its SQL-compatible endpoint is running.

## Backends

| Backend | Use case | Configuration |
| --- | --- | --- |
| `record_log` | Local/dev, no database dependency | `MATRIXARK_METADATA_BACKEND=record_log` |
| `sqlite` | Local tests and single-node demos | `MATRIXARK_METADATA_BACKEND=sqlite` plus `MATRIXARK_METADATA_DSN=/tmp/matrixark_metadata.sqlite3` |
| `mysql` | Cloud/control-plane deployment | `MATRIXARK_METADATA_BACKEND=mysql` plus MySQL DSN |
| `matrixkv_sql` | MatrixKV SQL-compatible deployment | `MATRIXARK_METADATA_BACKEND=matrixkv_sql` plus MatrixKV MySQL-compatible DSN |
| `bytekv_sql` | Legacy ByteKV SQL-compatible alias | `MATRIXARK_METADATA_BACKEND=bytekv_sql` plus ByteKV MySQL-compatible DSN |

## Production Requirement

Production/cloud deployment should fail closed unless a live SQL control plane is configured. Set:

```bash
export MATRIXARK_REQUIRE_SQL_METADATA=1
export MATRIXARK_METADATA_BACKEND=matrixkv_sql  # or mysql / bytekv_sql
export MATRIXARK_METADATA_AUTO_INIT=1        # create schema on startup, or pre-create it and keep live checks enabled
```

When `MATRIXARK_REQUIRE_SQL_METADATA=1` is set, MatrixArk rejects `record_log` and `sqlite` metadata backends. It also performs a live SQL readiness probe during access-manager startup, so a missing database, bad DSN, or unavailable MySQL/MatrixKV/ByteKV SQL endpoint fails deployment early.

## Local MySQL Docker

Run a local MySQL metadata backend and keep it running with Docker:

```bash
./tools/run_matrixark_metadata_mysql_local.sh
```

Equivalent manual command:

```bash
docker compose -f docker-compose.matrixark-metadata.yml up -d
export MATRIXARK_METADATA_BACKEND=mysql
export MATRIXARK_METADATA_DSN='mysql://matrixark:matrixark_password@127.0.0.1:3307/matrixark'
export MATRIXARK_METADATA_AUTO_INIT=1
export MATRIXARK_REQUIRE_SQL_METADATA=1
PYTHONPATH=tools python3 tools/check_matrixark_metadata_sql.py
```

The compose file starts `matrixark-mysql-metadata` on local port `3307` and stores data in the `matrixark_mysql_metadata` Docker volume.

## MySQL

```bash
export MATRIXARK_METADATA_BACKEND=mysql
export MATRIXARK_METADATA_DSN='mysql://matrixark:password@mysql:3306/matrixark'
export MATRIXARK_METADATA_AUTO_INIT=1
export MATRIXARK_REQUIRE_SQL_METADATA=1
```

## MatrixKV SQL

MatrixKV should be used as a MySQL-compatible control-plane database when its SQL service is enabled. The product backend name is `matrixkv_sql`; `bytekv_sql` remains only as a legacy-compatible alias.

```bash
export MATRIXARK_METADATA_BACKEND=matrixkv_sql
export MATRIXARK_METADATA_DSN='matrixkv+mysql://matrixark:password@matrixkv-sql:3306/matrixark'
export MATRIXARK_METADATA_AUTO_INIT=1
export MATRIXARK_REQUIRE_SQL_METADATA=1
PYTHONPATH=tools python3 tools/check_matrixark_metadata_sql.py
```

The current MatrixKV source tree is expected at `/root/src/github-services/MatrixKV`. Its SQL service exposes a MySQL-compatible protocol; MatrixArk stores only portal/control-plane metadata there, not raw context chunks, embeddings, or ContextPacks.

## ByteKV SQL

```bash
export MATRIXARK_METADATA_BACKEND=bytekv_sql
export MATRIXARK_METADATA_DSN='bytekv+mysql://matrixark:password@bytekv-sql:3306/matrixark'
export MATRIXARK_METADATA_AUTO_INIT=1
export MATRIXARK_REQUIRE_SQL_METADATA=1
```

ByteKV SQL is expected to expose a MySQL-compatible protocol for this MVP path.

## Tables

MatrixArk writes a compatibility event log plus production query tables. The append log remains the source for replay-compatible reads while the normalized tables give the portal efficient inventory, filtering, and analytics.

Compatibility table:

```sql
matrixark_metadata_records(record_type, account_id, tenant_id, user_id, api_key_id, created_at_ms, payload_json)
```

Normalized production tables:

| Table | Purpose | Primary lookup |
| --- | --- | --- |
| `matrixark_accounts` | account status and display metadata | `account_id` |
| `matrixark_tenants` | tenant/workspace status and tenant hash | `(account_id, tenant_id)` |
| `matrixark_users` | MatrixArk user profile, external subject, status | `(account_id, tenant_id, user_id)` |
| `matrixark_api_keys` | redacted API-key inventory, role, status, expiry, usage counters | `api_key_id` |
| `matrixark_api_key_usage` | per-call API-key usage rows | `(api_key_id, used_at_ms)` |
| `matrixark_sso_mappings` | Google/GitHub/Okta/Azure AD/OIDC identity mapping | `(provider, external_user_id)` |
| `matrixark_audit_logs` | admin, portal, key, SSO, replay, and denied-action audit | scope/time and action/time indexes |

Raw API keys are never stored. The query table stores only redacted inventory fields and a short hash prefix; the compatibility payload stores the API-key hash needed for lookup.

## Portal Behavior

The management portal now shows:

- configured metadata backend;
- active runtime backend from `matrixark_management_portal`;
- env snippets for record-log, MySQL, MatrixKV SQL, and ByteKV SQL;
- the metadata schema and storage policy;
- users, API keys, SSO links, usage, and audit without exposing raw key material.

Switching metadata backends does not automatically migrate old records. Copy existing `matrixark_*` admin records before changing from record-log to SQL in a real deployment.

For production portal analytics, query the normalized tables first. Use `matrixark_metadata_records` for compatibility replay, debugging, and migration backfill.

## Readiness Probe

`tools/check_matrixark_metadata_sql.py` verifies the configured SQL backend by building the MatrixArk metadata store, running `SELECT 1`, checking every normalized table, appending a `matrixark_metadata_probe` record, and reading it back. This is the deployment smoke test for the management portal control plane.

The probe requires `pymysql` or `mysql-connector-python` for MySQL/MatrixKV/ByteKV SQL.
