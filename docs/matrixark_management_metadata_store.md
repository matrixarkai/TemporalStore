# MatrixArk Management Metadata Store

MatrixArk separates **context serving data** from **control-plane metadata**.

- TemporalStore stores ContextNode, ContextEvent, ContextEntity, ContextSummary, ContextEmbedding, resources, skills, ContextPacks, and replay audit.
- The metadata store can store accounts, tenants, users, SSO mappings, API-key metadata, usage rows, and admin audit.

This lets the backend portal use a normal transactional SQL store for user management while keeping context retrieval TemporalStore-native.

## Backends

| Backend | Use case | Configuration |
| --- | --- | --- |
| `record_log` | Local/dev, no database dependency | `MATRIXARK_METADATA_BACKEND=record_log` |
| `sqlite` | Local tests and single-node demos | `MATRIXARK_METADATA_BACKEND=sqlite` plus `MATRIXARK_METADATA_DSN=/tmp/matrixark_metadata.sqlite3` |
| `mysql` | Cloud/control-plane deployment | `MATRIXARK_METADATA_BACKEND=mysql` plus MySQL DSN |
| `bytekv_sql` | ByteKV SQL-compatible deployment | `MATRIXARK_METADATA_BACKEND=bytekv_sql` plus ByteKV MySQL-compatible DSN |

## MySQL

```bash
export MATRIXARK_METADATA_BACKEND=mysql
export MATRIXARK_METADATA_DSN='mysql://matrixark:password@mysql:3306/matrixark'
export MATRIXARK_METADATA_AUTO_INIT=1
```

## ByteKV SQL

```bash
export MATRIXARK_METADATA_BACKEND=bytekv_sql
export MATRIXARK_METADATA_DSN='bytekv+mysql://matrixark:password@bytekv-sql:3306/matrixark'
export MATRIXARK_METADATA_AUTO_INIT=1
```

ByteKV SQL is expected to expose a MySQL-compatible protocol for this MVP path.

## Table

`matrixark_metadata_records` stores append-only JSON payloads with compact indexed columns:

```sql
record_type, account_id, tenant_id, user_id, api_key_id, created_at_ms, payload_json
```

Indexes:

- `(account_id, tenant_id, user_id)` for portal/user inventory.
- `(record_type, created_at_ms)` for audit and lifecycle queries.
- `(api_key_id)` for API-key lookup and revocation.

## Portal Behavior

The management portal now shows:

- configured metadata backend;
- active runtime backend from `matrixark_management_portal`;
- env snippets for record-log, MySQL, and ByteKV SQL;
- the metadata schema and storage policy;
- users, API keys, SSO links, usage, and audit without exposing raw key material.

Switching metadata backends does not automatically migrate old records. Copy existing `matrixark_*` admin records before changing from record-log to SQL in a real deployment.
