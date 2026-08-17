<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# Canonical ingest envelope schema

There is **one shared schema for all non-agent ingestion** — `message`, `feedback`,
`resource`, `skill`, and `business_data`. Every such call is normalized by the same
validator, so the schema is the single formal contract for what a non-agent ingest
request may contain.

- **Schema:** [`integrations/agent-hooks/shared/ingest_envelope_schema.json`](../integrations/agent-hooks/shared/ingest_envelope_schema.json)
  (JSON Schema draft 2020-12, title *MatrixArk Ingest Envelope*).
- **Source of truth (code):** `tools/matrixark_mcp_core.py::normalize_envelope`
  (+ `resolve_ingest_messages`), which delegates to `require_messages` /
  `optional_object` in `tools/matrixark_mcp_core_identity.py` (re-exported by
  `matrixark_mcp_core`). The schema **describes** the normalizer; the code wins on any
  disagreement.
- **Conformance test:** `tools/test_ingest_envelope_schema.py` asserts a set of valid
  and invalid payloads are accepted / rejected by **both** the JSON schema and
  `normalize_envelope`.

The **agent** capture path is different: agent hooks
(`tools/matrixark_agent_hook.py`) emit the separate capture format
[`integrations/agent-hooks/shared/agent_event_schema.json`](../integrations/agent-hooks/shared/agent_event_schema.json)
and then **normalize INTO** this same envelope. That agent-event schema is a capture
format, not the ingest contract described here.

## Content requirement by kind

| kind            | required content                                             |
| --------------- | ----------------------------------------------------------- |
| `message`       | non-empty `messages`                                        |
| `feedback`      | non-empty `messages`                                        |
| `business_data` | non-empty `messages`                                        |
| `resource`      | one of `messages` \| `text` (`resource_text`) \| `raw_uri`  |
| `skill`         | one of `messages` \| `text` (`resource_text`) \| `raw_uri`  |

For `resource`/`skill`, `text`/`resource_text` is synthesized into a one-item user
`messages` list; a `raw_uri` becomes the file/URI source. The literal `raw_uri` value
`"inline-resource"` is a placeholder and is **not** treated as a real source.

## Fields

| field               | type                | notes                                                                                             |
| ------------------- | ------------------- | ------------------------------------------------------------------------------------------------- |
| `kind`              | string enum         | `message` \| `feedback` \| `resource` \| `skill` \| `business_data`. Optional; endpoint supplies a default. |
| `messages`          | array of objects    | Each item `{role, content}`. See roles below. Content must be a non-empty string.                 |
| `text` / `resource_text` | string         | Single-call content for `resource`/`skill` ingest.                                                |
| `raw_uri`           | string              | Local file path or URI source for `resource`/`skill`; passthrough otherwise.                      |
| `resource_type`     | string              | Free-form resource classifier (passthrough).                                                      |
| `scope`             | object \| null      | Optional. Subfields (all optional strings): `account_id`, `tenant_id`, `team`, `user_id`, `session_id`, plus `sharing_scope`. `null` treated as `{}`. |
| `sharing_scope`     | string enum         | `private_user` (default) \| `tenant_shared` \| `global_shared`. Read from `scope.sharing_scope` or top level. |
| `metadata`          | object \| null      | Free-form. May carry `ingestion_time_ms` / `storage_options` overrides. `null` treated as `{}`.   |
| `ingestion_time_ms` | positive integer    | Epoch ms. Optional; defaults to now. (Code also accepts a numeric string/float, coerced via `int()`.) |
| `storage_options`   | object              | Routing hints (`storage_mode`, `oplog_mode`, `replication_mode`, `consistency`, `route`, `storage_family`/`family`, `write_mode`, `raft_mode`, `background_write`). Canonicalized into `storage_route`. |
| `wait`              | boolean             | Client-facing: block until durable/processed instead of async.                                    |
| `raw_object_backend`| string              | Client-facing: raw object-store backend selector for resource bytes.                              |
| `max_skill_bytes`   | integer             | Client-facing: cap on skill/resource text bytes.                                                  |
| `expires_at`        | number (unix secs)  | PurchaseMemory per-record TTL. Absolute expiry in **seconds**. Every record from this ingest is stamped ephemeral, stops surfacing from retrieve/get_all once `now >= expires_at`, is excluded from summaries, and is lazily purged. Wins over `ttl_seconds`. |
| `ttl_seconds`       | number              | Relative TTL: `expires_at = ingestion_time + ttl_seconds`. Ignored if `expires_at` is also present. |
| `retention_cutoff_ts` | number (unix secs) | Scope-level cutoff. Records in this subject scope whose occurrence time `< cutoff` are hidden and reclaimed. Persisted as a durable scope marker. |
| `identity_key`      | string              | Keyed-upsert identity of a fact within the scope (e.g. `user.email`). A later ingest with the same key supersedes / is rank-guarded against the prior value. |
| `truth_class`       | string              | Confidence class for the keyed-upsert rank guard → integer rank (default `asserted=3, reported=2, inferred=1, unknown=0`; override via `MATRIXARK_TRUTH_RANK` env JSON). New rank `>=` existing supersedes; lower rank is rejected. |

The TTL headers `X-Expires-At` / `X-Ttl-Seconds` on `POST /v1/ingest` are a
header-form of `expires_at` / `ttl_seconds` (the JSON body wins when both are set).

Passthrough fields copied verbatim by `normalize_envelope` when present:
`context_pack_id`, `query_id_hash`, `accepted_refs`, `rejected_refs`,
`source_event_ids`, `segment_provider`, `segment_model`, `segment_model_path`,
`segment_max_new_tokens`, `segment_provider_fallback`, `understanding_provider`,
`extraction_provider`. The envelope is open (`additionalProperties: true`).

### Message roles

Canonical roles are `user`, `assistant`, `tool`, `system`. The normalizer
(`normalize_message_role`) also accepts documented aliases (matched
case-insensitively after trimming) that collapse to a canonical role:

- `human`, `prompt` → `user`
- `agent`, `ai`, `bot`, `llm`, `model`, `assistant_response` → `assistant`
- `function`, `function_call_output`, `custom_tool_call_output`, `tool_call_output`,
  `tool_result`, `tool_output`, `tool-output`, `tooloutput` → `tool`

The schema's `role` enum lists both the canonical roles and these aliases so it
matches what the code accepts.
