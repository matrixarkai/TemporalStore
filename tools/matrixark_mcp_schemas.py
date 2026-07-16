#!/usr/bin/env python3
"""MCP tool schemas for MatrixArk.

Keeping schemas out of the runtime server makes the server entrypoint small and
keeps MCP contract changes easy to review.
"""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import BACKEND_READINESS_TIMEOUT_MS, Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import BACKEND_READINESS_TIMEOUT_MS, Json


try:
    from tools.matrixark_mcp_schema_common import MESSAGE_SCHEMA, METADATA_SCHEMA, SCOPE_SCHEMA
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_schema_common import MESSAGE_SCHEMA, METADATA_SCHEMA, SCOPE_SCHEMA


try:
    from tools.matrixark_mcp_storage_schemas import (
        PART_STORAGE_OPTIONS_SCHEMA,
        RECORD_STORAGE_OPTIONS_SCHEMA,
        STORAGE_OPTIONS_SCHEMA,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_storage_schemas import (
        PART_STORAGE_OPTIONS_SCHEMA,
        RECORD_STORAGE_OPTIONS_SCHEMA,
        STORAGE_OPTIONS_SCHEMA,
    )


AGENT_HOOK_SCHEMA: Json = {
    "type": "object",
    "description": "Optional auto-capture metadata from a host-agent hook.",
    "required": ["source", "hook_type", "hook_id", "observed_at_ms", "auto_captured"],
    "properties": {
        "source": {"type": "string", "minLength": 1},
        "hook_type": {
            "type": "string",
            "enum": [
                "before_llm",
                "after_llm",
                "tool_result",
                "resource_added",
                "feedback",
                "session_commit",
            ],
        },
        "hook_id": {"type": "string", "minLength": 1},
        "observed_at_ms": {"type": "integer"},
        "idempotency_key": {"type": "string"},
        "trigger": {"type": "string"},
        "auto_captured": {"type": "boolean"},
    },
    "additionalProperties": True,
}

try:
    from tools.matrixark_mcp_auth_schemas import (
        ADMIN_ACCOUNT_PROPERTIES,
        API_KEY_SCHEMA,
        IDEMPOTENCY_KEY_SCHEMA,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_auth_schemas import (
        ADMIN_ACCOUNT_PROPERTIES,
        API_KEY_SCHEMA,
        IDEMPOTENCY_KEY_SCHEMA,
    )


try:
    from tools.matrixark_mcp_admin_schemas import ADMIN_TOOLS
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_admin_schemas import ADMIN_TOOLS
TOOLS: list[Json] = [
    {
        "name": "matrixark_ingest",
        "description": "Ingest chat, business, tool, or resource context into MatrixArk.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["message", "feedback", "resource", "skill", "business_data"],
                    "default": "message",
                    "description": "Optional envelope kind. Defaults to message.",
                },
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Required. The only required top-level field for ingest.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "storage_options": STORAGE_OPTIONS_SCHEMA,
                "record_storage_options": RECORD_STORAGE_OPTIONS_SCHEMA,
                "part_storage_options": {
                    **RECORD_STORAGE_OPTIONS_SCHEMA,
                    "description": "Compatibility alias for record_storage_options. Prefer record_storage_options in new clients.",
                },
                "temporalstore_storage_mode": {"type": "string", "description": "Convenience alias for storage_options.storage_mode."},
                "temporalstore_oplog_mode": {"type": "string", "description": "Convenience alias for storage_options.oplog_mode."},
                "temporalstore_replication_mode": {"type": "string", "description": "Convenience alias for storage_options.replication_mode."},
                "temporalstore_raft_mode": {"type": "boolean", "description": "Convenience alias for storage_options.raft_mode."},
                "temporalstore_route": {"type": "string", "description": "Convenience alias for storage_options.route, e.g. shared_store_async, shared_store_sync, raft_async, raft_sync."},
                "temporalstore_storage_family": {"type": "string", "description": "Convenience alias for storage_options.storage_family, e.g. shared_store or raft."},
                "temporalstore_write_mode": {"type": "string", "description": "Convenience alias for storage_options.write_mode, e.g. async or sync."},
                "temporalstore_durability": {"type": "string", "description": "Convenience alias for storage_options.durability, e.g. async or sync."},
                "temporalstore_background_write": {"type": "boolean", "description": "Convenience alias for storage_options.background_write."},
                "temporalstore_read_preference": {"type": "string", "description": "Convenience alias for storage_options.read_preference, e.g. replica_preferred."},
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
                "async_processing": {
                    "type": "boolean",
                    "default": False,
                    "description": "For message/business_data/feedback ingest, return after auth and lightweight ContextEvent/session-buffer write; extraction, summaries, compression, and embeddings run through async/session commit paths.",
                },
                "raw_uri": {"type": "string", "description": "Optional resource URI/path when kind=resource."},
                "resource_type": {"type": "string", "description": "Optional resource type such as md, txt, pdf, url."},
                "wait": {"type": "boolean", "default": True, "description": "For resource/skill imports, wait for parsing and record writes in the local runtime. wait=false records a queued ResourceImportTask."},
                "resource_version": {"type": "string", "description": "Optional caller-supplied resource version. Defaults to parser content version."},
                "supersedes_chunk_hashes": {"type": "object", "description": "Optional map from source_ref or content_hash to the older chunk hash this import supersedes."},
                "deployment_scope": {
                    "type": "string",
                    "enum": ["local", "global", "cloud", "on_prem", "hybrid"],
                    "description": "Optional deployment visibility marker for resource/skill registry records. Defaults to local.",
                },
                "raw_storage_mode": {
                    "type": "string",
                    "enum": ["local", "cloud", "s3", "remote"],
                    "description": "Resource/skill raw-file handling. local stores the local URI/path. cloud uploads to S3 first, stores the s3:// URI, and parses from the uploaded object.",
                },
                "s3_bucket": {
                    "type": "string",
                    "description": "Optional cloud-mode bucket. Defaults to MATRIXARK_RESOURCE_S3_BUCKET or MATRIXARK_S3_BUCKET.",
                },
                "s3_prefix": {
                    "type": "string",
                    "description": "Optional cloud-mode object prefix. Defaults to MATRIXARK_RESOURCE_S3_PREFIX or matrixark/raw.",
                },
                "auto_batch_extract": {
                    "type": "boolean",
                    "default": True,
                    "description": "VikingMem-style default for message/business_data/feedback ingest: persist each raw message immediately, buffer session_buffer_event markers, and commit extraction once session_buffer_threshold pending events accumulate. Set false to disable automatic batch extraction.",
                },
                "session_buffer_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Write session_buffer_event markers for later matrixark_session_commit while raw ContextEvent/cold storage is still persisted immediately. Set false to force immediate-only extraction behavior.",
                },
                "session_buffer_threshold": {
                    "type": "integer",
                    "default": 20,
                    "description": "Pending same-session raw event threshold for VikingMem-style automatic one-pass batch extraction.",
                },
                "idle_commit_timeout_ms": {
                    "type": "integer",
                    "default": 300000,
                    "minimum": 0,
                    "description": "Idle timeout trigger in milliseconds. Defaults to 5 minutes; if previous pending same-session messages are older than this, MatrixArk commits that window before ingesting the new message. Set 0 to disable.",
                },
                "flush_session_buffer": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, commit pending same-session extraction immediately after this raw message is persisted, even below session_buffer_threshold.",
                },
                "conversation_done": {
                    "type": "boolean",
                    "default": False,
                    "description": "Alias for flush_session_buffer when the caller knows the conversation/thread has ended.",
                },
                "session_done": {
                    "type": "boolean",
                    "default": False,
                    "description": "Alias for flush_session_buffer when the caller knows the session has ended.",
                },
                "task_complete": {
                    "type": "boolean",
                    "default": False,
                    "description": "Alias for flush_session_buffer when the task has completed and buffered extraction should run now.",
                },
                "lifecycle_event_type": {
                    "type": "string",
                    "description": "Optional agent lifecycle event. stop/session_end/task_complete/post_compact style events force a session buffer commit after raw ingest.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_ingestion_dashboard",
        "description": "Paged dashboard rows showing what MatrixArk ingested for an account, tenant, user, session, or agent.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "table": {
                    "type": "string",
                    "enum": ["messages", "resources", "skills", "events", "entities", "context_packs"],
                    "default": "messages",
                    "description": "Which dashboard table to page.",
                },
                "page_size": {"type": "integer", "default": 25, "minimum": 1, "maximum": 200},
                "page_token": {"type": ["integer", "string"], "default": 0, "description": "Offset returned as next_page_token."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_list_resources",
        "description": "List governed MatrixArk resources visible to the current account/tenant/user/session scope.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "resource_type": {"type": "string", "description": "Optional filter such as md, txt, pdf."},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_list_skills",
        "description": "List governed MatrixArk skills visible to the current account/tenant/user/session scope.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "include_disabled": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_update_skill",
        "description": "Update a skill registry entry without rewriting the original SKILL.md manifest.",
        "inputSchema": {
            "type": "object",
            "required": ["skill_hash"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
                "skill_hash": {"type": "integer"},
                "status": {"type": "string", "enum": ["active", "disabled"]},
                "precedence": {"type": "string", "enum": ["low", "normal", "high", "critical"]},
                "owner_scope": {"type": "string"},
                "version": {"type": "string"},
                "triggers": {"type": "array", "items": {"type": "string"}},
                "allowed_tools": {"type": "array", "items": {"type": "string"}},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_session_commit",
        "description": "Commit pending same-session raw ContextEvents into derived entities, segments, summaries, and indexes without duplicating raw events.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "storage_options": STORAGE_OPTIONS_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
                "threshold_messages": {
                    "type": "integer",
                    "default": 20,
                    "description": "Minimum pending raw events unless force=true. Explicit session_commit defaults to force=true.",
                },
                "force": {
                    "type": "boolean",
                    "default": True,
                    "description": "Force commit even below threshold for session end/task complete hooks.",
                },
                "commit_reason": {
                    "type": "string",
                    "enum": ["threshold", "hook_boundary", "idle_timeout", "manual_api"],
                    "description": "Why the session window is being committed.",
                },
                "idle_timeout_ms": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Commit when pending messages are below threshold but the session has been idle for at least this long.",
                },
                "max_messages": {
                    "type": "integer",
                    "description": "Optional cap for how many pending raw events to commit in this batch. Threshold/rolling commits default to threshold_messages.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_refresh_summaries",
        "description": "Background worker entrypoint: refresh dirty ContextNode L0/L1 summaries and embeddings asynchronously from recent events, entities, segments, and child summaries.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
                "limit": {"type": "integer", "default": 64},
                "refreshed_at_ms": {
                    "type": "integer",
                    "description": "Optional deterministic refresh timestamp for tests/replay.",
                },
                "max_raw_events_per_node": {
                    "type": "integer",
                    "default": 256,
                    "description": "Keep at most this many newest raw ContextEvents per node for normal retrieval; older events are represented by automatic temporal compression summaries.",
                },
                "compression_window_events": {
                    "type": "integer",
                    "default": 64,
                    "description": "Maximum number of old source ContextEvents to summarize into one automatic ContextCompressionEvent.",
                },
                "min_compression_events": {
                    "type": "integer",
                    "default": 8,
                    "description": "Minimum old uncompressed ContextEvents required before automatic compression writes a new window.",
                },
                "max_compression_windows_per_node": {
                    "type": "integer",
                    "default": 4,
                    "description": "Maximum automatic compression windows to create per dirty node refresh.",
                },
                "min_compression_event_age_ms": {
                    "type": "integer",
                    "default": 0,
                    "description": "Only compress source ContextEvents at least this old. Use 0 for threshold-only dev/test compression; use a positive cold-window age in production.",
                },
                "raw_event_ttl_after_compression_ms": {
                    "type": "integer",
                    "default": 2592000000,
                    "description": "TTL marker for raw source events after compression. Events are not deleted inline; eviction workers must also check recall reinforcement markers.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_retrieve",
        "description": "Retrieve a token-budgeted MatrixArk context pack for a raw query.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Required raw user or agent query."},
                "scope": SCOPE_SCHEMA,
                "storage_options": STORAGE_OPTIONS_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "max_context_tokens": {
                    "type": "integer",
                    "default": 32000,
                    "description": "Optional shared prompt context budget for local plus MatrixArk remote context. Defaults to MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS, currently 32000.",
                },
                "include_superseded_resources": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, retrieval may include older resource versions for historical replay.",
                },
                "local_context": {
                    "type": "array",
                    "description": "Optional local context already selected by Codex/Cursor, such as file snippets, open-buffer summaries, or tool output. MatrixArk dedupes remote refs against this and only fills the remaining budget.",
                    "items": {
                        "oneOf": [
                            {"type": "string"},
                            {
                                "type": "object",
                                "properties": {
                                    "text": {"type": "string"},
                                    "content": {"type": "string"},
                                    "source": {"type": "string"},
                                    "ref": {"type": "string"},
                                    "ref_type": {"type": "string"},
                                },
                                "additionalProperties": True,
                            },
                        ]
                    },
                },
                "local_context_tokens": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Optional token count for local context when the caller already counted it. MatrixArk reserves at least this many tokens before adding remote refs.",
                },
                "local_context_safety_margin_tokens": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Optional extra reserve subtracted from the remote MatrixArk budget so agent-local context and prompt scaffolding do not overflow the shared context budget. Defaults to 5% of max_context_tokens capped at 512.",
                },
                "reference_time_ms": {
                    "type": "integer",
                    "description": "Optional retrieval clock for deterministic tests and replay.",
                },
                "ranking": {
                    "type": "object",
                    "description": "Optional multi-path recall config: time decay, business weights, and auxiliary quota.",
                    "properties": {
                        "weights": {
                            "type": "object",
                            "properties": {
                                "time": {"type": "number", "minimum": 0, "maximum": 1},
                                "business": {"type": "number", "minimum": 0, "maximum": 1},
                            },
                            "additionalProperties": True,
                        },
                        "freshness_tolerance_ms": {"type": "integer", "minimum": 0},
                        "half_life_ms": {"type": "integer", "minimum": 1},
                        "business_type_weights": {
                            "type": "object",
                            "additionalProperties": {"type": "number"},
                        },
                        "auxiliary_quota": {"type": "integer", "minimum": 0},
                    },
                    "additionalProperties": True,
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_batch_extract",
        "description": "Run schema-driven one-pass memory extraction over a logical session batch.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Session/message batch. Best quality is usually >= 20 messages or explicit force/session_commit.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "storage_options": STORAGE_OPTIONS_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
                "threshold_messages": {
                    "type": "integer",
                    "default": 20,
                    "description": "Default extraction threshold. Below this, extraction is deferred unless force=true.",
                },
                "force": {
                    "type": "boolean",
                    "default": False,
                    "description": "Force one-pass extraction even when the batch is below threshold.",
                },
                "segment_provider": {
                    "type": "string",
                    "enum": ["deterministic", "oss", "oss-fallback"],
                    "default": "deterministic",
                    "description": "Segment boundary detector. oss uses a local transformers model and emits the same ContextSegment JSON shape.",
                },
                "segment_model": {
                    "type": "string",
                    "default": "Qwen/Qwen2.5-0.5B-Instruct",
                    "description": "OSS instruct model name for segment_provider=oss.",
                },
                "segment_model_path": {
                    "type": "string",
                    "description": "Optional local model path for offline OSS segmentation.",
                },
                "segment_max_new_tokens": {
                    "type": "integer",
                    "default": 512,
                    "minimum": 64,
                    "description": "Maximum tokens generated by the OSS segment boundary model.",
                },
                "segment_provider_fallback": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, fall back to deterministic segmentation when the OSS model cannot load or returns invalid JSON.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_feedback",
        "description": "Capture final answer feedback, confirmations, corrections, and accepted refs.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Required. Feedback text or final answer messages.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "storage_options": STORAGE_OPTIONS_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "context_pack_id": {
                    "type": "string",
                    "description": "Optional but strongly recommended for confirmation/correction inference.",
                },
                "accepted_refs": {
                    "type": "array",
                    "description": "Optional refs the user/agent accepted from the prior ContextPack.",
                },
                "rejected_refs": {
                    "type": "array",
                    "description": "Optional refs the user/agent rejected from the prior ContextPack.",
                },
                "agent_hook": AGENT_HOOK_SCHEMA,
                "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
                "async_processing": {
                    "type": "boolean",
                    "default": False,
                    "description": "For feedback ingest, return after lightweight feedback ContextEvent write and defer extraction/summaries/embeddings.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_replay",
        "description": "Replay locally captured MatrixArk events for a context pack.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "context_pack_id": {"type": "string"},
                "pack_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
            },
        },
    },
    {
        "name": "matrixark_backend_ready",
        "description": "Run a low-cost backend topology/storage readiness probe before ingestion or parity tests.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "reason": {"type": "string", "default": "manual"},
                "probe": {"type": "boolean", "default": True},
                "timeout_ms": {"type": "integer", "default": BACKEND_READINESS_TIMEOUT_MS},
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_backend_metrics",
        "description": "Return backend health, readiness, and low-cost metrics for the active MatrixArk storage adapter.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    *ADMIN_TOOLS,
]
