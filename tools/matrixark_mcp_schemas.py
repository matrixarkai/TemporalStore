#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MCP tool schemas for MatrixArk.

Keeping schemas out of the runtime server makes the server entrypoint small and
keeps MCP contract changes easy to review.
"""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import *
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *


MESSAGE_SCHEMA: Json = {
    "type": "object",
    "description": "One chat/tool/system message. Only role and content are required.",
    "required": ["role", "content"],
    "properties": {
        "role": {"type": "string", "enum": ["user", "assistant", "tool", "system"]},
        "content": {"type": "string", "minLength": 1},
        "name": {"type": "string", "description": "Optional human/tool/agent name."},
        "created_at_ms": {
            "type": "integer",
            "description": "Optional source event time. MatrixArk still uses server ingestion time as the primary write key.",
        },
    },
    "additionalProperties": True,
}

EXTRACTION_PHASE_SCHEMA: Json = {
    "type": "string",
    "enum": ["provisional", "final", "standalone"],
    "description": (
        "Memory extraction phase. provisional is for threshold/idle live checkpoints that may be "
        "merged or superseded at Stop; final marks a session/task boundary; standalone is for "
        "explicit non-session batch extraction."
    ),
}

SCOPE_SCHEMA: Json = {
    "type": "object",
    "description": "Optional memory scope. Local mode defaults account_id to acct_local, tenant_id to the agent name, and user_id to the local OS account when omitted. Send user_id or session_id when the host agent knows them; both together give the best user and thread grouping.",
    "properties": {
        "account_id": {"type": "string"},
        "tenant_id": {"type": "string", "description": "Tenant/workspace id. In local mode this defaults to tenant_<agent_name>."},
        "agent_name": {"type": "string", "description": "Optional host agent name such as codex, claude, cursor, or local test. Used to derive the local tenant when tenant_id is omitted."},
        "user_id": {
            "type": "string",
            "description": "Optional user memory scope. In local mode this defaults to the local OS account. Useful alone, and stronger when paired with session_id.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional thread/run/session scope. Useful alone, and strongest when paired with user_id.",
        },
        "team": {"type": "string"},
        "project": {"type": "string"},
    },
    "additionalProperties": True,
}

METADATA_SCHEMA: Json = {
    "type": "object",
    "description": "Optional routing and evidence hints. MatrixArk can infer or fill these internally.",
    "properties": {
        "source": {"type": "string", "description": "Optional source name such as cursor, codex, tool, or resource parser."},
        "node_path": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional tree/path hint. MatrixArk may choose its own internal ContextNode path.",
        },
        "reply_to_message_id": {"type": "string", "description": "Optional message linkage for feedback."},
        "reply_to_context_pack_id": {
            "type": "string",
            "description": "Optional ContextPack linkage for confirmation/correction inference.",
        },
    },
    "additionalProperties": True,
}

STORAGE_OPTIONS_SCHEMA: Json = {
    "type": "object",
    "description": "Optional TemporalStore serving-mode request hint. Native deployments still decide the actual topology, but MatrixArk records this policy for routing, audit, replay, and benchmark parity.",
    "properties": {
        "route": {
            "type": "string",
            "enum": ["shared_store_async", "shared_store_sync", "raft_async", "raft_sync"],
            "description": "Compact write-route preset. Expands into storage_mode, replication_mode, oplog_mode, and raft_mode.",
        },
        "storage_family": {
            "type": "string",
            "enum": ["default", "shared_store", "raft"],
            "description": "Friendly route selector. Choose shared_store or raft, then combine with write_mode=async|sync.",
        },
        "durability": {
            "type": "string",
            "enum": ["default", "async", "sync"],
            "description": "Durability shorthand. Defaults to async for highest write/read QPS; set sync only for records that must be durable before ack. canonical_storage_route reads this and uses it as write_mode when write_mode is left at default.",
        },
        "read_preference": {
            "type": "string",
            "enum": ["default", "primary", "replica", "replica_preferred"],
            "description": "Requested serving read preference. Async context ingest defaults to replica_preferred so read-heavy paths can use replicas.",
        },
        "write_mode": {
            "type": "string",
            "enum": ["default", "async", "sync"],
            "description": "Per-message write behavior. async lets the native backend acknowledge after memory append/background oplog work; sync waits for the durable route.",
        },
        "background_write": {
            "type": "boolean",
            "description": "Optional explicit background-write hint for native backends. Defaults to true for async write_mode and false for sync write_mode.",
        },
        "storage_mode": {
            "type": "string",
            "enum": ["default", "local", "single_node", "multi_node", "shared_store", "raft"],
            "description": "Requested storage topology/mode for this operation.",
        },
        "oplog_mode": {
            "type": "string",
            "enum": ["default", "async", "sync"],
            "description": "Requested oplog durability behavior. async is the high-throughput default; sync is for stronger durability gates.",
        },
        "replication_mode": {
            "type": "string",
            "enum": ["default", "none", "shared_store", "raft"],
            "description": "Requested replication behavior.",
        },
        "raft_mode": {
            "type": "boolean",
            "description": "Convenience flag. true implies storage_mode=raft and replication_mode=raft unless explicitly supplied.",
        },
        "consistency": {
            "type": "string",
            "enum": ["default", "eventual", "read_your_writes", "linearizable"],
            "description": "Requested read/write consistency profile for benchmark and production policy.",
        },
    },
    "additionalProperties": True,
}

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
                "temporalstore_storage_mode": {"type": "string", "description": "Convenience alias for storage_options.storage_mode."},
                "temporalstore_oplog_mode": {"type": "string", "description": "Convenience alias for storage_options.oplog_mode."},
                "temporalstore_replication_mode": {"type": "string", "description": "Convenience alias for storage_options.replication_mode."},
                "temporalstore_raft_mode": {"type": "boolean", "description": "Convenience alias for storage_options.raft_mode."},
                "temporalstore_route": {"type": "string", "description": "Convenience alias for storage_options.route, e.g. shared_store_async, shared_store_sync, raft_async, raft_sync."},
                "temporalstore_storage_family": {"type": "string", "description": "Convenience alias for storage_options.storage_family, e.g. shared_store or raft."},
                "temporalstore_write_mode": {"type": "string", "description": "Convenience alias for storage_options.write_mode, e.g. async or sync."},
                "temporalstore_background_write": {"type": "boolean", "description": "Convenience alias for storage_options.background_write."},
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "idempotency_key": IDEMPOTENCY_KEY_SCHEMA,
                "async_processing": {
                    "type": "boolean",
                    "default": False,
                    "description": "For message/business_data/feedback ingest, return after auth and lightweight ContextEvent/session-buffer write; extraction, summaries, compression, and embeddings run through async/session commit paths.",
                },
                "expires_at": {"type": "number", "description": "PurchaseMemory per-record TTL: absolute expiry in unix SECONDS. The ingest closure becomes ephemeral (hidden from retrieve/get_all after expiry, excluded from summaries, lazily purged). Wins over ttl_seconds."},
                "ttl_seconds": {"type": "number", "description": "PurchaseMemory relative TTL: expires_at = ingestion_time + ttl_seconds. Ignored if expires_at is set."},
                "retention_cutoff_ts": {"type": "number", "description": "PurchaseMemory scope-level retention cutoff (unix seconds): records in the subject scope older than this are hidden and reclaimed."},
                "identity_key": {"type": "string", "description": "PurchaseMemory keyed-upsert identity of a fact within the scope (e.g. user.email). A later ingest with the same key supersedes or is rank-guarded per truth_class."},
                "truth_class": {"type": "string", "description": "PurchaseMemory confidence class for the keyed-upsert rank guard (asserted=3, reported=2, inferred=1, unknown=0; override via MATRIXARK_TRUTH_RANK)."},
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
                    "default": False,
                    "description": "If true, commit the same-session buffer once session_buffer_threshold pending events accumulate.",
                },
                "session_buffer_threshold": {
                    "type": "integer",
                    "default": 20,
                    "description": "Pending same-session raw event threshold for automatic one-pass batch extraction.",
                },
                "idle_commit_timeout_ms": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Optional idle timeout. If previous pending same-session messages are older than this, MatrixArk commits that window before ingesting the new message.",
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
                    "enum": [
                        "messages",
                        "resources",
                        "skills",
                        "events",
                        "entities",
                        "context_packs",
                        "summary_refresh",
                        "async_pipeline",
                    ],
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
        "name": "matrixark_embedding_status",
        "description": "How much of a scope is encoded and how much is still waiting: counts, the "
                       "models and vector widths in use, and any deferred extraction still to run.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
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
                "extraction_phase": EXTRACTION_PHASE_SCHEMA,
                "final_session_boundary": {
                    "type": "boolean",
                    "description": "Set true when this commit is the final semantic boundary for the session/task. Stop/hook_boundary force commits set this automatically.",
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
                "ensure_embeddings": {
                    "type": "boolean",
                    "default": True,
                    "description": "Also repair missing or stale embeddings for events, segments, entities/profiles, nodes, summaries, and compression summaries after dirty summary refresh.",
                },
                "embedding_backfill_limit": {
                    "type": "integer",
                    "default": 512,
                    "description": "Maximum source records whose missing or stale embeddings should be generated in this refresh call.",
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
                    # Both taken from the constant every server path falls back to when the caller
                    # omits this, rather than written out beside it. They said 128000 while the
                    # three consumers applied 500000, so an agent reading this schema was told a
                    # budget four times smaller than the one it would actually be given -- and
                    # nothing applies the schema's own default, so the number here is a claim about
                    # the server rather than an instruction to it.
                    "default": DEFAULT_MAX_CONTEXT_TOKENS,
                    "description": (
                        "Optional shared prompt context budget for local plus MatrixArk remote "
                        "context. Defaults to MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS, currently %d."
                        % DEFAULT_MAX_CONTEXT_TOKENS
                    ),
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
                "include_retrieval_metrics": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, include compact retrieval metrics such as remote budget, selected ref count, scanned records, cache/pushdown evidence, drop counters, and memory-layer budget/pressure for session/profile/cross-session refs.",
                },
                "include_retrieval_debug": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, return the full debug ContextPack instead of the compact prompt-facing pack. Use for audits/tests, not normal prompt assembly.",
                },
                "debug_context_pack": {
                    "type": "boolean",
                    "default": False,
                    "description": "Alias for include_retrieval_debug.",
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
        "outputSchema": {
            "type": "object",
            "description": (
                "Compact prompt-facing ContextPack. Normal retrieval output includes selected refs plus "
                "async freshness/readiness and bounded memory-layer budget/pressure summaries so agents can "
                "reason over same-session, cross-session/profile, summary, and shared context layers. debug-only "
                "hierarchy and lineage buckets are exposed only when include_retrieval_debug/debug_context_pack is enabled."
            ),
            "properties": {
                "selected_refs": {
                    "type": "array",
                    "description": "Prompt-facing refs selected under the shared local+remote context budget.",
                    "items": {"type": "object", "additionalProperties": True},
                },
                "memory_hierarchy": {
                    "type": "object",
                    "description": (
                        "Public retrieval hierarchy contract: same-session memory first, profile entity bridge "
                        "for cross-session state, bounded cross-session evidence, then summary/compression and shared context."
                    ),
                    "properties": {
                        "models": {
                            "type": "object",
                            "description": "Context data models participating in retrieval, including session entities and profile entities/indexes.",
                            "additionalProperties": True,
                        },
                        "retrieval_strategy": {"type": "string"},
                        "session_scope_mode": {"type": "string"},
                        "cross_session_enabled": {"type": "boolean"},
                        "cross_session_budget_tokens": {"type": "integer"},
                        "cross_session_remote_budget_tokens": {"type": "integer"},
                        "cross_session_computed_budget_tokens": {"type": "integer"},
                        "cross_session_budget_floor_tokens": {"type": "integer"},
                        "cross_session_budget_floor_applied": {"type": "boolean"},
                        "cross_session_budget_floor_status": {"type": "string"},
                        "cross_session_max_sessions": {"type": "integer"},
                        "cross_session_max_candidates": {"type": "integer"},
                        "shared_context_enabled": {"type": "boolean"},
                        "selected_ref_flow": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Ordered retrieval layer flow used when packing selected refs.",
                        },
                    },
                    "additionalProperties": True,
                },
                "async_pipeline_readiness": {
                    "type": "object",
                    "description": (
                        "Async extraction/summary/compression/embedding freshness state. If ready_for_retrieval is false, "
                        "freshness_warnings and remaining_stages explain stale profile summaries or pending follow-up work. "
                        "Pending source/layer lineage buckets are debug-only and omitted from default ContextPack output."
                    ),
                    "properties": {
                        "task_count": {"type": "integer"},
                        "ready_for_retrieval": {"type": "boolean"},
                        "remaining_stages": {"type": "array", "items": {"type": "string"}},
                        "remaining_stage_counts": {"type": "object", "additionalProperties": True},
                        "pending_memory_selection_policies": {"type": "object", "additionalProperties": True},
                        "freshness_warnings": {"type": "array", "items": {"type": "string"}},
                    },
                    "additionalProperties": True,
                },
                "memory_layer_budget": {
                    "type": "object",
                    "description": "Selected ref/token budget grouped by public memory dimensions such as memory scope, session continuity, ref type, and entity type. Source role/hook/codex lineage buckets are debug-only.",
                    "additionalProperties": True,
                },
                "dropped_memory_layer_budget": {
                    "type": "object",
                    "description": "Dropped ref/token budget grouped by memory layer dimensions, including cross-session/profile pressure and shadowed profile refs.",
                    "additionalProperties": True,
                },
                "memory_layer_pressure": {
                    "type": "object",
                    "description": "Budget pressure summary for selected and dropped memory layers, including profile/cross-session/assistant/tool pressure flags.",
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
                "extraction_phase": EXTRACTION_PHASE_SCHEMA,
                "final_session_boundary": {
                    "type": "boolean",
                    "description": "True when this batch represents the final semantic boundary for its session/task; false for threshold/idle provisional checkpoints.",
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
    {
        "name": "matrixark_auth_signup",
        "description": "Production signup: create account, tenant, user, first scoped API key, and audit record. In enforced mode call from a trusted gateway or an admin API key.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "trusted_gateway": {"type": "boolean", "default": False},
                "provider": {"type": "string", "description": "local, google, github, okta, azure_ad, or oidc."},
                "external_user_id": {"type": "string"},
                "email": {"type": "string"},
                "account_id": {"type": "string"},
                "account_name": {"type": "string"},
                "tenant_id": {"type": "string"},
                "tenant_name": {"type": "string"},
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "first_key_scopes": {"type": "array", "items": {"type": "string"}},
                "first_key_role": {"type": "string", "default": "owner"},
                "key_display_name": {"type": "string"},
                "allowed_user_ids": {"type": "array", "items": {"type": "string"}},
                "allowed_session_ids": {"type": "array", "items": {"type": "string"}},
                "allow_all_users": {"type": "boolean", "default": False},
                "expires_at_ms": {"type": "integer"},
                "password": {"type": "string", "description": "Optional password for email/password login; stored only as a salted PBKDF2-SHA256 hash. Omit for SSO-only users."},
                "key_prefix": {"type": "string", "default": "mk_live"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_auth_sso_callback",
        "description": "Trusted OAuth/OIDC gateway callback for Google/Gmail, GitHub, Okta, and Azure AD. MatrixArk stores mapped identity metadata only, never raw OAuth tokens.",
        "inputSchema": {
            "type": "object",
            "required": ["provider"],
            "properties": {
                "provider": {"type": "string", "enum": ["google", "gmail", "github", "okta", "azure_ad", "azuread", "oidc"]},
                "id_token": {"type": "string", "description": "Google OIDC ID token. When provided for provider google/gmail without trusted_gateway, MatrixArk verifies RS256 + claims against Google's JWKS in process and never stores it."},
                "google_client_id": {"type": "string", "description": "Google OAuth client id used as the expected token audience; falls back to the MATRIXARK_GOOGLE_CLIENT_ID environment variable."},
                "external_user_id": {"type": "string", "description": "Stable IdP subject, such as OIDC sub or GitHub id."},
                "email": {"type": "string"},
                "matrixark_user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "id_token_verified": {"type": "boolean", "default": False},
                "trusted_gateway": {"type": "boolean", "default": False},
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_auth_sso_login",
        "description": "Map verified Google/Okta/Azure AD/OIDC login claims into a MatrixArk user scope. In enforced mode the gateway must pass id_token_verified=true or trusted_gateway=true.",
        "inputSchema": {
            "type": "object",
            "required": ["provider"],
            "properties": {
                "provider": {"type": "string", "description": "google, okta, azure_ad, github, or another trusted IdP."},
                "external_user_id": {"type": "string", "description": "Stable IdP subject. For Google this is the OIDC sub claim when available."},
                "email": {"type": "string", "description": "Email claim, e.g. a Gmail or Google Workspace address."},
                "matrixark_user_id": {"type": "string", "description": "Optional explicit MatrixArk user id; otherwise derived from provider subject."},
                "display_name": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "id_token_verified": {"type": "boolean", "default": False, "description": "Set by the trusted portal/gateway after OAuth/OIDC token validation."},
                "trusted_gateway": {"type": "boolean", "default": False},
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_auth_login",
        "description": "Email/password login for users who registered without SSO. Verifies a salted PBKDF2-SHA256 hash; MatrixArk never stores or logs the plaintext password. Gmail/Google and GitHub users should use matrixark_auth_sso_callback instead.",
        "inputSchema": {
            "type": "object",
            "required": ["password"],
            "properties": {
                "email": {"type": "string", "description": "Registered email, e.g. a Gmail address used at signup."},
                "password": {"type": "string", "description": "Plaintext password, verified against a salted PBKDF2-SHA256 hash and never stored."},
                "user_id": {"type": "string", "description": "Optional explicit MatrixArk user id; otherwise resolved from the email."},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "provider": {"type": "string", "default": "password"},
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_management_portal",
        "description": "Return one backend portal payload for registration, API-key management, ingestion history, topology, metrics, and audit.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "page_size": {"type": "integer", "default": 10, "minimum": 1, "maximum": 50},
                "page_token": {"type": ["integer", "string"], "default": 0, "description": "Offset for live paged portal tables."},
                "include_revoked": {"type": "boolean", "default": False},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_account",
        "description": "Create a MatrixArk account and default tenant.",
        "inputSchema": {
            "type": "object",
            "properties": ADMIN_ACCOUNT_PROPERTIES,
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_update_account",
        "description": "Update account or tenant metadata and active/disabled status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "account_status": {"type": "string", "enum": ["active", "disabled"]},
                "tenant_status": {"type": "string", "enum": ["active", "disabled"]},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_accounts",
        "description": "List account and tenant metadata visible to the caller.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_user",
        "description": "Create or register a MatrixArk user under an account/tenant.",
        "inputSchema": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "status": {"type": "string", "enum": ["active", "disabled"], "default": "active"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_update_user",
        "description": "Update, enable, or disable a MatrixArk user under an account/tenant.",
        "inputSchema": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "status": {"type": "string", "enum": ["active", "disabled"]},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_users",
        "description": "List MatrixArk users for an account/tenant without exposing context data.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "status": {"type": "string", "enum": ["active", "disabled"]},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_api_key",
        "description": "Create a MatrixArk API key for an account/tenant. The raw key is returned once.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowed scopes such as context:ingest, context:retrieve, admin:api_key.",
                },
                "role": {"type": "string", "default": "service"},
                "display_name": {"type": "string"},
                "allowed_user_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional user allow-list. Empty means any user in the key tenant.",
                },
                "allowed_session_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional session allow-list. Empty means any session in the key tenant.",
                },
                "expires_at_ms": {
                    "type": "integer",
                    "description": "Optional future unix timestamp in milliseconds when this key expires.",
                },
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_apply_api_key",
        "description": "One-call local agent onboarding: create or reuse account, agent-derived tenant, local user, and return a scoped MatrixArk API key.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "account_id": {"type": "string", "description": "Optional account/customer id. Defaults to acct_local in local mode."},
                "tenant_id": {"type": "string", "description": "Optional tenant/workspace id. Defaults to tenant_<agent_name> in local mode."},
                "agent_name": {"type": "string", "description": "Agent name used for the local tenant, e.g. codex, claude, cursor."},
                "user_id": {"type": "string", "description": "Optional MatrixArk user id. Defaults to the local OS account."},
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "display_name": {"type": "string", "description": "Display name for the local MatrixArk user."},
                "external_subject": {"type": "string", "description": "Optional external subject such as local:<user>, okta:<id>, google:<id>."},
                "key_display_name": {"type": "string"},
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowed scopes for the new key. Defaults to context ingest/retrieve/feedback/replay plus resource and skill read.",
                },
                "role": {"type": "string", "default": "local_agent"},
                "allowed_user_ids": {"type": "array", "items": {"type": "string"}},
                "allowed_session_ids": {"type": "array", "items": {"type": "string"}},
                "allow_all_users": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, do not restrict the key to the derived local user.",
                },
                "expires_at_ms": {"type": "integer"},
                "key_prefix": {"type": "string", "default": "mk_local"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_api_keys",
        "description": "List MatrixArk API key metadata for an account/tenant. Raw keys and hashes are never returned.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "include_revoked": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_rotate_api_key",
        "description": "Revoke an active MatrixArk API key and create a replacement with the same scopes.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "api_key_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_revoke_api_key",
        "description": "Revoke a MatrixArk API key.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {"api_key": API_KEY_SCHEMA, "api_key_id": {"type": "string"}, "scope": SCOPE_SCHEMA},
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_map_sso_user",
        "description": "Map an external Okta/Google/Azure AD user id to a MatrixArk user id.",
        "inputSchema": {
            "type": "object",
            "required": ["provider", "external_user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "provider": {"type": "string", "description": "okta, google, azure_ad, or another IdP name."},
                "external_user_id": {"type": "string"},
                "matrixark_user_id": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_audit",
        "description": "List MatrixArk access-management audit records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_forget",
        "description": "Delete ALL memory for a scope (mem0 delete_all). subject = scope.user_id; requires confirm == scope.user_id (exact match).",
        "inputSchema": {
            "type": "object",
            "required": ["confirm"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "confirm": {"type": "string", "description": "Must equal scope.user_id (exact match, no wildcard)."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_delete",
        "description": "Delete a single memory by id/hash (mem0 delete). memory_id is the event_id_hash returned by ingest.",
        "inputSchema": {
            "type": "object",
            "required": ["memory_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "memory_id": {"type": "string", "description": "The memory id (event_id_hash) to delete."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_get_all",
        "description": "List a scope's active memories (mem0 get_all). Excludes forgotten/deleted records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "description": "Optional cap on the number of memories returned."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_get_resource_content",
        "description": "Return one resource's or skill's stored text, in order and a page at a time (chunk_offset / chunk_limit / max_chars).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "resource_hash": {"type": "integer", "description": "The resource to read (alias: skill_hash)."},
                "skill_hash": {"type": "integer", "description": "The skill to read (alias: resource_hash)."},
                "chunk_offset": {"type": "integer", "description": "First chunk to return; use next_chunk_offset to page."},
                "chunk_limit": {"type": "integer", "description": "Maximum chunks in this response."},
                "max_chars": {"type": "integer", "description": "Maximum characters in this response."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_list_users",
        "description": "List the users/agents/runs that hold memories (mem0 users). Excludes subjects whose memories were all forgotten or expired.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "limit": {"type": "integer", "description": "Optional cap on the number of subjects returned."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_reset",
        "description": "Wipe ALL memory for the caller's tenant (mem0 reset). Guarded by confirm == tenant_id or the literal 'RESET'.",
        "inputSchema": {
            "type": "object",
            "required": ["confirm"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "confirm": {"type": "string", "description": "Must equal the resolved tenant_id or the literal 'RESET'."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_get_memory",
        "description": "Fetch a single memory by id (mem0 get). memory_id is the event_id_hash returned by ingest; returns {found, memory, text, metadata, derived}.",
        "inputSchema": {
            "type": "object",
            "required": ["memory_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "memory_id": {"type": "string", "description": "The memory id (event_id_hash) to fetch."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_get_memory_by_key",
        "description": "Recall the single current LIVE keyed value for an identity_key in a scope (PurchaseMemory keyed-upsert). Returns the highest-truth-rank surviving record.",
        "inputSchema": {
            "type": "object",
            "required": ["identity_key"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "identity_key": {"type": "string", "description": "The logical identity of the fact to recall (e.g. 'user.email', 'subscription.plan')."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_update_memory",
        "description": "Update a memory's content (mem0 update). Supersede: ingests the new text in the same scope and tombstones the old id.",
        "inputSchema": {
            "type": "object",
            "required": ["memory_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "memory_id": {"type": "string", "description": "The memory id (event_id_hash) to update."},
                "data": {"type": "string", "description": "The new memory content (alias: text)."},
                "text": {"type": "string", "description": "The new memory content (alias: data)."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_memory_feedback",
        "description": "Rate an existing memory (mem0 feedback). The rating is stored against the memory, not as a memory of its own -- it does not appear in get_all or search. Read it back with matrixark_memory_history.",
        "inputSchema": {
            "type": "object",
            "required": ["memory_id", "feedback"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "memory_id": {"type": "string", "description": "The memory id (event_id_hash) to rate."},
                "feedback": {
                    "type": "string",
                    "enum": ["POSITIVE", "NEGATIVE", "VERY_NEGATIVE"],
                    "description": "The rating. An unrecognised value is refused rather than stored.",
                },
                "feedback_reason": {"type": "string", "description": "Optional free-text reason for the rating."},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_memory_history",
        "description": "Return the ordered change history for a memory id (mem0 history): ingest -> update/supersede -> delete, with timestamps.",
        "inputSchema": {
            "type": "object",
            "required": ["memory_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "memory_id": {"type": "string", "description": "The memory id (event_id_hash) whose history to return."},
            },
            "additionalProperties": True,
        },
    },
]
