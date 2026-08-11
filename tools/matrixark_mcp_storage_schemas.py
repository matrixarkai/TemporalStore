#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Storage policy MCP schemas for MatrixArk."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


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
        "write_mode": {
            "type": "string",
            "enum": ["default", "async", "sync"],
            "description": "Per-message write behavior. async lets the native backend acknowledge after memory append/background oplog work; sync waits for the durable route.",
        },
        "durability": {
            "type": "string",
            "enum": ["default", "async", "sync"],
            "description": "Durability shorthand. Defaults to async for highest write/read QPS; set sync only for records that must be durable before ack.",
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
        "read_preference": {
            "type": "string",
            "enum": ["default", "primary", "replica", "replica_preferred"],
            "description": "Requested serving read preference. Async context ingest defaults to replica_preferred so read-heavy paths can use replicas.",
        },
    },
    "additionalProperties": True,
}

RECORD_STORAGE_OPTIONS_SCHEMA: Json = {
    "type": "object",
    "description": "Optional per-record TemporalStore storage policy overrides. Each record kind inherits storage_options, and unspecified records default to async durability with replica-preferred reads.",
    "properties": {
        "raw_ingestion": STORAGE_OPTIONS_SCHEMA,
        "context_event": STORAGE_OPTIONS_SCHEMA,
        "session_buffer": STORAGE_OPTIONS_SCHEMA,
        "entity": STORAGE_OPTIONS_SCHEMA,
        "summary": STORAGE_OPTIONS_SCHEMA,
        "embedding": STORAGE_OPTIONS_SCHEMA,
        "index": STORAGE_OPTIONS_SCHEMA,
        "resource": STORAGE_OPTIONS_SCHEMA,
        "resource_chunk": STORAGE_OPTIONS_SCHEMA,
        "skill": STORAGE_OPTIONS_SCHEMA,
        "compression": STORAGE_OPTIONS_SCHEMA,
        "feedback": STORAGE_OPTIONS_SCHEMA,
        "debug": STORAGE_OPTIONS_SCHEMA,
    },
    "additionalProperties": STORAGE_OPTIONS_SCHEMA,
}

PART_STORAGE_OPTIONS_SCHEMA: Json = RECORD_STORAGE_OPTIONS_SCHEMA
