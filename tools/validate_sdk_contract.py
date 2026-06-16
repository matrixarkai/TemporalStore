#!/usr/bin/env python3
"""Validate the committed Rust-native SDK schema and documentation."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "temporalstore" / "v1" / "temporalstore.proto"
DOC = ROOT / "docs" / "client_sdk_contract.md"

REQUIRED_RPCS = [
    "Execute",
    "BatchExecute",
    "OpenTable",
    "SyncTopology",
    "GetClientPreflight",
]

REQUIRED_MESSAGES = [
    "Status",
    "ExecuteRequest",
    "ExecuteResponse",
    "BatchExecuteRequest",
    "BatchExecuteResponse",
    "OpenTableRequest",
    "OpenTableResponse",
    "SyncTopologyRequest",
    "SyncTopologyResponse",
    "ClientPreflightRequest",
    "ClientPreflightResponse",
    "TableTopology",
    "ShardTopology",
    "ServerEndpoint",
    "Command",
    "CommandResponse",
    "FeaturePoint",
    "SequenceFeatureRow",
    "ContextNode",
]

REQUIRED_COMMANDS = [
    "string_set",
    "string_get",
    "hash_multi_set",
    "hash_multi_get",
    "set_add",
    "set_members",
    "feature_append",
    "feature_query",
    "sequence_append",
    "sequence_query",
    "ips_add",
    "ips_query",
    "risk_increment",
    "risk_query",
    "context_node_upsert",
    "context_node_get",
    "common_expire",
    "common_exists",
]


def fail(message: str) -> None:
    raise SystemExit(f"sdk contract validation failed: {message}")


def main() -> int:
    if not PROTO.exists():
        fail(f"missing schema {PROTO}")
    if not DOC.exists():
        fail(f"missing doc {DOC}")

    proto = PROTO.read_text(encoding="utf-8")
    doc = DOC.read_text(encoding="utf-8")

    if 'syntax = "proto3";' not in proto:
        fail("schema must use proto3")
    if "package temporalstore.v1;" not in proto:
        fail("schema must use package temporalstore.v1")
    if "service TemporalStoreService" not in proto:
        fail("schema must define TemporalStoreService")

    for rpc in REQUIRED_RPCS:
        if not re.search(rf"\brpc\s+{rpc}\s*\(", proto):
            fail(f"missing rpc {rpc}")
        if f"`{rpc}`" not in doc:
            fail(f"doc must describe rpc {rpc}")

    for message in REQUIRED_MESSAGES:
        if not re.search(rf"\bmessage\s+{message}\b", proto):
            fail(f"missing message {message}")

    command_block_match = re.search(r"message\s+Command\s*\{(?P<body>.*?)\n\}", proto, re.S)
    if not command_block_match:
        fail("missing Command body")
    command_block = command_block_match.group("body")
    if "oneof kind" not in command_block:
        fail("Command must use oneof kind for versioned command dispatch")
    for command in REQUIRED_COMMANDS:
        if not re.search(rf"\b{command}\s*=", command_block):
            fail(f"missing command variant {command}")

    for phrase in [
        "generated tonic/prost",
        "HTTP/JSON",
        "shared C++/Rust corpus",
        "must continue to report the client as blocked",
    ]:
        if phrase not in doc:
            fail(f"doc must mention {phrase!r}")

    print("sdk contract validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
