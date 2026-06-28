#!/usr/bin/env python3
"""Validate the committed Rust-native SDK schema and documentation."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "temporalstore" / "v1" / "temporalstore.proto"
DOC = ROOT / "docs" / "client_sdk_contract.md"
BUILD_RS = ROOT / "crates" / "temporalstore-rust" / "build.rs"
SDK_RS = ROOT / "crates" / "temporalstore-rust" / "src" / "sdk.rs"

REQUIRED_RPCS = [
    "Execute",
    "BatchExecute",
    "OpenTable",
    "SyncTopology",
    "GetClientPreflight",
]

REQUIRED_DATA_NODE_RPCS = [
    "ExecuteStream",
    "LifecycleCallbacks",
    "WatchJobStatus",
]

REQUIRED_PROXY_RPCS = [
    "ProxyExecuteStream",
    "RouteCallbacks",
    "WatchProxyPreflight",
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
    "DataNodeStreamRequest",
    "DataNodeStreamEvent",
    "DataNodeJobControl",
    "DataNodeLifecycleCallback",
    "DataNodeLifecycleAck",
    "DataNodeJobStatusRequest",
    "DataNodeJobStatusEvent",
    "ProxyStreamRequest",
    "ProxyStreamEvent",
    "ProxyRouteCallback",
    "ProxyRouteAck",
    "ProxyPreflightWatchRequest",
    "ProxyPreflightEvent",
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
    if not BUILD_RS.exists():
        fail(f"missing Rust SDK build script {BUILD_RS}")
    if not SDK_RS.exists():
        fail(f"missing exported Rust SDK module {SDK_RS}")

    proto = PROTO.read_text(encoding="utf-8")
    doc = DOC.read_text(encoding="utf-8")
    build_rs = BUILD_RS.read_text(encoding="utf-8")
    sdk_rs = SDK_RS.read_text(encoding="utf-8")

    if 'syntax = "proto3";' not in proto:
        fail("schema must use proto3")
    if "package temporalstore.v1;" not in proto:
        fail("schema must use package temporalstore.v1")
    if "service TemporalStoreService" not in proto:
        fail("schema must define TemporalStoreService")
    if "service DataNodeService" not in proto:
        fail("schema must define DataNodeService")
    if "service ProxyService" not in proto:
        fail("schema must define ProxyService")

    rpc_decl = r"\b" + r"rpc"
    for rpc in REQUIRED_RPCS:
        if not re.search(rf"{rpc_decl}\s+{rpc}\s*\(", proto):
            fail(f"missing rpc {rpc}")
        if f"`{rpc}`" not in doc:
            fail(f"doc must describe rpc {rpc}")
    for rpc in REQUIRED_DATA_NODE_RPCS:
        if not re.search(rf"{rpc_decl}\s+{rpc}\s*\(", proto):
            fail(f"missing data-node rpc {rpc}")
    for rpc in REQUIRED_PROXY_RPCS:
        if not re.search(rf"{rpc_decl}\s+{rpc}\s*\(", proto):
            fail(f"missing proxy rpc {rpc}")

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
        "RESP replacement is covered",
        "tonic replacement is covered",
        "typed client migration is covered",
        "topology sync and route invalidation",
        "C++ partition-set/member/version route-cache tests",
        "Rust client route cache models the C++ partition-set hierarchy",
        "deployment placement and routing hooks",
        "location-affine secondary reads",
        "primary-only write routing",
        "exponential backoff with deterministic jitter",
        "per-command partial-failure preservation",
        "timeout-budget propagation tests",
        "retry budgets are covered",
        "admission policy is covered",
        "migration docs are validated",
        "supported command families",
        "shared C++/Rust corpus",
        "Rust-native HTTP/JSON, RESP, and tonic migration contract",
        "client/proxy readiness gate treats the Rust-native replacement contract",
    ]:
        if phrase not in doc:
            fail(f"doc must mention {phrase!r}")

    for phrase in [
        "tonic_build::configure()",
        ".build_client(true)",
        ".build_server(true)",
        "temporalstore.proto",
    ]:
        if phrase not in build_rs:
            fail(f"build.rs must mention {phrase!r}")

    for phrase in [
        'tonic::include_proto!("temporalstore.v1")',
        "TemporalStoreServiceClient",
        "TemporalStoreService",
        "TemporalStoreTonicAdapter",
        "open_table_sdk",
        "sync_topology_sdk",
        "client_preflight_sdk",
        "sdk_command_to_types",
        "types_command_response_to_sdk",
    ]:
        if phrase not in sdk_rs:
            fail(f"sdk.rs must mention {phrase!r}")

    client_rs = (ROOT / "crates" / "temporalstore-rust" / "src" / "client.rs").read_text(
        encoding="utf-8"
    )
    proxy_rs = (ROOT / "crates" / "temporalstore-rust" / "src" / "proxy.rs").read_text(
        encoding="utf-8"
    )
    readiness_rs = (
        ROOT / "crates" / "temporalstore-rust" / "src" / "readiness.rs"
    ).read_text(encoding="utf-8")

    for phrase in [
        "http_json_contract_tested",
        "resp_contract_tested",
        "tonic_contract_tested",
        "typed_table_client_tested",
        "topology_sync_tested",
        "retry_budget_tested",
        "supported_command_families",
        "migration_docs_ready",
    ]:
        if phrase not in client_rs:
            fail(f"client.rs must mention {phrase!r}")
        if phrase not in readiness_rs:
            fail(f"readiness.rs must consume client contract field {phrase!r}")

    for phrase in [
        "resp_migration_ready",
        "typed_client_delegation_tested",
        "route_invalidation_tested",
        "admission_policy_tested",
        "command_aliases_tested",
        "migration_docs_ready",
    ]:
        if phrase not in proxy_rs:
            fail(f"proxy.rs must mention {phrase!r}")
        if phrase not in readiness_rs:
            fail(f"readiness.rs must consume proxy contract field {phrase!r}")

    print("sdk contract validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
