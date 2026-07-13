#!/usr/bin/env python3
"""Validate the trimmed TemporalStore open-source surface.

This intentionally checks source policy rather than doing a full release build:
the goal is to catch accidental re-exposure of internal modules/models and
non-basic Redis commands in the public build surface.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def read(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


def require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def cxx_guarded_symbol(text: str, symbol: str) -> bool:
    pattern = re.compile(
        r"#ifndef\s+BCACHE2_OPEN_SOURCE_SURFACE(?:(?!#endif).)*"
        + re.escape(symbol)
        + r"(?:(?!#endif).)*#endif",
        re.DOTALL,
    )
    return bool(pattern.search(text))


def rust_allowlist_body(text: str) -> str:
    match = re.search(
        r"fn open_source_redis_command_allowed\(command: &str\) -> bool \{(?P<body>.*?)\n\}",
        text,
        re.DOTALL,
    )
    if not match:
        return ""
    return match.group("body")


def main() -> int:
    failures: list[str] = []

    manifest = json.loads(read("compat/redis_open_source_surface_manifest.json"))
    manifest_cxx_commands = manifest.get("cxx_commands", [])
    manifest_blocked_commands = set(manifest.get("blocked_commands", []))
    manifest_rust_extra_commands = set(manifest.get("rust_extra_commands", []))
    manifest_rust_normal_helpers = set(manifest.get("rust_normal_helpers", []))

    require(
        manifest.get("schema") == "temporalstore_open_source_redis_surface_v1",
        "Redis open-source surface manifest must use the v1 schema",
        failures,
    )
    require(
        manifest.get("cxx_command_count") == 47 and len(manifest_cxx_commands) == 47,
        "Redis open-source surface manifest must declare exactly 47 C++ commands",
        failures,
    )

    root_cmake = read("CMakeLists.txt")
    extension_cmake = read("src/extension/CMakeLists.txt")
    set_cmake = read("src/extension/set/CMakeLists.txt")
    model_cmake = read("src/model/CMakeLists.txt")
    model_manager = read("src/model/model_manager.cc")
    cxx_redis = read("src/server/redis_command_handler.cc")
    cxx_redis_service = read("src/server/redis_service.cc")
    rust_redis = read("crates/temporalstore-rust/src/redis.rs")
    redis_compat_smoke = read("tools/run_redis_compat_smoke_ubuntu22.sh")
    redis_live_smoke = read("tools/run_redis_live_storage_smoke_ubuntu22.sh")
    redis_production_gate = read("tools/run_redis_production_gate_ubuntu22.sh")
    redis_docs = read("docs/redis_compatibility_matrix.md")
    open_source_surface = read("docs/open_source_surface.md")
    cpp_api_parity_docs = read("docs/cpp_temporalstore_api_parity.md")
    temporal_adapters = read("tools/matrixark_mcp_temporal_adapters.py")
    raw_storage_contract = read("tools/matrixark_raw_message_storage_contract.py")
    dual_write_benchmark = read("tools/matrixark_dual_write_ingestion_benchmark.py")
    context_backfill = read("tools/matrixark_context_backfill.py")
    matrixobject_docs = read("docs/matrixobjectstore_design_extraction_and_readiness.md")
    context_resource = read("crates/temporalstore-rust/src/context_workflow/resource.rs")
    context_resource_tests = read("crates/temporalstore-rust/src/context_workflow/tests.rs")
    object_store_code = read("crates/temporalstore-snapshot/src/object_store.rs")
    object_store_docs = read("docs/object_store_backends.md")
    cxx_object_store_backend = read("src/stream/store/object_store_backend.h")
    cxx_object_store_guardrail = read("src/stream/test/object_store_guardrail_test.cc")
    storage_modes_harness = read("crates/temporalstore-rust/src/bin/storage_modes_harness.rs")

    require(
        "option(BCACHE2_OPEN_SOURCE_SURFACE" in root_cmake,
        "root CMake must define BCACHE2_OPEN_SOURCE_SURFACE",
        failures,
    )
    require(
        "add_compile_definitions(BCACHE2_OPEN_SOURCE_SURFACE)" in root_cmake,
        "root CMake must publish BCACHE2_OPEN_SOURCE_SURFACE to C++ code",
        failures,
    )

    for module in ("ips", "risk", "temporal_aggregate"):
        require(
            re.search(
                r"if\s*\(NOT BCACHE2_OPEN_SOURCE_SURFACE\)(?:(?!endif).)*"
                + re.escape(f"add_subdirectory({module})"),
                extension_cmake,
                re.DOTALL,
            )
            is not None,
            f"extension module {module} must be gated out of open-source builds",
            failures,
        )
    require(
        "add_subdirectory(set)" in extension_cmake,
        "set protobuf compatibility helper must remain available for C++ Redis compilation",
        failures,
    )
    require(
        re.search(
            r"if\s*\(NOT BCACHE2_OPEN_SOURCE_SURFACE\)(?:(?!endif).)*add_module\(set_module set_proto_lib\)",
            set_cmake,
            re.DOTALL,
        )
        is not None,
        "set module registration must be gated out of open-source builds",
        failures,
    )

    require(
        'set(SRCS\n        flags.cc\n        model_context.cc\n        model_manager.cc)' in model_cmake,
        "open-source model compile list must be trimmed to shared model sources",
        failures,
    )
    for symbol in ("REGISTER_MODEL(TimeSeriesModel", "REGISTER_MODEL(IpsModel", "REGISTER_MODEL(RiskHashModel"):
        require(
            cxx_guarded_symbol(model_manager, symbol),
            f"{symbol} must be disabled under BCACHE2_OPEN_SOURCE_SURFACE",
            failures,
        )
    for symbol in ("REGISTER_MODEL(FeatureModel", "REGISTER_MODEL(HashModel", "REGISTER_MODEL(CPCModel"):
        require(symbol in model_manager, f"{symbol} must remain available", failures)

    require(
        "IsOpenSourceRedisCommandAllowed" in cxx_redis,
        "C++ Redis handler must reject non-basic commands in open-source builds",
        failures,
    )
    require(
        "OpenSourceRedisCommands" in cxx_redis,
        "C++ Redis handler must expose one canonical trimmed open-source command descriptor table",
        failures,
    )
    require(
        "OpenSourceRedisCommandCount" in cxx_redis
        and "OpenSourceRedisCommands().size()" in cxx_redis,
        "C++ COMMAND COUNT must derive from the trimmed descriptor table",
        failures,
    )
    require(
        '(*c->reply)[i].SetString(commands[i].name)' in cxx_redis,
        "C++ COMMAND must advertise the trimmed open-source command names",
        failures,
    )
    cxx_descriptor_block = re.search(
        r"OpenSourceRedisCommands\(\) \{(?P<body>.*?)\n\}",
        cxx_redis,
        re.S,
    )
    require(cxx_descriptor_block is not None, "C++ trimmed Redis descriptor table must be discoverable", failures)
    cxx_descriptor_body = cxx_descriptor_block.group("body") if cxx_descriptor_block else ""
    require(
        cxx_descriptor_body.count("RedisCommand::CmdType::") == 47,
        "C++ trimmed Redis descriptor table must contain exactly 47 commands",
        failures,
    )
    for command in ("GET", "SET", "HSET", "HMSET", "HGET", "HSCAN", "INCR", "DECR", "DECRBY", "HINCRBY", "CLIENT", "QUIT"):
        require(
            f'"{command}"' in cxx_descriptor_body,
            f"C++ trimmed Redis descriptor table must advertise {command}",
            failures,
        )
    for command in ("SADD", "LPUSH", "ZADD", "SCAN", "KEYS", "CONFIG", "DBSIZE", "PARTITION", "HINCRBYFLOAT"):
        require(
            f'"{command}"' not in cxx_descriptor_body,
            f"C++ trimmed Redis descriptor table must not advertise {command}",
            failures,
        )

    cxx_advertised_commands = re.findall(
        r'\{RedisCommand::CmdType::k[A-Za-z0-9]+, "([A-Z0-9]+)"',
        cxx_descriptor_body,
    )
    require(
        cxx_advertised_commands == manifest_cxx_commands,
        "C++ trimmed Redis descriptor table must match compat/redis_open_source_surface_manifest.json",
        failures,
    )
    for command in manifest_blocked_commands:
        require(
            command not in cxx_advertised_commands,
            f"Redis surface manifest blocked command {command} must not be advertised by C++",
            failures,
        )

    cxx_registered_commands = {
        name.upper(): handler
        for name, handler in re.findall(
            r'RegisterCommand\("([a-z0-9]+)",\s*'
            r'RedisCommand::CmdType::k[A-Za-z0-9]+,\s*'
            r'-?\d+,\s*"[^"]*",\s*-?\d+,\s*-?\d+,\s*-?\d+,\s*'
            r'&RedisCommandHandler::([A-Za-z0-9_]+)',
            cxx_redis_service,
            re.S,
        )
    }
    for command in cxx_advertised_commands:
        require(
            command in cxx_registered_commands,
            f"C++ advertised Redis command {command} must be registered in RedisServiceImpl::InitCommands",
            failures,
        )
        require(
            cxx_registered_commands.get(command) != "Unsupported",
            f"C++ advertised Redis command {command} must not be wired to Unsupported",
            failures,
        )

    for denied in (
        "kBgSave",
        "kConfig",
        "kPartition",
        "kPSlotAdd",
        "kPSlotDel",
        "kPSlotInfo",
        "kPSlotMigrate",
        "kPSlotCountKeysInSlot",
        "kPSlotGetKeysInSlot",
        "kPSlotSetState",
        "kPSlotImport",
        "kPSlotSetVersion",
        "kPSlotHashKey",
        "kSlaveOf",
        "kPauseWrite",
        "kFlushAll",
        "kSAdd",
        "kSRem",
        "kSMembers",
        "kSCard",
        "kSIsMember",
        "kSMIsMember",
        "kSPop",
        "kSRandMember",
        "kSInter",
        "kSUnion",
        "kSDiff",
        "kLPush",
        "kRPush",
        "kLPushX",
        "kRPushX",
        "kLPop",
        "kRPop",
        "kLLen",
        "kLIndex",
        "kLRange",
        "kLTrim",
        "kLSet",
        "kLRem",
        "kZAdd",
        "kZIncrBy",
        "kZRem",
        "kZPopMin",
        "kZPopMax",
        "kZRemRangeByScore",
        "kZRemRangeByRank",
        "kZCard",
        "kZScore",
        "kZRank",
        "kZRevRank",
        "kZRange",
        "kZRevRange",
        "kZRangeByScore",
        "kZRevRangeByScore",
        "kZCount",
        "kZMScore",
    ):
        require(
            f"case RedisCommand::CmdType::{denied}" not in cxx_redis,
            f"C++ open-source Redis allowlist must not include {denied}",
            failures,
        )

    body = rust_allowlist_body(rust_redis)
    require(body, "Rust Redis open-source allowlist must exist", failures)
    for allowed in (
        "HSET",
        "HGET",
        "HGETALL",
        "GET",
        "SET",
        "QUIT",
        "CLIENT",
    ):
        require(f'"{allowed}"' in body, f"Rust allowlist must keep {allowed}", failures)
    for allowed in sorted(manifest_rust_extra_commands):
        require(
            f'"{allowed}"' in body,
            f"Rust allowlist must keep manifest extra command {allowed}",
            failures,
        )
    for denied in sorted(manifest_blocked_commands):
        require(f'"{denied}"' not in body, f"Rust allowlist must not include manifest blocked command {denied}", failures)
    for denied in (
        "SADD",
        "SCARD",
        "SDIFF",
        "SINTER",
        "SISMEMBER",
        "SMEMBERS",
        "SMISMEMBER",
        "SMOVE",
        "SPOP",
        "SRANDMEMBER",
        "SREM",
        "SSCAN",
        "SUNION",
        "LINDEX",
        "LINSERT",
        "LLEN",
        "LMOVE",
        "LPOP",
        "LPOS",
        "LPUSH",
        "LRANGE",
        "LREM",
        "LSET",
        "LTRIM",
        "RPOP",
        "RPOPLPUSH",
        "RPUSH",
        "ZADD",
        "ZCARD",
        "ZCOUNT",
        "ZDIFF",
        "ZINCRBY",
        "ZINTER",
        "ZMSCORE",
        "ZPOPMAX",
        "ZPOPMIN",
        "ZRANDMEMBER",
        "ZRANGE",
        "ZRANGEBYSCORE",
        "ZRANK",
        "ZREM",
        "ZREMRANGEBYRANK",
        "ZREMRANGEBYSCORE",
        "ZREVRANGE",
        "ZREVRANK",
        "ZSCAN",
        "ZSCORE",
        "ZUNION",
        "IPSADD",
        "FADD",
        "RISKDEBUG",
        "PARTITION",
        "DBSIZE",
        "CONFIG",
        "BGSAVE",
        "KEYS",
        "RANDOMKEY",
        "RENAME",
        "RENAMENX",
        "SLAVEOF",
    ):
        require(f'"{denied}"' not in body, f"Rust allowlist must not include {denied}", failures)

    for metric in (
        "redis_surface:trimmed_open_source",
        "redis_surface_schema:temporalstore_open_source_redis_surface_v1",
        "redis_surface_cxx_command_count:47",
        "redis_surface_blocked_command_family_count:10",
        "total_commands_processed",
        "rejected_commands",
        "open_source_rejected_commands",
        "unsupported_commands",
    ):
        require(metric in rust_redis, f"Rust Redis INFO stats must expose {metric}", failures)
    require(
        "total_commands_processed:0" not in rust_redis,
        "Rust Redis INFO stats must not hardcode total_commands_processed:0",
        failures,
    )

    for script_name, script in (
        ("run_redis_compat_smoke_ubuntu22.sh", redis_compat_smoke),
        ("run_redis_live_storage_smoke_ubuntu22.sh", redis_live_smoke),
    ):
        require(
            'REDIS_COMPAT_SURFACE="${REDIS_COMPAT_SURFACE:-trimmed}"' in script,
            f"{script_name} must default to the trimmed Redis surface",
            failures,
        )
        require(
            'REDIS_COMPAT_SURFACE}" == "full"' in script,
            f"{script_name} must keep broad collection compatibility opt-in",
            failures,
        )

    require(
        "Open-source production builds do not claim generic Redis SET/LIST/ZSET compatibility" in redis_docs,
        "Redis docs must state that generic collection clones are not part of the open-source claim",
        failures,
    )
    require(
        "auth/ping/info/config" not in open_source_surface.lower(),
        "open-source surface overview must not advertise Redis CONFIG as public",
        failures,
    )
    require(
        "server-configuration or broad" in open_source_surface
        and "`CONFIG`" in open_source_surface
        and "`DBSIZE`" in open_source_surface
        and "broad `KEYS`" in open_source_surface
        and "`SADD`" in open_source_surface
        and "`LPUSH`" in open_source_surface
        and "`ZADD`" in open_source_surface,
        "open-source surface overview must explicitly exclude config, broad keyspace scans, and collection clones",
        failures,
    )
    require(
        "Narrow `HSCAN` is kept only as a single-hash" in open_source_surface
        and "broad `KEYS` /\n`SCAN`" in open_source_surface,
        "open-source surface overview must document narrow HSCAN without broad keyspace SCAN",
        failures,
    )
    require(
        "compat/redis_open_source_surface_manifest.json" in open_source_surface
        and "MatrixObject is a shared object-store backend below TemporalStore storage/backfill" in open_source_surface
        and "must not expand the public Redis API" in open_source_surface,
        "open-source surface overview must tie Redis API scope to the manifest and keep MatrixObject below the Redis layer",
        failures,
    )
    require(
        "this document tracks broad C++/Rust API-parity" in cpp_api_parity_docs
        and "not the open-source production Redis surface" in cpp_api_parity_docs
        and "redis_compatibility_matrix.md" in cpp_api_parity_docs
        and "Generic\nSET/LIST/ZSET clones" in cpp_api_parity_docs
        and "broad `KEYS`/`SCAN`" in cpp_api_parity_docs,
        "C++ API parity doc must distinguish broad corpus/private compatibility from the trimmed open-source Redis surface",
        failures,
    )
    require(
        "REDIS_COMPAT_SURFACE=trimmed" in redis_production_gate,
        "Redis production gate must force the trimmed compatibility surface",
        failures,
    )
    require(
        "REDIS_EXPECT_UNSUPPORTED_COLLECTIONS=1" in redis_production_gate,
        "Redis production gate must assert unsupported collection-clone commands",
        failures,
    )
    require(
        'BENCH_KEYSPACE="${BENCH_KEYSPACE}"' in redis_production_gate,
        "Redis production gate must pass benchmark keyspace into the live smoke",
        failures,
    )
    for benchmark_command in ("HSET", "HGET", "HINCRBY", "HINCRBYFLOAT", "INCR", "EXPIRE"):
        require(
            benchmark_command in redis_compat_smoke,
            f"Redis compatibility benchmark must cover {benchmark_command}",
            failures,
        )
        require(
            f"`{benchmark_command}`" in redis_docs,
            f"Redis docs must describe {benchmark_command} benchmark coverage",
            failures,
        )
    for artifact_name in (
        "redis-benchmark-hset.csv",
        "redis-benchmark-hget.csv",
        "redis-benchmark-hincrby.csv",
        "redis-benchmark-hincrbyfloat.csv",
        "redis-benchmark-incr.csv",
        "redis-benchmark-expire.csv",
        "redis-benchmark-summary.json",
    ):
        require(
            artifact_name in redis_compat_smoke,
            f"Redis benchmark must write {artifact_name}",
            failures,
        )
    for summary_field in (
        "temporalstore_trimmed_redis_benchmark_summary_v1",
        "redis_surface_schema",
        "redis_surface_manifest_sha256",
        "blocked_command_family_count",
        "requests_per_second_min",
        "requests_per_second_max",
        "requests_per_second_avg",
    ):
        require(
            summary_field in redis_compat_smoke,
            f"Redis benchmark summary must include {summary_field}",
            failures,
        )
    require(
        "`redis-benchmark-summary.json`" in redis_docs
        and "Redis surface schema and manifest hash" in redis_docs,
        "Redis docs must document the benchmark JSON summary artifact and Redis surface metadata",
        failures,
    )
    for stale_claim in ("- Set: `SADD`", "- List: `LPUSH`", "- ZSet: `ZADD`"):
        require(stale_claim not in redis_docs, f"Redis docs must not keep stale claim {stale_claim}", failures)
    for rust_helper in sorted(manifest_rust_normal_helpers):
        require(
            f'"{rust_helper}"' in body,
            f"Rust allowlist must keep documented normal helper {rust_helper}",
            failures,
        )
        require(
            f"`{rust_helper}`" in redis_docs,
            f"Redis docs must document Rust normal helper {rust_helper}",
            failures,
        )
    require(
        "COMMAND COUNT=47" in redis_docs,
        "Redis docs must state the narrower C++ open-source COMMAND COUNT including aliases and HSCAN",
        failures,
    )
    require(
        "kHScan" in cxx_redis and "&RedisCommandHandler::HScan" in cxx_redis_service,
        "C++ Redis service must wire narrow HSCAN through the hash handler",
        failures,
    )
    for metric in (
        "redis_surface:trimmed_open_source",
        "redis_surface_schema:temporalstore_open_source_redis_surface_v1",
        "redis_surface_cxx_command_count:",
        "redis_surface_blocked_command_family_count:10",
    ):
        require(metric in cxx_redis, f"C++ Redis INFO stats must expose {metric}", failures)
    require(
        "HSCAN" in redis_compat_smoke and "HSCAN rh 0 MATCH f* COUNT 8" in redis_live_smoke,
        "Redis smokes must cover narrow HSCAN while broad scans stay unsupported",
        failures,
    )
    require(
        'REDIS_EXPECT_HINCRBYFLOAT:-0' in redis_live_smoke
        and "SKIP hincrbyfloat" in redis_live_smoke
        and "REDIS_EXPECT_HINCRBYFLOAT=1" in redis_docs
        and "C++ Redis bridge must not claim it until a native handler is added" in redis_docs,
        "C++ live Redis smoke must keep HINCRBYFLOAT opt-in until the C++ bridge wires a native handler",
        failures,
    )
    require(
        'REDIS_EXPECT_HINCRBYFLOAT:-0' in redis_compat_smoke
        and 'SKIP hincrbyfloat' in redis_compat_smoke
        and 'SKIP redis_benchmark_hincrbyfloat' in redis_compat_smoke
        and 'hincrbyfloat_enabled' in redis_compat_smoke
        and 'Rust-only/opt-in bridge capability' in redis_docs,
        "shared Redis compatibility smoke/docs must not require HINCRBYFLOAT unless explicitly enabled",
        failures,
    )

    for symbol in (
        "OBJECT_STORE_PROVIDER_ALIASES",
        "GENERIC_OBJECT_STORE_OPERATIONS",
        "generic_object_store_contract",
        "object_store_contract",
        "raw_message_provider_name",
    ):
        require(symbol in raw_storage_contract, f"raw-message contract must expose {symbol}", failures)
    require(
        '"object_store_name": raw_message_provider_name(resolved)' in raw_storage_contract,
        "raw-message object_store_name must be derived from the resolved backend provider",
        failures,
    )
    for operation in (
        "put_atomic",
        "put_unique",
        "put_if_absent",
        "put_path_unique",
        "get_range",
        "get_to_path",
        "head",
        "list_page",
        "delete_objects",
        "delete_prefix",
        "copy_object",
        "capabilities",
        "topology",
    ):
        require(f'"{operation}"' in raw_storage_contract, f"generic object-store contract must require {operation}", failures)
    for capability in (
        "conditional_create",
        "paginated_list",
        "delete_capability",
        "bulk_delete",
        "byte_range_read",
        "opaque_object_validators",
        "object_version_ids",
        "split_services",
    ):
        require(
            f'"{capability}"' in raw_storage_contract,
            f"generic object-store contract must expose capability {capability}",
            failures,
        )
    require(
        '"delete": "delete_capability"' in raw_storage_contract,
        "generic object-store contract must keep delete as a compatibility alias only",
        failures,
    )
    require(
        '"objectstore": "matrixobject"' in raw_storage_contract
        and '"matrixobjectstore": "matrixobject"' in raw_storage_contract,
        "MatrixObject backend aliases must normalize to canonical matrixobject",
        failures,
    )
    require(
        'SUPPORTED_BACKENDS = {"temporalstore", "matrixkv", "s3", "matrixobject"}' in raw_storage_contract,
        "raw-message contract must advertise matrixobject as the canonical object backend",
        failures,
    )
    require(
        'RAW_BACKEND_CHOICES = ["temporalstore", "matrixkv", "s3", "matrixobject"]' in dual_write_benchmark,
        "dual-write benchmark must advertise matrixobject as the canonical raw backend",
        failures,
    )
    require(
        "'matrixobject', 'objectstore'" in context_backfill,
        "context backfill must accept canonical matrixobject plus legacy objectstore alias",
        failures,
    )
    require(
        '"matrixark_raw_ingestion_matrixobject_ref"' in temporal_adapters
        and '"matrixark_raw_ingestion_objectstore_ref"' not in temporal_adapters,
        "MCP adapter raw-ingestion path must use canonical MatrixObject naming",
        failures,
    )
    require(
        'run_storage_modes(options, store, "matrixobject").await' in storage_modes_harness,
        "MatrixObject storage mode reports must emit canonical matrixobject URI scheme",
        failures,
    )
    for shared_store_alias in (
        '"matrixobject"',
        '"matrix_object"',
        '"matrixobject_local_compat"',
        '"matrixobjectstore"',
        '"matrix_object_store"',
        '"matrixobjectstore_local_compat"',
    ):
        require(
            shared_store_alias in storage_modes_harness,
            f"MatrixObject storage mode harness must accept alias {shared_store_alias}",
            failures,
        )
    require(
        "MatrixObject` as the public product/API name" in matrixobject_docs,
        "MatrixObject docs must use the short public product/API name",
        failures,
    )
    require(
        "generic object-store adapter contract" in matrixobject_docs,
        "MatrixObject docs must describe the generic object-store adapter",
        failures,
    )
    require(
        "Selection should be by URI scheme plus reported capabilities" in matrixobject_docs,
        "MatrixObject docs must require provider-neutral URI/capability selection",
        failures,
    )
    require(
        "failing closed" in matrixobject_docs,
        "MatrixObject docs must require unlinked remote providers to fail closed",
        failures,
    )
    require(
        "opaque manifest `version_id`" in matrixobject_docs,
        "MatrixObject docs must require provider-neutral manifest version validators",
        failures,
    )
    require(
        "matrixobject://matrixark/resources/" in context_resource
        and "objectstore://matrixark/resources/" not in context_resource,
        "context resource object refs must use canonical matrixobject:// URIs",
        failures,
    )
    require(
        '"matrixobject".to_string()' in context_resource
        and 'Some("matrixobject")' in context_resource_tests,
        "context resource metadata must report canonical matrixobject storage backend",
        failures,
    )
    require(
        "fn version_id(&self) -> String" in object_store_code
        and 'format!("mo:{}:{checksum_prefix}", self.created_at_ms)' in object_store_code,
        "MatrixObject metadata must expose a manifest-derived opaque version_id",
        failures,
    )
    require(
        "object_version_ids: true" in object_store_code
        and "object_version_id: true" in object_store_code
        and "ObjectStoreCapabilities::matrixobject" in object_store_code,
        "MatrixObject capabilities must report canonical object_version_ids support",
        failures,
    )
    require(
        "pub struct CanonicalObjectStoreCapabilities" in object_store_code
        and "fn canonical_capabilities(&self) -> CanonicalObjectStoreCapabilities" in object_store_code
        and "canonical_object_store_capabilities_hide_compatibility_aliases" in object_store_code
        and "canonical_capabilities()" in object_store_docs,
        "Rust object-store integrations must expose a canonical capabilities view for MatrixObject/S3/local adapters",
        failures,
    )
    require(
        "struct CanonicalObjectStoreBackendCapabilities" in cxx_object_store_backend
        and "CanonicalObjectStoreBackendCapabilityReport" in cxx_object_store_backend
        and "CanonicalCapabilityReportsHideCompatibilityAliases" in cxx_object_store_guardrail
        and "CanonicalObjectStoreBackendCapabilityReport()" in object_store_docs,
        "C++ object-store integrations must expose a canonical capabilities view for MatrixObject/S3/local adapters",
        failures,
    )
    for rust_capability in (
        "atomic_publish",
        "delete_capability",
        "object_copy",
        "prefix_delete",
        "opaque_object_validators",
        "object_version_ids",
    ):
        require(
            f"pub {rust_capability}: bool" in object_store_code,
            f"Rust object-store capability report must expose canonical {rust_capability}",
            failures,
        )
    for rust_compatibility_alias in (
        "atomic_put",
        "delete",
        "copy_object",
        "delete_prefix",
        "object_etag",
        "object_version_id",
    ):
        require(
            f"pub {rust_compatibility_alias}: bool" in object_store_code,
            f"Rust object-store capability report must keep compatibility alias {rust_compatibility_alias}",
            failures,
        )
    for symbol in (
        "ObjectStoreBackendCapabilities",
        "ObjectStoreBackendCapabilityReport",
        "ObjectStoreBackendRuntimeLinked",
    ):
        require(
            symbol in cxx_object_store_backend,
            f"C++ object-store backend contract must expose {symbol}",
            failures,
        )
    for field in (
        "operations_fail_closed",
        "atomic_publish",
        "unique_put",
        "conditional_create",
        "direct_upload_from_path",
        "direct_download_to_path",
        "metadata_head",
        "prefix_list",
        "paginated_list",
        "delete_capability",
        "bulk_delete",
        "object_copy",
        "prefix_delete",
        "byte_range_read",
        "checksum_sha256",
        "opaque_object_validators",
        "object_version_ids",
        "split_services",
        "s3_compatible",
        "local_file_compatible",
    ):
        require(
            field in cxx_object_store_backend,
            f"C++ object-store capability report must expose {field}",
            failures,
        )
    for compatibility_field in (
        "condition_metadata",
        "metadata_stat",
        "append_write",
        "delete_object",
        "copy_or_rename",
    ):
        require(
            compatibility_field in cxx_object_store_backend,
            f"C++ object-store capability report must keep compatibility alias {compatibility_field}",
            failures,
        )
    require(
        "PublicCapabilityReportsAreProviderNeutral" in cxx_object_store_guardrail,
        "C++ object-store guardrails must validate provider-neutral capability reports",
        failures,
    )
    require(
        "ObjectStoreBackendCapabilityReport" in cxx_object_store_guardrail
        and "operations_fail_closed" in cxx_object_store_guardrail,
        "C++ object-store guardrails must assert fail-closed capability behavior",
        failures,
    )

    if failures:
        print("open-source surface validation failed:")
        for failure in failures:
            print(f" - {failure}")
        return 1

    print("open-source surface validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
