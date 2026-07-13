#!/usr/bin/env python3
"""Validate the trimmed TemporalStore open-source surface.

This intentionally checks source policy rather than doing a full release build:
the goal is to catch accidental re-exposure of internal modules/models and
non-basic Redis commands in the public build surface.
"""

from __future__ import annotations

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

    root_cmake = read("CMakeLists.txt")
    extension_cmake = read("src/extension/CMakeLists.txt")
    set_cmake = read("src/extension/set/CMakeLists.txt")
    model_cmake = read("src/model/CMakeLists.txt")
    model_manager = read("src/model/model_manager.cc")
    cxx_redis = read("src/server/redis_command_handler.cc")
    rust_redis = read("crates/temporalstore-rust/src/redis.rs")
    redis_compat_smoke = read("tools/run_redis_compat_smoke_ubuntu22.sh")
    redis_live_smoke = read("tools/run_redis_live_storage_smoke_ubuntu22.sh")
    redis_docs = read("docs/redis_compatibility_matrix.md")
    raw_storage_contract = read("tools/matrixark_raw_message_storage_contract.py")
    matrixobject_docs = read("docs/matrixobjectstore_design_extraction_and_readiness.md")
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
    for allowed in ("kQuit", "kClient"):
        require(
            f"case RedisCommand::CmdType::{allowed}" in cxx_redis,
            f"C++ open-source Redis allowlist must keep basic client command {allowed}",
            failures,
        )
    require(
        "OpenSourceRedisCommandCount" in cxx_redis,
        "C++ COMMAND COUNT must report the trimmed open-source command count",
        failures,
    )
    require(
        "return 42;" in cxx_redis,
        "C++ COMMAND COUNT must match the trimmed open-source allowlist size",
        failures,
    )
    for denied in ("kSAdd", "kLPush", "kZAdd", "kPartition", "kBgSave", "kConfig"):
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
        "FAPPEND",
        "FQUERY",
        "RISKINCR",
        "CPCSET",
        "FOLQUERY",
        "QUIT",
        "CLIENT",
    ):
        require(f'"{allowed}"' in body, f"Rust allowlist must keep {allowed}", failures)
    for denied in (
        "SADD",
        "LPUSH",
        "ZADD",
        "IPSADD",
        "FADD",
        "RISKDEBUG",
        "PARTITION",
        "DBSIZE",
        "CONFIG",
    ):
        require(f'"{denied}"' not in body, f"Rust allowlist must not include {denied}", failures)

    for metric in (
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
    for stale_claim in ("- Set: `SADD`", "- List: `LPUSH`", "- ZSet: `ZADD`"):
        require(stale_claim not in redis_docs, f"Redis docs must not keep stale claim {stale_claim}", failures)
    for rust_helper in (
        "MSETNX",
        "TOUCH",
        "EXPIREAT",
        "PEXPIREAT",
        "EXPIRETIME",
        "PEXPIRETIME",
        "GETRANGE",
        "SETRANGE",
        "INCRBYFLOAT",
        "HINCRBYFLOAT",
    ):
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
        "COMMAND COUNT=42" in redis_docs,
        "Redis docs must state the narrower C++ open-source COMMAND COUNT",
        failures,
    )

    for symbol in (
        "OBJECT_STORE_PROVIDER_ALIASES",
        "GENERIC_OBJECT_STORE_OPERATIONS",
        "generic_object_store_contract",
        "object_store_contract",
    ):
        require(symbol in raw_storage_contract, f"raw-message contract must expose {symbol}", failures)
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
        "bulk_delete",
        "byte_range_read",
        "opaque_object_validators",
        "split_services",
    ):
        require(
            f'"{capability}"' in raw_storage_contract,
            f"generic object-store contract must expose capability {capability}",
            failures,
        )
    require(
        '"matrixobjectstore": "objectstore"' in raw_storage_contract,
        "MatrixObject legacy backend alias must stay compatible",
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

    if failures:
        print("open-source surface validation failed:")
        for failure in failures:
            print(f" - {failure}")
        return 1

    print("open-source surface validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
