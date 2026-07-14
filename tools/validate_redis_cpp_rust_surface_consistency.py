#!/usr/bin/env python3
"""Validate C++/Rust Redis and data-model command surface consistency.

The first open-source release intentionally keeps one shared Redis-compatible
base plus Rust-only explicit Feature/Risk model commands. This gate catches
C++/Rust/manifest drift and prevents CPC/FOL aliases from re-entering the
trimmed public surface.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "compat" / "redis_cpp_rust_surface_parity_contract.json"
MANIFEST = ROOT / "compat" / "redis_open_source_surface_manifest.json"
CXX = ROOT / "src" / "server" / "redis_command_handler.cc"
RUST = ROOT / "crates" / "temporalstore-rust" / "src" / "redis.rs"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def extract_cxx_open_source_commands(source: str) -> list[str]:
    match = re.search(
        r"OpenSourceRedisCommands\(\) \{.*?static const std::vector<OpenSourceRedisCommandDescriptor> commands = \{(?P<body>.*?)\n    \};",
        source,
        re.S,
    )
    if not match:
        raise AssertionError("could not locate C++ OpenSourceRedisCommands descriptor table")
    return re.findall(r'\{\s*RedisCommand::CmdType::k[A-Za-z0-9]+\s*,\s*"([A-Z0-9]+)"\s*,', match.group("body"))


def extract_rust_allowlist(source: str) -> set[str]:
    match = re.search(
        r"fn open_source_redis_command_allowed\(command: &str\) -> bool \{(?P<body>.*?)\n\}",
        source,
        re.S,
    )
    if not match:
        raise AssertionError("could not locate Rust open_source_redis_command_allowed")
    return set(re.findall(r'"([A-Z0-9]+)"', match.group("body")))


def main() -> int:
    failures: list[str] = []
    contract = load_json(CONTRACT)
    manifest = load_json(MANIFEST)
    cxx_source = CXX.read_text()
    rust_source = RUST.read_text()

    shared = contract["shared_minimal_redis_commands"]
    feature = contract["rust_public_data_model_commands"]["feature"]
    risk = contract["rust_public_data_model_commands"]["risk_frequency_control"]
    model_commands = feature + risk

    manifest_cxx = manifest.get("cxx_commands", [])
    manifest_rust_extra = manifest.get("rust_extra_commands", [])
    cxx_commands = extract_cxx_open_source_commands(cxx_source)
    rust_allow = extract_rust_allowlist(rust_source)

    if contract.get("surface") != manifest.get("surface"):
        failures.append("manifest and parity contract must use the same public surface identity")
    if contract.get("surface") != "trimmed_open_source_context_feature_risk":
        failures.append("public surface identity must be Risk-specific, not generic frequency-control")
    if "Risk is the only public frequency-cap/risk-control model" not in contract.get("rule", ""):
        failures.append("parity contract must state Risk is the only public frequency-cap/risk-control model")
    public_contract_text = json.dumps(contract, sort_keys=True).lower() + "\n" + json.dumps(manifest, sort_keys=True).lower()
    for private_term in ("audit", "replay", "debug", "cpc", "fol", "ips"):
        if private_term in public_contract_text:
            failures.append(f"public Redis/data-model contract must not catalog private/internal model family: {private_term}")

    if manifest_cxx != shared:
        failures.append("manifest cxx_commands must exactly match shared minimal Redis commands")
    if cxx_commands != shared:
        failures.append("C++ OpenSourceRedisCommands must exactly match shared minimal Redis commands")
    if manifest.get("cxx_command_count") != len(shared):
        failures.append("manifest cxx_command_count must match shared minimal Redis command count")
    if contract.get("cxx_public_command_count") != len(shared):
        failures.append("contract cxx_public_command_count must match shared minimal Redis command count")

    if sorted(manifest_rust_extra) != sorted(model_commands):
        failures.append("manifest rust_extra_commands must be exactly Feature plus single Risk model commands")
    expected_rust = set(shared + model_commands)
    if rust_allow != expected_rust:
        missing = sorted(expected_rust - rust_allow)
        extra = sorted(rust_allow - expected_rust)
        failures.append(f"Rust trimmed allowlist drifted; missing={missing} extra={extra}")
    if contract.get("rust_trimmed_public_command_count") != len(expected_rust):
        failures.append("contract rust_trimmed_public_command_count must match shared plus model commands")

    if failures:
        print("redis C++/Rust surface consistency validation failed:")
        for failure in failures:
            print(f" - {failure}")
        return 1
    print("redis C++/Rust surface consistency validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
