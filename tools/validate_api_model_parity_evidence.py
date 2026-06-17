#!/usr/bin/env python3
"""Validate Rust evidence for executable shared API/model parity cases."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"


@dataclass(frozen=True)
class RustEvidence:
    path: str
    snippets: tuple[str, ...]


@dataclass(frozen=True)
class ApiModelArea:
    name: str
    corpus_cases: tuple[str, ...]
    command_kinds: tuple[str, ...]
    response_kinds: tuple[str, ...]
    rust_evidence: tuple[RustEvidence, ...]


AREAS: tuple[ApiModelArea, ...] = (
    ApiModelArea(
        name="common_redis_string_hash_set",
        corpus_cases=(
            "common_string_hash_core",
            "common_lifecycle_delete_ttl",
            "hash_single_field_and_delete",
            "redis_compatible_set_core",
            "common_not_found_and_empty_reads",
        ),
        command_kinds=(
            "string_set",
            "string_get",
            "common_ttl",
            "common_delete",
            "hash_set",
            "hash_get",
            "hash_multi_get",
            "set_add",
            "set_members",
        ),
        response_kinds=("empty", "bytes", "integer", "hash_entries", "values", "members"),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/redis.rs",
                ("execute_redis_command", "RespValue", "MGET", "EXISTS"),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/temporalstore_compat.rs",
                ("onebox_proxy_hash_multi_command_parity_over_redis_resp",),
            ),
        ),
    ),
    ApiModelArea(
        name="feature_sequence_timestamped_pages",
        corpus_cases=(
            "feature_packed_timestamped_pages",
            "sequence_cpp_feature_rows",
            "timestamped_query_bounds",
            "feature_policy_filter_aggregate_lifecycle",
            "sequence_batch_filter_groups",
            "mixed_model_restart_persistence",
        ),
        command_kinds=(
            "feature_append",
            "feature_append_with_policy",
            "feature_query",
            "feature_query_filtered",
            "feature_replace",
            "feature_delete",
            "feature_agg_query",
            "sequence_add",
            "sequence_query",
            "sequence_batch_query",
        ),
        response_kinds=("feature_points", "aggregate", "sequence_rows", "sequence_row_groups"),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/types.rs",
                (
                    "FeatureAppend",
                    "FeatureAppendWithPolicy",
                    "FeatureQueryFiltered",
                    "FeatureReplace",
                    "FeatureDelete",
                    "FeatureAggQuery",
                    "SequenceAdd",
                    "SequenceBatchQuery",
                    "SequenceQuery",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/engine.rs",
                ("FeaturePageLayoutReport", "packed_timestamped", "feature_page_layout"),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/temporalstore_compat.rs",
                ("cxx_long_sequence_feature_5k_ordered_windows_and_random_filters",),
            ),
        ),
    ),
    ApiModelArea(
        name="ips_risk_models",
        corpus_cases=(
            "ips_options_range",
            "risk_counter_window",
            "risk_family_query_and_delete",
        ),
        command_kinds=(
            "ips_add_with_options",
            "ips_query_range",
            "risk_increment",
            "risk_count",
            "risk_set",
            "risk_family_query",
        ),
        response_kinds=("feature_points", "integer", "empty"),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/types.rs",
                ("IpsAddWithOptions", "IpsSnapshotReport", "RiskIncrement", "RiskFamilyQuery"),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/client.rs",
                ("ips_snapshot_report", "ips_query_range_with_options", "risk_family_query"),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/temporalstore_compat.rs",
                ("cxx_redis_feature_commands_cover_module_flow",),
            ),
        ),
    ),
    ApiModelArea(
        name="context_models_sdk_wire",
        corpus_cases=(
            "context_node_roundtrip",
            "context_event_index_audit_dirty_models",
            "context_missing_node_semantics",
        ),
        command_kinds=(
            "context_upsert_node",
            "context_get_node",
            "context_write_event",
            "context_query_events",
            "context_write_index_ref",
            "context_query_index",
            "context_write_pack_audit",
            "context_query_pack_audit",
            "context_mark_summary_dirty",
            "context_query_summary_dirty",
        ),
        response_kinds=(
            "context_node",
            "context_object_key",
            "context_events",
            "context_index_refs",
            "context_pack_audits",
            "context_summary_dirty_markers",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/types.rs",
                (
                    "ContextNodeModel",
                    "ContextEventModel",
                    "ContextWire",
                    "context_models_round_trip_cpp_wire_payloads_and_type_alias",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/sdk.rs",
                ("ContextNodeUpsert", "ContextNodeGet", "sdk_context_node_to_types"),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/unified_temporalstore_corpus.rs",
                ("context_summary_dirty_markers", "ContextPackAudits"),
            ),
        ),
    ),
)


def load_corpus() -> dict:
    with CORPUS.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_case_and_kinds(area: ApiModelArea, corpus: dict) -> tuple[int, int]:
    cases = {case["name"]: case for case in corpus["cases"]}
    required_cases = set(corpus["coverage"]["required_case_names"])
    required_commands = set(corpus["coverage"]["required_command_kinds"])
    required_responses = set(corpus["coverage"]["required_response_kinds"])
    seen_commands: set[str] = set()
    seen_responses: set[str] = set()

    for case_name in area.corpus_cases:
        if case_name not in required_cases:
            raise SystemExit(f"{area.name}: {case_name} missing from coverage.required_case_names")
        if case_name not in cases:
            raise SystemExit(f"{area.name}: missing corpus case {case_name}")
        for step in cases[case_name]["steps"]:
            command_kind = step["command"]["kind"]
            if command_kind == "existing_test":
                raise SystemExit(f"{area.name}: executable API area includes existing_test step")
            seen_commands.add(command_kind)
            if "expect" in step:
                seen_responses.add(step["expect"]["kind"])

    for command_kind in area.command_kinds:
        if command_kind not in required_commands:
            raise SystemExit(f"{area.name}: {command_kind} missing from coverage.required_command_kinds")
        if command_kind not in seen_commands:
            raise SystemExit(f"{area.name}: {command_kind} missing from area corpus cases")
    for response_kind in area.response_kinds:
        if response_kind not in required_responses:
            raise SystemExit(f"{area.name}: {response_kind} missing from coverage.required_response_kinds")
        if response_kind not in seen_responses:
            raise SystemExit(f"{area.name}: {response_kind} missing from area expected responses")

    return len(seen_commands), len(seen_responses)


def validate_rust_evidence(area: ApiModelArea) -> int:
    count = 0
    for evidence in area.rust_evidence:
        path = ROOT / evidence.path
        if not path.exists():
            raise SystemExit(f"{area.name}: missing Rust evidence file {evidence.path}")
        text = path.read_text(encoding="utf-8", errors="ignore")
        for snippet in evidence.snippets:
            if snippet not in text:
                raise SystemExit(
                    f"{area.name}: Rust evidence file {evidence.path} missing snippet {snippet!r}"
                )
            count += 1
    return count


def main() -> int:
    corpus = load_corpus()
    total_command_kinds: set[str] = set()
    total_response_kinds: set[str] = set()
    total_evidence = 0
    for area in AREAS:
        command_count, response_count = validate_case_and_kinds(area, corpus)
        total_command_kinds.update(area.command_kinds)
        total_response_kinds.update(area.response_kinds)
        total_evidence += validate_rust_evidence(area)
        print(
            f"validated API/model parity area: {area.name} "
            f"commands={command_count} responses={response_count}"
        )

    print(f"api_model_parity_areas={len(AREAS)}")
    print(f"required_command_kinds={len(total_command_kinds)}")
    print(f"required_response_kinds={len(total_response_kinds)}")
    print(f"rust_evidence_snippets={total_evidence}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
