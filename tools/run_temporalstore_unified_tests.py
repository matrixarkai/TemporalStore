#!/usr/bin/env python3
"""Run the shared TemporalStore C++/Rust behavioral corpus.

The JSON corpus is the test contract. Rust executes it through an integration
test. C++ should expose a runner command that accepts the same corpus path via
TS_CPP_UNIFIED_TEST_CMD, using "{corpus}" as an optional path placeholder.
When TS_CPP_REPO or --cpp-repo is provided, the command also gets "{cpp_repo}"
rendered and otherwise runs from that repository root.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def default_corpus_path() -> Path:
    override = os.environ.get("TEMPORALSTORE_TEST_CORPUS")
    if override:
        return Path(override)
    candidates = [
        ROOT / "third_party" / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
        ROOT.parent / "TemporalStoreTestCorpus" / "cases" / "unified_temporalstore_cases.json",
        ROOT / "compat" / "unified_temporalstore_cases.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[-1]


DEFAULT_CORPUS = default_corpus_path()
DEFAULT_CPP_RUNNER_RELATIVE = Path("tools") / "run_temporalstore_unified_tests.sh"
CPP_RAFT_PARITY_SUITE = "cpp_data_raft_parity"
COMBINED_RAFT_GATE_CASES = {
    "raft_data_node_scale_failover_snapshot",
    "raft_data_node_mixed_rw_and_membership",
    "raft_production_gate",
}
COMBINED_RAFT_GATE = "tools/run_raft_distributed_parity.sh"
COMBINED_RAFT_VALIDATOR = (
    "python3 tools/validate_aws_validation_log.py --job "
    "temporalstore-raft-distributed-parity-validation --log <raft-distributed-parity.json>"
)
RUSTRAFT_FAULT_ACCEPTANCE_KEYWORDS = {
    "raft_rustraft_packet_loss_fault_harness": [
        ["majority", "continues"],
        ["minority", "rejects", "stale", "reads"],
        ["healed", "catches up"],
    ],
    "raft_rustraft_slow_wal_fsync_fault_harness": [
        ["backpressure", "activates"],
        ["no committed write", "lost"],
        ["lag", "latency", "pressure"],
    ],
    "raft_rustraft_snapshot_during_membership_fault_harness": [
        ["snapshot floor", "consistent"],
        ["membership generation", "consistent"],
        ["restart", "snapshot floor", "membership generation"],
    ],
    "raft_rustraft_leader_transfer_high_write_fault_harness": [
        ["commit exactly once", "fail safely"],
        ["committed write", "lost", "duplicated"],
        ["final leader", "all committed entries"],
    ],
    "raft_rustraft_follower_rejoin_compacted_logs_fault_harness": [
        ["installs snapshot"],
        ["replays retained", "tail"],
        ["read-eligible", "catch-up"],
    ],
    "raft_rustraft_rolling_restart_joint_consensus_fault_harness": [
        ["joint consensus", "survives"],
        ["completes safely", "rolls back safely"],
        ["membership state", "not lost"],
    ],
}
BENCHMARK_REQUIRED_REPORT_FIELDS = {
    "benchmark_family",
    "benchmark_hit_at_k",
    "benchmark_recall_at_k",
    "benchmark_mean_reciprocal_rank",
    "benchmark_token_reduction_percent",
    "benchmark_retrieval_p50_ms",
    "benchmark_retrieval_p95_ms",
    "benchmark_reader_p50_ms",
    "benchmark_reader_p95_ms",
    "benchmark_quality_ready",
    "benchmark_threshold_passed",
    "benchmark_threshold_violation_count",
    "benchmark_threshold_violations",
    "benchmark_thresholds",
    "benchmark_per_query_count",
    "category_breakdown",
    "weak_category_count",
    "weak_categories",
    "weak_category_policy",
    "case_count",
    "hit_rate",
    "reader_hit_rate",
    "reader_mode_requested",
    "reader_mode_effective",
    "reader_provider_name",
    "reader_model",
}
BENCHMARK_THRESHOLD_PROFILES = {
    "fixture",
    "locomo_full",
    "longmemeval_full",
    "oss_reader_full",
}
CPP_ADAPTER_STATUSES = {
    "mixed_native_and_static_surface_gate",
    "native_adapter_contract",
    "native_runner_mapped",
    "temporary_static_surface_gate",
}
STATIC_CPP_MODES = {
    "static",
    "rust_executable_cxx_static",
}
COMPARISON_OUTPUT_FIELDS = {
    "rust_only_misses",
    "cpp_only_misses",
    "shared_hard_failures",
    "output_diffs",
    "latency_deltas",
}
STORAGE_CPP_REPORT_ADAPTER = ROOT / "compat" / "cpp_unified_case_report_adapter.h"
STORAGE_REPORT_FIELDS = {
    "StorageUnifiedEvidence",
    "StoragePassedStep",
    "StorageFailedStep",
    "storage_write_sequence",
    "storage_read_sequence",
    "storage_cold_scan_sequence",
    "storage_lifecycle_phases",
    "storage_cache_layers",
    "storage_cache_semantics",
    "storage_reclaim_semantics",
    "storage_manager_prepare_count",
    "storage_manager_reclaim_count",
    "storage_manager_evict_count",
    "storage_manager_expire_count",
    "storage_manager_page_gc_count",
    "storage_manager_block_gc_count",
    "storage_manager_compaction_count",
    "storage_manager_index_gc_count",
    "storage_manager_delayed_destroy_count",
    "storage_manager_follower_cursor_safety_count",
    "storage_manager_watermark_progress_count",
    "stream_rollover_count",
    "segment_open_count",
    "segment_sealed_count",
    "storage_zone_total_bytes",
    "storage_zone_used_bytes",
    "storage_zone_stale_bytes",
    "append_log_replay_records",
    "append_log_reclaimed_records",
    "slot_dirty_generation_count",
    "slot_tombstone_count",
    "slot_stale_ref_count",
    "slot_owner_mismatch_count",
    "page_index_rebuild_count",
    "block_index_rebuild_count",
    "object_index_rebuild_count",
    "page_index_lookup_count",
    "page_index_lookup_ms",
    "page_index_cache_hit_rate",
    "block_index_lookup_count",
    "block_index_lookup_ms",
    "block_index_cache_hit_rate",
    "page_reads",
    "page_writes",
    "block_reads",
    "block_writes",
    "bytes_read",
    "bytes_written",
    "memory_cache_hits",
    "memory_cache_misses",
    "page_index_cache_hits",
    "page_index_cache_misses",
    "block_index_cache_hits",
    "block_index_cache_misses",
    "disk_cache_hits",
    "disk_cache_misses",
    "shared_store_read_throughs",
    "cache_refills",
    "cache_invalidations",
    "cache_writeback_queue_depth",
    "cache_writeback_rejections",
    "cold_scan_no_cache_reads",
    "cold_scan_page_reads",
    "hot_cache_promotions",
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "stale_pages_rewritten",
    "stale_pages_skipped",
    "stale_blocks_rewritten",
    "stale_blocks_skipped",
    "delayed_destroy_backlog",
    "follower_cursor_retention_floor",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
    "append_watermark",
    "compaction_watermark",
}

CPP_RUNNER_TEMPLATE = r"""#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --corpus)
      CORPUS="${2:-}"
      shift 2
      ;;
    --corpus=*)
      CORPUS="${1#--corpus=}"
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${CORPUS}" ]]; then
  CORPUS="${ROOT}/compat/unified_temporalstore_cases.json"
fi
if [[ ! -f "${CORPUS}" ]]; then
  echo "unified TemporalStore corpus not found: ${CORPUS}" >&2
  exit 2
fi
if [[ -z "${TS_CPP_UNIFIED_NATIVE_CMD:-}" ]]; then
  cat >&2 <<'MSG'
TS_CPP_UNIFIED_NATIVE_CMD is not set.

Set it to the C++ TemporalStore corpus executor. The command may use a
{corpus} placeholder. Example:

  TS_CPP_UNIFIED_NATIVE_CMD='bazel run //temporalstore:corpus_runner -- {corpus}' \
    tools/run_temporalstore_unified_tests.sh --corpus /path/to/compat/unified_temporalstore_cases.json
MSG
  exit 2
fi

if [[ "${TS_CPP_UNIFIED_NATIVE_CMD}" == *"{corpus}"* ]]; then
  CMD="${TS_CPP_UNIFIED_NATIVE_CMD//\{corpus\}/${CORPUS}}"
else
  CMD="${TS_CPP_UNIFIED_NATIVE_CMD} ${CORPUS}"
fi

cd "${ROOT}"
echo "+ ${CMD}"
exec bash -lc "${CMD}"
"""


def validate_corpus(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        corpus = json.load(handle)

    if corpus.get("schema_version") != 1:
        raise SystemExit(f"{path}: unsupported schema_version={corpus.get('schema_version')!r}")
    cases = corpus.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit(f"{path}: cases must be a non-empty list")
    coverage = corpus.get("coverage")
    if not isinstance(coverage, dict):
        raise SystemExit(f"{path}: coverage must declare required shared C++/Rust test families")
    required_case_names = coverage.get("required_case_names")
    required_raft_case_names = coverage.get("required_raft_case_names", [])
    required_command_kinds = coverage.get("required_command_kinds")
    required_response_kinds = coverage.get("required_response_kinds")
    for field_name, values in [
        ("required_case_names", required_case_names),
        ("required_raft_case_names", required_raft_case_names),
        ("required_command_kinds", required_command_kinds),
        ("required_response_kinds", required_response_kinds),
    ]:
        if not isinstance(values, list) or not values or not all(
            isinstance(value, str) and value for value in values
        ):
            raise SystemExit(f"{path}: coverage.{field_name} must be a non-empty string list")

    seen_case_names = set()
    seen_command_kinds = set()
    seen_response_kinds = set()
    seen_cpp_suites = set()
    seen_static_cpp_suites = set()
    for case in cases:
        if not case.get("name"):
            raise SystemExit(f"{path}: every case must have a name")
        if case["name"] in seen_case_names:
            raise SystemExit(f"{path}: duplicate case name {case['name']}")
        seen_case_names.add(case["name"])
        if not isinstance(case.get("shard_id"), int):
            raise SystemExit(f"{path}: case {case.get('name')!r} must have an integer shard_id")
        steps = case.get("steps")
        if not isinstance(steps, list) or not steps:
            raise SystemExit(f"{path}: case {case['name']} must have non-empty steps")
        case_step_names = set()
        case_command_signatures = set()
        for step in steps:
            if not step.get("name"):
                raise SystemExit(f"{path}: case {case['name']} has an unnamed step")
            if step["name"] in case_step_names:
                raise SystemExit(f"{path}: duplicate step name {case['name']}/{step['name']}")
            case_step_names.add(step["name"])
            if not isinstance(step.get("command"), dict):
                raise SystemExit(f"{path}: step {case['name']}/{step.get('name')} needs command")
            if "kind" not in step["command"]:
                raise SystemExit(f"{path}: step {case['name']}/{step['name']} command needs kind")
            command_signature = json.dumps(step["command"], sort_keys=True, separators=(",", ":"))
            if command_signature in case_command_signatures:
                raise SystemExit(
                    f"{path}: duplicate command payload in case {case['name']} "
                    f"at step {step['name']}"
                )
            case_command_signatures.add(command_signature)
            seen_command_kinds.add(step["command"]["kind"])
            if "expect" in step:
                if not isinstance(step["expect"], dict) or "kind" not in step["expect"]:
                    raise SystemExit(
                        f"{path}: step {case['name']}/{step['name']} expect needs kind"
                )
                seen_response_kinds.add(step["expect"]["kind"])
            if step["command"].get("suite") == CPP_RAFT_PARITY_SUITE:
                validate_cpp_raft_step(path, case, step)
                validate_rustraft_fault_acceptance(path, case, step)
                validate_byteraft_report_contract(path, case, step)
            if step["command"].get("suite") == "cpp_context_benchmark_parity":
                validate_context_benchmark_step(path, case, step)
            if step["command"].get("kind") == "existing_test":
                suite = step["command"].get("suite")
                mode = step["command"].get("mode")
                if isinstance(suite, str) and suite:
                    seen_cpp_suites.add(suite)
                    if mode in STATIC_CPP_MODES:
                        seen_static_cpp_suites.add(suite)
        if case["name"] in COMBINED_RAFT_GATE_CASES:
            validate_combined_raft_case(path, case)
    missing_cases = sorted(set(required_case_names) - seen_case_names)
    missing_raft_cases = sorted(set(required_raft_case_names) - seen_case_names)
    missing_commands = sorted(set(required_command_kinds) - seen_command_kinds)
    missing_responses = sorted(set(required_response_kinds) - seen_response_kinds)
    if missing_cases:
        raise SystemExit(f"{path}: missing required cases: {', '.join(missing_cases)}")
    if missing_raft_cases:
        raise SystemExit(f"{path}: missing required Raft cases: {', '.join(missing_raft_cases)}")
    if missing_commands:
        raise SystemExit(f"{path}: missing required command kinds: {', '.join(missing_commands)}")
    if missing_responses:
        raise SystemExit(f"{path}: missing required response kinds: {', '.join(missing_responses)}")
    validate_cpp_adapter_coverage(path, coverage, seen_cpp_suites, seen_static_cpp_suites)
    validate_storage_cpp_report_adapter(path)
    return corpus


def validate_storage_cpp_report_adapter(path: Path) -> None:
    if not STORAGE_CPP_REPORT_ADAPTER.exists():
        raise SystemExit(
            f"{path}: missing C++ storage report adapter {STORAGE_CPP_REPORT_ADAPTER}"
        )
    adapter = STORAGE_CPP_REPORT_ADAPTER.read_text(encoding="utf-8")
    missing = sorted(field for field in STORAGE_REPORT_FIELDS if field not in adapter)
    if missing:
        raise SystemExit(
            f"{path}: C++ storage report adapter missing canonical storage fields: "
            + ", ".join(missing)
        )


def case_matches_family(case: dict, family: str, family_suites: set[str]) -> bool:
    if case.get("family") == family:
        return True
    for step in case.get("steps") or []:
        if not isinstance(step, dict):
            continue
        command = step.get("command") if isinstance(step.get("command"), dict) else {}
        if command.get("suite") in family_suites:
            return True
    return False


def filtered_corpus_for_family(corpus: dict, source_path: Path, family: str) -> Path:
    coverage = corpus.get("coverage") if isinstance(corpus.get("coverage"), dict) else {}
    family_suites: set[str] = set()
    for entry in coverage.get("cpp_adapter_coverage") or []:
        if isinstance(entry, dict) and entry.get("family") == family:
            family_suites.update(str(suite) for suite in entry.get("suites") or [])
    cases = [
        case
        for case in corpus.get("cases") or []
        if isinstance(case, dict) and case_matches_family(case, family, family_suites)
    ]
    if not cases:
        raise SystemExit(f"{source_path}: no shared corpus cases matched --family {family!r}")
    filtered = dict(corpus)
    filtered["name"] = f"{corpus.get('name', 'temporalstore_unified_corpus')}::{family}"
    filtered["source_corpus"] = str(source_path)
    filtered["family_filter"] = family
    filtered["cases"] = cases
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        delete=False,
        prefix=f"temporalstore-unified-{family.replace('/', '_')}-",
        suffix=".json",
    )
    with handle:
        json.dump(filtered, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return Path(handle.name)


def validate_cpp_adapter_coverage(
    path: Path,
    coverage: dict,
    seen_cpp_suites: set[str],
    seen_static_cpp_suites: set[str],
) -> None:
    adapter_coverage = coverage.get("cpp_adapter_coverage")
    if not isinstance(adapter_coverage, list) or not adapter_coverage:
        raise SystemExit(f"{path}: coverage.cpp_adapter_coverage must be a non-empty list")
    mapped_suites: set[str] = set()
    static_gate_suites: set[str] = set()
    for index, entry in enumerate(adapter_coverage):
        location = f"{path}: coverage.cpp_adapter_coverage[{index}]"
        family = entry.get("family")
        if not isinstance(family, str) or not family:
            raise SystemExit(f"{location}: family must be a non-empty string")
        suites = entry.get("suites")
        if not isinstance(suites, list) or not suites or not all(
            isinstance(suite, str) and suite for suite in suites
        ):
            raise SystemExit(f"{location}: suites must be a non-empty string list")
        status = entry.get("status")
        if status not in CPP_ADAPTER_STATUSES:
            raise SystemExit(
                f"{location}: status must be one of {', '.join(sorted(CPP_ADAPTER_STATUSES))}"
            )
        if status in {"temporary_static_surface_gate", "mixed_native_and_static_surface_gate"}:
            blocker = entry.get("blocker")
            expected_runner = entry.get("expected_runner_command")
            if not isinstance(blocker, str) or len(blocker.strip()) < 24:
                raise SystemExit(f"{location}: static gates must declare a blocker")
            if not isinstance(expected_runner, str) or "{corpus}" not in expected_runner:
                raise SystemExit(
                    f"{location}: static gates must declare expected_runner_command "
                    "with a {corpus} placeholder"
                )
            static_gate_suites.update(suites)
        if status in {"native_adapter_contract", "native_runner_mapped", "mixed_native_and_static_surface_gate"}:
            runner = entry.get("runner_command")
            if not isinstance(runner, str) or not runner:
                raise SystemExit(f"{location}: native C++ adapter entries must declare runner_command")
        comparison = entry.get("comparison_command")
        if comparison is not None and not isinstance(comparison, str):
            raise SystemExit(f"{location}: comparison_command must be a string when present")
        mapped_suites.update(suites)

    unmapped = sorted(seen_cpp_suites - mapped_suites)
    if unmapped:
        raise SystemExit(
            f"{path}: C++ suites missing coverage.cpp_adapter_coverage entries: "
            + ", ".join(unmapped)
        )
    missing_static_blockers = sorted(seen_static_cpp_suites - static_gate_suites)
    if missing_static_blockers:
        raise SystemExit(
            f"{path}: static C++ suites must have temporary_static_surface_gate blockers: "
            + ", ".join(missing_static_blockers)
        )

    comparison_outputs = coverage.get("comparison_outputs")
    if not isinstance(comparison_outputs, dict):
        raise SystemExit(f"{path}: coverage.comparison_outputs must be an object")
    comparator = comparison_outputs.get("case_report_comparator")
    if not isinstance(comparator, str) or not comparator:
        raise SystemExit(f"{path}: coverage.comparison_outputs.case_report_comparator is required")
    comparator_path = ROOT / comparator
    if not comparator_path.exists():
        raise SystemExit(f"{path}: comparison output tool does not exist: {comparator}")
    fields = comparison_outputs.get("required_fields")
    if not isinstance(fields, list) or not all(isinstance(field, str) for field in fields):
        raise SystemExit(f"{path}: coverage.comparison_outputs.required_fields must be strings")
    missing_fields = sorted(COMPARISON_OUTPUT_FIELDS - set(fields))
    if missing_fields:
        raise SystemExit(
            f"{path}: comparison_outputs.required_fields missing {', '.join(missing_fields)}"
        )


def validate_context_benchmark_step(path: Path, case: dict, step: dict) -> None:
    location = f"{path}: case {case['name']} step {step['name']}"
    command = step["command"]
    contract = command.get("report_contract")
    if not isinstance(contract, dict):
        raise SystemExit(f"{location}: context benchmark step must declare report_contract")
    fields = contract.get("required_fields")
    if not isinstance(fields, list) or not fields:
        raise SystemExit(f"{location}: report_contract.required_fields must be a non-empty list")
    missing_fields = sorted(BENCHMARK_REQUIRED_REPORT_FIELDS - set(fields))
    if missing_fields:
        raise SystemExit(
            f"{location}: report_contract.required_fields missing {', '.join(missing_fields)}"
        )
    threshold_profiles = command.get("threshold_profiles")
    if not isinstance(threshold_profiles, list) or not threshold_profiles:
        raise SystemExit(f"{location}: threshold_profiles must be a non-empty list")
    unknown_profiles = sorted(set(threshold_profiles) - BENCHMARK_THRESHOLD_PROFILES)
    if unknown_profiles:
        raise SystemExit(f"{location}: unknown threshold profiles {', '.join(unknown_profiles)}")
    for field_name in ("rust_runner", "cpp_runner_contract", "archive_contract"):
        if not isinstance(command.get(field_name), str) or not command[field_name]:
            raise SystemExit(f"{location}: benchmark contract must declare {field_name}")
    required_paths = command.get("required_paths")
    if case["name"] == "context_benchmark_full_dataset_gates":
        if not isinstance(required_paths, list) or "tools/compare_context_benchmark_archives.py" not in required_paths:
            raise SystemExit(
                f"{location}: full benchmark contract must require "
                "tools/compare_context_benchmark_archives.py"
            )
        if "tools/fetch_longmemeval_s.py" not in required_paths:
            raise SystemExit(
                f"{location}: full benchmark contract must require tools/fetch_longmemeval_s.py"
            )
    datasets = command.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise SystemExit(f"{location}: benchmark contract must declare datasets")
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise SystemExit(f"{location}: each dataset entry must be an object")
        for field_name in ("name", "artifact_kind", "threshold_profile"):
            if not isinstance(dataset.get(field_name), str) or not dataset[field_name]:
                raise SystemExit(f"{location}: dataset entry must declare {field_name}")
        if dataset["threshold_profile"] not in BENCHMARK_THRESHOLD_PROFILES:
            raise SystemExit(
                f"{location}: dataset {dataset['name']} uses unknown threshold profile "
                f"{dataset['threshold_profile']}"
            )


def validate_cpp_raft_step(path: Path, case: dict, step: dict) -> None:
    location = f"{path}: case {case['name']} step {step['name']}"
    if step["command"].get("mode") == "static":
        return
    rust_runner = step["command"].get("rust_runner")
    if not isinstance(rust_runner, str) or not rust_runner:
        raise SystemExit(f"{location}: cpp_data_raft_parity step must declare rust_runner")
    if "metaserver" in step["name"] or "metaserver" in case["name"]:
        expected_validator = "temporalstore-metaserver-raft-validation"
    elif "production_gate" in step["name"] or case["name"] == "raft_production_gate":
        expected_validator = "temporalstore-raft-distributed-parity-validation"
    else:
        expected_validator = "temporalstore-raft"
    rust_validator = step["command"].get("rust_validator", "")
    if expected_validator not in rust_validator and case["name"] != "raft_production_gate":
        raise SystemExit(
            f"{location}: rust_validator must include {expected_validator!r}"
        )


def validate_byteraft_report_contract(path: Path, case: dict, step: dict) -> None:
    if "byteraft" not in case["name"].lower() and "rustraft" not in case["name"].lower():
        return
    location = f"{path}: case {case['name']} step {step['name']}"
    command = step["command"]
    contract = command.get("report_contract")
    if not isinstance(contract, dict):
        raise SystemExit(f"{location}: ByteRaft shared case must declare report_contract")
    if contract.get("schema") != "temporalstore_unified_case_report_v1":
        raise SystemExit(
            f"{location}: report_contract.schema must be temporalstore_unified_case_report_v1"
        )
    required_fields = contract.get("required_fields")
    expected = {"schema", "producer", "generated_at_ms", "cases"}
    if not isinstance(required_fields, list) or not expected.issubset(set(required_fields)):
        raise SystemExit(
            f"{location}: report_contract.required_fields must include "
            + ", ".join(sorted(expected))
        )
    case_fields = set(contract.get("case_fields") or [])
    if not {"name", "status", "steps"}.issubset(case_fields):
        raise SystemExit(f"{location}: report_contract.case_fields must include name, status, steps")
    step_fields = set(contract.get("step_fields") or [])
    if not {"name", "status", "output", "latency_ms"}.issubset(step_fields):
        raise SystemExit(
            f"{location}: report_contract.step_fields must include name, status, output, latency_ms"
        )
    if command.get("cpp_report_adapter") != "compat/cpp_unified_case_report_adapter.h":
        raise SystemExit(
            f"{location}: ByteRaft shared case must declare "
            "cpp_report_adapter=compat/cpp_unified_case_report_adapter.h"
        )
    for field_name in ("rust_report_contract", "cpp_runner_contract", "comparator"):
        if not isinstance(command.get(field_name), str) or not command[field_name]:
            raise SystemExit(f"{location}: ByteRaft shared case must declare {field_name}")
    for required in ("temporalstore_unified_case_report_v1", "latency_ms"):
        if required not in command["cpp_runner_contract"]:
            raise SystemExit(f"{location}: cpp_runner_contract must include {required!r}")
    comparator = command["comparator"]
    for required in (
        "tools/compare_unified_cpp_rust_case_reports.py",
        "--require-schema temporalstore_unified_case_report_v1",
        "--require-field cases",
        "--require-field producer",
        "--require-field generated_at_ms",
    ):
        if required not in comparator:
            raise SystemExit(f"{location}: comparator must include {required!r}")


def validate_rustraft_fault_acceptance(path: Path, case: dict, step: dict) -> None:
    expected = RUSTRAFT_FAULT_ACCEPTANCE_KEYWORDS.get(case["name"])
    if expected is None:
        return
    location = f"{path}: case {case['name']} step {step['name']}"
    criteria = step["command"].get("acceptance_criteria")
    if not isinstance(criteria, list) or len(criteria) < len(expected):
        raise SystemExit(f"{location}: RustRaft fault case must declare acceptance_criteria")
    normalized = [" ".join(str(item).lower().split()) for item in criteria]
    for keyword_group in expected:
        if not any(all(keyword in criterion for keyword in keyword_group) for criterion in normalized):
            raise SystemExit(
                f"{location}: acceptance_criteria missing keywords "
                + ", ".join(keyword_group)
            )


def validate_combined_raft_case(path: Path, case: dict) -> None:
    gate = case.get("rust_parity_gate")
    validator = case.get("rust_parity_validator")
    if case["name"] == "raft_production_gate":
        step_runners = " ".join(
            step.get("command", {}).get("rust_runner", "") for step in case.get("steps", [])
        )
        if COMBINED_RAFT_GATE not in step_runners:
            raise SystemExit(
                f"{path}: case {case['name']} rust_runner must include {COMBINED_RAFT_GATE}"
            )
        step_validators = " ".join(
            step.get("command", {}).get("rust_validator", "") for step in case.get("steps", [])
        )
        if "temporalstore-raft-distributed-parity-validation" not in step_validators:
            raise SystemExit(
                f"{path}: case {case['name']} rust_validator must include combined Raft validation"
            )
        return
    if gate != COMBINED_RAFT_GATE:
        raise SystemExit(
            f"{path}: case {case['name']} rust_parity_gate must be {COMBINED_RAFT_GATE!r}"
        )
    if validator != COMBINED_RAFT_VALIDATOR:
        raise SystemExit(
            f"{path}: case {case['name']} rust_parity_validator must be {COMBINED_RAFT_VALIDATOR!r}"
        )


def run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+ " + " ".join(shlex.quote(part) for part in cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def run_rust(corpus: Path) -> None:
    env = os.environ.copy()
    env["TS_UNIFIED_TEMPORALSTORE_CORPUS"] = str(corpus)
    run(
        [
            "cargo",
            "test",
            "-p",
            "temporalstore-rust",
            "--test",
            "unified_temporalstore_corpus",
            "--",
            "--test-threads=1",
        ],
        env=env,
    )


def render_cpp_command(command: str, corpus: Path, cpp_repo: Path | None) -> str:
    values = {"corpus": str(corpus)}
    if cpp_repo is not None:
        values["cpp_repo"] = str(cpp_repo)
    if "{corpus}" in command or "{cpp_repo}" in command:
        return command.format(**values)
    return f"{command} {shlex.quote(str(corpus))}"


def discover_cpp_command(cpp_repo: Path | None) -> str | None:
    command = os.environ.get("TS_CPP_UNIFIED_TEST_CMD")
    if command:
        return command
    if cpp_repo is None:
        return None
    candidate = cpp_repo / DEFAULT_CPP_RUNNER_RELATIVE
    if candidate.exists():
        return f"{shlex.quote(str(candidate))} --corpus {{corpus}}"
    return None


def install_cpp_runner(cpp_repo: Path, overwrite: bool) -> Path:
    target = cpp_repo / DEFAULT_CPP_RUNNER_RELATIVE
    if target.exists() and not overwrite:
        raise SystemExit(
            f"C++ unified runner already exists: {target}; pass --overwrite-cpp-runner to replace it"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(CPP_RUNNER_TEMPLATE, encoding="utf-8")
    target.chmod(0o755)
    return target


def run_cpp(corpus: Path, required: bool, native_required: bool, cpp_repo: Path | None) -> None:
    direct_command = os.environ.get("TS_CPP_UNIFIED_TEST_CMD")
    native_command = os.environ.get("TS_CPP_UNIFIED_NATIVE_CMD")
    if native_required and not direct_command and not native_command:
        raise SystemExit(
            "--require-cpp-native needs TS_CPP_UNIFIED_TEST_CMD or "
            "TS_CPP_UNIFIED_NATIVE_CMD so the C++ side executes the corpus, "
            "not only the discovery/surface hook"
        )
    command = discover_cpp_command(cpp_repo)
    if not command:
        message = (
            "no C++ unified corpus runner configured; set TS_CPP_UNIFIED_TEST_CMD "
            "to the C++ corpus runner command, optionally using {corpus} and "
            "{cpp_repo} placeholders, or set TS_CPP_REPO/--cpp-repo to a checkout "
            "containing tools/run_temporalstore_unified_tests.sh"
        )
        if required:
            raise SystemExit(message)
        print(f"warning: {message}", file=sys.stderr)
        return

    rendered = render_cpp_command(command, corpus, cpp_repo)
    cwd = cpp_repo if cpp_repo is not None else ROOT
    print(f"+ {rendered}", flush=True)
    subprocess.run(rendered, cwd=cwd, shell=True, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--rust", action="store_true", help="run the Rust corpus executor")
    parser.add_argument("--cpp", action="store_true", help="run the C++ corpus executor")
    parser.add_argument(
        "--both",
        action="store_true",
        help="run both Rust and C++ corpus executors",
    )
    parser.add_argument(
        "--cpp-repo",
        type=Path,
        default=Path(os.environ["TS_CPP_REPO"]) if os.environ.get("TS_CPP_REPO") else None,
        help="C++ TemporalStore checkout root; also used as cwd for the C++ runner",
    )
    parser.add_argument(
        "--require-cpp",
        action="store_true",
        help="fail if --cpp is requested but TS_CPP_UNIFIED_TEST_CMD is unset",
    )
    parser.add_argument(
        "--require-cpp-native",
        action="store_true",
        help="fail unless a native C++ corpus executor is configured with TS_CPP_UNIFIED_TEST_CMD or TS_CPP_UNIFIED_NATIVE_CMD",
    )
    parser.add_argument(
        "--family",
        help="run only shared corpus cases for one coverage.cpp_adapter_coverage family",
    )
    parser.add_argument("--validate-only", action="store_true", help="only validate corpus JSON")
    parser.add_argument(
        "--install-cpp-runner",
        action="store_true",
        help="write tools/run_temporalstore_unified_tests.sh into --cpp-repo using the shared wrapper contract",
    )
    parser.add_argument(
        "--overwrite-cpp-runner",
        action="store_true",
        help="allow --install-cpp-runner to replace an existing C++ wrapper",
    )
    parser.add_argument(
        "--print-cpp-runner-template",
        action="store_true",
        help="print the installable C++ wrapper template and exit",
    )
    args = parser.parse_args()

    if args.print_cpp_runner_template:
        print(CPP_RUNNER_TEMPLATE, end="")
        return 0

    corpus = args.corpus.resolve()
    data = validate_corpus(corpus)
    run_corpus = corpus
    if args.family:
        run_corpus = filtered_corpus_for_family(data, corpus, args.family)
    print(
        f"validated {data['name']} schema={data['schema_version']} "
        f"cases={len(data['cases'])} path={corpus}"
    )
    if args.family:
        filtered_data = json.loads(run_corpus.read_text(encoding="utf-8"))
        print(
            f"using family-filtered corpus family={args.family} "
            f"cases={len(filtered_data['cases'])} path={run_corpus}"
        )

    if args.validate_only:
        return 0
    install_only = args.install_cpp_runner and not args.rust and not args.cpp and not args.both
    if args.both:
        args.rust = True
        args.cpp = True
    if not args.rust and not args.cpp and not install_only:
        args.rust = True
    cpp_repo = args.cpp_repo.resolve() if args.cpp_repo is not None else None
    if cpp_repo is not None and not cpp_repo.exists():
        raise SystemExit(f"C++ repo does not exist: {cpp_repo}")
    if args.install_cpp_runner:
        if cpp_repo is None:
            raise SystemExit("--install-cpp-runner requires --cpp-repo or TS_CPP_REPO")
        target = install_cpp_runner(cpp_repo, args.overwrite_cpp_runner)
        print(f"installed C++ unified runner: {target}")
        if install_only:
            return 0
    if args.rust:
        run_rust(run_corpus)
    if args.cpp:
        run_cpp(run_corpus, args.require_cpp or args.require_cpp_native, args.require_cpp_native, cpp_repo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
