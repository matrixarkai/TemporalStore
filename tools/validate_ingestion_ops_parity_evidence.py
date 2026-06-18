#!/usr/bin/env python3
"""Validate Rust evidence for ingestion and production-ops parity gates."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
INGESTION_SUITE = "cpp_ingestion_parity"


@dataclass(frozen=True)
class Evidence:
    path: str
    snippets: tuple[str, ...]


@dataclass(frozen=True)
class IngestionOpsArea:
    name: str
    corpus_case: str | None
    evidence: tuple[Evidence, ...]


AREAS: tuple[IngestionOpsArea, ...] = (
    IngestionOpsArea(
        name="ingestion_kafka_flink_durability",
        corpus_case=None,
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "IngestionBatchRequest",
                    "KafkaOffsetLedgerEntry",
                    "FlinkCheckpointAction",
                    "IngestionDeadLetter",
                    "ingest_batch",
                    "commit_kafka_offset",
                    "compute_max_kafka_lag",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "ingestion_batch_reports_duplicate_kafka_offsets_without_nooping_valid_records",
                    "ingestion_persists_kafka_ledger_dead_letters_lag_and_flink_checkpoints",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="ingestion_metrics_readiness",
        corpus_case=None,
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/engine.rs",
                (
                    "temporalstore_ingestion_kafka_lag",
                    "temporalstore_ingestion_kafka_committed_offset",
                    "temporalstore_ingestion_flink_checkpoint_state",
                    "temporalstore_ingestion_records_total",
                    "dead_letter",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "ingestion_readiness_report",
                    "ingestion_network_runtime_readiness_report",
                    "Kafka consumer group runtime covers partition assignment",
                    "Flink production checkpoint handshake covers precommit",
                    "Raft failover/restart idempotence harness proves committed Kafka offsets",
                    "Kafka lag and ingestion/dead-letter counters",
                    "Prometheus ingestion metrics",
                    "ingestion_readiness_report_tracks_done_and_remaining_production_gaps",
                    "ingestion_network_runtime_readiness_covers_connectors_and_raft_harness",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="production_readiness_ops_matrix",
        corpus_case=None,
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/readiness.rs",
                (
                    "production_readiness_report",
                    "service_readiness_summaries",
                    "production_readiness_report_lists_blockers_for_all_major_services",
                    "production_readiness_report_summarizes_requested_service_readiness",
                    "external_chaos_gate",
                    "durable Kafka offset ledger",
                ),
            ),
            Evidence(
                "tools/validate_readiness_workflow.py",
                (
                    "REQUIRED_SERVICES",
                    "REQUIRED_SNIPPETS",
                    "Validate unified C++/Rust corpus",
                    "Capture production readiness report",
                ),
            ),
            Evidence(
                "docs/ci/rust-production-readiness.workflow.yml",
                (
                    "rust-production-readiness",
                    "readiness_gate",
                    "Run unified local validation",
                    "Run unified Rust corpus",
                    "service-readiness",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="scale_fault_chaos_gates",
        corpus_case=None,
        evidence=(
            Evidence(
                "tools/run_temporalstore_unified_validation.sh",
                (
                    "scale_harness",
                    "storage_modes_harness",
                    "readiness_gate",
                    "validate_aws_validation_log.py",
                ),
            ),
            Evidence(
                "tools/run_temporalstore_parity_gate.sh",
                (
                    "distributed_raft_harness",
                    "storage_fault_matrix_harness",
                    "raft_secondary_replication_harness",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/external_chaos_gate.rs",
                (
                    "ExternalChaosGateReport",
                    "ChaosScenario",
                    "storage_fault_matrix_harness",
                    "raft_secondary_replication_harness",
                    "external_chaos_gate",
                ),
            ),
            Evidence(
                "tools/validate_aws_validation_log.py",
                (
                    "validate_storage_fault_matrix",
                    "validate_raft_secondary",
                    "validate_raft",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="ingestion_kafka_offset_ledger_shared",
        corpus_case="ingestion_kafka_offset_ledger",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "KafkaOffsetLedgerEntry",
                    "commit_kafka_offset",
                    "ingestion_batch_reports_duplicate_kafka_offsets_without_nooping_valid_records",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="ingestion_kafka_rebalance_backpressure_shared",
        corpus_case="ingestion_kafka_rebalance_backpressure",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "KafkaConsumerGroupRuntimeReport",
                    "rebalance_required",
                    "backpressure_active",
                    "kafka_consumer_group_runtime_reports_rebalance_and_backpressure",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="ingestion_flink_checkpoint_lifecycle_shared",
        corpus_case="ingestion_flink_checkpoint_lifecycle",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "FlinkCheckpointAction::Precommit",
                    "FlinkCheckpointAction::Commit",
                    "FlinkCheckpointAction::Abort",
                    "ingestion_persists_kafka_ledger_dead_letters_lag_and_flink_checkpoints",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="ingestion_dead_letter_export_shared",
        corpus_case="ingestion_dead_letter_export",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "IngestionDeadLetter",
                    "dead_letter_export_report",
                    "dead_letter_export_and_raft_failover_idempotence_reports_are_ready",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="ingestion_lag_metrics_shared",
        corpus_case="ingestion_lag_metrics",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "compute_max_kafka_lag",
                    "max_kafka_lag",
                    "Kafka lag and ingestion/dead-letter counters",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/engine.rs",
                ("temporalstore_ingestion_kafka_lag",),
            ),
        ),
    ),
    IngestionOpsArea(
        name="ingestion_restart_idempotence_shared",
        corpus_case="ingestion_restart_idempotence",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/ingestion.rs",
                (
                    "raft_failover_idempotence_report",
                    "committed_offsets_preserved",
                    "flink_checkpoint_preserved",
                    "no_duplicate_writes",
                ),
            ),
        ),
    ),
)


def load_corpus() -> dict:
    with CORPUS.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def case_map(corpus: dict) -> dict[str, dict]:
    cases = corpus.get("cases")
    if not isinstance(cases, list):
        raise SystemExit(f"{CORPUS}: cases must be a list")
    return {case["name"]: case for case in cases}


def validate_corpus_area(area: IngestionOpsArea, cases: dict[str, dict], required: set[str]) -> set[str]:
    if area.corpus_case is None:
        return set()
    if area.corpus_case not in required:
        raise SystemExit(f"{area.name}: {area.corpus_case} missing from coverage.required_case_names")
    case = cases.get(area.corpus_case)
    if case is None:
        raise SystemExit(f"{area.name}: missing shared corpus case {area.corpus_case}")
    steps = case.get("steps") or []
    if not steps:
        raise SystemExit(f"{area.name}: corpus case {area.corpus_case} has no steps")

    paths: set[str] = set()
    for step in steps:
        command = step.get("command", {})
        if command.get("kind") != "existing_test":
            raise SystemExit(f"{area.name}: {area.corpus_case}/{step.get('name')} is not existing_test")
        if command.get("suite") != INGESTION_SUITE:
            raise SystemExit(
                f"{area.name}: {area.corpus_case}/{step.get('name')} suite "
                f"{command.get('suite')!r} != {INGESTION_SUITE!r}"
            )
        required_paths = command.get("required_paths") or []
        if not required_paths:
            raise SystemExit(f"{area.name}: {area.corpus_case}/{step.get('name')} has no required_paths")
        paths.update(required_paths)
    return paths


def validate_area(area: IngestionOpsArea) -> int:
    snippet_count = 0
    for evidence in area.evidence:
        path = ROOT / evidence.path
        if not path.exists():
            raise SystemExit(f"{area.name}: missing evidence file {evidence.path}")
        text = path.read_text(encoding="utf-8", errors="ignore")
        for snippet in evidence.snippets:
            if snippet not in text:
                raise SystemExit(
                    f"{area.name}: evidence file {evidence.path} missing snippet {snippet!r}"
                )
            snippet_count += 1
    return snippet_count


def validate_cpp_paths(area: IngestionOpsArea, paths: set[str], cpp_repo: Path) -> set[str]:
    checked: set[str] = set()
    for required_path in paths:
        if not (cpp_repo / required_path).exists():
            raise SystemExit(f"{area.name}: C++ required path missing: {required_path}")
        checked.add(required_path)
    return checked


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp-repo", type=Path, help="optional C++ checkout for required path checks")
    args = parser.parse_args()

    corpus = load_corpus()
    cases = case_map(corpus)
    required = set(corpus.get("coverage", {}).get("required_case_names", []))
    total_cpp_paths: set[str] = set()
    checked_cpp_paths: set[str] = set()
    total_snippets = 0
    for area in AREAS:
        paths = validate_corpus_area(area, cases, required)
        total_cpp_paths.update(paths)
        total_snippets += validate_area(area)
        if args.cpp_repo is not None:
            checked_cpp_paths.update(validate_cpp_paths(area, paths, args.cpp_repo))
        print(f"validated ingestion/ops parity area: {area.name}")
    print(f"ingestion_ops_parity_areas: {len(AREAS)}")
    print(f"corpus_required_cpp_paths: {len(total_cpp_paths)}")
    print(f"rust_evidence_snippets: {total_snippets}")
    if args.cpp_repo is not None:
        print(f"checked_cpp_required_paths: {len(checked_cpp_paths)}")


if __name__ == "__main__":
    main()
