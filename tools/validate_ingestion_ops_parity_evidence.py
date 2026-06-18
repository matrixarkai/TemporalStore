#!/usr/bin/env python3
"""Validate Rust evidence for ingestion and production-ops parity gates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Evidence:
    path: str
    snippets: tuple[str, ...]


@dataclass(frozen=True)
class IngestionOpsArea:
    name: str
    evidence: tuple[Evidence, ...]


AREAS: tuple[IngestionOpsArea, ...] = (
    IngestionOpsArea(
        name="ingestion_kafka_flink_durability",
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
                    "ingestion network runtime readiness covers local API ingestion",
                    "Kafka lag and ingestion/dead-letter counters",
                    "Prometheus ingestion metrics",
                    "ingestion_readiness_report_tracks_done_and_remaining_production_gaps",
                    "ingestion_network_runtime_readiness_keeps_real_connectors_blocked",
                ),
            ),
        ),
    ),
    IngestionOpsArea(
        name="production_readiness_ops_matrix",
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
)


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


def main() -> None:
    total_snippets = 0
    for area in AREAS:
        total_snippets += validate_area(area)
        print(f"validated ingestion/ops parity area: {area.name}")
    print(f"ingestion_ops_parity_areas: {len(AREAS)}")
    print(f"rust_evidence_snippets: {total_snippets}")


if __name__ == "__main__":
    main()
