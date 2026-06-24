#!/usr/bin/env python3
"""Validate the consolidated Rust-vs-C++ TemporalStore parity report."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "docs" / "rust_vs_cpp_temporalstore_parity_report.md"


REQUIRED_PHRASES = [
    "no brpc or Thrift in Rust",
    "no byte-for-byte C++ page/log layout requirement",
    "Rust-native HTTP/JSON, RESP, and tonic",
    "live ByteStore/S3 remains out of scope",
    "`raft_replication`",
    "`storage_cache`",
    "`client` / `proxy`",
    "`data_node`",
    "`metaserver`",
    "`benchmarks`",
    "`scale_testing`",
    "OpenRaft process rollout",
    "Slot dump/load",
    "full Rust TemporalStore replay",
    "VikingMem paper-comparable",
    "C++ static surface gates",
    "ContextEntity",
    "ContextSegment",
    "context_benchmark_injection_entity_segment_index",
    "packed LOCOMO",
    "82 cases",
    "<cpp-temporalstore-checkout>",
    "ContextChildModel",
    "ContextEmbeddingModel",
    "ContextSummaryModel",
    "ContextCompressionModel",
    "context_tree_embedding_summary_compression",
    "context_temporal_compression_replayable_summary",
    "`ContextEntityModel`",
    "`QUERY_NODE_CONTEXT`",
    "`WRITE_EXTRACTED_EVENT`",
    "cross_storage_control_agent_parity",
    "storage dump/load/cache recovery",
    "client/proxy topology refresh",
    "data-node lifecycle barriers",
    "metaserver scheduler tokens",
    "Context agent resource/skill parser",
]


REQUIRED_DOC_LINKS = [
    "docs/storage_raft_production_readiness_plan.md",
    "docs/distributed_raft_readiness.md",
    "docs/client_vs_cpp.md",
    "docs/client_sdk_contract.md",
    "docs/data_node_vs_cpp.md",
    "docs/metaserver_vs_cpp.md",
    "docs/unified_test_case_inventory.md",
    "docs/rust_temporalstore_locomo_longmemeval_benchmark_metrics.md",
    "docs/benchmark_reproducibility_evidence.md",
    "docs/context_benchmark_entity_segment_index_contract.md",
    "docs/cross_storage_control_agent_parity.md",
]


def main() -> None:
    text = REPORT.read_text(encoding="utf-8")
    missing = [phrase for phrase in REQUIRED_PHRASES if phrase not in text]
    missing.extend(link for link in REQUIRED_DOC_LINKS if link not in text)
    if missing:
        details = "\n".join(f"- {item}" for item in missing)
        raise SystemExit(f"{REPORT} is missing required parity evidence text:\n{details}")
    print(f"validated {REPORT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
