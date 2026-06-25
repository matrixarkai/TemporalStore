# Rust TemporalStore Context Pipeline Run Metrics

## Summary

Rust TemporalStore was used for the context ingestion, extraction-style context event storage,
retrieval, injection, shared-store replay, Raft read, and benchmark fixture pipeline. This is a
Rust-backed pipeline run, not a Python-only diagnostic run.

Run date: 2026-06-21

Native checkout used for faster Rust builds:

```bash
/home/vj/temporalstore-rust-native
```

Command:

```bash
cd /home/vj/temporalstore-rust-native
mkdir -p benchmark_reports/context_pipeline
cargo run -p temporalstore-rust --bin context_workflow_harness \
  > benchmark_reports/context_pipeline/rust_context_pipeline_latest.json
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-context-workflow-validation \
  --log benchmark_reports/context_pipeline/rust_context_pipeline_latest.json
```

Validation result:

```text
temporalstore-context-workflow-validation: JSON validation passed
```

## Pipeline Readiness

| Metric | Value |
| --- | --- |
| `context_pipeline_ready` | `true` |
| `pipeline_stage_ready_count` | `7` |
| `ingest_extract_ready` | `true` |
| `retrieve_pipeline_ready` | `true` |
| `management_ready` | `true` |
| `restart_replay_ready` | `true` |
| `shared_store_sync_ready` | `true` |
| `shared_store_async_ready` | `true` |
| `raft_read_ready` | `true` |
| `unified_corpus_ready` | `true` |

## Ingestion And Extraction

| Metric | Value |
| --- | --- |
| `extraction_ok` | `true` |
| `ingest_extract_accepted` | `2` |
| `ingest_extract_failed` | `0` |
| `ingest_extract_source_count` | `2` |
| `ingest_extract_unique_nodes` | `2` |
| `blocked_block_count` | `0` |

## Retrieval And Injection

| Metric | Value |
| --- | --- |
| `retrieve_block_count` | `3` |
| `selected_block_count` | `3` |
| `injected_prompt_contains_context` | `true` |
| `benchmark_retrieve_p50_ms` | `142` |
| `benchmark_retrieve_p95_ms` | `170` |
| `benchmark_retrieve_queries_per_sec` | `4.934210526315789` |
| `benchmark_inject_p50_ms` | `173` |
| `benchmark_inject_p95_ms` | `178` |
| `benchmark_inject_queries_per_sec` | `5.982053838484546` |

## Built-In Context Benchmark

| Metric | Value |
| --- | --- |
| `benchmark_ready` | `true` |
| `benchmark_profile` | `vikingmem_harness_profile` |
| `benchmark_topic_count` | `6` |
| `benchmark_source_count` | `48` |
| `benchmark_query_count` | `6` |
| `benchmark_per_query_count` | `6` |
| `benchmark_hit_at_k` | `1.0` |
| `benchmark_recall_at_k` | `1.0` |
| `benchmark_mean_reciprocal_rank` | `1.0` |
| `benchmark_evidence_retention_at_k` | `1.0` |
| `benchmark_zero_hit_queries` | `0` |
| `benchmark_token_reduction_percent` | `83.67876` |
| `benchmark_avg_retrieved_blocks_per_query` | `144.0` |
| `benchmark_avg_selected_blocks_per_query` | `8.0` |
| `benchmark_avg_selected_tokens_per_query` | `252.0` |
| `benchmark_threshold_passed` | `true` |
| `benchmark_threshold_violation_count` | `0` |

## Benchmark Sweep

| Metric | Value |
| --- | --- |
| `benchmark_sweep_ready` | `true` |
| `benchmark_sweep_profile_count` | `4` |
| `benchmark_sweep_profile_signature_count` | `4` |
| `benchmark_sweep_total_queries` | `20` |
| `benchmark_sweep_total_sources` | `204` |
| `benchmark_sweep_total_zero_hit_queries` | `0` |
| `benchmark_sweep_min_hit_at_k` | `1.0` |
| `benchmark_sweep_min_mean_reciprocal_rank` | `1.0` |
| `benchmark_sweep_min_evidence_retention_at_k` | `1.0` |
| `benchmark_sweep_min_token_reduction_percent` | `57.170174` |
| `benchmark_sweep_max_retrieve_p95_ms` | `353` |
| `benchmark_sweep_max_inject_p95_ms` | `402` |
| `benchmark_sweep_all_thresholds_passed` | `true` |
| `benchmark_sweep_threshold_violation_count` | `0` |

## External LOCOMO / LongMemEval-Style Fixture

| Metric | Value |
| --- | --- |
| `external_benchmark_ready` | `true` |
| `external_benchmark_dataset` | `locomo_style+longmemeval_s_style` |
| `external_benchmark_source` | `built-in-locomo-longmemeval-fixture` |
| `external_benchmark_case_count` | `18` |
| `external_benchmark_category_count` | `7` |
| `external_benchmark_hit_at_k` | `1.0` |
| `external_benchmark_mean_reciprocal_rank` | `0.5555556` |
| `external_benchmark_zero_hit_queries` | `0` |
| `external_benchmark_category_zero_hit_queries` | `0` |
| `external_benchmark_min_category_hit_at_k` | `1.0` |
| `external_benchmark_min_category_mean_reciprocal_rank` | `0.5` |
| `external_benchmark_answer_term_coverage` | `1.0` |
| `external_benchmark_all_expected_terms_matched` | `true` |
| `external_benchmark_missing_expected_terms` | `0` |
| `external_benchmark_missing_expected_refs` | `0` |
| `external_benchmark_rust_context_event_ingest` | `true` |
| `external_benchmark_ingested_source_sets` | `18` |
| `external_benchmark_retrieved_source_sets` | `18` |
| `external_benchmark_total_retrieved_blocks` | `108` |
| `external_benchmark_direct_source_scoring` | `false` |
| `external_benchmark_all_source_replay` | `false` |

## Interpretation

This run proves the local Rust TemporalStore context pipeline is active and passing for the built-in
workflow and LOCOMO/LongMemEval-style fixture gates. It does not claim a live VikingMem paper score:
full paper-comparable evidence still requires a real dataset run plus a configured GPT-4o-mini or
OpenAI-compatible reader endpoint with archived model-call metadata.
