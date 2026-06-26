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

## Resource, Skill, And Conversation Scale Addendum

Run date: 2026-06-25

Command:

```bash
cd /mnt/c/Users/Deeproute/Documents/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore
cargo run -p temporalstore-rust --bin context_workflow_harness \
  > /tmp/temporalstore-context-workflow-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-context-workflow-validation \
  --log /tmp/temporalstore-context-workflow-validation.log
```

Validation result:

```text
temporalstore-context-workflow-validation: JSON validation passed
```

This run extends the harness so Rust TemporalStore now runs a combined resource, skill, and
conversation scale slice through the same engine used by the context workflow gate. The path uses
`ingest_resource_skill_context` for governed resources and `SKILL.md` inputs, `ingest_extract_context`
for conversation turns, `retrieve_context` for summary-driven retrieval, and
`validate_resource_skill_secondary_indexes` for post-ingest secondary-index queryability.

| Metric | Value |
| --- | --- |
| `resource_skill_scale.ready` | `true` |
| `resource_skill_scale.resource_count` | `4` |
| `resource_skill_scale.skill_count` | `3` |
| `resource_skill_scale.conversation_source_count` | `24` |
| `resource_skill_scale.total_source_count` | `37` |
| `resource_skill_scale.accepted_sources` | `37` |
| `resource_skill_scale.failed_sources` | `0` |
| `resource_skill_scale.retrieved_block_count` | `102` |
| `resource_skill_scale.retrieved_event_count` | `28` |
| `resource_skill_scale.selected_skill_count` | `3` |
| `resource_skill_scale.resource_lifecycle_watched_count` | `3` |
| `resource_skill_scale.skill_registry_enabled_count` | `3` |
| `resource_skill_scale.skill_registry_disabled_count` | `0` |
| `resource_skill_scale.ingest_ms` | `29402` |
| `resource_skill_scale.retrieve_ms` | `559` |
| `resource_skill_scale.secondary_index_validation_ms` | `1` |

### Context Model Fanout

| Model / Index | Count |
| --- | --- |
| `fanout_node_count` | `13` |
| `fanout_event_count` | `13` |
| `fanout_segment_count` | `13` |
| `fanout_entity_count` | `13` |
| `fanout_child_ref_count` | `13` |
| `fanout_embedding_count` | `39` |
| `fanout_summary_count` | `26` |
| `fanout_compression_count` | `13` |
| `fanout_dirty_marker_count` | `13` |
| `fanout_secondary_index_count` | `65` |
| `fanout_ready` | `true` |

### Embeddings, Summary Retrieval, And Secondary Indexes

| Metric | Value |
| --- | --- |
| `embedding_ref_count` | `39` |
| `embedding_requested_vectors` | `39` |
| `embedding_generated_vectors` | `39` |
| `embedding_live_call_count` | `0` |
| `embedding_mock_generation_count` | `13` |
| `embedding_production_evidence_ready` | `false` |
| `summary_embedding_candidate_count` | `37` |
| `summary_embedding_selected_count` | `37` |
| `verbose_filter_group_count` | `1` |
| `selected_ref_count` | `102` |
| `secondary_index_ready` | `true` |
| `secondary_index_checked_refs` | `54` |
| `secondary_index_found_refs` | `54` |
| `secondary_index_missing_refs` | `0` |

The embedding rows are generated by the default deterministic provider in this local run, so this is
Rust-backed scale evidence, not live OSS-model production evidence. A paper-comparable or production
embedding claim still requires a configured live embedding/reader endpoint with `embedding_live_call_count`
and reader model-call metadata greater than zero.

The local benchmark sweep in this harness keeps correctness, evidence-retention, token-reduction, and
latency gates active, and uses an explicit `0.5` qps/source-per-second throughput floor to avoid false
failures from the mounted Windows checkout. Native Ubuntu checkouts should continue to use stricter
throughput/SLO gates for production benchmark evidence.

## Interpretation

This run proves the local Rust TemporalStore context pipeline is active and passing for the built-in
workflow and LOCOMO/LongMemEval-style fixture gates. It does not claim a live VikingMem paper score:
full paper-comparable evidence still requires a real dataset run plus a configured GPT-4o-mini or
OpenAI-compatible reader endpoint with archived model-call metadata.
