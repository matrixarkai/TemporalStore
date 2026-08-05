//! Context-pipeline benchmark + sweep helpers, split from context_workflow.rs.
use super::*;

pub fn run_context_pipeline_benchmark(
    engine: &TemporalEngine,
    request: ContextPipelineBenchmarkRequest,
) -> ContextPipelineBenchmarkReport {
    let source_count = request.source_count.clamp(1, 10_000);
    let query_count = request.query_count.clamp(1, 1_000);
    let profile = if request.profile.trim().is_empty() {
        default_benchmark_profile()
    } else {
        request.profile.clone()
    };
    let provider = normalize_provider(request.provider.clone());
    let mut total_source_tokens = 0u32;
    let mut sources = Vec::with_capacity(source_count);
    let mut topic_source_counts = vec![0usize; query_count];
    for index in 0..source_count {
        let source_kind = benchmark_source_kind(index);
        let topic_index = index % query_count;
        topic_source_counts[topic_index] += 1;
        let body = benchmark_context_body(index, topic_index, topic_source_counts[topic_index]);
        total_source_tokens = total_source_tokens.saturating_add(estimate_tokens(&body));
        sources.push(ContextExtractRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            source_kind,
            source_id: format!("bench-context-{index}"),
            title: format!("Benchmark context item {index}"),
            body,
            timestamp_ms: 1_000 + index as u64,
            provider: provider.clone(),
        });
    }

    let ingest_start = Instant::now();
    let ingest = ingest_extract_context(
        engine,
        ContextIngestExtractRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            sources,
            query: "checkout benchmark".to_string(),
            start_time_ms: 0,
            end_time_ms: 1_000 + source_count as u64 + 1,
            max_events: request.max_events,
            provider: provider.clone(),
        },
    );
    let ingest_extract_elapsed_ms = ingest_start.elapsed().as_millis();

    let mut retrieve_latencies = Vec::with_capacity(query_count);
    let mut inject_latencies = Vec::with_capacity(query_count);
    let mut retrieval_successes = 0usize;
    let mut injection_successes = 0usize;
    let mut selected_context_tokens = 0u32;
    let mut max_selected_tokens_per_query = 0u32;
    let mut total_retrieved_blocks = 0usize;
    let mut total_selected_blocks = 0usize;
    let mut retrieve_total_elapsed_ms = 0u128;
    let mut inject_total_elapsed_ms = 0u128;
    let mut reciprocal_rank_sum = 0.0f32;
    let mut hit_count = 0usize;
    let mut retained_evidence_count = 0usize;
    let mut per_query = Vec::with_capacity(query_count);
    let min_sources_per_topic = topic_source_counts
        .iter()
        .copied()
        .min()
        .unwrap_or_default();
    let max_sources_per_topic = topic_source_counts
        .iter()
        .copied()
        .max()
        .unwrap_or_default();

    for query_index in 0..query_count {
        let expected_topic = format!("topic {query_index}");
        let query_id = format!("bench-query-{query_index}");
        let retrieve_request = ContextRetrieveRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            node_hashes: ingest.node_hashes.clone(),
            query: benchmark_query_for_topic(query_index),
            start_time_ms: 0,
            end_time_ms: 1_000 + source_count as u64 + 1,
            max_events: request.max_events,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: default_summary_fanout_node_limit(),
            max_event_nodes: default_event_fanout_node_limit(),
            prefer_current_agent: false,
            current_agent_scope_key: default_current_agent_scope_key(),
            provider: ContextModelProviderConfig::default(),
        };
        let retrieve_start = Instant::now();
        let retrieve = retrieve_context(engine, retrieve_request.clone());
        let retrieve_elapsed = retrieve_start.elapsed().as_millis();
        retrieve_latencies.push(retrieve_elapsed);
        retrieve_total_elapsed_ms += retrieve_elapsed;
        if retrieve.status.ok && !retrieve.blocks.is_empty() {
            retrieval_successes += 1;
        }
        let hit_rank = retrieve
            .blocks
            .iter()
            .position(|block| {
                block
                    .text
                    .to_ascii_lowercase()
                    .contains(expected_topic.as_str())
            })
            .map(|index| index + 1);
        let reciprocal_rank = hit_rank.map(|rank| 1.0 / rank as f32).unwrap_or(0.0);
        if hit_rank.is_some() {
            hit_count += 1;
        }
        reciprocal_rank_sum += reciprocal_rank;

        let inject_start = Instant::now();
        let inject = inject_context(
            engine,
            ContextInjectRequest {
                retrieve: retrieve_request,
                prompt: format!("Answer benchmark query {query_index}."),
                session_hash: 42_000 + query_index as u64,
                query_id: query_id.clone(),
                max_prompt_tokens: 256,
                provider: ContextModelProviderConfig::default(),
            },
        );
        let inject_elapsed = inject_start.elapsed().as_millis();
        inject_latencies.push(inject_elapsed);
        inject_total_elapsed_ms += inject_elapsed;
        let selected_tokens = inject
            .selected_blocks
            .iter()
            .map(|block| block.estimated_tokens)
            .sum::<u32>();
        max_selected_tokens_per_query = max_selected_tokens_per_query.max(selected_tokens);
        total_retrieved_blocks += retrieve.blocks.len();
        total_selected_blocks += inject.selected_blocks.len();
        if inject.status.ok {
            injection_successes += 1;
            selected_context_tokens = selected_context_tokens.saturating_add(selected_tokens);
        }
        let evidence_retained = inject.selected_blocks.iter().any(|block| {
            block
                .text
                .to_ascii_lowercase()
                .contains(expected_topic.as_str())
        });
        if evidence_retained {
            retained_evidence_count += 1;
        }
        per_query.push(ContextPipelineBenchmarkQueryReport {
            query_id,
            expected_topic,
            expected_topic_source_count: topic_source_counts
                .get(query_index)
                .copied()
                .unwrap_or_default(),
            retrieved_blocks: retrieve.blocks.len(),
            selected_blocks: inject.selected_blocks.len(),
            selected_tokens,
            evidence_retained,
            hit_rank,
            reciprocal_rank,
            retrieve_elapsed_ms: retrieve_elapsed,
            inject_elapsed_ms: inject_elapsed,
        });
    }

    retrieve_latencies.sort_unstable();
    inject_latencies.sort_unstable();
    let retrieve_p50_ms = percentile_latency(&retrieve_latencies, 50);
    let retrieve_p95_ms = percentile_latency(&retrieve_latencies, 95);
    let inject_p50_ms = percentile_latency(&inject_latencies, 50);
    let inject_p95_ms = percentile_latency(&inject_latencies, 95);
    let full_context_query_tokens = total_source_tokens.saturating_mul(query_count as u32);
    let token_reduction_percent =
        token_reduction_percent(full_context_query_tokens, selected_context_tokens);
    let recall_at_k = retrieval_successes as f32 / query_count as f32;
    let hit_at_k = hit_count as f32 / query_count as f32;
    let mean_reciprocal_rank = reciprocal_rank_sum / query_count as f32;
    let evidence_retention_at_k = retained_evidence_count as f32 / query_count as f32;
    let zero_hit_queries = query_count.saturating_sub(hit_count);
    let avg_retrieved_blocks_per_query = total_retrieved_blocks as f64 / query_count as f64;
    let avg_selected_blocks_per_query = total_selected_blocks as f64 / query_count as f64;
    let avg_selected_tokens_per_query = selected_context_tokens as f64 / query_count as f64;
    let threshold_violations = benchmark_threshold_violations(
        &request.thresholds,
        hit_at_k,
        mean_reciprocal_rank,
        recall_at_k,
        evidence_retention_at_k,
        token_reduction_percent,
        max_selected_tokens_per_query,
        retrieve_p50_ms,
        retrieve_p95_ms,
        rate_per_sec(source_count, ingest_extract_elapsed_ms),
        rate_per_sec(query_count, retrieve_total_elapsed_ms),
        rate_per_sec(query_count, inject_total_elapsed_ms),
    );
    let threshold_passed = threshold_violations.is_empty();
    let status = if ingest.status.ok
        && retrieval_successes == query_count
        && injection_successes == query_count
        && hit_count == query_count
        && retained_evidence_count == query_count
        && threshold_passed
    {
        Status::ok()
    } else {
        Status::error(
            "context_pipeline_benchmark_incomplete",
            format!(
                "accepted={} failed={} retrieval_successes={} injection_successes={} queries={} threshold_violations={:?}",
                ingest.accepted,
                ingest.failed,
                retrieval_successes,
                injection_successes,
                query_count,
                threshold_violations
            ),
        )
    };

    ContextPipelineBenchmarkReport {
        status,
        benchmark_name: "vikingmem_style_context_management_local".to_string(),
        workload_signature: stable_hash64(&format!(
            "context-benchmark:{profile}:{source_count}:{query_count}:{}:{}",
            request.max_events, provider.provider_name
        )),
        topic_count: query_count,
        min_sources_per_topic,
        max_sources_per_topic,
        source_kind_coverage_count: ingest.summary.source_kind_counts.len(),
        profile,
        source_count,
        query_count,
        accepted_sources: ingest.accepted,
        failed_sources: ingest.failed,
        retrieval_successes,
        injection_successes,
        hit_at_k,
        mean_reciprocal_rank,
        total_source_tokens: full_context_query_tokens,
        selected_context_tokens,
        token_reduction_percent,
        recall_at_k,
        evidence_retention_at_k,
        ingest_extract_elapsed_ms,
        retrieve_total_elapsed_ms,
        inject_total_elapsed_ms,
        ingest_sources_per_sec: rate_per_sec(source_count, ingest_extract_elapsed_ms),
        retrieve_queries_per_sec: rate_per_sec(query_count, retrieve_total_elapsed_ms),
        inject_queries_per_sec: rate_per_sec(query_count, inject_total_elapsed_ms),
        retrieve_p50_ms,
        retrieve_p95_ms,
        inject_p50_ms,
        inject_p95_ms,
        avg_retrieved_blocks_per_query,
        avg_selected_blocks_per_query,
        avg_selected_tokens_per_query,
        max_selected_tokens_per_query,
        zero_hit_queries,
        thresholds: request.thresholds,
        threshold_passed,
        threshold_violations,
        per_query,
        source_kind_counts: ingest.summary.source_kind_counts,
        provider_counts: ingest.summary.provider_counts,
        evidence: vec![
            "VikingMem-style local benchmark covers extraction, hierarchical retrieval, budgeted injection, latency, hit@k, MRR, throughput, recall proxy, evidence retention, and token reduction".to_string(),
            "Synthetic workload uses mixed Context source kinds and deterministic local providers".to_string(),
        ],
    }
}

pub fn run_context_pipeline_benchmark_sweep(
    engine: &TemporalEngine,
    request: ContextPipelineBenchmarkSweepRequest,
) -> ContextPipelineBenchmarkSweepReport {
    let profiles = if request.profiles.is_empty() {
        default_benchmark_sweep_profiles()
    } else {
        request.profiles.clone()
    };
    let mut reports = Vec::with_capacity(profiles.len());
    for (index, profile) in profiles.into_iter().enumerate() {
        reports.push(run_context_pipeline_benchmark(
            engine,
            ContextPipelineBenchmarkRequest {
                shard_id: request.shard_id,
                tenant_hash: request.tenant_hash + index as u64,
                profile: profile.profile,
                source_count: profile.source_count,
                query_count: profile.query_count,
                max_events: profile.max_events,
                provider: request.provider.clone(),
                thresholds: request.thresholds.clone(),
            },
        ));
    }
    let profile_count = reports.len();
    let all_profiles_ready = reports.iter().all(|report| report.status.ok);
    let min_hit_at_k = reports
        .iter()
        .map(|report| report.hit_at_k)
        .fold(1.0f32, f32::min);
    let min_mean_reciprocal_rank = reports
        .iter()
        .map(|report| report.mean_reciprocal_rank)
        .fold(1.0f32, f32::min);
    let min_evidence_retention_at_k = reports
        .iter()
        .map(|report| report.evidence_retention_at_k)
        .fold(1.0f32, f32::min);
    let min_token_reduction_percent = reports
        .iter()
        .map(|report| report.token_reduction_percent)
        .fold(100.0f32, f32::min);
    let max_retrieve_p95_ms = reports
        .iter()
        .map(|report| report.retrieve_p95_ms)
        .max()
        .unwrap_or_default();
    let max_inject_p95_ms = reports
        .iter()
        .map(|report| report.inject_p95_ms)
        .max()
        .unwrap_or_default();
    let total_sources = reports.iter().map(|report| report.source_count).sum();
    let total_queries = reports.iter().map(|report| report.query_count).sum();
    let profile_signatures = reports
        .iter()
        .map(|report| report.workload_signature)
        .collect::<Vec<_>>();
    let min_sources_per_topic = reports
        .iter()
        .map(|report| report.min_sources_per_topic)
        .min()
        .unwrap_or_default();
    let max_sources_per_topic = reports
        .iter()
        .map(|report| report.max_sources_per_topic)
        .max()
        .unwrap_or_default();
    let min_source_kind_coverage_count = reports
        .iter()
        .map(|report| report.source_kind_coverage_count)
        .min()
        .unwrap_or_default();
    let total_zero_hit_queries = reports.iter().map(|report| report.zero_hit_queries).sum();
    let total_selected_tokens = reports
        .iter()
        .map(|report| report.selected_context_tokens as u64)
        .sum::<u64>();
    let max_selected_tokens_per_query = reports
        .iter()
        .map(|report| report.max_selected_tokens_per_query)
        .max()
        .unwrap_or_default();
    let avg_selected_tokens_per_query = if total_queries == 0 {
        0.0
    } else {
        total_selected_tokens as f64 / total_queries as f64
    };
    let all_thresholds_passed = reports.iter().all(|report| report.threshold_passed);
    let threshold_violations = reports
        .iter()
        .flat_map(|report| {
            report
                .threshold_violations
                .iter()
                .map(|violation| format!("{}:{violation}", report.profile))
        })
        .collect::<Vec<_>>();
    let status = if profile_count > 0
        && all_profiles_ready
        && all_thresholds_passed
        && min_hit_at_k >= 1.0
        && min_mean_reciprocal_rank > 0.0
        && min_evidence_retention_at_k >= 1.0
        && min_token_reduction_percent > 0.0
    {
        Status::ok()
    } else {
        Status::error(
            "context_pipeline_benchmark_sweep_incomplete",
            format!(
                "profiles={profile_count} ready={all_profiles_ready} min_hit_at_k={min_hit_at_k:.3} min_mrr={min_mean_reciprocal_rank:.3} min_evidence_retention={min_evidence_retention_at_k:.3} min_token_reduction={min_token_reduction_percent:.3}"
            ),
        )
    };

    ContextPipelineBenchmarkSweepReport {
        status,
        benchmark_name: "vikingmem_style_context_management_sweep".to_string(),
        profile_count,
        reports,
        all_profiles_ready,
        min_hit_at_k,
        min_mean_reciprocal_rank,
        min_evidence_retention_at_k,
        min_token_reduction_percent,
        max_retrieve_p95_ms,
        max_inject_p95_ms,
        total_sources,
        total_queries,
        profile_signatures,
        min_sources_per_topic,
        max_sources_per_topic,
        min_source_kind_coverage_count,
        total_zero_hit_queries,
        avg_selected_tokens_per_query,
        max_selected_tokens_per_query,
        all_thresholds_passed,
        threshold_violations,
        evidence: vec![
            "Benchmark sweep runs multiple deterministic profile sizes through the same Context pipeline".to_string(),
            "Sweep aggregates readiness, threshold gates, hit@k, MRR, evidence retention, token budget, token reduction, latency, total source count, and total query count".to_string(),
        ],
    }
}

pub(crate) fn benchmark_threshold_violations(
    thresholds: &ContextPipelineBenchmarkThresholds,
    hit_at_k: f32,
    mean_reciprocal_rank: f32,
    recall_at_k: f32,
    evidence_retention_at_k: f32,
    token_reduction_percent: f32,
    max_selected_tokens_per_query: u32,
    retrieve_p50_ms: u128,
    retrieve_p95_ms: u128,
    ingest_sources_per_sec: f64,
    retrieve_queries_per_sec: f64,
    inject_queries_per_sec: f64,
) -> Vec<String> {
    let mut violations = Vec::new();
    if hit_at_k < thresholds.min_hit_at_k {
        violations.push(format!(
            "hit_at_k {hit_at_k:.3} below {:.3}",
            thresholds.min_hit_at_k
        ));
    }
    if mean_reciprocal_rank < thresholds.min_mean_reciprocal_rank {
        violations.push(format!(
            "mean_reciprocal_rank {mean_reciprocal_rank:.3} below {:.3}",
            thresholds.min_mean_reciprocal_rank
        ));
    }
    if recall_at_k < thresholds.min_recall_at_k {
        violations.push(format!(
            "recall_at_k {recall_at_k:.3} below {:.3}",
            thresholds.min_recall_at_k
        ));
    }
    if evidence_retention_at_k < thresholds.min_evidence_retention_at_k {
        violations.push(format!(
            "evidence_retention_at_k {evidence_retention_at_k:.3} below {:.3}",
            thresholds.min_evidence_retention_at_k
        ));
    }
    if token_reduction_percent < thresholds.min_token_reduction_percent {
        violations.push(format!(
            "token_reduction_percent {token_reduction_percent:.3} below {:.3}",
            thresholds.min_token_reduction_percent
        ));
    }
    if max_selected_tokens_per_query > thresholds.max_selected_tokens_per_query {
        violations.push(format!(
            "max_selected_tokens_per_query {max_selected_tokens_per_query} above {}",
            thresholds.max_selected_tokens_per_query
        ));
    }
    if retrieve_p50_ms > thresholds.max_retrieve_p50_ms {
        violations.push(format!(
            "retrieve_p50_ms {retrieve_p50_ms} above {}",
            thresholds.max_retrieve_p50_ms
        ));
    }
    if retrieve_p95_ms > thresholds.max_retrieve_p95_ms {
        violations.push(format!(
            "retrieve_p95_ms {retrieve_p95_ms} above {}",
            thresholds.max_retrieve_p95_ms
        ));
    }
    if ingest_sources_per_sec < thresholds.min_ingest_sources_per_sec {
        violations.push(format!(
            "ingest_sources_per_sec {ingest_sources_per_sec:.3} below {:.3}",
            thresholds.min_ingest_sources_per_sec
        ));
    }
    if retrieve_queries_per_sec < thresholds.min_retrieve_queries_per_sec {
        violations.push(format!(
            "retrieve_queries_per_sec {retrieve_queries_per_sec:.3} below {:.3}",
            thresholds.min_retrieve_queries_per_sec
        ));
    }
    if inject_queries_per_sec < thresholds.min_inject_queries_per_sec {
        violations.push(format!(
            "inject_queries_per_sec {inject_queries_per_sec:.3} below {:.3}",
            thresholds.min_inject_queries_per_sec
        ));
    }
    violations
}

pub(crate) fn context_source_kind_name(kind: ContextSourceKind) -> &'static str {
    match kind {
        ContextSourceKind::Document => "document",
        ContextSourceKind::Chat => "chat",
        ContextSourceKind::Ticket => "ticket",
        ContextSourceKind::Code => "code",
        ContextSourceKind::Incident => "incident",
        ContextSourceKind::UserEvent => "user_event",
    }
}

pub(crate) fn benchmark_source_kind(index: usize) -> ContextSourceKind {
    match index % 6 {
        0 => ContextSourceKind::Incident,
        1 => ContextSourceKind::Ticket,
        2 => ContextSourceKind::Document,
        3 => ContextSourceKind::Chat,
        4 => ContextSourceKind::Code,
        _ => ContextSourceKind::UserEvent,
    }
}

pub(crate) fn benchmark_context_body(index: usize, topic_index: usize, topic_sequence: usize) -> String {
    let is_latest_update = topic_sequence > 1;
    let update_marker = if is_latest_update {
        "latest memory update"
    } else {
        "earlier memory"
    };
    let detail = match (topic_index % 4, is_latest_update) {
        (0, true) => "checkout payment risk score changed after a fraud review, with the current status captured for later QA",
        (0, false) => "checkout payment risk score baseline from the original fraud review remains available as historical context",
        (1, true) => "backend service dependency outage created a current temporal incident timeline and recovery sequence",
        (1, false) => "backend service dependency health snapshot captured the initial incident history before recovery",
        (2, true) => "customer preference was updated during a later conversation session and replaced the stale setting",
        (2, false) => "customer preference captured the original conversation setting before any later change",
        (_, true) => "support ticket follow-up recorded the agent action, user ask, and open helpdesk state",
        (_, false) => "support ticket captured the first user ask before the agent follow-up action",
    };
    format!(
        "VikingMem-style benchmark context item {index}: {update_marker} for topic {topic_index}; {detail}; retrieval hint and follow-up action are preserved."
    )
}

pub(crate) fn benchmark_query_for_topic(topic_index: usize) -> String {
    let topic_phrase = format!("topic {topic_index}");
    match topic_index % 4 {
        0 => format!("latest payment fraud status {topic_phrase}"),
        1 => format!("recent service outage timeline {topic_phrase}"),
        2 => format!("customer preference update conversation {topic_phrase}"),
        _ => format!("support ticket follow up {topic_phrase}"),
    }
}

pub(crate) fn percentile_latency(sorted_latencies: &[u128], percentile: usize) -> u128 {
    if sorted_latencies.is_empty() {
        return 0;
    }
    let rank = ((sorted_latencies.len() - 1) * percentile.min(100)) / 100;
    sorted_latencies[rank]
}

pub(crate) fn token_reduction_percent(full_tokens: u32, selected_tokens: u32) -> f32 {
    if full_tokens == 0 {
        return 0.0;
    }
    let saved = full_tokens.saturating_sub(selected_tokens);
    (saved as f32 * 100.0) / full_tokens as f32
}

pub(crate) fn rate_per_sec(count: usize, elapsed_ms: u128) -> f64 {
    if elapsed_ms == 0 {
        return count as f64;
    }
    (count as f64 * 1000.0) / elapsed_ms as f64
}
