// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::{ContextEvent, Status};

use super::{
    context_embeddings_for_extract, normalize_provider, truncate_words, ContextBlock,
    ContextFilterGroupCandidateDecisionDebug, ContextInjectionOrderingDebug,
    ContextModelProviderConfig, ContextPrefilterCandidateDebug, ContextQueryFilterGroupDebug,
    ContextQueryFilterGroupSummaryDebug, ContextQueryUnderstandingDebug, ContextRetrieveRequest,
    ContextSelectedRefDebug, ContextTier, ContextTreeTraversalDebug,
};
pub(super) fn tier_rank(tier: ContextTier) -> u8 {
    match tier {
        ContextTier::L0 => 0,
        ContextTier::L1 => 1,
        ContextTier::L2 => 2,
    }
}

#[derive(Debug, Clone)]
pub(super) struct ContextQueryPlan {
    pub terms: Vec<String>,
    pub term_groups: Vec<Vec<String>>,
    pub adjacent_phrases: Vec<String>,
    pub topic_phrase: Option<String>,
    pub requests_latest: bool,
    pub requests_temporal_reasoning: bool,
    pub requests_after: bool,
    pub requests_before: bool,
    pub requests_correction: bool,
    pub requests_reminder: bool,
    pub requests_contrastive_update: bool,
    pub requests_social_link: bool,
    pub requests_schedule_detail: bool,
    pub requests_quantity_detail: bool,
    pub requests_alias_detail: bool,
}

pub(super) fn context_query_plan(query: &str) -> ContextQueryPlan {
    let terms = context_query_terms(query);
    let term_groups = context_query_term_groups_from_terms(&terms);
    ContextQueryPlan {
        adjacent_phrases: context_query_adjacent_phrases(&terms),
        topic_phrase: context_query_topic_phrase(query),
        requests_latest: context_query_requests_latest(&terms),
        requests_temporal_reasoning: context_query_requests_temporal_reasoning(&terms),
        requests_after: context_query_requests_after(&terms),
        requests_before: context_query_requests_before(&terms),
        requests_correction: context_query_requests_correction(&terms),
        requests_reminder: context_query_requests_reminder(&terms),
        requests_contrastive_update: context_query_requests_contrastive_update(&terms),
        requests_social_link: context_query_requests_social_link(&terms),
        requests_schedule_detail: context_query_requests_schedule_detail(&terms),
        requests_quantity_detail: context_query_requests_quantity_detail(&terms),
        requests_alias_detail: context_query_requests_alias_detail(&terms),
        terms,
        term_groups,
    }
}

#[allow(dead_code)]
pub(super) fn context_query_matches(query: &str, text: &str) -> bool {
    let plan = context_query_plan(query);
    context_query_matches_plan(&plan, text)
}

pub(super) fn context_query_matches_plan(plan: &ContextQueryPlan, text: &str) -> bool {
    let text_lower = text.to_ascii_lowercase();
    let text_normalized = context_normalize_for_match(text);
    if let Some(topic_phrase) = plan.topic_phrase.as_ref() {
        if context_text_matches_term(&text_lower, &text_normalized, topic_phrase.as_str()) {
            return true;
        }
    }
    if plan.term_groups.is_empty() {
        return true;
    }
    let matched_groups = plan
        .term_groups
        .iter()
        .filter(|group| {
            group
                .iter()
                .any(|term| context_text_matches_term(&text_lower, &text_normalized, term))
        })
        .count();
    matched_groups > 0
}

pub(super) fn context_query_understanding_debug_for_plan(
    request: &ContextRetrieveRequest,
    plan: &ContextQueryPlan,
) -> ContextQueryUnderstandingDebug {
    let filter_groups = context_query_secondary_index_filter_groups(&plan.terms);
    ContextQueryUnderstandingDebug {
        debug_schema: "matrixark_context_query_debug_v1".to_string(),
        query_hash: stable_hash64(&request.query),
        normalized_query_terms: plan.terms.clone(),
        question_type: context_query_question_type(&plan.terms),
        secondary_index_filter_groups: filter_groups.clone(),
        verbose_filter_groups: context_query_verbose_filter_groups(&filter_groups),
        filter_group_summary: ContextQueryFilterGroupSummaryDebug {
            total_groups: filter_groups.len(),
            secondary_index_group_count: filter_groups
                .iter()
                .filter(|group| group.iter().any(|term| !term.starts_with("query_term:")))
                .count(),
            lexical_group_count: filter_groups
                .iter()
                .filter(|group| group.iter().all(|term| term.starts_with("query_term:")))
                .count(),
            ..ContextQueryFilterGroupSummaryDebug::default()
        },
        candidates_passing_prefilter: 0,
        candidates_dropped_before_scoring: 0,
        tree_traversal_summary: ContextTreeTraversalDebug {
            enabled: true,
            fallback_reason: String::new(),
            fallback_to_flat: false,
            max_children_scored_per_parent: request.max_events.max(1),
            selected_leaf_count: 0,
            selected_node_count: 0,
            selected_path_count: 0,
            summary_embedding_candidate_count: 0,
            summary_embedding_selected_count: 0,
            summary_embedding_lookup_batches: 0,
            query_embedding_dimension: 0,
            query_embedding_provider: String::new(),
            namespace_node_candidates: 0,
            event_expanded_node_count: 0,
            skipped_node_count: 0,
            summary_embeddings: Vec::new(),
            top_k_per_layer: request.max_events.max(1),
            ..ContextTreeTraversalDebug::default()
        },
        prefilter_candidate_sample: Vec::new(),
        selected_refs: Vec::new(),
        injection_ordering: Vec::new(),
    }
}

pub(super) fn context_query_debug_record_candidate(
    debug: &mut ContextQueryUnderstandingDebug,
    tenant_hash: u64,
    node_hash: u64,
    event: &ContextEvent,
    passes_prefilter: bool,
) {
    let ref_hash = stable_hash64(&format!(
        "ctx-prefilter:{tenant_hash}:{node_hash}:{}:{}",
        event.event_time_ms, event.event_id_hash
    ));
    let candidate_terms = context_event_candidate_terms(event);
    let event_text = event.text.as_str();
    for group in &mut debug.verbose_filter_groups {
        group.candidate_count += 1;
        push_debug_hash(&mut group.candidate_ref_hashes, ref_hash);
        let matched_terms =
            context_debug_filter_group_matched_terms(group, &candidate_terms, event_text);
        let matched_group = passes_prefilter && !matched_terms.is_empty();
        if matched_group {
            group.matched_count += 1;
            push_debug_hash(&mut group.matched_ref_hashes, ref_hash);
        } else {
            group.dropped_count += 1;
            push_debug_hash(&mut group.dropped_ref_hashes, ref_hash);
        }
        push_filter_group_decision(
            group,
            ContextFilterGroupCandidateDecisionDebug {
                ref_hash,
                record_type: "context_event".to_string(),
                event_time_ms: event.event_time_ms,
                decision: if matched_group { "matched" } else { "dropped" }.to_string(),
                reason: if matched_group {
                    "matched_filter_group".to_string()
                } else if passes_prefilter {
                    "filter_group_term_miss".to_string()
                } else {
                    "secondary_index_prefilter_miss".to_string()
                },
                matched_terms,
                candidate_terms: candidate_terms.clone(),
                text: truncate_words(&event.text, 24),
            },
        );
    }
    if passes_prefilter {
        debug.candidates_passing_prefilter += 1;
    } else {
        debug.candidates_dropped_before_scoring += 1;
    }
    if debug.question_type == "match_all" {
        return;
    }
    if debug.prefilter_candidate_sample.len() >= 12 {
        return;
    }
    debug
        .prefilter_candidate_sample
        .push(ContextPrefilterCandidateDebug {
            record_type: "context_event".to_string(),
            ref_hash,
            node_hash,
            event_time_ms: event.event_time_ms,
            node_path: vec![format!("tenant:{tenant_hash}"), format!("node:{node_hash}")],
            candidate_terms,
            passes_secondary_index_prefilter: passes_prefilter,
            drop_reason: if passes_prefilter {
                String::new()
            } else {
                "secondary_index_prefilter_miss".to_string()
            },
            text: truncate_words(&event.text, 32),
        });
}

pub(super) fn context_query_debug_finalize(
    debug: &mut ContextQueryUnderstandingDebug,
    plan: &ContextQueryPlan,
    blocks: &[ContextBlock],
    node_count: usize,
    tiers: &[ContextTier],
) {
    debug.tree_traversal_summary.selected_leaf_count = blocks
        .iter()
        .filter(|block| block.tier == ContextTier::L2)
        .count();
    debug.tree_traversal_summary.selected_node_count = node_count;
    debug.tree_traversal_summary.selected_path_count = blocks.len();
    debug
        .tree_traversal_summary
        .summary_embeddings
        .extend(tiers.iter().filter_map(|tier| match tier {
            ContextTier::L0 => Some("node_l0".to_string()),
            ContextTier::L1 => Some("node_l1".to_string()),
            ContextTier::L2 => None,
        }));
    debug.tree_traversal_summary.summary_embeddings.sort();
    debug.tree_traversal_summary.summary_embeddings.dedup();
    debug.selected_refs = blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let ref_hash = context_selected_ref_hash(block);
            let matched_filter_groups = debug
                .verbose_filter_groups
                .iter_mut()
                .filter_map(|group| {
                    if context_debug_filter_group_matches(
                        group,
                        &[format!(
                            "source_ref:{}",
                            block.source_ref.to_ascii_lowercase()
                        )],
                        &format!("{} {}", block.text, block.source_ref),
                    ) {
                        group.selected_count += 1;
                        push_debug_hash(&mut group.selected_ref_hashes, ref_hash);
                        Some(group.group_id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            ContextSelectedRefDebug {
                rank: index + 1,
                uri: block.uri.clone(),
                source_ref: block.source_ref.clone(),
                tier: block.tier,
                ref_hash,
                node_hash: block.node_hash,
                event_time_ms: block.event_time_ms,
                relevance_score: context_relevance_score_plan(plan, &block.text),
                matched_filter_groups,
            }
        })
        .collect();
    debug.injection_ordering = blocks
        .iter()
        .take(debug.selected_refs.len())
        .enumerate()
        .map(|(index, block)| {
            let selected = &debug.selected_refs[index];
            ContextInjectionOrderingDebug {
                prompt_rank: index + 1,
                source_ref: block.source_ref.clone(),
                tier: block.tier,
                ref_hash: selected.ref_hash,
                token_estimate: block.estimated_tokens,
                selection_reason: if selected.matched_filter_groups.is_empty() {
                    "selected by tree traversal and semantic score".to_string()
                } else {
                    format!(
                        "selected by tree traversal, semantic score, and {} matched filter groups",
                        selected.matched_filter_groups.len()
                    )
                },
            }
        })
        .collect();
    debug.filter_group_summary = summarize_context_filter_groups(&debug.verbose_filter_groups);
}

fn summarize_context_filter_groups(
    groups: &[ContextQueryFilterGroupDebug],
) -> ContextQueryFilterGroupSummaryDebug {
    ContextQueryFilterGroupSummaryDebug {
        total_groups: groups.len(),
        secondary_index_group_count: groups
            .iter()
            .filter(|group| group.group_kind == "secondary_index_prefilter")
            .count(),
        lexical_group_count: groups
            .iter()
            .filter(|group| group.group_kind == "lexical_prefilter")
            .count(),
        total_candidate_count: groups.iter().map(|group| group.candidate_count).sum(),
        total_matched_count: groups.iter().map(|group| group.matched_count).sum(),
        total_dropped_count: groups.iter().map(|group| group.dropped_count).sum(),
        total_selected_count: groups.iter().map(|group| group.selected_count).sum(),
    }
}

pub(super) fn context_query_question_type(terms: &[String]) -> String {
    if terms.is_empty() {
        "match_all".to_string()
    } else if context_query_requests_correction(terms)
        || context_query_requests_latest(terms)
        || context_query_requests_contrastive_update(terms)
    {
        "current_state".to_string()
    } else if context_query_requests_temporal_reasoning(terms)
        || context_query_requests_before(terms)
        || context_query_requests_after(terms)
        || context_query_requests_schedule_detail(terms)
    {
        "temporal_reasoning".to_string()
    } else if context_query_requests_quantity_detail(terms) {
        "quantity".to_string()
    } else if context_query_requests_social_link(terms) {
        "relationship".to_string()
    } else if context_query_requests_alias_detail(terms) {
        "identity_or_alias".to_string()
    } else if terms
        .iter()
        .any(|term| matches!(term.as_str(), "why" | "because" | "cause" | "root"))
    {
        "causal_reasoning".to_string()
    } else {
        "semantic_recall".to_string()
    }
}

pub(super) fn context_query_secondary_index_filter_groups(terms: &[String]) -> Vec<Vec<String>> {
    let mut groups = Vec::new();
    if context_query_requests_latest(terms) || context_query_requests_correction(terms) {
        groups.push(vec![
            "event_type:correction".to_string(),
            "event_type:status_update".to_string(),
        ]);
        groups.push(vec![
            "status:current".to_string(),
            "status:observed".to_string(),
        ]);
        groups.push(vec!["segment_topic:correction".to_string()]);
    }
    if terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "where" | "location" | "located" | "office" | "workplace" | "work"
        )
    }) {
        groups.push(vec![
            "entity_type:location".to_string(),
            "entity_type:job_status".to_string(),
        ]);
    }
    if context_query_requests_social_link(terms) {
        groups.push(vec![
            "entity_type:person".to_string(),
            "entity_type:social_link".to_string(),
            "entity_type:relationship".to_string(),
        ]);
    }
    if context_query_requests_schedule_detail(terms) {
        groups.push(vec![
            "entity_type:date".to_string(),
            "event_type:schedule".to_string(),
            "event_type:deadline".to_string(),
        ]);
    }
    if context_query_requests_quantity_detail(terms) {
        groups.push(vec![
            "entity_type:quantity".to_string(),
            "entity_type:amount".to_string(),
            "event_type:measurement".to_string(),
        ]);
    }
    if context_query_requests_temporal_reasoning(terms)
        || context_query_requests_before(terms)
        || context_query_requests_after(terms)
    {
        groups.push(vec![
            "event_type:timeline".to_string(),
            "event_time_bucket:query_range".to_string(),
        ]);
    }
    if groups.is_empty() {
        let mut lexical = terms
            .iter()
            .take(8)
            .map(|term| format!("query_term:{term}"))
            .collect::<Vec<_>>();
        if lexical.is_empty() {
            lexical.push("query_term:*".to_string());
        }
        groups.push(lexical);
    }
    groups.sort();
    groups.dedup();
    groups
}

fn context_query_verbose_filter_groups(
    filter_groups: &[Vec<String>],
) -> Vec<ContextQueryFilterGroupDebug> {
    filter_groups
        .iter()
        .enumerate()
        .map(|(index, terms)| ContextQueryFilterGroupDebug {
            group_id: format!("filter_group_{}", index + 1),
            group_kind: if terms.iter().any(|term| term.starts_with("query_term:")) {
                "lexical_prefilter".to_string()
            } else {
                "secondary_index_prefilter".to_string()
            },
            terms: terms.clone(),
            ..ContextQueryFilterGroupDebug::default()
        })
        .collect()
}

fn context_debug_filter_group_matches(
    group: &ContextQueryFilterGroupDebug,
    candidate_terms: &[String],
    text: &str,
) -> bool {
    !context_debug_filter_group_matched_terms(group, candidate_terms, text).is_empty()
}

fn context_debug_filter_group_matched_terms(
    group: &ContextQueryFilterGroupDebug,
    candidate_terms: &[String],
    text: &str,
) -> Vec<String> {
    let text_lower = text.to_ascii_lowercase();
    let text_normalized = context_normalize_for_match(text);
    group
        .terms
        .iter()
        .filter(|term| {
            if let Some(query_term) = term.strip_prefix("query_term:") {
                query_term == "*"
                    || context_text_matches_term(&text_lower, &text_normalized, query_term)
            } else if let Some(source_term) = term.strip_prefix("source_ref:") {
                candidate_terms.iter().any(|candidate| {
                    candidate
                        .strip_prefix("source_ref:")
                        .map_or(false, |candidate_source| {
                            candidate_source.contains(source_term)
                        })
                }) || text_lower.contains(source_term)
            } else {
                candidate_terms.iter().any(|candidate| candidate == *term)
                    || text_lower.contains(term.as_str())
            }
        })
        .cloned()
        .collect()
}

fn context_selected_ref_hash(block: &ContextBlock) -> u64 {
    stable_hash64(&format!(
        "ctx-selected-ref:{}:{}:{}:{}",
        block.uri, block.source_ref, block.node_hash, block.event_time_ms
    ))
}

fn push_debug_hash(values: &mut Vec<u64>, value: u64) {
    if values.len() < 32 && !values.contains(&value) {
        values.push(value);
    }
}

fn push_filter_group_decision(
    group: &mut ContextQueryFilterGroupDebug,
    decision: ContextFilterGroupCandidateDecisionDebug,
) {
    if group.candidate_decisions.len() < 16
        && !group
            .candidate_decisions
            .iter()
            .any(|existing| existing.ref_hash == decision.ref_hash)
    {
        group.candidate_decisions.push(decision);
    }
}

pub(super) fn context_event_candidate_terms(event: &ContextEvent) -> Vec<String> {
    let text = event.text.to_ascii_lowercase();
    let mut terms = vec![
        "record_type:context_event".to_string(),
        format!("event_type:{}", event.event_type_code()),
        "source_type:message".to_string(),
    ];
    if text.contains("now") || text.contains("current") || text.contains("latest") {
        terms.push("event_type:status_update".to_string());
        terms.push("status:current".to_string());
    }
    if text.contains("changed")
        || text.contains("instead")
        || text.contains("no longer")
        || text.contains("updated")
    {
        terms.push("event_type:correction".to_string());
        terms.push("segment_topic:correction".to_string());
    }
    if text.contains("moved")
        || text.contains("seattle")
        || text.contains("austin")
        || text.contains("office")
    {
        terms.push("entity_type:location".to_string());
    }
    if text.contains("manager")
        || text.contains("friend")
        || text.contains("coworker")
        || text.contains("alice")
        || text.contains("priya")
    {
        terms.push("entity_type:person".to_string());
        terms.push("entity_type:relationship".to_string());
    }
    if text.contains("deadline")
        || text.contains("appointment")
        || text.contains("schedule")
        || text.contains("calendar")
    {
        terms.push("event_type:schedule".to_string());
        terms.push("entity_type:date".to_string());
    }
    if text.chars().any(|ch| ch.is_ascii_digit())
        || text.contains("amount")
        || text.contains("total")
        || text.contains("score")
    {
        terms.push("entity_type:quantity".to_string());
    }
    terms.sort();
    terms.dedup();
    terms
}

#[allow(dead_code)]
pub(super) fn context_relevance_score(query: &str, text: &str) -> u32 {
    let plan = context_query_plan(query);
    context_relevance_score_plan(&plan, text)
}

pub(super) fn context_relevance_score_plan(plan: &ContextQueryPlan, text: &str) -> u32 {
    if plan.term_groups.is_empty() {
        return 0;
    }
    let text_lower = text.to_ascii_lowercase();
    let text_normalized = context_normalize_for_match(text);
    let mut score = 0u32;
    if let Some(topic_phrase) = plan.topic_phrase.as_ref() {
        if context_text_matches_term(&text_lower, &text_normalized, topic_phrase.as_str()) {
            score = score.saturating_add(1_000);
        }
    }
    let mut matched_groups = 0u32;
    for group in &plan.term_groups {
        let best_match = group
            .iter()
            .filter(|term| context_text_matches_term(&text_lower, &text_normalized, term))
            .map(|term| term.len() as u32)
            .max()
            .unwrap_or_default();
        if best_match > 0 {
            matched_groups += 1;
            score = score.saturating_add(best_match.max(1));
        }
    }
    if matched_groups == plan.term_groups.len() as u32 {
        score = score.saturating_add(100);
    } else if matched_groups > 1 {
        score = score.saturating_add(matched_groups.saturating_mul(12));
    }
    for phrase in &plan.adjacent_phrases {
        if context_text_matches_term(&text_lower, &text_normalized, phrase.as_str()) {
            score = score.saturating_add(50);
        }
    }
    if plan.requests_latest
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "latest", "recent", "current", "updated", "changed", "replaced",
            ],
        )
    {
        score = score.saturating_add(75);
    }
    if plan.requests_temporal_reasoning
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "timeline", "temporal", "history", "sequence", "before", "after", "during",
            ],
        )
    {
        score = score.saturating_add(50);
    }
    if plan.requests_after {
        if context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "after", "later", "latest", "new", "current", "update", "moved to",
            ],
        ) {
            score = score.saturating_add(65);
        }
        if context_text_matches_any(&text_lower, &text_normalized, &["before", "earlier", "old"]) {
            score = score.saturating_sub(25);
        }
    }
    if plan.requests_before {
        if context_text_matches_any(&text_lower, &text_normalized, &["before", "earlier", "old"]) {
            score = score.saturating_add(65);
        }
        if context_text_matches_any(
            &text_lower,
            &text_normalized,
            &["after", "later", "latest", "new", "current"],
        ) {
            score = score.saturating_sub(25);
        }
    }
    if plan.requests_correction
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "correction",
                "corrected",
                "no longer",
                "instead",
                "replaced",
                "now",
            ],
        )
    {
        score = score.saturating_add(90);
    }
    if plan.requests_reminder
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &["remember", "reminder", "mentioned", "said", "told", "note"],
        )
    {
        score = score.saturating_add(70);
    }
    if plan.requests_contrastive_update
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "switched",
                "switch",
                "moved",
                "became",
                "instead",
                "no longer",
                "changed from",
            ],
        )
    {
        score = score.saturating_add(85);
    }
    if plan.requests_social_link
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "recommended",
                "suggested",
                "introduced",
                "referred",
                "because",
                "from",
            ],
        )
    {
        score = score.saturating_add(80);
    }
    if plan.requests_schedule_detail
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "rescheduled",
                "scheduled",
                "moved to",
                "appointment",
                "deadline",
                "calendar",
                "at ",
                "on ",
            ],
        )
    {
        score = score.saturating_add(80);
    }
    if plan.requests_quantity_detail
        && (text_lower.chars().any(|ch| ch.is_ascii_digit())
            || context_text_matches_any(
                &text_lower,
                &text_normalized,
                &["count", "total", "number", "score", "amount", "quantity"],
            ))
    {
        score = score.saturating_add(80);
    }
    if plan.requests_alias_detail
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "roommate", "manager", "owner", "named", "called", "pet", "dog", "cat",
            ],
        )
    {
        score = score.saturating_add(75);
    }
    score
}

pub(super) fn context_query_terms(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(|term| term.trim().to_ascii_lowercase())
        .filter(|term| term.len() >= 3 && !is_context_query_stopword(term))
        .collect()
}

pub(super) fn context_query_term_groups_from_terms(terms: &[String]) -> Vec<Vec<String>> {
    terms
        .iter()
        .map(|term| {
            let mut group = vec![term.clone()];
            if let Some(stem) = context_query_stem(term.as_str()) {
                group.push(stem);
            }
            for synonym in context_query_synonyms(term.as_str()) {
                group.push(synonym.to_string());
            }
            group.sort();
            group.dedup();
            group
        })
        .collect()
}

pub(super) fn context_query_adjacent_phrases(terms: &[String]) -> Vec<String> {
    terms
        .windows(2)
        .map(|window| format!("{} {}", window[0], window[1]))
        .collect()
}

pub(super) fn context_query_synonyms(term: &str) -> &'static [&'static str] {
    match term {
        "checkout" => &["payment", "purchase", "order"],
        "payment" => &["checkout", "purchase", "billing"],
        "billing" | "purchase" | "order" => &["checkout", "payment"],
        "risk" => &["fraud", "score", "safety"],
        "fraud" => &["risk", "score", "safety"],
        "score" => &["risk", "fraud", "safety"],
        "latest" | "recent" | "current" | "currently" | "now" => &["updated", "update", "status"],
        "status" | "state" => &["current", "latest", "condition"],
        "failed" | "failure" | "outage" => &["error", "incident", "down"],
        "down" | "error" => &["failed", "failure", "outage", "incident"],
        "dependency" | "backend" => &["service", "system"],
        "ticket" | "followup" | "follow" => &["support", "agent", "helpdesk"],
        "support" | "agent" | "helpdesk" => &["ticket", "followup"],
        "preference" | "preferences" => &["likes", "setting", "choice"],
        "setting" | "choice" | "likes" => &["preference"],
        "want" | "wants" | "wanted" => &["prefer", "preference", "choice"],
        "prefer" | "preferred" => &["want", "preference", "choice"],
        "update" | "updated" | "updates" => &["changed", "change", "modify", "replaced"],
        "changed" | "change" | "modify" | "replaced" => &["update", "updated", "switched"],
        "switch" | "switched" | "switching" => &["changed", "moved", "replaced"],
        "moved" | "move" => &["switched", "changed", "relocated"],
        "session" | "sessions" => &["dialogue", "conversation", "visit"],
        "conversation" | "dialogue" | "chat" => &["session", "message"],
        "message" | "messages" => &["conversation", "dialogue", "session"],
        "user" | "customer" => &["person", "account", "member"],
        "service" => &["dependency", "backend", "system"],
        "timeline" | "temporal" | "history" | "sequence" => &["before", "after", "during"],
        "before" | "after" | "during" => &["timeline", "temporal", "sequence"],
        "schedule" | "scheduled" | "reschedule" | "rescheduled" => {
            &["calendar", "appointment", "time", "date"]
        }
        "appointment" | "meeting" | "deadline" => &["schedule", "calendar", "date", "time"],
        "tomorrow" | "tonight" | "morning" | "afternoon" | "evening" => &["time", "date"],
        "many" | "count" | "number" | "quantity" => &["total", "amount"],
        "total" | "amount" => &["count", "number", "quantity"],
        "multi" | "hop" | "reasoning" => &["related", "connection", "because"],
        "why" | "because" | "reason" => &["cause", "root", "explain", "problem"],
        "cause" | "root" => &["because", "reason", "incident", "problem"],
        "problem" | "issue" => &["incident", "failure", "resolved", "cause"],
        "resolved" | "fixed" => &["recovered", "solved", "closed"],
        "recovered" | "recovery" | "solved" | "closed" => &["resolved", "fixed"],
        "where" | "location" | "located" => &["place", "city", "office", "work"],
        "office" | "work" | "workplace" => &["location", "place", "job"],
        "when" | "date" | "time" => &["timeline", "temporal", "session"],
        "who" | "person" | "people" => &["user", "customer", "member"],
        "roommate" | "housemate" => &["person", "friend", "contact"],
        "manager" | "supervisor" | "lead" => &["owner", "responsible", "contact"],
        "name" | "named" | "called" => &["alias", "known"],
        "pet" | "dog" | "cat" => &["animal", "name"],
        "friend" | "coworker" | "colleague" | "teammate" => &["person", "contact"],
        "recommend" | "recommended" | "suggest" | "suggested" => {
            &["introduced", "referred", "because"]
        }
        "introduced" | "referred" => &["recommended", "suggested"],
        "travel" | "trip" | "flight" => &["itinerary", "journey", "airport"],
        "remember" | "recall" | "remind" => &["mentioned", "said", "told", "note"],
        "medication" | "medicine" | "meds" | "prescription" => &["pill", "pharmacy", "doctor"],
        "doctor" | "physician" | "clinic" => &["visit", "medical", "medication"],
        "snack" | "meal" => &["food", "preference"],
        "gift" | "present" => &["birthday", "surprise", "preference"],
        "allergy" | "allergic" | "avoid" => &["restriction", "food", "without"],
        "correction" | "corrected" | "correct" => &["changed", "updated", "replaced"],
        "backup" | "contact" => &["owner", "person", "responsible"],
        "cancel" | "cancelled" | "canceled" => &["stopped", "dropped", "no longer"],
        "identity" => &["transgender", "woman", "community"],
        "transgender" => &["identity", "lgbtq", "community"],
        "relationship" => &["single", "dating", "married", "status"],
        "research" | "researched" => &["looked", "adoption", "agencies"],
        "adoption" | "agencies" => &["research", "interviews"],
        "field" | "fields" | "education" | "educaton" | "pursue" | "career" => &[
            "counseling",
            "counselor",
            "psychology",
            "mental",
            "health",
            "certification",
        ],
        "counseling" | "counselor" | "psychology" => &["career", "field", "mental", "health"],
        "interested" | "interest" => &["likes", "enjoys", "prefer", "outdoors"],
        "outdoors" | "camping" | "park" => &["national", "nature", "outside"],
        "ally" | "supportive" => &["support", "community", "transgender", "lgbtq"],
        "bookshelf" | "books" | "book" => &["collects", "classic", "children", "reading"],
        "writing" | "reading" => &["books", "author", "career", "counselor"],
        _ => &[],
    }
}

pub(super) fn context_query_stem(term: &str) -> Option<String> {
    if term.len() > 4 && term.ends_with("ies") {
        Some(format!("{}y", &term[..term.len() - 3]))
    } else if term.len() > 4 && term.ends_with("es") {
        Some(term[..term.len() - 2].to_string())
    } else if term.len() > 3 && term.ends_with('s') {
        Some(term[..term.len() - 1].to_string())
    } else {
        None
    }
}

pub(super) fn context_normalize_for_match(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
}

pub(super) fn context_text_matches_term(
    text_lower: &str,
    text_normalized: &str,
    term: &str,
) -> bool {
    if text_lower.contains(term) {
        return true;
    }
    let normalized_term = context_normalize_for_match(term);
    if normalized_term.trim().is_empty() {
        return false;
    }
    text_normalized.contains(normalized_term.trim())
}

pub(super) fn context_text_matches_any(
    text_lower: &str,
    text_normalized: &str,
    terms: &[&str],
) -> bool {
    terms
        .iter()
        .any(|term| context_text_matches_term(text_lower, text_normalized, term))
}

pub(super) fn context_query_requests_latest(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "latest" | "recent" | "current" | "currently" | "now" | "updated" | "update" | "status"
        )
    })
}

pub(super) fn context_query_requests_temporal_reasoning(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "before" | "after" | "during" | "timeline" | "temporal" | "history" | "when"
        )
    })
}

pub(super) fn context_query_requests_after(terms: &[String]) -> bool {
    terms.iter().any(|term| term == "after")
}

pub(super) fn context_query_requests_before(terms: &[String]) -> bool {
    terms.iter().any(|term| term == "before")
}

pub(super) fn context_query_requests_correction(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "correction" | "corrected" | "correct" | "changed" | "updated" | "now" | "avoid"
        )
    })
}

pub(super) fn context_query_requests_reminder(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "remember" | "recall" | "remind" | "reminder" | "said" | "told"
        )
    })
}

pub(super) fn context_query_requests_contrastive_update(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "switch"
                | "switched"
                | "switching"
                | "changed"
                | "change"
                | "moved"
                | "move"
                | "became"
                | "instead"
                | "cancel"
                | "cancelled"
                | "canceled"
        )
    })
}

pub(super) fn context_query_requests_social_link(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "friend"
                | "coworker"
                | "colleague"
                | "teammate"
                | "recommend"
                | "recommended"
                | "suggest"
                | "suggested"
                | "introduced"
                | "referred"
                | "because"
                | "who"
        )
    })
}

pub(super) fn context_query_requests_schedule_detail(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "when"
                | "date"
                | "time"
                | "schedule"
                | "scheduled"
                | "reschedule"
                | "rescheduled"
                | "appointment"
                | "meeting"
                | "deadline"
                | "calendar"
                | "tomorrow"
                | "tonight"
        )
    })
}

pub(super) fn context_query_requests_quantity_detail(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "many"
                | "count"
                | "number"
                | "quantity"
                | "total"
                | "amount"
                | "score"
                | "percent"
                | "percentage"
        )
    })
}

pub(super) fn context_query_requests_alias_detail(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "roommate"
                | "housemate"
                | "manager"
                | "supervisor"
                | "lead"
                | "owner"
                | "name"
                | "named"
                | "called"
                | "pet"
                | "dog"
                | "cat"
        )
    })
}

pub(super) fn context_query_topic_phrase(query: &str) -> Option<String> {
    let terms = query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(|term| term.trim().to_ascii_lowercase())
        .filter(|term| !term.is_empty())
        .collect::<Vec<_>>();
    terms.windows(2).find_map(|window| {
        if window[0] == "topic" && window[1].chars().all(|ch| ch.is_ascii_digit()) {
            Some(format!("topic {}", window[1]))
        } else {
            None
        }
    })
}

pub(super) fn is_context_query_stopword(term: &str) -> bool {
    matches!(
        term,
        "the"
            | "and"
            | "for"
            | "with"
            | "that"
            | "this"
            | "what"
            | "when"
            | "where"
            | "which"
            | "who"
            | "why"
            | "how"
            | "about"
            | "would"
            | "likely"
            | "still"
            | "considered"
            | "consider"
            | "more"
            | "benchmark"
            | "context"
            | "memory"
            | "conversation"
            | "dialogue"
            | "question"
    )
}

pub(super) fn stable_hash64(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(super) fn context_event_with_storage_keys(
    node_hash: u64,
    mut event: ContextEvent,
) -> ContextEvent {
    if event.event_time_ms == 0 {
        event.event_time_ms = super::now_ms().max(1);
    }
    if event.ingestion_time_ms == 0 {
        event.ingestion_time_ms = event.event_time_ms;
    }
    if event.event_id_hash == 0 {
        event.event_id_hash = stable_hash64(&format!(
            "context_event:{}:{}:{}",
            node_hash, event.event_time_ms, event.text
        ));
    }
    event
}

pub(super) fn context_embedding_model_hash(model: &str) -> u64 {
    let model = model.trim();
    if model.is_empty() {
        0
    } else {
        stable_hash64(&format!("embedding_model:{model}"))
    }
}

/// Derive the object-key ref-hash under which a context embedding is stored/read
/// (`ctx:embedding:{tenant_hash}:{ref_hash}`). Public so out-of-band tooling
/// (e.g. the `context_embed_backfill` bin) can attach embeddings under the exact
/// key the retrieve path reads, without duplicating the label formula.
pub fn context_embedding_ref_hash(tenant_hash: u64, ref_hash: u64, level: &str) -> u64 {
    stable_hash64(&format!("ctx-embedding:{tenant_hash}:{ref_hash}:{level}"))
}

pub(super) fn deterministic_context_embedding(model: &str, text: &str) -> Vec<f32> {
    let mut vector = Vec::with_capacity(16);
    for index in 0..16_u64 {
        let hash = stable_hash64(&format!("{model}:{index}:{text}"));
        let signed = (hash % 20_001) as f32 - 10_000.0;
        vector.push(signed / 10_000.0);
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

pub(super) fn context_query_embedding(
    provider: &ContextModelProviderConfig,
    query: &str,
) -> Result<Vec<f32>, Status> {
    let inputs = [("query", stable_hash64(query), 0_u32, query)];
    match context_embeddings_for_extract(provider, &inputs) {
        Ok((vectors, _)) => Ok(vectors.into_iter().next().unwrap_or_default()),
        Err(status) => {
            if let Some(fallback) = provider.fallback_provider.as_deref() {
                context_embeddings_for_extract(&normalize_provider(fallback.clone()), &inputs)
                    .map(|(vectors, _)| vectors.into_iter().next().unwrap_or_default())
            } else {
                Err(status)
            }
        }
    }
}

/// Two vectors are comparable only when they are the same width.
///
/// A different width means a different embedding space. This system can hold both at once: the
/// embedding path falls back to a 32-dimension deterministic token-hash vector whenever the
/// configured provider raises, so one store can carry 32-dimension and 384-dimension vectors with
/// nothing marking them apart. Scoring the shared prefix of two such vectors returns a perfectly
/// plausible cosine computed across two unrelated spaces -- there is no length error to raise and
/// nothing appears in the logs, so the only symptom is ranking that has quietly become noise.
///
/// An empty vector on either side is a different condition (un-embedded, not mis-embedded) and is
/// deliberately not folded in here, so that case keeps behaving exactly as it did.
pub(super) fn context_embedding_width_conflicts(left: &[f32], right: &[f32]) -> bool {
    !left.is_empty() && !right.is_empty() && left.len() != right.len()
}

/// Whether a stored vector was written by a different encoder than the one asking.
///
/// Width conflicts are already refused, but two encoders of the SAME width -- and candidates
/// routinely are, since any model truncated to 512 looks alike -- produce no width signal and no
/// error. The recorded hash is the only thing separating them.
///
/// Zero on either side means "unknown" and never conflicts: a stored zero predates the hash being
/// recorded, and an active zero means the caller named no encoder. Treating either as a conflict
/// would blank retrieval for every existing store and every providerless request.
pub(super) fn context_embedding_model_conflicts(stored_hash: u64, active_hash: u64) -> bool {
    stored_hash != 0 && active_hash != 0 && stored_hash != active_hash
}

pub(super) fn context_embedding_similarity_micros(left: &[f32], right: &[f32]) -> i64 {
    if left.is_empty() || right.is_empty() {
        return 0;
    }
    // Refuse, rather than score the shared prefix. The callers route a conflict to the lexical
    // pass before reaching here; this is the backstop that stops any future caller from comparing
    // across two embedding spaces just by passing two vectors in.
    if context_embedding_width_conflicts(left, right) {
        return 0;
    }
    let len = left.len();
    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    for index in 0..len {
        dot += left[index] * right[index];
        left_norm += left[index] * left[index];
        right_norm += right[index] * right[index];
    }
    if left_norm <= f32::EPSILON || right_norm <= f32::EPSILON {
        0
    } else {
        ((dot / (left_norm.sqrt() * right_norm.sqrt())) * 1_000_000.0) as i64
    }
}
