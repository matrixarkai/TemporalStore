use std::collections::HashSet;

use serde_json::Value;

use crate::matrixark_rust_proxy_pack::{
    candidate_text, context_class_name, sparse_query_score,
};
use crate::matrixark_rust_proxy_scope::{
    continuity_boost, cross_session_rerank_boost, session_continuity_status,
};

pub(crate) struct ScoredCandidate<'a> {
    pub(crate) score: f64,
    pub(crate) record: &'a Value,
    pub(crate) session_continuity: String,
    pub(crate) continuity_boost: f64,
    pub(crate) cross_session_rerank_boost: f64,
}

pub(crate) fn score_retrieve_candidates<'a>(
    records: &'a [Value],
    query_terms: &HashSet<String>,
    scope_for_continuity: Option<&Value>,
    question_type: &str,
    min_similarity_score: f64,
    max_global_candidates: u64,
) -> Vec<ScoredCandidate<'a>> {
    let mut scored: Vec<ScoredCandidate<'a>> = records
        .iter()
        .filter(|record| {
            matches!(
                record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                "context_compression_event"
                    | "context_entity"
                    | "context_event"
                    | "context_segment"
                    | "context_summary"
                    | "resource_chunk"
                    | "skill_section"
            ) && !candidate_text(record).is_empty()
        })
        .map(|record| {
            let text = candidate_text(record);
            let mut score = sparse_query_score(query_terms, &text);
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_entity")
            ) {
                score += 0.08;
            }
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_compression_event")
            ) {
                score += 0.06;
            }
            let context_class = context_class_name(record);
            let session_continuity = session_continuity_status(record, scope_for_continuity);
            let continuity_boost_value =
                continuity_boost(record, &context_class, &session_continuity);
            score += continuity_boost_value;
            let cross_session_rerank_boost_value = cross_session_rerank_boost(
                record,
                &context_class,
                &session_continuity,
                question_type,
            );
            score += cross_session_rerank_boost_value;
            ScoredCandidate {
                score,
                record,
                session_continuity,
                continuity_boost: continuity_boost_value,
                cross_session_rerank_boost: cross_session_rerank_boost_value,
            }
        })
        .filter(|candidate| candidate.score >= min_similarity_score)
        .collect();
    scored.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if scored.len() > max_global_candidates as usize {
        scored.truncate(max_global_candidates as usize);
    }
    scored
}
