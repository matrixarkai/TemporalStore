// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn evidence_scores_keep_both_signals_without_penalizing_lexical_fallback() {
    assert_eq!(context_evidence_score(Some(200_000), 187_000), 387_000);
    assert_eq!(context_evidence_score(None, 187_000), 187_000);
    assert_eq!(context_evidence_score(Some(-200_000), 187_000), 187_000);
    assert_eq!(context_evidence_score(Some(200_000), 0), 200_000);
    assert_eq!(context_evidence_score(Some(i64::MAX), 1), i64::MAX);
}

#[test]
fn latest_evidence_survives_a_bounded_heap_without_hiding_history() {
    let query = "What is the dog's name in the latest pet update?";
    let texts = [
        "Old profile note: the family dog was called Pepper in a previous home.",
        "Latest pet update: the newly adopted dog is named Miso and needs evening walks.",
        "Latest update: the bicycle has a new chain.",
    ];
    let model = default_embedding_model();
    let query_vector = deterministic_context_embedding(&model, query);
    let plan = context_query_plan(query);
    for limit in [1, 2, 3] {
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2]] {
            let mut heap = BinaryHeap::new();
            for index in order {
                let text = texts[index];
                let semantic = context_embedding_similarity_micros(
                    &query_vector,
                    &deterministic_context_embedding(&model, text),
                );
                let lexical = lexical_score_to_micros(context_relevance_score_plan(&plan, text));
                retain_ranked_context_block(
                    &mut heap,
                    RankedContextBlock {
                        score: context_evidence_score(Some(semantic), lexical),
                        block: ContextBlock {
                            record_type: "raw_message".into(),
                            uri: format!("event:{index}"),
                            tier: ContextTier::L2,
                            node_hash: index as u64,
                            event_time_ms: (index as u64 + 1) * 1_000,
                            text: text.into(),
                            estimated_tokens: estimate_tokens(text),
                            source_ref: String::new(),
                        },
                    },
                    limit,
                );
                assert!(heap.len() <= limit);
            }
            let ranked = take_ranked_context_blocks(heap);
            assert!(ranked[0].block.text.contains("Miso"));
            if limit == 3 {
                assert!(ranked.iter().any(|row| row.block.text.contains("Pepper")));
            }
        }
    }
}
