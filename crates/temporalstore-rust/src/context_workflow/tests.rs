// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn with_secondary_indexes<T>(body: impl FnOnce() -> T + std::panic::UnwindSafe) -> T {
    // The gate is read per call, and CI runs --test-threads=1, so set/restore around the body
    // is race-free there; the restore survives a failing assertion.
    std::env::set_var("MATRIXARK_CONTEXT_SECONDARY_INDEX", "1");
    let outcome = std::panic::catch_unwind(body);
    std::env::remove_var("MATRIXARK_CONTEXT_SECONDARY_INDEX");
    match outcome {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn test_engine() -> TemporalEngine {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine
}

// shared-corpus: context_retrieval_batch_node_summary_lookup
#[test]
fn context_get_nodes_batches_summary_lookup_for_retrieval() {
    let engine = test_engine();
    let tenant_hash = 91;
    for node_hash in [11, 22] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash,
                node: ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 0,
                    canonical_name: format!("node-{node_hash}"),
                    l0: format!("l0 summary for node {node_hash}"),
                    status: 0,
                    last_event_time_ms: 1_000 + node_hash,
                    l1_ref: format!("l1 summary for node {node_hash}"),
                    raw_metadata_ref: format!("source://node/{node_hash}"),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                },
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNodes {
            tenant_hash,
            node_hashes: vec![11, 22, 33],
        },
    });

    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::ContextNodes { nodes } => {
            assert_eq!(nodes.len(), 2);
            assert_eq!(nodes[0].node_hash, 11);
            assert_eq!(nodes[1].node_hash, 22);
            assert!(nodes[0].l0.contains("node 11"));
            // l1_ref is a deprecated hot-schema field dropped from the persisted wire
            // (its L1 summary now lives in ContextSummary records); assert the
            // surviving l0 summary content for node 22 instead.
            assert!(nodes[1].l0.contains("node 22"));
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

// shared-corpus: context_retrieval_qa_synonym_ranking
#[test]
fn context_relevance_ranks_qa_synonyms_and_phrases() {
    let relevant =
        "Checkout incident: payment safety score spiked after a backend dependency outage.";
    let distractor = "Support ticket: user asked for help updating a notification preference.";

    assert!(context_query_matches("payment fraud score", relevant));
    assert!(
        context_relevance_score("payment fraud score", relevant)
            > context_relevance_score("payment fraud score", distractor)
    );
    assert!(
        context_relevance_score("service timeline outage", relevant)
            > context_relevance_score("service timeline outage", distractor)
    );
    assert!(context_relevance_score("payment fraud score", relevant) >= 100);

    let updated_memory = benchmark_context_body(7, 2, 4);
    let stale_memory = benchmark_context_body(1, 2, 1);
    assert!(context_query_matches(
        "latest customer preference update topic 2",
        &updated_memory
    ));
    assert!(
        context_relevance_score("latest customer preference update topic 2", &updated_memory)
            > context_relevance_score("latest customer preference update topic 2", &stale_memory)
    );

    let locomo_memory = "During the latest conversation, Alice replaced her office preference with the downtown location after the billing issue was resolved.";
    let locomo_stale =
        "Earlier conversation memory: Alice preferred the airport office before the later change.";
    assert!(context_query_matches(
        "What is Alice's current office choice after the payment problem?",
        locomo_memory
    ));
    assert!(
        context_relevance_score(
            "What is Alice's current office choice after the payment problem?",
            locomo_memory
        ) > context_relevance_score(
            "What is Alice's current office choice after the payment problem?",
            locomo_stale
        )
    );
    let debug_terms = context_query_terms("Where is Alice currently located?");
    let debug_groups = context_query_secondary_index_filter_groups(&debug_terms);
    assert_eq!(context_query_question_type(&debug_terms), "current_state");
    assert!(debug_groups
        .iter()
        .any(|group| group.iter().any(|term| term == "entity_type:location")));
    assert!(debug_groups
        .iter()
        .any(|group| group.iter().any(|term| term == "event_type:status_update")));

    let longmem_memory = "Support follow-up: the user sent messages across sessions and the helpdesk agent changed the notification setting during the most recent chat.";
    assert!(context_query_matches(
        "Which preference was updated in the recent multi session messages?",
        longmem_memory
    ));

    let corrected_memory = "Latest correction: Jordan no longer avoids almonds; Jordan should avoid peanuts now because of a new food restriction.";
    let stale_food_memory =
        "Earlier chat: Jordan said almonds were the only snack to avoid and peanuts were fine.";
    assert!(context_query_matches(
        "What snack should Jordan avoid now after the correction?",
        corrected_memory
    ));
    assert!(
        context_relevance_score(
            "What snack should Jordan avoid now after the correction?",
            corrected_memory
        ) > context_relevance_score(
            "What snack should Jordan avoid now after the correction?",
            stale_food_memory
        )
    );

    let medication_memory = "In the later session Morgan said to remember lisinopril, the blood pressure medication, before the doctor appointment.";
    let stale_clinic_memory =
        "A previous clinic message mentioned bringing an insurance card to the physician visit.";
    assert!(context_query_matches(
        "Which medication did Morgan say to remember before the doctor appointment?",
        medication_memory
    ));
    assert!(
        context_relevance_score(
            "Which medication did Morgan say to remember before the doctor appointment?",
            medication_memory
        ) > context_relevance_score(
            "Which medication did Morgan say to remember before the doctor appointment?",
            stale_clinic_memory
        )
    );

    let hobby_switch = "Later update: Priya cancelled guitar lessons and switched to a pottery class instead for the spring session.";
    let stale_hobby = "Earlier conversation: Priya planned guitar lessons and had not picked a replacement hobby yet.";
    assert!(context_query_matches(
        "Which hobby did Priya switch to after cancelling guitar lessons?",
        hobby_switch
    ));
    assert!(
        context_relevance_score(
            "Which hobby did Priya switch to after cancelling guitar lessons?",
            hobby_switch
        ) > context_relevance_score(
            "Which hobby did Priya switch to after cancelling guitar lessons?",
            stale_hobby
        )
    );

    let current_backup =
            "Most recent staffing update: Sam moved teams, so Riley became the backup contact for payment escalation now.";
    let stale_backup =
        "Old support note: Sam was the backup contact for payment escalation before the team move.";
    assert!(context_query_matches(
        "Who is the backup contact now after Sam moved teams?",
        current_backup
    ));
    assert!(
        context_relevance_score(
            "Who is the backup contact now after Sam moved teams?",
            current_backup
        ) > context_relevance_score(
            "Who is the backup contact now after Sam moved teams?",
            stale_backup
        )
    );

    let cafe_recommendation = "Later chat: Omar recommended the quiet riverside cafe, and Nina booked it after the conference.";
    let stale_cafe = "Earlier conversation: Nina wanted to book a cafe after the conference but had not chosen one yet.";
    assert!(context_query_matches(
        "Who recommended the cafe that Nina booked after the conference?",
        cafe_recommendation
    ));
    assert!(
        context_relevance_score(
            "Who recommended the cafe that Nina booked after the conference?",
            cafe_recommendation
        ) > context_relevance_score(
            "Who recommended the cafe that Nina booked after the conference?",
            stale_cafe
        )
    );

    let project_suggestion = "Later planning note: Dana suggested the observability dashboard because the team needed better benchmark traces, so Lee picked that project.";
    let stale_project = "Initial planning thread: Lee considered a search cleanup project and had not chosen the final work item.";
    assert!(context_query_matches(
        "Which project did Lee pick because Dana suggested it during planning?",
        project_suggestion
    ));
    assert!(
        context_relevance_score(
            "Which project did Lee pick because Dana suggested it during planning?",
            project_suggestion
        ) > context_relevance_score(
            "Which project did Lee pick because Dana suggested it during planning?",
            stale_project
        )
    );

    let rescheduled_appointment = "Latest calendar update: Maya rescheduled the dentist appointment to Thursday at 3pm after the clinic called.";
    let stale_appointment =
        "Earlier memory: Maya had a dentist appointment scheduled for Tuesday morning.";
    assert!(context_query_matches(
        "When is Maya's dentist appointment after it was rescheduled?",
        rescheduled_appointment
    ));
    assert!(
        context_relevance_score(
            "When is Maya's dentist appointment after it was rescheduled?",
            rescheduled_appointment
        ) > context_relevance_score(
            "When is Maya's dentist appointment after it was rescheduled?",
            stale_appointment
        )
    );

    let updated_deadline = "Calendar update: the report deadline moved to June 24 so the benchmark review could finish first.";
    let stale_deadline =
        "Old planning note: the report deadline was June 17 before the later schedule change.";
    assert!(context_query_matches(
        "What is the new report deadline after the calendar update?",
        updated_deadline
    ));
    assert!(
        context_relevance_score(
            "What is the new report deadline after the calendar update?",
            updated_deadline
        ) > context_relevance_score(
            "What is the new report deadline after the calendar update?",
            stale_deadline
        )
    );

    let updated_guest_count =
        "Final RSVP update: Sofia confirmed 7 guests for dinner after two neighbors joined.";
    let stale_guest_count =
        "Earlier dinner plan: Sofia expected 4 guests before the final RSVP update.";
    assert!(context_query_matches(
        "How many guests did Sofia confirm after the dinner update?",
        updated_guest_count
    ));
    assert!(
        context_relevance_score(
            "How many guests did Sofia confirm after the dinner update?",
            updated_guest_count
        ) > context_relevance_score(
            "How many guests did Sofia confirm after the dinner update?",
            stale_guest_count
        )
    );

    let updated_risk_score =
            "Latest fraud review: the checkout risk score was updated to 87 after the payment incident escalated.";
    let stale_risk_score =
            "Earlier fraud review: the checkout risk score was 42 before the payment incident escalated.";
    assert!(context_query_matches(
        "What risk score was recorded after the latest fraud review?",
        updated_risk_score
    ));
    assert!(
        context_relevance_score(
            "What risk score was recorded after the latest fraud review?",
            updated_risk_score
        ) > context_relevance_score(
            "What risk score was recorded after the latest fraud review?",
            stale_risk_score
        )
    );

    let new_roommate =
            "After the move, Emma said her new roommate is named Lena and they share the corner apartment.";
    let old_roommate =
        "Earlier chat: Emma's roommate was called Nora before Emma moved apartments.";
    assert!(context_query_matches(
        "What is Emma's roommate's name after the move?",
        new_roommate
    ));
    assert!(
        context_relevance_score(
            "What is Emma's roommate's name after the move?",
            new_roommate
        ) > context_relevance_score(
            "What is Emma's roommate's name after the move?",
            old_roommate
        )
    );

    let new_pet = "Latest pet update: the newly adopted dog is named Miso and needs evening walks.";
    let old_pet = "Old profile note: the family dog was called Pepper in a previous home.";
    assert!(context_query_matches(
        "What is the dog's name in the latest pet update?",
        new_pet
    ));
    assert!(
        context_relevance_score("What is the dog's name in the latest pet update?", new_pet)
            > context_relevance_score("What is the dog's name in the latest pet update?", old_pet)
    );
}

// shared-corpus: context_retrieval_precomputed_query_plan_parity
#[test]
fn context_query_plan_matches_legacy_match_and_score_helpers() {
    let cases = [
        (
            "What is Alice's current office choice after the payment problem?",
            "During the latest conversation, Alice replaced her office preference with the downtown location after the billing issue was resolved.",
        ),
        (
            "Which medication did Morgan say to remember before the doctor appointment?",
            "In the later session Morgan said to remember lisinopril, the blood pressure medication, before the doctor appointment.",
        ),
        (
            "How many total checkout retries happened across sessions?",
            "Across three sessions the support agent counted 12 checkout retries, 4 payment errors, and 2 successful follow-ups.",
        ),
        (
            "Which hobby did Priya switch to after cancelling guitar lessons?",
            "Later update: Priya cancelled guitar lessons and switched to a pottery class instead for the spring session.",
        ),
    ];

    for (query, text) in cases {
        let plan = context_query_plan(query);
        assert_eq!(
            context_query_matches(query, text),
            context_query_matches_plan(&plan, text)
        );
        assert_eq!(
            context_relevance_score(query, text),
            context_relevance_score_plan(&plan, text)
        );
        assert!(!plan.terms.is_empty());
        assert_eq!(
            plan.term_groups,
            context_query_term_groups_from_terms(&plan.terms)
        );
    }
}

#[test]
fn context_extract_gates_l1_for_thin_sources() {
    let engine = test_engine();
    // Thin single-sentence source: L1 is gated -> L0 only, 2 embeddings.
    let thin = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 5,
            source_kind: ContextSourceKind::Chat,
            source_id: "thin".to_string(),
            title: "note".to_string(),
            body: "Checkout failed once.".to_string(),
            timestamp_ms: 1,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(thin.status.ok, "{:?}", thin.status);
    assert!(!thin.l0.is_empty());
    assert!(thin.l1.is_empty(), "thin source should skip L1: {:?}", thin.l1);
    assert!(thin.node.l1_ref.is_empty());
    assert_eq!(thin.embedding_generation.requested_vector_count, 2);

    // Richer multi-sentence source: L1 is warranted -> L0 + L1, 3 embeddings.
    let rich = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 5,
            source_kind: ContextSourceKind::Chat,
            source_id: "rich".to_string(),
            title: "note".to_string(),
            body: "Checkout failed during payment. The risk score spiked sharply. The fraud team paused the account."
                .to_string(),
            timestamp_ms: 2,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(rich.status.ok, "{:?}", rich.status);
    assert!(!rich.l1.is_empty());
    assert_eq!(rich.embedding_generation.requested_vector_count, 3);
}

#[test]
fn context_extract_stores_embedding_vectors_on_the_records_themselves() {
    // Step 2 of the embedding fold populates event and summary records with their own vector,
    // which is the only place they live. Nothing else READS the inline
    // field yet, so without this assertion "population" could be silently writing empty vectors
    // and every existing test would still pass.
    let engine = test_engine();
    let report = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 77,
            source_kind: ContextSourceKind::Chat,
            source_id: "inline-vector".to_string(),
            title: "note".to_string(),
            body: "Checkout failed during payment. The risk score spiked sharply.                    The fraud team paused the account."
                .to_string(),
            timestamp_ms: 7,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    // Rich body -> L1 is warranted, so all three vectors exist: node_l0, node_l1, event_text.
    assert_eq!(report.embedding_generation.requested_vector_count, 3);

    let events = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEvents {
                tenant_hash: 77,
                node_hash: report.node.node_hash,
                start_time_ms: 0,
                end_time_ms: 4_000_000_000_000,
                limit: None,
                max_scan: None,
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: 0.0,
                min_importance: 0.0,
            },
        })
        .response
    {
        CommandResponse::ContextEvents { events, .. } => events,
        other => panic!("expected events, got {other:?}"),
    };
    let stored = events.first().expect("the extract wrote an event");
    assert!(
        !stored.vector.is_empty(),
        "event was stored without its inline vector"
    );

    let summaries = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaries {
                tenant_hash: 77,
                node_hash: report.node.node_hash,
                level: 1,
                as_of_ms: 4_000_000_000_000,
                limit: None,
            },
        })
        .response
    {
        CommandResponse::ContextSummaries { summaries, .. } => summaries,
        other => panic!("expected summaries, got {other:?}"),
    };
    assert!(!summaries.is_empty(), "the extract wrote no summaries");
    for summary in &summaries {
        assert!(
            !summary.vector.is_empty(),
            "summary level {} was stored without its inline vector",
            summary.level
        );
    }
}

// shared-corpus: context_compression_secondary_index_query_debug_flow
#[test]
fn context_workflow_extracts_retrieves_and_injects_mock_context() {
    let engine = test_engine();
    let extract = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 42,
            source_kind: ContextSourceKind::Incident,
            source_id: "INC-1".to_string(),
            title: "Checkout incident".to_string(),
            body: "Customer checkout failed. Payment risk score spiked.".to_string(),
            timestamp_ms: 1_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(extract.status.ok);
    assert!(extract.node_uri.starts_with("tsctx://tenant/42/node/"));

    let retrieve = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash: 42,
            node_hashes: vec![extract.node.node_hash],
            query: "checkout".to_string(),
            start_time_ms: 0,
            end_time_ms: 2_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(retrieve.status.ok);
    assert!(retrieve
        .blocks
        .iter()
        .any(|block| block.tier == ContextTier::L0));
    assert!(retrieve
        .blocks
        .iter()
        .any(|block| block.tier == ContextTier::L2));
    assert!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .enabled
    );
    assert_eq!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .summary_embedding_candidate_count,
        1
    );
    assert_eq!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .summary_embedding_selected_count,
        1
    );
    assert_eq!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .query_embedding_dimension,
        16
    );
    assert!(retrieve
        .query_understanding_debug
        .tree_traversal_summary
        .summary_embeddings
        .iter()
        .any(|entry| entry.starts_with("node:") && entry.contains(":score:")));
    assert!(!retrieve
        .query_understanding_debug
        .verbose_filter_groups
        .is_empty());
    assert!(retrieve
        .query_understanding_debug
        .verbose_filter_groups
        .iter()
        .any(|group| group.candidate_count > 0 && group.matched_count > 0));
    assert!(!retrieve.query_understanding_debug.selected_refs.is_empty());
    assert_eq!(retrieve.query_understanding_debug.selected_refs[0].rank, 1);
    assert!(retrieve
        .query_understanding_debug
        .selected_refs
        .iter()
        .any(|selected| selected
            .matched_filter_groups
            .iter()
            .any(|group_id| group_id.starts_with("filter_group_"))));
    assert!(retrieve.parity.pipeline_ready);
    assert!(retrieve.parity.native_context_models_ready);
    assert!(retrieve.parity.reference_tiers_ready);
    assert!(retrieve.parity.shared_store_sync_ready);
    assert!(retrieve.parity.raft_read_ready);

    let inject = inject_context(
        &engine,
        ContextInjectRequest {
            retrieve: ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash: 42,
                node_hashes: vec![extract.node.node_hash],
                query: "checkout".to_string(),
                start_time_ms: 0,
                end_time_ms: 2_000,
                max_events: 8,
                min_confidence: 0.0,
                min_importance: 0.0,
                tiers: default_tiers(),
                max_summary_nodes: 32,
                max_event_nodes: 16,
                prefer_current_agent: false,
                current_agent_scope_key: "agent:codex".to_string(),
                provider: ContextModelProviderConfig::default(),
            },
            prompt: "Explain current risk.".to_string(),
            session_hash: 7,
            query_id: "q1".to_string(),
            max_prompt_tokens: 128,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(inject.status.ok);
    assert!(inject.injected_prompt.contains("<context>"));
    assert!(!inject.audit.selected_refs.is_empty());
}

// shared-corpus: context_query_debug_filter_group_parity
#[test]
fn context_query_debug_reports_filter_groups_drops_and_injection_order() {
    let engine = test_engine();
    let relevant = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 4242,
            source_kind: ContextSourceKind::Incident,
            source_id: "INC-payment-1".to_string(),
            title: "Payment risk incident".to_string(),
            body:
                "Latest checkout update: payment risk score moved to 87 after the gateway timeout."
                    .to_string(),
            timestamp_ms: 10_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    let unrelated = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 4242,
            source_kind: ContextSourceKind::Chat,
            source_id: "CHAT-preference-1".to_string(),
            title: "Notification preference".to_string(),
            body: "Support chat: the user changed the email notification preference to weekly."
                .to_string(),
            timestamp_ms: 11_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(relevant.status.ok, "{:?}", relevant.status);
    assert!(unrelated.status.ok, "{:?}", unrelated.status);

    let report = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash: 4242,
            node_hashes: vec![relevant.node.node_hash, unrelated.node.node_hash],
            query: "What is the latest checkout payment risk score?".to_string(),
            start_time_ms: 0,
            end_time_ms: 20_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    let debug = &report.query_understanding_debug;
    assert_eq!(debug.debug_schema, "matrixark_context_query_debug_v1");
    assert_ne!(debug.query_hash, 0);
    assert!(debug
        .normalized_query_terms
        .contains(&"checkout".to_string()));
    assert_eq!(debug.question_type, "current_state");
    assert!(debug.filter_group_summary.total_groups >= 2);
    assert!(debug.filter_group_summary.secondary_index_group_count >= 1);
    assert!(debug.filter_group_summary.total_candidate_count >= 2);
    assert!(debug.filter_group_summary.total_matched_count >= 1);
    assert!(debug.filter_group_summary.total_dropped_count >= 1);
    assert!(debug.candidates_passing_prefilter >= 1);
    assert!(debug.candidates_dropped_before_scoring >= 1);
    assert!(debug
        .verbose_filter_groups
        .iter()
        .any(|group| group.group_kind == "secondary_index_prefilter"
            && group.matched_count > 0
            && group.dropped_count > 0
            && group
                .candidate_decisions
                .iter()
                .any(|decision| decision.decision == "matched"
                    && decision.reason == "matched_filter_group"
                    && !decision.matched_terms.is_empty())
            && group
                .candidate_decisions
                .iter()
                .any(|decision| decision.decision == "dropped"
                    && decision.reason == "secondary_index_prefilter_miss"
                    && decision.matched_terms.is_empty())));
    assert!(debug.prefilter_candidate_sample.iter().any(|candidate| {
        !candidate.passes_secondary_index_prefilter
            && candidate.drop_reason == "secondary_index_prefilter_miss"
    }));
    assert_eq!(
        debug
            .tree_traversal_summary
            .summary_embedding_candidate_count,
        2
    );
    assert_eq!(
        debug
            .tree_traversal_summary
            .summary_embedding_selected_count,
        2
    );
    assert_eq!(
        debug
            .tree_traversal_summary
            .summary_embedding_lookup_batches,
        1
    );
    assert!(!debug.selected_refs.is_empty());
    assert_eq!(debug.injection_ordering.len(), debug.selected_refs.len());
    assert_eq!(debug.injection_ordering[0].prompt_rank, 1);
    assert_eq!(
        debug.injection_ordering[0].ref_hash,
        debug.selected_refs[0].ref_hash
    );
    assert!(debug.injection_ordering[0]
        .selection_reason
        .contains("tree traversal"));
}

// shared-corpus: context_hierarchical_summary_secondary_index_fanout
#[test]
fn context_retrieval_limits_namespace_fanout_with_summary_and_locality_plan() {
    let engine = test_engine();
    let tenant_hash = 606060;
    let mut node_hashes = Vec::new();
    for (index, (title, body)) in [
        (
            "Checkout payment risk",
            "Checkout payment risk score is 91 after the payment gateway timeout.",
        ),
        (
            "Gardening preference",
            "The user likes basil and keeps herbs near the kitchen window.",
        ),
        (
            "Calendar reminder",
            "Team lunch is scheduled next week with no payment discussion.",
        ),
        (
            "Travel note",
            "The train ticket was moved to Friday morning.",
        ),
        (
            "Music note",
            "The playlist now starts with piano practice notes.",
        ),
    ]
    .iter()
    .enumerate()
    {
        let extract = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash,
                source_kind: ContextSourceKind::Chat,
                source_id: format!("fanout-source-{index}"),
                title: (*title).to_string(),
                body: (*body).to_string(),
                timestamp_ms: 10_000 + index as u64,
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(extract.status.ok, "{:?}", extract.status);
        node_hashes.push(extract.node.node_hash);
    }

    let report = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash,
            node_hashes: node_hashes.clone(),
            query: "checkout payment risk score".to_string(),
            start_time_ms: 0,
            end_time_ms: 20_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            max_summary_nodes: 5,
            max_event_nodes: 1,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert_eq!(report.fanout_plan.namespace_node_candidates, 5);
    assert_eq!(report.fanout_plan.summary_candidate_nodes, 5);
    assert_eq!(report.fanout_plan.event_expanded_nodes, 1);
    assert_eq!(report.fanout_plan.skipped_node_count, 4);
    assert!(report.fanout_plan.fanout_reduced);
    // The fanout limit bounds EVENT expansion -- the expensive path -- not summary
    // breadth. All 5 namespace candidates contribute their (cheap) summary, so
    // node_count tracks summary_candidate_nodes; only 1 node is expanded to events and
    // the other 4 are skipped, which is what the assertions above pin down.
    assert_eq!(report.node_count, 5);
    assert_eq!(report.event_count, 1);
    assert!(report
        .fanout_plan
        .locality_keys
        .iter()
        .all(|key| key.starts_with("tenant:606060:node:")));
    assert_eq!(
        report
            .query_understanding_debug
            .tree_traversal_summary
            .namespace_node_candidates,
        5
    );
    assert_eq!(
        report
            .query_understanding_debug
            .tree_traversal_summary
            .event_expanded_node_count,
        1
    );
    assert_eq!(
        report
            .query_understanding_debug
            .tree_traversal_summary
            .skipped_node_count,
        4
    );
    assert!(report
        .blocks
        .iter()
        .any(|block| block.tier == ContextTier::L2));
}

// shared-corpus: context_benchmark_injection_entity_slab_index
#[test]
fn context_benchmark_injection_uses_entity_slab_l0_l1_and_secondary_index() {
    with_secondary_indexes(|| {
        let engine = test_engine();
        let extract = extract_context(
                &engine,
                ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: 20260622,
                    source_kind: ContextSourceKind::Chat,
                    source_id: "locomo-conv-7-session-3-turn-18".to_string(),
                    title: "Caroline medical appointment update".to_string(),
                    body: "Caroline told Maya on March 12 that her cardiology appointment moved after the museum visit. Maya should remind her brother Leo before Friday. The updated cardiology appointment is now confirmed for the following week.".to_string(),
                    timestamp_ms: 1_712_300_000_000,
                    provider: ContextModelProviderConfig::default(),
                },
            );
        assert!(extract.status.ok, "{:?}", extract.status);

        let node_entity: crate::ContextNode = extract.node.clone();
        let slab: crate::ContextSlab = extract.event.clone();
        assert_eq!(node_entity.node_hash, extract.index_ref.primary_node_hash);
        assert_eq!(slab.event_id_hash, extract.index_ref.event_id_hash);
        assert!(node_entity.l0.contains("Caroline"));
        assert!(node_entity.l1_ref.contains("kind=Chat"));
        assert!(slab
            .text
            .contains("cardiology appointment moved after the museum visit"));
        assert!(slab.related_node_hashes.is_empty());
        assert_eq!(extract.related_node_hashes, vec![node_entity.node_hash]);

        let indexed = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryIndex {
                tenant_hash: 20260622,
                index_name: "source".to_string(),
                index_value_hash: stable_hash64("locomo-conv-7-session-3-turn-18"),
                scope_hash: 0,
                start_time_ms: 1_712_299_000_000,
                end_time_ms: 1_712_301_000_000,
                limit: Some(4),
            },
        });
        let refs = match indexed.response {
            CommandResponse::ContextIndexRefs { refs, .. } => refs,
            other => panic!("unexpected secondary index response: {other:?}"),
        };
        assert_eq!(refs, vec![extract.index_ref.clone()]);

        let retrieve = ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash: 20260622,
            node_hashes: vec![refs[0].primary_node_hash],
            query: "When did Caroline move the cardiology appointment after the museum visit?"
                .to_string(),
            start_time_ms: 1_712_299_000_000,
            end_time_ms: 1_712_301_000_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        };
        let retrieved = retrieve_context(&engine, retrieve.clone());
        assert!(retrieved.status.ok, "{:?}", retrieved.status);
        assert!(retrieved
            .blocks
            .iter()
            .any(|block| { block.tier == ContextTier::L0 && block.text == node_entity.l0 }));
        assert!(retrieved
            .blocks
            .iter()
            .any(|block| block.tier == ContextTier::L1));
        assert!(retrieved
            .blocks
            .iter()
            .any(|block| { block.tier == ContextTier::L2 && block.text == slab.text }));
        let l1_summary = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaries {
                tenant_hash: 20260622,
                node_hash: node_entity.node_hash,
                level: 2,
                as_of_ms: slab.event_time_ms,
                limit: Some(2),
            },
        });
        assert!(matches!(
            l1_summary.response,
            CommandResponse::ContextSummaries { ref summaries, .. }
                if summaries.iter().any(|summary| summary.text == node_entity.l1_ref)
        ));

        let inject = inject_context(
            &engine,
            ContextInjectRequest {
                retrieve,
                prompt: "Answer with retrieved TemporalStore memory.".to_string(),
                session_hash: stable_hash64("locomo-conv-7"),
                query_id: "locomo-context-entity-segment-index".to_string(),
                max_prompt_tokens: 192,
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(inject.status.ok, "{:?}", inject.status);
        assert!(inject
            .selected_blocks
            .iter()
            .any(|block| block.tier == ContextTier::L0));
        assert!(inject
            .selected_blocks
            .iter()
            .any(|block| block.tier == ContextTier::L1));
        assert!(inject
            .selected_blocks
            .iter()
            .any(|block| block.tier == ContextTier::L2));
        assert!(inject.injected_prompt.contains(&node_entity.l0));
        assert!(inject.injected_prompt.contains("cardiology appointment"));
        assert!(inject.injected_prompt.contains("museum visit"));
        assert!(inject
            .audit
            .selected_refs
            .iter()
            .any(|audit| audit.node_hash == node_entity.node_hash
                && audit.event_time_ms == slab.event_time_ms));
})
}

// shared-corpus: context_management_ingest_retrieve_pipeline
#[test]
fn context_management_ingest_extract_builds_retrieval_pipeline() {
    let engine = test_engine();
    let manage = context_pipeline_manage_report();
    assert!(manage.pipeline_ready);
    assert!(manage.management_ready);
    assert!(manage.ingestion_extraction_ready);
    assert!(manage.retrieval_ready);
    assert!(manage.injection_ready);
    assert!(manage
        .supported_routes
        .contains(&"/context/ingest_extract".to_string()));
    assert!(manage
        .supported_routes
        .contains(&"/context/manage".to_string()));
    assert_eq!(
        manage.stages,
        vec!["manage", "ingest", "extract", "index", "retrieve", "inject", "audit"]
    );
    assert_eq!(manage.stage_reports.len(), manage.stages.len());
    assert!(manage.stage_reports.iter().all(|stage| stage.ready));
    assert!(manage
        .provider_names
        .contains(&"mock-openai-compatible".to_string()));
    assert!(manage
        .policy_controls
        .contains(&"tenant isolation".to_string()));

    let ingest = ingest_extract_context(
        &engine,
        ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash: 77,
            sources: vec![
                ContextExtractRequest {
                    shard_id: 999,
                    tenant_hash: 0,
                    source_kind: ContextSourceKind::Incident,
                    source_id: "INC-CTX-1".to_string(),
                    title: "Checkout context incident".to_string(),
                    body: "Checkout retries failed after proxy route movement.".to_string(),
                    timestamp_ms: 1_000,
                    provider: ContextModelProviderConfig::default(),
                },
                ContextExtractRequest {
                    shard_id: 999,
                    tenant_hash: 0,
                    source_kind: ContextSourceKind::Ticket,
                    source_id: "TICKET-CTX-1".to_string(),
                    title: "Support context ticket".to_string(),
                    body: "Support requested retrieval context for the checkout failure."
                        .to_string(),
                    timestamp_ms: 1_500,
                    provider: ContextModelProviderConfig::default(),
                },
            ],
            query: "checkout".to_string(),
            start_time_ms: 0,
            end_time_ms: 3_000,
            max_events: 4,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(ingest.status.ok, "{:?}", ingest.status);
    assert_eq!(ingest.accepted, 2);
    assert_eq!(ingest.failed, 0);
    assert_eq!(ingest.node_hashes.len(), 2);
    assert_eq!(ingest.retrieve_request.shard_id, 1);
    assert_eq!(ingest.retrieve_request.tenant_hash, 77);
    assert_eq!(ingest.retrieve_request.node_hashes, ingest.node_hashes);
    assert_eq!(ingest.summary.source_count, 2);
    assert_eq!(ingest.summary.accepted, 2);
    assert_eq!(ingest.summary.failed, 0);
    assert_eq!(ingest.summary.unique_node_count, 2);
    assert_eq!(ingest.summary.extracted_node_count, 2);
    assert_eq!(ingest.summary.extracted_event_count, 2);
    assert_eq!(ingest.summary.extracted_index_ref_count, 2);
    assert_eq!(ingest.summary.extracted_dirty_marker_count, 2);
    assert_eq!(ingest.summary.extracted_summary_ref_count, 4);
    assert_eq!(ingest.summary.retrieval_node_count, 2);
    assert_eq!(ingest.summary.source_kind_counts.get("incident"), Some(&1));
    assert_eq!(ingest.summary.source_kind_counts.get("ticket"), Some(&1));
    assert_eq!(
        ingest.summary.provider_counts.get("mock-openai-compatible"),
        Some(&2)
    );

    let retrieve = retrieve_context(&engine, ingest.retrieve_request.clone());
    assert!(retrieve.status.ok, "{:?}", retrieve.status);
    assert!(retrieve.event_count >= 2);
    assert!(retrieve
        .blocks
        .iter()
        .any(|block| block.text.to_ascii_lowercase().contains("checkout")));
    assert!(retrieve.parity.pipeline_ready);

    let benchmark = run_context_pipeline_benchmark(
        &engine,
        ContextPipelineBenchmarkRequest {
            shard_id: 1,
            tenant_hash: 88,
            profile: "reference_unit_profile".to_string(),
            source_count: 12,
            query_count: 3,
            max_events: 6,
            provider: ContextModelProviderConfig::default(),
            thresholds: ContextPipelineBenchmarkThresholds::default(),
        },
    );
    assert!(benchmark.status.ok, "{:?}", benchmark.status);
    assert_eq!(
        benchmark.benchmark_name,
        "reference_style_context_management_local"
    );
    assert_eq!(benchmark.profile, "reference_unit_profile");
    assert_ne!(benchmark.workload_signature, 0);
    assert_eq!(benchmark.topic_count, 3);
    assert_eq!(benchmark.min_sources_per_topic, 4);
    assert_eq!(benchmark.max_sources_per_topic, 4);
    assert!(benchmark.source_kind_coverage_count >= 3);
    assert_eq!(benchmark.source_count, 12);
    assert_eq!(benchmark.query_count, 3);
    assert_eq!(benchmark.accepted_sources, 12);
    assert_eq!(benchmark.failed_sources, 0);
    assert_eq!(benchmark.retrieval_successes, 3);
    assert_eq!(benchmark.injection_successes, 3);
    assert_eq!(benchmark.hit_at_k, 1.0);
    assert_eq!(benchmark.mean_reciprocal_rank, 1.0);
    assert_eq!(benchmark.evidence_retention_at_k, 1.0);
    assert!(benchmark.ingest_sources_per_sec > 0.0);
    assert!(benchmark.retrieve_queries_per_sec > 0.0);
    assert!(benchmark.inject_queries_per_sec > 0.0);
    assert_eq!(benchmark.per_query.len(), 3);
    assert!(benchmark
        .per_query
        .iter()
        .all(|query| query.hit_rank.is_some()
            && query.reciprocal_rank > 0.0
            && query.evidence_retained
            && query.expected_topic_source_count == 4));
    assert!(benchmark.recall_at_k >= 1.0);
    assert!(benchmark.token_reduction_percent > 0.0);
    assert!(benchmark.max_selected_tokens_per_query <= 256);
    assert!(benchmark.threshold_passed);
    assert!(benchmark.threshold_violations.is_empty());
    assert_eq!(benchmark.thresholds.min_hit_at_k, 1.0);
    assert_eq!(benchmark.thresholds.min_mean_reciprocal_rank, 0.0);
    assert_eq!(benchmark.thresholds.min_evidence_retention_at_k, 1.0);
    assert_eq!(benchmark.thresholds.max_selected_tokens_per_query, 256);
    assert!(benchmark.source_kind_counts.len() >= 3);
    assert_eq!(
        benchmark.provider_counts.get("mock-openai-compatible"),
        Some(&12)
    );

    let mut unit_sweep_thresholds = ContextPipelineBenchmarkThresholds::default();
    unit_sweep_thresholds.min_ingest_sources_per_sec = 0.0;
    unit_sweep_thresholds.min_retrieve_queries_per_sec = 0.0;
    unit_sweep_thresholds.min_inject_queries_per_sec = 0.0;
    let sweep = run_context_pipeline_benchmark_sweep(
        &engine,
        ContextPipelineBenchmarkSweepRequest {
            shard_id: 1,
            tenant_hash: 100,
            profiles: vec![
                ContextPipelineBenchmarkSweepProfile {
                    profile: "unit_sweep_small".to_string(),
                    source_count: 12,
                    query_count: 2,
                    max_events: 4,
                },
                ContextPipelineBenchmarkSweepProfile {
                    profile: "unit_sweep_medium".to_string(),
                    source_count: 12,
                    query_count: 3,
                    max_events: 6,
                },
            ],
            provider: ContextModelProviderConfig::default(),
            thresholds: unit_sweep_thresholds,
        },
    );
    assert!(sweep.status.ok, "{:?}", sweep.status);
    assert_eq!(
        sweep.benchmark_name,
        "reference_style_context_management_sweep"
    );
    assert_eq!(sweep.profile_count, 2);
    assert!(sweep.all_profiles_ready);
    assert_eq!(sweep.total_sources, 24);
    assert_eq!(sweep.total_queries, 5);
    assert_eq!(sweep.profile_signatures.len(), 2);
    assert!(sweep
        .profile_signatures
        .iter()
        .all(|signature| *signature != 0));
    assert!(sweep.min_sources_per_topic > 0);
    assert!(sweep.max_sources_per_topic >= sweep.min_sources_per_topic);
    assert!(sweep.min_source_kind_coverage_count >= 3);
    assert_eq!(sweep.min_hit_at_k, 1.0);
    assert!(sweep.min_mean_reciprocal_rank > 0.0);
    assert_eq!(sweep.min_evidence_retention_at_k, 1.0);
    assert!(sweep.min_token_reduction_percent > 0.0);
    assert!(sweep.max_selected_tokens_per_query <= 256);
    assert!(sweep.all_thresholds_passed);
    assert!(sweep.threshold_violations.is_empty());
    assert_eq!(sweep.reports.len(), 2);
}

#[test]
fn text_with_nothing_to_redact_still_went_through_the_filter() {
    // pii_filtering_applied answered "did redaction change the text", not "did this text go
    // through the filter". With filtering on, most text has no personal data in it, so the flag
    // read false in the ordinary healthy case: anyone auditing whether filtering ran got "no"
    // for every clean record, which is the majority of them.
    let policy = ContextWorkflowPolicy {
        allowed_provider_kinds: vec![ContextProviderKind::OpenAiCompatible],
        allowed_models: vec!["context-prod".to_string()],
        max_extract_body_bytes: 256,
        max_prompt_tokens: 64,
        pii_filtering_enabled: true,
        tenant_isolation_required: true,
        rate_limit_per_minute: 100,
        provider_failure_budget: 3,
    };
    let request = ContextExtractRequest {
        shard_id: 1,
        tenant_hash: 9,
        source_kind: ContextSourceKind::Ticket,
        source_id: "T-2".to_string(),
        title: "Billing".to_string(),
        body: "Customer asked when the next invoice run happens".to_string(),
        timestamp_ms: 1,
        provider: ContextModelProviderConfig {
            provider_name: "openai-compatible".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            model: "context-prod".to_string(),
            mock_mode: false,
            ..ContextModelProviderConfig::default()
        },
    };

    let report = validate_context_extract_policy(&policy, &request);
    assert!(report.status.ok);
    assert_eq!(
        report.sanitized_text, request.body,
        "premise: this text has nothing in it to redact"
    );
    assert!(
        report.pii_filtering_applied,
        "the text went through the filter -- it simply had nothing to remove, which is not          the same as the filter not running"
    );

    // And the other direction, so "applied" cannot quietly come to mean something else: with
    // filtering switched off, nothing went through the filter and the report has to say so.
    let unfiltered = ContextWorkflowPolicy {
        pii_filtering_enabled: false,
        ..policy
    };
    let report = validate_context_extract_policy(&unfiltered, &request);
    assert!(report.status.ok);
    assert!(
        !report.pii_filtering_applied,
        "filtering is switched off, so it was not applied to this text"
    );
}

#[test]
fn context_workflow_policy_controls_provider_model_and_pii() {
    let policy = ContextWorkflowPolicy {
        allowed_provider_kinds: vec![ContextProviderKind::OpenAiCompatible],
        allowed_models: vec!["context-prod".to_string()],
        max_extract_body_bytes: 256,
        max_prompt_tokens: 64,
        pii_filtering_enabled: true,
        tenant_isolation_required: true,
        rate_limit_per_minute: 100,
        provider_failure_budget: 3,
    };
    let request = ContextExtractRequest {
        shard_id: 1,
        tenant_hash: 9,
        source_kind: ContextSourceKind::Ticket,
        source_id: "T-1".to_string(),
        title: "Billing".to_string(),
        body: "Customer jane@example.com has account 1234567890".to_string(),
        timestamp_ms: 1,
        provider: ContextModelProviderConfig {
            provider_name: "openai-compatible".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            model: "context-prod".to_string(),
            mock_mode: false,
            ..ContextModelProviderConfig::default()
        },
    };

    let report = validate_context_extract_policy(&policy, &request);
    assert!(report.status.ok);
    assert!(report.provider_allowed);
    assert!(report.model_allowed);
    assert!(report.pii_filtering_applied);
    assert!(report.sanitized_text.contains("[redacted-email]"));
    assert!(report.sanitized_text.contains("[redacted-id]"));
}

// shared-corpus: context_management_ingest_retrieve_pipeline
#[test]
fn context_workflow_exposes_reference_open_source_vlm_profiles() {
    let providers = default_context_model_providers();
    let reference_provider = providers
        .iter()
        .find(|provider| provider.provider_name == "reference-open-source-vlm")
        .expect("reference open-source provider profile should be exposed");
    assert_eq!(
        reference_provider.provider_kind,
        ContextProviderKind::OpenAiCompatible
    );
    assert_eq!(reference_provider.vlm_model, "qwen2.5vl:7b");
    assert_eq!(reference_provider.embedding_model, "nomic-embed-text");
    assert_eq!(reference_provider.base_url, "http://127.0.0.1:11434/v1");
    let matrixark_provider = providers
        .iter()
        .find(|provider| provider.provider_name == "matrixark-native-oss-context")
        .expect("MatrixArk OSS provider profile should be exposed");
    assert_eq!(matrixark_provider.model, "google/flan-t5-small");
    assert_eq!(
        matrixark_provider.embedding_model,
        "sentence-transformers/all-MiniLM-L6-v2"
    );
    let reference_reader = providers
        .iter()
        .find(|provider| provider.provider_name == "reference-gpt-4o-mini-reader")
        .expect("reference GPT-4o-mini reader profile should be exposed");
    assert_eq!(reference_reader.model, "gpt-4o-mini");
    assert_eq!(reference_reader.api_key_env, "OPENAI_API_KEY");

    let state = context_workflow_state_report();
    assert_eq!(
        state
            .context_model_descriptors
            .iter()
            .map(|descriptor| (descriptor.name.as_str(), descriptor.model_id))
            .collect::<Vec<_>>(),
        vec![
            ("ContextNodeModel", 9),
            ("ContextEventModel", 10),
            ("ContextIndexModel", 11),
            ("ContextAuditModel", 12),
            ("ContextChildModel", 14),
            ("ContextSummaryModel", 16),
            ("ContextCompressionModel", 17),
            ("ContextEntityModel", 18),
        ]
    );
    assert!(state.parity.native_context_model_ids_ready);
    assert!(state.parity.native_context_timeline_semantics_ready);
    assert!(state.parity.native_context_validation_limits_ready);
    assert!(state
        .reference_model_profiles
        .iter()
        .any(|profile| profile.vlm_model == "qwen2.5vl:7b"
            && profile.embedding_model == "nomic-embed-text"
            && profile
                .capabilities
                .contains(&"vlm_image_content_understanding".to_string())));
    assert!(state
        .reference_model_profiles
        .iter()
        .any(|profile| profile.vlm_model.contains("InternVL")));
    assert!(state.reference_model_profiles.iter().any(|profile| {
        profile.profile_name == "reference-gpt-4o-mini-reader"
            && profile.chat_model == "gpt-4o-mini"
            && profile
                .capabilities
                .contains(&"reference_reader_parity".to_string())
    }));
    assert!(state.reference_model_profiles.iter().any(|profile| {
        profile.profile_name == "matrixark-native-oss-context"
            && profile.chat_model == "google/flan-t5-small"
            && profile.embedding_model == "sentence-transformers/all-MiniLM-L6-v2"
            && profile
                .capabilities
                .contains(&"native_path_oss_model_parity".to_string())
    }));
    assert!(state.reference_model_profiles.iter().any(|profile| {
        profile.profile_name == "reference-minigpt4-gpt-style-vlm"
            && profile.vlm_model == "Vision-CAIR/MiniGPT-4"
            && profile
                .capabilities
                .contains(&"gpt_style_vlm_reasoning".to_string())
    }));
    assert!(state.open_model_provider_packaged);
    assert!(!state.open_model_local_run_proven);
    assert!(state.vlm_provider_configured);
    assert!(!state.vlm_benchmark_proven);
}

// shared-corpus: context_reference_blocks_provider_switches
#[test]
fn context_reference_blocks_and_provider_model_switches_are_reported() {
    let engine = test_engine();
    let open_source_text_provider = ContextModelProviderConfig {
        provider_name: "matrixark-native-oss-context".to_string(),
        provider_kind: ContextProviderKind::OpenAiCompatible,
        base_url: "http://127.0.0.1:8000/v1".to_string(),
        model: "google/flan-t5-small".to_string(),
        embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
        vlm_model: "none".to_string(),
        mock_mode: true,
        ..ContextModelProviderConfig::default()
    };
    let reference_vlm_provider = ContextModelProviderConfig {
        provider_name: "reference-minigpt4-gpt-style-vlm".to_string(),
        provider_kind: ContextProviderKind::OpenAiCompatible,
        base_url: "http://127.0.0.1:8000/v1".to_string(),
        model: "lmsys/vicuna-7b-v1.5".to_string(),
        embedding_model: "BAAI/bge-m3".to_string(),
        vlm_model: "Vision-CAIR/MiniGPT-4".to_string(),
        mock_mode: true,
        ..ContextModelProviderConfig::default()
    };
    let ingest = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: 20260620,
                sources: vec![
                    ContextExtractRequest {
                        shard_id: 0,
                        tenant_hash: 0,
                        source_kind: ContextSourceKind::Chat,
                        source_id: "open-text-memory".to_string(),
                        title: "Text memory".to_string(),
                        body: "Open-source text reader memory: Dana suggested the observability dashboard for Lee.".to_string(),
                        timestamp_ms: 1_000,
                        provider: open_source_text_provider.clone(),
                    },
                    ContextExtractRequest {
                        shard_id: 0,
                        tenant_hash: 0,
                        source_kind: ContextSourceKind::Document,
                        source_id: "vlm-receipt-memory".to_string(),
                        title: "Receipt image memory".to_string(),
                        body: "Reference VLM memory: receipt image shows Northstar Cafe total $18.40.".to_string(),
                        timestamp_ms: 2_000,
                        provider: reference_vlm_provider.clone(),
                    },
                ],
                query: "Which project did Dana suggest and what receipt total did the VLM see?"
                    .to_string(),
                start_time_ms: 0,
                end_time_ms: 3_000,
                max_events: 8,
                provider: ContextModelProviderConfig::default(),
            },
        );
    assert!(ingest.status.ok, "{:?}", ingest.status);
    assert_eq!(ingest.accepted, 2);
    assert_eq!(
        ingest
            .summary
            .provider_counts
            .get("matrixark-native-oss-context"),
        Some(&1)
    );
    assert_eq!(
        ingest
            .summary
            .provider_counts
            .get("reference-minigpt4-gpt-style-vlm"),
        Some(&1)
    );
    assert!(ingest
        .extracts
        .iter()
        .any(|extract| extract.provider.model == "google/flan-t5-small"
            && extract.provider.embedding_model == "sentence-transformers/all-MiniLM-L6-v2"));
    assert!(ingest.extracts.iter().any(|extract| {
        extract.provider.vlm_model == "Vision-CAIR/MiniGPT-4"
            && extract.provider.embedding_model == "BAAI/bge-m3"
    }));
    for extract in &ingest.extracts {
        let summaries = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaries {
                tenant_hash: 20260620,
                node_hash: extract.node.node_hash,
                level: 1,
                as_of_ms: extract.event.event_time_ms + 1,
                limit: Some(4),
            },
        });
        assert!(matches!(
            summaries.response,
            CommandResponse::ContextSummaries { ref summaries, .. }
                if summaries.iter().any(|summary| summary.text == extract.l0)
        ));

        // The vectors live on their owners: both summary levels answer the batched
        // vector query, and the event itself carries one. No separate rows exist to ask.
        for level in [1u32, 2] {
            let vectors = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextQuerySummaryVectors {
                    tenant_hash: 20260620,
                    node_hashes: vec![extract.node.node_hash],
                    level,
                    as_of_ms: extract.event.event_time_ms + 1,
                },
            });
            assert!(
                matches!(
                    vectors.response,
                    CommandResponse::ContextSummaryVectors { ref vectors }
                        if vectors.len() == 1 && vectors[0].vector.len() == 16
                ),
                "level {level} summary must carry its vector"
            );
        }
        let events = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEvents {
                tenant_hash: 20260620,
                node_hash: extract.node.node_hash,
                start_time_ms: 0,
                end_time_ms: extract.event.event_time_ms + 1,
                limit: Some(8),
                max_scan: None,
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: 0.0,
                min_importance: 0.0,
            },
        });
        assert!(matches!(
            events.response,
            CommandResponse::ContextEvents { ref events, .. }
                if events
                    .iter()
                    .any(|event| event.event_id_hash == extract.event.event_id_hash
                        && event.vector.len() == 16)
        ));

        // The node itself must carry its L0 vector off the same ingest -- the traversal scores
        // from node.vector first, so a happy-path ingest that left it empty would strand every
        // fresh node on the separate-record fallback and the records could never be retired.
        let node = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNode {
                tenant_hash: 20260620,
                node_hash: extract.node.node_hash,
            },
        });
        assert!(matches!(
            node.response,
            CommandResponse::ContextNode { node: Some(ref node), .. }
                if node.vector.len() == 16
                    && node.embedding_model_hash != 0
                    && node.embedding_updated_at_ms == extract.event.event_time_ms
        ));
    }

    let retrieve = retrieve_context(&engine, ingest.retrieve_request);
    assert!(retrieve.status.ok, "{:?}", retrieve.status);
    assert!(retrieve.blocks.iter().any(|block| {
        block.tier == ContextTier::L2 && block.text.contains("observability dashboard")
    }));
    assert!(retrieve
        .blocks
        .iter()
        .any(|block| { block.tier == ContextTier::L2 && block.text.contains("Northstar Cafe") }));
}

// shared-corpus: context_injection_prompt_pack_ordering
#[test]
fn context_injection_prompt_pack_preserves_retrieved_evidence_ordering() {
    let engine = test_engine();
    let stale = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 20260621,
            source_kind: ContextSourceKind::Chat,
            source_id: "pet-stale".to_string(),
            title: "Old pet note".to_string(),
            body: "Old profile note: the family dog was called Pepper in a previous home."
                .to_string(),
            timestamp_ms: 1_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    let current = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 20260621,
            source_kind: ContextSourceKind::UserEvent,
            source_id: "pet-current".to_string(),
            title: "Latest pet note".to_string(),
            body: "Latest pet update: the newly adopted dog is named Miso and needs evening walks."
                .to_string(),
            timestamp_ms: 2_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(stale.status.ok);
    assert!(current.status.ok);
    let retrieve = ContextRetrieveRequest {
        shard_id: 1,
        tenant_hash: 20260621,
        node_hashes: vec![stale.node.node_hash, current.node.node_hash],
        query: "What is the dog's name in the latest pet update?".to_string(),
        start_time_ms: 0,
        end_time_ms: 3_000,
        max_events: 8,
        min_confidence: 0.0,
        min_importance: 0.0,
        tiers: vec![ContextTier::L2],
        max_summary_nodes: 32,
        max_event_nodes: 16,
        prefer_current_agent: false,
        current_agent_scope_key: "agent:codex".to_string(),
        provider: ContextModelProviderConfig::default(),
    };
    let retrieved = retrieve_context(&engine, retrieve.clone());
    assert!(retrieved.status.ok, "{:?}", retrieved.status);
    assert!(retrieved.blocks.len() >= 2);
    assert!(retrieved.blocks[0].text.contains("Miso"));
    assert!(retrieved.blocks[1].text.contains("Pepper"));

    let inject = inject_context(
        &engine,
        ContextInjectRequest {
            retrieve,
            prompt: "Answer from current memory only.".to_string(),
            session_hash: 99,
            query_id: "pet-current-pack".to_string(),
            max_prompt_tokens: 128,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(inject.status.ok, "{:?}", inject.status);
    assert!(inject.injected_prompt.contains("<context>"));
    let miso_pos = inject
        .injected_prompt
        .find("Miso")
        .expect("current evidence should be injected");
    let pepper_pos = inject
        .injected_prompt
        .find("Pepper")
        .expect("stale evidence should still be available after current evidence");
    assert!(miso_pos < pepper_pos);
    assert_eq!(inject.audit.selected_refs[0].event_time_ms, 2_000);
    assert_eq!(inject.audit.selected_refs[1].event_time_ms, 1_000);
}

// shared-corpus: context_reference_reasoning_vlm_parity
#[test]
fn context_reference_reasoning_vlm_cases_cover_required_gaps() {
    let state = context_workflow_state_report();
    for required_category in [
        "multi_hop_reasoning",
        "temporal",
        "memory_update",
        "stale_memory",
        "open_domain_retrieval",
        "vlm_image_content_understanding",
    ] {
        assert!(
            state
                .reference_parity_categories
                .contains(&required_category.to_string()),
            "missing reference parity category {required_category}"
        );
    }
    assert_eq!(state.reference_parity_cases.len(), 6);
    assert!(state
        .reference_parity_cases
        .iter()
        .any(|case| case.uses_vlm && !case.benchmark_proven));
    assert!(state
        .reference_parity_cases
        .iter()
        .filter(|case| !case.uses_vlm)
        .all(|case| case.benchmark_proven));

    for case in state.reference_parity_cases {
        assert!(
            context_query_matches(&case.query, &case.positive_memory),
            "{} did not match its positive memory",
            case.case_name
        );
        assert!(
            context_relevance_score(&case.query, &case.positive_memory)
                > context_relevance_score(&case.query, &case.stale_memory),
            "{} did not outrank stale memory",
            case.case_name
        );
        for term in case.expected_terms {
            let text_lower = case.positive_memory.to_ascii_lowercase();
            let text_normalized = context_normalize_for_match(&case.positive_memory);
            assert!(
                context_text_matches_term(&text_lower, &text_normalized, &term),
                "{} positive memory did not expose expected term {term}",
                case.case_name
            );
        }
    }
}

#[test]
fn context_workflow_policy_rejects_disallowed_runtime_controls() {
    let policy = ContextWorkflowPolicy {
        allowed_provider_kinds: vec![ContextProviderKind::Mock],
        allowed_models: vec!["context-prod".to_string()],
        max_extract_body_bytes: 8,
        max_prompt_tokens: 4,
        pii_filtering_enabled: true,
        tenant_isolation_required: true,
        rate_limit_per_minute: 0,
        provider_failure_budget: 0,
    };
    let request = ContextInjectRequest {
        retrieve: ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash: 0,
            node_hashes: vec![1],
            query: "risk".to_string(),
            start_time_ms: 0,
            end_time_ms: 10,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
        prompt: "one two three four five".to_string(),
        session_hash: 7,
        query_id: "q-policy".to_string(),
        max_prompt_tokens: 32,
        provider: ContextModelProviderConfig {
            provider_kind: ContextProviderKind::OpenAiCompatible,
            model: "wrong-model".to_string(),
            mock_mode: false,
            ..ContextModelProviderConfig::default()
        },
    };

    let report = validate_context_inject_policy(&policy, &request);
    assert!(!report.status.ok);
    assert!(!report.provider_allowed);
    assert!(!report.model_allowed);
    assert!(!report.prompt_size_allowed);
    assert!(!report.tenant_isolation_applied);
    assert!(!report.rate_limit_allowed);
    assert!(!report.provider_failure_budget_allowed);
}

#[test]
fn context_workflow_extracts_with_openai_compatible_provider() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        for expected_path in ["/v1/chat/completions", "/v1/embeddings"] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let read = stream.read(&mut chunk).unwrap();
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let request = String::from_utf8_lossy(&buffer[..header_end + 4]);
                    if request.contains(format!("POST {expected_path} HTTP/1.1").as_str()) {
                        assert!(request.contains("Authorization: Bearer test-context-key"));
                        let content_length = request
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("content-length:")
                                    .or_else(|| line.strip_prefix("Content-Length:"))
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or_default();
                        if buffer.len() >= header_end + 4 + content_length {
                            break;
                        }
                    }
                }
            }
            let body_start = buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
                .unwrap_or(buffer.len());
            let request_body: serde_json::Value =
                serde_json::from_slice(&buffer[body_start..]).unwrap();
            let body = if expected_path.ends_with("chat/completions") {
                assert_eq!(request_body["model"], "context-live-test");
                serde_json::json!({
                    "choices": [{
                        "message": {
                            "content": "{\"l0\":\"live checkout incident\",\"l1\":\"kind=Incident; live facts=payment risk; customer impact\"}"
                        }
                    }]
                })
                .to_string()
            } else {
                assert_eq!(request_body["model"], "context-embedding-live-test");
                assert_eq!(request_body["input"].as_array().unwrap().len(), 3);
                serde_json::json!({
                    "data": [
                        {"embedding": [1.0, 0.0, 0.0, 0.0]},
                        {"embedding": [0.0, 1.0, 0.0, 0.0]},
                        {"embedding": [0.0, 0.0, 1.0, 0.0]}
                    ]
                })
                .to_string()
            };
            write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            stream.flush().unwrap();
        }
    });
    std::env::set_var("TS_CONTEXT_TEST_KEY", "test-context-key");
    let engine = test_engine();
    let report = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 1,
            source_kind: ContextSourceKind::Document,
            source_id: "doc".to_string(),
            title: "doc".to_string(),
            body: "Customer checkout failed during payment. The risk score spiked sharply. The fraud team paused the account.".to_string(),
            timestamp_ms: 1,
            provider: ContextModelProviderConfig {
                provider_name: "live-test".to_string(),
                provider_kind: ContextProviderKind::OpenAiCompatible,
                base_url: format!("http://{addr}/v1"),
                api_key_env: "TS_CONTEXT_TEST_KEY".to_string(),
                model: "context-live-test".to_string(),
                embedding_model: "context-embedding-live-test".to_string(),
                mock_mode: false,
                ..ContextModelProviderConfig::default()
            },
        },
    );
    assert!(report.status.ok, "{}", report.status.message);
    assert_eq!(report.l0, "live checkout incident");
    assert!(report.l1.contains("payment risk"));
    assert_eq!(report.provider.provider_name, "live-test");
    assert_eq!(report.embedding_generation.provider_name, "live-test");
    assert_eq!(
        report.embedding_generation.embedding_model,
        "context-embedding-live-test"
    );
    assert_eq!(report.embedding_generation.vector_dimension, 4);
    assert_eq!(report.embedding_generation.requested_vector_count, 3);
    assert_eq!(report.embedding_generation.generated_vector_count, 3);
    assert_eq!(report.embedding_generation.batch_count, 1);
    assert_eq!(report.embedding_generation.live_call_count, 1);
    assert!(!report.embedding_generation.mock_mode);
    assert!(report.embedding_generation.production_evidence_ready);
    handle.join().unwrap();
    std::env::remove_var("TS_CONTEXT_TEST_KEY");
}

// --- Hybrid retrieve scoring (un-embedded nodes rankable via lexical) ---

// Upsert a raw ContextNode + one event with NO embedding (mirrors raw-first bulk
// ingest). `last_event_time_ms` is shared across callers in a test so the freshness
// tiebreak does not decide ranking -- lexical/cosine score must.
fn upsert_raw_context_node(
    engine: &TemporalEngine,
    tenant_hash: u64,
    node_hash: u64,
    text: &str,
    event_time_ms: u64,
) {
    let ok = engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash,
                node: ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 0,
                    canonical_name: format!("node {node_hash}"),
                    l0: text.to_string(),
                    status: 1,
                    last_event_time_ms: event_time_ms,
                    l1_ref: String::new(),
                    raw_metadata_ref: format!("src://{node_hash}"),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                },
            },
        })
        .status
        .ok;
    assert!(ok);
    let ok = engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash,
                node_hash,
                event: ContextEvent {
                    event_id_hash: node_hash.wrapping_mul(11).max(1),
                    event_time_ms,
                    ingestion_time_ms: event_time_ms,
                    kind: 0,
                    event_type: 1,
                    actor_hash: 1,
                    status: 1,
                    valid_until_ms: 0,
                    confidence: 1.0,
                    importance: 1.0,
                    text: text.to_string(),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                },
                first_write_only: false,
                cold_storage: false,
            },
        })
        .status
        .ok;
    assert!(ok);
}

fn retrieve_top_node(engine: &TemporalEngine, tenant_hash: u64, node_hashes: Vec<u64>, query: &str) -> u64 {
    let report = retrieve_context(
        engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash,
            node_hashes,
            query: query.to_string(),
            start_time_ms: 0,
            end_time_ms: 1_000_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: 1,
            max_event_nodes: 1,
            prefer_current_agent: false,
            current_agent_scope_key: String::new(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    report
        .blocks
        .first()
        .map(|block| block.node_hash)
        .unwrap_or(0)
}

#[test]
fn hybrid_lexical_ranks_unembedded_nodes_above_flat_zero() {
    let engine = test_engine();
    let tenant_hash = 77;
    // Both un-embedded. The relevant node has the LARGER node_hash so that WITHOUT
    // hybrid (all scores 0) the ascending node_hash tiebreak would pick the
    // irrelevant one; hybrid lexical scoring must flip that.
    let irrelevant = 111u64;
    let relevant = 222u64;
    upsert_raw_context_node(
        &engine,
        tenant_hash,
        irrelevant,
        "Support ticket: user asked to update a notification preference.",
        5_000,
    );
    upsert_raw_context_node(
        &engine,
        tenant_hash,
        relevant,
        "Checkout incident: payment fraud score spiked after an outage.",
        5_000,
    );

    let top = retrieve_top_node(
        &engine,
        tenant_hash,
        vec![irrelevant, relevant],
        "payment fraud score",
    );
    assert_eq!(
        top, relevant,
        "un-embedded node with lexical overlap must outrank the flat-zero node"
    );
}

#[test]
fn hybrid_mixed_store_keeps_both_embedded_and_unembedded_rankable() {
    let engine = test_engine();
    let tenant_hash = 88;
    // One node embedded via the extract path (stores cosine-scorable embeddings).
    let embedded = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash,
            source_kind: ContextSourceKind::Incident,
            source_id: "emb".to_string(),
            title: "deploy".to_string(),
            body: "Deployment rollout paused after a latency regression.".to_string(),
            timestamp_ms: 6_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(embedded.status.ok, "{:?}", embedded.status);
    // One node left un-embedded (raw upsert), lexically matching a different query.
    let raw_node = 999u64;
    upsert_raw_context_node(
        &engine,
        tenant_hash,
        raw_node,
        "Checkout incident: payment fraud score spiked after an outage.",
        6_000,
    );

    // Query that lexically matches ONLY the un-embedded node: it must still surface
    // (mixed store does not collapse un-embedded nodes to 0/invisible).
    let report = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash,
            node_hashes: vec![embedded.node.node_hash, raw_node],
            query: "payment fraud score".to_string(),
            start_time_ms: 0,
            end_time_ms: 1_000_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: 8,
            max_event_nodes: 8,
            prefer_current_agent: false,
            current_agent_scope_key: String::new(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert!(
        report.blocks.iter().any(|block| block.node_hash == raw_node),
        "un-embedded lexical-matching node must be retrievable in a mixed store"
    );
}

#[test]
fn context_workflow_falls_back_when_live_provider_fails() {
    let engine = test_engine();
    let report = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 1,
            source_kind: ContextSourceKind::Document,
            source_id: "doc".to_string(),
            title: "doc".to_string(),
            body: "body".to_string(),
            timestamp_ms: 1,
            provider: ContextModelProviderConfig {
                provider_name: "offline-live-provider".to_string(),
                provider_kind: ContextProviderKind::OpenAiCompatible,
                base_url: "http://127.0.0.1:9/v1".to_string(),
                mock_mode: false,
                timeout_ms: 25,
                max_retries: 0,
                fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
                ..ContextModelProviderConfig::default()
            },
        },
    );
    assert!(report.status.ok, "{}", report.status.message);
    assert!(report
        .provider
        .provider_name
        .starts_with("offline-live-provider+fallback:"));
    assert!(report.embedding_generation.mock_mode);
    assert!(!report.embedding_generation.production_evidence_ready);
    assert_eq!(report.l0, "doc: body");
}

// shared-corpus: context_resource_large_object_store_spill
#[test]
fn context_resource_parser_spills_large_resource_body_to_object_store_ref() {
    let large_text = "x".repeat(1_048_577);
    let report = parse_context_resource(ContextResourceParseRequest {
        raw_uri: "large-resource.pdf".to_string(),
        resource_type: Some("pdf".to_string()),
        text: large_text,
        max_chunk_chars: 262_144,
        overlap_chars: 0,
        chunk_hash_base: Some(42_000),
        owner_scope: "workspace:bench".to_string(),
        version: "v-large".to_string(),
        watch_interval_minutes: 0,
        parser_name: "unit-test-parser".to_string(),
    });
    assert!(report.status.ok);
    assert_eq!(report.payload_size_bytes, 1_048_577);
    assert_eq!(report.max_inline_bytes, 1_048_576);
    assert!(!report.inline_payload);
    assert!(report
        .external_object_uri
        .starts_with("objectstore://matrixark/resources/"));
    assert_eq!(
        report.lifecycle.external_object_uri,
        report.external_object_uri
    );
    assert_eq!(
        report.lifecycle.payload_size_bytes,
        report.payload_size_bytes
    );
    assert!(report.chunks.iter().all(|chunk| {
        chunk.metadata.get("storage_backend").map(String::as_str) == Some("objectstore")
            && chunk.metadata.get("storage_value_mode").map(String::as_str)
                == Some("object_ref_json")
            && chunk.metadata.get("external_object_uri") == Some(&report.external_object_uri)
    }));
}

// shared-corpus: context_resource_skill_parser_reference_parity
#[test]
fn context_resource_parser_matches_reference_stable_refs() {
    let report = parse_context_resource(ContextResourceParseRequest {
        raw_uri: "runbook.md".to_string(),
        resource_type: Some("md".to_string()),
        text: "# Rollback\n\nUse canary rollback. See [runbook](baseline://resources/runbook-extra.md).\n\n## Checks\n\nConfirm p95 latency.\n\n```bash\ncurl /health\n```".to_string(),
        max_chunk_chars: 1_400,
        overlap_chars: 120,
        chunk_hash_base: Some(900),
        owner_scope: "team:ops".to_string(),
        version: "v1".to_string(),
        watch_interval_minutes: 60,
        parser_name: "unit-test-parser".to_string(),
    });
    assert!(report.status.ok);
    assert_eq!(report.resource_type, "md");
    assert_eq!(report.uri_scheme, "file");
    assert_eq!(report.resource_title, "runbook.md");
    assert_eq!(report.lifecycle.owner_scope, "team:ops");
    assert_eq!(report.lifecycle.parser_name, "unit-test-parser");
    assert_eq!(report.lifecycle.parser_version, "reference-compatible-v1");
    assert_eq!(report.lifecycle.version, "v1");
    assert_eq!(
        report.lifecycle.import_kind,
        ContextResourceImportKind::Markdown
    );
    assert_eq!(
        report.lifecycle.action,
        ContextResourceLifecycleAction::Watch
    );
    assert!(report.lifecycle.watched);
    assert_eq!(report.lifecycle.watch_interval_minutes, 60);
    assert_eq!(report.lifecycle.next_refresh_after_ms, 3_600_000);
    assert_eq!(report.chunks.len(), 2);
    assert_eq!(report.chunks[0].chunk_hash, 900);
    assert!(report.chunks[0].content_hash != 0);
    assert_eq!(report.chunks[0].source_ref, "runbook.md#heading=rollback");
    assert_eq!(report.chunks[0].heading_path, vec!["rollback"]);
    assert_eq!(
        report.chunks[0]
            .metadata
            .get("line_start")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(report.chunks[1].chunk_hash, 901);
    assert_eq!(report.chunks[1].source_ref, "runbook.md#heading=checks");
    assert_eq!(
        report.chunks[1].parent_source_ref.as_deref(),
        Some("runbook.md#heading=rollback")
    );
    assert_eq!(report.source_refs.len(), report.chunks.len());
    assert_eq!(
        report.chunks[1]
            .metadata
            .get("heading_level")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        report.chunks[0]
            .metadata
            .get("linked_refs")
            .map(String::as_str),
        Some("baseline://resources/runbook-extra.md")
    );
    assert_eq!(
        report.chunks[1]
            .metadata
            .get("code_language")
            .map(String::as_str),
        Some("bash")
    );
    assert_eq!(
        report.chunks[1]
            .metadata
            .get("chunk_kind")
            .map(String::as_str),
        Some("code")
    );
}

// shared-corpus: context_resource_lifecycle_reference_parity
#[test]
fn context_resource_lifecycle_models_import_paths_refresh_and_delete() {
    let url = parse_context_resource(ContextResourceParseRequest {
        raw_uri: "https://docs.example.com/runbook".to_string(),
        resource_type: Some("html".to_string()),
        text: "Remote runbook body".to_string(),
        max_chunk_chars: 1_400,
        overlap_chars: 120,
        chunk_hash_base: None,
        owner_scope: "team:docs".to_string(),
        version: "etag-1".to_string(),
        watch_interval_minutes: 30,
        parser_name: "url-parser".to_string(),
    });
    let git = parse_context_resource(ContextResourceParseRequest {
        raw_uri: "git@github.com:matrixarkai/TemporalStore.git".to_string(),
        resource_type: Some("git".to_string()),
        text: "README source".to_string(),
        max_chunk_chars: 1_400,
        overlap_chars: 120,
        chunk_hash_base: None,
        owner_scope: "team:code".to_string(),
        version: "main@abc".to_string(),
        watch_interval_minutes: 0,
        parser_name: "git-parser".to_string(),
    });
    let pdf = parse_context_resource(ContextResourceParseRequest {
        raw_uri: "file:///tmp/report.pdf".to_string(),
        resource_type: None,
        text: "Extracted PDF text".to_string(),
        max_chunk_chars: 1_400,
        overlap_chars: 120,
        chunk_hash_base: None,
        owner_scope: "team:docs".to_string(),
        version: "pdf-v1".to_string(),
        watch_interval_minutes: 0,
        parser_name: "pdf-parser".to_string(),
    });
    let wiki = parse_context_resource(ContextResourceParseRequest {
        raw_uri: "wiki://doc/abc123".to_string(),
        resource_type: Some("doc".to_string()),
        text: "Wiki imported document".to_string(),
        max_chunk_chars: 1_400,
        overlap_chars: 120,
        chunk_hash_base: None,
        owner_scope: "team:docs".to_string(),
        version: "doc-v1".to_string(),
        watch_interval_minutes: 10,
        parser_name: "wiki-parser".to_string(),
    });

    assert_eq!(url.lifecycle.import_kind, ContextResourceImportKind::Url);
    assert_eq!(
        git.lifecycle.import_kind,
        ContextResourceImportKind::GitRepo
    );
    assert_eq!(pdf.lifecycle.import_kind, ContextResourceImportKind::Pdf);
    assert_eq!(
        wiki.lifecycle.import_kind,
        ContextResourceImportKind::WikiDoc
    );
    assert!(url.chunks[0].metadata.contains_key("parser_name"));

    let report = update_context_resource_lifecycle(
        vec![
            url.lifecycle.clone(),
            git.lifecycle.clone(),
            pdf.lifecycle.clone(),
            wiki.lifecycle.clone(),
        ],
        vec![
            ContextResourceLifecycleUpdate {
                raw_uri: url.raw_uri.clone(),
                action: ContextResourceLifecycleAction::Refresh,
                owner_scope: String::new(),
                version: "etag-2".to_string(),
                watch_interval_minutes: 30,
                observed_at_ms: 1_000,
            },
            ContextResourceLifecycleUpdate {
                raw_uri: git.raw_uri.clone(),
                action: ContextResourceLifecycleAction::Delete,
                owner_scope: String::new(),
                version: String::new(),
                watch_interval_minutes: 0,
                observed_at_ms: 1_000,
            },
        ],
    );
    assert_eq!(report.watched_count, 2);
    assert_eq!(report.deleted_count, 1);
    assert!(report
        .resources
        .iter()
        .any(|resource| resource.raw_uri == url.raw_uri
            && resource.version == "etag-2"
            && resource.invalidates_version == "etag-1"
            && resource.next_refresh_after_ms == 1_801_000));
    assert!(report
        .resources
        .iter()
        .any(|resource| resource.raw_uri == git.raw_uri && resource.deleted));
    assert_eq!(report.import_kinds.get("url").copied(), Some(1));
    assert_eq!(report.import_kinds.get("git_repo").copied(), Some(1));
    assert_eq!(report.import_kinds.get("pdf").copied(), Some(1));
    assert_eq!(report.import_kinds.get("wiki_doc").copied(), Some(1));
}

// shared-corpus: context_resource_skill_parser_reference_parity
#[test]
fn context_skill_parser_extracts_frontmatter_and_capability_sections() {
    let skill = parse_context_skill_markdown(
            "skills/context-debug/SKILL.md",
            "---\nname: context-debug\ndescription: Trace context ingestion and retrieval.\nversion: 1.2.0\nowner_scope: team:context\nprecedence: high\nenabled: true\ntags: [context, debug, reference]\nallowed_tools:\n  - context_workflow_harness\n  - codex_context_hook\ntriggers: [context-debug, retrieval-trace]\nmodels: [nomic-embed-text, qwen2.5vl]\n---\n\n# Context Debug\n\n## When To Use\n\n- Use for context trace debugging.\n\n## Tools\n\n- context_workflow_harness\n- `codex_context_hook` captures prompt context.\n\n## Resources\n\n- [Debug Resource](baseline://resources/context-debug.md)\n\n## Examples\n\n- Query the context debug flow for stale entity filters.\n",
        );
    assert!(skill.status.ok);
    assert_eq!(skill.skill_name, "context-debug");
    assert_eq!(skill.description, "Trace context ingestion and retrieval.");
    assert_eq!(skill.version, "1.2.0");
    assert_eq!(skill.owner_scope, "team:context");
    assert!(skill.enabled);
    assert_eq!(skill.precedence, ContextSkillPrecedence::High);
    assert_eq!(
        skill.front_matter.get("tags").map(String::as_str),
        Some("[context, debug, reference]")
    );
    assert!(skill.tag_refs.contains(&"context".to_string()));
    assert!(skill.tag_refs.contains(&"debug".to_string()));
    assert!(skill.tag_refs.contains(&"reference".to_string()));
    assert!(skill.capability_refs.contains(&"when-to-use".to_string()));
    assert!(skill.capability_refs.contains(&"tools".to_string()));
    assert!(skill
        .instruction_refs
        .contains(&"Use for context trace debugging".to_string()));
    assert!(skill
        .tool_refs
        .contains(&"context_workflow_harness".to_string()));
    assert!(skill.tool_refs.contains(&"codex_context_hook".to_string()));
    assert!(skill
        .allowed_tools
        .contains(&"context_workflow_harness".to_string()));
    assert!(skill
        .allowed_tools
        .contains(&"codex_context_hook".to_string()));
    assert!(skill.triggers.contains(&"context-debug".to_string()));
    assert!(skill.triggers.contains(&"retrieval-trace".to_string()));
    assert!(skill.model_refs.contains(&"nomic-embed-text".to_string()));
    assert!(skill.model_refs.contains(&"qwen2.5vl".to_string()));
    assert!(skill
        .resource_refs
        .contains(&"baseline://resources/context-debug.md".to_string()));
    assert!(skill
        .example_refs
        .contains(&"Query the context debug flow for stale entity filters".to_string()));
    assert!(skill.resource.chunks.iter().all(|chunk| chunk
        .metadata
        .get("resource_type")
        .map(String::as_str)
        == Some("skill")));
}

// shared-corpus: context_resource_skill_registry_reference_parity
#[test]
fn context_skill_registry_supports_updates_and_retrieval_selection() {
    let active = parse_context_skill_markdown(
        "skills/context-debug/SKILL.md",
        "---\nname: context-debug\ndescription: Trace context retrieval.\nversion: 1.0.0\nowner_scope: team:context\nprecedence: low\nallowed_tools: [context_workflow_harness]\ntriggers: [context-debug, retrieval]\n---\n\n# Context Debug\n",
    );
    let disabled = parse_context_skill_markdown(
        "skills/payment-debug/SKILL.md",
        "---\nname: payment-debug\ndescription: Disabled payment workflow.\nversion: 0.9.0\nowner_scope: team:payments\nstatus: disabled\nprecedence: critical\nallowed_tools: [context_workflow_harness]\ntriggers: [payment]\n---\n\n# Payment Debug\n",
    );
    let registry = context_skill_registry_from_parsed(&[active, disabled], 10);
    assert_eq!(registry.enabled_count, 1);
    assert_eq!(registry.disabled_count, 1);
    assert_eq!(
        registry.highest_precedence,
        ContextSkillPrecedence::Critical
    );

    let updated = update_context_skill_registry(
        registry.entries,
        vec![
            ContextSkillRegistryUpdate {
                skill_name: "payment-debug".to_string(),
                enabled: Some(true),
                precedence: Some(ContextSkillPrecedence::Critical),
                owner_scope: Some("team:context".to_string()),
                triggers: Some(vec!["payment".to_string(), "retrieval".to_string()]),
                allowed_tools: Some(vec!["context_workflow_harness".to_string()]),
                version: Some("1.1.0".to_string()),
                updated_at_ms: 20,
            },
            ContextSkillRegistryUpdate {
                skill_name: "context-debug".to_string(),
                enabled: Some(false),
                precedence: None,
                owner_scope: None,
                triggers: None,
                allowed_tools: None,
                version: None,
                updated_at_ms: 21,
            },
        ],
    );
    assert_eq!(updated.enabled_count, 1);
    assert!(updated
        .version_updates
        .contains(&"payment-debug:0.9.0->1.1.0".to_string()));

    let selection = select_context_skills_for_retrieval(ContextSkillSelectionRequest {
        query: "payment retrieval trace".to_string(),
        owner_scope: "team:context".to_string(),
        tool_name: "context_workflow_harness".to_string(),
        include_disabled: false,
        allowed_scope_layers: Vec::new(),
        limit: 4,
        registry: updated.entries,
    });
    assert!(selection.status.ok, "{:?}", selection);
    assert_eq!(selection.selected[0].skill_name, "payment-debug");
    assert_eq!(selection.selected[0].version, "1.1.0");
    assert_eq!(
        selection.selected[0].precedence,
        ContextSkillPrecedence::Critical
    );
    assert!(selection.selected[0]
        .matched_triggers
        .contains(&"payment".to_string()));
    assert!(selection
        .skipped_disabled
        .contains(&"context-debug".to_string()));
}

// shared-corpus: context_resource_skill_parser_reference_parity
#[test]
fn parsed_resource_and_skill_chunks_feed_rust_ingestion_and_retrieval() {
    with_secondary_indexes(|| {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(1024 * 1024, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);
        let report = ingest_resource_skill_context(
            &engine,
            ContextResourceSkillIngestRequest {
                shard_id: 1,
                tenant_hash: 42,
                resources: vec![ContextResourceParseRequest {
                raw_uri: "baseline://resources/runbook.md".to_string(),
                resource_type: Some("md".to_string()),
                text: "# Incident\n\nCheckout latency increased because the payment dependency timed out.\n\n## Fix\n\nRollback the payment gateway canary and verify p95 latency."
                    .to_string(),
                max_chunk_chars: 220,
                overlap_chars: 40,
                chunk_hash_base: None,
                owner_scope: "team:payments".to_string(),
                version: "v1".to_string(),
                watch_interval_minutes: 15,
                parser_name: "unit-test-parser".to_string(),
                },
                ],
                skills: vec![ContextSkillIngestInput {
                    raw_uri: "skills/payment-incident/SKILL.md".to_string(),
                    text: "---\nname: payment-incident\ndescription: Debug payment incident context.\nprecedence: high\nowner_scope: team:payments\nallowed_tools: [context_workflow_harness]\ntriggers: [payment, checkout, latency]\n---\n\n# Payment Incident\n\n## When To Use\n\nUse when checkout latency or payment risk spikes.\n"
                        .to_string(),
                }],
                query: "payment dependency rollback p95 latency".to_string(),
                start_time_ms: 0,
                end_time_ms: 10_000,
                max_events: 8,
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(
            report.status.ok,
            "status={:?} fanout={:?} secondary_indexes={:?}",
            report.status, report.fanout, report.secondary_indexes
        );
        assert_eq!(report.ingest.failed, 0);
        assert!(report.ingest.extracts.iter().all(|extract| {
            extract.event.source_ref.is_empty()
                && extract.event.related_node_hashes.is_empty()
                && extract.event.compact_attrs.is_empty()
                && !extract.source_ref.is_empty()
                && !extract.related_node_hashes.is_empty()
                && extract.summary_refs.len() == 2
        }));
        assert_eq!(report.resource_lifecycle.watched_count, 1);
        assert_eq!(
            report
                .resource_lifecycle
                .import_kinds
                .get("markdown")
                .copied(),
            Some(1)
        );
        assert_eq!(report.skill_registry.enabled_count, 1);
        assert_eq!(
            report.skill_registry.highest_precedence,
            ContextSkillPrecedence::High
        );
        assert_eq!(
            report.skill_selection.selected[0].skill_name,
            "payment-incident"
        );
        assert!(report.skill_selection.selected[0]
            .matched_triggers
            .contains(&"payment".to_string()));
        assert!(report.fanout.query_back_ok, "{:?}", report.fanout);
        assert!(
            report.secondary_indexes.query_back_ok,
            "{:?}",
            report.secondary_indexes
        );
        assert_eq!(report.fanout.node_count, report.ingest.accepted);
        assert_eq!(report.fanout.event_count, report.ingest.accepted);
        assert_eq!(report.fanout.slab_count, report.ingest.accepted);
        assert_eq!(report.fanout.entity_count, report.ingest.accepted);
        assert_eq!(report.fanout.child_ref_count, report.ingest.accepted);
        assert_eq!(report.fanout.compression_count, report.ingest.accepted);
        assert_eq!(report.fanout.summary_count, report.ingest.accepted * 2);
        assert_eq!(report.fanout.embedding_count, report.ingest.accepted * 3);
        assert_eq!(report.fanout.dirty_marker_count, report.ingest.accepted);
        assert!(!report.secondary_indexes.resource_refs.is_empty());
        assert!(!report.secondary_indexes.skill_refs.is_empty());
        assert!(!report.secondary_indexes.entity_refs.is_empty());
        assert!(!report.secondary_indexes.source_refs.is_empty());
        assert!(!report.secondary_indexes.summary_refs.is_empty());
        assert!(report
            .retrieval
            .blocks
            .iter()
            .any(|block| block.text.contains("payment dependency timed out")
                || block.text.contains("payment gateway canary")));

        let extract_node_hashes: Vec<u64> = report
            .ingest
            .extracts
            .iter()
            .map(|extract| extract.node.node_hash)
            .collect();
        let secondary_indexes = report.secondary_indexes.clone();
        let checked_ref_count = secondary_indexes.resource_refs.len()
            + secondary_indexes.skill_refs.len()
            + secondary_indexes.entity_refs.len()
            + secondary_indexes.source_refs.len()
            + secondary_indexes.summary_refs.len();
        drop(engine);

        let restored = TemporalEngine::with_local_dirs(1024 * 1024, &cache_dir, &page_dir, &index_dir);
        restored.load_shard(1);
        // The vectors persisted on the nodes themselves, so a cold reload proves embedding
        // durability by fetching the owners.
        let nodes = restored.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNodes {
                tenant_hash: 42,
                node_hashes: extract_node_hashes.clone(),
            },
        });
        assert!(matches!(
            nodes.response,
            CommandResponse::ContextNodes { ref nodes }
                if nodes.len() == extract_node_hashes.len()
                    && nodes.iter().all(|node| node.vector.len() == 16)
        ));
        let validation = validate_resource_skill_secondary_indexes(
            &restored,
            ContextResourceSkillSecondaryIndexValidationRequest {
                shard_id: 1,
                tenant_hash: 42,
                start_time_ms: 0,
                end_time_ms: 10_000,
                secondary_indexes,
            },
        );
        assert!(
            validation.status.ok,
            "secondary index validation failed: {:?}",
            validation
        );
        assert!(validation.query_back_ok);
        assert_eq!(validation.checked_ref_count, checked_ref_count);
        assert_eq!(validation.found_ref_count, checked_ref_count);
        assert!(validation.missing_refs.is_empty());
        assert_eq!(validation.families.len(), 5);
        assert!(validation
            .families
            .iter()
            .all(|family| family.checked_ref_count > 0
                && family.checked_ref_count == family.found_ref_count
                && family.missing_refs.is_empty()));
})
}

// shared-corpus: context_resource_skill_live_embedding_summary_retrieval
#[test]
fn resource_ingest_uses_live_embeddings_and_summary_retrieval() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        for expected_path in ["/v1/chat/completions", "/v1/embeddings", "/v1/embeddings"] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let read = stream.read(&mut chunk).unwrap();
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let request = String::from_utf8_lossy(&buffer[..header_end + 4]);
                    if request.contains(format!("POST {expected_path} HTTP/1.1").as_str()) {
                        assert!(request.contains("Authorization: Bearer test-context-key"));
                        let content_length = request
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("content-length:")
                                    .or_else(|| line.strip_prefix("Content-Length:"))
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or_default();
                        if buffer.len() >= header_end + 4 + content_length {
                            break;
                        }
                    }
                }
            }
            let body_start = buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
                .unwrap_or(buffer.len());
            let request_body: serde_json::Value =
                serde_json::from_slice(&buffer[body_start..]).unwrap();
            let body = if expected_path.ends_with("chat/completions") {
                assert_eq!(request_body["model"], "resource-chat-live-test");
                serde_json::json!({
                    "choices": [{
                        "message": {
                            "content": "{\"l0\":\"payment latency runbook\",\"l1\":\"kind=Document; facts=payment timeout rollback p95 latency\"}"
                        }
                    }]
                })
                .to_string()
            } else {
                assert_eq!(request_body["model"], "resource-embedding-live-test");
                let count = request_body["input"].as_array().unwrap().len();
                let data = (0..count)
                    .map(|index| {
                        serde_json::json!({
                            "embedding": [1.0_f32, index as f32, 0.25_f32, 0.0_f32]
                        })
                    })
                    .collect::<Vec<_>>();
                serde_json::json!({ "data": data }).to_string()
            };
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            stream.flush().unwrap();
        }
    });

    std::env::set_var("TS_CONTEXT_TEST_KEY", "test-context-key");
    let engine = test_engine();
    let provider = ContextModelProviderConfig {
        provider_name: "resource-live".to_string(),
        provider_kind: ContextProviderKind::OpenAiCompatible,
        base_url: format!("http://{addr}/v1"),
        api_key_env: "TS_CONTEXT_TEST_KEY".to_string(),
        model: "resource-chat-live-test".to_string(),
        embedding_model: "resource-embedding-live-test".to_string(),
        mock_mode: false,
        ..ContextModelProviderConfig::default()
    };
    let report = ingest_resource_skill_context(
        &engine,
        ContextResourceSkillIngestRequest {
            shard_id: 1,
            tenant_hash: 77,
            resources: vec![ContextResourceParseRequest {
                raw_uri: "baseline://resources/payment-runbook.md".to_string(),
                resource_type: Some("md".to_string()),
                text: "# Payment Runbook\n\nPayment dependency timeout requires rollback and p95 latency validation."
                    .to_string(),
                max_chunk_chars: 1_400,
                overlap_chars: 120,
                chunk_hash_base: None,
                owner_scope: "team:payments".to_string(),
                version: "v1".to_string(),
                watch_interval_minutes: 0,
                parser_name: "unit-test-parser".to_string(),
            }],
            skills: Vec::new(),
            query: "payment timeout p95 rollback".to_string(),
            start_time_ms: 0,
            end_time_ms: 10_000,
            max_events: 4,
            provider,
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert_eq!(report.embedding_evidence.extract_count, 1);
    assert_eq!(report.embedding_evidence.requested_vector_count, 3);
    assert_eq!(report.embedding_evidence.generated_vector_count, 3);
    assert_eq!(report.embedding_evidence.live_call_count, 1);
    assert_eq!(report.embedding_evidence.mock_generation_count, 0);
    assert!(report.embedding_evidence.production_evidence_ready);
    assert_eq!(
        report.embedding_evidence.provider_names,
        vec!["resource-live".to_string()]
    );
    assert_eq!(report.embedding_evidence.vector_dimensions, vec![4]);
    let traversal = &report
        .retrieval
        .query_understanding_debug
        .tree_traversal_summary;
    assert_eq!(traversal.query_embedding_provider, "resource-live");
    assert_eq!(traversal.query_embedding_dimension, 4);
    assert_eq!(traversal.summary_embedding_candidate_count, 1);
    assert_eq!(traversal.summary_embedding_selected_count, 1);
    assert!(traversal
        .summary_embeddings
        .iter()
        .any(|entry| entry.starts_with("node:") && entry.contains(":score:")));
    handle.join().unwrap();
    std::env::remove_var("TS_CONTEXT_TEST_KEY");
}

#[test]
fn retrieval_ranks_by_vectors_that_live_only_on_the_nodes() {
    // No separate embedding rows exist anywhere in this store. If the summary-scoring pass
    // still read node_l0 through the rows, every node here would score zero, selection would
    // collapse to the lexical/recency fallback, and dropping the rows would degrade retrieval
    // silently -- this test is what makes that impossible to miss.
    let engine = test_engine();
    const TENANT: u64 = 6001;
    const EVENT_TIME: u64 = 1_781_700_000_000;
    let provider = ContextModelProviderConfig::default();
    let query = "how do we deploy the ingest service";
    let query_vector = query::context_query_embedding(&provider, query).unwrap();
    // An orthogonal-ish vector: shift every component's mass to a different slot so the cosine
    // against the query is far from 1.
    let mut away = query_vector.clone();
    away.rotate_left(1);

    // The decoy is built to win every path EXCEPT embedding scoring: its summary text repeats
    // the query's own words (so the lexical fallback prefers it) and it is the most recent (so
    // recency ordering prefers it). Only a real cosine score against the inline vectors puts
    // the matching node ahead -- which is what failing over to the rows (all absent) would lose.
    for (node_hash, name, summary, at, vector) in [
        (11u64, "matching", "release runbook notes".to_string(),
         EVENT_TIME, query_vector.clone()),
        (12, "decoy", format!("{query} {query}"), EVENT_TIME + 500, away.clone()),
        (13, "other", "unrelated text".to_string(), EVENT_TIME, away.clone()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: name.to_string(),
                    l0: summary.clone(),
                    status: 0,
                    last_event_time_ms: at,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                },
            },
        });
        assert!(response.status.ok);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertSummary {
                tenant_hash: TENANT,
                summary: ContextSummary {
                    node_hash,
                    level: 1,
                    text: summary.clone(),
                    valid_from_ms: at,
                    vector: Vec::new(),
                },
            },
        });
        assert!(response.status.ok);
        // The ONLY place the vector goes: the node itself.
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash: TENANT,
                node_hash,
                model_hash: 1,
                vector,
                updated_at_ms: at,
            },
        });
        assert!(response.status.ok);
    }

    let retrieve = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash: TENANT,
            node_hashes: vec![11, 12, 13],
            query: query.to_string(),
            start_time_ms: 0,
            end_time_ms: EVENT_TIME + 1_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: 1,
            max_event_nodes: 4,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:test".to_string(),
            provider,
        },
    );
    assert!(retrieve.status.ok, "{:?}", retrieve.status);
    let summary_nodes: Vec<u64> = retrieve
        .blocks
        .iter()
        .filter(|block| block.tier == ContextTier::L0)
        .map(|block| block.node_hash)
        .collect();
    assert_eq!(
        vec![11u64],
        summary_nodes,
        "with one summary slot, the node whose inline vector matches the query must win it"
    );
    assert_eq!(
        0, retrieve.fanout_plan.l0_row_fallback_nodes,
        "every node here carries its vector inline, so nothing may fall back to the rows"
    );
}


/// A lifecycle record that says nothing about where its payload lives must decode as holding it
/// inline, which is what constructing one gives you.
///
/// `#[serde(default)]` on a bool decodes an absent field to `false`, and here false means "the
/// payload is in the object store" -- so a record that simply did not mention it would send a
/// reader to an `external_object_uri` it does not carry. The type's own `Default` says true, and
/// decoding must not disagree with constructing.
#[test]
fn omitting_inline_payload_still_means_the_payload_is_held_inline() {
    let mut body: serde_json::Value =
        serde_json::to_value(ContextResourceLifecycleRecord::default()).unwrap();
    let removed = body.as_object_mut().unwrap().remove("inline_payload");
    assert!(removed.is_some(), "a serialised record should carry the field");

    let silent: ContextResourceLifecycleRecord = serde_json::from_value(body).unwrap();
    assert!(
        silent.inline_payload,
        "a record that does not mention where its payload lives decoded as pointing at the object \
         store, and carries no uri to point with"
    );
    assert_eq!(
        silent.inline_payload,
        ContextResourceLifecycleRecord::default().inline_payload,
        "an omitted field must decode to what the type's own default says"
    );

    // Saying it explicitly still works -- this is about what silence means.
    let mut external: serde_json::Value =
        serde_json::to_value(ContextResourceLifecycleRecord::default()).unwrap();
    external
        .as_object_mut()
        .unwrap()
        .insert("inline_payload".to_string(), serde_json::Value::Bool(false));
    let stated: ContextResourceLifecycleRecord = serde_json::from_value(external).unwrap();
    assert!(!stated.inline_payload);
}


// ---------------------------------------------------------------------------------------------
// Changing the embedding model. Two encoders are frequently the SAME WIDTH -- all-MiniLM-L6-v2 and
// BGE-M3 truncated to 384 are both 384 -- so a swap mixes two unrelated vector spaces with no
// length mismatch anywhere to raise an error. Nothing in the logs, no failed request: retrieval
// just quietly stops matching on anything ingested before the change. These three guards are what
// stands between a customer picking a different encoder and that outcome.
// ---------------------------------------------------------------------------------------------

#[test]
fn embedding_similarity_refuses_to_score_vectors_of_different_widths() {
    // Scoring the shared prefix returns a perfectly plausible cosine -- and a plausible number is
    // worse than none, because it RANKS. A wide vector whose first 16 elements happen to line up
    // would outrank a correctly-scored one.
    let short = vec![1.0_f32, 0.0, 0.0, 0.0];
    let mut long = short.clone();
    long.extend_from_slice(&[0.0, 0.0, 0.0, 0.0]);

    assert_eq!(
        0,
        context_embedding_similarity_micros(&short, &long),
        "a vector of another width was scored on its shared prefix"
    );
    assert_eq!(
        0,
        context_embedding_similarity_micros(&long, &short),
        "the length check must hold whichever side is longer"
    );

    // The same-width path must still score, or this test would pass by refusing everything.
    assert_eq!(
        1_000_000,
        context_embedding_similarity_micros(&short, &short),
        "identical vectors must still score 1.0"
    );
    assert!(
        context_embedding_similarity_micros(&long, &long) > 0,
        "same-width vectors must still be compared"
    );
}

#[test]
fn embedding_model_hash_identifies_the_encoder_and_treats_unset_as_unknown() {
    assert_ne!(
        context_embedding_model_hash("encoder-alpha"),
        context_embedding_model_hash("encoder-beta"),
        "two encoders must not share a hash, or the guard cannot tell them apart"
    );
    assert_eq!(
        context_embedding_model_hash("encoder-alpha"),
        context_embedding_model_hash("  encoder-alpha  "),
        "surrounding whitespace must not make the same encoder look like a different one"
    );
    // 0 means "unknown", and unknown must stay scoreable -- every store written before the hash
    // existed carries 0, and refusing those would take an upgrade straight to dark retrieval.
    assert_eq!(0, context_embedding_model_hash(""));
    assert_eq!(0, context_embedding_model_hash("   "));
    assert_ne!(0, context_embedding_model_hash("encoder-alpha"));
}

fn provider_with_encoder(chat: &str, encoder: &str) -> ContextModelProviderConfig {
    ContextModelProviderConfig {
        model: chat.to_string(),
        embedding_model: encoder.to_string(),
        ..ContextModelProviderConfig::default()
    }
}

fn ingest_with_encoder(
    engine: &TemporalEngine,
    tenant_hash: u64,
    provider: ContextModelProviderConfig,
) -> u64 {
    let extract = extract_context(
        engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash,
            source_kind: ContextSourceKind::Chat,
            source_id: "encoder-swap".to_string(),
            title: "Deployment note".to_string(),
            body: "The observability dashboard for the Northstar rollout lives in Grafana."
                .to_string(),
            timestamp_ms: 5_000,
            provider,
        },
    );
    assert!(extract.status.ok, "{:?}", extract.status);
    extract.node.node_hash
}

#[test]
fn ingest_records_the_embedding_model_not_the_chat_model() {
    // These are two different fields and were two different values. Hashing `provider.model` --
    // the CHAT model -- recorded something that does not identify the encoder at all, and
    // disagreed with the drainer, which hashes `provider.embedding_model`. So an inline-embedded
    // node and a drainer-embedded node carried different hashes for the SAME encoder, and the
    // retrieve guard could not compare them: it either skipped vectors that were fine or scored
    // vectors that were not.
    let engine = test_engine();
    let node_hash = ingest_with_encoder(
        &engine,
        20260830,
        provider_with_encoder("chat-alpha", "encoder-beta"),
    );

    let node = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 20260830,
            node_hash,
        },
    });
    let stored = match node.response {
        CommandResponse::ContextNode { node: Some(node), .. } => node,
        other => panic!("expected the node back, got {other:?}"),
    };
    assert!(
        !stored.vector.is_empty(),
        "nothing was embedded, so the hash assertions below would pass on an empty happy path"
    );
    assert_eq!(
        context_embedding_model_hash("encoder-beta"),
        stored.embedding_model_hash,
        "the node did not record the encoder that made its vector"
    );
    assert_ne!(
        context_embedding_model_hash("chat-alpha"),
        stored.embedding_model_hash,
        "the node recorded the chat model, which says nothing about the vector"
    );
}

#[test]
fn ingest_and_the_drainer_agree_on_what_a_given_encoder_hashes_to() {
    // The two paths that write this field must derive it from the same input. When they did not,
    // whether a node was comparable depended on which path happened to embed it.
    let engine = test_engine();
    let node_hash = ingest_with_encoder(
        &engine,
        20260831,
        provider_with_encoder("chat-alpha", "encoder-beta"),
    );
    let node = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 20260831,
            node_hash,
        },
    });
    let stored = match node.response {
        CommandResponse::ContextNode { node: Some(node), .. } => node,
        other => panic!("expected the node back, got {other:?}"),
    };
    let drainer_hash = context_embedding_model_hash(
        &provider_with_encoder("chat-alpha", "encoder-beta").embedding_model,
    );
    assert_ne!(0, drainer_hash);
    assert_eq!(
        drainer_hash, stored.embedding_model_hash,
        "the inline path and the deferred path disagree about the same encoder"
    );
}

fn retrieve_with_encoder(
    engine: &TemporalEngine,
    tenant_hash: u64,
    node_hash: u64,
    provider: ContextModelProviderConfig,
) -> ContextRetrieveReport {
    retrieve_context(
        engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash,
            // The local workflow refuses a retrieve that names no nodes rather than walking the
            // whole tenant, so the caller supplies the one it just ingested.
            node_hashes: vec![node_hash],
            query: "observability dashboard".to_string(),
            start_time_ms: 0,
            end_time_ms: 10_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider,
        },
    )
}

#[test]
fn retrieve_does_not_score_a_vector_made_by_another_encoder() {
    // The whole failure mode in one test: same width, different space, no error anywhere. A node
    // embedded by one encoder and queried by another must be handed to the hybrid lexical pass --
    // treated exactly as an un-embedded node -- rather than scored against a query vector from an
    // unrelated space.
    let engine = test_engine();
    let node_hash = ingest_with_encoder(
        &engine,
        20260832,
        provider_with_encoder("chat-alpha", "encoder-alpha"),
    );

    let same = retrieve_with_encoder(
        &engine,
        20260832,
        node_hash,
        provider_with_encoder("chat-alpha", "encoder-alpha"),
    );
    assert!(same.status.ok, "{:?}", same.status);
    assert!(
        same.fanout_plan.summary_candidate_nodes > 0,
        "no candidate nodes at all, so the counts below would be vacuously equal"
    );
    assert_eq!(
        0, same.fanout_plan.l0_row_fallback_nodes,
        "a node embedded by the encoder doing the retrieve must be scored, not skipped"
    );

    let foreign = retrieve_with_encoder(
        &engine,
        20260832,
        node_hash,
        provider_with_encoder("chat-alpha", "encoder-beta"),
    );
    assert!(foreign.status.ok, "{:?}", foreign.status);
    assert_eq!(
        same.fanout_plan.summary_candidate_nodes, foreign.fanout_plan.summary_candidate_nodes,
        "the two retrieves must consider the same nodes, or the comparison is between two \
         different searches"
    );
    assert!(
        foreign.fanout_plan.l0_row_fallback_nodes > 0,
        "a vector made by a different encoder was scored against this query -- the two are the \
         same width and no length mismatch would have caught it"
    );
}

#[test]
fn retrieve_still_scores_vectors_whose_encoder_was_never_recorded() {
    // 0 means unknown, not foreign. Every store written before the hash existed carries 0, so
    // refusing those would turn an upgrade into silently dark retrieval -- the same outcome the
    // guard exists to prevent.
    let engine = test_engine();
    let node_hash = ingest_with_encoder(
        &engine,
        20260833,
        provider_with_encoder("chat-alpha", "encoder-alpha"),
    );
    let node = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 20260833,
            node_hash,
        },
    });
    let mut stored = match node.response {
        CommandResponse::ContextNode { node: Some(node), .. } => node,
        other => panic!("expected the node back, got {other:?}"),
    };
    assert!(!stored.vector.is_empty());
    // Rewrite it the way a store predating the field looks.
    let vector = stored.vector.clone();
    stored.embedding_model_hash = 0;
    let put = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: 20260833,
            node: stored,
        },
    });
    assert!(put.status.ok, "{:?}", put.status);
    let reread = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 20260833,
            node_hash,
        },
    });
    match reread.response {
        CommandResponse::ContextNode { node: Some(node), .. } => {
            assert_eq!(0, node.embedding_model_hash, "the rewrite did not take");
            assert_eq!(vector, node.vector, "the rewrite dropped the vector it was testing");
        }
        other => panic!("expected the node back, got {other:?}"),
    }

    let report = retrieve_with_encoder(
        &engine,
        20260833,
        node_hash,
        provider_with_encoder("chat-alpha", "encoder-beta"),
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert!(report.fanout_plan.summary_candidate_nodes > 0);
    assert_eq!(
        0, report.fanout_plan.l0_row_fallback_nodes,
        "a vector whose encoder was never recorded was refused -- unknown is not foreign"
    );
}


#[test]
fn retrieve_falls_back_on_a_width_mismatch_even_when_no_encoder_was_recorded() {
    // The hash check cannot see this one: a store written before the field existed carries 0 on
    // both sides, so nothing is "foreign", and it can still hold vectors of another width --
    // `mixed_dimensions` is a state the product reports, so this is not hypothetical.
    //
    // Scoring it returns 0, and the damage is not the 0: the call site counts the node as scored,
    // and the hybrid lexical pass only rescues nodes that were never scored. The node ends up with
    // no cosine and no lexical score either.
    let engine = test_engine();
    let node_hash = ingest_with_encoder(
        &engine,
        20260834,
        provider_with_encoder("chat-alpha", "encoder-alpha"),
    );
    let node = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 20260834,
            node_hash,
        },
    });
    let mut stored = match node.response {
        CommandResponse::ContextNode { node: Some(node), .. } => node,
        other => panic!("expected the node back, got {other:?}"),
    };
    assert!(!stored.vector.is_empty());

    // A vector of another width, with no encoder recorded -- exactly what an older store holds.
    stored.vector = vec![0.5_f32; stored.vector.len() * 2];
    stored.embedding_model_hash = 0;
    let put = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: 20260834,
            node: stored,
        },
    });
    assert!(put.status.ok, "{:?}", put.status);

    let report = retrieve_with_encoder(
        &engine,
        20260834,
        node_hash,
        provider_with_encoder("chat-alpha", "encoder-alpha"),
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert!(
        report.fanout_plan.summary_candidate_nodes > 0,
        "no candidates, so the count below would be vacuously zero"
    );
    assert!(
        report.fanout_plan.l0_row_fallback_nodes > 0,
        "a vector of another width was scored and counted as scored, which is what keeps the \
         hybrid lexical pass from rescuing the node"
    );
}
