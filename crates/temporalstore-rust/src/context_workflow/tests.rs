use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

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
    assert!(retrieve.parity.pipeline_ready);
    assert!(retrieve.parity.cpp_context_models_ready);
    assert!(retrieve.parity.openviking_tiers_ready);
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

// shared-corpus: context_benchmark_injection_entity_segment_index
#[test]
fn context_benchmark_injection_uses_entity_segment_l0_l1_and_secondary_index() {
    let engine = test_engine();
    let extract = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 20260622,
                source_kind: ContextSourceKind::Chat,
                source_id: "locomo-conv-7-session-3-turn-18".to_string(),
                title: "Caroline medical appointment update".to_string(),
                body: "Caroline told Maya on March 12 that her cardiology appointment moved after the museum visit, and Maya should remind her brother Leo before Friday.".to_string(),
                timestamp_ms: 1_712_300_000_000,
                provider: ContextModelProviderConfig::default(),
            },
        );
    assert!(extract.status.ok, "{:?}", extract.status);

    let node_entity: crate::ContextNode = extract.node.clone();
    let segment: crate::ContextSegment = extract.event.clone();
    assert_eq!(node_entity.node_hash, extract.index_ref.primary_node_hash);
    assert_eq!(segment.event_id_hash, extract.index_ref.event_id_hash);
    assert!(node_entity.l0.contains("Caroline"));
    assert!(node_entity.l1_ref.contains("kind=Chat"));
    assert!(segment
        .text
        .contains("cardiology appointment moved after the museum visit"));
    assert_eq!(segment.related_node_hashes, vec![node_entity.node_hash]);

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
        .any(|block| { block.tier == ContextTier::L1 && block.text == node_entity.l1_ref }));
    assert!(retrieved
        .blocks
        .iter()
        .any(|block| { block.tier == ContextTier::L2 && block.text == segment.text }));

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
    assert!(inject.injected_prompt.contains(&node_entity.l1_ref));
    assert!(inject.injected_prompt.contains(&segment.text));
    assert!(inject
        .audit
        .selected_refs
        .iter()
        .any(|audit| audit.node_hash == node_entity.node_hash
            && audit.event_time_ms == segment.event_time_ms));
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
            profile: "vikingmem_unit_profile".to_string(),
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
        "vikingmem_style_context_management_local"
    );
    assert_eq!(benchmark.profile, "vikingmem_unit_profile");
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
            thresholds: ContextPipelineBenchmarkThresholds::default(),
        },
    );
    assert!(sweep.status.ok, "{:?}", sweep.status);
    assert_eq!(
        sweep.benchmark_name,
        "vikingmem_style_context_management_sweep"
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
fn context_workflow_exposes_openviking_open_source_vlm_profiles() {
    let providers = default_context_model_providers();
    let openviking_provider = providers
        .iter()
        .find(|provider| provider.provider_name == "openviking-open-source-vlm")
        .expect("OpenViking open-source provider profile should be exposed");
    assert_eq!(
        openviking_provider.provider_kind,
        ContextProviderKind::OpenAiCompatible
    );
    assert_eq!(openviking_provider.vlm_model, "qwen2.5vl:7b");
    assert_eq!(openviking_provider.embedding_model, "nomic-embed-text");
    assert_eq!(openviking_provider.base_url, "http://127.0.0.1:11434/v1");
    let matrixark_cpp_provider = providers
        .iter()
        .find(|provider| provider.provider_name == "matrixark-cpp-oss-context")
        .expect("MatrixArk C++ path OSS provider profile should be exposed");
    assert_eq!(matrixark_cpp_provider.model, "google/flan-t5-small");
    assert_eq!(
        matrixark_cpp_provider.embedding_model,
        "sentence-transformers/all-MiniLM-L6-v2"
    );
    let vikingmem_reader = providers
        .iter()
        .find(|provider| provider.provider_name == "vikingmem-gpt-4o-mini-reader")
        .expect("VikingMem GPT-4o-mini reader profile should be exposed");
    assert_eq!(vikingmem_reader.model, "gpt-4o-mini");
    assert_eq!(vikingmem_reader.api_key_env, "OPENAI_API_KEY");

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
            ("ContextDirtyModel", 13),
            ("ContextChildModel", 14),
            ("ContextEmbeddingModel", 15),
            ("ContextSummaryModel", 16),
            ("ContextCompressionModel", 17),
            ("ContextEntityModel", 18),
        ]
    );
    assert!(state.parity.cpp_context_model_ids_ready);
    assert!(state.parity.cpp_context_timeline_semantics_ready);
    assert!(state.parity.cpp_context_validation_limits_ready);
    assert!(state
        .openviking_model_profiles
        .iter()
        .any(|profile| profile.vlm_model == "qwen2.5vl:7b"
            && profile.embedding_model == "nomic-embed-text"
            && profile
                .capabilities
                .contains(&"vlm_image_content_understanding".to_string())));
    assert!(state
        .openviking_model_profiles
        .iter()
        .any(|profile| profile.vlm_model.contains("InternVL")));
    assert!(state.openviking_model_profiles.iter().any(|profile| {
        profile.profile_name == "vikingmem-gpt-4o-mini-reader"
            && profile.chat_model == "gpt-4o-mini"
            && profile
                .capabilities
                .contains(&"vikingmem_reader_parity".to_string())
    }));
    assert!(state.openviking_model_profiles.iter().any(|profile| {
        profile.profile_name == "matrixark-cpp-oss-context"
            && profile.chat_model == "google/flan-t5-small"
            && profile.embedding_model == "sentence-transformers/all-MiniLM-L6-v2"
            && profile
                .capabilities
                .contains(&"cpp_path_oss_model_parity".to_string())
    }));
    assert!(state.openviking_model_profiles.iter().any(|profile| {
        profile.profile_name == "openviking-minigpt4-gpt-style-vlm"
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

// shared-corpus: context_openviking_blocks_provider_switches
#[test]
fn context_openviking_blocks_and_provider_model_switches_are_reported() {
    let engine = test_engine();
    let open_source_text_provider = ContextModelProviderConfig {
        provider_name: "matrixark-cpp-oss-context".to_string(),
        provider_kind: ContextProviderKind::OpenAiCompatible,
        base_url: "http://127.0.0.1:8000/v1".to_string(),
        model: "google/flan-t5-small".to_string(),
        embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
        vlm_model: "none".to_string(),
        mock_mode: true,
        ..ContextModelProviderConfig::default()
    };
    let openviking_vlm_provider = ContextModelProviderConfig {
        provider_name: "openviking-minigpt4-gpt-style-vlm".to_string(),
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
                        body: "OpenViking VLM memory: receipt image shows Northstar Cafe total $18.40.".to_string(),
                        timestamp_ms: 2_000,
                        provider: openviking_vlm_provider.clone(),
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
            .get("matrixark-cpp-oss-context"),
        Some(&1)
    );
    assert_eq!(
        ingest
            .summary
            .provider_counts
            .get("openviking-minigpt4-gpt-style-vlm"),
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

        let embedding_refs = vec![
            context_embedding_ref_hash(20260620, extract.node.node_hash, "node_l0"),
            context_embedding_ref_hash(20260620, extract.node.node_hash, "node_l1"),
            context_embedding_ref_hash(20260620, extract.event.event_id_hash, "event_text"),
        ];
        let embeddings = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEmbeddings {
                tenant_hash: 20260620,
                ref_hashes: embedding_refs,
                limit: Some(8),
            },
        });
        assert!(matches!(
            embeddings.response,
            CommandResponse::ContextEmbeddings { ref embeddings }
                if embeddings.len() == 3
                    && embeddings.iter().all(|embedding| embedding.vector.len() == 16)
                    && embeddings.iter().all(|embedding| embedding.updated_at_ms == extract.event.event_time_ms)
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

// shared-corpus: context_openviking_reasoning_vlm_parity
#[test]
fn context_openviking_reasoning_vlm_cases_cover_required_gaps() {
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
                .openviking_parity_categories
                .contains(&required_category.to_string()),
            "missing OpenViking parity category {required_category}"
        );
    }
    assert_eq!(state.openviking_parity_cases.len(), 6);
    assert!(state
        .openviking_parity_cases
        .iter()
        .any(|case| case.uses_vlm && !case.benchmark_proven));
    assert!(state
        .openviking_parity_cases
        .iter()
        .filter(|case| !case.uses_vlm)
        .all(|case| case.benchmark_proven));

    for case in state.openviking_parity_cases {
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
            body: "body".to_string(),
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
    assert_eq!(report.l0, "doc: body");
}

// shared-corpus: context_resource_skill_parser_openviking_parity
#[test]
fn context_resource_parser_matches_openviking_stable_refs() {
    let report = parse_context_resource(ContextResourceParseRequest {
        raw_uri: "runbook.md".to_string(),
        resource_type: Some("md".to_string()),
        text: "# Rollback\n\nUse canary rollback. See [runbook](viking://resources/runbook-extra.md).\n\n## Checks\n\nConfirm p95 latency.\n\n```bash\ncurl /health\n```".to_string(),
        max_chunk_chars: 1_400,
        overlap_chars: 120,
        chunk_hash_base: Some(900),
    });
    assert!(report.status.ok);
    assert_eq!(report.resource_type, "md");
    assert_eq!(report.uri_scheme, "file");
    assert_eq!(report.resource_title, "runbook.md");
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
        Some("viking://resources/runbook-extra.md")
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

// shared-corpus: context_resource_skill_parser_openviking_parity
#[test]
fn context_skill_parser_extracts_frontmatter_and_capability_sections() {
    let skill = parse_context_skill_markdown(
            "skills/context-debug/SKILL.md",
            "---\nname: context-debug\ndescription: Trace context ingestion and retrieval.\nversion: 1.2.0\nowner_scope: team:context\ntags: [context, debug, openviking]\nallowed_tools:\n  - context_workflow_harness\n  - codex_context_hook\ntriggers: [context-debug, retrieval-trace]\nmodels: [nomic-embed-text, qwen2.5vl]\n---\n\n# Context Debug\n\n## When To Use\n\n- Use for context trace debugging.\n\n## Tools\n\n- context_workflow_harness\n- `codex_context_hook` captures prompt context.\n\n## Resources\n\n- [Debug Resource](viking://resources/context-debug.md)\n\n## Examples\n\n- Query the context debug flow for stale entity filters.\n",
        );
    assert!(skill.status.ok);
    assert_eq!(skill.skill_name, "context-debug");
    assert_eq!(skill.description, "Trace context ingestion and retrieval.");
    assert_eq!(skill.version, "1.2.0");
    assert_eq!(skill.owner_scope, "team:context");
    assert_eq!(
        skill.front_matter.get("tags").map(String::as_str),
        Some("[context, debug, openviking]")
    );
    assert!(skill.tag_refs.contains(&"context".to_string()));
    assert!(skill.tag_refs.contains(&"debug".to_string()));
    assert!(skill.tag_refs.contains(&"openviking".to_string()));
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
        .contains(&"viking://resources/context-debug.md".to_string()));
    assert!(skill
        .example_refs
        .contains(&"Query the context debug flow for stale entity filters".to_string()));
    assert!(skill.resource.chunks.iter().all(|chunk| chunk
        .metadata
        .get("resource_type")
        .map(String::as_str)
        == Some("skill")));
}

// shared-corpus: context_resource_skill_parser_openviking_parity
#[test]
fn parsed_resource_and_skill_chunks_feed_rust_ingestion_and_retrieval() {
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
            raw_uri: "viking://resources/runbook.md".to_string(),
            resource_type: Some("md".to_string()),
            text: "# Incident\n\nCheckout latency increased because the payment dependency timed out.\n\n## Fix\n\nRollback the payment gateway canary and verify p95 latency."
                .to_string(),
            max_chunk_chars: 220,
            overlap_chars: 40,
            chunk_hash_base: None,
            },
            ],
            skills: vec![ContextSkillIngestInput {
                raw_uri: "skills/payment-incident/SKILL.md".to_string(),
                text: "---\nname: payment-incident\ndescription: Debug payment incident context.\n---\n\n# Payment Incident\n\n## When To Use\n\nUse when checkout latency or payment risk spikes.\n"
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
    assert!(report.fanout.query_back_ok, "{:?}", report.fanout);
    assert!(
        report.secondary_indexes.query_back_ok,
        "{:?}",
        report.secondary_indexes
    );
    assert_eq!(report.fanout.node_count, report.ingest.accepted);
    assert_eq!(report.fanout.event_count, report.ingest.accepted);
    assert_eq!(report.fanout.segment_count, report.ingest.accepted);
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

    let embedding_refs = report.embedding_refs.clone();
    let secondary_indexes = report.secondary_indexes.clone();
    let checked_ref_count = secondary_indexes.resource_refs.len()
        + secondary_indexes.skill_refs.len()
        + secondary_indexes.entity_refs.len()
        + secondary_indexes.source_refs.len()
        + secondary_indexes.summary_refs.len();
    drop(engine);

    let restored = TemporalEngine::with_local_dirs(1024 * 1024, &cache_dir, &page_dir, &index_dir);
    restored.load_shard(1);
    let embeddings = restored.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEmbeddings {
            tenant_hash: 42,
            ref_hashes: embedding_refs,
            limit: Some(16),
        },
    });
    assert!(matches!(
        embeddings.response,
        CommandResponse::ContextEmbeddings { ref embeddings }
            if embeddings.len() >= report.ingest.accepted * 3
                && embeddings.iter().all(|embedding| embedding.vector.len() == 16)
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
}
