//! Part 1 of engine tests, split from engine/tests.rs.
#![allow(clippy::all)]
use super::*;

// shared-corpus: dynamic_event_replication_mode_selection
#[test]
fn replicated_execute_selects_sync_async_or_raft_without_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let request = ReplicatedBatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            ReplicatedCommand {
                command: Command::StringSet {
                    key: "sync-event".to_string(),
                    value: b"sync".to_vec(),
                },
                replication_mode: EventReplicationMode::SyncStorage,
            },
            ReplicatedCommand {
                command: Command::StringSet {
                    key: "async-event".to_string(),
                    value: b"async".to_vec(),
                },
                replication_mode: EventReplicationMode::AsyncStorage,
            },
            ReplicatedCommand {
                command: Command::StringSet {
                    key: "raft-event".to_string(),
                    value: b"raft".to_vec(),
                },
                replication_mode: EventReplicationMode::Raft,
            },
        ],
    };

    let response = engine.batch_execute_replicated(request);
    assert!(response.status.ok);
    assert!(response.responses.iter().all(|response| response.status.ok));
    assert_eq!(
        response
            .replication
            .iter()
            .map(|report| report.effective_mode)
            .collect::<Vec<_>>(),
        vec![
            EventReplicationMode::SyncStorage,
            EventReplicationMode::AsyncStorage,
            EventReplicationMode::Raft,
        ]
    );
    assert!(response
        .replication
        .iter()
        .all(|report| report.accepted && !report.restart_required));
}

// shared-corpus: context_events_segments_entities_child_refs context_event_index_audit_dirty_models
#[test]
fn context_models_match_cpp_keys_timeline_pages_and_filters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let node = ContextNode {
        node_hash: 42,
        parent_hash: 7,
        kind: 3,
        canonical_name: "checkout".to_string(),
        l0: "service".to_string(),
        status: 1,
        last_event_time_ms: 1_000,
        summary_dirty: true,
        l1_ref: "l1://summary".to_string(),
        raw_metadata_ref: "raw://node".to_string(),
    };
    let cpp_node = ContextNode {
        status: 0,
        summary_dirty: false,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        ..node.clone()
    };
    let upsert = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: 11,
            node: node.clone(),
        },
    });
    assert!(upsert.status.ok);
    assert!(matches!(
        upsert.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:node:11:42"
    ));

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 11,
            node_hash: 42,
        },
    });
    assert!(matches!(
        get.response,
        CommandResponse::ContextNode { node: Some(ref stored), .. } if stored == &cpp_node
    ));
    let meta = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: "ctx:node:11:42".to_string(),
            field: CONTEXT_NODE_FIELD.to_string(),
        },
    });
    assert!(matches!(
        meta.response,
        CommandResponse::Bytes { value: Some(ref bytes) }
            if ContextNode::decode_context_value(bytes).as_ref() == Some(&cpp_node)
    ));

    let entity = ContextEntity {
        entity_hash: 7001,
        node_hash: 42,
        entity_type: 1,
        name: "gpu_purchase_request".to_string(),
        value: "approved".to_string(),
        updated_at_ms: 1_000,
        valid_from_ms: 1_000,
        confidence: 0.97,
        source_event_hashes: vec![5],
    };
    let entity_upsert = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertEntity {
            tenant_hash: 11,
            entity: entity.clone(),
        },
    });
    assert!(entity_upsert.status.ok);
    assert!(matches!(
        entity_upsert.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:entity:11:42:7001"
    ));
    let entity_get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetEntity {
            tenant_hash: 11,
            node_hash: 42,
            entity_hash: 7001,
        },
    });
    assert!(matches!(
        entity_get.response,
        CommandResponse::ContextEntity { entity: Some(ref stored), .. } if stored == &entity
    ));
    let entity_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEntities {
            tenant_hash: 11,
            node_hash: 42,
            entity_hashes: vec![7001, 8888],
            limit: Some(10),
        },
    });
    assert!(matches!(
        entity_query.response,
        CommandResponse::ContextEntities { ref entities, .. }
            if entities == &vec![entity.clone()]
    ));

    let event_a = ContextEvent {
        event_id_hash: 5,
        event_time_ms: 1_000,
        ingestion_time_ms: 1_000,
        kind: 9,
        event_type: 2,
        actor_hash: 77,
        status: 1,
        valid_until_ms: 0,
        confidence: 0.9,
        importance: 0.7,
        text: "first".to_string(),
        source_ref: "src://a".to_string(),
        related_node_hashes: vec![42],
        compact_attrs: vec![1, 2, 3],
    };
    let mut event_b = event_a.clone();
    event_b.event_id_hash = 6;
    event_b.text = "second".to_string();

    for event in [event_a.clone(), event_b.clone()] {
        let write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 11,
                node_hash: 42,
                event,
                first_write_only: true,
                cold_storage: false,
            },
        });
        assert!(write.status.ok);
        assert!(matches!(
            write.response,
            CommandResponse::ContextObjectKey { ref object_key }
                if object_key == "ctx:event:11:42"
        ));
    }
    let duplicate = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteEvent {
            tenant_hash: 11,
            node_hash: 42,
            event: ContextEvent {
                text: "ignored".to_string(),
                ..event_a.clone()
            },
            first_write_only: true,
            cold_storage: false,
        },
    });
    assert!(duplicate.status.ok);

    let queried = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: 11,
            node_hash: 42,
            start_time_ms: 999,
            end_time_ms: 1_001,
            limit: Some(10),
            current_valid_only: true,
            as_of_ms: 0,
            kinds: vec![2],
            statuses: Vec::new(),
            min_confidence: 0.8,
            min_importance: 0.6,
        },
    });
    assert!(matches!(
        queried.response,
        CommandResponse::ContextEvents { ref object_key, ref events }
            if object_key == "ctx:event:11:42"
                && events.iter().map(|event| event.text.as_str()).collect::<Vec<_>>()
                    == vec!["first", "second"]
    ));

    let index_ref = ContextIndexRef {
        primary_node_hash: 42,
        primary_event_time_ms: 1_000,
        event_id_hash: 5,
    };
    let index_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteIndexRef {
            tenant_hash: 11,
            index_name: "actor".to_string(),
            index_value_hash: 77,
            scope_hash: 3,
            event_time_ms: 1_000,
            index_ref: index_ref.clone(),
        },
    });
    assert!(matches!(
        index_write.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctxidx:11:actor:77:3"
    ));
    let index_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryIndex {
            tenant_hash: 11,
            index_name: "actor".to_string(),
            index_value_hash: 77,
            scope_hash: 3,
            start_time_ms: 999,
            end_time_ms: 1_001,
            limit: None,
        },
    });
    assert!(matches!(
        index_query.response,
        CommandResponse::ContextIndexRefs { refs, .. } if refs == vec![index_ref]
    ));

    let extracted_event = ContextEvent {
        event_id_hash: 445,
        event_time_ms: 1_781_500_000_000,
        ingestion_time_ms: 1_781_500_000_000,
        kind: 7,
        event_type: 7,
        actor_hash: 0,
        status: 1,
        valid_until_ms: 0,
        confidence: 0.96,
        importance: 0.88,
        text: "Finance confirmed the Project 1 GPU purchase approval.".to_string(),
        source_ref: "cursor://701".to_string(),
        related_node_hashes: vec![42],
        compact_attrs: Vec::new(),
    };
    let extracted = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteExtractedEvent {
            tenant_hash: 11,
            node_hash: 42,
            event: extracted_event.clone(),
            indexes: ContextExtractedEventIndexes {
                scope_hash: 3001,
                entity_hashes: vec![501, 502],
                status_hash: 601,
                source_hash: 701,
                event_time_bucket_ms: 1_781_500_000_000,
                disabled_indexes: Vec::new(),
            },
            first_write_only: true,
            cold_storage: false,
        },
    });
    assert!(matches!(
        extracted.response,
        CommandResponse::ContextExtractedEventWrite {
            ref event_object_key,
            written_index_count: 6,
            ref index_object_keys,
        } if event_object_key == "ctx:event:11:42" && index_object_keys.len() == 6
    ));
    for (index_name, value_hash, start_time_ms, end_time_ms) in [
        ("event_kind", 7, 1_781_499_999_999, 1_781_500_000_001),
        ("entity", 501, 1_781_499_999_999, 1_781_500_000_001),
        ("entity", 502, 1_781_499_999_999, 1_781_500_000_001),
        ("status", 601, 1_781_499_999_999, 1_781_500_000_001),
        ("source", 701, 1_781_499_999_999, 1_781_500_000_001),
        (
            "event_time_bucket",
            1_781_500_000_000,
            1_781_499_999_999,
            1_781_500_000_001,
        ),
    ] {
        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryIndex {
                tenant_hash: 11,
                index_name: index_name.to_string(),
                index_value_hash: value_hash,
                scope_hash: 3001,
                start_time_ms,
                end_time_ms,
                limit: Some(10),
            },
        });
        assert!(matches!(
            query.response,
            CommandResponse::ContextIndexRefs { refs, .. }
                if refs.len() == 1
                    && refs[0].primary_node_hash == 42
                    && refs[0].primary_event_time_ms == extracted_event.event_time_ms
                    && refs[0].event_id_hash == extracted_event.event_id_hash
        ));
    }

    let disabled_source = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteExtractedEvent {
            tenant_hash: 11,
            node_hash: 43,
            event: ContextEvent {
                event_id_hash: 446,
                event_time_ms: 1_781_500_000_010,
                ingestion_time_ms: 1_781_500_000_010,
                kind: 8,
                event_type: 8,
                actor_hash: 0,
                status: 1,
                valid_until_ms: 0,
                confidence: 0.9,
                importance: 0.8,
                text: "A low-noise event that should not be source-indexed.".to_string(),
                source_ref: "cursor://701".to_string(),
                related_node_hashes: vec![43],
                compact_attrs: Vec::new(),
            },
            indexes: ContextExtractedEventIndexes {
                scope_hash: 3001,
                entity_hashes: Vec::new(),
                status_hash: 602,
                source_hash: 701,
                event_time_bucket_ms: 1_781_500_000_000,
                disabled_indexes: vec![InternalContextIndex::Source],
            },
            first_write_only: false,
            cold_storage: false,
        },
    });
    assert!(matches!(
        disabled_source.response,
        CommandResponse::ContextExtractedEventWrite {
            written_index_count: 3,
            ..
        }
    ));
    let disabled_source_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryIndex {
            tenant_hash: 11,
            index_name: "source".to_string(),
            index_value_hash: 701,
            scope_hash: 3001,
            start_time_ms: 1_781_500_000_009,
            end_time_ms: 1_781_500_000_011,
            limit: Some(10),
        },
    });
    assert!(matches!(
        disabled_source_query.response,
        CommandResponse::ContextIndexRefs { refs, .. } if refs.is_empty()
    ));

    let audit = ContextPackAudit {
        query_id: "q1".to_string(),
        session_hash: 99,
        request_time_ms: 2_000,
        query_hash: 123,
        max_prompt_tokens: 4096,
        selected_tokens: 128,
        selected_refs: vec![ContextAuditRef {
            node_hash: 42,
            event_time_ms: 1_000,
            reason: "ranked".to_string(),
        }],
        blocked_refs: Vec::new(),
    };
    let audit_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWritePackAudit {
            tenant_hash: 11,
            audit: audit.clone(),
        },
    });
    assert!(matches!(
        audit_write.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:audit:11:99"
    ));
    let audit_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryPackAudit {
            tenant_hash: 11,
            session_hash: 99,
            start_time_ms: 1_999,
            end_time_ms: 2_001,
            limit: None,
        },
    });
    assert!(matches!(
        audit_query.response,
        CommandResponse::ContextPackAudits { audits, .. } if audits == vec![audit]
    ));

    let marker = ContextSummaryDirtyMarker {
        node_hash: 42,
        event_time_ms: 3_000,
        reason: 4,
        propagate_depth: 2,
    };
    let dirty_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextMarkSummaryDirty {
            tenant_hash: 11,
            marker: marker.clone(),
        },
    });
    assert!(matches!(
        dirty_write.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:dirty:11:42"
    ));
    let dirty_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQuerySummaryDirty {
            tenant_hash: 11,
            node_hash: 42,
            start_time_ms: 2_999,
            end_time_ms: 3_001,
            limit: None,
        },
    });
    assert!(matches!(
        dirty_query.response,
        CommandResponse::ContextSummaryDirtyMarkers { markers, .. } if markers == vec![marker]
    ));

    assert!(
        engine
            .bucket_storage_summaries(1)
            .iter()
            .map(|summary| summary.page_ref_count)
            .sum::<u64>()
            >= 5
    );
    let recovery = engine.storage_recovery_report(1);
    assert!(
        recovery.total_page_refs >= 5,
        "context pages should be visible to recovery accounting"
    );
}

// shared-corpus: context_tree_embedding_summary_compression
#[test]
fn context_tree_embedding_summary_and_compression_match_cpp_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 1001;
    const ROOT: u64 = 10;
    const GPU: u64 = 20;
    const COST: u64 = 30;
    const EVENT_TIME: u64 = 1_781_500_000_000;

    for node in [
        ContextNode {
            node_hash: ROOT,
            parent_hash: 0,
            kind: 1,
            canonical_name: "company_a".to_string(),
            l0: "Company A context root.".to_string(),
            status: 0,
            last_event_time_ms: 0,
            summary_dirty: false,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
        },
        ContextNode {
            node_hash: GPU,
            parent_hash: ROOT,
            kind: 2,
            canonical_name: "gpu_purchase".to_string(),
            l0: "GPU purchase leaf node.".to_string(),
            status: 0,
            last_event_time_ms: 0,
            summary_dirty: false,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node,
            },
        });
        assert!(response.status.ok);
    }

    let child_gpu = ContextChildRef {
        parent_hash: ROOT,
        child_hash: GPU,
        updated_at_ms: EVENT_TIME,
    };
    for (child_ref, created) in [
        (child_gpu.clone(), true),
        (
            ContextChildRef {
                parent_hash: ROOT,
                child_hash: COST,
                updated_at_ms: EVENT_TIME,
            },
            true,
        ),
        (child_gpu.clone(), false),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertChildRef {
                tenant_hash: TENANT,
                child_ref,
            },
        });
        assert!(matches!(
            response.response,
            CommandResponse::ContextChildRefs {
                ref object_key,
                created: Some(actual_created),
                ..
            } if object_key == "ctx:child:1001:10"
                && actual_created == created
        ));
    }
    let children = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryChildren {
            tenant_hash: TENANT,
            parent_hash: ROOT,
            limit: Some(10),
        },
    });
    assert!(matches!(
        children.response,
        CommandResponse::ContextChildRefs { refs, .. }
            if refs.len() == 2 && refs[0].child_hash == GPU
    ));

    for (ref_hash, first, second) in [(GPU, 1.0, 0.0), (COST, 0.0, 1.0)] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertEmbedding {
                tenant_hash: TENANT,
                embedding: ContextEmbedding {
                    ref_hash,
                    level: 1,
                    model_hash: 0,
                    vector: vec![first, second],
                    updated_at_ms: EVENT_TIME,
                },
            },
        });
        assert!(response.status.ok);
    }
    let traversal = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextTraverseTree {
            tenant_hash: TENANT,
            start_node_hash: ROOT,
            query_vector: vec![1.0, 0.0],
            max_depth: Some(2),
            top_k_per_depth: Some(1),
            max_children_scored_per_parent: Some(10),
            max_candidate_nodes: Some(4),
            leaf_only: true,
        },
    });
    assert!(matches!(
        traversal.response,
        CommandResponse::ContextTraversedNodes { ref nodes }
            if nodes.len() == 1 && nodes[0].node_hash == GPU && nodes[0].score > 0.99
    ));

    for (text, valid_from_ms) in [
        ("L0 GPU purchase summary.", EVENT_TIME),
        ("Latest overall GPU purchase summary.", EVENT_TIME + 5),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertSummary {
                tenant_hash: TENANT,
                summary: ContextSummary {
                    node_hash: GPU,
                    level: 1,
                    text: text.to_string(),
                    valid_from_ms,
                },
            },
        });
        assert!(response.status.ok);
    }
    let summaries = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQuerySummaries {
            tenant_hash: TENANT,
            node_hash: GPU,
            level: 1,
            as_of_ms: EVENT_TIME + 1,
            limit: Some(10),
        },
    });
    assert!(matches!(
        summaries.response,
        CommandResponse::ContextSummaries { ref summaries, .. }
            if summaries.len() == 1 && summaries[0].text == "L0 GPU purchase summary."
    ));

    let compression = ContextCompressionEvent {
        compression_id_hash: 5001,
        node_hash: GPU,
        source_start_ms: EVENT_TIME - 1000,
        source_end_ms: EVENT_TIME,
        compressed_time_ms: EVENT_TIME,
        summary: "Older GPU purchase timeline compressed into one summary.".to_string(),
    };
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteCompressionEvent {
            tenant_hash: TENANT,
            event: compression.clone(),
        },
    });
    assert!(response.status.ok);
    let compression_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryCompressionEvents {
            tenant_hash: TENANT,
            node_hashes: vec![GPU],
            start_time_ms: EVENT_TIME - 2000,
            end_time_ms: EVENT_TIME + 1,
            limit: Some(10),
        },
    });
    assert!(matches!(
        compression_query.response,
        CommandResponse::ContextCompressionEvents { ref events, .. }
            if events == &vec![compression.clone()]
    ));

    let node_context = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryNodeContext {
            tenant_hash: TENANT,
            node_hash: GPU,
            summary_level: Some(1),
            as_of_ms: EVENT_TIME + 10,
            cold_start_time_ms: EVENT_TIME - 2000,
            cold_end_time_ms: EVENT_TIME + 1,
            compression_limit: Some(10),
        },
    });
    assert!(matches!(
        node_context.response,
        CommandResponse::ContextNodeContext {
            node_exists: true,
            overall_summary_exists: true,
            overall_summary: Some(ref summary),
            ref cold_window_summaries,
            ..
        } if summary.text == "Latest overall GPU purchase summary."
            && cold_window_summaries.len() == 1
            && cold_window_summaries[0].summary == compression.summary
    ));
}

// shared-corpus: context_temporal_compression_replayable_summary
#[test]
fn context_temporal_compression_builds_replayable_summary_without_deleting_sources() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3003;
    const NODE: u64 = 9100;
    const START: u64 = 1_781_400_000_000;
    const COMPRESSED_AT: u64 = 1_781_500_000_000;

    for (offset_ms, event_id, text) in [
        (0, 7001, "Week-old approval was created."),
        (10, 7002, "Week-old approval was reviewed by finance."),
        (20, 7003, "Week-old approval was confirmed by infra."),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: ContextEvent {
                    event_id_hash: event_id,
                    event_time_ms: START + offset_ms,
                    ingestion_time_ms: START + offset_ms,
                    kind: 7,
                    event_type: 7,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.96,
                    importance: 0.82,
                    text: text.to_string(),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                },
                first_write_only: false,
                cold_storage: false,
            },
        });
        assert!(response.status.ok);
    }

    let compressed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextCompressEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            source_start_ms: START,
            source_end_ms: START + 20,
            compressed_time_ms: COMPRESSED_AT,
            max_source_events: Some(2),
            min_confidence: 0.9,
            min_importance: 0.8,
        },
    });
    assert!(matches!(
        compressed.response,
        CommandResponse::ContextCompressionEvents {
            ref object_key,
            ref events,
            source_event_count: Some(2),
            truncated_source_events: Some(true),
        } if object_key == "ctx:compress:3003:9100"
            && events.len() == 1
            && events[0].summary.contains("Temporal compression window")
            && events[0].summary.contains("Week-old approval was created")
    ));

    let raw_events = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            start_time_ms: START,
            end_time_ms: START + 20,
            limit: Some(10),
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.0,
            min_importance: 0.0,
        },
    });
    assert!(matches!(
        raw_events.response,
        CommandResponse::ContextEvents { ref events, .. } if events.len() == 3
    ));
}

// shared-corpus: context_temporal_compression_cold_scan context_raw_backfill_cold_ingest
#[test]
fn context_temporal_compression_and_raw_backfill_use_cold_storage_without_cache_promotion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3010;
    const NODE: u64 = 9200;
    const START: u64 = 1_782_400_000_000;

    let cache_puts_before = engine.cache().stats().puts;
    let block_writes_before = engine.block_store().stats().writes;
    for idx in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: ContextEvent {
                    event_id_hash: 8000 + idx,
                    event_time_ms: START + idx * 10,
                    ingestion_time_ms: START + idx * 10,
                    kind: 7,
                    event_type: 7,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.95,
                    importance: 0.85,
                    text: format!("Cold backfill event {idx}"),
                    source_ref: "backfill://raw-query".to_string(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                },
                first_write_only: false,
                cold_storage: true,
            },
        });
        assert!(response.status.ok);
    }
    assert!(engine.block_store().stats().writes > block_writes_before);
    assert_eq!(engine.cache().stats().puts, cache_puts_before);

    let block_reads_before = engine.block_store().stats().reads;
    let cache_puts_before_compress = engine.cache().stats().puts;
    let compressed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextCompressEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            source_start_ms: START,
            source_end_ms: START + 20,
            compressed_time_ms: START + 1_000,
            max_source_events: Some(3),
            min_confidence: 0.9,
            min_importance: 0.8,
        },
    });
    assert!(compressed.status.ok);
    assert!(matches!(
        compressed.response,
        CommandResponse::ContextCompressionEvents {
            ref events,
            source_event_count: Some(3),
            ..
        } if events.len() == 1
    ));
    assert!(engine.block_store().stats().reads > block_reads_before);
    assert_eq!(engine.cache().stats().puts, cache_puts_before_compress);
}

#[test]
fn live_page_segment_ids_scan_all_index_backed_data_models() {
    let mut shard = ShardState::default();
    shard.strings.insert(
        "string".to_string(),
        BlockAddress {
            page_segment_id: 7,
            offset: 0,
            length: 1,
            page_id: None,
            object_id: None,
            routing_bucket: None,
            band_id: None,
            generation: None,
            sha256: None,
        },
    );
    shard.hashes.entry("hash".to_string()).or_default().insert(
        "field".to_string(),
        BlockAddress {
            page_segment_id: 8,
            offset: 0,
            length: 1,
            page_id: None,
            object_id: None,
            routing_bucket: None,
            band_id: None,
            generation: None,
            sha256: None,
        },
    );
    shard.sets.entry("set".to_string()).or_default().insert(
        b"member".to_vec(),
        BlockAddress {
            page_segment_id: 9,
            offset: 0,
            length: 1,
            page_id: None,
            object_id: None,
            routing_bucket: None,
            band_id: None,
            generation: None,
            sha256: None,
        },
    );
    shard
        .features
        .entry("feature".to_string())
        .or_default()
        .insert(
            10,
            BlockAddress {
                page_segment_id: 10,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_bucket: None,
                band_id: None,
                generation: None,
                sha256: None,
            },
        );
    shard
        .sequences
        .entry("sequence".to_string())
        .or_default()
        .insert(
            11,
            BlockAddress {
                page_segment_id: 11,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_bucket: None,
                band_id: None,
                generation: None,
                sha256: None,
            },
        );
    shard.ips.entry("ips".to_string()).or_default().insert(
        12,
        BlockAddress {
            page_segment_id: 12,
            offset: 0,
            length: 1,
            page_id: None,
            object_id: None,
            routing_bucket: None,
            band_id: None,
            generation: None,
            sha256: None,
        },
    );
    shard.ips_meta.entry("ips".to_string()).or_default().insert(
        13,
        IpsPointMeta {
            address: BlockAddress {
                page_segment_id: 13,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_bucket: None,
                band_id: None,
                generation: None,
                sha256: None,
            },
            action_type: Some(1),
            table_id: Some(2),
            request_id: Some("r".to_string()),
        },
    );
    shard
        .risk
        .entry("risk".to_string())
        .or_default()
        .insert(14, 1);

    let ids = collect_live_page_segment_ids(&shard)
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![7, 8, 9, 10, 11, 12]);
}

#[test]
fn page_compaction_rewrites_live_addresses_and_allows_old_segment_gc() {
    let page_dir = unique_temp_path("compact-pages");
    let index_dir = unique_temp_path("compact-index");
    let block_store = LocalBlockStore::new(&page_dir);
    let engine = TemporalEngine::with_cache_block_store_and_index_dir(
        MultiLayerCache::default(),
        block_store.clone(),
        &index_dir,
    );
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v1".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v2".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                    value: b"hv".to_vec(),
                },
            })
            .status
            .ok
    );
    assert_eq!(engine.live_page_segment_ids(1), vec![0]);

    let report = engine.compact_shard_pages(1).unwrap();
    assert_eq!(report.previous_page_segment_id, 0);
    assert_eq!(report.compacted_page_segment_id, 1);
    assert_eq!(report.rewritten_page_refs, 2);
    assert_eq!(report.stale_page_segment_ids, vec![0]);
    assert_eq!(report.before.live_page_segment_count, 1);
    assert_eq!(report.before.total_page_count, 3);
    assert_eq!(report.before.live_page_refs, 2);
    assert_eq!(report.before.stale_page_estimate, 1);
    assert_eq!(report.before.live_ref_density_basis_points, 6_666);
    assert_eq!(report.after.live_page_segment_count, 1);
    assert_eq!(report.after.total_page_count, 2);
    assert_eq!(report.after.live_page_refs, 2);
    assert_eq!(report.after.stale_page_estimate, 0);
    assert_eq!(report.after.live_ref_density_basis_points, 10_000);
    assert_eq!(engine.live_page_segment_ids(1), vec![1]);
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let string_address = shard.strings.get("k").expect("string address");
        let hash_address = shard
            .hashes
            .get("h")
            .and_then(|fields| fields.get("f"))
            .expect("hash address");
        assert_eq!(
            string_address.object_id,
            Some(stable_page_object_id(1, "string", "k", None))
        );
        assert_eq!(
            string_address.routing_bucket,
            Some(page_routing_bucket("k", 0, u32::MAX))
        );
        assert_eq!(
            hash_address.object_id,
            Some(stable_page_object_id(1, "hash", "h", Some("f")))
        );
        assert_eq!(
            hash_address.routing_bucket,
            Some(page_routing_bucket("h", 0, u32::MAX))
        );
    }

    let gc = block_store
        .gc_segments_before_with_live_refs(1, engine.live_page_segment_ids(1))
        .unwrap();
    assert_eq!(gc.removed_page_segment_ids, vec![0]);
    assert_eq!(block_store.segment_ids().unwrap(), vec![1]);

    let restarted = TemporalEngine::with_cache_block_store_and_index_dir(
        MultiLayerCache::default(),
        block_store,
        &index_dir,
    );
    restarted.load_shard(1);
    assert_eq!(
        restarted
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        }
    );
    assert_eq!(
        restarted
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashGet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"hv".to_vec())
        }
    );
}

#[test]
// shared-corpus: storage_dump_load_recovery storage_cache_refill storage_tombstone_compaction;
fn page_compaction_reports_model_layouts_tombstones_object_pages_and_density() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for command in [
        Command::StringSet {
            key: "compact-string".to_string(),
            value: b"old".to_vec(),
        },
        Command::StringSet {
            key: "compact-string".to_string(),
            value: b"new".to_vec(),
        },
        Command::HashSet {
            key: "compact-hash".to_string(),
            field: "field".to_string(),
            value: b"hash-value".to_vec(),
        },
        Command::SetAdd {
            key: "compact-set".to_string(),
            member: b"member".to_vec(),
        },
        Command::FeatureAppend {
            key: "compact-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
        Command::SequenceAdd {
            key: "compact-sequence".to_string(),
            rows: vec![
                SequenceFeatureRow {
                    timestamp_ms: 22,
                    gid: 1,
                    action_type: 2,
                    duration: 3,
                    author_id: 4,
                },
                SequenceFeatureRow {
                    timestamp_ms: 23,
                    gid: 2,
                    action_type: 3,
                    duration: 4,
                    author_id: 5,
                },
            ],
        },
        Command::IpsLoad {
            key: "compact-ips-layout".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 30,
                    value: b"thirty".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 40,
                    value: b"forty".to_vec(),
                },
            ],
        },
        Command::RiskSet {
            family: RiskFamily::Cpc,
            key: "compact-risk".to_string(),
            timestamp_ms: 45,
            amount: 3,
        },
        Command::ContextWriteEvent {
            tenant_hash: 7,
            node_hash: 9,
            event: ContextEvent {
                event_id_hash: 50,
                event_time_ms: 50,
                ingestion_time_ms: 50,
                kind: 0,
                event_type: 1,
                actor_hash: 0,
                status: 0,
                valid_until_ms: 0,
                confidence: 1.0,
                importance: 0.5,
                text: "context event".to_string(),
                source_ref: String::new(),
                related_node_hashes: Vec::new(),
                compact_attrs: Vec::new(),
            },
            first_write_only: false,
            cold_storage: false,
        },
        Command::ContextUpsertEmbedding {
            tenant_hash: 7,
            embedding: ContextEmbedding {
                ref_hash: 90,
                level: 1,
                model_hash: 700,
                vector: vec![0.25, 0.75],
                updated_at_ms: 51,
            },
        },
        Command::ContextUpsertSummary {
            tenant_hash: 7,
            summary: ContextSummary {
                node_hash: 9,
                level: 1,
                text: "compact summary".to_string(),
                valid_from_ms: 52,
            },
        },
        Command::CommonDelete {
            key: "compact-set".to_string(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    let async_response = engine.execute_replicated(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "compact-hot-object".to_string(),
                value: b"hot".to_vec(),
            },
        }
        .with_async_storage(),
    );
    assert!(async_response.status.ok, "{async_response:?}");

    let before = engine.storage_recovery_report(1);
    assert!(before.object_lifecycle.tombstoned_object_ids >= 1);
    assert!(before.object_lifecycle.stale_object_ids >= 1);

    let report = engine.compact_shard_pages(1).unwrap();
    assert!(report.model_layout_compaction_ready, "{report:?}");
    assert!(report.model_layout_compaction_blockers.is_empty());
    assert!(report
        .model_layout_compaction_evidence
        .iter()
        .any(|item| item.contains("rewrites live refs by model layout")));
    assert!(report
        .model_layout_compaction_evidence
        .iter()
        .any(|item| item.contains("tombstone object ids are preserved")));
    assert_eq!(report.rewritten_object_pages, report.rewritten_page_refs);
    assert!(report.rewritten_object_pages >= 5);
    assert!(report.reclaimable_stale_page_segment_count >= 1);
    assert_eq!(
        report.reclaimable_stale_page_segment_count,
        report.stale_page_segment_ids.len()
    );
    assert!(report.model_policy_family_count >= 8);
    assert!(report.tombstone_policy_model_count >= 1);
    assert!(report.stale_density_policy_model_count >= 1);
    assert!(report.layout_aware_policy_model_count >= 6);
    assert!(report.before.stale_page_estimate >= 1);
    assert_eq!(report.after.stale_page_estimate, 0);
    assert!(
        report.before.live_ref_density_basis_points < report.after.live_ref_density_basis_points
    );
    assert_eq!(
        report.tombstoned_object_ids_before,
        report.tombstoned_object_ids_after
    );
    assert!(report.tombstoned_object_ids_after >= 1);

    let layout = |kind: &str| {
        report
            .model_layouts
            .iter()
            .find(|layout| layout.kind == kind)
            .unwrap_or_else(|| panic!("missing layout for {kind}: {:?}", report.model_layouts))
    };
    assert_eq!(layout("string").unique_page_refs, 2);
    assert_eq!(layout("hash").unique_page_refs, 1);
    assert_eq!(layout("feature").index_refs, 2);
    assert_eq!(layout("feature").unique_page_refs, 1);
    assert_eq!(layout("feature").packed_timestamped_pages, 1);
    assert_eq!(layout("sequence").index_refs, 2);
    assert_eq!(layout("sequence").unique_page_refs, 1);
    assert_eq!(layout("sequence").packed_timestamped_pages, 1);
    assert_eq!(layout("ips").index_refs, 2);
    assert_eq!(layout("ips").unique_page_refs, 1);
    assert_eq!(layout("context_event").index_refs, 1);
    assert_eq!(layout("context_embedding").unique_page_refs, 1);
    assert_eq!(layout("context_summary").index_refs, 1);

    let policy = |model_id: &str| {
        report
            .before
            .model_policies
            .iter()
            .find(|policy| policy.model_id == model_id)
            .unwrap_or_else(|| {
                panic!(
                    "missing model policy for {model_id}: {:?}",
                    report.before.model_policies
                )
            })
    };
    assert!(policy("string").stale_density_triggered);
    assert!(policy("hash").layout_aware_rewrite_required);
    assert!(policy("feature").layout_aware_rewrite_required);
    assert!(policy("sequence").layout_aware_rewrite_required);
    assert!(policy("ips").layout_aware_rewrite_required);
    assert!(policy("risk").layout_aware_rewrite_required);
    assert!(policy("context_event").layout_aware_rewrite_required);
    assert!(policy("context_embedding").layout_aware_rewrite_required);
    assert!(policy("context_summary").layout_aware_rewrite_required);
    assert!(policy("hash").object_page_packing_enabled);
    assert!(policy("feature").cold_page_rewrite_eligible_refs >= 1);

    let rewrite_policy = |model_id: &str| {
        report
            .model_rewrite_policies
            .iter()
            .find(|policy| policy.model_id == model_id)
            .unwrap_or_else(|| {
                panic!(
                    "missing rewrite policy for {model_id}: {:?}",
                    report.model_rewrite_policies
                )
            })
    };
    for model_id in [
        "string",
        "hash",
        "feature",
        "ips",
        "risk",
        "context_event",
        "context_embedding",
        "context_summary",
    ] {
        assert!(
            rewrite_policy(model_id).rewritten_page_refs >= 1,
            "expected rewrite evidence for {model_id}"
        );
    }

    let after = engine.storage_recovery_report(1);
    assert_eq!(after.object_lifecycle.owner_mismatch_page_refs, 0);
    assert_eq!(after.object_lifecycle.missing_owner_page_refs, 0);
    assert_eq!(after.object_lifecycle.reused_object_id_conflicts, 0);
    assert_eq!(
        after.object_lifecycle.live_page_refs,
        report.after.live_page_refs
    );
    assert!(after
        .object_lifecycle
        .tombstoned_object_keys
        .iter()
        .any(|key| key == "compact-set"));
    let object_runtime = engine.object_manager_runtime_report(1);
    assert!(object_runtime.object_page_count >= 1);
    assert!(
        engine
            .bucket_storage_summaries(1)
            .iter()
            .flat_map(|summary| summary.page_segment_ids.iter().copied())
            .all(|segment_id| segment_id == report.compacted_page_segment_id),
        "all index summaries should move to compacted segment: {:?}",
        engine.bucket_storage_summaries(1)
    );
}

// shared-corpus: storage_manager_background_loop;
#[test]
fn storage_manager_cycle_runs_prepare_reclaim_evict_expire_compact_and_index_gc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for command in [
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"old".to_vec(),
        },
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"new".to_vec(),
        },
        Command::FeatureAppend {
            key: "manager-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
        Command::StringSet {
            key: "manager-expire".to_string(),
            value: b"gone".to_vec(),
        },
        Command::CommonExpire {
            key: "manager-expire".to_string(),
            ttl_ms: 1,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        max_dump_buckets_per_round: 16,
        min_undumped_oplog_records: 0,
        warm_cache: true,
        ..StorageManagerCycleRequest::default()
    });

    assert!(report.completed, "{report:?}");
    for phase in [
        "prepare",
        "reclaim_oplog",
        "evict",
        "expire",
        "compact",
        "index_gc",
    ] {
        assert!(
            report
                .stages
                .iter()
                .any(|entry| entry.stage == phase && entry.enabled),
            "missing attempted phase {phase}: {report:?}"
        );
    }
    assert!(report.lifecycle_report.is_some());
    assert!(report
        .expiry_report
        .as_ref()
        .is_some_and(|expiry| expiry.expired_records_removed >= 1));
    assert!(report
        .compaction_report
        .as_ref()
        .is_some_and(|compaction| compaction.model_layout_compaction_ready));
    assert!(
        report
            .stages
            .iter()
            .find(|phase| phase.stage == "prepare")
            .unwrap()
            .dirty_bucket_count
            >= 1
    );
    assert_eq!(
        report.cxx_stage_order,
        vec![
            "prepare",
            "reclaim_oplog",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
    );
}

// shared-corpus: storage_data_structure_api_parity
#[test]
fn storage_data_structure_api_parity_report_covers_stream_block_and_manager_surfaces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for (key, value) in [("parity-a", b"one".to_vec()), ("parity-b", b"two".to_vec())] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    engine.block_store().roll_segment().unwrap();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "parity-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"feature".to_vec(),
            }],
        },
    });
    assert!(response.status.ok, "{response:?}");

    let report = engine.storage_data_structure_api_parity_report(1);
    assert!(report.ready, "{report:?}");
    assert!(report.bucket_object_page_authority_ready);
    assert!(report.bucket_store_layout_api_ready);
    assert!(report.object_manager_runtime_api_ready);
    assert!(report.block_address_api_ready);
    assert!(report.block_store_segment_api_ready);
    assert!(report.stream_backed_band_api_ready);
    assert!(report.legacy_page_zone_aliases_ready);
    assert!(report.storage_manager_phase_api_ready);
    assert!(report.storage_manager_pressure_api_ready);
    assert!(report.storage_manager_merged_dump_load_api_ready);
    assert!(report.bucket_count >= 1);
    assert!(report.page_index_count >= 1);
    assert!(report.block_index_count >= 1);
    assert!(report.stream_band_count >= 2);
    assert!(report.stream_record_count >= 3);
    assert_eq!(
        report.storage_manager_stage_order,
        vec![
            "prepare",
            "reclaim_oplog",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
    );
}

// shared-corpus: storage_manager_background_loop;
#[test]
fn storage_manager_loop_runs_prepare_reclaim_evict_expire_compact_and_index_gc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for command in [
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"old".to_vec(),
        },
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"new".to_vec(),
        },
        Command::FeatureAppend {
            key: "manager-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
        Command::StringSet {
            key: "manager-expire".to_string(),
            value: b"gone".to_vec(),
        },
        Command::CommonExpire {
            key: "manager-expire".to_string(),
            ttl_ms: 1,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.run_storage_manager_loop(StorageManagerLoopRequest {
        shard_id: 1,
        apply: true,
        expire_records: true,
        compact_pages: true,
        lifecycle: StorageLifecycleRequest {
            shard_id: 1,
            max_dump_buckets_per_round: 16,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: true,
            prune_bucket_dump_manifests: true,
            roll_forward_bucket_dump_installs: true,
            invalidate_cache: true,
            warm_cache: true,
            ..StorageLifecycleRequest::default()
        },
    });

    assert!(report.loop_ready, "{report:?}");
    for phase in [
        "prepare", "reclaim", "evict", "expire", "compact", "index_gc",
    ] {
        assert!(
            report
                .phases
                .iter()
                .any(|entry| entry.phase == phase && entry.attempted),
            "missing attempted phase {phase}: {report:?}"
        );
    }
    assert!(report.lifecycle.dump_manifest.is_some());
    assert!(report.expiry_sweep.expired_records_removed >= 1);
    assert!(report
        .compaction
        .as_ref()
        .is_some_and(|compaction| compaction.model_layout_compaction_ready));
    assert!(report
        .phases
        .iter()
        .find(|phase| phase.phase == "prepare")
        .unwrap()
        .evidence
        .iter()
        .any(|item| item.contains("dirty slots")));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("prepare/reclaim/evict/expire/compact/index-GC")));
}

#[test]
fn recovery_reports_owner_mismatch_and_compaction_refuses_it() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "owned".to_string(),
                    value: b"value".to_vec(),
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        let page = shard
            .bucket_index
            .bucket_map
            .values_mut()
            .flat_map(|bucket| bucket.page_index.values_mut())
            .find(|page| page.object_key == "owned")
            .expect("owned slot page");
        page.address.object_id = Some(page.object_id.wrapping_add(1));
    }

    let recovery = engine.storage_recovery_report(1);
    assert_eq!(recovery.owner_mismatch_page_refs.len(), 1);
    assert!(!recovery.segment_integrity.integrity_ok);
    assert_eq!(recovery.segment_integrity.owner_mismatch_page_ref_count, 1);
    assert_eq!(recovery.segment_integrity.missing_owner_page_ref_count, 0);
    assert_eq!(recovery.object_lifecycle.live_object_ids, 1);
    assert_eq!(recovery.object_lifecycle.live_page_refs, 1);
    assert_eq!(recovery.object_lifecycle.owner_mismatch_page_refs, 1);
    assert_eq!(
        recovery.owner_mismatch_page_refs[0].expected_object_id,
        stable_page_object_id(1, "string", "owned", None)
    );
    assert_eq!(recovery.boundary.owner_mismatch_page_refs.len(), 1);
    assert_eq!(
        recovery.boundary.object_lifecycle.owner_mismatch_page_refs,
        1
    );

    let err = engine.compact_shard_pages(1).unwrap_err();
    assert_eq!(err.code, "page_compaction_owner_mismatch");
}

#[test]
fn recovery_reports_reused_object_id_conflicts() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for key in ["first", "second"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    },
                })
                .status
                .ok
        );
    }

    let reused_object_id = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        let first_object_id = shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.page_index.values())
            .find(|page| page.object_key == "first")
            .map(|page| page.object_id)
            .expect("first object id");
        let second = shard
            .bucket_index
            .bucket_map
            .values_mut()
            .flat_map(|bucket| bucket.page_index.values_mut())
            .find(|page| page.object_key == "second")
            .expect("second slot page");
        second.address.object_id = Some(first_object_id);
        first_object_id
    };

    let recovery = engine.storage_recovery_report(1);
    assert_eq!(recovery.object_lifecycle.live_object_ids, 2);
    assert_eq!(recovery.object_lifecycle.live_page_refs, 2);
    assert_eq!(recovery.object_lifecycle.reused_object_id_conflicts, 1);
    assert_eq!(
        recovery.object_lifecycle.reused_object_ids,
        vec![reused_object_id]
    );
    assert_eq!(recovery.object_lifecycle.owner_mismatch_page_refs, 1);
    assert_eq!(
        recovery
            .boundary
            .object_lifecycle
            .reused_object_id_conflicts,
        1
    );
}

#[test]
fn crash_recovery_report_covers_oplog_index_page_and_band_manifest() {
    let cache_dir = unique_temp_path("recovery-cache");
    let page_dir = unique_temp_path("recovery-pages");
    let index_dir = unique_temp_path("recovery-index");
    let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v1".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_segment().unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                    value: b"hv".to_vec(),
                },
            })
            .status
            .ok
    );

    let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
    recovered.load_shard(1);
    let report = recovered.storage_recovery_report(1);

    assert!(report.index_bytes > 0);
    assert!(report.index_write_atomic);
    assert_eq!(report.oplog_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert_eq!(report.active_page_segment_ids, vec![0, 1]);
    assert_eq!(report.live_page_segment_ids, vec![0, 1]);
    assert_eq!(report.total_page_refs, 2);
    assert_eq!(report.readable_page_refs, 2);
    assert!(report.all_live_pages_readable);
    assert!(report.segment_integrity.integrity_ok);
    assert!(!report.segment_integrity.reclaim_required);
    assert_eq!(report.segment_integrity.indexed_page_segment_count, 2);
    assert_eq!(report.segment_integrity.discovered_page_segment_count, 2);
    assert_eq!(report.segment_integrity.live_page_segment_count, 2);
    assert_eq!(report.segment_integrity.unreadable_page_ref_count, 0);
    assert_eq!(report.zone_descriptors.len(), 2);
    assert_eq!(
        report.zone_descriptors[0].state,
        BlockStoreBandState::Sealed
    );
    assert_eq!(
        report.zone_descriptors[1].state,
        BlockStoreBandState::Active
    );
    assert_eq!(report.zone_summary.sealed_bands, 1);
    assert_eq!(report.zone_summary.active_bands, 1);
    assert_eq!(report.zone_summary.delayed_destroy_bands, 0);
    assert_eq!(
        report.zone_summary.sealed_physical_bytes,
        report.zone_descriptors[0].physical_bytes
    );
    assert_eq!(
        report.zone_summary.active_physical_bytes,
        report.zone_descriptors[1].physical_bytes
    );
    assert_eq!(
        report.zone_summary.live_physical_bytes,
        report.zone_descriptors[0].physical_bytes + report.zone_descriptors[1].physical_bytes
    );
    assert_eq!(report.page_segment_live_reports.len(), 2);
    assert_eq!(report.page_segment_live_reports[0].page_segment_id, 0);
    assert_eq!(report.page_segment_live_reports[0].page_count, 1);
    assert_eq!(report.page_segment_live_reports[0].live_page_refs, 1);
    assert_eq!(
        report.page_segment_live_reports[0].readable_live_page_refs,
        1
    );
    assert_eq!(
        report.page_segment_live_reports[0].unreadable_live_page_refs,
        0
    );
    assert_eq!(report.page_segment_live_reports[0].stale_page_estimate, 0);
    assert_eq!(
        report.page_segment_live_reports[0].live_ref_density_basis_points,
        10_000
    );
    assert_eq!(report.page_segment_live_reports[0].live_object_count, 1);
    assert_eq!(
        report.page_segment_live_reports[0].live_routing_bucket_count,
        1
    );
    assert_eq!(report.page_segment_live_reports[0].live_logical_bytes, 2);
    assert!(report.page_segment_live_reports[0].live_physical_bytes > 0);

    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashGet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"hv".to_vec())
        }
    );
}

#[test]
fn crash_recovery_report_marks_stale_segment_density_after_overwrite() {
    let cache_dir = unique_temp_path("recovery-density-cache");
    let page_dir = unique_temp_path("recovery-density-pages");
    let index_dir = unique_temp_path("recovery-density-index");
    let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "hot".to_string(),
                    value: b"old".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "hot".to_string(),
                    value: b"new".to_vec(),
                },
            })
            .status
            .ok
    );

    let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
    recovered.load_shard(1);
    let report = recovered.storage_recovery_report(1);
    let segment = report
        .page_segment_live_reports
        .iter()
        .find(|segment| segment.page_segment_id == 0)
        .expect("segment 0 live-density report");

    assert_eq!(segment.page_count, 2);
    assert_eq!(segment.live_page_refs, 1);
    assert_eq!(segment.readable_live_page_refs, 1);
    assert_eq!(segment.stale_page_estimate, 1);
    assert_eq!(segment.live_ref_density_basis_points, 5_000);
    assert_eq!(segment.live_logical_bytes, 3);
    assert_eq!(segment.live_object_count, 1);
    assert_eq!(segment.live_routing_bucket_count, 1);
}

#[test]
// shared-corpus: storage_cache_refill storage_matrixraft_cache_refill_pressure;
fn cold_index_page_address_reads_from_disk_cache_or_block_store_and_refills_memory() {
    let root = tempfile::tempdir().unwrap();
    let cache_dir = root.path().join("cache");
    let page_dir = root.path().join("pages");
    let index_dir = root.path().join("index");
    let engine = TemporalEngine::with_local_dirs(128, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "cold-key".to_string(),
                    value: b"cold-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let page_key = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let address = shard.strings.get("cold-key").expect("indexed page address");
        assert_ne!(address.page_segment_id, HOT_PAGE_SEGMENT_ID);
        CacheKey::page_with_slot(
            1,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_bucket,
        )
    };

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "cold-key".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"cold-value".to_vec())
        }
    );
    assert_eq!(engine.block_store().stats().reads, 1);
    assert!(engine.cache().stats().misses >= 2);
    assert!(engine.cache().stats().puts >= 2);
    assert_eq!(
        engine.cache().get_memory(&page_key),
        Some(b"cold-value".to_vec())
    );

    let _ = engine.cache().invalidate(&CacheKey::string(1, "cold-key"));
    engine.cache().clear_memory_for_test();
    assert_eq!(engine.cache().get_memory(&page_key), None);
    let reads_before_disk_cache = engine.block_store().stats().reads;
    let disk_cache_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "cold-key".to_string(),
        },
    });
    assert_eq!(
        disk_cache_read.response,
        CommandResponse::Bytes {
            value: Some(b"cold-value".to_vec())
        }
    );
    assert_eq!(engine.block_store().stats().reads, reads_before_disk_cache);
    assert!(engine.cache().stats().disk_hits >= 1);
    assert_eq!(
        engine.cache().get_memory(&page_key),
        Some(b"cold-value".to_vec())
    );

    let cold_restart = TemporalEngine::with_local_dirs(
        128,
        root.path().join("fresh-cache"),
        &page_dir,
        &index_dir,
    );
    cold_restart.load_shard(1);
    let restart_read = cold_restart.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "cold-key".to_string(),
        },
    });
    assert_eq!(
        restart_read.response,
        CommandResponse::Bytes {
            value: Some(b"cold-value".to_vec())
        }
    );
    assert_eq!(cold_restart.block_store().stats().reads, 1);
    assert!(cold_restart.cache().stats().misses >= 2);
    assert!(cold_restart.cache().stats().puts >= 2);
}

#[test]
fn crash_recovery_rebuilds_missing_band_manifest_from_page_stream() {
    let cache_dir = unique_temp_path("recovery-rebuild-cache");
    let page_dir = unique_temp_path("recovery-rebuild-pages");
    let index_dir = unique_temp_path("recovery-rebuild-index");
    let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "before".to_string(),
                    value: b"before".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_segment().unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "after".to_string(),
                    value: b"after".to_vec(),
                },
            })
            .status
            .ok
    );

    fs::remove_file(page_dir.join("page_extent_manifest.json")).unwrap();
    let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
    recovered.load_shard(1);
    let report = recovered.storage_recovery_report(1);

    assert_eq!(report.oplog_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert_eq!(report.active_page_segment_ids, vec![0, 1]);
    assert_eq!(report.live_page_segment_ids, vec![0, 1]);
    assert_eq!(report.total_page_refs, 2);
    assert!(report.all_live_pages_readable);
    assert_eq!(report.zone_descriptors.len(), 2);
    assert_eq!(
        report.zone_descriptors[0].state,
        BlockStoreBandState::Sealed
    );
    assert_eq!(
        report.zone_descriptors[1].state,
        BlockStoreBandState::Active
    );
    assert_eq!(report.zone_summary.sealed_bands, 1);
    assert_eq!(report.zone_summary.active_bands, 1);
    assert!(report.zone_summary.live_physical_bytes > 0);
    assert!(page_dir.join("page_extent_manifest.json").exists());
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "before".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"before".to_vec())
        }
    );
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "after".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"after".to_vec())
        }
    );
}

#[test]
fn durable_writes_stamp_stable_object_ids_on_page_addresses() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                table_name: "table".to_string(),
                shard_uri: "local://1".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 20,
                readonly: false,
                load_version: 1,
                local_node_id: None,
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                    value: b"hv".to_vec(),
                },
            })
            .status
            .ok
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");
    let string_address = shard.strings.get("k").expect("string address");
    let hash_address = shard
        .hashes
        .get("h")
        .and_then(|fields| fields.get("f"))
        .expect("hash address");

    assert_eq!(
        string_address.object_id,
        Some(stable_page_object_id(1, "string", "k", None))
    );
    assert_eq!(
        string_address.routing_bucket,
        Some(page_routing_bucket("k", 10, 20))
    );
    assert_eq!(
        string_address.band_id,
        Some(string_address.page_segment_id)
    );
    assert_eq!(
        hash_address.object_id,
        Some(stable_page_object_id(1, "hash", "h", Some("f")))
    );
    assert_eq!(
        hash_address.routing_bucket,
        Some(page_routing_bucket("h", 10, 20))
    );
    assert_eq!(hash_address.band_id, Some(hash_address.page_segment_id));
    assert_ne!(string_address.object_id, hash_address.object_id);
}

#[test]
fn string_setex_sets_value_and_ttl() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                    ttl_ms: 60_000,
                },
            })
            .status
            .ok
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    let ttl = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonTtl {
            key: "k".to_string(),
        },
    });
    let CommandResponse::Integer { value } = ttl.response else {
        panic!("expected ttl integer response");
    };
    assert!(value > 0);
}

#[test]
fn expiry_sweep_removes_expired_records_without_lazy_read() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "expire-me".to_string(),
                    value: b"gone".to_vec(),
                    ttl_ms: 1,
                },
            })
            .status
            .ok
    );
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.sweep_expired_records(1).unwrap();
    assert_eq!(report.expired_records_removed, 1);
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "expire-me".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );
    assert_eq!(
        engine
            .sweep_expired_records(1)
            .unwrap()
            .expired_records_removed,
        0
    );
}

#[test]
fn string_get_uses_memory_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024, dir.path());
    let engine = TemporalEngine::new(cache.clone());
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    let stats = cache.stats();
    assert_eq!(stats.misses, 2);
    assert!(stats.memory_hits >= 1);
    assert!(stats.puts >= 2);
}

