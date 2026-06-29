use super::*;
use crate::block_store::BlockStoreExtentState;
use crate::engine::golden::{
    cpp_api_golden_corpus_report, cpp_feature_sequence_golden_corpus_report,
};
use crate::types::{
    ContextAuditRef, ContextChildRef, ContextCompressionEvent, ContextExtractedEventIndexes,
    ContextSummary, ContextWire, FeatureFilter, FeatureFilterOp, ReplicatedCommand,
};
use crate::{BlockAddress, BlockStoreOptions, LocalBlockStore};

fn wait_for_fresh_admission_second() {
    loop {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch");
        if elapsed.subsec_millis() < 100 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

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
            .slot_storage_summaries(1)
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
    for (child_ref, created, count) in [
        (child_gpu.clone(), true, 1),
        (
            ContextChildRef {
                parent_hash: ROOT,
                child_hash: COST,
                updated_at_ms: EVENT_TIME,
            },
            true,
            2,
        ),
        (child_gpu.clone(), false, 2),
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
                parent_child_count: Some(actual_count),
                ..
            } if object_key == "ctx:child:1001:10"
                && actual_created == created
                && actual_count == count
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
            routing_slot: None,
            extent_id: None,
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
            routing_slot: None,
            extent_id: None,
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
            routing_slot: None,
            extent_id: None,
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
                routing_slot: None,
                extent_id: None,
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
                routing_slot: None,
                extent_id: None,
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
            routing_slot: None,
            extent_id: None,
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
                routing_slot: None,
                extent_id: None,
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
            string_address.routing_slot,
            Some(page_routing_slot("k", 0, u32::MAX))
        );
        assert_eq!(
            hash_address.object_id,
            Some(stable_page_object_id(1, "hash", "h", Some("f")))
        );
        assert_eq!(
            hash_address.routing_slot,
            Some(page_routing_slot("h", 0, u32::MAX))
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
// shared-corpus: storage_dump_load_recovery storage_cache_refill;
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
    assert!(report.slot_layout_transition_count >= 1);
    assert!(report
        .slot_layout_states_after
        .iter()
        .any(|state| state.state == "object_page" && state.object_count >= 1));
    assert!(report
        .slot_layout_states_after
        .iter()
        .any(|state| state.state == "packed_timestamped_page" && state.object_count >= 1));
    assert!(report
        .slot_layout_states_after
        .iter()
        .any(|state| state.state == "tombstone" && state.object_count >= 1));
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
    assert_eq!(layout("ips").index_refs, 2);
    assert_eq!(layout("ips").unique_page_refs, 1);
    assert_eq!(layout("context_event").index_refs, 1);

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
    assert!(object_runtime.layout_transition_count >= 1);
    assert!(object_runtime.object_page_count >= 1);
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

    let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        max_dump_slots_per_round: 16,
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
            .dirty_slot_count
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
        let address = shard.strings.get_mut("owned").expect("string address");
        address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
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
            .strings
            .get("first")
            .and_then(|address| address.object_id)
            .expect("first object id");
        let second = shard.strings.get_mut("second").expect("second address");
        second.object_id = Some(first_object_id);
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
fn crash_recovery_report_covers_oplog_index_page_and_extent_manifest() {
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
        BlockStoreExtentState::Sealed
    );
    assert_eq!(
        report.zone_descriptors[1].state,
        BlockStoreExtentState::Active
    );
    assert_eq!(report.zone_summary.sealed_extents, 1);
    assert_eq!(report.zone_summary.active_extents, 1);
    assert_eq!(report.zone_summary.delayed_destroy_extents, 0);
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
        report.page_segment_live_reports[0].live_routing_slot_count,
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
    assert_eq!(segment.live_routing_slot_count, 1);
}

#[test]
// shared-corpus: storage_cache_refill storage_rustraft_cache_refill_pressure;
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
            address.routing_slot,
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
fn crash_recovery_rebuilds_missing_extent_manifest_from_page_stream() {
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
        BlockStoreExtentState::Sealed
    );
    assert_eq!(
        report.zone_descriptors[1].state,
        BlockStoreExtentState::Active
    );
    assert_eq!(report.zone_summary.sealed_extents, 1);
    assert_eq!(report.zone_summary.active_extents, 1);
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
                start_routing_slot: 10,
                end_routing_slot: 20,
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
        string_address.routing_slot,
        Some(page_routing_slot("k", 10, 20))
    );
    assert_eq!(
        string_address.extent_id,
        Some(string_address.page_segment_id)
    );
    assert_eq!(
        hash_address.object_id,
        Some(stable_page_object_id(1, "hash", "h", Some("f")))
    );
    assert_eq!(
        hash_address.routing_slot,
        Some(page_routing_slot("h", 10, 20))
    );
    assert_eq!(hash_address.extent_id, Some(hash_address.page_segment_id));
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

#[test]
fn memory_miss_reads_local_page_file_using_index_address() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(block_store.stats().writes, 1);

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);

    let second = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        second.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().memory_hits, 1);

    cache.clear_memory_for_test();
    let third = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        third.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().disk_hits, 1);
}

#[test]
fn three_layer_cache_reads_memory_then_block_cache_then_local_file() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().puts, 2);
    assert!(cache.stats().memory_bytes > 0);
    assert!(cache.stats().disk_bytes > 0);

    let memory = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        memory.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert!(cache.stats().memory_hits >= 1);
    assert_eq!(block_store.stats().reads, 1);

    cache.clear_memory_for_test();
    let block_cache = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        block_cache.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(cache.stats().disk_hits, 1);
    assert_eq!(block_store.stats().reads, 1);

    cache.invalidate_shard(1).unwrap();
    let local_file = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        local_file.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 2);
    assert!(cache.stats().puts >= 4);
    assert!(cache.stats().memory_bytes > 0);
    assert!(cache.stats().disk_bytes > 0);

    let observation = engine.rust_storage_observation(1).unwrap();
    assert!(observation.observed_memory_hit);
    assert!(observation.observed_block_cache_hit);
    assert!(observation.observed_local_file_read);
    assert!(observation.observed_cache_invalidation);
    assert!(observation.cache_memory_bytes > 0);
    assert!(observation.cache_disk_bytes > 0);
    assert!(observation.local_page_bytes_written > 0);
    assert!(observation.local_page_bytes_read > 0);
}

#[test]
fn tiny_memory_cache_eviction_refills_from_persistence_then_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);

    let target_value = b"target-value-0123456789".to_vec();
    for (key, value) in [
        ("target", target_value.clone()),
        ("evict-a", b"eviction-value-a-0123456789".to_vec()),
        ("evict-b", b"eviction-value-b-0123456789".to_vec()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    let first_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        first_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(block_store.stats().reads, 1);

    for key in ["evict-a", "evict-b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: key.to_string(),
            },
        });
        assert!(
            response.status.ok,
            "eviction pressure read should pass: {response:?}"
        );
    }
    assert!(
        cache.stats().memory_evictions > 0,
        "reading multiple persisted blocks through a tiny memory cache should evict older blocks"
    );
    assert!(
        cache.stats().disk_bytes > 0,
        "persistent page read should populate block-cache files"
    );

    let target_page_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let address = shards
            .get(&1)
            .expect("shard should exist")
            .strings
            .get("target")
            .expect("target address should exist");
        CacheKey::page_with_slot(
            1,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
        )
    };
    assert_eq!(
        cache.get_memory(&target_page_key),
        None,
        "target page block should have been evicted from memory"
    );

    let disk_hits_before = cache.stats().disk_hits;
    let file_reads_before_block_hit = block_store.stats().reads;
    let second_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        second_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        block_store.stats().reads,
        file_reads_before_block_hit,
        "memory miss should hit disk block cache instead of rereading block store"
    );
    assert!(
        cache.stats().disk_hits > disk_hits_before,
        "block cache should serve the read and promote it to memory"
    );
    assert_eq!(
        cache.get_memory(&target_page_key),
        Some(target_value),
        "disk block hit should promote the page block into memory"
    );
}

#[test]
fn restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let original =
        TemporalEngine::with_local_dirs(32, dir.path().join("cache-a"), &page_dir, &index_dir);
    original.load_shard(1);
    let target_value = b"restart-target-value-0123456789".to_vec();
    let write = original.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "target".to_string(),
            value: target_value.clone(),
        },
    });
    assert!(write.status.ok, "{write:?}");
    assert_eq!(original.block_store().stats().writes, 1);

    let restarted =
        TemporalEngine::with_local_dirs(32, dir.path().join("cache-b"), &page_dir, &index_dir);
    restarted.load_shard(1);
    let restarted_cache = restarted.cache();
    let restarted_block_store = restarted.block_store();
    let target_page_key = {
        let shards = restarted.shards.read().expect("shards lock poisoned");
        let address = shards
            .get(&1)
            .expect("shard should exist after index replay")
            .strings
            .get("target")
            .expect("target address should be restored from index")
            .clone();
        CacheKey::page_with_slot(
            1,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
        )
    };

    let first_read = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        first_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        restarted_block_store.stats().reads,
        1,
        "restart should miss memory and load the persisted page once"
    );
    assert_eq!(
        restarted_cache.get_memory(&target_page_key),
        Some(target_value.clone()),
        "persistent page read should refill the memory cache"
    );
    assert!(
        restarted_cache.stats().disk_bytes > 0,
        "persistent page read should also write the disk block cache"
    );

    restarted_cache.clear_memory_for_test();
    assert_eq!(restarted_cache.get_memory(&target_page_key), None);
    let disk_hits_before = restarted_cache.stats().disk_hits;
    let page_reads_before = restarted_block_store.stats().reads;
    let second_read = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        second_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        restarted_block_store.stats().reads,
        page_reads_before,
        "memory miss after restart should use the disk block cache"
    );
    assert!(
        restarted_cache.stats().disk_hits > disk_hits_before,
        "disk block cache should serve the second read"
    );
    assert_eq!(
        restarted_cache.get_memory(&target_page_key),
        Some(target_value),
        "disk block hit should promote the page block back into memory"
    );
}

#[test]
fn page_reads_fill_compressed_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::with_block_options(
        1024 * 1024,
        dir.path().join("cache"),
        crate::cache::CacheBlockOptions {
            compression: crate::cache::CacheCompression::Zstd { level: 1 },
            min_compress_bytes: 16,
        },
    );
    let engine = TemporalEngine::with_cache_block_store_and_index_dir(
        cache.clone(),
        LocalBlockStore::new(dir.path().join("pages")),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let value = vec![b'x'; 4096];
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "large".to_string(),
            value: value.clone(),
        },
    });

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes { value: Some(value) }
    );
    assert!(cache.stats().compressed_puts >= 1);
    assert!(cache.stats().compression_bytes_saved > 0);

    cache.clear_memory_for_test();
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large".to_string(),
        },
    });
    assert!(cache.stats().compressed_hits >= 1);
}

#[test]
fn local_dirs_constructor_applies_block_store_compression_options() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs_and_block_store_options(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
        BlockStoreOptions {
            compression_enabled: false,
            ..BlockStoreOptions::default()
        },
    );
    engine.load_shard(1);
    let value = b"engine-page-policy-".repeat(80);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "large-policy".to_string(),
            value: value.clone(),
        },
    });

    let block_store = engine.block_store();
    let stats = block_store.stats();
    assert_eq!(stats.writes, 1);
    assert_eq!(stats.compressed_records_written, 0);
    assert_eq!(stats.compression_bytes_saved, 0);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large-policy".to_string(),
        },
    });
    assert_eq!(read.response, CommandResponse::Bytes { value: Some(value) });
}

#[test]
fn write_invalidates_cached_string() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024, dir.path());
    let engine = TemporalEngine::new(cache.clone());
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"old".to_vec(),
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
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"new".to_vec(),
        },
    });
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"new".to_vec())
        }
    );
    assert!(cache.stats().invalidations >= 2);
}

#[test]
fn async_storage_string_write_stays_on_hot_memory_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "hot".to_string(),
            value: b"value".to_vec(),
        },
    });
    assert!(write.status.ok);
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 0);
    assert_eq!(engine.index_log_store().stats(1).writes, 0);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "hot".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"value".to_vec())
        }
    );
    assert_eq!(engine.block_store().stats().reads, 0);
    assert!(engine.cache().stats().memory_hits >= 1);
}

#[test]
fn durable_execute_overrides_async_storage_for_raft_local_file_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );

    let write = engine.execute_durable(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "raft".to_string(),
            value: b"value".to_vec(),
        },
    });
    assert!(write.status.ok);
    assert_eq!(engine.block_store().stats().writes, 1);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    assert_eq!(engine.index_log_store().stats(1).writes, 1);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "raft".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"value".to_vec())
        }
    );
}

#[test]
fn durable_index_survives_restart_and_points_to_page_file() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache-a");
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1024, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"persisted".to_vec(),
        },
    });

    let restarted =
        TemporalEngine::with_local_dirs(1024, dir.path().join("cache-b"), &page_dir, &index_dir);
    restarted.load_shard(1);
    let response = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"persisted".to_vec())
        }
    );
    assert_eq!(restarted.block_store().stats().reads, 1);
}

#[test]
fn hash_incrby_rejects_non_integer_and_overflow_like_cpp() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiSet {
            key: "h".to_string(),
            entries: vec![
                ("alpha".to_string(), b"abc".to_vec()),
                ("mixed".to_string(), b"123abc".to_vec()),
                ("max".to_string(), i64::MAX.to_string().into_bytes()),
                ("min".to_string(), i64::MIN.to_string().into_bytes()),
            ],
        },
    });

    for field in ["alpha", "mixed"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashIncrBy {
                key: "h".to_string(),
                field: field.to_string(),
                increment: 1,
            },
        });
        assert_eq!(response.status.code, "unmatched");
        assert_eq!(response.response, CommandResponse::Empty);
    }

    let overflow = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashIncrBy {
            key: "h".to_string(),
            field: "max".to_string(),
            increment: 1,
        },
    });
    assert_eq!(overflow.status.code, "out_of_range");
    let underflow = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashIncrBy {
            key: "h".to_string(),
            field: "min".to_string(),
            increment: -1,
        },
    });
    assert_eq!(underflow.status.code, "out_of_range");
}

#[test]
fn feature_append_packs_many_timestamp_values_into_one_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let first = SequenceFeatureRow {
        timestamp_ms: 10,
        gid: 1,
        action_type: 2,
        duration: 3,
        author_id: 4,
    };
    let second = SequenceFeatureRow {
        timestamp_ms: 20,
        gid: 5,
        action_type: 6,
        duration: 7,
        author_id: 8,
    };
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "packed-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: second.timestamp_ms,
                    value: second.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: first.timestamp_ms,
                    value: first.encode_cpp_feature_value(),
                },
            ],
        },
    });
    assert!(response.status.ok);

    let (first_address, second_address) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("packed-feature"))
            .expect("feature series should exist");
        (
            series.get(&10).expect("first point").clone(),
            series.get(&20).expect("second point").clone(),
        )
    };
    assert_eq!(first_address, second_address);
    assert_eq!(
        first_address.object_id,
        Some(stable_page_object_id(1, "feature", "packed-feature", None))
    );
    let packed_bytes = engine.block_store().read(&first_address).unwrap();
    let packed_points = decode_feature_page(&packed_bytes).expect("packed feature page");
    assert_eq!(packed_points.len(), 2);
    assert_eq!(packed_points[0].timestamp_ms, 10);
    assert_eq!(packed_points[1].timestamp_ms, 20);

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: first.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: second.encode_cpp_feature_value(),
                },
            ]
        }
    );

    let filtered = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: "packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            count: None,
            filters: vec![FeatureFilter {
                field: "gid".to_string(),
                op: FeatureFilterOp::Equal,
                value: 5,
            }],
        },
    });
    assert_eq!(
        filtered.response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 20,
                value: second.encode_cpp_feature_value(),
            }]
        }
    );

    let agg = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: "packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            aggregator: "count".to_string(),
            count: None,
        },
    });
    assert_eq!(agg.response, CommandResponse::Aggregate { value: 2 });
}

#[test]
fn feature_append_chunks_and_persists_timestamped_kv_pages() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let points = (0..10)
        .map(|offset| FeaturePoint {
            timestamp_ms: 1_000 + offset,
            value: vec![b'a' + offset as u8; 10 * 1024],
        })
        .collect::<Vec<_>>();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "chunked-feature".to_string(),
            points: points.clone(),
        },
    });
    assert!(response.status.ok);

    let addresses = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("chunked-feature"))
            .expect("feature series should exist");
        unique_timestamped_kv_page_addresses(series)
    };
    assert!(
        addresses.len() > 1,
        "large timestamped KV batch should be split into page chunks"
    );
    let mut persisted_timestamps = Vec::new();
    for address in &addresses {
        assert_eq!(
            address.object_id,
            Some(stable_page_object_id(1, "feature", "chunked-feature", None))
        );
        let bytes = engine.block_store().read(address).unwrap();
        let chunk = decode_feature_page(&bytes).expect("persisted packed page chunk");
        assert!(!chunk.is_empty());
        assert!(bytes.len() <= TIMESTAMPED_KV_PAGE_TARGET_BYTES + 12 * 1024);
        persisted_timestamps.extend(chunk.into_iter().map(|point| point.timestamp_ms));
    }
    persisted_timestamps.sort_unstable();
    assert_eq!(
        persisted_timestamps,
        points
            .iter()
            .map(|point| point.timestamp_ms)
            .collect::<Vec<_>>()
    );

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "chunked-feature".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: None,
        },
    });
    assert_eq!(query.response, CommandResponse::FeaturePoints { points });
}

#[test]
fn feature_append_keeps_oversized_single_timestamped_value_readable() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let points = vec![FeaturePoint {
        timestamp_ms: 1_000,
        value: vec![b'x'; TIMESTAMPED_KV_PAGE_TARGET_BYTES + 8 * 1024],
    }];
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "oversized-single-feature".to_string(),
            points: points.clone(),
        },
    });
    assert!(response.status.ok);

    let addresses = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("oversized-single-feature"))
            .expect("feature series should exist");
        unique_timestamped_kv_page_addresses(series)
    };
    assert_eq!(addresses.len(), 1);
    let bytes = engine.block_store().read(&addresses[0]).unwrap();
    assert!(bytes.len() > TIMESTAMPED_KV_PAGE_TARGET_BYTES);
    assert_eq!(decode_feature_page(&bytes).unwrap(), points);

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "oversized-single-feature".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: None,
        },
    });
    assert_eq!(query.response, CommandResponse::FeaturePoints { points });
    assert!(
        engine
            .storage_production_readiness_report(1)
            .production_ready
    );
}

#[test]
fn feature_recovery_validates_packed_page_layout() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "layout-feature".to_string(),
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
    });
    assert!(response.status.ok);

    let report = engine.storage_recovery_report(1);
    assert_eq!(report.feature_page_layout.indexed_feature_points, 2);
    assert_eq!(report.feature_page_layout.unique_feature_page_refs, 1);
    assert_eq!(report.feature_page_layout.packed_feature_pages, 1);
    assert_eq!(report.feature_page_layout.legacy_feature_value_pages, 0);
    assert!(report
        .feature_page_layout
        .corrupt_packed_feature_pages
        .is_empty());
    assert!(report
        .feature_page_layout
        .missing_indexed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .orphan_packed_timestamps
        .is_empty());
}

#[test]
fn feature_recovery_reports_index_timestamp_missing_from_packed_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "layout-feature".to_string(),
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
    });
    assert!(response.status.ok);

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let series = shards
            .get_mut(&1)
            .and_then(|shard| shard.features.get_mut("layout-feature"))
            .expect("feature series should exist");
        let address = series.get(&10).expect("packed page").clone();
        series.insert(30, address);
    }

    let report = engine.storage_recovery_report(1);
    assert_eq!(
        report
            .feature_page_layout
            .missing_indexed_timestamps
            .iter()
            .map(|mismatch| mismatch.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![30]
    );
    let readiness = engine.storage_production_readiness_report(1);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
}

#[test]
fn feature_recovery_reports_packed_timestamp_orphaned_from_index() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "layout-feature".to_string(),
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
    });
    assert!(response.status.ok);

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let series = shards
            .get_mut(&1)
            .and_then(|shard| shard.features.get_mut("layout-feature"))
            .expect("feature series should exist");
        series.remove(&20);
    }

    let report = engine.storage_recovery_report(1);
    assert_eq!(
        report
            .feature_page_layout
            .orphan_packed_timestamps
            .iter()
            .map(|mismatch| mismatch.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![20]
    );
    let readiness = engine.storage_production_readiness_report(1);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
}

#[test]
fn feature_recovery_reports_duplicate_timestamps_inside_packed_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let duplicate_page = encode_feature_page(&[
        FeaturePoint {
            timestamp_ms: 10,
            value: b"ten".to_vec(),
        },
        FeaturePoint {
            timestamp_ms: 10,
            value: b"ten-duplicate".to_vec(),
        },
        FeaturePoint {
            timestamp_ms: 20,
            value: b"twenty".to_vec(),
        },
    ]);
    let address = engine
        .block_store()
        .append_with_page_metadata(
            &duplicate_page,
            Some(stable_page_object_id(1, "feature", "layout-feature", None)),
            Some(page_routing_slot("layout-feature", 0, u32::MAX)),
        )
        .expect("duplicate packed page append");

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        let series = shard
            .features
            .entry("layout-feature".to_string())
            .or_default();
        series.insert(10, address.clone());
        series.insert(20, address);
    }

    let report = engine.storage_recovery_report(1);
    assert_eq!(
        report
            .feature_page_layout
            .duplicate_packed_timestamps
            .iter()
            .map(|mismatch| mismatch.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![10]
    );
    assert!(report
        .feature_page_layout
        .missing_indexed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .orphan_packed_timestamps
        .is_empty());
    let readiness = engine.storage_production_readiness_report(1);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
}

#[test]
fn feature_recovery_reports_corrupt_packed_timestamped_page() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut corrupt_page = FEATURE_PAGE_MAGIC.to_vec();
    corrupt_page.extend_from_slice(br#"{"version":1,"points":"not-a-point-list"}"#);
    let address = engine
        .block_store()
        .append_with_page_metadata(
            &corrupt_page,
            Some(stable_page_object_id(1, "feature", "corrupt-feature", None)),
            Some(page_routing_slot("corrupt-feature", 0, u32::MAX)),
        )
        .expect("corrupt packed page append");

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        shard
            .features
            .entry("corrupt-feature".to_string())
            .or_default()
            .insert(10, address);
    }

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "corrupt-feature".to_string(),
            start_ms: 0,
            end_ms: 20,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints { points: vec![] }
    );

    let readiness = engine.storage_production_readiness_report(1);
    assert!(!readiness.production_ready);
    assert!(readiness
        .blockers
        .contains(&"feature_page_layout_mismatch".to_string()));
    assert_eq!(readiness.corrupt_feature_page_count, 1);
    assert!(
        readiness.feature_page_layout.corrupt_packed_feature_pages[0]
            .error
            .contains("invalid packed feature page payload")
    );
}

#[test]
fn feature_recovery_reports_unsupported_packed_timestamped_page_version() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let page = PackedFeaturePage {
        version: 2,
        points: vec![FeaturePoint {
            timestamp_ms: 10,
            value: b"ten".to_vec(),
        }],
    };
    let mut bytes = FEATURE_PAGE_MAGIC.to_vec();
    bytes.extend_from_slice(&serde_json::to_vec(&page).unwrap());
    let address = engine
        .block_store()
        .append_with_page_metadata(
            &bytes,
            Some(stable_page_object_id(
                1,
                "feature",
                "versioned-feature",
                None,
            )),
            Some(page_routing_slot("versioned-feature", 0, u32::MAX)),
        )
        .expect("unsupported packed page append");

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        shard
            .features
            .entry("versioned-feature".to_string())
            .or_default()
            .insert(10, address);
    }

    let readiness = engine.storage_production_readiness_report(1);
    assert!(!readiness.production_ready);
    assert_eq!(readiness.corrupt_feature_page_count, 1);
    assert!(
        readiness.feature_page_layout.corrupt_packed_feature_pages[0]
            .error
            .contains("unsupported packed feature page version 2")
    );
}

#[test]
fn feature_compaction_rewrites_shared_packed_page_once() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "compact-packed-feature".to_string(),
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
    });
    assert!(response.status.ok);

    let before = engine.storage_recovery_report(1);
    assert_eq!(before.total_page_refs, 1);
    let report = engine.compact_shard_pages(1).unwrap();
    assert_eq!(report.rewritten_page_refs, 1);
    assert_eq!(report.after.live_page_refs, 1);

    let (first_address, second_address) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let series = shards
            .get(&1)
            .and_then(|shard| shard.features.get("compact-packed-feature"))
            .expect("feature series should exist");
        (
            series.get(&10).expect("first point").clone(),
            series.get(&20).expect("second point").clone(),
        )
    };
    assert_eq!(first_address, second_address);

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "compact-packed-feature".to_string(),
            start_ms: 0,
            end_ms: 30,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ]
        }
    );
    let after = engine.storage_recovery_report(1);
    assert_eq!(after.total_page_refs, 1);
    assert_eq!(after.object_lifecycle.live_page_refs, 1);
    assert_eq!(after.object_lifecycle.reused_object_id_conflicts, 0);
}

#[test]
fn feature_append_rejects_cpp_hard_size_limit_before_mutation() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "huge-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"kept".to_vec(),
            }],
        },
    });

    let oversized_points = (0..FEATURE_ADD_HARD_MAX_SIZE)
        .map(|offset| FeaturePoint {
            timestamp_ms: 10 + offset as u64,
            value: b"x".to_vec(),
        })
        .collect::<Vec<_>>();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "huge-feature".to_string(),
            points: oversized_points,
        },
    });
    assert_eq!(response.status.ok, false);
    assert_eq!(response.status.code, "invalid_argument");
    assert!(response
        .status
        .message
        .contains("huge-feature size bigger than 100000"));

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "huge-feature".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: Some(10),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"kept".to_vec(),
            }]
        }
    );
}

#[test]
fn cpp_feature_sequence_golden_corpus_passes() {
    let report = cpp_feature_sequence_golden_corpus_report();
    assert_eq!(report.corpus, "feature_sequence_cpp_proto_v1");
    assert_eq!(report.total_cases, 8);
    assert_eq!(report.passed_cases, report.total_cases);
    assert_eq!(report.failed_cases, 0);
    assert!(report.passed(), "{report:#?}");
}

#[test]
fn cpp_api_golden_corpus_passes() {
    let report = cpp_api_golden_corpus_report();
    assert_eq!(report.corpus, "cpp_api_golden_corpus_v1");
    assert_eq!(report.total_cases, 16);
    assert!(report.passed(), "{report:#?}");
    assert_eq!(report.passed_cases, report.total_cases);
    assert_eq!(report.failed_cases, 0);
}

#[test]
fn feature_replace_delete_and_agg_query() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"3".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 30,
                    value: b"4".to_vec(),
                },
            ],
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "sum".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 9 }
    );
    for (aggregator, count, expected) in [
        ("avg", None, 3),
        ("first", None, 2),
        ("last", None, 4),
        ("events", None, 3),
        ("last", Some(2), 3),
    ] {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: aggregator.to_string(),
                        count,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: expected },
            "{aggregator} aggregate should match C++ window semantics"
        );
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 100,
                    end_ms: 200,
                    aggregator: "avg".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 0 }
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureReplace {
            key: "f".to_string(),
            start_ms: 0,
            end_ms: 20,
            points: vec![FeaturePoint {
                timestamp_ms: 15,
                value: b"10".to_vec(),
            }],
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "sum".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 14 }
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureDelete {
            key: "f".to_string(),
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAggQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "count".to_string(),
                    count: None,
                },
            })
            .response,
        CommandResponse::Aggregate { value: 0 }
    );
}

#[test]
fn common_delete_removes_all_data_types_for_key() {
    let engine = TemporalEngine::default();
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
        command: Command::SetAdd {
            key: "k".to_string(),
            member: b"m".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonDelete {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SetMembers {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Members {
            members: Vec::new()
        }
    );
}

#[test]
fn common_delete_removes_cpp_risk_family_records_for_logical_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (family, amount) in [
        (RiskFamily::H, 5),
        (RiskFamily::Cpc, 7),
        (RiskFamily::Fol, 11),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskSet {
                family,
                key: "risk-cpp".to_string(),
                timestamp_ms: 10,
                amount,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExists {
                    key: "risk-cpp".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 1 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: "risk-cpp".to_string(),
                },
            })
            .response,
        CommandResponse::Empty
    );
    for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family,
                        key: "risk-cpp".to_string(),
                        start_ms: 0,
                        end_ms: 20,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
    }
}

#[test]
fn common_expire_and_ttl_work() {
    let engine = TemporalEngine::default();
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
        command: Command::CommonExpire {
            key: "k".to_string(),
            ttl_ms: 0,
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Integer { value: -2 }
    );
}

#[test]
fn common_expire_missing_key_matches_cpp_not_found() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "missing".to_string(),
            ttl_ms: 1000,
        },
    });
    assert_eq!(response.status.code, "not_found");
}

#[test]
fn common_expire_and_ttl_cover_cpp_risk_family_records_for_logical_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskSet {
            family: RiskFamily::Cpc,
            key: "risk-expire".to_string(),
            timestamp_ms: 10,
            amount: 3,
        },
    });
    assert!(response.status.ok, "{response:?}");

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "risk-expire".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: -1 }
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: "risk-expire".to_string(),
                    ttl_ms: 0,
                },
            })
            .status
            .ok
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "risk-expire".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: -2 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskFamilyQuery {
                    family: RiskFamily::Cpc,
                    key: "risk-expire".to_string(),
                    start_ms: 0,
                    end_ms: 20,
                    aggregator: "sum".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 0 }
    );
}

#[test]
fn long_sequence_query_keeps_timestamp_order_and_applies_random_filters() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let base_ts = 1_700_000_000_000_u64;
    let row_count = 5_000_u64;
    let key = "long-sequence".to_string();

    let ordered_rows = (0..row_count)
        .map(|offset| SequenceFeatureRow {
            timestamp_ms: base_ts + offset,
            gid: 10_000 + offset,
            action_type: (offset % 7) as u32,
            duration: (50 + (offset * 37) % 1_000) as u32,
            author_id: 500 + (offset * 17) % 97,
        })
        .collect::<Vec<_>>();
    let shuffled_rows = (0..row_count)
        .map(|i| ordered_rows[((i * 2_919) % row_count) as usize].clone())
        .collect::<Vec<_>>();

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: key.clone(),
            rows: shuffled_rows,
        },
    });

    for seed in 0..20_u64 {
        let start_offset = (seed * 313) % 4_400;
        let end_offset = (start_offset + 250 + (seed * 97) % 700).min(row_count - 1);
        let count = 25 + (seed as usize % 40);
        let filters = vec![
            FeatureFilter {
                field: "action_type".to_string(),
                op: FeatureFilterOp::NotEqual,
                value: seed % 7,
            },
            FeatureFilter {
                field: "duration".to_string(),
                op: FeatureFilterOp::GreaterOrEqual,
                value: 100 + (seed * 29) % 500,
            },
            FeatureFilter {
                field: "author_id".to_string(),
                op: FeatureFilterOp::LessOrEqual,
                value: 560 + (seed * 11) % 30,
            },
        ];

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceQuery {
                key: key.clone(),
                start_ms: base_ts + start_offset,
                end_ms: base_ts + end_offset,
                count,
                filters: filters.clone(),
            },
        });
        let CommandResponse::SequenceRows { rows } = response.response else {
            panic!("expected sequence rows");
        };
        let expected = ordered_rows
            .iter()
            .filter(|row| row.timestamp_ms >= base_ts + start_offset)
            .filter(|row| row.timestamp_ms <= base_ts + end_offset)
            .take(count)
            .filter(|row| {
                filters
                    .iter()
                    .all(|filter| sequence_filter_matches(row, filter))
            })
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(rows, expected, "seed {seed}");
        assert!(rows
            .windows(2)
            .all(|pair| pair[0].timestamp_ms < pair[1].timestamp_ms));
        assert!(rows.len() <= count);
    }
}

#[test]
fn ips_query_last_returns_recent_instances() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, value) in [(1, b"a".to_vec()), (2, b"b".to_vec()), (3, b"c".to_vec())] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAdd {
                key: "ips".to_string(),
                timestamp_ms,
                instance: value,
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsQueryLast {
                    key: "ips".to_string(),
                    count: 2,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 3,
                    value: b"c".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 2,
                    value: b"b".to_vec(),
                }
            ]
        }
    );
}

#[test]
fn risk_count_sums_window() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, amount) in [(10, 1), (20, 2), (30, 4)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskIncrement {
                key: "risk".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskCount {
                    key: "risk".to_string(),
                    start_ms: 15,
                    end_ms: 30,
                },
            })
            .response,
        CommandResponse::Integer { value: 6 }
    );
}

// shared-corpus: engine_lifecycle_load_config_membership_unload
#[test]
fn control_api_load_config_info_stats_membership_and_unload() {
    let engine = TemporalEngine::default();
    assert_eq!(
        engine.set_config(SetConfigRequest {
            shard_id: 7,
            config: Config {
                version: 2,
                feature_max_size: 123,
                ..Config::default()
            },
        }),
        Status::error("shard_not_found", "shard is not loaded")
    );
    assert_eq!(engine.get_config(7).status.code, "shard_not_found");
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 7,
                load_version: 42,
                local_node_id: Some(2),
                shard_uri: "file:///tmp/shard-7".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 20,
                readonly: false,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );
    let duplicate_load = engine.load_shard_with(LoadShardRequest {
        shard_id: 7,
        load_version: 43,
        local_node_id: Some(2),
        shard_uri: "file:///tmp/shard-7-duplicate".to_string(),
        start_routing_slot: 10,
        end_routing_slot: 20,
        readonly: false,
        table_name: "table".to_string(),
    });
    assert!(!duplicate_load.status.ok);
    assert_eq!(duplicate_load.status.code, "already_exists");
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 2,
                    feature_max_size: 123,
                    maxmemory_bytes: Some(3000),
                    extend_config: BTreeMap::from([(
                        "test_config".to_string(),
                        "test_value".to_string(),
                    )]),
                    ..Config::default()
                },
            })
            .ok
    );
    let config = engine.get_config(7).config;
    assert_eq!(config.feature_max_size, 123);
    assert_eq!(config.maxmemory_bytes, Some(3000));
    assert_eq!(
        config.extend_config.get("test_config"),
        Some(&"test_value".to_string())
    );
    assert_eq!(
        engine.set_config(SetConfigRequest {
            shard_id: 7,
            config: Config {
                version: 1,
                feature_max_size: 456,
                ..Config::default()
            },
        }),
        Status::error("failed_precondition", "legacy config version")
    );
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 2,
                    feature_max_size: 456,
                    ..Config::default()
                },
            })
            .ok
    );
    assert_eq!(engine.get_config(7).config.feature_max_size, 123);
    assert!(
        engine
            .update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 3,
                replica_membership_version: 4,
                replica_node_ids: vec![1, 2, 3],
                leader_node_id: Some(1),
            })
            .ok
    );
    let info = engine.get_info(7).info.unwrap();
    assert_eq!(info.load_version, 42);
    assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
    assert_eq!(info.membership_version, 3);
    assert_eq!(info.replica_membership_version, 4);
    assert!(info.membership_valid);
    assert_eq!(
        engine.update_membership(MembershipUpdateRequest {
            shard_id: 7,
            membership_version: 2,
            replica_membership_version: 5,
            replica_node_ids: vec![1, 3],
            leader_node_id: Some(1),
        }),
        Status::error("failed_precondition", "legacy membership info")
    );
    assert_eq!(
        engine.update_membership(MembershipUpdateRequest {
            shard_id: 7,
            membership_version: 3,
            replica_membership_version: 3,
            replica_node_ids: vec![1, 3],
            leader_node_id: Some(1),
        }),
        Status::error("failed_precondition", "legacy membership unit info")
    );
    assert!(
        engine
            .update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 4,
                replica_membership_version: 5,
                replica_node_ids: vec![1, 3],
                leader_node_id: Some(1),
            })
            .ok
    );
    let info = engine.get_info(7).info.unwrap();
    assert_eq!(info.replica_node_ids, vec![1, 3]);
    assert!(!info.membership_valid);

    engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let stats = engine.get_stats(7).stats.unwrap();
    assert_eq!(stats.string_records, 1);
    assert_eq!(stats.total_records, 1);
    assert_eq!(stats.load_version, 42);
    assert!(!stats.readonly);
    assert!(stats.storage_bytes > 0);
    assert_eq!(stats.block_store.writes, 1);

    assert!(
        engine
            .unload_shard_with(UnloadShardRequest { shard_id: 7 })
            .status
            .ok
    );
    let after_unload = engine.get_info(7);
    assert!(!after_unload.status.ok);
    assert_eq!(after_unload.status.code, "shard_not_found");
    assert_eq!(engine.get_config(7).status.code, "shard_not_found");
    let second_unload = engine.unload_shard_with(UnloadShardRequest { shard_id: 7 });
    assert!(!second_unload.status.ok);
    assert_eq!(second_unload.status.code, "shard_not_found");
}

// shared-corpus: engine_lifecycle_reload_metadata_readonly_stale_version
#[test]
fn engine_reload_shard_updates_metadata_and_rejects_stale_version() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 7,
                load_version: 42,
                local_node_id: Some(2),
                shard_uri: "file:///tmp/shard-7".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 20,
                readonly: false,
                table_name: "old_table".to_string(),
            })
            .status
            .ok
    );
    assert!(
        engine
            .update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 3,
                replica_membership_version: 4,
                replica_node_ids: vec![1, 2, 3],
                leader_node_id: Some(1),
            })
            .ok
    );

    let stale = engine.reload_shard_with(LoadShardRequest {
        shard_id: 7,
        load_version: 41,
        local_node_id: Some(9),
        shard_uri: "file:///tmp/stale".to_string(),
        start_routing_slot: 100,
        end_routing_slot: 200,
        readonly: true,
        table_name: "stale_table".to_string(),
    });
    assert!(!stale.status.ok);
    assert_eq!(stale.status.code, "stale_load_version");
    let unchanged = engine.get_info(7).info.unwrap();
    assert_eq!(unchanged.load_version, 42);
    assert_eq!(unchanged.table_name, "old_table");
    assert!(!unchanged.readonly);

    let reload = engine.reload_shard_with(LoadShardRequest {
        shard_id: 7,
        load_version: 43,
        local_node_id: Some(9),
        shard_uri: "file:///tmp/shard-7-reloaded".to_string(),
        start_routing_slot: 100,
        end_routing_slot: 200,
        readonly: true,
        table_name: "new_table".to_string(),
    });
    assert!(reload.status.ok, "{reload:?}");
    let info = engine.get_info(7).info.unwrap();
    assert_eq!(info.load_version, 43);
    assert_eq!(info.local_node_id, Some(9));
    assert_eq!(info.table_name, "new_table");
    assert_eq!(info.start_routing_slot, 100);
    assert_eq!(info.end_routing_slot, 200);
    assert!(info.readonly);
    assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
    assert_eq!(info.membership_version, 3);
    assert_eq!(info.replica_membership_version, 4);
    assert!(info.membership_valid);

    let write = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(write.status.code, "readonly_shard");
}

#[test]
fn control_api_reads_page_and_index_streams() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"stream-value".to_vec(),
        },
    });

    let page = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        page_segment_id: 0,
        offset: 0,
        size: 12,
    });
    assert_eq!(page.data, b"stream-value".to_vec());

    let index = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Index,
        page_segment_id: 0,
        offset: 0,
        size: 32,
    });
    assert!(index.status.ok);
    assert!(!index.data.is_empty());

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        page_segment_id: 0,
        start_offset: 0,
        end_offset: 12,
        max_bytes: 12,
    });
    assert_eq!(scan.records.len(), 1);
    assert_eq!(scan.records[0].data, b"stream-value".to_vec());

    let invalid = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        page_segment_id: 0,
        start_offset: 12,
        end_offset: 1,
        max_bytes: 12,
    });
    assert_eq!(invalid.status.code, "invalid_stream_range");
    assert!(invalid.records.is_empty());
}

#[test]
fn control_api_reads_and_scans_wal_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k2".to_string(),
            value: b"v2".to_vec(),
        },
    });

    let stream = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        page_segment_id: 0,
        offset: 0,
        size: 4096,
    });
    assert!(stream.status.ok);
    let text = String::from_utf8(stream.data).unwrap();
    assert!(text.contains("\"sequence\":1"));
    assert!(text.contains("\"sequence\":2"));

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        page_segment_id: 0,
        start_offset: 0,
        end_offset: 4096,
        max_bytes: 4096,
    });
    assert_eq!(scan.records.len(), 2);
    assert_eq!(
        engine
            .get_stats(1)
            .stats
            .unwrap()
            .write_ahead_log
            .last_sequence,
        2
    );
}

#[test]
fn control_api_reads_and_scans_index_log_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"hv".to_vec(),
        },
    });

    let stream = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::IndexLog,
        page_segment_id: 0,
        offset: 0,
        size: 8192,
    });
    assert!(stream.status.ok);
    let text = String::from_utf8(stream.data).unwrap();
    assert!(text.contains("\"sequence\":1"));
    assert!(text.contains("\"sequence\":2"));
    assert!(text.contains("\"strings\""));
    assert!(text.contains("\"hashes\""));

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::IndexLog,
        page_segment_id: 0,
        start_offset: 0,
        end_offset: 8192,
        max_bytes: 8192,
    });
    assert_eq!(scan.records.len(), 2);

    let last_record: crate::index_log::IndexLogRecord =
        serde_json::from_slice(&scan.records[1].data).unwrap();
    assert_eq!(last_record.sequence, 2);
    assert_eq!(
        last_record.index["hashes"]["h"]["f"]["page_segment_id"],
        serde_json::json!(0)
    );
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);
}

#[test]
fn readonly_shard_rejects_writes_but_allows_reads() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 1,
                local_node_id: None,
                shard_uri: "file:///tmp/readonly".to_string(),
                start_routing_slot: 0,
                end_routing_slot: 99,
                readonly: true,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(!write.status.ok);
    assert_eq!(write.status.code, "readonly_shard");

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(read.status.ok);
    assert_eq!(read.response, CommandResponse::Bytes { value: None });
}

#[test]
fn checked_execute_rejects_stale_load_version() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 7,
                local_node_id: None,
                shard_uri: "file:///tmp/versioned".to_string(),
                start_routing_slot: 0,
                end_routing_slot: 99,
                readonly: false,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );

    let stale = engine.execute_checked(CheckedExecuteRequest {
        shard_id: 1,
        load_version: 6,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(stale.status.code, "load_version_mismatch");

    let current = engine.execute_checked(CheckedExecuteRequest {
        shard_id: 1,
        load_version: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(current.status.ok);
}

#[test]
fn loaded_shard_stats_reports_per_shard_load() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.load_shard(2);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "a".to_string(),
            value: b"1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"2".to_vec(),
        },
    });

    let stats = engine.loaded_shard_stats();
    assert_eq!(stats.len(), 2);
    assert!(stats
        .iter()
        .any(|stat| stat.shard_id == 1 && stat.string_records == 1));
    assert!(stats
        .iter()
        .any(|stat| stat.shard_id == 2 && stat.hash_records == 1));
}

#[test]
fn string_set_conditional_supports_nx_xx_and_get() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v1".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
        },
    });
    assert_eq!(first.response, CommandResponse::Integer { value: 1 });

    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v2".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
        },
    });
    assert_eq!(rejected.response, CommandResponse::Integer { value: 0 });

    let old = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v3".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfExists,
            return_old: true,
        },
    });
    assert_eq!(
        old.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v3".to_vec())
        }
    );
}

#[test]
fn ips_remove_delete_and_count_are_supported() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for timestamp_ms in [10, 20, 30] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAdd {
                key: "ips".to_string(),
                timestamp_ms,
                instance: timestamp_ms.to_string().into_bytes(),
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsCount {
                    key: "ips".to_string(),
                    start_ms: 0,
                    end_ms: 25,
                },
            })
            .response,
        CommandResponse::Integer { value: 2 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsRemove {
                    key: "ips".to_string(),
                    timestamp_ms: 20,
                },
            })
            .response,
        CommandResponse::Integer { value: 1 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsDelete {
                    key: "ips".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 1 }
    );
}

#[test]
fn ips_pages_store_timestamp_keys_with_values() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsLoad {
                    key: "packed-ips".to_string(),
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
            })
            .status
            .ok
    );

    let (first_address, second_address, meta_address) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let series = shard.ips.get("packed-ips").expect("IPS series");
        let meta = shard.ips_meta.get("packed-ips").expect("IPS metadata");
        (
            series.get(&10).expect("first IPS point").clone(),
            series.get(&20).expect("second IPS point").clone(),
            meta.get(&20).expect("second IPS metadata").address.clone(),
        )
    };
    assert_eq!(first_address, second_address);
    assert_eq!(second_address, meta_address);
    assert_eq!(
        first_address.object_id,
        Some(stable_page_object_id(1, "ips", "packed-ips", None))
    );

    let bytes = engine.block_store().read(&first_address).unwrap();
    let packed_points = decode_feature_page(&bytes).expect("packed IPS page");
    assert_eq!(
        packed_points,
        vec![
            FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            },
            FeaturePoint {
                timestamp_ms: 20,
                value: b"twenty".to_vec(),
            },
        ]
    );

    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::IpsQueryRange {
            key: "packed-ips".to_string(),
            start_ms: 0,
            end_ms: 30,
            count: None,
        },
    });
    assert_eq!(
        query.response,
        CommandResponse::FeaturePoints {
            points: packed_points
        }
    );
}

#[test]
fn recovery_validates_all_timestamped_kv_page_families() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let feature_points = (0..8)
        .map(|idx| FeaturePoint {
            timestamp_ms: 1_000 + idx,
            value: vec![b'f'; 10 * 1024],
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: "all-family-feature".to_string(),
                    points: feature_points.clone(),
                },
            })
            .status
            .ok
    );

    let sequence_rows = (0..8)
        .map(|idx| SequenceFeatureRow {
            timestamp_ms: 2_000 + idx,
            gid: idx,
            action_type: 7,
            duration: 11,
            author_id: 13,
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceAdd {
                    key: "all-family-sequence".to_string(),
                    rows: sequence_rows.clone(),
                },
            })
            .status
            .ok
    );

    let ips_points = (0..8)
        .map(|idx| FeaturePoint {
            timestamp_ms: 3_000 + idx,
            value: vec![b'i'; 10 * 1024],
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsLoad {
                    key: "all-family-ips".to_string(),
                    points: ips_points.clone(),
                },
            })
            .status
            .ok
    );

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteEvent {
                    tenant_hash: 44,
                    node_hash: 55,
                    event: ContextEvent {
                        event_id_hash: 66,
                        event_time_ms: 4_000,
                        ingestion_time_ms: 4_000,
                        kind: 1,
                        event_type: 2,
                        actor_hash: 77,
                        status: 1,
                        valid_until_ms: 0,
                        confidence: 0.99,
                        importance: 0.75,
                        text: "context event".to_string(),
                        source_ref: "local://test".to_string(),
                        related_node_hashes: vec![55],
                        compact_attrs: vec![1, 2, 3],
                    },
                    first_write_only: false,
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteIndexRef {
                    tenant_hash: 44,
                    index_name: "actor".to_string(),
                    index_value_hash: 77,
                    scope_hash: 1,
                    event_time_ms: 4_000,
                    index_ref: ContextIndexRef {
                        primary_node_hash: 55,
                        primary_event_time_ms: 4_000,
                        event_id_hash: 66,
                    },
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWritePackAudit {
                    tenant_hash: 44,
                    audit: ContextPackAudit {
                        query_id: "q-all-family".to_string(),
                        session_hash: 88,
                        request_time_ms: 4_100,
                        query_hash: 99,
                        max_prompt_tokens: 128,
                        selected_tokens: 32,
                        selected_refs: vec![ContextAuditRef {
                            node_hash: 55,
                            event_time_ms: 4_000,
                            reason: "selected".to_string(),
                        }],
                        blocked_refs: Vec::new(),
                    },
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextMarkSummaryDirty {
                    tenant_hash: 44,
                    marker: ContextSummaryDirtyMarker {
                        node_hash: 55,
                        event_time_ms: 4_200,
                        reason: 9,
                        propagate_depth: 2,
                    },
                },
            })
            .status
            .ok
    );

    let report = engine.storage_recovery_report(1);
    assert_eq!(report.feature_page_layout.indexed_timestamped_points, 28);
    assert!(report.feature_page_layout.packed_timestamped_pages >= 10);
    assert!(
        report
            .feature_page_layout
            .unique_timestamped_page_refs
            .saturating_sub(report.feature_page_layout.packed_timestamped_pages)
            <= report.feature_page_layout.legacy_timestamped_value_pages
    );
    assert!(report
        .feature_page_layout
        .corrupt_packed_feature_pages
        .is_empty());
    assert!(report
        .feature_page_layout
        .missing_indexed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .orphan_packed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .duplicate_packed_timestamps
        .is_empty());

    let families = report
        .feature_page_layout
        .families
        .iter()
        .map(|family| (family.kind.as_str(), family))
        .collect::<BTreeMap<_, _>>();
    for kind in [
        "feature",
        "sequence",
        "ips",
        "context_event",
        "context_index",
        "context_audit",
        "context_dirty",
    ] {
        let family = families.get(kind).expect("timestamped family report");
        assert!(family.indexed_points > 0, "{kind}");
        assert!(family.packed_pages > 0, "{kind}");
        assert_eq!(family.corrupt_pages, 0, "{kind}");
        assert_eq!(family.mismatch_count, 0, "{kind}");
    }
    assert!(
        families
            .get("feature")
            .expect("feature family")
            .unique_page_refs
            > 1
    );
    assert!(families.get("ips").expect("ips family").unique_page_refs > 1);

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "all-family-feature".to_string(),
                    start_ms: 1_000,
                    end_ms: 1_010,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: feature_points
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: "all-family-sequence".to_string(),
                    start_ms: 2_000,
                    end_ms: 2_010,
                    count: 16,
                    filters: Vec::new(),
                },
            })
            .response,
        CommandResponse::SequenceRows {
            rows: sequence_rows
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsQueryRange {
                    key: "all-family-ips".to_string(),
                    start_ms: 3_000,
                    end_ms: 3_010,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints { points: ips_points }
    );
}

#[test]
fn ips_compaction_rewrites_shared_timestamped_page_once() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsLoad {
                    key: "compact-ips".to_string(),
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
            })
            .response,
        CommandResponse::Integer { value: 2 }
    );

    let report = engine.compact_shard_pages(1).unwrap();
    assert_eq!(report.rewritten_page_refs, 1);

    let (first_address, second_address, meta_address) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let series = shard.ips.get("compact-ips").expect("IPS series");
        let meta = shard.ips_meta.get("compact-ips").expect("IPS metadata");
        (
            series.get(&10).expect("first IPS point").clone(),
            series.get(&20).expect("second IPS point").clone(),
            meta.get(&20).expect("second IPS metadata").address.clone(),
        )
    };
    assert_eq!(first_address, second_address);
    assert_eq!(second_address, meta_address);
    let bytes = engine.block_store().read(&first_address).unwrap();
    assert_eq!(
        decode_feature_page(&bytes).expect("packed IPS page"),
        vec![
            FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            },
            FeaturePoint {
                timestamp_ms: 20,
                value: b"twenty".to_vec(),
            },
        ]
    );
}

#[test]
fn risk_change_matches_cpp_distinct_field_semantics() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, value) in [(10, "device-a"), (20, "device-a"), (30, "device-b")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskChangeAdd {
                key: "risk-change".to_string(),
                timestamp_ms,
                value: value.as_bytes().to_vec(),
                precision_ms: Some(10),
                ttl_ms: None,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskQuery {
                    key: "risk-change".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "change".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 2 }
    );

    for (timestamp_ms, value) in [(10, "buyer-1"), (20, "buyer-1"), (30, "buyer-2")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskChangeAdd {
                key: risk_family_key(RiskFamily::H, "risk-change"),
                timestamp_ms,
                value: value.as_bytes().to_vec(),
                precision_ms: None,
                ttl_ms: None,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskFamilyQuery {
                    family: RiskFamily::H,
                    key: "risk-change".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "change".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 2 }
    );
}

#[test]
fn risk_query_supports_first_last_and_detail_list() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, amount) in [(10, 5), (20, -2), (30, 7)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskIncrement {
                key: "risk".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    for (aggregator, expected) in [("first", 5), ("last", 7)] {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskQuery {
                        key: "risk".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: aggregator.to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: expected }
        );
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskDetail {
                    key: "risk".to_string(),
                    start_ms: 15,
                    end_ms: 40,
                    count: Some(2),
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"-2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 30,
                    value: b"7".to_vec(),
                },
            ]
        }
    );
}

#[test]
fn risk_fol_matches_cpp_first_last_string_semantics() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for (occur_time_ms, value) in [(20, "middle"), (10, "first"), (30, "last")] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFolSet {
                        key: "risk-fol-first".to_string(),
                        value: value.as_bytes().to_vec(),
                        occur_time_ms,
                        ttl_ms: 60_000,
                        fol_type: RiskFolType::First,
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFolSet {
                        key: "risk-fol-last".to_string(),
                        value: value.as_bytes().to_vec(),
                        occur_time_ms,
                        ttl_ms: 60_000,
                        fol_type: RiskFolType::Last,
                    },
                })
                .status
                .ok
        );
    }

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskFolQuery {
                    key: "risk-fol-first".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"first".to_vec()),
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskFolQuery {
                    key: "risk-fol-last".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"last".to_vec()),
        }
    );
}

#[test]
fn feature_write_policy_sequence_batch_ips_dimensions_and_risk_precision_work() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "feature-policy".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"old".to_vec(),
            }],
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppendWithPolicy {
                    key: "feature-policy".to_string(),
                    points: vec![FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ignored".to_vec(),
                    }],
                    policy: FeatureWritePolicy::InsertIfAbsent,
                },
            })
            .response,
        CommandResponse::Integer { value: 0 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppendWithPolicy {
                    key: "feature-policy".to_string(),
                    points: vec![FeaturePoint {
                        timestamp_ms: 10,
                        value: b"new".to_vec(),
                    }],
                    policy: FeatureWritePolicy::ReplaceExisting,
                },
            })
            .response,
        CommandResponse::Integer { value: 1 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "feature-policy".to_string(),
                    start_ms: 0,
                    end_ms: 20,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"new".to_vec(),
            }]
        }
    );

    for (key, gid, action_type) in [("seq-a", 1, 7), ("seq-b", 2, 8)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: key.to_string(),
                rows: vec![SequenceFeatureRow {
                    timestamp_ms: 100,
                    gid,
                    action_type,
                    duration: 5,
                    author_id: 9,
                }],
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceBatchQuery {
                    queries: vec![
                        SequenceQuerySpec {
                            key: "seq-a".to_string(),
                            start_ms: 0,
                            end_ms: 200,
                            count: 10,
                            filters: vec![FeatureFilter {
                                field: "action_type".to_string(),
                                op: FeatureFilterOp::Equal,
                                value: 7,
                            }],
                        },
                        SequenceQuerySpec {
                            key: "seq-b".to_string(),
                            start_ms: 0,
                            end_ms: 200,
                            count: 10,
                            filters: Vec::new(),
                        },
                    ],
                },
            })
            .response,
        CommandResponse::SequenceRowGroups {
            groups: vec![
                (
                    "seq-a".to_string(),
                    vec![SequenceFeatureRow {
                        timestamp_ms: 100,
                        gid: 1,
                        action_type: 7,
                        duration: 5,
                        author_id: 9,
                    }],
                ),
                (
                    "seq-b".to_string(),
                    vec![SequenceFeatureRow {
                        timestamp_ms: 100,
                        gid: 2,
                        action_type: 8,
                        duration: 5,
                        author_id: 9,
                    }],
                ),
            ],
        }
    );

    for (timestamp_ms, value, action_type, request_id) in [
        (10, b"a10".to_vec(), Some(1), Some("r1".to_string())),
        (20, b"a20".to_vec(), Some(2), Some("r2".to_string())),
        (30, b"a30".to_vec(), Some(1), Some("r3".to_string())),
    ] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAddWithOptions {
                key: "ips-dim".to_string(),
                timestamp_ms,
                instance: value,
                action_type,
                table_id: Some(99),
                request_id,
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAddWithOptions {
                    key: "ips-dim".to_string(),
                    timestamp_ms: 40,
                    instance: b"dup".to_vec(),
                    action_type: Some(1),
                    table_id: Some(99),
                    request_id: Some("r1".to_string()),
                },
            })
            .response,
        CommandResponse::Integer { value: 0 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsQueryRangeWithOptions {
                    key: "ips-dim".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    count: None,
                    action_type: Some(1),
                    table_id: Some(99),
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"a10".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 30,
                    value: b"a30".to_vec(),
                },
            ]
        }
    );

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskIncrementWithOptions {
            key: "risk-bucket".to_string(),
            timestamp_ms: 1_234,
            amount: 3,
            precision_ms: Some(1_000),
            ttl_ms: Some(60_000),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskIncrementWithOptions {
            key: "risk-bucket".to_string(),
            timestamp_ms: 1_999,
            amount: 4,
            precision_ms: Some(1_000),
            ttl_ms: None,
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskDetail {
                    key: "risk-bucket".to_string(),
                    start_ms: 0,
                    end_ms: 2_000,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 1_000,
                value: b"7".to_vec(),
            }]
        }
    );
    assert!(matches!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "risk-bucket".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value } if value > 0
    ));
}

#[test]
fn maxmemory_config_rejects_writes_when_storage_budget_is_exhausted() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            maxmemory_bytes: Some(0),
            ..Config::default()
        },
    });

    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "first".to_string(),
            value: b"y".to_vec(),
        },
    });
    assert_eq!(rejected.status.code, "storage_quota_exceeded");
}

#[test]
fn write_qps_config_rejects_writes_after_admission_limit() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            write_qps: Some(1),
            ..Config::default()
        },
    });
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "first".to_string(),
                    value: b"x".to_vec(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "second".to_string(),
            value: b"y".to_vec(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "write_qps limit exceeded");
}

#[test]
fn read_qps_config_rejects_reads_after_admission_limit() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "first".to_string(),
                    value: b"x".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            read_qps: Some(1),
            ..Config::default()
        },
    });
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "first".to_string(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "first".to_string(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "read_qps limit exceeded");
}

#[test]
fn table_write_qps_config_is_shared_across_loaded_table_shards() {
    let engine = TemporalEngine::default();
    for shard_id in [1, 2] {
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id,
                    load_version: 1,
                    local_node_id: Some(1),
                    shard_uri: format!("local://feature_table/{shard_id}"),
                    start_routing_slot: 0,
                    end_routing_slot: u32::MAX,
                    readonly: false,
                    table_name: "feature_table".to_string(),
                })
                .status
                .ok
        );
        engine.set_config(SetConfigRequest {
            shard_id,
            config: Config {
                version: 2,
                table_write_qps: Some(1),
                ..Config::default()
            },
        });
    }
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "first".to_string(),
                    value: b"x".to_vec(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringSet {
            key: "second".to_string(),
            value: b"y".to_vec(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "table_write_qps limit exceeded");
}

#[test]
fn tenant_read_qps_config_is_shared_across_tables() {
    let engine = TemporalEngine::default();
    for (shard_id, table_name, key) in [(1, "feature_table", "k1"), (2, "risk_table", "k2")] {
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id,
                    load_version: 1,
                    local_node_id: Some(1),
                    shard_uri: format!("local://{table_name}/{shard_id}"),
                    start_routing_slot: 0,
                    end_routing_slot: u32::MAX,
                    readonly: false,
                    table_name: table_name.to_string(),
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"value".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.set_config(SetConfigRequest {
            shard_id,
            config: Config {
                version: 2,
                tenant_name: Some("tenant-a".to_string()),
                tenant_read_qps: Some(1),
                ..Config::default()
            },
        });
    }
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k1".to_string(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringGet {
            key: "k2".to_string(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "tenant_read_qps limit exceeded");
}

#[test]
fn stats_include_cpp_style_partition_and_object_manager_accounting() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 9,
                load_version: 77,
                local_node_id: Some(3),
                shard_uri: "local://table/shard-9".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 20,
                readonly: false,
                table_name: "feature_table".to_string(),
            })
            .status
            .ok
    );
    for command in [
        Command::StringSet {
            key: "string-key".to_string(),
            value: b"v".to_vec(),
        },
        Command::HashSet {
            key: "hash-key".to_string(),
            field: "a".to_string(),
            value: b"1".to_vec(),
        },
        Command::HashSet {
            key: "hash-key".to_string(),
            field: "b".to_string(),
            value: b"2".to_vec(),
        },
        Command::SetAdd {
            key: "set-key".to_string(),
            member: b"m1".to_vec(),
        },
        Command::SetAdd {
            key: "set-key".to_string(),
            member: b"m2".to_vec(),
        },
        Command::FeatureAppend {
            key: "feature-key".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 1,
                    value: b"f1".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 2,
                    value: b"f2".to_vec(),
                },
            ],
        },
        Command::SequenceAdd {
            key: "sequence-key".to_string(),
            rows: vec![
                SequenceFeatureRow {
                    timestamp_ms: 10,
                    gid: 1,
                    action_type: 2,
                    duration: 3,
                    author_id: 4,
                },
                SequenceFeatureRow {
                    timestamp_ms: 20,
                    gid: 5,
                    action_type: 6,
                    duration: 7,
                    author_id: 8,
                },
            ],
        },
        Command::IpsAdd {
            key: "ips-key".to_string(),
            timestamp_ms: 30,
            instance: b"i".to_vec(),
        },
        Command::RiskSet {
            family: RiskFamily::Cpc,
            key: "risk-key".to_string(),
            timestamp_ms: 40,
            amount: 5,
        },
    ] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 9,
                    command,
                })
                .status
                .ok
        );
    }

    let stats = engine.get_stats(9).stats.unwrap();
    assert_eq!(stats.total_records, 7);
    assert_eq!(stats.object_manager.object_count, 7);
    assert_eq!(stats.object_manager.page_ref_count, 10);
    assert_eq!(stats.object_manager.dirty_object_count, 7);
    assert!(stats.object_manager.dirty_slot_count > 0);
    assert!(stats.object_manager.dirty_slot_count <= 7);
    assert_eq!(stats.object_manager.routing_slot_count, 11);
    assert_eq!(stats.partition_info.table_name, "feature_table");
    assert_eq!(stats.partition_info.shard_uri, "local://table/shard-9");
    assert_eq!(stats.partition_info.start_routing_slot, 10);
    assert_eq!(stats.partition_info.end_routing_slot, 20);
    assert_eq!(stats.partition_info.object_manager, stats.object_manager);
    assert!(stats.block_store_extents.active_extents >= 1);
    assert!(stats.block_store_extents.active_physical_bytes > 0);
    assert_eq!(
        stats.block_store_extents.live_physical_bytes,
        stats.block_store_extents.active_physical_bytes
            + stats.block_store_extents.sealed_physical_bytes
    );
}

#[test]
fn prometheus_metrics_include_records_cache_page_and_wal() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    engine.block_store().roll_segment().unwrap();

    let metrics = engine.prometheus_metrics();
    assert!(metrics.contains("temporalstore_shard_records{shard_id=\"1\",kind=\"string\"} 1"));
    assert!(metrics.contains("temporalstore_cache_operations_total"));
    assert!(metrics.contains(
        "temporalstore_cache_operations_total{shard_id=\"1\",kind=\"memory_evictions\"}"
    ));
    assert!(metrics.contains("temporalstore_block_store_operations_total"));
    assert!(metrics
        .contains("temporalstore_block_store_extent_count{shard_id=\"1\",state=\"sealed\"} 1"));
    assert!(
        metrics.contains("temporalstore_block_store_extent_bytes{shard_id=\"1\",kind=\"live\"}")
    );
    assert!(metrics
        .contains("temporalstore_block_store_extent_bytes{shard_id=\"1\",kind=\"total_known\"}"));
    assert!(metrics.contains(
        "temporalstore_block_store_extent_oldest_unix_ms{shard_id=\"1\",scope=\"known\"}"
    ));
    assert!(metrics.contains(
        "temporalstore_block_store_extent_oldest_unix_ms{shard_id=\"1\",scope=\"live\"}"
    ));
    assert!(metrics.contains(
        "temporalstore_block_store_extent_oldest_age_ms{shard_id=\"1\",scope=\"known\"}"
    ));
    assert!(metrics
        .contains("temporalstore_block_store_extent_oldest_age_ms{shard_id=\"1\",scope=\"live\"}"));
    assert!(metrics.contains("temporalstore_wal_records_total{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_oplog_records_total{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_object_manager_objects{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_object_manager_page_refs{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_object_manager_dirty_objects{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_storage_slot_page_refs{shard_id=\"1\""));
    assert!(metrics.contains("temporalstore_storage_slot_bytes{shard_id=\"1\""));
    assert!(metrics.contains("temporalstore_storage_slot_dirty_objects{shard_id=\"1\""));
    assert!(metrics.contains("temporalstore_partition_routing_slots{shard_id=\"1\"} 4294967295"));
}

#[test]
fn slot_storage_summaries_track_live_refs_dirty_slots_and_manifest_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_slot: 10,
        end_routing_slot: 12,
        readonly: false,
        table_name: String::new(),
    });
    for key in ["alpha", "beta", "gamma"] {
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

    let summaries = engine.slot_storage_summaries(1);
    assert!(!summaries.is_empty());
    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.page_ref_count)
            .sum::<u64>(),
        3
    );
    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.dirty_object_count)
            .sum::<u64>(),
        3
    );
    let dirty_slot = summaries
        .iter()
        .find(|summary| summary.dirty_object_count > 0)
        .unwrap()
        .routing_slot;
    let manifest = engine
        .create_slot_dump_manifest(1, [dirty_slot])
        .expect("slot dump manifest should persist");
    engine.validate_slot_dump_manifest(&manifest).unwrap();
    let summaries = engine.slot_storage_summaries(1);
    assert!(summaries
        .iter()
        .filter(|summary| summary.routing_slot == dirty_slot)
        .all(|summary| summary.last_dump_sequence == manifest.index_log_sequence));
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn slot_page_ownership_is_first_class_and_survives_reload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_slot: 10,
        end_routing_slot: 12,
        readonly: false,
        table_name: String::new(),
    });
    for field in ["field-a", "field-b"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "hash-key".to_string(),
                        field: field.to_string(),
                        value: field.as_bytes().to_vec(),
                    },
                })
                .status
                .ok
        );
    }

    let physical_before_reload = engine.storage_physical_index_report(1);
    assert!(physical_before_reload.slot_index_authority);
    assert_eq!(physical_before_reload.page_index_count, 2);
    assert_eq!(physical_before_reload.dirty_slot_count, 1);
    assert_eq!(physical_before_reload.missing_object_id_count, 0);
    assert_eq!(physical_before_reload.missing_routing_slot_count, 0);
    assert!(physical_before_reload.slot_nodes.iter().any(|slot| {
        slot.page_ref_count == 2
            && slot.object_count == 2
            && slot.dirty_generation >= 2
            && slot.page_indexes.iter().all(|page| {
                page.model_id == "hash" && page.dirty && !page.deleted && !page.log_backed
            })
    }));
    assert_eq!(
        engine
            .slot_storage_summaries(1)
            .iter()
            .map(|summary| summary.object_count)
            .sum::<u64>(),
        2
    );
    let ownership = engine.slot_object_page_ownership_report(1);
    assert!(ownership.first_class_index_present);
    assert!(!ownership.derived_from_model_maps);
    assert_eq!(ownership.page_ref_count, 2);
    assert_eq!(ownership.missing_owner_page_ref_count, 0);
    assert_eq!(ownership.owner_mismatch_page_ref_count, 0);
    let physical = engine.storage_physical_index_report(1);
    assert!(physical.slot_index_authority);
    assert_eq!(physical.page_index_count, 2);
    assert_eq!(physical.dirty_slot_count, 1);

    engine.unload_shard(1);
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_slot: 10,
        end_routing_slot: 12,
        readonly: false,
        table_name: String::new(),
    });
    let physical_after_reload = engine.storage_physical_index_report(1);
    assert!(physical_after_reload.slot_index_authority);
    assert_eq!(physical_after_reload.page_index_count, 2);
    assert_eq!(physical_after_reload.dirty_slot_count, 0);
    assert!(physical_after_reload
        .slot_nodes
        .iter()
        .any(|slot| slot.page_ref_count == 2 && slot.object_count == 2));
    let reloaded_ownership = engine.slot_object_page_ownership_report(1);
    assert!(reloaded_ownership.first_class_index_present);
    assert!(!reloaded_ownership.derived_from_model_maps);
    assert_eq!(reloaded_ownership.page_ref_count, 2);
    assert_eq!(reloaded_ownership.missing_owner_page_ref_count, 0);
    assert_eq!(reloaded_ownership.owner_mismatch_page_ref_count, 0);
    let reloaded_physical = engine.storage_physical_index_report(1);
    assert!(reloaded_physical.slot_index_authority);
    assert_eq!(reloaded_physical.page_index_count, 2);
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn slot_index_is_authoritative_when_secondary_views_are_missing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "slot-authority".to_string(),
                    value: b"slot-value".to_vec(),
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard loaded");
        assert!(!shard.slot_index.slot_map.is_empty());
        shard.strings.clear();
        shard.hashes.clear();
        shard.sets.clear();
    }

    let exists = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExists {
            key: "slot-authority".to_string(),
        },
    });
    assert!(exists.status.ok);
    assert_eq!(exists.response, CommandResponse::Integer { value: 1 });

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "slot-authority".to_string(),
        },
    });
    assert!(get.status.ok);
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"slot-value".to_vec())
        }
    );
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn legacy_model_maps_are_promoted_to_slot_index_authority() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "legacy-map".to_string(),
                    value: b"promoted".to_vec(),
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard loaded");
        assert!(!shard.strings.is_empty());
        shard.slot_index.slot_map.clear();
    }

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "legacy-map".to_string(),
        },
    });
    assert!(get.status.ok);
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"promoted".to_vec())
        }
    );
    let physical = engine.storage_physical_index_report(1);
    assert!(physical.slot_index_authority);
    assert_eq!(physical.page_index_count, 1);
    assert_eq!(physical.missing_object_id_count, 0);
    assert_eq!(physical.missing_routing_slot_count, 0);
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn core_index_loads_legacy_slot_page_field_names() {
    let legacy_json = r#"{
        "slots": {
            "7": {
                "routing_slot": 7,
                "layout": "SinglePageObject",
                "dirty": false,
                "meta_loaded": true,
                "loading": false,
                "in_memory": true,
                "ttl_ms": null,
                "dirty_generation": 3,
                "last_dump_sequence": 11,
                "object_ids": [42],
                "page_refs": {
                    "string:k::1:2": {
                        "object_key": "k",
                        "model_id": "string",
                        "object_id": 42,
                        "address": {
                            "page_segment_id": 1,
                            "offset": 2,
                            "length": 3,
                            "page_id": 4,
                            "object_id": 42,
                            "routing_slot": 7
                        },
                        "dirty": false,
                        "deleted": false,
                        "log_backed": true
                    }
                }
            }
        }
    }"#;

    let index: CoreIndex = serde_json::from_str(legacy_json).unwrap();
    let slot = index.slot_map.get(&7).expect("legacy slot should load");
    assert!(slot.object_index.contains(&42));
    assert_eq!(slot.page_index.len(), 1);
    assert_eq!(
        slot.page_index
            .values()
            .next()
            .expect("legacy page index should load")
            .address
            .routing_slot,
        Some(7)
    );
}

// shared-corpus: cpp_storage_object_page_slot_parity_surfaces;
#[test]
fn object_manager_runtime_report_tracks_residency_layout_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_slot: 10,
        end_routing_slot: 12,
        readonly: false,
        table_name: String::new(),
    });

    for command in [
        Command::StringSet {
            key: "object-a".to_string(),
            value: b"a".to_vec(),
        },
        Command::HashSet {
            key: "hash-a".to_string(),
            field: "field-a".to_string(),
            value: b"field".to_vec(),
        },
        Command::FeatureReplace {
            key: "feature-a".to_string(),
            start_ms: 0,
            end_ms: 100,
            points: vec![FeaturePoint {
                timestamp_ms: 42,
                value: b"42".to_vec(),
            }],
        },
        Command::StringSet {
            key: "object-delete".to_string(),
            value: b"delete".to_vec(),
        },
        Command::CommonDelete {
            key: "object-delete".to_string(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }

    let report = engine.object_manager_runtime_report(1);
    assert!(report.runtime_ready, "{report:?}");
    assert!(report.routing_slot_count >= 1);
    assert!(report.object_count >= 4);
    assert!(report.page_ref_count >= 3);
    assert!(report.cold_object_count >= 3);
    assert!(report.tombstone_object_count >= 1);
    assert!(report.dirty_object_count >= 4);
    assert!(report.dirty_slot_count >= 1);
    assert!(report.max_dirty_generation >= 1);
    assert!(report.object_page_count >= 2);
    assert!(report.packed_timestamped_page_count >= 1);
    assert_eq!(report.missing_owner_page_ref_count, 0);
    assert_eq!(report.owner_mismatch_page_ref_count, 0);
    assert_eq!(report.reused_object_id_conflict_count, 0);
    assert!(report.blockers.is_empty());
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("hot/cold/tombstone object state")));

    engine.unload_shard(1);
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_slot: 10,
        end_routing_slot: 12,
        readonly: false,
        table_name: String::new(),
    });
    let reloaded = engine.object_manager_runtime_report(1);
    assert!(reloaded.runtime_ready, "{reloaded:?}");
    assert_eq!(reloaded.object_count, report.object_count);
    assert_eq!(reloaded.page_ref_count, report.page_ref_count);
    assert_eq!(
        reloaded.tombstone_object_count,
        report.tombstone_object_count
    );
    assert_eq!(
        reloaded.packed_timestamped_page_count,
        report.packed_timestamped_page_count
    );
}

#[test]
fn slot_dump_manifest_validation_rejects_checksum_and_missing_segments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let mut manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    manifest.logical_bytes = manifest.logical_bytes.saturating_add(1);
    assert!(
        !engine
            .validate_slot_dump_manifest(&manifest)
            .unwrap_err()
            .ok
    );

    let mut missing = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    missing.page_segment_ids.push(999_999);
    missing.checksum = slot_dump_manifest_checksum(&missing).unwrap();
    let missing_preflight = engine.slot_dump_install_preflight_report(&missing);
    assert!(!missing_preflight.install_safe);
    assert_eq!(missing_preflight.missing_page_segment_ids, vec![999_999]);
    assert!(missing_preflight
        .blockers
        .contains(&"missing_page_segments".to_string()));
    assert!(!engine.validate_slot_dump_manifest(&missing).unwrap_err().ok);

    let mut incomplete = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    incomplete.page_segment_ids.clear();
    incomplete.checksum = slot_dump_manifest_checksum(&incomplete).unwrap();
    assert_eq!(
        engine
            .validate_slot_dump_manifest(&incomplete)
            .unwrap_err()
            .code,
        "slot_dump_page_segment_mismatch"
    );

    let corrupt = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    let segment_id = corrupt.page_segment_ids[0];
    let mut segment = engine.block_store().read_segment(segment_id).unwrap();
    *segment.last_mut().unwrap() ^= 0xff;
    let _ = engine.block_store().install_segment(segment_id, &segment);
    let corrupt_preflight = engine.slot_dump_install_preflight_report(&corrupt);
    assert!(!corrupt_preflight.install_safe);
    assert!(corrupt_preflight
        .corrupt_page_segment_ids
        .contains(&segment_id));
    assert!(corrupt_preflight.unreadable_page_ref_count > 0);
    assert!(corrupt_preflight.unreadable_page_bytes > 0);
    assert!(corrupt_preflight
        .blockers
        .contains(&"unreadable_page_refs".to_string()));
    assert_eq!(
        engine
            .validate_slot_dump_manifest(&corrupt)
            .unwrap_err()
            .code,
        "slot_dump_unreadable_page_refs"
    );
}

#[test]
fn slot_dump_manifest_install_restores_index_and_rejects_partial_or_stale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "restore-me".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");

    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("pages"),
        dir.path().join("restore-indexes"),
    );
    restore_engine.load_shard(1);
    let safe_preflight = restore_engine.slot_dump_install_preflight_report(&manifest);
    assert!(safe_preflight.install_safe, "{safe_preflight:?}");
    assert!(safe_preflight.blockers.is_empty());
    assert_eq!(
        safe_preflight.manifest_index_log_sequence,
        manifest.index_log_sequence
    );
    restore_engine
        .install_slot_dump_manifest(&manifest)
        .expect("manifest should install");
    assert!(
        fs::read_dir(dir.path().join("restore-indexes"))
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp-")),
        "slot dump install should not leave atomic index temp files"
    );
    assert!(restore_engine.interrupted_slot_dump_installs(1).is_empty());
    let markers = list_slot_dump_install_markers_at(&restore_engine.index_dir, 1).unwrap();
    assert!(markers.iter().any(|marker| marker.phase == "prepare"));
    assert!(markers.iter().any(|marker| marker.phase == "install"));
    assert!(markers.iter().any(|marker| marker.phase == "commit"));
    let response = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "restore-me".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );

    let mut partial = manifest.clone();
    partial.index_bytes.clear();
    partial.checksum = slot_dump_manifest_checksum(&partial).unwrap();
    assert_eq!(
        restore_engine
            .install_slot_dump_manifest(&partial)
            .unwrap_err()
            .code,
        "slot_dump_partial_manifest"
    );

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "newer".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let stale_preflight = engine.slot_dump_install_preflight_report(&manifest);
    assert!(!stale_preflight.install_safe);
    assert!(stale_preflight.stale_manifest);
    assert!(stale_preflight
        .blockers
        .contains(&"stale_manifest_sequence".to_string()));
    assert_eq!(
        engine
            .install_slot_dump_manifest(&manifest)
            .unwrap_err()
            .code,
        "slot_dump_stale_manifest"
    );
}

// shared-corpus: storage_merged_dump_load_policy
#[test]
fn storage_merged_dump_load_policy_coordinates_dump_load_replay_and_index_gc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "merged-a".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "merged-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"seven".to_vec(),
            }],
        },
    });

    let report =
        engine.storage_merged_dump_load_policy_report(StorageMergedDumpLoadPolicyRequest {
            lifecycle: StorageLifecycleRequest {
                shard_id: 1,
                max_dump_slots_per_round: 16,
                prune_slot_dump_manifests: true,
                roll_forward_slot_dump_installs: true,
                invalidate_cache: true,
                warm_cache: true,
                ..StorageLifecycleRequest::default()
            },
            create_dump_manifest: true,
            install_dump_manifest: false,
        });
    assert!(report.policy_ready, "{report:?}");
    assert!(report.dump_manifest_created);
    assert!(report.load_preflight_safe);
    assert!(report.replay_boundary_safe);
    assert!(report.manifest_chain_valid);
    assert!(report.follower_retention_safe);
    assert!(report.index_gc_ready);
    assert!(report.manifest_id.is_some());
    assert!(!report.manifest_slot_ids.is_empty());
    assert!(report.manifest_checksum_validated);
    assert!(report.manifest_generation_validated);
    assert!(report.sequence_boundaries_validated);
    assert!(report.page_segments_validated);
    assert!(report.live_page_refs_validated);
    assert!(report.object_lifecycle_validated);
    assert!(report.merged_manifest_validated);
    assert!(report.source_slot_coverage_validated);

    let manifest = latest_slot_dump_manifest_at(&engine.index_dir, 1).unwrap();
    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("pages"),
        dir.path().join("restore-indexes"),
    );
    restore_engine.load_shard(1);
    restore_engine
        .install_slot_dump_manifest(&manifest)
        .expect("merged policy manifest should install into restore engine");
    let get = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "merged-a".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    let feature = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "merged-feature".to_string(),
            start_ms: 0,
            end_ms: 20,
            count: None,
        },
    });
    assert!(matches!(
        feature.response,
        CommandResponse::FeaturePoints { ref points } if points.len() == 1
    ));

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "merged-a".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let stale_preflight = engine.slot_dump_install_preflight_report(&manifest);
    assert!(!stale_preflight.install_safe, "{stale_preflight:?}");
    assert!(stale_preflight
        .blockers
        .contains(&"stale_page_conflicts".to_string()));
    assert!(stale_preflight.stale_page_conflict_count > 0);
    assert!(!engine
        .install_slot_dump_manifest(&manifest)
        .unwrap_err()
        .code
        .is_empty());
}

#[test]
fn slot_dump_install_markers_report_interrupted_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "marker".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_slot_dump_install_marker(
        &engine.index_dir,
        &SlotDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: "interrupted".to_string(),
            phase: "prepare".to_string(),
            oplog_sequence: manifest.oplog_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let interrupted = engine.interrupted_slot_dump_installs(1);
    assert_eq!(interrupted.len(), 1);
    assert_eq!(interrupted[0].phase, "prepare");
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.interrupted_slot_dump_installs, interrupted);
    assert_eq!(boundary.prepared_slot_dump_install_count, 1);
    assert_eq!(boundary.installed_slot_dump_install_count, 0);
    assert_eq!(boundary.unknown_slot_dump_install_count, 0);
    let readiness = engine.storage_production_readiness_report(1);
    assert_eq!(readiness.interrupted_slot_dump_install_count, 1);
    assert_eq!(readiness.prepared_slot_dump_install_count, 1);
    assert_eq!(readiness.installed_slot_dump_install_count, 0);
    assert_eq!(readiness.unknown_slot_dump_install_count, 0);
}

#[test]
fn slot_dump_install_roll_forward_completes_safe_installed_marker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "roll".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_slot_dump_install_marker(
        &engine.index_dir,
        &SlotDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "install".to_string(),
            oplog_sequence: manifest.oplog_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let dry_run = engine.slot_dump_install_roll_forward_reports(1);
    assert_eq!(dry_run.len(), 1);
    assert!(dry_run[0].can_roll_forward);
    assert_eq!(dry_run[0].reason, "commit_ready");

    let applied = engine.roll_forward_slot_dump_installs(1);
    assert_eq!(applied.len(), 1);
    assert!(applied[0].completed_commit);
    assert!(applied[0].obsolete_marker_files_removed > 0);
    assert!(engine.interrupted_slot_dump_installs(1).is_empty());
    let marker_files =
        slot_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
    assert!(marker_files
        .iter()
        .all(|(marker, _)| marker.phase == "commit"));
}

#[test]
fn slot_dump_install_roll_forward_retries_safe_prepare_marker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "retry-prepare".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_slot_dump_install_marker(
        &engine.index_dir,
        &SlotDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "prepare".to_string(),
            oplog_sequence: manifest.oplog_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let dry_run = engine.slot_dump_install_roll_forward_reports(1);
    assert_eq!(dry_run.len(), 1);
    assert!(dry_run[0].can_retry_install);
    assert!(!dry_run[0].can_roll_forward);
    assert_eq!(dry_run[0].reason, "install_retry_ready");

    let applied = engine.roll_forward_slot_dump_installs(1);
    assert_eq!(applied.len(), 1);
    assert!(applied[0].completed_install);
    assert!(applied[0].completed_commit);
    assert!(applied[0].obsolete_marker_files_removed > 0);
    assert!(engine.interrupted_slot_dump_installs(1).is_empty());
    let marker_files =
        slot_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
    assert!(marker_files
        .iter()
        .all(|(marker, _)| marker.phase == "commit"));
}

#[test]
fn slot_dump_recovery_reports_broken_manifest_parent_chain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "chain".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("parent manifest should persist");
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "chain".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("child manifest should persist");
    assert_eq!(child.parent_manifest_id, Some(parent.manifest_id.clone()));

    fs::remove_file(slot_dump_manifest_path(
        &engine.index_dir,
        1,
        &parent.manifest_id,
    ))
    .unwrap();
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.manifest_chain_issues.len(), 1);
    assert_eq!(
        boundary.manifest_chain_issues[0].manifest_id,
        child.manifest_id
    );
    assert_eq!(
        boundary.manifest_chain_issues[0].reason,
        "missing_parent_manifest"
    );
}

#[test]
fn slot_dump_manifest_prune_keeps_latest_parent_chain_and_removes_obsolete_fork() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "prune".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "prune".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-fork", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.dump_generation_id = slot_dump_generation_id(&fork);
    fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
    engine.persist_slot_dump_manifest(&fork).unwrap();
    write_slot_dump_install_marker(
        &engine.index_dir,
        &SlotDumpInstallMarker {
            shard_id: 1,
            manifest_id: fork.manifest_id.clone(),
            phase: "commit".to_string(),
            oplog_sequence: fork.oplog_sequence,
            index_log_sequence: fork.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let plan = engine.slot_dump_manifest_prune_plan(1);
    assert!(plan.retained_manifest_ids.contains(&parent.manifest_id));
    assert!(plan.retained_manifest_ids.contains(&child.manifest_id));
    assert_eq!(plan.prunable_manifest_ids, vec![fork.manifest_id.clone()]);
    assert_eq!(
        plan.prunable_marker_manifest_ids,
        vec![fork.manifest_id.clone()]
    );

    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: Vec::new(),
        max_dump_slots_per_round: 0,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: true,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    let report = lifecycle
        .manifest_prune_report
        .expect("lifecycle should apply manifest prune");
    assert_eq!(report.removed_manifest_ids, vec![fork.manifest_id.clone()]);
    assert_eq!(report.removed_marker_files, 1);
    assert_eq!(
        lifecycle.manifest_prune_plan.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );
    assert!(slot_dump_manifest_path(&engine.index_dir, 1, &parent.manifest_id).exists());
    assert!(slot_dump_manifest_path(&engine.index_dir, 1, &child.manifest_id).exists());
    assert!(!slot_dump_manifest_path(&engine.index_dir, 1, &fork.manifest_id).exists());
}

#[test]
fn slot_dump_manifest_prune_is_blocked_by_lagging_follower_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "cursor".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "cursor".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-follower-anchor", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
    fork.dump_generation_id = slot_dump_generation_id(&fork);
    fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
    engine.persist_slot_dump_manifest(&fork).unwrap();

    let no_cursor = engine.slot_dump_manifest_prune_plan(1);
    assert_eq!(
        no_cursor.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );

    let lagging_cursor = SlotDumpFollowerReplayCursor {
        follower_id: "follower-a".to_string(),
        shard_id: 1,
        oplog_sequence: fork.oplog_sequence,
        index_log_sequence: fork.index_log_sequence,
    };
    let blocked =
        engine.slot_dump_manifest_prune_plan_with_follower_cursors(1, vec![lagging_cursor.clone()]);
    assert!(blocked.prunable_manifest_ids.is_empty());
    assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
    assert_eq!(blocked.follower_blocks.len(), 1);
    assert_eq!(blocked.follower_blocks[0].follower_id, "follower-a");
    assert_eq!(blocked.follower_blocks[0].manifest_id, fork.manifest_id);
    assert!(blocked
        .reasons
        .contains(&"follower_cursor_blocks_prune".to_string()));

    let caught_up = engine.slot_dump_manifest_prune_plan_with_follower_cursors(
        1,
        vec![SlotDumpFollowerReplayCursor {
            oplog_sequence: child.oplog_sequence,
            index_log_sequence: child.index_log_sequence,
            ..lagging_cursor
        }],
    );
    assert_eq!(
        caught_up.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );
}

#[test]
fn slot_dump_manifest_prune_is_blocked_by_raft_snapshot_reference() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "snapshot".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "snapshot".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-snapshot-anchor", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
    fork.dump_generation_id = slot_dump_generation_id(&fork);
    fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
    engine.persist_slot_dump_manifest(&fork).unwrap();

    let no_snapshot = engine.slot_dump_manifest_prune_plan(1);
    assert_eq!(
        no_snapshot.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );

    let snapshot_ref = SlotDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-0007".to_string(),
        shard_id: 1,
        last_included_index: 7,
        last_included_term: 2,
        oplog_sequence: fork.oplog_sequence,
        index_log_sequence: fork.index_log_sequence,
    };
    let blocked = engine.slot_dump_manifest_prune_plan_with_retention_refs(
        1,
        Vec::<SlotDumpFollowerReplayCursor>::new(),
        vec![snapshot_ref.clone()],
    );
    assert!(blocked.prunable_manifest_ids.is_empty());
    assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
    assert_eq!(blocked.raft_snapshot_blocks.len(), 1);
    assert_eq!(
        blocked.raft_snapshot_blocks[0].snapshot_id,
        "raft-snapshot-0007"
    );
    assert_eq!(
        blocked.raft_snapshot_blocks[0].manifest_id,
        fork.manifest_id
    );
    assert!(blocked
        .reasons
        .contains(&"raft_snapshot_blocks_prune".to_string()));

    let advanced = engine.slot_dump_manifest_prune_plan_with_retention_refs(
        1,
        Vec::<SlotDumpFollowerReplayCursor>::new(),
        vec![SlotDumpRaftSnapshotRef {
            oplog_sequence: child.oplog_sequence,
            index_log_sequence: child.index_log_sequence,
            ..snapshot_ref
        }],
    );
    assert_eq!(
        advanced.prunable_manifest_ids,
        vec![fork.manifest_id.clone()]
    );
}

#[test]
fn slot_dump_manifest_rejects_generation_mismatch_and_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "generation".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    assert_eq!(manifest.version, 3);
    assert!(!manifest.dump_generation_id.is_empty());
    assert_eq!(manifest.object_lifecycle.live_object_ids, 1);
    assert_eq!(manifest.object_lifecycle.live_page_refs, 1);

    let mut legacy_v2 = manifest.clone();
    legacy_v2.version = 2;
    legacy_v2.object_lifecycle = StorageObjectLifecycleReport::default();
    let legacy_generation_id = slot_dump_generation_id(&legacy_v2);
    legacy_v2.object_lifecycle.live_object_ids = 99;
    assert_eq!(slot_dump_generation_id(&legacy_v2), legacy_generation_id);

    let mut mismatched = manifest.clone();
    mismatched.dump_generation_id = "wrong-generation".to_string();
    mismatched.checksum = slot_dump_manifest_checksum(&mismatched).unwrap();
    assert_eq!(
        engine
            .validate_slot_dump_manifest(&mismatched)
            .unwrap_err()
            .code,
        "slot_dump_generation_mismatch"
    );

    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("pages"),
        dir.path().join("restore-indexes"),
    );
    restore_engine.load_shard(1);
    restore_engine
        .install_slot_dump_manifest(&manifest)
        .expect("first generation should install");

    let mut fork = manifest.clone();
    let extra_slot = fork
        .slot_ids
        .iter()
        .copied()
        .max()
        .unwrap_or_default()
        .saturating_add(1);
    fork.slot_ids.push(extra_slot);
    fork.dump_generation_id = slot_dump_generation_id(&fork);
    fork.manifest_id = format!("{}-fork", fork.manifest_id);
    fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
    assert_eq!(
        restore_engine
            .install_slot_dump_manifest(&fork)
            .unwrap_err()
            .code,
        "slot_dump_generation_conflict"
    );
}

#[test]
fn slot_dump_manifest_rejects_object_lifecycle_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "lifecycle".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_slot_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_lifecycle = manifest.clone();
    stale_lifecycle.object_lifecycle.live_object_ids = stale_lifecycle
        .object_lifecycle
        .live_object_ids
        .saturating_add(1);
    stale_lifecycle.dump_generation_id = slot_dump_generation_id(&stale_lifecycle);
    stale_lifecycle.checksum = slot_dump_manifest_checksum(&stale_lifecycle).unwrap();
    assert_eq!(
        engine
            .validate_slot_dump_manifest(&stale_lifecycle)
            .unwrap_err()
            .code,
        "slot_dump_object_lifecycle_mismatch"
    );

    let mut reused_owner = manifest.clone();
    {
        let mut restored = serde_json::from_slice::<ShardState>(&reused_owner.index_bytes)
            .expect("manifest index should decode");
        let address = restored
            .strings
            .get_mut("lifecycle")
            .expect("manifest string address");
        address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
        reused_owner.index_bytes = serde_json::to_vec(&restored).unwrap();
        reused_owner.index_sha256 = sha256_hex_bytes(&reused_owner.index_bytes);
        reused_owner.dump_generation_id = slot_dump_generation_id(&reused_owner);
        reused_owner.checksum = slot_dump_manifest_checksum(&reused_owner).unwrap();
    }
    assert_eq!(
        engine
            .validate_slot_dump_manifest(&reused_owner)
            .unwrap_err()
            .code,
        "slot_dump_object_lifecycle_mismatch"
    );
}

#[test]
fn slot_dump_manifest_rejects_slot_summary_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "slot-summary".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_slot_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_summary = manifest.clone();
    let summary = stale_summary
        .slot_summaries
        .first_mut()
        .expect("slot summary should exist");
    summary.page_ref_count = summary.page_ref_count.saturating_add(1);
    stale_summary.dump_generation_id = slot_dump_generation_id(&stale_summary);
    stale_summary.checksum = slot_dump_manifest_checksum(&stale_summary).unwrap();

    assert_eq!(
        engine
            .validate_slot_dump_manifest(&stale_summary)
            .unwrap_err()
            .code,
        "slot_dump_slot_summary_mismatch"
    );
}

#[test]
fn slot_dump_manifest_rejects_byte_accounting_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "byte-accounting".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_slot_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_bytes = manifest.clone();
    stale_bytes.logical_bytes = stale_bytes.logical_bytes.saturating_add(1);
    stale_bytes.checksum = slot_dump_manifest_checksum(&stale_bytes).unwrap();

    assert_eq!(
        engine
            .validate_slot_dump_manifest(&stale_bytes)
            .unwrap_err()
            .code,
        "slot_dump_byte_accounting_mismatch"
    );
}

#[test]
fn slot_dump_manifest_rejects_non_canonical_slot_and_page_segment_ids() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "canonical".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_slot_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut duplicate_slot = manifest.clone();
    duplicate_slot.slot_ids.push(
        duplicate_slot
            .slot_ids
            .first()
            .copied()
            .expect("slot id should exist"),
    );
    duplicate_slot.dump_generation_id = slot_dump_generation_id(&duplicate_slot);
    duplicate_slot.checksum = slot_dump_manifest_checksum(&duplicate_slot).unwrap();
    assert_eq!(
        engine
            .validate_slot_dump_manifest(&duplicate_slot)
            .unwrap_err()
            .code,
        "slot_dump_slot_ids_not_canonical"
    );

    let mut duplicate_page_segment = manifest.clone();
    duplicate_page_segment.page_segment_ids.push(
        duplicate_page_segment
            .page_segment_ids
            .first()
            .copied()
            .expect("page segment id should exist"),
    );
    duplicate_page_segment.dump_generation_id = slot_dump_generation_id(&duplicate_page_segment);
    duplicate_page_segment.checksum = slot_dump_manifest_checksum(&duplicate_page_segment).unwrap();
    assert_eq!(
        engine
            .validate_slot_dump_manifest(&duplicate_page_segment)
            .unwrap_err()
            .code,
        "slot_dump_page_segment_ids_not_canonical"
    );
}

#[test]
fn storage_lifecycle_plan_and_boundary_report_cover_dirty_and_orphan_segments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.block_store().roll_segment().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        },
    });

    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: Vec::new(),
        max_dump_slots_per_round: 0,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: false,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!plan.dirty_slots.is_empty());
    assert_eq!(plan.selected_dump_slots, plan.dirty_slots);
    assert!(plan.reasons.contains(&"dirty_slot_dump".to_string()));
    assert!(plan.stale_page_segment_ids.contains(&0));
    assert!(plan
        .reasons
        .contains(&"ranked_reclaim_candidates".to_string()));
    assert!(!plan.reclaim_candidates.is_empty());
    assert_eq!(plan.reclaim_candidates[0].page_segment_id, 0);
    assert_eq!(plan.reclaim_candidates[0].reason, "orphan_segment");
    assert!(plan.reclaim_candidates[0].stale_physical_bytes > 0);
    assert!(plan.reclaim_candidates[0].reclaim_score > 0);

    let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: plan.selected_dump_slots.clone(),
        max_dump_slots_per_round: 0,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: false,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: true,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(report.dump_manifest.is_some());
    assert_eq!(report.object_lifecycle.live_object_ids, 1);
    assert_eq!(report.object_lifecycle.live_page_refs, 1);
    assert_eq!(report.object_lifecycle.stale_object_ids, 1);
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.latest_safe_oplog_sequence, 2);
    assert_eq!(boundary.latest_dump_oplog_sequence, 2);
    assert!(boundary.orphan_page_segment_ids.contains(&0));
}

#[test]
fn storage_production_readiness_reports_warnings_without_blocking_dirty_shard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "ready-key".to_string(),
                    value: b"ready-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report(1);
    assert!(report.production_ready, "{report:?}");
    assert!(report.blockers.is_empty());
    assert_eq!(report.dirty_slot_count, 1);
    assert!(report
        .warnings
        .contains(&"dirty_slots_pending_dump".to_string()));
    assert!(report.segment_integrity.integrity_ok);
    assert_eq!(report.segment_integrity.unreadable_page_ref_count, 0);
    assert_eq!(report.unreadable_page_ref_count, 0);
    assert_eq!(report.owner_mismatch_page_ref_count, 0);
    assert!(report.log_compatibility.rust_native_replay_safe);
    assert!(!report.log_compatibility.cxx_binary_compatible);
    assert_eq!(
        report.log_compatibility.oplog_format,
        "rust-jsonl-command-v1"
    );
    assert_eq!(
        report.log_compatibility.index_log_format,
        "rust-jsonl-shard-index-v1"
    );
    assert_eq!(
        report.log_compatibility.compatibility_mode,
        "rust_native_migration_only"
    );
    assert!(report.log_compatibility.migration_required);
    assert!(report.log_compatibility.golden_conversion_required);
    assert!(!report.log_compatibility.cxx_reader_supported);
    assert!(!report.log_compatibility.cxx_writer_supported);
    assert!(report.page_format_compatibility.rust_native_read_safe);
    assert!(!report.page_format_compatibility.cxx_page_header_compatible);
    assert_eq!(
        report.page_format_compatibility.page_format,
        "rust-page-envelope-v6"
    );
    assert_eq!(
        report.page_format_compatibility.compatibility_mode,
        "rust_envelope_migration_only"
    );
    assert!(report.page_format_compatibility.migration_required);
    assert!(report.page_format_compatibility.golden_conversion_required);
    assert!(
        !report
            .page_format_compatibility
            .cxx_page_header_reader_supported
    );
    assert!(
        !report
            .page_format_compatibility
            .cxx_page_header_writer_supported
    );
    assert!(report.page_format_compatibility.checksum_protected);
    assert!(report.page_format_compatibility.object_ids_embedded);
    assert!(report.block_store_bytes_written > 0);
}

#[test]
fn storage_log_compatibility_report_counts_jsonl_sequences_and_cxx_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..2 {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("log-key-{index}"),
                        value: format!("log-value-{index}").into_bytes(),
                    },
                })
                .status
                .ok
        );
    }

    let report = engine.storage_log_compatibility_report(1);
    assert_eq!(report.shard_id, 1);
    assert_eq!(report.compatibility_mode, "rust_native_migration_only");
    assert!(report.migration_required);
    assert!(!report.cxx_reader_supported);
    assert!(!report.cxx_writer_supported);
    assert!(report.golden_conversion_required);
    assert_eq!(report.oplog_last_sequence, 2);
    assert_eq!(report.index_log_last_sequence, 2);
    assert_eq!(report.oplog_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert!(report.oplog_bytes > 0);
    assert!(report.index_log_bytes > 0);
    assert!(report.rust_native_replay_safe);
    assert!(!report.cxx_binary_compatible);
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| { gap.contains("migration-only") && gap.contains("binary log") }));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("C++ binary/protobuf oplog")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("golden log conversion/replay")));
}

#[test]
fn storage_page_format_compatibility_report_counts_zones_and_header_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "page-format-key".to_string(),
                    value: vec![11; 512],
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_segment().unwrap();

    let report = engine.storage_page_format_compatibility_report(1);
    assert_eq!(report.shard_id, 1);
    assert_eq!(report.page_format, "rust-page-envelope-v6");
    assert_eq!(report.rust_envelope_version, 6);
    assert_eq!(report.compatibility_mode, "rust_envelope_migration_only");
    assert!(report.migration_required);
    assert!(!report.cxx_page_header_reader_supported);
    assert!(!report.cxx_page_header_writer_supported);
    assert!(report.golden_conversion_required);
    assert!(report.rust_native_read_safe);
    assert!(!report.cxx_page_header_compatible);
    assert!(report.checksum_protected);
    assert!(report.object_ids_embedded);
    assert!(report.routing_slots_embedded);
    assert!(report.compression_supported);
    assert_eq!(report.sealed_zones, 1);
    assert_eq!(report.active_zones, 1);
    assert!(report.live_physical_bytes > 0);
    assert!(report.page_store_writes > 0);
    assert!(report.page_store_bytes_written > 0);
    assert!(report.logical_bytes_written >= 512);
    assert!(report.compressed_records_written > 0);
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("migration-only") && gap.contains("page-header")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("C++ protobuf page header")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("golden page conversion/replay")));
}

#[test]
fn storage_production_readiness_policy_can_block_dirty_dump_lag_and_missing_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "policy-key".to_string(),
                    value: b"policy-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report_with_policy(
        1,
        StorageProductionReadinessPolicy {
            max_dirty_slots: Some(0),
            max_undumped_oplog_records: Some(0),
            require_slot_dump_manifest: true,
            ..StorageProductionReadinessPolicy::default()
        },
    );

    assert!(!report.production_ready, "{report:?}");
    assert_eq!(report.policy.max_dirty_slots, Some(0));
    assert_eq!(report.dirty_slot_count, 1);
    assert!(report.undumped_oplog_records > 0);
    assert!(report
        .blockers
        .contains(&"dirty_slots_exceed_policy".to_string()));
    assert!(report
        .blockers
        .contains(&"undumped_oplog_records_exceed_policy".to_string()));
    assert!(report
        .blockers
        .contains(&"slot_dump_manifest_required".to_string()));
}

#[test]
fn storage_production_readiness_policy_can_promote_warnings_to_blockers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "warn-key".to_string(),
                    value: b"warn-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report_with_policy(
        1,
        StorageProductionReadinessPolicy {
            block_on_warnings: true,
            ..StorageProductionReadinessPolicy::default()
        },
    );

    assert!(!report.production_ready, "{report:?}");
    assert!(report
        .warnings
        .contains(&"dirty_slots_pending_dump".to_string()));
    assert!(report
        .blockers
        .contains(&"warnings_exceed_policy".to_string()));
}

#[test]
fn storage_production_readiness_blocks_corrupt_live_page_segments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "corrupt-key".to_string(),
                    value: b"corrupt-value".to_vec(),
                },
            })
            .status
            .ok
    );
    let segment_id = engine.live_page_segment_ids(1)[0];
    let mut bytes = engine.block_store().read_segment(segment_id).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    engine
        .block_store()
        .install_segment(segment_id, &bytes)
        .unwrap();

    let report = engine.storage_production_readiness_report(1);
    assert!(!report.production_ready, "{report:?}");
    assert!(report
        .blockers
        .contains(&"corrupt_page_segments".to_string()));
    assert!(report
        .blockers
        .contains(&"unreadable_live_page_refs".to_string()));
    assert!(report
        .blockers
        .contains(&"storage_segment_integrity_failed".to_string()));
    assert!(!report.segment_integrity.integrity_ok);
    assert!(report.segment_integrity.corrupt_page_segment_count > 0);
    assert!(report.segment_integrity.unreadable_page_ref_count > 0);
    assert!(report.corrupt_page_segment_count > 0);
    assert!(report.unreadable_page_ref_count > 0);
}

#[test]
fn storage_lifecycle_apply_warms_cache_from_page_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "warm-me".to_string(),
            value: vec![7; 128],
        },
    });
    engine.cache().invalidate_shard(1).unwrap();
    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: Vec::new(),
        max_dump_slots_per_round: 0,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: false,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: plan.selected_dump_slots,
        max_dump_slots_per_round: 0,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: false,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: true,
        ..StorageLifecycleRequest::default()
    });
    assert!(report.cache_warmup_page_refs >= 1);
    assert_eq!(
        report.cache_warmup.warmed_page_refs,
        report.cache_warmup_page_refs
    );
    assert!(report.cache_warmup.considered_page_refs >= 1);
    assert!(report.cache_warmup.block_store_reads >= 1);
    assert!(report.cache_warmup.warmed_bytes >= 128);
    assert_eq!(report.cache_warmup.failed_page_refs, 0);
    assert!(engine.cache().stats().puts >= 1);
}

#[test]
fn storage_cache_warmup_report_filters_slots_and_counts_cache_hits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let first_key = "warm-slot-a";
    let first_slot = engine.routing_slot_for_key(1, first_key);
    let second_key = (0..100)
        .map(|index| format!("warm-slot-b-{index}"))
        .find(|key| engine.routing_slot_for_key(1, key) != first_slot)
        .expect("test should find a key in another slot");
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: first_key.to_string(),
            value: b"value-a".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: second_key,
            value: b"value-b".to_vec(),
        },
    });
    engine.cache().invalidate_shard(1).unwrap();

    let slot = first_slot;
    let first = engine.storage_cache_warmup_report(1, [slot]);
    assert_eq!(first.selected_slots, vec![slot]);
    assert_eq!(first.considered_page_refs, 1);
    assert_eq!(first.skipped_page_refs, 1);
    assert_eq!(first.block_store_reads, 1);
    assert_eq!(first.already_cached_page_refs, 0);
    assert_eq!(first.failed_page_refs, 0);
    assert!(first.warmed_bytes > 0);

    let second = engine.storage_cache_warmup_report(1, [slot]);
    assert_eq!(second.considered_page_refs, 1);
    assert_eq!(second.skipped_page_refs, 1);
    assert_eq!(second.block_store_reads, 0);
    assert_eq!(second.already_cached_page_refs, 1);
    assert_eq!(second.warmed_page_refs, 1);
}

#[test]
fn storage_cache_inspection_reports_slot_entries_and_invalidates_slot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let key = "slot-cache-key";
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: b"slot-cache-value".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.cache().invalidate_shard(1).unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            })
            .status
            .ok
    );

    let slot = engine.routing_slot_for_key(1, key);
    let report = engine.storage_cache_inspection_report(1);
    assert!(report.stats.disk_fills >= 1);
    assert!(report
        .entries
        .iter()
        .any(|entry| entry.selector.starts_with(&format!("slot-{slot}:"))));
    assert!(report
        .slot_summaries
        .iter()
        .any(|summary| summary.routing_slot == slot && summary.entry_count >= 1));

    let invalidated = engine
        .invalidate_storage_cache_slot(StorageCacheInvalidateSlotRequest {
            shard_id: 1,
            routing_slot: slot,
        })
        .unwrap();
    assert!(invalidated.memory_entries_removed >= 1);
    let after = engine.storage_cache_inspection_report(1);
    assert!(!after
        .entries
        .iter()
        .any(|entry| entry.selector.starts_with(&format!("slot-{slot}:"))));
}

#[test]
fn tiny_cache_dump_load_restart_refills_from_disk_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let restore_index_dir = dir.path().join("restore-indexes");
    let engine = TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);
    let target_value = b"dump-load-target-1234".to_vec();
    for (key, value) in [
        ("target", target_value.clone()),
        ("churn-a", b"cache-churn-a-1234".to_vec()),
        ("churn-b", b"cache-churn-b-1234".to_vec()),
    ] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                })
                .status
                .ok
        );
    }
    let target_page_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let address = shards
            .get(&1)
            .unwrap()
            .strings
            .get("target")
            .unwrap()
            .clone();
        CacheKey::page_with_slot(
            1,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
        )
    };

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "target".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    for key in ["churn-a", "churn-b"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string(),
                    },
                })
                .status
                .ok
        );
    }
    assert!(engine.cache().stats().memory_evictions > 0);
    assert!(engine.cache().stats().disk_bytes > 0);
    assert_eq!(engine.cache().get_memory(&target_page_key), None);
    let manifest = engine
        .create_slot_dump_manifest(1, Vec::new())
        .expect("slot dump manifest should persist");
    engine.validate_slot_dump_manifest(&manifest).unwrap();

    let restored = TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &restore_index_dir);
    restored.load_shard(1);
    restored
        .install_slot_dump_manifest(&manifest)
        .expect("slot dump should install after restart");
    let page_reads_before = restored.block_store().stats().reads;
    let disk_hits_before = restored.cache().stats().disk_hits;
    let response = restored.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(target_value)
        }
    );
    assert_eq!(
        restored.block_store().stats().reads,
        page_reads_before,
        "restored engine should refill from disk block cache before block store"
    );
    assert!(restored.cache().stats().disk_hits > disk_hits_before);

    let slot = restored.routing_slot_for_key(1, "target");
    let cache_report = restored.storage_cache_inspection_report(1);
    assert!(cache_report
        .slot_summaries
        .iter()
        .any(|summary| summary.routing_slot == slot && summary.entry_count >= 1));
    let invalidated = restored
        .invalidate_storage_cache_slot(StorageCacheInvalidateSlotRequest {
            shard_id: 1,
            routing_slot: slot,
        })
        .unwrap();
    assert!(invalidated.memory_entries_removed >= 1);
    let readiness = restored.storage_production_readiness_report(1);
    assert!(readiness.production_ready, "{readiness:?}");
    assert_eq!(readiness.unreadable_page_ref_count, 0);
    assert_eq!(readiness.corrupt_page_segment_count, 0);
}

#[test]
fn storage_lifecycle_plan_matches_cpp_delayed_and_limited_dirty_slot_dump_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..128 {
        let key = format!("slot-{i}");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: key.as_bytes().to_vec(),
            },
        });
        let observed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
            ..StorageLifecycleRequest::default()
        });
        if observed.dirty_slots.len() >= 3 {
            break;
        }
    }

    let delayed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: Vec::new(),
        max_dump_slots_per_round: 0,
        min_undumped_oplog_records: 99,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: false,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(delayed.dump_delayed);
    assert!(delayed.selected_dump_slots.is_empty());
    assert!(delayed
        .reasons
        .contains(&"dirty_slot_dump_delayed".to_string()));

    let limited = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: Vec::new(),
        max_dump_slots_per_round: 2,
        min_undumped_oplog_records: 1,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: false,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!limited.dump_delayed);
    assert!(limited.undumped_oplog_records >= 3);
    assert_eq!(limited.selected_dump_slots.len(), 2);
    assert!(limited.dirty_slots.len() >= limited.selected_dump_slots.len());

    let explicit = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_slots: vec![delayed.dirty_slots[0]],
        max_dump_slots_per_round: 0,
        min_undumped_oplog_records: 99,
        purge_delayed_destroy: false,
        prune_slot_dump_manifests: false,
        roll_forward_slot_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!explicit.dump_delayed);
    assert_eq!(explicit.selected_dump_slots, vec![delayed.dirty_slots[0]]);
}
