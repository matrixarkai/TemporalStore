// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 1 of engine tests, split from engine/tests.rs.
#![allow(clippy::all)]
use super::*;

// shared-corpus: dynamic_event_replication_mode_selection
#[test]
fn stale_shard_index_is_refused_rather_than_decoded_with_the_wrong_key_meaning() {
    // A pre-rekey index decodes CLEANLY -- context_events is still
    // HashMap<String, BTreeMap<u64, BlockAddress>> -- while the u64 changed meaning from
    // timeline_key to event_id_hash. None of that is type-visible, so without a version stamp
    // the engine would serve an index whose events are unaddressable and whose time-windowed
    // reads return nothing, raising no error at all.
    //
    // Built from a REAL serialized shard rather than hand-written JSON, so the fixture cannot
    // drift from the struct and quietly stop exercising the decode path.
    let shard = super::super::state::ShardState::default();
    let stamped = super::super::stamp_index_format_version(&shard);
    assert_eq!(
        stamped.get("index_format_version").and_then(|v| v.as_u64()),
        Some(u64::from(super::super::SHARD_INDEX_FORMAT_VERSION)),
        "every write path must stamp the current format version"
    );

    let current = serde_json::from_value::<super::super::state::ShardState>(stamped.clone())
        .expect("a stamped index must decode");
    assert_eq!(
        current.index_format_version,
        super::super::SHARD_INDEX_FORMAT_VERSION
    );

    // Exactly what a file written before the stamp existed looks like.
    let mut legacy = stamped;
    legacy
        .as_object_mut()
        .expect("index is a json object")
        .remove("index_format_version");
    let stale = serde_json::from_value::<super::super::state::ShardState>(legacy)
        .expect("a stale index still DECODES -- that is the whole problem");
    assert_eq!(
        stale.index_format_version, 0,
        "an index written before the stamp must read as version 0"
    );
    assert!(
        stale.index_format_version < super::super::SHARD_INDEX_FORMAT_VERSION,
        "the load guard must classify a pre-stamp index as stale and refuse it"
    );
}

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

// shared-corpus: context_events_slabs_entities_child_refs context_event_index_audit_dirty_models
#[test]
fn context_models_match_keys_timeline_pages_and_filters() {
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
        l1_ref: "l1://summary".to_string(),
        raw_metadata_ref: "raw://node".to_string(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    };
    let native_node = ContextNode {
        status: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
        ..node.clone()
    };
    let upsert = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: 11,
            node: Box::new(node.clone()),
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
        CommandResponse::ContextNode { node: Some(ref stored), .. } if stored == &native_node
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
            if ContextNode::decode_context_value(bytes).as_ref() == Some(&native_node)
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
        vector: Vec::new(),
    };
    let entity_upsert = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertEntity {
            tenant_hash: 11,
            entity: entity.clone(),
        },
    });
    assert!(entity_upsert.status.ok);
    // The collection key, matching the event contract asserted below ("ctx:event:11:42").
    // Entities are grouped by node in shard state now, so the object key a command reports has
    // to be the key that state is stored under -- every key-driven mechanism in engine.rs
    // (key-state capture/apply, tombstone removal, membership) looks it up directly. The
    // per-entity key "ctx:entity:11:42:7001" still exists where it must: as the page object id,
    // so two entities of one node cannot share a page, and as the persisted entry key, which is
    // what keeps the on-disk format identical across this change.
    assert!(matches!(
        entity_upsert.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:entity:11:42"
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
        vector: Vec::new(),
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
                event: Box::new(event),
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
            event: Box::new(ContextEvent {
                text: "ignored".to_string(),
                ..event_a.clone()
            }),
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
            max_scan: None,
            current_valid_only: true,
            as_of_ms: 0,
            kinds: vec![2],
            statuses: Vec::new(),
            min_confidence: 0.8,
            min_importance: 0.6,
        },
    });
    // Newest-first: the timeline scan is deliberately `.rev()`ed so retrieval reaches
    // recent, serving-relevant context without walking cold history first. Both events
    // share event_time_ms, so the tie breaks on event_id_hash -- 6 ("second") then
    // 5 ("first").
    assert!(matches!(
        queried.response,
        CommandResponse::ContextEvents { ref object_key, ref events }
            if object_key == "ctx:event:11:42"
                && events.iter().map(|event| event.text.as_str()).collect::<Vec<_>>()
                    == vec!["second", "first"]
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
        vector: Vec::new(),
    };
    let extracted = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteExtractedEvent {
            tenant_hash: 11,
            node_hash: 42,
            event: Box::new(extracted_event.clone()),
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
            event: Box::new(ContextEvent {
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
                vector: Vec::new(),
            }),
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

    let dirty = ContextDirtyNode {
        node_hash: 42,
        first_event_time_ms: 3_000,
        last_event_time_ms: 3_000,
        reason: 4,
        propagate_depth: 2,
        mark_count: 1,
    };
    let dirty_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextMarkSummaryDirty {
            tenant_hash: 11,
            node_hash: dirty.node_hash,
            event_time_ms: dirty.last_event_time_ms,
            reason: dirty.reason,
            propagate_depth: dirty.propagate_depth,
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
        CommandResponse::ContextSummaryDirtyNodes { nodes, .. } if nodes == vec![dirty]
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

#[test]
fn context_query_events_applies_limit_after_filter_like_native() {
    // QueryEvents filters within the (bounded) scan and applies `limit` AFTER filtering.
    // Rust previously took `limit` off the raw timeline first, so matching events beyond the
    // first `limit` timeline entries were silently dropped -- e.g. many earlier
    // low-confidence events hid the high-confidence matches on the live retrieval path.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let base = ContextEvent {
        event_id_hash: 0,
        event_time_ms: 0,
        ingestion_time_ms: 0,
        kind: 9,
        event_type: 2,
        actor_hash: 77,
        status: 1,
        valid_until_ms: 0,
        confidence: 0.1,
        importance: 0.7,
        text: String::new(),
        source_ref: "s".to_string(),
        related_node_hashes: vec![42],
        compact_attrs: vec![],
        vector: Vec::new(),
    };
    // 100 low-confidence events at earlier timestamps, then 5 high-confidence ones later.
    for i in 0..100u64 {
        let mut event = base.clone();
        event.event_id_hash = i + 1;
        event.event_time_ms = 1_000 + i;
        event.ingestion_time_ms = 1_000 + i;
        event.confidence = 0.1;
        event.text = format!("low{i}");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 11,
                node_hash: 42,
                event: Box::new(event),
                first_write_only: false,
                cold_storage: false,
            },
        });
    }
    for i in 0..5u64 {
        let mut event = base.clone();
        event.event_id_hash = 1_000 + i;
        event.event_time_ms = 1_200 + i;
        event.ingestion_time_ms = 1_200 + i;
        event.confidence = 0.9;
        event.text = format!("high{i}");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 11,
                node_hash: 42,
                event: Box::new(event),
                first_write_only: false,
                cold_storage: false,
            },
        });
    }
    let queried = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: 11,
            node_hash: 42,
            start_time_ms: 0,
            end_time_ms: 100_000,
            limit: Some(100),
            max_scan: None,
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.6,
            min_importance: 0.0,
        },
    });
    let CommandResponse::ContextEvents { events, .. } = queried.response else {
        panic!("expected ContextEvents");
    };
    assert_eq!(
        events.len(),
        5,
        "the 5 high-confidence events must survive limit-after-filter, not be hidden by the \
         100 earlier low-confidence events"
    );
}

// shared-corpus: context_tree_embedding_summary_compression
#[test]
fn context_tree_embedding_summary_and_compression_match_round_trip() {
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
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: Vec::new(),
            embedding_model_hash: 0,
            embedding_updated_at_ms: 0,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        },
        ContextNode {
            node_hash: GPU,
            parent_hash: ROOT,
            kind: 2,
            canonical_name: "gpu_purchase".to_string(),
            l0: "GPU purchase leaf node.".to_string(),
            status: 0,
            last_event_time_ms: 0,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: Vec::new(),
            embedding_model_hash: 0,
            embedding_updated_at_ms: 0,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(node),
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

    // Store these the way PRODUCTION writers do: on the node itself, addressed by the node.
    for (node_hash, first, second) in [(GPU, 1.0, 0.0), (COST, 0.0, 1.0)] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash: TENANT,
                node_hash,
                model_hash: 1,
                vector: vec![first, second],
                updated_at_ms: EVENT_TIME,
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
                    vector: Vec::new(),
                    embedding_model_hash: 0,
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
                event: Box::new(ContextEvent {
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
                    vector: Vec::new(),
                }),
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
                event: Box::new(ContextEvent {
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
                    vector: Vec::new(),
                }),
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
fn live_page_slab_ids_scan_all_index_backed_data_models() {
    let mut shard = ShardState::default();
    shard.strings.insert(
        "string".to_string(),
        BlockAddress::from_parts(7, 0, 1, None, None, None, None, None),
    );
    shard.hashes.entry("hash".to_string()).or_default().insert(
        "field".to_string(),
        BlockAddress::from_parts(8, 0, 1, None, None, None, None, None),
    );
    shard.sets.entry("set".to_string()).or_default().insert(
        b"member".to_vec(),
        BlockAddress::from_parts(9, 0, 1, None, None, None, None, None),
    );
    shard
        .features
        .entry("feature".to_string())
        .or_default()
        .insert(
            10,
            BlockAddress::from_parts(10, 0, 1, None, None, None, None, None),
        );
    shard
        .features
        .entry("sequence".to_string())
        .or_default()
        .insert(
            11,
            BlockAddress::from_parts(11, 0, 1, None, None, None, None, None),
        );
    shard
        .control_state
        .entry("control_state".to_string())
        .or_default()
        .insert(14, 1);

    let ids = collect_live_page_slab_ids(&shard)
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![7, 8, 9, 10, 11]);
}

#[test]
fn page_compaction_rewrites_live_addresses_and_allows_old_slab_gc() {
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
    assert_eq!(engine.live_page_slab_ids(1), vec![0]);

    let report = engine.compact_shard_pages(1).unwrap();
    assert_eq!(report.previous_page_slab_id, 0);
    assert_eq!(report.compacted_page_slab_id, 1);
    assert_eq!(report.rewritten_page_refs, 2);
    assert_eq!(report.stale_page_slab_ids, vec![0]);
    assert_eq!(report.before.live_page_slab_count, 1);
    assert_eq!(report.before.total_page_count, 3);
    assert_eq!(report.before.live_page_refs, 2);
    assert_eq!(report.before.stale_page_estimate, 1);
    assert_eq!(report.before.live_ref_density_basis_points, 6_666);
    assert_eq!(report.after.live_page_slab_count, 1);
    assert_eq!(report.after.total_page_count, 2);
    assert_eq!(report.after.live_page_refs, 2);
    assert_eq!(report.after.stale_page_estimate, 0);
    assert_eq!(report.after.live_ref_density_basis_points, 10_000);
    assert_eq!(engine.live_page_slab_ids(1), vec![1]);
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
            string_address.object_id(),
            Some(stable_page_object_id(1, "string", "k", None))
        );
        assert_eq!(
            string_address.routing_bucket(),
            Some(page_routing_bucket("k", 0, u32::MAX))
        );
        assert_eq!(
            hash_address.object_id(),
            Some(stable_page_object_id(1, "hash", "h", Some("f")))
        );
        assert_eq!(
            hash_address.routing_bucket(),
            Some(page_routing_bucket("h", 0, u32::MAX))
        );
    }

    let gc = block_store
        .gc_slabs_before_with_live_refs(1, engine.live_page_slab_ids(1))
        .unwrap();
    assert_eq!(gc.removed_page_slab_ids, vec![0]);
    assert_eq!(block_store.slab_ids().unwrap(), vec![1]);

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
        Command::ControlStateSet {
            family: ControlStateFamily::Distinct,
            key: "compact-control_state".to_string(),
            timestamp_ms: 45,
            amount: 3,
        },
        Command::ContextWriteEvent {
            tenant_hash: 7,
            node_hash: 9,
            event: Box::new(ContextEvent {
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
                vector: Vec::new(),
            }),
            first_write_only: false,
            cold_storage: false,
        },
        Command::ContextUpsertSummary {
            tenant_hash: 7,
            summary: ContextSummary {
                node_hash: 9,
                level: 1,
                text: "compact summary".to_string(),
                valid_from_ms: 52,
                vector: Vec::new(),
                embedding_model_hash: 0,
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
    assert!(report.reclaimable_stale_page_slab_count >= 1);
    assert_eq!(
        report.reclaimable_stale_page_slab_count,
        report.stale_page_slab_ids.len()
    );
    assert!(report.model_policy_family_count >= 6);
    assert!(report.tombstone_policy_model_count >= 1);
    assert!(report.stale_density_policy_model_count >= 1);
    assert!(report.layout_aware_policy_model_count >= 5);
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
    // Sequence folds into the feature family (shared timestamped-KV storage), so the
    // feature layout now aggregates both the feature series and the formerly separate
    // sequence series; there is no distinct "sequence" layout.
    assert_eq!(layout("feature").index_refs, 4);
    assert_eq!(layout("feature").unique_page_refs, 2);
    assert_eq!(layout("feature").packed_timestamped_pages, 2);
    assert_eq!(layout("context_event").index_refs, 1);
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
    assert!(policy("control_state").layout_aware_rewrite_required);
    assert!(policy("context_event").layout_aware_rewrite_required);
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
        "control_state",
        "context_event",
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
            .flat_map(|summary| summary.page_slab_ids.iter().copied())
            .all(|slab_id| slab_id == report.compacted_page_slab_id),
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
        min_undumped_wal_records: 0,
        warm_cache: true,
        ..StorageManagerCycleRequest::default()
    });

    assert!(report.completed, "{report:?}");
    for phase in [
        "prepare",
        "reclaim_wal",
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
        report.native_stage_order,
        vec![
            "prepare",
            "reclaim_wal",
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
    engine.block_store().roll_slab().unwrap();
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
    assert!(report.block_store_slab_api_ready);
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
            "reclaim_wal",
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
            min_undumped_wal_records: 0,
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
            .find(|page| page.object_key == Arc::from("owned"))
            .expect("owned slot page");
        page.address.set_object_id(Some(page.object_id().wrapping_add(1)));
    }

    let recovery = engine.storage_recovery_report(1);
    assert_eq!(recovery.owner_mismatch_page_refs.len(), 1);
    assert!(!recovery.slab_integrity.integrity_ok);
    assert_eq!(recovery.slab_integrity.owner_mismatch_page_ref_count, 1);
    assert_eq!(recovery.slab_integrity.missing_owner_page_ref_count, 0);
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
            .find(|page| page.object_key == Arc::from("first"))
            .map(|page| page.object_id())
            .expect("first object id");
        let second = shard
            .bucket_index
            .bucket_map
            .values_mut()
            .flat_map(|bucket| bucket.page_index.values_mut())
            .find(|page| page.object_key == Arc::from("second"))
            .expect("second slot page");
        second.address.set_object_id(Some(first_object_id));
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
fn crash_recovery_report_covers_wal_index_page_and_band_manifest() {
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
    engine.block_store().roll_slab().unwrap();
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

    // Base-only single-barrier recovery re-derives page layout from WAL replay (the out-of-band
    // roll_slab() is not a WAL command), so the detailed physical report -- slab ids, zone
    // descriptors, per-slab density -- differs from the delta-fold path. It still recovers every
    // acked write (asserted by the reads below) with all live pages readable and integral.
    if crate::engine::wal_single_barrier() {
        assert!(report.all_live_pages_readable);
        assert!(report.slab_integrity.integrity_ok);
        assert_eq!(report.slab_integrity.unreadable_page_ref_count, 0);
    } else {
    assert!(report.index_bytes > 0);
    assert!(report.index_write_atomic);
    assert_eq!(report.wal_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert_eq!(report.active_page_slab_ids, vec![0, 1]);
    assert_eq!(report.live_page_slab_ids, vec![0, 1]);
    assert_eq!(report.total_page_refs, 2);
    assert_eq!(report.readable_page_refs, 2);
    assert!(report.all_live_pages_readable);
    assert!(report.slab_integrity.integrity_ok);
    assert!(!report.slab_integrity.reclaim_required);
    assert_eq!(report.slab_integrity.indexed_page_slab_count, 2);
    assert_eq!(report.slab_integrity.discovered_page_slab_count, 2);
    assert_eq!(report.slab_integrity.live_page_slab_count, 2);
    assert_eq!(report.slab_integrity.unreadable_page_ref_count, 0);
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
    assert_eq!(report.page_slab_live_reports.len(), 2);
    assert_eq!(report.page_slab_live_reports[0].page_slab_id, 0);
    assert_eq!(report.page_slab_live_reports[0].page_count, 1);
    assert_eq!(report.page_slab_live_reports[0].live_page_refs, 1);
    assert_eq!(
        report.page_slab_live_reports[0].readable_live_page_refs,
        1
    );
    assert_eq!(
        report.page_slab_live_reports[0].unreadable_live_page_refs,
        0
    );
    assert_eq!(report.page_slab_live_reports[0].stale_page_estimate, 0);
    assert_eq!(
        report.page_slab_live_reports[0].live_ref_density_basis_points,
        10_000
    );
    assert_eq!(report.page_slab_live_reports[0].live_object_count, 1);
    assert_eq!(
        report.page_slab_live_reports[0].live_routing_bucket_count,
        1
    );
    assert_eq!(report.page_slab_live_reports[0].live_logical_bytes, 2);
    assert!(report.page_slab_live_reports[0].live_physical_bytes > 0);
    }

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
fn crash_recovery_report_marks_stale_slab_density_after_overwrite() {
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
    // The overwrite keeps exactly one live object ("hot"="new", 3 bytes) under any recovery mode.
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "hot".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"new".to_vec())
        }
    );
    let report = recovered.storage_recovery_report(1);
    let slab = report
        .page_slab_live_reports
        .iter()
        .find(|slab| slab.page_slab_id == 0)
        .expect("segment 0 live-density report");

    // The single live object is exactly the same regardless of recovery mode.
    assert_eq!(slab.live_page_refs, 1);
    assert_eq!(slab.readable_live_page_refs, 1);
    assert_eq!(slab.live_logical_bytes, 3);
    assert_eq!(slab.live_object_count, 1);
    assert_eq!(slab.live_routing_bucket_count, 1);
    if !crate::engine::wal_single_barrier() {
        // Default path: the delta fold reconstructs exactly the two physical pages (stale old +
        // live new) -> 50% live density. Base-only single-barrier recovery replays the two writes
        // on top of the pages that happened to survive a clean in-process reload, so the slab
        // physically holds extra stale pages (same single live object, reclaimed by GC). On a real
        // power cut the un-synced pages are gone and replay rebuilds them cleanly. Physical density
        // is therefore not asserted under the flag.
        assert_eq!(slab.page_count, 2);
        assert_eq!(slab.stale_page_estimate, 1);
        assert_eq!(slab.live_ref_density_basis_points, 5_000);
    }
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
        assert_ne!(address.page_slab_id, HOT_PAGE_SLAB_ID);
        CacheKey::page_with_slot(
            1,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket(),
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
    engine.block_store().roll_slab().unwrap();
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

    assert_eq!(report.wal_records, 2);
    assert!(report.all_live_pages_readable);
    assert!(report.zone_summary.live_physical_bytes > 0);
    // The band manifest was rebuilt (from the page stream on the default path; from WAL-replayed
    // pages under the single barrier). Recovery of both acked writes is asserted by the reads below.
    assert!(page_dir.join("page_extent_manifest.json").exists());
    if !crate::engine::wal_single_barrier() {
        // Default path: the delta fold reconstructs the exact on-disk page layout at the original
        // addresses, so the sealed(slab 0)+active(slab 1) split from the out-of-band roll_slab()
        // survives. Base-only single-barrier recovery re-derives layout by replaying the WAL (the
        // roll_slab() is not a WAL command, so both writes replay into the active slab) -- a
        // different but valid physical layout that preserves the same logical state.
        assert_eq!(report.index_log_records, 2);
        assert_eq!(report.active_page_slab_ids, vec![0, 1]);
        assert_eq!(report.live_page_slab_ids, vec![0, 1]);
        assert_eq!(report.total_page_refs, 2);
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
    }
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
        string_address.object_id(),
        Some(stable_page_object_id(1, "string", "k", None))
    );
    assert_eq!(
        string_address.routing_bucket(),
        Some(page_routing_bucket("k", 10, 20))
    );
    assert_eq!(
        string_address.band_id(),
        Some(string_address.page_slab_id)
    );
    assert_eq!(
        hash_address.object_id(),
        Some(stable_page_object_id(1, "hash", "h", Some("f")))
    );
    assert_eq!(
        hash_address.routing_bucket(),
        Some(page_routing_bucket("h", 10, 20))
    );
    assert_eq!(hash_address.band_id(), Some(hash_address.page_slab_id));
    assert_ne!(string_address.object_id(), hash_address.object_id());
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

/// A sink that just remembers what it was told, so a test can ask whether a write reached it.
#[derive(Debug, Default)]
struct RecordingWalSink {
    seen: std::sync::Mutex<Vec<(ShardId, Command)>>,
}

impl crate::data_node::SharedWalSink for RecordingWalSink {
    fn record_write(&self, shard_id: ShardId, command: &Command) {
        self.seen
            .lock()
            .expect("recording sink lock poisoned")
            .push((shard_id, command.clone()));
    }
}

/// An expiry deletion has to reach the mirror, not only this node's log.
///
/// Expiry appends its tombstone straight to the WAL, outside the request path, so nothing at
/// the data-node layer ever sees it. In shared mode that meant the deletion reached the local
/// log and no other: a successor replaying the shared log never observed it, reapplied the
/// earlier write, and the key came back -- which is the very failure the tombstone was added
/// to prevent, reappearing one level up.
#[test]
fn an_expiry_deletion_reaches_the_maintenance_mirror() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let sink = std::sync::Arc::new(RecordingWalSink::default());
    engine.set_maintenance_wal_mirror(sink.clone());

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

    let seen = sink
        .seen
        .lock()
        .expect("recording sink lock poisoned")
        .clone();
    assert_eq!(
        seen,
        vec![(
            1u64,
            Command::CommonDelete {
                key: "expire-me".to_string()
            }
        )],
        "the expiry tombstone must reach the mirror, not just the local log"
    );
}

/// With no mirror attached the sweep behaves exactly as it did before one existed.
#[test]
fn an_expiry_sweep_without_a_mirror_is_unchanged() {
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

// shared-corpus: context_dirty_coalescing_is_visible
#[test]
fn repeated_marks_coalesce_and_the_query_says_so() {
    // Dirty tracking is a coalescing map, not a log of records. The record type that used to
    // carry a mark could hold only one timestamp, so a query had to flatten first and last into
    // a single event_time_ms and drop the mark count entirely -- the caller could not tell one
    // edit from fifty, nor how much time the dirty span covered.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3300;
    const NODE: u64 = 88;

    for event_time_ms in [5_000_u64, 9_000, 7_000] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: TENANT,
                node_hash: NODE,
                event_time_ms,
                reason: 2,
                propagate_depth: 1,
            },
        });
    }

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQuerySummaryDirty {
            tenant_hash: TENANT,
            node_hash: NODE,
            start_time_ms: 0,
            end_time_ms: 100_000,
            limit: None,
        },
    });
    let CommandResponse::ContextSummaryDirtyNodes { nodes, .. } = response.response else {
        panic!("expected ContextSummaryDirtyNodes");
    };

    // Three marks, one entry -- that is the coalescing, and it is why nothing is appended per
    // edit. Before, a caller could only have learned this by counting records that no longer
    // exist.
    assert_eq!(1, nodes.len(), "repeated marks must not accumulate entries");
    let node = &nodes[0];
    assert_eq!(3, node.mark_count, "the query must report how many marks arrived");
    assert_eq!(5_000, node.first_event_time_ms, "earliest event that made it dirty");
    assert_eq!(
        9_000, node.last_event_time_ms,
        "latest event, and NOT the last mark to arrive -- 7_000 came in last"
    );
    assert_eq!(2, node.reason);
    assert_eq!(1, node.propagate_depth);
}

// shared-corpus: context_dirty_window_uses_the_whole_span
#[test]
fn a_query_window_is_matched_against_the_whole_dirty_span() {
    // The span matters for more than reporting: a window that overlaps only the earliest event
    // must still find the node. Flattened to one timestamp, the early half of the span was
    // invisible and a drain scoped to it would skip work it owed.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3301;
    const NODE: u64 = 89;

    for event_time_ms in [1_000_u64, 50_000] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: TENANT,
                node_hash: NODE,
                event_time_ms,
                reason: 0,
                propagate_depth: 0,
            },
        });
    }

    let found = |start: u64, end: u64| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash: TENANT,
                node_hash: NODE,
                start_time_ms: start,
                end_time_ms: end,
                limit: None,
            },
        });
        match response.response {
            CommandResponse::ContextSummaryDirtyNodes { nodes, .. } => !nodes.is_empty(),
            _ => false,
        }
    };

    assert!(found(0, 2_000), "a window over the earliest event must match");
    assert!(found(40_000, 60_000), "a window over the latest event must match");
    assert!(found(10_000, 20_000), "a window inside the span must match");
    assert!(!found(60_000, 70_000), "a window past the span must not match");
}

// shared-corpus: context_node_inline_embedding
#[test]
fn a_node_embedding_lands_on_the_node_itself() {
    // Addressing the write by the node is the whole point: the old ref hash of
    // (tenant, owner, level) is one-way, so a vector written under it could only ever be
    // found again by recomputing that hash from an owner someone already had in hand.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 4242;
    const NODE: u64 = 77;

    let node = ContextNode {
        node_hash: NODE,
        parent_hash: 0,
        kind: 1,
        canonical_name: "session/with-a-vector".to_string(),
        l0: "the text this node is about".to_string(),
        status: 0,
        last_event_time_ms: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    };
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node: Box::new(node),
        },
    });

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextSetNodeEmbedding {
            tenant_hash: TENANT,
            node_hash: NODE,
            model_hash: 909,
            vector: vec![0.25, 0.5, -0.75],
            updated_at_ms: 1_781_000_000_000,
        },
    });

    let fetched = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: TENANT,
            node_hash: NODE,
        },
    });
    let CommandResponse::ContextNode { node, .. } = fetched.response else {
        panic!("expected ContextNode");
    };
    let node = node.expect("the node must still be there");
    assert_eq!(vec![0.25, 0.5, -0.75], node.vector, "vector did not reach the node");
    assert_eq!(909, node.embedding_model_hash);
    assert_eq!(1_781_000_000_000, node.embedding_updated_at_ms);
    // The rest of the node must survive a write that only meant to attach a vector.
    assert_eq!("session/with-a-vector", node.canonical_name);
    assert_eq!("the text this node is about", node.l0);
    assert_eq!(1, node.kind);
}

// shared-corpus: context_node_inline_embedding_replaces
#[test]
fn re_embedding_a_node_replaces_the_vector_rather_than_accumulating() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 4243;
    const NODE: u64 = 78;

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node: Box::new(ContextNode {
                node_hash: NODE,
                parent_hash: 0,
                kind: 1,
                canonical_name: "n".to_string(),
                l0: "text".to_string(),
                status: 0,
                last_event_time_ms: 0,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: Vec::new(),
                embedding_model_hash: 0,
                embedding_updated_at_ms: 0,
                summary_vector: Vec::new(),
                summary_vector_valid_from_ms: 0,
                summary_vector_model_hash: 0,
            }),
        },
    });
    for (model, vector, at) in [
        (1_u64, vec![1.0_f32, 0.0], 1_781_000_000_000_u64),
        (2, vec![0.0, 1.0], 1_781_000_000_500),
    ] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash: TENANT,
                node_hash: NODE,
                model_hash: model,
                vector,
                updated_at_ms: at,
            },
        });
    }
    let fetched = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: TENANT,
            node_hash: NODE,
        },
    });
    let CommandResponse::ContextNode { node, .. } = fetched.response else {
        panic!("expected ContextNode");
    };
    let node = node.expect("node must exist");
    assert_eq!(vec![0.0, 1.0], node.vector, "the newer vector must win outright");
    assert_eq!(2, node.embedding_model_hash);
    assert_eq!(1_781_000_000_500, node.embedding_updated_at_ms);
}

// shared-corpus: context_node_inline_embedding_no_node
#[test]
fn an_embedding_for_a_node_that_does_not_exist_is_refused_not_invented() {
    // Writing a placeholder node here would put a node into the tree that ingest never
    // created -- a node with a vector, no name and no text, which retrieval would then score.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextSetNodeEmbedding {
            tenant_hash: 4244,
            node_hash: 999,
            model_hash: 1,
            vector: vec![1.0],
            updated_at_ms: 1_781_000_000_000,
        },
    });
    let CommandResponse::ContextObjectKey { object_key } = response.response else {
        panic!("expected ContextObjectKey");
    };
    assert!(object_key.is_empty(), "no node means nothing was written");

    let fetched = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 4244,
            node_hash: 999,
        },
    });
    let CommandResponse::ContextNode { node, .. } = fetched.response else {
        panic!("expected ContextNode");
    };
    assert!(node.is_none(), "a node must not be conjured by embedding it");
}

// shared-corpus: context_node_inline_embedding_query
#[test]
fn node_vectors_can_be_asked_for_by_owner() {
    // The read that the separate record could never offer. ContextQueryEmbeddings is keyed by a
    // hash of (tenant, owner, level), so a caller must already hold every owner just to build
    // the keys, and the reply cannot say which owner each vector came from -- the caller has to
    // rebuild that mapping itself. Asking by node returns the pairing directly.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 5150;

    for (node_hash, vector) in [(1_u64, vec![1.0_f32, 0.0]), (2, vec![0.0, 1.0]), (3, vec![])] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: format!("n{node_hash}"),
                    l0: format!("text {node_hash}"),
                    status: 0,
                    last_event_time_ms: 0,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        });
        if !vector.is_empty() {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextSetNodeEmbedding {
                    tenant_hash: TENANT,
                    node_hash,
                    model_hash: 5,
                    vector,
                    updated_at_ms: 1_781_000_000_000,
                },
            });
        }
    }

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryNodeEmbeddings {
            tenant_hash: TENANT,
            node_hashes: vec![1, 2, 3, 4],
        },
    });
    let CommandResponse::ContextNodeEmbeddings { embeddings } = response.response else {
        panic!("expected ContextNodeEmbeddings");
    };

    // Node 3 exists but was never embedded, and node 4 does not exist at all. Both are omitted
    // rather than returned as an empty vector, so "not embedded yet" stays distinguishable from
    // "embedded to zeros" -- a caller that cannot tell them apart scores an un-embedded node as
    // maximally dissimilar instead of falling back to lexical matching.
    assert_eq!(2, embeddings.len(), "only embedded nodes may come back");
    let by_node: std::collections::BTreeMap<u64, Vec<f32>> = embeddings.into_iter().collect();
    assert_eq!(Some(&vec![1.0, 0.0]), by_node.get(&1));
    assert_eq!(Some(&vec![0.0, 1.0]), by_node.get(&2));
    assert!(!by_node.contains_key(&3), "an un-embedded node must not appear");
    assert!(!by_node.contains_key(&4), "a missing node must not appear");
}

// shared-corpus: context_traversal_scores_from_inline_vector
#[test]
fn traversal_scores_a_child_whose_only_vector_is_on_the_node() {
    // The retirement-readiness assertion: NO separate embedding record exists here. If the
    // traversal still reaches for one, dropping those records would silently turn every
    // tree query into an empty answer -- so this test must hold BEFORE they can go.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 5001;
    const ROOT: u64 = 1;
    const NEAR: u64 = 2;
    const FAR: u64 = 3;
    const EVENT_TIME: u64 = 1_781_600_000_000;

    for (node_hash, name) in [(ROOT, "root"), (NEAR, "near"), (FAR, "far")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash,
                    parent_hash: if node_hash == ROOT { 0 } else { ROOT },
                    kind: 1,
                    canonical_name: name.to_string(),
                    l0: format!("{name} text"),
                    status: 0,
                    last_event_time_ms: EVENT_TIME,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        });
        assert!(response.status.ok);
    }
    for child_hash in [NEAR, FAR] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertChildRef {
                tenant_hash: TENANT,
                child_ref: ContextChildRef {
                    parent_hash: ROOT,
                    child_hash,
                    updated_at_ms: EVENT_TIME,
                },
            },
        });
        assert!(response.status.ok);
    }
    // Vectors arrive ONLY through the node-addressed write -- no ContextUpsertEmbedding at all.
    for (node_hash, first, second) in [(NEAR, 1.0, 0.0), (FAR, 0.0, 1.0)] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash: TENANT,
                node_hash,
                model_hash: 7,
                vector: vec![first, second],
                updated_at_ms: EVENT_TIME,
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
    assert!(
        matches!(
            traversal.response,
            CommandResponse::ContextTraversedNodes { ref nodes }
                if nodes.len() == 1 && nodes[0].node_hash == NEAR && nodes[0].score > 0.99
        ),
        "a child whose only vector lives on the node must be scored"
    );
}

// shared-corpus: context_extracted_event_time_query
#[test]
fn an_extracted_event_is_visible_to_time_ranged_queries() {
    // The event rekey moved the primary map to EVENT ID keys with a separate time index; this
    // arm was missed, kept inserting timeline keys into the id-keyed map and never fed the time
    // index -- so every extracted event wrote successfully and was invisible to time queries.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: serde_json::from_str(r#"{"kind":"context_write_extracted_event","tenant_hash":99,"node_hash":700,"event":{"event_id_hash":701,"event_time_ms":510,"ingestion_time_ms":515,"type":4,"confidence":0.9,"importance":0.8,"text":"probe event"},"indexes":{"entity_hashes":[9001],"status_hash":77,"source_hash":88,"event_time_bucket_ms":500}}"#).unwrap(),
    });
    assert!(write.status.ok);
    assert!(matches!(
        write.response,
        CommandResponse::ContextExtractedEventWrite { ref event_object_key, .. }
            if event_object_key == "ctx:event:99:700"
    ));
    let q = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: serde_json::from_str(r#"{"kind":"context_query_events","tenant_hash":99,"node_hash":700,"start_time_ms":500,"end_time_ms":520,"limit":null}"#).unwrap(),
    });
    assert!(
        matches!(
            q.response,
            CommandResponse::ContextEvents { ref events, .. }
                if events.len() == 1 && events[0].event_id_hash == 701
        ),
        "a successfully written extracted event must be visible to a time-ranged query"
    );
}

#[test]
fn scratch_engine_index_dir_dies_with_the_last_engine_clone() {
    let engine = TemporalEngine::default();
    let index_dir = engine.index_dir.clone();
    assert!(index_dir.exists(), "a scratch engine must create its index dir");
    let clone = engine.clone();
    drop(engine);
    assert!(index_dir.exists(), "a live engine clone must keep the scratch dir");
    drop(clone);
    assert!(
        !index_dir.exists(),
        "the last engine clone must remove the scratch index dir on drop"
    );
}

#[test]
fn served_index_container_round_trips_and_still_reads_plain_json() {
    use crate::engine::{decode_index_bytes, encode_index_bytes};

    let mut shard = ShardState::default();
    shard.strings.insert(
        "container-probe".to_string(),
        BlockAddress::from_parts(7, 11, 13, None, None, None, None, None),
    );

    // Container OFF: raw JSON, and JSON is what an older binary would have written.
    // Say "0" rather than unsetting: unsetting selects the DEFAULT, which is the container,
    // so an unset variable stopped meaning "off" the moment the default changed.
    std::env::set_var("TS_INDEX_BINARY", "0");
    let plain = encode_index_bytes(&shard);
    assert_eq!(plain.first(), Some(&b'{'), "container off must write JSON");
    let decoded = decode_index_bytes(&plain).expect("json index decodes");
    assert!(decoded.strings.contains_key("container-probe"));

    // Container ON: a magic-prefixed payload that decodes back to the same state...
    std::env::set_var("TS_INDEX_BINARY", "1");
    let wrapped = encode_index_bytes(&shard);
    std::env::remove_var("TS_INDEX_BINARY");
    assert!(wrapped.starts_with(b"TSIDX\x01"), "container on must write the container");
    assert_ne!(wrapped.first(), Some(&b'{'));
    let decoded = decode_index_bytes(&wrapped).expect("container index decodes");
    assert!(decoded.strings.contains_key("container-probe"));

    // ...and reading is unconditional: a container decodes with the flag off, which is what
    // makes the write flag safe to turn on and off independently of any reader.
    let decoded_again = decode_index_bytes(&wrapped).expect("container decodes with flag off");
    assert!(decoded_again.strings.contains_key("container-probe"));

    // An unknown payload codec must be refused, never guessed at: a mis-parsed index serves
    // wrong data with no error anywhere, which is the failure mode this container exists to stop.
    let mut future = wrapped.clone();
    future[6] = 0xEE;
    let refused = decode_index_bytes(&future).expect_err("unknown codec must refuse");
    assert!(refused.contains("cannot read"), "unhelpful refusal: {refused}");
}

#[test]
fn binary_index_payload_round_trips_and_refuses_a_shape_it_cannot_read() {
    use crate::engine::{decode_index_bytes, encode_index_bytes};

    let mut shard = ShardState::default();
    shard.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    for i in 0..64u64 {
        shard.strings.insert(
            format!("object-{i}"),
            BlockAddress::from_parts(i, i * 7, i + 1, Some(i), Some(i * 3), Some((i % 8) as u32), Some(i), None),
        );
    }
    shard.hashes.entry("hash-object".to_string()).or_default().insert(
        "component".to_string(),
        BlockAddress::from_parts(9, 1, 2, None, None, None, None, None),
    );
    shard.applied_wal_sequence = Some(4242);

    std::env::set_var("TS_INDEX_BINARY", "1");
    std::env::set_var("TS_INDEX_CODEC", "msgpack");
    let binary = encode_index_bytes(&shard);
    std::env::remove_var("TS_INDEX_BINARY");
    std::env::remove_var("TS_INDEX_CODEC");

    assert!(binary.starts_with(b"TSIDX\x01"), "binary payload must ride the container");
    assert_eq!(binary[6], 2, "payload codec id");

    // Exact round-trip: the durable image has to come back as the same state, not merely a
    // plausible one -- so compare the maps, not just that it decoded.
    let decoded = decode_index_bytes(&binary).expect("binary index decodes");
    assert_eq!(decoded.strings.len(), shard.strings.len());
    for (key, address) in &shard.strings {
        assert_eq!(decoded.strings.get(key), Some(address), "address changed for {key}");
    }
    assert_eq!(decoded.hashes, shard.hashes);
    assert_eq!(decoded.applied_wal_sequence, shard.applied_wal_sequence);
    assert_eq!(decoded.index_format_version, shard.index_format_version);

    // The payload is addressed by field ORDER, so a struct of a different shape must be refused
    // rather than decoded into something plausible and wrong. The version stamp sits outside the
    // payload precisely so it can be checked before any of it is parsed.
    let mut wrong_version = binary.clone();
    wrong_version[7..11].copy_from_slice(&99u32.to_be_bytes());
    let refused = decode_index_bytes(&wrong_version).expect_err("a foreign struct version refuses");
    assert!(refused.contains("struct version"), "unhelpful refusal: {refused}");

    // And every older on-disk shape still loads: plain JSON, and the compressed-JSON payload.
    // "0" rather than unset -- unset now selects the container, which is not what is under
    // test here.
    std::env::set_var("TS_INDEX_BINARY", "0");
    let plain = encode_index_bytes(&shard);
    assert_eq!(plain.first(), Some(&b'{'), "container off still writes JSON");
    assert_eq!(
        decode_index_bytes(&plain).expect("json decodes").strings.len(),
        shard.strings.len()
    );

    std::env::set_var("TS_INDEX_BINARY", "1");
    std::env::set_var("TS_INDEX_CODEC", "zstd-json");
    let compressed_json = encode_index_bytes(&shard);
    std::env::remove_var("TS_INDEX_BINARY");
    std::env::remove_var("TS_INDEX_CODEC");
    assert_eq!(compressed_json[6], 1, "zstd-json keeps codec id 1");
    assert_eq!(
        decode_index_bytes(&compressed_json).expect("zstd-json decodes").strings.len(),
        shard.strings.len()
    );

    // The binary payload is the smaller one -- that is the whole point of adding it.
    assert!(
        binary.len() < plain.len(),
        "binary {} not smaller than json {}",
        binary.len(),
        plain.len()
    );
}


/// Where does the cost of reading ONE summary go: fetching the bytes, or decoding them?
///
/// The command-level measurement says 253 allocations and 289 KB per candidate for a 4 KB summary
/// -- a 70x amplification -- and two guesses about why have already been wrong, including one that
/// made it worse. So split the read in half and count each half, and print how many points the
/// extent holds, which settles whether siblings are being decoded at all.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_reading_one_summary_actually_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_reading_one_summary_actually_costs() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  text   points/extent   extent bytes   read allocs   read bytes   decode allocs   decode bytes
"
    );
    for text_len in [16usize, 512, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let text = "s".repeat(text_len);
        for node_hash in 1..=120u64 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertSummary {
                    tenant_hash: 7201,
                    summary: crate::types::ContextSummary {
                        node_hash,
                        level: 2,
                        text: text.clone(),
                        valid_from_ms: 1_000,
                        vector: vec![0.25_f32; 16],
                        embedding_model_hash: 0,
                    },
                },
            });
            assert!(response.status.ok, "{:?}", response.status);
        }

        let address = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let key = super::context::context_summary_key(7201, 60, 2);
            let series = shard
                .context_summaries
                .get(&key)
                .expect("the summary series must exist, or this measures nothing");
            series
                .iter()
                .next()
                .map(|(_, address)| address.clone())
                .expect("one entry")
        };

        // Warm, so neither half is charged for filling a cache that a steady-state read finds warm.
        let _ = super::read_page_bytes(&engine.cache, &engine.page_store, 1, &address);

        let read_probe = crate::alloc_probe::Probe::start();
        let mut bytes = Vec::new();
        for _ in 0..5 {
            bytes = super::read_page_bytes(&engine.cache, &engine.page_store, 1, &address)
                .expect("the page must read, or the split below is measuring a None");
        }
        let read = read_probe.stop();

        let decode_probe = crate::alloc_probe::Probe::start();
        let mut points = 0usize;
        for _ in 0..5 {
            points = match super::packed_pages::decode_feature_page_strict(&bytes) {
                super::state::PackedFeaturePageDecode::Packed(p) => p.len(),
                super::state::PackedFeaturePageDecode::Legacy => 1,
                super::state::PackedFeaturePageDecode::Corrupt(_) => 0,
            };
        }
        let decode = decode_probe.stop();

        println!(
            "  {text_len:>4}   {points:>13}   {:>12}   {:>11}   {:>10}   {:>13}   {:>12}",
            bytes.len(),
            read.allocs / 5,
            read.alloc_bytes / 5,
            decode.allocs / 5,
            decode.alloc_bytes / 5,
        );

        // Cold walk over every address, which is what a batch read actually does: 120 distinct
        // extents rather than one warm one. Warming a single address measures the best case and
        // would report it as the cost.
        let addresses: Vec<crate::block_store::BlockAddress> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            (1..=120u64)
                .filter_map(|node_hash| {
                    let key = super::context::context_summary_key(7201, node_hash, 2);
                    shard
                        .context_summaries
                        .get(&key)
                        .and_then(|series| series.iter().next())
                        .map(|(_, address)| address.clone())
                })
                .collect()
        };
        assert_eq!(addresses.len(), 120, "every summary must be addressable");
        let wal_resident = addresses
            .iter()
            .filter(|a| crate::wal_record::is_wal_resident(a.page_slab_id))
            .count();
        let with_page_id = addresses.iter().filter(|a| a.page_id().is_some()).count();
        let distinct_slabs: std::collections::BTreeSet<u64> =
            addresses.iter().map(|a| a.page_slab_id).collect();
        println!(
            "         {wal_resident}/120 wal_resident addresses, {with_page_id} carry a page_id, {} distinct slabs, block_in_wal enabled={}",
            distinct_slabs.len(),
            std::env::var("TS_BLOCK_IN_WAL").unwrap_or_else(|_| "unset(on)".to_string()),
        );
        let extent_total: u64 = addresses.iter().map(|a| a.length).sum();

        let walk_probe = crate::alloc_probe::Probe::start();
        let mut decoded = 0usize;
        for address in &addresses {
            if let Some(page) = super::read_page_bytes(&engine.cache, &engine.page_store, 1, address)
            {
                if let super::state::PackedFeaturePageDecode::Packed(points) =
                    super::packed_pages::decode_feature_page_strict(&page)
                {
                    decoded += points.len();
                }
            }
        }
        let walk = walk_probe.stop();

        // The block-store read alone, over the same addresses, on a store whose cache is already
        // warm from the walk above -- so this is purely file seek + read_exact + decode.
        let read_only_probe = crate::alloc_probe::Probe::start();
        let mut read_bytes_total = 0usize;
        for address in &addresses {
            if let Ok(b) = engine.page_store.read(address) {
                read_bytes_total += b.len();
            }
        }
        let read_only = read_only_probe.stop();
        println!(
            "         block-store read alone: {} allocs/addr, {} bytes/addr ({} payload bytes returned in total)",
            read_only.allocs / 120,
            read_only.alloc_bytes / 120,
            read_bytes_total,
        );
        println!(
            "         cold walk over every address: {} allocs/addr, {} bytes/addr, {} extents totalling {} bytes, {decoded} points decoded",
            walk.allocs / 120,
            walk.alloc_bytes / 120,
            addresses.len(),
            extent_total,
        );
    }
    println!(
        "
  points/extent > 1 means siblings ARE decoded for every read, and a page-granular cache pays.
  points/extent == 1 means each record has its own extent and the cost is inside one record.
"
    );
}


/// How many distinct pages do the nodes a retrieve scores actually span?
///
/// The node fetch is now nearly all of a retrieve's per-candidate cost, and with the discarded
/// text measured at 1 allocation of 16 it is essentially one page read per candidate. Clustering
/// the nodes a traversal co-selects would only help if they currently sit on DIFFERENT pages.
///
/// candidates-per-extent is the ceiling: 1.0 means every candidate costs its own page read and
/// clustering could remove nearly all of them; a high number means they already share and there is
/// nothing here worth a storage-layout change.
///
///   cargo test -p temporalstore-rust --lib how_many_pages_do_a_retrieves_candidates_span -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn how_many_pages_do_a_retrieves_candidates_span() {
    println!(
        "
  nodes   distinct extents   nodes/extent   distinct slabs   bytes addressed
"
    );
    for count in [40_u64, 120, 360] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let tenant = 7401_u64;
        for node_hash in 1..=count {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode {
                    tenant_hash: tenant,
                    node: Box::new(crate::types::ContextNode {
                        node_hash,
                        parent_hash: 0,
                        kind: 1,
                        canonical_name: format!("session/node-{node_hash}"),
                        l0: "a summary of roughly the length a real extract produces for one turn"
                            .to_string(),
                        status: 0,
                        last_event_time_ms: 1_781_700_000_000,
                        l1_ref: String::new(),
                        raw_metadata_ref: String::new(),
                        vector: vec![0.25_f32; 16],
                        embedding_model_hash: 7,
                        embedding_updated_at_ms: 1,
                        summary_vector: vec![0.5_f32; 16],
                        summary_vector_valid_from_ms: 1_781_700_000_000,
                        summary_vector_model_hash: 7,
                    }),
                },
            });
            assert!(response.status.ok, "upsert {node_hash}: {:?}", response.status);
        }

        // The addresses the scoring pass would read, straight from the shard's own map.
        let (extents, slabs, bytes) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let mut extents: std::collections::BTreeSet<(u64, u64, u64)> =
                std::collections::BTreeSet::new();
            let mut slabs: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            let mut bytes = 0_u64;
            for node_hash in 1..=count {
                let key = super::context::context_node_key(tenant, node_hash);
                let address = shard
                    .hashes
                    .get(&key)
                    .and_then(|fields| fields.values().next())
                    .or_else(|| shard.context_nodes.get(&key));
                if let Some(address) = address {
                    if extents.insert((address.page_slab_id, address.offset, address.length)) {
                        bytes += address.length;
                    }
                    slabs.insert(address.page_slab_id);
                }
            }
            (extents.len(), slabs.len(), bytes)
        };
        assert!(extents > 0, "no node addresses resolved -- this measures nothing");
        println!(
            "  {count:>5}   {extents:>16}   {:>12.2}   {slabs:>14}   {bytes:>15}",
            count as f64 / extents as f64,
        );

        // Span versus payload: if these extents are adjacent, `read_range` can fetch many nodes in
        // one read and slice them apart, which needs no durable format change. If they are spread
        // through the slab, only a real re-layout helps.
        let (span, largest_gap, ordered) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let mut ranges: Vec<(u64, u64)> = Vec::new();
            for node_hash in 1..=count {
                let key = super::context::context_node_key(tenant, node_hash);
                if let Some(address) = shard
                    .hashes
                    .get(&key)
                    .and_then(|fields| fields.values().next())
                    .or_else(|| shard.context_nodes.get(&key))
                {
                    ranges.push((address.offset, address.length));
                }
            }
            ranges.sort_unstable();
            let ordered = ranges.len();
            let span = match (ranges.first(), ranges.last()) {
                (Some(first), Some(last)) => (last.0 + last.1).saturating_sub(first.0),
                _ => 0,
            };
            let mut largest_gap = 0_u64;
            for pair in ranges.windows(2) {
                let end_of_first = pair[0].0 + pair[0].1;
                largest_gap = largest_gap.max(pair[1].0.saturating_sub(end_of_first));
            }
            (span, largest_gap, ordered)
        };
        println!(
            "          span {span} bytes over {ordered} extents holding {bytes} bytes  \
({:.2}x the payload), largest gap {largest_gap}",
            span as f64 / bytes.max(1) as f64,
        );
    }
    println!(
        "
  nodes/extent near 1.0  => every candidate costs its own page read; clustering is the lever.
  nodes/extent high      => candidates already share pages and a layout change wins nothing.
"
    );
}


/// The same contiguity question, but after a REAL ingest rather than a node-only fixture.
///
/// Writing only nodes packs them perfectly (span == payload, zero gaps) and that is not the shape
/// a deployment has: an ingest writes events, summaries, entities and index rows in between, so the
/// nodes a retrieve scores are separated by records it does not want.
///
/// Reports the contiguous RUNS the node extents form. Reads per retrieve fall from one-per-candidate
/// to one-per-run if the fetch coalesces, and the foreign bytes column says what that costs.
///
///   cargo test -p temporalstore-rust --lib how_scattered_are_node_extents_after_a_real_ingest -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn how_scattered_are_node_extents_after_a_real_ingest() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    println!(
        "
  adds   node extents   runs   extents/run   node bytes   span bytes   foreign bytes dragged
"
    );
    for adds in [40_usize, 120] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let tenant = 7501_u64;
        let mut node_hashes: Vec<u64> = Vec::new();
        for index in 0..adds {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: tenant,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: tenant,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("LOC-{index:06}"),
                        title: format!("locality {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} about the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "ingest {index}: {:?}", report.status);
            node_hashes.extend(report.node_hashes.iter().copied());
        }
        node_hashes.sort_unstable();
        node_hashes.dedup();
        assert!(!node_hashes.is_empty(), "no nodes ingested -- nothing to measure");

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let mut ranges: Vec<(u64, u64, u64)> = Vec::new(); // (slab, offset, length)
        for node_hash in &node_hashes {
            let key = super::context::context_node_key(tenant, *node_hash);
            if let Some(address) = shard
                .hashes
                .get(&key)
                .and_then(|fields| fields.values().next())
                .or_else(|| shard.context_nodes.get(&key))
            {
                ranges.push((address.page_slab_id, address.offset, address.length));
            }
        }
        ranges.sort_unstable();
        let node_bytes: u64 = ranges.iter().map(|r| r.2).sum();

        // A run is a maximal group of extents that a single range read could cover: same slab, and
        // each starting where the previous ended.
        let mut runs = 0_usize;
        let mut span = 0_u64;
        let mut previous: Option<(u64, u64)> = None; // (slab, end offset)
        let mut run_start: Option<(u64, u64)> = None;
        for (slab, offset, length) in &ranges {
            match previous {
                Some((prev_slab, prev_end)) if prev_slab == *slab && prev_end == *offset => {}
                _ => {
                    if let (Some((_, start)), Some((_, end))) = (run_start, previous) {
                        span += end.saturating_sub(start);
                    }
                    runs += 1;
                    run_start = Some((*slab, *offset));
                }
            }
            previous = Some((*slab, offset + length));
        }
        if let (Some((_, start)), Some((_, end))) = (run_start, previous) {
            span += end.saturating_sub(start);
        }

        println!(
            "  {adds:>4}   {:>12}   {runs:>4}   {:>11.2}   {node_bytes:>10}   {span:>10}   {:>21}",
            ranges.len(),
            ranges.len() as f64 / runs.max(1) as f64,
            span.saturating_sub(node_bytes),
        );
    }
    println!(
        "
  extents/run near 1  => nodes are isolated between other records; only a re-layout helps.
  extents/run high    => a coalescing fetch cuts reads by that factor with no format change.
"
    );
}


/// Does the per-ingest bucket-index reconstruct change anything, or recompute what is already right?
///
/// It costs 94% of an add's allocations at 320 memories and grows with the store. Since a context
/// write now maintains the index rather than asking for a rebuild, the rebuild at the end of each
/// ingest may be pure overhead -- and if it is, skipping it removes almost all of an add's
/// allocation.
///
/// Snapshots every bucket before and after, and reports the buckets that differ. A difference is
/// not a failure of this test: it names precisely what a write still fails to maintain, which is
/// the next thing to fix.
///
///   cargo test -p temporalstore-rust --lib does_the_per_ingest_reconstruct_change_anything -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn does_the_per_ingest_reconstruct_change_anything() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    /// (routing bucket, pages, live object ids, tombstoned ids, layout, deleted, in_memory)
    type Shape = (u32, usize, usize, usize, String, bool, bool);

    fn snapshot(engine: &TemporalEngine) -> Vec<Shape> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let mut out: Vec<Shape> = shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(routing_bucket, bucket)| {
                (
                    *routing_bucket,
                    bucket.page_index.len(),
                    bucket.object_index.len(),
                    bucket.deleted_object_index.len(),
                    format!("{:?}", bucket.layout),
                    bucket.deleted,
                    bucket.in_memory,
                )
            })
            .collect();
        out.sort_unstable();
        out
    }

    for rung in [40_usize, 160] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..rung {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: 7901,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: 7901,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("REDUN-{index:06}"),
                        title: format!("redundancy {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "grow {index}: {:?}", report.status);
        }

        // The ingest already ran a reconstruct at its end. Snapshot, run another, compare: a
        // reconstruct that changes nothing on an already-reconstructed store is idempotent, which
        // is necessary but not sufficient. The interesting case is the one below it.
        let before_idempotent = snapshot(&engine);
        engine.reconstruct_bucket_index_now(1);
        let after_idempotent = snapshot(&engine);

        // The real question: does a reconstruct change anything after ONE MORE plain write, which
        // is what the ingest's own final reconstruct is there to absorb?
        let node = crate::types::ContextNode {
            node_hash: 8_500_000 + rung as u64,
            parent_hash: 0,
            kind: 1,
            canonical_name: format!("session/redun-{rung}"),
            l0: "one more write, then reconstruct".to_string(),
            status: 0,
            last_event_time_ms: 1_781_700_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: vec![0.25_f32; 16],
            embedding_model_hash: 7,
            embedding_updated_at_ms: 1,
            summary_vector: vec![0.5_f32; 16],
            summary_vector_valid_from_ms: 1_781_700_000_000,
            summary_vector_model_hash: 7,
        };
        let wrote = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode { tenant_hash: 7901, node: Box::new(node) },
        });
        assert!(wrote.status.ok, "{:?}", wrote.status);
        let before_write = snapshot(&engine);
        engine.reconstruct_bucket_index_now(1);
        let after_write = snapshot(&engine);

        let idempotent_differs = before_idempotent
            .iter()
            .zip(after_idempotent.iter())
            .filter(|(a, b)| a != b)
            .count()
            + before_idempotent.len().abs_diff(after_idempotent.len());
        let write_differs = before_write
            .iter()
            .zip(after_write.iter())
            .filter(|(a, b)| a != b)
            .count()
            + before_write.len().abs_diff(after_write.len());

        println!(
            "  corpus {rung:>4}: buckets {:>4}   reconstruct-after-reconstruct differs in {idempotent_differs}   reconstruct-after-one-write differs in {write_differs}",
            before_idempotent.len(),
        );
        if write_differs > 0 {
            for (a, b) in before_write.iter().zip(after_write.iter()).filter(|(a, b)| a != b).take(3)
            {
                println!("      before {a:?}");
                println!("      after  {b:?}");
            }
        }
    }
    println!(
        "
  both columns 0 => the reconstruct recomputes an index the writes already maintain, and the
                    ingest's final call is removable (94% of an add's allocations at 320 memories).
  non-zero      => that difference is what a write still fails to maintain; fix that first.
"
    );
}


/// Full-contents version of the reconstruct-redundancy question.
///
/// The shape comparison said a reconstruct changes nothing, but it compared counts after a bare
/// node write. Removing a call that keeps a durable index correct deserves the strong form: every
/// page entry's identity, address and flags, captured after a COMPLETE ingest rather than one
/// write.
///
///   cargo test -p temporalstore-rust --lib deep_compare_the_index_a_reconstruct_produces -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn deep_compare_the_index_a_reconstruct_produces() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    /// Every field of every page entry, ordered, so two indexes compare exactly.
    fn deep(engine: &TemporalEngine) -> Vec<String> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let mut rows: Vec<String> = Vec::new();
        for (routing_bucket, bucket) in shard.bucket_index.bucket_map.iter() {
            for (field_key, page) in bucket.page_index.iter() {
                rows.push(format!(
                    "{routing_bucket}|{field_key}|{}|{}|{:?}|{}|{}|{}|{}|{}|{}|{}|{}",
                    page.object_key,
                    page.model_id,
                    page.component,
                    page.object_id(),
                    page.address.page_slab_id,
                    page.address.offset,
                    page.address.length,
                    page.dirty,
                    page.deleted,
                    page.log_backed,
                    bucket.layout as u8 as u32,
                ));
            }
            for object_id in bucket.object_index.iter() {
                rows.push(format!("{routing_bucket}|obj|{object_id}"));
            }
            for object_id in bucket.deleted_object_index.iter() {
                rows.push(format!("{routing_bucket}|tomb|{object_id}"));
            }
        }
        rows.sort_unstable();
        rows
    }

    for rung in [30_usize, 120] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let mut ingest = |index: usize| {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: 8101,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: 8101,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("DEEP-{index:06}"),
                        title: format!("deep {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "ingest {index}: {:?}", report.status);
        };

        for index in 0..rung {
            ingest(index);
        }

        // A COMPLETE ingest, then compare the index before and after a reconstruct. The ingest ran
        // its own reconstruct at the end, so this asks whether a further one would find anything to
        // change -- which is exactly what removing the call would rely on.
        ingest(rung);
        let before = deep(&engine);
        engine.reconstruct_bucket_index_now(1);
        let after = deep(&engine);

        let only_before = before.iter().filter(|row| !after.contains(row)).count();
        let only_after = after.iter().filter(|row| !before.contains(row)).count();
        println!(
            "  corpus {rung:>4}: {} page/index rows   only-before {only_before}   only-after {only_after}",
            before.len(),
        );
        for row in before.iter().filter(|row| !after.contains(row)).take(2) {
            println!("      lost by reconstruct: {row}");
        }
        for row in after.iter().filter(|row| !before.contains(row)).take(2) {
            println!("      added by reconstruct: {row}");
        }
    }
    println!(
        "
  both 0 => the reconstruct reproduces the index the writes already built, entry for entry, and
            the ingest's final call can go (94% of an add's allocations at 320 memories).
  non-0  => those rows are what a write fails to maintain, and are the thing to fix instead.
"
    );
}


/// Series append or block-store append: which one grows with the store?
///
/// `ContextUpsertSummary` costs 1,553 allocations at 40 memories and 10,667 at 320, while
/// `ContextUpsertNode` is flat at ~194. They differ in their write primitive, so measure the
/// primitives directly on grown stores with FRESH keys -- nothing merges into an existing series,
/// so anything that grows is responding to the store rather than to the key.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib which_write_primitive_grows_with_the_store -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn which_write_primitive_grows_with_the_store() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };
    use crate::types::FeaturePoint;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  primitive                        corpus 40   corpus 320   growth
"
    );

    let mut series_small = 0_u64;
    let mut series_large = 0_u64;
    let mut value_small = 0_u64;
    let mut value_large = 0_u64;

    for (rung, series_out, value_out) in [
        (40_usize, &mut series_small, &mut value_small),
        (320_usize, &mut series_large, &mut value_large),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..rung {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: 8301,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: 8301,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("PRIM-{index:06}"),
                        title: format!("prim {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "grow {index}: {:?}", report.status);
        }

        let payload = vec![7_u8; 512];
        let shards = engine.shards.read().expect("engine lock poisoned");
        let _ = shards.get(&1).expect("loaded shard");
        drop(shards);

        // The series primitive, on a fresh key.
        let warm_key = format!("probe:series:warm:{rung}");
        let _ = super::packed_pages::append_timestamped_kv_pages(
            &engine.cache,
            &engine.page_store,
            1,
            "context_summary",
            &warm_key,
            vec![FeaturePoint { timestamp_ms: 1, value: payload.clone() }],
            0,
            false,
            // promote_sync_writes: every caller in the engine passes true, and this probe
            // exists to characterise the primitive as the engine uses it.
            true,
        );
        let key = format!("probe:series:{rung}");
        let probe = crate::alloc_probe::Probe::start();
        let _ = super::packed_pages::append_timestamped_kv_pages(
            &engine.cache,
            &engine.page_store,
            1,
            "context_summary",
            &key,
            vec![FeaturePoint { timestamp_ms: 1, value: payload.clone() }],
            0,
            false,
            // promote_sync_writes: every caller in the engine passes true, and this probe
            // exists to characterise the primitive as the engine uses it.
            true,
        );
        *series_out = probe.stop().allocs;

        // The plain value append, for contrast.
        let _ = super::append_value(
            &engine.cache,
            &engine.page_store,
            1,
            &payload,
            Some(9_900_000 + rung as u64),
            Some(0),
            false,
        );
        let probe = crate::alloc_probe::Probe::start();
        let _ = super::append_value(
            &engine.cache,
            &engine.page_store,
            1,
            &payload,
            Some(9_950_000 + rung as u64),
            Some(0),
            false,
        );
        *value_out = probe.stop().allocs;
    }

    println!(
        "  append_timestamped_kv_pages    {series_small:>9}   {series_large:>10}   {:>6.2}x",
        series_large as f64 / series_small.max(1) as f64,
    );
    println!(
        "  append_value                   {value_small:>9}   {value_large:>10}   {:>6.2}x",
        value_large as f64 / value_small.max(1) as f64,
    );
    println!(
        "
  series grows, value flat => the series primitive is the cost.
  both grow                => the block-store append beneath them is.
  neither grows            => the cost is elsewhere in the command arm.
"
    );
}


/// What does post-write index maintenance cost, per key kind?
///
/// A summary write costs 1,554 allocations at 40 memories and 10,671 at 320; a node write is flat
/// at ~197. The append primitives underneath both are flat, no rebuild fires, and every counted
/// bucket walk is flat -- so the cost is in the maintenance that succeeds, not in the write and not
/// in a fallback. The two arms differ in which branch of `sync_context_pages_for_object` they take:
/// a node's page is filed under `shard.hashes` and goes through `upsert_bucket_index_page_with`, a
/// summary is a timestamped series and goes through `sync_bucket_index_object_pages`.
///
/// Calling maintenance directly, on the same grown stores, one key of each kind, decides it.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_post_write_maintenance_costs_per_key_kind -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_post_write_maintenance_costs_per_key_kind() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 8807;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  corpus   maintained key kind      allocs   covered
"
    );

    for rung in [40_usize, 320] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..rung {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: TENANT,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("MAINT-{index:06}"),
                        title: format!("maint {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "grow {index}: {:?}", report.status);
        }

        // Real keys, written through the real arms, so maintenance sees exactly what it sees in
        // production rather than a key that happens to be absent.
        let node_hash = 9_800_000 + rung as u64;
        let node_write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(crate::types::ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: format!("session/maint-{node_hash}"),
                    l0: "probe node".to_string(),
                    status: 0,
                    last_event_time_ms: 1_781_700_000_000,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: vec![0.25_f32; 16],
                    embedding_model_hash: 7,
                    embedding_updated_at_ms: 1,
                    summary_vector: vec![0.5_f32; 16],
                    summary_vector_valid_from_ms: 1_781_700_000_000,
                    summary_vector_model_hash: 7,
                }),
            },
        });
        assert!(node_write.status.ok, "{:?}", node_write.status);
        let summary_write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertSummary {
                tenant_hash: TENANT,
                summary: crate::types::ContextSummary {
                    node_hash,
                    level: 2,
                    text: "a probe summary of ordinary length for one turn".to_string(),
                    valid_from_ms: 1_781_700_000_000,
                    vector: vec![0.5_f32; 16],
                    embedding_model_hash: 7,
                },
            },
        });
        assert!(summary_write.status.ok, "{:?}", summary_write.status);

        let node_key = super::context::context_node_key(TENANT, node_hash);
        let summary_key = super::context::context_summary_key(TENANT, node_hash, 2);

        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        for (label, key) in [("context node   ", &node_key), ("context summary", &summary_key)] {
            // Warm once: the first maintenance of a key does one-off work.
            let _ = super::storage_bucket_internals::sync_context_pages_for_object(
                shard, 1, key,
            );
            let probe = crate::alloc_probe::Probe::start();
            let covered = super::storage_bucket_internals::sync_context_pages_for_object(
                shard, 1, key,
            );
            let allocs = probe.stop().allocs;
            println!("  {rung:>6}   {label}      {allocs:>9}   {covered}");
        }
    }

    println!(
        "
  summary rising while node stays flat => maintenance is the cost, and the fix belongs in the
  series branch of it. Both flat => maintenance is not the cost either, and what remains is the
  wrapper around the arm.
"
    );
}


/// The incrementally maintained object->page lookup must equal a rebuilt one.
///
/// `sync_bucket_index_object_pages` used to end by rebuilding the shard's entire object->page
/// lookup, which made every timestamped-series write O(pages in the shard) -- 10,457 allocations for
/// one summary write at 320 memories against 27 for a node write, which never reaches that path.
/// The rebuild is now confined to establishing an empty lookup, and the steady state applies only
/// the pages a write touched.
///
/// A rebuild is self-correcting: it cleared the lookup and refilled it from the buckets, so it
/// repaired any drift for free. Incremental maintenance does not, so the equivalence has to be
/// asserted rather than assumed. Rewrites matter as much as first writes: a rewrite drops this
/// object's entries and inserts new ones, and applying those two in the wrong order deletes what the
/// same call just wrote.
#[test]
fn the_maintained_page_lookup_matches_a_rebuilt_one() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 6203;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    for index in 0..24_usize {
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: TENANT,
                sources: vec![ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    source_kind: ContextSourceKind::Incident,
                    source_id: format!("LOOKUP-{index:06}"),
                    title: format!("lookup {index}"),
                    body: format!(
                        "{}{}",
                        format!("entry {index} covering the depot rota "),
                        "context payload sentence. ".repeat(12)
                    ),
                    timestamp_ms: 1_000 + index as u64,
                    provider: ContextModelProviderConfig::default(),
                }],
                provider: ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(report.status.ok, "ingest {index}: {:?}", report.status);
    }

    // Write the same summary key twice at different times, then a second key once: the first pair
    // exercises the drop-then-insert path, the single write the plain insert.
    for (node_hash, valid_from_ms) in [
        (7_100_001_u64, 1_781_700_000_000_u64),
        (7_100_001, 1_781_700_000_001),
        (7_100_002, 1_781_700_000_000),
    ] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertSummary {
                tenant_hash: TENANT,
                summary: crate::types::ContextSummary {
                    node_hash,
                    level: 2,
                    text: "a summary written to exercise lookup maintenance".to_string(),
                    valid_from_ms,
                    vector: vec![0.5_f32; 16],
                    embedding_model_hash: 7,
                },
            },
        });
        assert!(out.status.ok, "summary {node_hash}: {:?}", out.status);
    }

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("loaded shard");

    let maintained = shard.bucket_index.object_page_lookup.clone();
    let maintained_total = shard.bucket_index.object_component_page_refs;
    assert!(
        !maintained.is_empty(),
        "the lookup is empty, so this test would pass without maintaining anything"
    );

    shard.bucket_index.rebuild_object_page_lookup();
    let rebuilt = &shard.bucket_index.object_page_lookup;

    // Identity is (model, object) now rather than one concatenated key, so the comparison
    // names both halves instead of a composite.
    let missing: Vec<String> = rebuilt
        .iter()
        .filter(|(model, object, _)| maintained.get(model, object).is_none())
        .map(|(model, object, _)| format!("{model}/{object}"))
        .collect();
    let extra: Vec<String> = maintained
        .iter()
        .filter(|(model, object, _)| rebuilt.get(model, object).is_none())
        .map(|(model, object, _)| format!("{model}/{object}"))
        .collect();
    let differing: Vec<String> = rebuilt
        .iter()
        .filter(|(model, object, refs)| {
            maintained
                .get(model, object)
                .map(|held| held != *refs)
                .unwrap_or(false)
        })
        .map(|(model, object, _)| format!("{model}/{object}"))
        .collect();
    assert!(
        missing.is_empty() && extra.is_empty() && differing.is_empty(),
        "maintained lookup differs from a rebuilt one: {} missing {:?}, {} extra {:?}, {} differing {:?}",
        missing.len(),
        missing.iter().take(4).collect::<Vec<_>>(),
        extra.len(),
        extra.iter().take(4).collect::<Vec<_>>(),
        differing.len(),
        differing.iter().take(4).collect::<Vec<_>>(),
    );
    assert_eq!(
        maintained_total, shard.bucket_index.object_component_page_refs,
        "the page-ref total drifted; the stats path reads it instead of walking the shard"
    );
}

/// Which structures does one record actually land in?
///
/// A record occupies about six index entries' worth of memory
/// (`how_many_map_entries_is_one_record`), so the capacity ceiling moves by holding it in fewer
/// places. That is only actionable if the places are named. This writes a known number of
/// records and counts what each structure ends up holding.
///
/// Counts, not bytes: a structure holding one entry per record is a candidate whatever its entry
/// costs, and a structure holding none is not.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib where_a_record_is_kept -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn where_a_record_is_kept() {
    const RECORDS: usize = 5_000;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("kept-{index:08}"),
                value: vec![b'v'; 512],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");

    let rows: Vec<(&str, usize)> = vec![
        ("strings", shard.strings.len()),
        ("hashes", shard.hashes.len()),
        ("sets", shard.sets.len()),
        ("expires_at_ms", shard.expires_at_ms.len()),
        ("seen", shard.seen.len()),
        ("wal_resident_pages", shard.wal_resident_pages.len()),
        ("buckets_pending_flag_refresh", shard.buckets_pending_flag_refresh.len()),
    ];

    println!();
    println!("  {RECORDS} plain string writes land in:");
    println!("  structure                        entries   per record");
    let mut per_record_total = 0.0;
    for (name, count) in &rows {
        let per = *count as f64 / RECORDS as f64;
        per_record_total += per;
        println!("  {name:32} {count:>7} {per:>12.2}");
    }
    println!("  ------------------------------------------------------");
    println!("  {:32} {:>7} {per_record_total:>12.2}", "counted here", "");
    println!();
    println!("  A structure at 1.00 per record holds every record; one at 0.00 holds none of");
    println!("  them and is not where the memory goes. Anything the six entries' worth is made");
    println!("  of that does NOT appear above lives outside ShardState -- the bucket index and");
    println!("  the page-address maps beneath it are the next place to look.");

    assert_eq!(shard.strings.len(), RECORDS, "every write should be in strings");
}

/// How many map entries' worth of memory does one record actually occupy?
///
/// A record keeps ~1,040 bytes resident whatever kind it is
/// (`what_a_records_kind_costs_resident`), and that number sets a node's ceiling at roughly 20M
/// records on a 32 GB box. A record's primary home is one entry in `strings: HashMap<String,
/// BlockAddress>`, so this measures what ONE such entry costs on its own and divides.
///
/// A ratio near 1 would mean the entry is simply expensive and the only fix is a smaller entry.
/// A ratio well above 1 means the record is held in several places, and the fix is to hold it in
/// fewer.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib how_many_map_entries_is_one_record -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn how_many_map_entries_is_one_record() {
    use std::collections::HashMap;

    const RECORDS: u64 = 20_000;

    // A real address, produced by a real write, so the entry measured is the one the store keeps.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("floor-{index:08}"),
                value: vec![b'v'; 1536],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let address = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        shard.strings.values().next().cloned().expect("a written page")
    };

    // The floor: the same key and the same address, in a map of their own.
    let probe = crate::alloc_probe::Probe::start();
    let mut bare: HashMap<String, crate::block_store::BlockAddress> =
        HashMap::with_capacity(RECORDS as usize);
    for index in 0..RECORDS {
        bare.insert(format!("floor-{index:08}"), address.clone());
    }
    let counts = probe.stop();
    let bare_live = counts.alloc_bytes.saturating_sub(counts.free_bytes);
    std::hint::black_box(&bare);

    let per_entry = bare_live as f64 / RECORDS as f64;
    const RECORD_RESIDENT: f64 = 1040.0;   // measured by what_a_records_kind_costs_resident

    println!();
    println!("  one HashMap<String, BlockAddress> entry   {per_entry:>8.0} bytes");
    println!("  a record resident in the engine           {RECORD_RESIDENT:>8.0} bytes");
    println!("  ratio                                     {:>8.1}x",
             RECORD_RESIDENT / per_entry);
    println!();
    if RECORD_RESIDENT / per_entry > 1.6 {
        println!("  A record occupies several entries' worth. It is not that the entry is");
        println!("  expensive -- the record is held in more than one place, and the ceiling moves");
        println!("  by holding it in fewer rather than by shrinking any single structure.");
    } else {
        println!("  A record is about one entry. The entry itself is the cost, so the ceiling");
        println!("  moves only by making the key or the address smaller.");
    }

    assert!(bare_live > 0, "measured nothing");
}

/// How much of a record's resident cost is the core index entry, and how much is its kind?
///
/// A context node keeps ~1.34 kB resident and none of it is the payload
/// (`what_a_resident_record_is_made_of`) -- it is fixed index structure, which is the only thing
/// left that could move a node's ~20M-record ceiling. This asks which structure: a plain string
/// lands in the fewest maps a record can land in, so what a context node costs ABOVE it is what
/// the context indexes add.
///
/// Both write the same number of records with a payload of the same size, so the difference is
/// the indexing and not the data.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_records_kind_costs_resident -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_records_kind_costs_resident() {
    const RECORDS: u64 = 30_000;
    const PAYLOAD: usize = 1536;

    let engine_for = || {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        (dir, engine)
    };

    // --- a plain string: the fewest maps a record can land in ---------------------------
    let (_dir_a, engine_a) = engine_for();
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..RECORDS {
        let response = engine_a.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("kind-{index:08}"),
                value: vec![b'v'; PAYLOAD],
            },
        });
        assert!(response.status.ok, "string write {index}: {:?}", response.status);
    }
    let string_counts = probe.stop();
    let string_live = string_counts.alloc_bytes.saturating_sub(string_counts.free_bytes);

    // --- a context node: the same bytes, more indexes -----------------------------------
    let (_dir_b, engine_b) = engine_for();
    let vector: Vec<f32> = (0..PAYLOAD / 4).map(|i| (i as f32) * 0.001).collect();
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..RECORDS {
        let node = crate::types::ContextNode {
            node_hash: 1_000_000 + index,
            parent_hash: 0,
            kind: 1,
            canonical_name: format!("kind-{index:08}"),
            l0: String::new(),
            status: 0,
            last_event_time_ms: 1_780_000_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: vector.clone(),
            embedding_model_hash: 909,
            embedding_updated_at_ms: 1_780_000_000_000,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        };
        let response = engine_b.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode { tenant_hash: 7, node: Box::new(node) },
        });
        assert!(response.status.ok, "node write {index}: {:?}", response.status);
    }
    let node_counts = probe.stop();
    let node_live = node_counts.alloc_bytes.saturating_sub(node_counts.free_bytes);

    let per = |v: u64| v as f64 / RECORDS as f64;
    println!();
    println!("  record kind                 live/record   allocated/record   freed %");
    println!("  string ({PAYLOAD} B value)      {:>8.0} B  {:>15.0} B  {:>6.1}%",
             per(string_live), per(string_counts.alloc_bytes),
             100.0 * string_counts.free_bytes as f64 / string_counts.alloc_bytes as f64);
    println!("  context node ({PAYLOAD} B vec)  {:>8.0} B  {:>15.0} B  {:>6.1}%",
             per(node_live), per(node_counts.alloc_bytes),
             100.0 * node_counts.free_bytes as f64 / node_counts.alloc_bytes as f64);
    println!();
    let extra = per(node_live) - per(string_live);
    println!("  a context node keeps {extra:+.0} bytes more than a string carrying the same bytes");
    if extra > per(string_live) {
        println!("  -- the context indexes cost MORE than the core entry, so they are where the");
        println!("     ceiling is. Which of them is the next thing to split out.");
    } else {
        println!("  -- the core index entry dominates, so every record kind pays about the same");
        println!("     and the ceiling is not specific to context nodes.");
    }

    assert!(string_live > 0 && node_live > 0, "measured nothing");
}

/// What the 1.3 kB a record keeps resident is actually made of.
///
/// A node keeps 1.302 kB of live memory per record and RSS grows 1.586 kB
/// (`how_much_of_rss_per_record_is_live`), which is what limits a node to roughly 20M records on
/// a 32 GB box. Notably that is LESS than the 1,536-byte vector each record carries, so the
/// payload is not what is retained -- it goes to a page and the page is not held. What stays is
/// index structure, and this attributes it by writing the same corpus with one thing removed at
/// a time.
///
/// Differences, not absolutes: the baseline includes fixed costs that do not scale, and only
/// what separates two runs is per-record.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_resident_record_is_made_of -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_resident_record_is_made_of() {
    const RECORDS: u64 = 30_000;

    let live_for = |label: &str, dims: usize, name_len: usize, text_len: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let vector: Vec<f32> = (0..dims).map(|i| (i as f32) * 0.001).collect();

        let probe = crate::alloc_probe::Probe::start();
        for index in 0..RECORDS {
            let node = crate::types::ContextNode {
                node_hash: 1_000_000 + index,
                parent_hash: 0,
                kind: 1,
                canonical_name: format!("{:width$}", index, width = name_len),
                l0: "t".repeat(text_len),
                status: 0,
                last_event_time_ms: 1_780_000_000_000,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: vector.clone(),
                embedding_model_hash: 909,
                embedding_updated_at_ms: 1_780_000_000_000,
                summary_vector: Vec::new(),
                summary_vector_valid_from_ms: 0,
                summary_vector_model_hash: 0,
            };
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode { tenant_hash: 7, node: Box::new(node) },
            });
            assert!(response.status.ok, "{label} write {index}: {:?}", response.status);
        }
        let counts = probe.stop();
        let live = counts.alloc_bytes.saturating_sub(counts.free_bytes);
        println!("  {label:34} {:>9.3} kB/record live",
                 live as f64 / RECORDS as f64 / 1024.0);
        live
    };

    println!();
    println!("  variant                             live memory kept per record");
    let base = live_for("baseline: 384 dims, 40-char name", 384, 40, 42);
    let no_vector = live_for("no vector at all", 0, 40, 42);
    let short_name = live_for("4-char canonical name", 384, 4, 42);
    let no_text = live_for("no l0 text", 384, 40, 0);

    let per = |v: u64| v as f64 / RECORDS as f64;
    println!();
    println!("  removing            saves per record");
    println!("    the vector        {:>8.1} bytes", per(base) - per(no_vector));
    println!("    36 name chars     {:>8.1} bytes", per(base) - per(short_name));
    println!("    the l0 text       {:>8.1} bytes", per(base) - per(no_text));
    println!("    ---------------------------------");
    println!("    unattributed      {:>8.1} bytes",
             per(no_vector).min(per(short_name)).min(per(no_text)));
    println!();
    println!("  A vector costs 1,536 bytes on the wire. If removing it saves far less than that,");
    println!("  the resident cost is the INDEX ENTRY for the record, not its payload -- and the");
    println!("  capacity ceiling moves only by making that entry smaller.");

    assert!(base > 0, "measured nothing");
}

/// Of the RSS a node holds per record, how much is LIVE and how much is the allocator's?
///
/// A soak measured a marginal 1.561 kB of RSS per record, linear, which puts a 32 GB node out of
/// memory near 20M records. That figure only bounds capacity if it is real data. Process RSS
/// conflates three things -- memory allocated and still held, memory freed but retained by the
/// allocator, and memory that never came from the heap -- and a proxy measurement in this project
/// once found 71% of RSS was retention rather than live data.
///
/// The counting allocator knows the difference: allocated bytes minus freed bytes is what the
/// process is actually still holding. Comparing that against RSS says whether the capacity limit
/// is the data or the allocator, and those have completely different fixes.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib how_much_of_rss_per_record_is_live -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn how_much_of_rss_per_record_is_live() {
    fn rss_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|rest| rest.trim().split_whitespace().next()?.parse().ok())
                })
            })
            .unwrap_or(0)
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    const RECORDS: u64 = 60_000;
    let vector: Vec<f32> = (0..384).map(|i| (i as f32) * 0.001).collect();

    let rss_before = rss_kb();
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..RECORDS {
        let node = crate::types::ContextNode {
            node_hash: 1_000_000 + index,
            parent_hash: 0,
            kind: 1,
            canonical_name: format!("session/live-{index}"),
            l0: format!("memory {index}: the user prefers aisle seats"),
            status: 0,
            last_event_time_ms: 1_780_000_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: vector.clone(),
            embedding_model_hash: 909,
            embedding_updated_at_ms: 1_780_000_000_000,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: 7,
                node: Box::new(node),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let counts = probe.stop();
    let rss_after = rss_kb();

    let live_bytes = counts.alloc_bytes.saturating_sub(counts.free_bytes);
    let rss_bytes = (rss_after.saturating_sub(rss_before)) * 1024;
    let per = |v: u64| v as f64 / RECORDS as f64;

    println!();
    println!("  over {RECORDS} records:");
    println!("    allocated        {:>12} bytes   ({:.3} kB/record)",
             counts.alloc_bytes, per(counts.alloc_bytes) / 1024.0);
    println!("    freed            {:>12} bytes", counts.free_bytes);
    println!("    STILL LIVE       {:>12} bytes   ({:.3} kB/record)",
             live_bytes, per(live_bytes) / 1024.0);
    println!("    RSS grew by      {:>12} bytes   ({:.3} kB/record)",
             rss_bytes, per(rss_bytes) / 1024.0);
    println!();
    if rss_bytes > 0 {
        let retained = rss_bytes.saturating_sub(live_bytes);
        println!("    live is {:.0}% of the RSS growth; the other {:.0}% ({:.3} kB/record) is",
                 100.0 * live_bytes as f64 / rss_bytes as f64,
                 100.0 * retained as f64 / rss_bytes as f64,
                 per(retained) / 1024.0);
        println!("    memory the allocator kept rather than data the node holds.");
        println!();
        if retained > live_bytes {
            println!("    RETENTION DOMINATES: the capacity limit is the allocator, and an arena or a");
            println!("    periodic trim would move it further than shrinking any structure would.");
        } else {
            println!("    LIVE DATA DOMINATES: the per-record structures are the capacity limit, so");
            println!("    the only thing that moves it is holding less per record.");
        }
    }

    assert!(counts.alloc_bytes > 0, "measured nothing");
}

/// Is a put expensive because it STORES, or because it EVICTS?
///
/// Storing a 1 KB page measured 51 allocations and 43.6 KB
/// (`what_the_page_cache_costs_per_call`), which is a 43x amplification and too large to be the
/// copy. That measurement used a cache too small to hold the working set, so every put also
/// evicted. This separates the two: the same put into a cache with room to spare cannot be
/// evicting.
///
/// It matters which it is. If storing is expensive, every write to the cache costs it. If
/// EVICTING is expensive, only a cache under pressure pays -- which is every cache on a corpus
/// larger than it, but the fix is an admission policy rather than a cheaper store.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib is_a_put_expensive_to_store_or_to_evict -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn is_a_put_expensive_to_store_or_to_evict() {
    use matrixcache::CacheKey;

    const ROUNDS: u64 = 200;
    let per = |n: u64| n as f64 / ROUNDS as f64;

    let measure = |cache_bytes: usize, label: &str| {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            cache_bytes,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let cache = &engine.cache;

        // Keys and buffers built outside the window; `put` takes both by value.
        let mut pending: Vec<(CacheKey, Vec<u8>)> = (0..ROUNDS)
            .map(|round| {
                (
                    CacheKey::page_with_slot(1, 2, round * 4096, 1024, Some(3)),
                    vec![b'p'; 1024],
                )
            })
            .collect();
        // Warm: the first put touches one-off structures.
        if let Some((key, value)) = pending.pop() {
            let _ = cache.put(key, value);
        }

        let mut allocs = 0u64;
        let mut bytes = 0u64;
        let mut n = 0u64;
        for (key, value) in pending.drain(..) {
            let probe = crate::alloc_probe::Probe::start();
            let stored = cache.put(key, value);
            let counts = probe.stop();
            assert!(stored.is_ok(), "{label}: the put must succeed");
            allocs += counts.allocs;
            bytes += counts.alloc_bytes;
            n += 1;
        }
        let per_n = |v: u64| v as f64 / n as f64;
        println!("  {label:38} {:>7.2} {:>9.0}", per_n(allocs), per_n(bytes));
        (per_n(allocs), per_n(bytes))
    };

    println!();
    println!("  cache size                              allocs     bytes   (per 1 KB put)");
    // 200 pages of 1 KB need ~200 KB; 64 MB has room for all of them, 8 KB for none.
    let (roomy_allocs, roomy_bytes) = measure(64 * 1024 * 1024, "64 MB (room to spare, no eviction)");
    let (tight_allocs, tight_bytes) = measure(8 * 1024, "8 KB (evicts on every put)");

    println!();
    println!("  eviction adds {:+.2} allocations and {:+.0} bytes per put",
             tight_allocs - roomy_allocs, tight_bytes - roomy_bytes);
    if roomy_allocs > tight_allocs - roomy_allocs {
        println!("  STORING dominates: the cost is paid by every put, pressure or not.");
    } else {
        println!("  EVICTING dominates: a cache with room is cheap and one under pressure is not,");
        println!("  so the fix is to stop admitting pages that will be evicted before they are");
        println!("  read again, rather than to make the store itself cheaper.");
    }

    assert!(roomy_allocs > 0.0 && tight_allocs > 0.0, "measured nothing");
    let _ = (roomy_bytes, tight_bytes);
}

/// Does a missed lookup pay for tiers that hold nothing?
///
/// A cache miss costs 18 allocations (`what_the_page_cache_costs_per_call`), which contradicts
/// `get_with_tier`'s own claim that a miss "releases the lock having touched nothing". One
/// candidate: the lookup walks memory, then pmem, then SSD, and the SSD tier builds its key as a
/// String before asking -- so a tier with no capacity still costs something to consult.
///
/// `disk_cache_tier: false` zeroes the pmem and SSD capacities, which is what a one-box or raft
/// node runs with. If the miss gets cheaper, walking dead tiers is part of the 18.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib does_a_miss_pay_for_empty_tiers -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn does_a_miss_pay_for_empty_tiers() {
    use matrixcache::CacheKey;

    const ROUNDS: u64 = 200;
    let per = |n: u64| n as f64 / ROUNDS as f64;

    let measure = |disk_cache_tier: bool, label: &str| {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
            8 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
            Default::default(),
            disk_cache_tier,
        );
        engine.load_shard(1);
        let cache = &engine.cache;

        // Keys built outside the window: this measures the lookup, not the key.
        let absent: Vec<CacheKey> = (0..ROUNDS)
            .map(|round| CacheKey::page_with_slot(1, 3, 5_000_000 + round * 4096, 1024, Some(4)))
            .collect();
        // Warm whatever one-off structures the first lookup touches.
        let _ = cache.get(&absent[0]);

        let mut allocs = 0u64;
        let mut bytes = 0u64;
        for key in &absent {
            let probe = crate::alloc_probe::Probe::start();
            let got = cache.get(key);
            let counts = probe.stop();
            assert!(matches!(got, Ok(None)), "{label}: this key must be absent");
            allocs += counts.allocs;
            bytes += counts.alloc_bytes;
        }
        println!("  {label:34} {:>7.2} {:>8.0}", per(allocs), per(bytes));
        (allocs, bytes)
    };

    println!();
    println!("  cache shape                       allocs    bytes   (per missed lookup)");
    let (with_disk, _) = measure(true, "memory + pmem + ssd tiers");
    let (memory_only, _) = measure(false, "memory only (one-box / raft)");

    println!();
    let saved = per(with_disk) - per(memory_only);
    if saved > 0.5 {
        println!("  Walking the empty disk tiers costs {saved:.2} allocations per missed lookup.");
        println!("  A one-box or raft node has those tiers switched off already, so it does not");
        println!("  pay this -- but a shared-storage node consults them on every miss.");
    } else {
        println!("  The disk tiers are not the cost: a miss allocates about the same either way,");
        println!("  so the {:.0} allocations are in the memory-tier lookup itself.", per(memory_only));
    }

    assert!(with_disk > 0, "measured nothing");
    assert!(memory_only > 0, "measured nothing");
}

/// What the CACHE costs, split into the three calls a page-read miss makes.
///
/// A miss spends ~60 allocations and ~86 KB inside the cache against 13 allocations and 1.5 KB
/// actually reading the page (`where_a_page_read_miss_allocates`). That is the read path's real
/// memory cost, and 60 is too many to guess at: this splits it into building the key, the lookup
/// that misses, and the put that stores the page.
///
/// Keys and buffers are built BEFORE the measured window -- `put` takes both by value, and
/// counting their construction would attribute the caller's work to the cache.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_the_page_cache_costs_per_call -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_the_page_cache_costs_per_call() {
    use matrixcache::CacheKey;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let cache = &engine.cache;

    const ROUNDS: u64 = 100;
    let per = |n: u64| n as f64 / ROUNDS as f64;

    // --- building the key -----------------------------------------------------------------
    let mut key_allocs = 0u64;
    let mut key_bytes = 0u64;
    for round in 0..ROUNDS {
        let probe = crate::alloc_probe::Probe::start();
        let key = CacheKey::page_with_slot(1, 0, round * 4096, 1024, Some(7));
        let counts = probe.stop();
        std::hint::black_box(&key);
        key_allocs += counts.allocs;
        key_bytes += counts.alloc_bytes;
    }

    // --- a lookup that misses -------------------------------------------------------------
    let absent: Vec<CacheKey> = (0..ROUNDS)
        .map(|round| CacheKey::page_with_slot(1, 9, 1_000_000 + round * 4096, 1024, Some(7)))
        .collect();
    let mut miss_allocs = 0u64;
    let mut miss_bytes = 0u64;
    for key in &absent {
        let probe = crate::alloc_probe::Probe::start();
        let got = cache.get(key);
        let counts = probe.stop();
        assert!(matches!(got, Ok(None)), "this key must not be present");
        miss_allocs += counts.allocs;
        miss_bytes += counts.alloc_bytes;
    }

    // --- storing a page -------------------------------------------------------------------
    let mut pending: Vec<(CacheKey, Vec<u8>)> = (0..ROUNDS)
        .map(|round| {
            (
                CacheKey::page_with_slot(1, 0, round * 4096, 1024, Some(7)),
                vec![b'v'; 1024],
            )
        })
        .collect();
    let mut put_allocs = 0u64;
    let mut put_bytes = 0u64;
    for (key, value) in pending.drain(..) {
        let probe = crate::alloc_probe::Probe::start();
        let stored = cache.put(key, value);
        let counts = probe.stop();
        assert!(stored.is_ok(), "the put must succeed");
        put_allocs += counts.allocs;
        put_bytes += counts.alloc_bytes;
    }

    println!();
    println!("  call                     allocs   bytes    (1 KB page, cache too small to hold it)");
    println!("  CacheKey::page_with_slot {:>7.2} {:>7.0}", per(key_allocs), per(key_bytes));
    println!("  get (misses)             {:>7.2} {:>7.0}", per(miss_allocs), per(miss_bytes));
    println!("  put (stores + evicts)    {:>7.2} {:>7.0}", per(put_allocs), per(put_bytes));
    println!("  ------------------------------------------");
    println!("  sum                      {:>7.2} {:>7.0}",
             per(key_allocs) + per(miss_allocs) + per(put_allocs),
             per(key_bytes) + per(miss_bytes) + per(put_bytes));
    println!();
    println!("  The read path calls all three on every miss. Whichever dominates is where the");
    println!("  read path's memory goes, and a page nothing will ask for again pays it for");
    println!("  nothing.");

    assert!(key_allocs + miss_allocs + put_allocs > 0, "measured nothing");
}

/// Where the 38 allocations of a page-read MISS actually go.
///
/// A hit costs 2 allocations and 92 bytes; a miss costs 38 and 141 KB for a 1 KB page
/// (`what_a_page_read_costs_hit_against_miss`). On a corpus larger than the cache essentially
/// every read misses, so the miss path is the one a deployment runs. This splits it into the
/// three things it does -- the failed cache lookup, the block-store read, and the put that
/// caches the result -- because each implies a different fix and 38 is too many to guess at.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib where_a_page_read_miss_allocates -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn where_a_page_read_miss_allocates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("attrib-{index:03}"),
                value: vec![b'v'; 1024],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let addresses: Vec<_> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        shard.strings.values().cloned().collect()
    };
    assert!(addresses.len() >= 8, "need several pages: {}", addresses.len());
    let page_store = &engine.page_store;

    const ROUNDS: u64 = 100;
    let per = |n: u64| n as f64 / ROUNDS as f64;

    // The block-store read on its own: no cache involved at all.
    let mut store_allocs = 0u64;
    let mut store_bytes = 0u64;
    for round in 0..ROUNDS {
        let address = &addresses[round as usize % addresses.len()];
        let probe = crate::alloc_probe::Probe::start();
        let got = page_store.read(address);
        let counts = probe.stop();
        assert!(got.is_ok(), "the page must read back from the block store");
        store_allocs += counts.allocs;
        store_bytes += counts.alloc_bytes;
    }

    // The whole miss, for the total these parts have to add up to.
    let mut whole_allocs = 0u64;
    let mut whole_bytes = 0u64;
    for round in 0..ROUNDS {
        let address = &addresses[round as usize % addresses.len()];
        let probe = crate::alloc_probe::Probe::start();
        let got = super::read_page_bytes(&engine.cache, page_store, 1, address);
        let counts = probe.stop();
        assert!(got.is_some(), "the page must read back");
        whole_allocs += counts.allocs;
        whole_bytes += counts.alloc_bytes;
    }

    println!();
    println!("  part                          allocs/read   bytes/read");
    println!("  block-store read alone        {:>11.2} {:>12.0}",
             per(store_allocs), per(store_bytes));
    println!("  whole miss (read_page_bytes)  {:>11.2} {:>12.0}",
             per(whole_allocs), per(whole_bytes));
    println!("  ---------------------------------------------------------");
    println!("  cache lookup + put + key      {:>11.2} {:>12.0}",
             per(whole_allocs) - per(store_allocs), per(whole_bytes) - per(store_bytes));
    println!();
    if per(store_allocs) > per(whole_allocs) - per(store_allocs) {
        println!("  the BLOCK STORE dominates: the read itself is the cost, and caching it is cheap.");
    } else {
        println!("  the CACHE dominates: reading the page is cheap and storing it is not, so an");
        println!("  admission policy that declined to cache a page nothing will ask for again");
        println!("  would remove most of a miss.");
    }

    assert!(store_allocs > 0, "measured nothing on the block-store read");
    assert!(whole_allocs >= store_allocs,
            "the whole miss cannot cost less than the read it contains");
}

/// What a page read costs on a HIT against a MISS.
///
/// `read_page_shared` exists so the node fetch does not own a copy of the page, and on a cache
/// hit that is exactly what happens: `get_shared` hands back an `Arc<[u8]>`. Its miss path is
/// `read_page_bytes(..).map(Arc::from)`, and `Arc::<[u8]>::from(Vec<u8>)` allocates a second
/// buffer and copies the page into it before dropping the Vec -- so a miss pays MORE than the
/// owning read it delegates to, and the saving applies to hits only.
///
/// Whether that matters depends on the hit rate. A soak reading uniformly over 276k records
/// measured the oldest 5,000 records and the newest 5,000 at indistinguishable latency (0.856 vs
/// 0.864 ms), i.e. essentially always missing -- so on a corpus larger than the cache the miss
/// path is the common one.
///
/// The cache clear sits OUTSIDE the measured window, so what is counted is the read alone.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_page_read_costs_hit_against_miss -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_page_read_costs_hit_against_miss() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("page-read-{index:03}"),
                value: vec![b'v'; 1024],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let address = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        shard
            .strings
            .values()
            .next()
            .cloned()
            .expect("the writes above put at least one page in the index")
    };

    let cache = &engine.cache;
    let page_store = &engine.page_store;

    // Warm once: the first read of anything touches one-off structures that would otherwise be
    // counted against whichever arm ran first.
    let warm = super::read_page_shared(cache, page_store, 1, &address)
        .expect("the page reads back");
    assert!(!warm.is_empty(), "an empty page would make every number below meaningless");

    const ROUNDS: u64 = 100;

    let mut hit_allocs = 0u64;
    let mut hit_bytes = 0u64;
    for _ in 0..ROUNDS {
        let probe = crate::alloc_probe::Probe::start();
        let got = super::read_page_shared(cache, page_store, 1, &address);
        let counts = probe.stop();
        assert!(got.is_some(), "the page must read back on the hit path");
        hit_allocs += counts.allocs;
        hit_bytes += counts.alloc_bytes;
    }

    // Misses the way they actually happen: a cache too small to hold the working set, read
    // round-robin so each page has been evicted by the time it comes round again. Clearing the
    // whole cache instead would fold the cost of rebuilding its internals into the read.
    let small = TemporalEngine::with_local_dirs(
        8 * 1024,
        dir.path().join("cache-small"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    small.load_shard(1);
    let addresses: Vec<_> = {
        let shards = small.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        shard.strings.values().cloned().collect()
    };
    assert!(addresses.len() >= 8, "need several pages to cycle through: {}", addresses.len());
    let small_cache = &small.cache;
    let small_pages = &small.page_store;
    for address in &addresses {
        let _ = super::read_page_shared(small_cache, small_pages, 1, address);
    }

    let mut miss_allocs = 0u64;
    let mut miss_bytes = 0u64;
    for round in 0..ROUNDS {
        let address = &addresses[round as usize % addresses.len()];
        let probe = crate::alloc_probe::Probe::start();
        let got = super::read_page_shared(small_cache, small_pages, 1, address);
        let counts = probe.stop();
        assert!(got.is_some(), "the page must read back on the miss path");
        miss_allocs += counts.allocs;
        miss_bytes += counts.alloc_bytes;
    }

    // And the owning read on a miss, for the comparison that matters: the shared read is
    // supposed to be the cheaper of the two.
    let mut owning_allocs = 0u64;
    let mut owning_bytes = 0u64;
    for round in 0..ROUNDS {
        let address = &addresses[round as usize % addresses.len()];
        let probe = crate::alloc_probe::Probe::start();
        let got = super::read_page_bytes(small_cache, small_pages, 1, address);
        let counts = probe.stop();
        assert!(got.is_some(), "the page must read back");
        owning_allocs += counts.allocs;
        owning_bytes += counts.alloc_bytes;
    }

    let per = |n: u64| n as f64 / ROUNDS as f64;
    println!();
    println!("  path                       allocs/read   bytes/read");
    println!("  shared, cache hit          {:>11.2} {:>12.0}", per(hit_allocs), per(hit_bytes));
    println!("  shared, cache miss         {:>11.2} {:>12.0}", per(miss_allocs), per(miss_bytes));
    println!("  owning (read_page_bytes)   {:>11.2} {:>12.0}", per(owning_allocs), per(owning_bytes));
    println!();
    println!(
        "  a miss costs {:+.2} allocations and {:+.0} bytes against a hit",
        per(miss_allocs) - per(hit_allocs),
        per(miss_bytes) - per(hit_bytes)
    );
    println!(
        "  the shared read costs {:+.2} allocations and {:+.0} bytes against the OWNING read it",
        per(miss_allocs) - per(owning_allocs),
        per(miss_bytes) - per(owning_bytes)
    );
    println!("  delegates to on a miss -- the Arc::from is the difference. Positive means the");
    println!("  zero-copy read is the more expensive one whenever the page is not already cached.");

    assert!(hit_allocs > 0, "measured nothing on the hit path");
    assert!(
        miss_allocs > hit_allocs,
        "a miss must cost more than a hit, or the cache was not actually cleared"
    );
}



/// Reading the page, or decoding it: which half of a node fetch costs the 15 allocations?
///
/// Retrieve is 16.4 allocations per candidate and the node fetch is 15.0 of them -- the largest
/// remaining cost on any indicator once ingest went flat. `load_context_node` is a map lookup, then
/// `read_page_bytes`, then `context_from_bytes`. The lookup cannot be 15, so it is one of the other
/// two, and each implies a different fix: a read that dominates asks for buffer reuse, a decode that
/// dominates asks about the record's shape -- four Strings and two vectors per node, built whether
/// or not the scoring pass reads them.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_the_two_halves_of_a_node_fetch_cost -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_the_two_halves_of_a_node_fetch_cost() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 5501;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..80_usize {
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: TENANT,
                sources: vec![ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    source_kind: ContextSourceKind::Incident,
                    source_id: format!("FETCH-{index:06}"),
                    title: format!("fetch {index}"),
                    body: format!(
                        "{}{}",
                        format!("entry {index} covering the depot rota "),
                        "context payload sentence. ".repeat(40)
                    ),
                    timestamp_ms: 1_000 + index as u64,
                    provider: ContextModelProviderConfig::default(),
                }],
                provider: ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(report.status.ok, "grow {index}: {:?}", report.status);
    }

    // A node that exists, found the way retrieval finds one.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");
    let (object_key, address) = shard
        .hashes
        .iter()
        .find_map(|(key, fields)| {
            fields
                .get(super::constants::CONTEXT_NODE_FIELD)
                .map(|address| (key.clone(), address.clone()))
        })
        .expect("the ingest wrote at least one context node page");

    let cache = &engine.cache;
    let page_store = &engine.page_store;

    // Warm: the first read of a page touches one-off structures.
    let warm = super::read_page_bytes(cache, page_store, 1, &address)
        .expect("the page reads back");
    assert!(!warm.is_empty(), "an empty page would make every number below meaningless");

    let probe = crate::alloc_probe::Probe::start();
    let bytes = super::read_page_bytes(cache, page_store, 1, &address)
        .expect("the page reads back");
    let read_allocs = probe.stop().allocs;

    let probe = crate::alloc_probe::Probe::start();
    let node = super::context::context_from_bytes::<crate::types::ContextNode>(&bytes)
        .expect("the page decodes to a node");
    let decode_allocs = probe.stop().allocs;

    println!(
        "
  half                         allocs
  read_page_bytes           {read_allocs:>9}
  context_from_bytes        {decode_allocs:>9}
  ------------------------------------
  together                  {:>9}   (a whole fetch measures ~15 per candidate)
",
        read_allocs + decode_allocs,
    );

    println!(
        "  what the decode built, and what scoring actually reads:
    canonical_name    {:>5} bytes
    l0                {:>5} bytes   <- the discarded text
    l1_ref            {:>5} bytes
    raw_metadata_ref  {:>5} bytes
    vector            {:>5} floats
    summary_vector    {:>5} floats  <- the one scoring reads
",
        node.canonical_name.len(),
        node.l0.len(),
        node.l1_ref.len(),
        node.raw_metadata_ref.len(),
        node.vector.len(),
        node.summary_vector.len(),
    );
    println!(
        "  read >> decode => reuse the read buffer.
  decode >> read => the record's shape is the cost, and a projection that skips fields scoring
                    never reads is worth its complexity.
"
    );
}


/// Every context object key, byte for byte.
///
/// These keys are stored in the shard's model maps and in the bucket index, and every read compares
/// one against keys already written. Building them directly instead of through `format!` saves an
/// allocation each -- `format!` costs two, the String it returns plus one inside the formatting
/// machinery -- but only if the bytes do not move. A changed byte here does not fail anything on its
/// own: it makes existing records unfindable, which reads as data loss.
#[test]
fn context_object_keys_keep_their_exact_bytes() {
    use super::context::*;

    assert_eq!(context_node_key(81, 4242), "ctx:node:81:4242");
    assert_eq!(context_event_key(81, 4242), "ctx:event:81:4242");
    assert_eq!(context_audit_key(81, 4242), "ctx:audit:81:4242");
    assert_eq!(context_dirty_key(81, 4242), "ctx:dirty:81:4242");
    assert_eq!(context_embedding_dirty_key(81, 4242), "ctx:embdirty:81:4242");
    assert_eq!(context_entity_key(81, 4242, 7), "ctx:entity:81:4242:7");
    assert_eq!(context_entity_collection_key(81, 4242), "ctx:entity:81:4242");
    assert_eq!(context_child_key(81, 4242), "ctx:child:81:4242");
    assert_eq!(context_summary_key(81, 4242, 2), "ctx:summary:81:4242:2");
    assert_eq!(context_compression_key(81, 4242), "ctx:compress:81:4242");

    // Zero and the largest u64 both round-trip: a key builder that sized its buffer for typical
    // values would truncate here, and truncation is exactly the silent kind of wrong.
    assert_eq!(context_node_key(0, 0), "ctx:node:0:0");
    assert_eq!(
        context_node_key(u64::MAX, u64::MAX),
        "ctx:node:18446744073709551615:18446744073709551615"
    );
    assert_eq!(
        context_summary_key(u64::MAX, u64::MAX, u32::MAX),
        "ctx:summary:18446744073709551615:18446744073709551615:4294967295"
    );

    // An entity key and its collection key share a prefix; the third part is what separates them,
    // so one must never be mistaken for the other.
    assert_ne!(context_entity_key(81, 4242, 7), context_entity_collection_key(81, 4242));

    // Distinct inputs stay distinct across the colon: (1, 23) and (12, 3) must not collide.
    assert_ne!(context_node_key(1, 23), context_node_key(12, 3));
}


/// How much resident memory is the allocator holding rather than the store?
///
/// A proxy measured 444.9 MB resident against 128.6 MB of live data. The only `#[global_allocator]`
/// in this tree is the test-only counting probe, so production runs on glibc malloc with no tuning:
/// freed chunks sit in per-thread arenas and go back to the OS only when the heap top is free or
/// `malloc_trim` is called. Nothing here calls it and nothing caps arenas.
///
/// This churns allocations in the shape the decode path produces, drops them, and reads RSS three
/// times:
///
///   after the churn            -> what the work cost
///   after dropping everything  -> what the allocator kept
///   after `malloc_trim(0)`     -> what it returns when asked
///
/// RSS flat on the drop and falling on the trim means the retention is the allocator's, and a trim
/// on an existing maintenance path recovers it. RSS falling on the drop means there is nothing to
/// fix here.
///
///   cargo test -p temporalstore-rust --lib how_much_resident_memory_is_the_allocator_holding -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn how_much_resident_memory_is_the_allocator_holding() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    extern "C" {
        /// glibc: release free heap above `pad` back to the OS. Returns non-zero if it freed any.
        fn malloc_trim(pad: usize) -> i32;
    }

    fn resident_kb() -> u64 {
        // The same source `wal.rs` reads. Zero means unreadable, which the assertions below catch
        // rather than quietly reporting a 0 KB result as an improvement.
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|rest| rest.split_whitespace().next().map(str::to_string))
                })
            })
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    let baseline = resident_kb();
    assert!(baseline > 0, "could not read VmRSS, so every number below would be fiction");

    // The shapes a decode produces, at a size worth measuring: byte buffers off pages, the strings
    // a record carries, and the float vectors. Held all at once, then dropped all at once.
    {
        let mut held: Vec<(Vec<u8>, String, Vec<f32>)> = Vec::with_capacity(20_000);
        for index in 0..20_000_u32 {
            held.push((
                vec![(index % 251) as u8; 512],
                format!("ctx:node:{index}:{index} a canonical name and some discarded text"),
                vec![index as f32 / 1024.0; 384],
            ));
        }
        let churned = resident_kb();
        println!(
            "
  resident memory, KB
    baseline                    {baseline:>9}
    holding 20,000 records      {churned:>9}   (+{} KB)",
            churned.saturating_sub(baseline),
        );
        assert!(
            churned > baseline,
            "holding 20,000 records did not raise RSS, so this probe is not measuring anything"
        );
    }

    let dropped = resident_kb();
    println!("    after dropping them all     {dropped:>9}   (+{} KB over baseline)",
        dropped.saturating_sub(baseline));

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        let returned = unsafe { malloc_trim(0) };
        let trimmed = resident_kb();
        println!(
            "    after malloc_trim(0)        {trimmed:>9}   (+{} KB over baseline, trim returned {returned})
",
            trimmed.saturating_sub(baseline),
        );
        let kept_after_drop = dropped.saturating_sub(baseline);
        let kept_after_trim = trimmed.saturating_sub(baseline);
        println!(
            "  the allocator kept {kept_after_drop} KB after the drop and {kept_after_trim} KB after the trim:
  a large fall here is memory a proxy could return on any maintenance tick, for one call.
"
        );
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    println!("    malloc_trim: not glibc on this target, so this half did not run\n");
}


/// Where do the decode's six allocations go?
///
/// A node fetch is ten allocations per retrieval candidate: ~3 building the cache key, 6 decoding
/// the record, ~1 on the lookups. The key strings are a floor while `CacheKey` keeps its serialized
/// shape. The six are the open question, and "the record's shape" is an assumption until counted.
///
/// Decoding the same record with one field emptied at a time names each allocation: what the count
/// drops by is what that field costs. What does not move is not where the cost is.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_each_field_of_a_node_costs_to_decode -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_each_field_of_a_node_costs_to_decode() {
    use crate::types::{ContextNode, ContextWire};

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    let full = |mutate: fn(&mut ContextNode)| {
        let mut node = ContextNode {
            node_hash: 4242,
            parent_hash: 7,
            kind: 1,
            canonical_name: "session/a-canonical-name-of-ordinary-length".to_string(),
            l0: "the discarded text: a sentence of the length an extract actually produces, which \
                 the scoring pass never reads and the pack only needs for the winners"
                .to_string(),
            status: 0,
            last_event_time_ms: 1_781_700_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: (0..384).map(|index| index as f32 / 1024.0).collect(),
            embedding_model_hash: 7,
            embedding_updated_at_ms: 1,
            summary_vector: (0..384).map(|index| index as f32 / 512.0).collect(),
            summary_vector_valid_from_ms: 1_781_700_000_000,
            summary_vector_model_hash: 7,
        };
        mutate(&mut node);
        node.encode_context_value()
    };

    let measure = |bytes: &[u8]| {
        // Warm: the first decode of a shape touches one-off structures.
        let _ = ContextNode::decode_context_value(bytes).expect("decodes");
        let probe = crate::alloc_probe::Probe::start();
        let node = ContextNode::decode_context_value(bytes).expect("decodes");
        let counts = probe.stop();
        (counts.allocs, counts.alloc_bytes, node)
    };

    let (base, base_bytes, decoded) = measure(&full(|_| {}));
    assert!(
        !decoded.vector.is_empty() && !decoded.canonical_name.is_empty(),
        "the fixture must actually carry the fields whose cost is being attributed"
    );

    println!(
        "
  what a whole node decodes to: {base} allocations, {base_bytes} bytes

  field emptied            allocs   saved   bytes saved
"
    );

    for (label, mutate) in [
        ("canonical_name  ", (|node: &mut ContextNode| node.canonical_name.clear()) as fn(&mut ContextNode)),
        ("l0 (the text)   ", |node: &mut ContextNode| node.l0.clear()),
        ("vector          ", |node: &mut ContextNode| node.vector.clear()),
        ("summary_vector  ", |node: &mut ContextNode| node.summary_vector.clear()),
    ] {
        let (allocs, bytes, _) = measure(&full(mutate));
        println!(
            "  {label}       {allocs:>6}   {:>5}   {:>11}",
            base.saturating_sub(allocs),
            base_bytes.saturating_sub(bytes),
        );
    }

    // The marker case: a summary vector equal to the node's own is written once and cloned on the
    // way out, so it costs an allocation that no field of the encoding pays for.
    let same = full(|node| node.summary_vector = node.vector.clone());
    let (same_allocs, _, same_node) = measure(&same);
    assert_eq!(
        same_node.summary_vector, same_node.vector,
        "the marker must still resolve to the node's own vector"
    );
    println!(
        "
  summary vector marked as the node's own: {same_allocs} allocations ({} vs a distinct one)
  -- the clone the marker resolves to is an allocation the wire never carries.
",
        if same_allocs >= base { format!("+{}", same_allocs - base) } else { format!("-{}", base - same_allocs) },
    );
}


/// A sized vector holds exactly what a grown one held.
///
/// `unpack_scaled_vector` now counts the varints and reserves before filling, instead of growing
/// from empty -- eight reallocations at 384 dimensions, on the default storage form. That is only
/// safe if the count agrees with the decode: too small and it grows anyway, too large and the slack
/// is held for as long as the node is cached, and wrong in a way that stops early loses values with
/// nothing to show for it.
///
/// Asserted across widths, and at the two edges where the counting walk and the decoding walk could
/// disagree about where to stop: nothing to decode, and bytes that end mid-value.
#[test]
fn a_sized_vector_decodes_exactly_what_a_grown_one_did() {
    use crate::types::{ContextNode, ContextWire};

    for width in [0_usize, 1, 16, 384, 1024] {
        let vector: Vec<f32> = (0..width).map(|index| (index as f32) / 97.0 - 3.0).collect();
        let node = ContextNode {
            node_hash: 11,
            parent_hash: 0,
            kind: 1,
            canonical_name: "session/sized".to_string(),
            l0: "text".to_string(),
            status: 0,
            last_event_time_ms: 1_781_700_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: vector.clone(),
            embedding_model_hash: 7,
            embedding_updated_at_ms: 1,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        };
        let decoded = ContextNode::decode_context_value(&node.encode_context_value())
            .expect("a node round-trips");
        assert_eq!(decoded.vector.len(), width, "width {width}: length");
        // The stored form rounds, so the values are compared as the encoding can represent them
        // rather than bit for bit -- what must not move is which values are there and in what order.
        for (index, (before, after)) in vector.iter().zip(decoded.vector.iter()).enumerate() {
            assert!(
                (before - after).abs() < 0.01,
                "width {width}, position {index}: {before} decoded as {after}"
            );
        }
        assert_eq!(
            decoded.vector.capacity(),
            decoded.vector.len(),
            "width {width}: the vector must be sized exactly, or the slack is held while the node is \
             cached"
        );
    }

    // Bytes that end mid-value: the counting walk and the decoding walk must stop in the same
    // place, or the reserve and the fill disagree.
    let node = ContextNode {
        node_hash: 12,
        parent_hash: 0,
        kind: 1,
        canonical_name: String::new(),
        l0: String::new(),
        status: 0,
        last_event_time_ms: 1,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: (0..64).map(|index| index as f32).collect(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    };
    let encoded = node.encode_context_value();
    for cut in [1_usize, encoded.len() / 3, encoded.len() / 2, encoded.len() - 1] {
        // Whatever it makes of a truncated record, it must not panic and must not claim more values
        // than it wrote.
        if let Some(decoded) = ContextNode::decode_context_value(&encoded[..cut]) {
            assert!(
                decoded.vector.len() <= 64,
                "a truncated record decoded {} values out of at most 64",
                decoded.vector.len()
            );
        }
    }
}


/// What does encoding a record cost?
///
/// `update` is the largest per-call cost of any api -- 192 allocations, flat in the corpus, so the
/// cost is the call and not the store. A write encodes the record first, and every
/// `encode_context_proto_value` starts from `Vec::new()`: the shape that made a decode eighteen
/// allocations instead of four.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_encoding_a_record_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_encoding_a_record_costs() {
    use crate::types::{ContextNode, ContextSummary, ContextWire};

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    let node_with = |mutate: fn(&mut ContextNode)| {
        let mut node = ContextNode {
            node_hash: 4242,
            parent_hash: 7,
            kind: 1,
            canonical_name: "session/a-canonical-name-of-ordinary-length".to_string(),
            l0: "the text an extract actually produces, long enough that its length is not noise \
                 next to the vectors it sits beside"
                .to_string(),
            status: 0,
            last_event_time_ms: 1_781_700_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: (0..384).map(|index| index as f32 / 1024.0).collect(),
            embedding_model_hash: 7,
            embedding_updated_at_ms: 1,
            summary_vector: (0..384).map(|index| index as f32 / 512.0).collect(),
            summary_vector_valid_from_ms: 1_781_700_000_000,
            summary_vector_model_hash: 7,
        };
        mutate(&mut node);
        node
    };

    let measure = |node: &ContextNode| {
        let warm = node.encode_context_value();
        assert!(!warm.is_empty(), "an empty encoding would make this vacuous");
        let probe = crate::alloc_probe::Probe::start();
        let encoded = node.encode_context_value();
        let counts = probe.stop();
        (counts.allocs, counts.alloc_bytes, encoded.len())
    };

    let (base, base_bytes, encoded_len) = measure(&node_with(|_| {}));
    println!(
        "
  encoding one node: {base} allocations, {base_bytes} bytes allocated, {encoded_len} bytes produced

  field emptied            allocs   saved
"
    );
    for (label, mutate) in [
        ("canonical_name  ", (|node: &mut ContextNode| node.canonical_name.clear()) as fn(&mut ContextNode)),
        ("l0 (the text)   ", |node: &mut ContextNode| node.l0.clear()),
        ("vector          ", |node: &mut ContextNode| node.vector.clear()),
        ("summary_vector  ", |node: &mut ContextNode| node.summary_vector.clear()),
    ] {
        let (allocs, _, _) = measure(&node_with(mutate));
        println!("  {label}       {allocs:>6}   {:>5}", base.saturating_sub(allocs));
    }

    // The other record an ingest writes, at the same vector width.
    let summary = ContextSummary {
        node_hash: 4242,
        level: 2,
        text: "a summary of ordinary length for one turn".to_string(),
        valid_from_ms: 1_781_700_000_000,
        vector: (0..384).map(|index| index as f32 / 768.0).collect(),
        embedding_model_hash: 7,
    };
    let warm = summary.encode_context_value();
    assert!(!warm.is_empty());
    let probe = crate::alloc_probe::Probe::start();
    let encoded = summary.encode_context_value();
    let counts = probe.stop();
    println!(
        "
  encoding one summary: {} allocations, {} bytes produced
",
        counts.allocs,
        encoded.len(),
    );
}


/// Where do a node write's 188 allocations go?
///
/// `update` is the largest per-call cost of any api and it is flat in the corpus, so the cost is the
/// call, not the store. Encoding is 9 of it, the value append 8, the post-write maintenance 27 --
/// about a quarter. "The write path" is not an answer for the rest.
///
/// The read path is the precedent: a decode was assumed to cost its fields and turned out to be
/// sixteen eighteenths two vectors grown from empty.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_node_write_spends_its_allocations_on -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_a_node_write_spends_its_allocations_on() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 7411;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..40_usize {
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: TENANT,
                sources: vec![ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    source_kind: ContextSourceKind::Incident,
                    source_id: format!("WRITE-{index:06}"),
                    title: format!("write {index}"),
                    body: format!(
                        "{}{}",
                        format!("entry {index} covering the depot rota "),
                        "context payload sentence. ".repeat(40)
                    ),
                    timestamp_ms: 1_000 + index as u64,
                    provider: ContextModelProviderConfig::default(),
                }],
                provider: ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(report.status.ok, "grow {index}: {:?}", report.status);
    }

    let node = |hash: u64, vector_width: usize, text: &str| crate::types::ContextNode {
        node_hash: hash,
        parent_hash: 0,
        kind: 1,
        canonical_name: format!("session/write-{hash}"),
        l0: text.to_string(),
        status: 0,
        last_event_time_ms: 1_781_700_000_000,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: (0..vector_width).map(|index| index as f32 / 1024.0).collect(),
        embedding_model_hash: 7,
        embedding_updated_at_ms: 1,
        summary_vector: (0..vector_width).map(|index| index as f32 / 512.0).collect(),
        summary_vector_valid_from_ms: 1_781_700_000_000,
        summary_vector_model_hash: 7,
    };

    let long_text = "the text an extract produces, long enough that its length is not noise";
    let mut tag = 9_000_000_u64;
    let mut measure = |width: usize, text: &str| {
        tag += 2;
        // Warm on one key, measure another: the first write of a shape touches one-off structures.
        let warm = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode { tenant_hash: TENANT, node: Box::new(node(tag, width, text)) },
        });
        assert!(warm.status.ok, "{:?}", warm.status);
        let probe = crate::alloc_probe::Probe::start();
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(node(tag + 1, width, text)),
            },
        });
        let counts = probe.stop();
        assert!(out.status.ok, "{:?}", out.status);
        (counts.allocs, counts.alloc_bytes)
    };

    let (full, full_bytes) = measure(384, long_text);
    let (no_vectors, _) = measure(0, long_text);
    let (no_text, _) = measure(384, "");
    let (neither, _) = measure(0, "");

    println!(
        "
  a whole node write: {full} allocations, {full_bytes} bytes

  written without            allocs   saved
    both vectors            {no_vectors:>8}   {:>5}
    the text                {no_text:>8}   {:>5}
    both                    {neither:>8}   {:>5}

  what stays with everything stripped is the write path itself: the append, the staging, the
  index maintenance and the log record. That floor is the thing worth attacking, and it is
  {neither} of {full}.
",
        full.saturating_sub(no_vectors),
        full.saturating_sub(no_text),
        full.saturating_sub(neither),
    );

    // A second write of the SAME record: the change-detecting paths should short-circuit, so what
    // remains is what a write costs even when it changes nothing.
    let repeat = node(tag + 1, 384, long_text);
    let warm = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode { tenant_hash: TENANT, node: Box::new(repeat.clone()) },
    });
    assert!(warm.status.ok, "{:?}", warm.status);
    let probe = crate::alloc_probe::Probe::start();
    let out = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode { tenant_hash: TENANT, node: Box::new(repeat) },
    });
    let unchanged = probe.stop().allocs;
    assert!(out.status.ok, "{:?}", out.status);
    println!(
        "  rewriting a record byte-for-byte identical: {unchanged} allocations
  -- if that is close to {full}, the write does the same work whether or not anything changed.
"
    );
}


/// What does capturing a write's key states cost?
///
/// A node write is 155 allocations and 144 of them are machinery: stripping both vectors and the
/// text saves 11. `capture_key_states` runs on every write and builds a `serde_json::json!` object
/// per touched key -- fourteen field lookups, each with a string key and a serialised value.
///
/// JSON on a write path is worth a number rather than a suspicion.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_capturing_a_writes_key_states_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_capturing_a_writes_key_states_costs() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 7717;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut node_hash = 0_u64;
    for index in 0..40_usize {
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: TENANT,
                sources: vec![ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    source_kind: ContextSourceKind::Incident,
                    source_id: format!("KS-{index:06}"),
                    title: format!("ks {index}"),
                    body: format!(
                        "{}{}",
                        format!("entry {index} covering the depot rota "),
                        "context payload sentence. ".repeat(40)
                    ),
                    timestamp_ms: 1_000 + index as u64,
                    provider: ContextModelProviderConfig::default(),
                }],
                provider: ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(report.status.ok, "grow {index}: {:?}", report.status);
        if let Some(first) = report.node_hashes.first() {
            node_hash = *first;
        }
    }
    assert!(node_hash != 0, "the ingest reported no node, so there is no real key to capture");

    let key = super::context::context_node_key(TENANT, node_hash);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");

    let keys = vec![key.clone()];
    // Warm: the first call of a shape touches one-off structures.
    let warm = super::capture_key_states(shard, &keys);
    assert_eq!(warm.len(), 1, "one key in, one blob out");
    assert!(
        warm[0].get("context_nodes").is_some() || warm[0].get("key").is_some(),
        "the blob must actually describe the key, or this measures an empty object"
    );

    let probe = crate::alloc_probe::Probe::start();
    let states = super::capture_key_states(shard, &keys);
    let counts = probe.stop();
    assert_eq!(states.len(), 1);

    // What it costs to then serialise that blob, which is what the delta record does with it.
    let probe = crate::alloc_probe::Probe::start();
    let encoded = serde_json::to_vec(&states).expect("the blob serialises");
    let encode_counts = probe.stop();

    println!(
        "
  capturing one key's state: {} allocations, {} bytes
  serialising it:            {} allocations, {} bytes produced

  a whole node write is 155 allocations, of which 144 are machinery rather than the record.
  this piece is {} of that machinery, and it runs once per touched key on every write.
",
        counts.allocs,
        counts.alloc_bytes,
        encode_counts.allocs,
        encoded.len(),
        counts.allocs,
    );
}


/// Does a cached node carry its own vector twice?
///
/// A decoded node at 384 dimensions is 3,264 bytes and 3,072 of those are its two vectors. The wire
/// already knows the two are often identical -- the writer marks it instead of encoding the vector
/// twice -- and the decoder resolves the marker with a clone.
///
/// A clone is cheap to make and expensive to keep: these records live in the shard's maps for as
/// long as they are cached. If the two are equal in practice, every cached node holds 1.5 KB of
/// duplicate at 384 dimensions for nothing.
///
///   cargo test -p temporalstore-rust --lib does_a_cached_node_carry_its_vector_twice -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn does_a_cached_node_carry_its_vector_twice() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 8123;
    const CORPUS: usize = 60;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let mut hashes = Vec::new();
    for index in 0..CORPUS {
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: TENANT,
                sources: vec![ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    source_kind: ContextSourceKind::Incident,
                    source_id: format!("DUP-{index:06}"),
                    title: format!("dup {index}"),
                    body: format!(
                        "{}{}",
                        format!("entry {index} covering the depot rota "),
                        "context payload sentence. ".repeat(40)
                    ),
                    timestamp_ms: 1_000 + index as u64,
                    provider: ContextModelProviderConfig::default(),
                }],
                provider: ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(report.status.ok, "ingest {index}: {:?}", report.status);
        hashes.extend(report.node_hashes.iter().copied());
    }
    assert!(!hashes.is_empty(), "the ingest reported no nodes, so there is nothing to inspect");

    let mut both_present = 0_usize;
    let mut identical = 0_usize;
    let mut summary_empty = 0_usize;
    let mut vector_bytes = 0_usize;
    let mut duplicate_bytes = 0_usize;
    let mut width = 0_usize;

    for hash in &hashes {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNode { tenant_hash: TENANT, node_hash: *hash },
        });
        let CommandResponse::ContextNode { node: Some(node), .. } = out.response else {
            continue;
        };
        width = width.max(node.vector.len());
        vector_bytes += node.vector.len() * 4 + node.summary_vector.len() * 4;
        if node.summary_vector.is_empty() {
            summary_empty += 1;
        } else {
            both_present += 1;
            if node.summary_vector == node.vector {
                identical += 1;
                duplicate_bytes += node.summary_vector.len() * 4;
            }
        }
    }

    let inspected = hashes.len();
    println!(
        "
  nodes inspected                     {inspected}
  vector width                        {width}
  carrying both vectors               {both_present}
    of which byte-identical           {identical}
  carrying no summary vector          {summary_empty}

  vector bytes held across the corpus {vector_bytes}
  of which duplicate                  {duplicate_bytes}
"
    );
    if identical > 0 {
        println!(
            "  {identical} of {both_present} nodes hold the same vector twice: {} bytes each, {duplicate_bytes} across
  this corpus. These records live in the shard's maps while cached, so that is resident memory
  spent on a copy the wire deliberately did not make.
",
            duplicate_bytes / identical.max(1),
        );
    } else {
        println!(
            "  no node holds a duplicate: the two vectors differ wherever both are present, so the
  clone the decoder makes is carrying a distinct value and there is nothing to reclaim here.
"
        );
    }
}


/// Which piece of the write machinery holds the unaccounted allocations?
///
/// A node write is 155 allocations, 144 of them machinery rather than the record. Encoding is 9,
/// the value append 8, post-write maintenance 27, the key-state capture 4 -- 48 accounted. The
/// remaining hundred has been called "the WAL and the log" and never counted.
///
/// It matters beyond its own number: an add is ~1,342 allocations and roughly eight writes, so
/// per-write machinery carries about 8x leverage on the largest api there is.
///
/// Switching a piece off and re-measuring attributes it without instrumenting internals. A flag
/// that changes nothing eliminates its piece; one that moves the count names it.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib which_piece_of_the_write_machinery_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn which_piece_of_the_write_machinery_costs() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 7919;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    // One write, measured on a store grown by real ingests, with whatever flags are set when it
    // runs. Each arm builds its own store so a flag cannot be observed through state another arm
    // left behind.
    fn one_write_costs(tag: u64) -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..40_usize {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: TENANT,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("MACH-{tag}-{index:06}"),
                        title: format!("mach {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "grow {index}: {:?}", report.status);
        }

        let node = |hash: u64| crate::types::ContextNode {
            node_hash: hash,
            parent_hash: 0,
            kind: 1,
            canonical_name: format!("session/mach-{hash}"),
            l0: "the text an extract produces".to_string(),
            status: 0,
            last_event_time_ms: 1_781_700_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: (0..384).map(|i| i as f32 / 1024.0).collect(),
            embedding_model_hash: 7,
            embedding_updated_at_ms: 1,
            summary_vector: (0..384).map(|i| i as f32 / 512.0).collect(),
            summary_vector_valid_from_ms: 1_781_700_000_000,
            summary_vector_model_hash: 7,
        };
        // Warm on one key, measure another.
        let warm = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode { tenant_hash: TENANT, node: Box::new(node(9_000_000 + tag)) },
        });
        assert!(warm.status.ok, "{:?}", warm.status);
        let probe = crate::alloc_probe::Probe::start();
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(node(9_500_000 + tag)),
            },
        });
        let allocs = probe.stop().allocs;
        assert!(out.status.ok, "{:?}", out.status);
        allocs
    }

    let baseline = one_write_costs(1);
    println!(
        "
  one node write, everything at its default: {baseline} allocations

  piece switched off                   allocs   difference
"
    );

    for (label, flag, value) in [
        ("TS_WAL_PREALLOCATE=0        ", "TS_WAL_PREALLOCATE", "0"),
        ("TS_ENGINE_CONCURRENT_COMMIT=0", "TS_ENGINE_CONCURRENT_COMMIT", "0"),
        ("TS_INDEX_BINARY=0           ", "TS_INDEX_BINARY", "0"),
    ] {
        std::env::set_var(flag, value);
        let with_flag = one_write_costs(2);
        std::env::remove_var(flag);
        let delta = with_flag as i64 - baseline as i64;
        println!("  {label}    {with_flag:>7}   {delta:>+10}");
    }

    println!(
        "
  a flag that changes nothing eliminates its piece. one that moves the count names it, and the
  sign says which way: negative means the default is paying for something.
"
    );
}


/// Does reading a node's history scale with how much history it has?
///
/// `history` measures 16 allocations and flat -- but flat against the CORPUS, which is the wrong
/// axis. Growing the store adds nodes; it does not add summaries to any one node. A history read
/// walks one node's summaries, so the axis that matters is how many that node has, and nothing has
/// varied it.
///
/// That is the shape that hid the ingest quadratic: flat in the thing being varied, proportional to
/// the thing that is not.
///
/// Flat per summary is the right answer -- a read returning more must do more. Superlinear is not,
/// and neither is a bounded `limit` still paying for everything behind it.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib does_reading_a_history_scale_with_the_history -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn does_reading_a_history_scale_with_the_history() {
    const TENANT: u64 = 8221;
    const NODE: u64 = 4_242_424;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  summaries   no limit   per returned   read limit 8   bytes
"
    );

    let mut previous_all: Option<u64> = None;
    for count in [1_usize, 8, 64, 256] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        // One node's history, written as the ingest writes it: a summary per turn, each with its
        // own valid_from so they are distinct points in time rather than one row overwritten.
        for turn in 0..count {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertSummary {
                    tenant_hash: TENANT,
                    summary: crate::types::ContextSummary {
                        node_hash: NODE,
                        level: 2,
                        text: format!("summary of turn {turn}, of ordinary length for one turn"),
                        valid_from_ms: 1_781_700_000_000 + turn as u64,
                        vector: (0..384).map(|i| (i + turn) as f32 / 1024.0).collect(),
                        embedding_model_hash: 7,
                    },
                },
            });
            assert!(out.status.ok, "write {turn}: {:?}", out.status);
        }

        let read = |limit: Option<usize>| {
            let request = || ExecuteRequest {
                shard_id: 1,
                command: Command::ContextQuerySummaries {
                    tenant_hash: TENANT,
                    node_hash: NODE,
                    level: 2,
                    as_of_ms: 4_000_000_000_000,
                    limit,
                },
            };
            // Warm: the first read of a shape touches one-off structures.
            let warm = engine.execute(request());
            assert!(warm.status.ok, "{:?}", warm.status);
            let probe = crate::alloc_probe::Probe::start();
            let out = engine.execute(request());
            let counts = probe.stop();
            assert!(out.status.ok, "{:?}", out.status);
            let returned = match out.response {
                CommandResponse::ContextSummaries { summaries, .. } => summaries.len(),
                other => panic!("expected summaries, got {other:?}"),
            };
            (counts.allocs, counts.alloc_bytes, returned)
        };

        let (all, all_bytes, returned_all) = read(None);
        let (limited, _, returned_limited) = read(Some(8));
        // `None` is not "unlimited": `context_limit` resolves it to the default cap, so a node with
        // a long history cannot make one read expensive. That bound is the property worth holding.
        let default_cap = 100;
        assert_eq!(
            returned_all,
            count.min(default_cap),
            "a read with no limit must return up to the default cap, and no more"
        );
        assert_eq!(
            returned_limited,
            count.min(8),
            "a limited read must return the limit, not everything"
        );

        println!(
            "  {count:>9}   {all:>8}   {:>11.2}   {limited:>12}   {all_bytes:>11}",
            all as f64 / count.min(100) as f64,
        );
        if let Some(previous) = previous_all {
            assert!(
                all >= previous,
                "reading more history cost less, which means this is not measuring the read"
            );
        }
        previous_all = Some(all);
    }

    println!(
        "
  per returned flat   => the read costs what it returns, which is the right shape.
  per returned rising => it does work proportional to the history beyond returning it.
  limit 8 flat as the history grows => a bounded read does NOT pay for the whole history.
  no-limit column flattening at the cap => a bare None is the default cap, not the whole history.
"
    );
}


/// Do the batch reads scale with the batch, or with something behind it?
///
/// `get_all` and `query embeddings` were measured against CORPUS size and looked fine. They take a
/// batch of node hashes, and that is a different axis: a per-item cost constant in the corpus can
/// still be superlinear in the batch. `history` just showed that measuring the wrong axis proves
/// nothing -- it read "flat" against one that could not have shown a problem.
///
/// Store fixed, batch varied. Per item flat is right. Per item rising means the read does work
/// quadratic in its own batch, which is what to find before a caller asks for a thousand.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib do_the_batch_reads_scale_with_the_batch -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn do_the_batch_reads_scale_with_the_batch() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 8419;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut hashes = Vec::new();
    for index in 0..320_usize {
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: TENANT,
                sources: vec![ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    source_kind: ContextSourceKind::Incident,
                    source_id: format!("BATCH-{index:06}"),
                    title: format!("batch {index}"),
                    body: format!(
                        "{}{}",
                        format!("entry {index} covering the depot rota "),
                        "context payload sentence. ".repeat(40)
                    ),
                    timestamp_ms: 1_000 + index as u64,
                    provider: ContextModelProviderConfig::default(),
                }],
                provider: ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(report.status.ok, "grow {index}: {:?}", report.status);
        hashes.extend(report.node_hashes.iter().copied());
    }
    assert!(hashes.len() >= 256, "need at least 256 real nodes, got {}", hashes.len());

    let measure = |command: Command| {
        let request = || ExecuteRequest { shard_id: 1, command: command.clone() };
        let warm = engine.execute(request());
        assert!(warm.status.ok, "{:?}", warm.status);
        let probe = crate::alloc_probe::Probe::start();
        let out = engine.execute(request());
        let counts = probe.stop();
        assert!(out.status.ok, "{:?}", out.status);
        counts.allocs
    };

    println!(
        "
  batch   get_all   per node   embeddings   per node   get_all (absent ids)
"
    );

    for size in [1_usize, 8, 64, 256] {
        let present: Vec<u64> = hashes.iter().copied().take(size).collect();
        // Ids that were never written: same shape of request, nothing to find.
        let absent: Vec<u64> = (0..size).map(|index| 77_000_000 + index as u64).collect();

        let nodes = measure(Command::ContextGetNodes {
            tenant_hash: TENANT,
            node_hashes: present.clone(),
        });
        let embeddings = measure(Command::ContextQueryNodeEmbeddings {
            tenant_hash: TENANT,
            node_hashes: present.clone(),
        });
        let missing = measure(Command::ContextGetNodes {
            tenant_hash: TENANT,
            node_hashes: absent,
        });

        println!(
            "  {size:>5}   {nodes:>7}   {:>8.2}   {embeddings:>10}   {:>8.2}   {missing:>20}",
            nodes as f64 / size as f64,
            embeddings as f64 / size as f64,
        );
    }

    println!(
        "
  per node flat   => the batch costs what it returns.
  per node rising => the read does work quadratic in its own batch size.
  absent column tracking the present one => a stale id costs as much as a real one.
"
    );
}


/// Does a batch embeddings query answer for every node it was asked about?
///
/// The batch probe showed `query embeddings` at 3.80 allocations per node for a batch of 256 and
/// 8.23 for 64. A per-item cost that halves as the batch grows is the signature of a cap rather than
/// of amortisation, and the arm confirms it: `.take(context_limit(None))`, which is 100.
///
/// So a caller asking about 256 nodes is answered about 100, silently -- no error, no field saying
/// it was truncated, and no `limit` parameter to raise, unlike `ContextQuerySummaries`.
///
/// This is a retrieval question before it is a cost one: a scoring pass that asks for its
/// candidates' embeddings and receives a prefix scores the remainder as if they had none.
#[test]
fn a_batch_embeddings_query_answers_for_every_node_asked() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 8677;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut hashes = Vec::new();
    for index in 0..160_usize {
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: TENANT,
                sources: vec![ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    source_kind: ContextSourceKind::Incident,
                    source_id: format!("CAP-{index:06}"),
                    title: format!("cap {index}"),
                    body: format!(
                        "{}{}",
                        format!("entry {index} covering the depot rota "),
                        "context payload sentence. ".repeat(12)
                    ),
                    timestamp_ms: 1_000 + index as u64,
                    provider: ContextModelProviderConfig::default(),
                }],
                provider: ContextModelProviderConfig::default(),
                start_time_ms: 0,
                end_time_ms: 0,
                max_events: 0,
                query: String::new(),
            },
        );
        assert!(report.status.ok, "ingest {index}: {:?}", report.status);
        hashes.extend(report.node_hashes.iter().copied());
    }
    assert!(
        hashes.len() > 100,
        "this test needs more than the default cap of nodes to say anything, got {}",
        hashes.len()
    );

    let answered_for = |batch: &[u64]| {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryNodeEmbeddings {
                tenant_hash: TENANT,
                node_hashes: batch.to_vec(),
            },
        });
        assert!(out.status.ok, "{:?}", out.status);
        match out.response {
            CommandResponse::ContextNodeEmbeddings { embeddings, .. } => embeddings.len(),
            other => panic!("expected embeddings, got {other:?}"),
        }
    };

    // Below the cap, every node asked about is answered for.
    let small: Vec<u64> = hashes.iter().copied().take(64).collect();
    assert_eq!(answered_for(&small), 64, "a batch under the cap must answer for all of it");

    // Above it, the caller is answered about a prefix -- and has no way to ask for the rest.
    let large: Vec<u64> = hashes.iter().copied().take(160).collect();
    let answered = answered_for(&large);
    assert_eq!(
        answered, 160,
        "asked about {} nodes and answered about {answered}: the batch is truncated at the default \
         limit with nothing telling the caller, so the nodes past it score as if they had no \
         embedding",
        large.len()
    );
}

/// What hash-table resizing costs, and what pre-sizing would save.
///
/// An 8-hour soak shows RSS growing as a STAIRCASE rather than a line: 1.378 kB/record of quiet
/// growth plus four jumps that together are 21% of all growth. The jumps land at 0.916, 0.893,
/// 0.881 and 0.876 of a power of two, converging on hashbrown's 7/8 load factor -- so they are
/// table resizes, and the freed old table is not returned to the OS.
///
/// This measures the same thing locally and without the OS in the way: the same entries, built
/// incrementally versus built into a pre-sized map.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_resizing_a_shard_map_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_resizing_a_shard_map_costs() {
    use std::collections::HashMap;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: "seed".into(), value: vec![b'v'; 512] },
    });
    assert!(response.status.ok, "seed write: {:?}", response.status);
    let address = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        shard.strings.values().next().cloned().expect("a written page")
    };

    // Just past a 7/8 boundary, so the incremental build pays for a resize the pre-sized one skips.
    const ENTRIES: usize = 300_000;

    let build = |capacity: Option<usize>| -> (u64, u64) {
        let probe = crate::alloc_probe::Probe::start();
        let mut map: HashMap<String, crate::block_store::BlockAddress> = match capacity {
            Some(n) => HashMap::with_capacity(n),
            None => HashMap::new(),
        };
        for index in 0..ENTRIES {
            map.insert(format!("resize-{index:08}"), address.clone());
        }
        let counts = probe.stop();
        std::hint::black_box(&map);
        (counts.alloc_bytes.saturating_sub(counts.free_bytes), counts.alloc_bytes)
    };

    let (grown_live, grown_alloc) = build(None);
    let (sized_live, sized_alloc) = build(Some(ENTRIES));

    let per = |v: u64| v as f64 / ENTRIES as f64;
    println!();
    println!("  {ENTRIES} entries, identical keys and addresses");
    println!("  build             live/entry   allocated/entry");
    println!("  grown from empty  {:>8.0} B   {:>13.0} B", per(grown_live), per(grown_alloc));
    println!("  pre-sized         {:>8.0} B   {:>13.0} B", per(sized_live), per(sized_alloc));
    println!();
    println!("  pre-sizing saves  {:>8.0} B/entry live   ({:+.1}%)",
             per(grown_live) - per(sized_live),
             100.0 * (per(grown_live) - per(sized_live)) / per(grown_live));
    println!("  and avoids        {:>8.0} B/entry of allocation churn",
             per(grown_alloc) - per(sized_alloc));
    println!();
    if per(grown_live) - per(sized_live) < 8.0 {
        println!("  Live cost is the SAME either way: the old table is genuinely freed, so a resize");
        println!("  leaves no permanent slack. The RSS jump the soak sees is therefore the ALLOCATOR");
        println!("  holding freed memory rather than returning it, which is why it looks permanent");
        println!("  from outside the process and is recoverable from inside it.");
    } else {
        println!("  The grown map carries permanent slack, so pre-sizing is a live-memory win.");
    }
    println!();
    println!("  What pre-sizing does buy is the churn above, and the transient: during a resize");
    println!("  BOTH tables exist at once, so the peak is higher than the plateau on either side.");
    println!("  A node sized to its steady-state RSS can still die during a resize.");

    assert!(grown_live > 0 && sized_live > 0, "measured nothing");
}

/// Is the per-record resident cost actually per-record, or is it fixed cost divided by a small
/// corpus?
///
/// The figure of ~1,040 bytes per record comes from dividing live memory by 30,000 records, and
/// only 184 of those bytes are the map entry the record lands in
/// (`what_resizing_a_shard_map_costs`). The remaining ~850 could be per-record structure somewhere
/// else -- or it could be the engine's fixed setup cost divided by a corpus too small to amortise
/// it, which would make it vanish at scale rather than being anything to fix.
///
/// The same mistake is already on file at the other end: a 90-second run reads ~3 kB/record where
/// an 8-hour one reads 1.38. So measure at two sizes and take the SLOPE, which is the only part
/// that scales, and the INTERCEPT, which is what never amortises.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib is_the_per_record_cost_really_per_record -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn is_the_per_record_cost_really_per_record() {
    let live_at = |records: u64| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let probe = crate::alloc_probe::Probe::start();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..records {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("scale-{index:08}"),
                    value: vec![b'v'; 512],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        let counts = probe.stop();
        std::hint::black_box(&engine);
        counts.alloc_bytes.saturating_sub(counts.free_bytes)
    };

    // The engine is INSIDE the probe, so fixed setup cost is counted rather than excluded --
    // that is the thing being separated out.
    const SMALL: u64 = 30_000;
    const LARGE: u64 = 240_000;

    let small_live = live_at(SMALL);
    let large_live = live_at(LARGE);

    let small_per = small_live as f64 / SMALL as f64;
    let large_per = large_live as f64 / LARGE as f64;
    let slope = (large_live as f64 - small_live as f64) / (LARGE - SMALL) as f64;
    let intercept = small_live as f64 - slope * SMALL as f64;

    println!();
    println!("  records     live total      live/record");
    println!("  {SMALL:>7}  {:>12}  {small_per:>13.0} B", small_live);
    println!("  {LARGE:>7}  {:>12}  {large_per:>13.0} B", large_live);
    println!();
    println!("  slope (the part that scales)      {slope:>9.0} B/record");
    println!("  intercept (fixed, never amortises) {:>8.2} MB", intercept / 1024.0 / 1024.0);
    println!();
    println!("  a map entry costs                 {:>9} B", 184);
    println!("  unattributed per-record structure {:>9.0} B", slope - 184.0);
    println!();
    if (small_per - slope).abs() > 0.25 * small_per {
        println!("  The two per-record figures DISAGREE, so the smaller one was inflated by fixed");
        println!("  cost divided across too few records. Only the slope is a per-record cost, and");
        println!("  a capacity estimate must use it -- the intercept is paid once.");
    } else {
        println!("  The two agree, so the cost really is per-record and fixed setup is negligible");
        println!("  at both sizes. The unattributed bytes above are real structure to find.");
    }

    assert!(small_live > 0 && large_live > 0, "measured nothing");
}

/// Which structure outside the shard index grows with the record count?
///
/// The per-record resident cost has a slope of ~1,026 bytes
/// (`is_the_per_record_cost_really_per_record`), and only 184 of those are the map entry the
/// record lands in. The other ~842 are not in `ShardState`'s other maps
/// (`where_a_record_is_kept`), and the WAL and index-log in-memory state is keyed per SHARD rather
/// than per record, so neither can hold a per-record cost. That leaves the page store.
///
/// Counts at two corpus sizes, so a structure is judged by whether it GROWS rather than by
/// whether it is non-empty.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_grows_outside_the_shard_index -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_grows_outside_the_shard_index() {
    let counts_at = |records: u64| -> (usize, usize, usize) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..records {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("grow-{index:08}"),
                    value: vec![b'v'; 512],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        let bands = engine.page_store.zone_descriptors().len();
        let slabs = engine.page_store.slab_ids().map(|v| v.len()).unwrap_or(0);
        let strings = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            shards.get(&1).expect("loaded shard").strings.len()
        };
        (bands, slabs, strings)
    };

    const SMALL: u64 = 20_000;
    const LARGE: u64 = 160_000;
    let (small_bands, small_slabs, small_strings) = counts_at(SMALL);
    let (large_bands, large_slabs, large_strings) = counts_at(LARGE);

    println!();
    println!("  structure          at {SMALL:>7}   at {LARGE:>7}   per record   grows?");
    let row = |name: &str, a: usize, b: usize| {
        let per = (b as f64 - a as f64) / (LARGE - SMALL) as f64;
        println!("  {name:16} {a:>10} {b:>12} {per:>12.4}   {}",
                 if per > 0.01 { "yes" } else { "no" });
        per
    };
    let band_per = row("page bands", small_bands, large_bands);
    row("page slabs", small_slabs, large_slabs);
    row("index entries", small_strings, large_strings);

    println!();
    println!("  A band descriptor is on the order of 100 bytes, so at {band_per:.4} bands per");
    println!("  record it accounts for roughly {:.1} bytes of the ~842 unattributed.", band_per * 100.0);
    if band_per * 100.0 < 100.0 {
        println!("  That is nowhere near enough: the page store is NOT where the memory goes, and");
        println!("  the remaining structure is somewhere these counts do not reach.");
    }

    assert_eq!(large_strings as u64, LARGE, "every write should be indexed");
}

/// Attribute the per-record cost by DROPPING structures and measuring what comes back.
///
/// Counting entries has narrowed this as far as it can: the index holds 1.00 entries per record
/// and every other structure counted holds none (`where_a_record_is_kept`,
/// `what_grows_outside_the_shard_index`), yet one entry measures 184 bytes against a per-record
/// slope of ~1,026 (`what_resizing_a_shard_map_costs`,
/// `is_the_per_record_cost_really_per_record`). Counts cannot close that -- a structure can hold
/// one entry per record and that entry can be far bigger than the same entry built by hand.
///
/// So stop counting and free things: drop the shard, and whatever the allocator takes back was
/// held by it. That is attribution by subtraction, and it cannot miss a structure the way an
/// enumeration can.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_dropping_the_shard_gives_back -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_dropping_the_shard_gives_back() {
    const RECORDS: u64 = 120_000;

    let dir = tempfile::tempdir().unwrap();

    let build = crate::alloc_probe::Probe::start();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("drop-{index:08}"),
                value: vec![b'v'; 512],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let built = build.stop();
    let live_after_build = built.alloc_bytes.saturating_sub(built.free_bytes);

    // Free ONLY the shard. The engine, its stores and its caches stay alive, so whatever comes
    // back was held by the shard and nothing else.
    let release = crate::alloc_probe::Probe::start();
    let shard = engine.shards.write().expect("engine lock poisoned").remove(&1);
    drop(shard);
    let released = release.stop();
    let freed_by_shard = released.free_bytes.saturating_sub(released.alloc_bytes);

    let per = |v: u64| v as f64 / RECORDS as f64;
    println!();
    println!("  live after writing {RECORDS} records   {:>12} B   {:>7.0} B/record",
             live_after_build, per(live_after_build));
    println!("  freed by dropping the shard          {:>12} B   {:>7.0} B/record",
             freed_by_shard, per(freed_by_shard));
    println!("  still held by the engine             {:>12} B   {:>7.0} B/record",
             live_after_build.saturating_sub(freed_by_shard),
             per(live_after_build) - per(freed_by_shard));
    println!();
    let shard_share = 100.0 * freed_by_shard as f64 / live_after_build as f64;
    println!("  the shard holds {shard_share:.0}% of it");
    println!();
    if shard_share > 60.0 {
        println!("  So the cost IS the shard index, and one of its entries is far more expensive");
        println!("  in place than the same entry built by hand. The difference is what to chase:");
        println!("  the key is stored once per map, but the value carried alongside it is not the");
        println!("  bare address a hand-built map holds.");
    } else {
        println!("  So the shard is NOT the holder, and the memory is in the engine's own stores");
        println!("  despite their per-shard keying. The next cut is per store rather than per map.");
    }

    assert!(live_after_build > 0, "measured nothing");
}

/// Every place a record is kept, counted across the WHOLE shard rather than a sample of it.
///
/// An earlier version of this probe listed seven `ShardState` fields, found only `strings`
/// populated, and concluded a record is held once. `ShardState` has 42 fields, so that conclusion
/// was drawn from a sixth of the structure -- and dropping the shard frees 963 bytes per record
/// against a 184-byte map entry (`what_dropping_the_shard_gives_back`), which a single home cannot
/// explain.
///
/// So this counts every container the shard owns, including the bucket index's own per-object
/// maps, and asserts its own extent: if a field is added to `ShardState` and not listed here, the
/// count below stops matching the struct and the test says so rather than quietly under-reporting
/// again.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib where_a_record_is_kept_everywhere -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn where_a_record_is_kept_everywhere() {
    const RECORDS: usize = 20_000;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("kept-{index:08}"),
                value: vec![b'v'; 512],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");

    // The bucket index keeps its own per-object maps, one set per bucket, so they have to be
    // summed across buckets rather than read off a single length.
    let buckets = &shard.bucket_index.bucket_map;
    let object_index: usize = buckets.values().map(|b| b.object_index.len()).sum();
    let deleted_object_index: usize = buckets.values().map(|b| b.deleted_object_index.len()).sum();
    let page_index: usize = buckets.values().map(|b| b.page_index.len()).sum();

    let rows: Vec<(&str, usize)> = vec![
        ("strings", shard.strings.len()),
        ("hashes", shard.hashes.len()),
        ("sets", shard.sets.len()),
        ("zsets", shard.zsets.len()),
        ("lists", shard.lists.len()),
        ("seen", shard.seen.len()),
        ("buckets", shard.buckets.len()),
        ("expires_at_ms", shard.expires_at_ms.len()),
        ("features", shard.features.len()),
        ("feature_values", shard.feature_values.len()),
        ("feature_rollups", shard.feature_rollups.len()),
        ("sequences", shard.sequences.len()),
        ("control_state", shard.control_state.len()),
        ("control_state_pages", shard.control_state_pages.len()),
        ("control_state_changes", shard.control_state_changes.len()),
        ("control_state_change_sketch", shard.control_state_change_sketch.len()),
        ("control_state_selection", shard.control_state_selection.len()),
        ("control_state_uuid", shard.control_state_uuid.len()),
        ("control_state_rollups", shard.control_state_rollups.len()),
        ("context_nodes", shard.context_nodes.len()),
        ("context_events", shard.context_events.len()),
        ("context_event_timeline", shard.context_event_timeline.len()),
        ("context_indexes", shard.context_indexes.len()),
        ("context_audits", shard.context_audits.len()),
        ("context_entities", shard.context_entities.len()),
        ("context_children", shard.context_children.len()),
        ("context_summaries", shard.context_summaries.len()),
        ("context_compressions", shard.context_compressions.len()),
        ("context_dirty_index", shard.context_dirty_index.len()),
        ("context_embedding_dirty_index", shard.context_embedding_dirty_index.len()),
        ("context_compression_watermark", shard.context_compression_watermark.len()),
        ("wal_resident_pages", shard.wal_resident_pages.len()),
        ("buckets_pending_flag_refresh", shard.buckets_pending_flag_refresh.len()),
        ("dirty_objects", shard.dirty_objects.len()),
        ("bucket_recency", shard.bucket_recency.len()),
        ("bucket_index.bucket_map", buckets.len()),
        ("bucket_index.kind_pool", shard.bucket_index.kind_pool.len()),
        ("bucket_index.object_page_lookup", shard.bucket_index.object_page_lookup.len()),
        ("  .object_index", object_index),
        ("  .deleted_object_index", deleted_object_index),
        ("  .page_index", page_index),
    ];

    // Extent, so this cannot silently under-report the way its predecessor did. ShardState has 42
    // fields and they are accounted for exactly once each:
    //
    //   35  top-level containers, every one listed above
    //    6  scalars that cannot hold a per-record entry (index_format_version,
    //       control_coalesce_persist, control_distinct_sketch, applied_wal_sequence,
    //       promote_scan_done, evict_sampler)
    //    1  bucket_index, expanded into the 6 rows above: its own bucket_map, kind_pool and
    //       object_page_lookup, plus the three per-object maps summed across buckets
    const TOP_LEVEL_CONTAINERS: usize = 35;
    const NON_CONTAINER_FIELDS: usize = 6;
    const BUCKET_INDEX_ROWS: usize = 6;
    assert_eq!(
        TOP_LEVEL_CONTAINERS + NON_CONTAINER_FIELDS + 1,
        42,
        "ShardState field count changed -- update this probe, it is under-reporting"
    );
    assert_eq!(
        rows.len(),
        TOP_LEVEL_CONTAINERS + BUCKET_INDEX_ROWS,
        "a listed field is missing from the rows above"
    );

    println!();
    println!("  {RECORDS} plain string writes, every container the shard owns:");
    println!("  structure                        entries   per record");
    let mut homes = 0;
    let mut total = 0.0;
    for (name, count) in &rows {
        let per = *count as f64 / RECORDS as f64;
        total += per;
        if per > 0.01 {
            homes += 1;
            println!("  {name:32} {count:>7} {per:>12.2}   <-- holds every record");
        } else if *count > 0 {
            println!("  {name:32} {count:>7} {per:>12.4}", );
        }
    }
    println!("  {:32} {:>7} {total:>12.2}", "counted, per record", "");
    println!();
    println!("  {homes} structures hold roughly one entry per record.");
    println!("  Dropping the shard returns 963 B/record and one map entry costs 184, so a record");
    println!("  paying for {homes} homes is the shape of that {:.1}x -- and the fix is to keep it in", 963.0 / 184.0);
    println!("  fewer places rather than to shrink any one of them.");
    println!();
    println!("  ({TOP_LEVEL_CONTAINERS} of 42 ShardState fields are containers and all are listed, plus");
    println!("   {BUCKET_INDEX_ROWS} rows expanded out of bucket_index; the other {NON_CONTAINER_FIELDS} are scalars.)");

    assert_eq!(shard.strings.len(), RECORDS, "every write should be in strings");
}

/// What a bounded routing-slot range would save a one-box node.
///
/// A record is kept in seven places (`where_a_record_is_kept_everywhere`), and four of them --
/// the slot map, slot recency, the object index and the page index -- hold one entry per record
/// only because each record gets its own routing slot. That happens when a shard has the default
/// full-u32 slot range, which is what a node with no meta server to assign it one inherits: the
/// live one-box soak node reports routing_slot_count 4,294,967,295 and a dirty slot count equal
/// to its object count.
///
/// A meta-server-managed shard gets 0..1023 instead. This loads the same corpus both ways and
/// counts, so the saving is measured rather than argued.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_bounded_slot_range_saves -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_bounded_slot_range_saves() {
    const RECORDS: usize = 40_000;

    let run = |end_routing_bucket: u32| -> (usize, usize, usize, usize, u64) {
        let dir = tempfile::tempdir().unwrap();
        let probe = crate::alloc_probe::Probe::start();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let load = engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1,
            load_version: 0,
            local_node_id: Some(1),
            shard_uri: "local://shard/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket,
            readonly: false,
            table_name: String::new(),
        });
        assert!(load.status.ok, "load: {:?}", load.status);
        for index in 0..RECORDS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("slot-{index:08}"),
                    value: vec![b'v'; 512],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        let counts = probe.stop();
        let live = counts.alloc_bytes.saturating_sub(counts.free_bytes);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let buckets = &shard.bucket_index.bucket_map;
        let objects: usize = buckets.values().map(|b| b.object_index.len()).sum();
        let pages: usize = buckets.values().map(|b| b.page_index.len()).sum();
        (buckets.len(), shard.bucket_recency.len(), objects, pages, live)
    };

    let (wide_slots, wide_recency, wide_objects, wide_pages, wide_live) = run(u32::MAX);
    let (narrow_slots, narrow_recency, narrow_objects, narrow_pages, narrow_live) = run(1023);

    let per = |v: u64| v as f64 / RECORDS as f64;
    println!();
    println!("  {RECORDS} records, same corpus, two slot ranges");
    println!("  structure            0..u32::MAX      0..1023");
    println!("  slot map          {wide_slots:>13} {narrow_slots:>12}");
    println!("  slot recency      {wide_recency:>13} {narrow_recency:>12}");
    println!("  object index      {wide_objects:>13} {narrow_objects:>12}");
    println!("  page index        {wide_pages:>13} {narrow_pages:>12}");
    println!();
    println!("  live memory       {:>11.0} B {:>10.0} B  per record",
             per(wide_live), per(narrow_live));
    println!("  bounding the range saves {:>6.0} B/record  ({:+.1}%)",
             per(wide_live) - per(narrow_live),
             100.0 * (per(wide_live) - per(narrow_live)) / per(wide_live));
    println!();
    println!("  The object and page indexes still hold one entry per record either way -- an");
    println!("  object has to be findable. What collapses is the PER-SLOT overhead: one slot");
    println!("  node per record becomes one per 1024 records, and slot recency with it.");

    assert!(narrow_slots <= 1024, "a bounded range must not exceed its slot count");
    assert_eq!(wide_objects, RECORDS, "every record should be indexed either way");
    assert_eq!(narrow_objects, RECORDS, "every record should be indexed either way");
}

/// Does a bounded slot range make writes CHEAPER, or only smaller?
///
/// Bounding a one-box shard's routing-slot range saves 244 B/record of live memory
/// (`what_a_bounded_slot_range_saves`) by collapsing four per-record homes into two. Whether it
/// also makes a write faster is a separate question: fewer live entries does not by itself mean
/// less work per write, and the two arms could allocate identically and just retain differently.
///
/// Counted rather than timed, because a count says the same thing on a loaded machine and this
/// box swings by 5-30% run to run.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib does_a_bounded_slot_range_make_writes_cheaper -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn does_a_bounded_slot_range_make_writes_cheaper() {
    const RECORDS: u64 = 40_000;

    // Interleaved would be better still, but these two arms cannot share a process without
    // sharing an engine, so they run in sequence with the SAME corpus and the same key shape.
    let arm = |end_routing_bucket: u32| -> (f64, f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let load = engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1,
            load_version: 0,
            local_node_id: Some(1),
            shard_uri: "local://shard/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket,
            readonly: false,
            table_name: String::new(),
        });
        assert!(load.status.ok, "load: {:?}", load.status);

        // Measure the STEADY state: the first writes build fixed structure, so counting from
        // empty would charge one arm for setup the other also pays.
        for index in 0..RECORDS / 4 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("warm-{index:08}"),
                    value: vec![b'v'; 512],
                },
            });
            assert!(response.status.ok, "warm {index}: {:?}", response.status);
        }
        let probe = crate::alloc_probe::Probe::start();
        for index in 0..RECORDS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cost-{index:08}"),
                    value: vec![b'v'; 512],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        let counts = probe.stop();
        let n = RECORDS as f64;
        (
            counts.allocs as f64 / n,
            counts.alloc_bytes as f64 / n,
            counts.alloc_bytes.saturating_sub(counts.free_bytes) as f64 / n,
        )
    };

    let (wide_ops, wide_bytes, wide_live) = arm(u32::MAX);
    let (narrow_ops, narrow_bytes, narrow_live) = arm(1023);

    println!();
    println!("  steady-state cost of one write, {RECORDS} writes after a warm-up");
    println!("  slot range        allocations   allocated B   live B");
    println!("  0..u32::MAX      {wide_ops:>12.1} {wide_bytes:>13.0} {wide_live:>8.0}");
    println!("  0..1023          {narrow_ops:>12.1} {narrow_bytes:>13.0} {narrow_live:>8.0}");
    println!();
    println!("  bounding the range changes a write by {:+.1} allocations ({:+.1}%)",
             narrow_ops - wide_ops, 100.0 * (narrow_ops - wide_ops) / wide_ops);
    println!("  and {:+.0} allocated bytes ({:+.1}%)",
             narrow_bytes - wide_bytes, 100.0 * (narrow_bytes - wide_bytes) / wide_bytes);
    println!();
    if narrow_ops < wide_ops * 0.97 {
        println!("  So it is a LATENCY win as well as a footprint one: the per-slot work a write");
        println!("  does is real work, not just retained bytes.");
    } else if narrow_ops > wide_ops * 1.03 {
        println!("  Writes cost MORE with a bounded range -- slots now hold many objects each, so");
        println!("  the per-slot structures a write touches are bigger. That is a real trade and");
        println!("  the footprint saving is not free.");
    } else {
        println!("  Writes cost the SAME. The saving is purely retained memory, so it raises the");
        println!("  ceiling without moving latency either way -- worth taking, but do not expect");
        println!("  it to show up in a latency chart.");
    }

    assert!(wide_ops > 0.0 && narrow_ops > 0.0, "measured nothing");
}

/// What each of the seven homes actually costs, by freeing them one at a time.
///
/// A record is kept in seven places (`where_a_record_is_kept_everywhere`) and the shard holds 963
/// B/record in total (`what_dropping_the_shard_gives_back`), but a count says nothing about which
/// home is expensive -- a structure holding one small entry per record and one holding a fat entry
/// per record look identical when counted.
///
/// So free them one at a time and watch what the allocator takes back. Order matters and is fixed
/// here: dropping a structure that owns its keys frees those keys, so whichever runs first is
/// credited with them. That is called out per row rather than hidden.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_each_home_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_each_home_costs() {
    const RECORDS: u64 = 60_000;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("home-{index:08}"),
                value: vec![b'v'; 512],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("loaded shard");

    // Captures nothing, so it does not fight the closures below for the shard borrow.
    let freed_by = |f: &mut dyn FnMut()| -> f64 {
        let probe = crate::alloc_probe::Probe::start();
        f();
        let counts = probe.stop();
        counts.free_bytes.saturating_sub(counts.alloc_bytes) as f64 / RECORDS as f64
    };

    // Least entangled first: these own nothing the others need.
    let mut freed_rows: Vec<(&str, f64)> = Vec::new();
    let per = freed_by(&mut || shard.bucket_recency.clear());
    freed_rows.push(("bucket_recency", per));
    let per = freed_by(&mut || shard.dirty_objects.clear());
    freed_rows.push(("dirty_objects", per));
    let per = freed_by(&mut || shard.bucket_index.object_page_lookup.clear());
    freed_rows.push(("bucket_index.object_page_lookup", per));
    let per = freed_by(&mut || {
        for bucket in shard.bucket_index.bucket_map.values_mut() {
            bucket.object_index.clear();
        }
    });
    freed_rows.push(("bucket_map .object_index", per));
    let per = freed_by(&mut || shard.bucket_index.bucket_map.clear());
    freed_rows.push(("bucket_map (the rest)", per));
    let per = freed_by(&mut || shard.strings.clear());
    freed_rows.push(("strings", per));

    println!();
    println!("  {RECORDS} records, each home freed in turn");
    println!("  home                              freed B/record");
    let mut total = 0.0;
    for (label, per) in &freed_rows {
        total += per;
        println!("  {label:34} {per:>10.0}");
    }
    println!("  ----------------------------------------------");
    println!("  {:34} {total:>10.0}", "accounted for");
    println!("  {:34} {:>10}", "the shard holds (measured separately)", 963);
    println!();
    println!("  Two things keep the rows below the total, and neither is an error:");
    println!("   - `clear()` drops entries but KEEPS the table, by design, so a row is credited");
    println!("     with its entries and not with its capacity. The shortfall above is mostly");
    println!("     retained tables.");
    println!("   - a row reading 0 can be correct rather than broken: a HashMap<u32, u64> has no");
    println!("     per-entry heap at all, and ObjectIndex is inline while a slot holds one object.");
    println!();
    println!("  Order is fixed and matters: a structure that owns its keys frees them, so an");
    println!("  earlier row is credited with anything a later row merely borrowed. The page index");
    println!("  is not freed separately because it goes with the slot map it lives in -- which is");
    println!("  why the slot map is the largest row and why bounding the slot range only recovers");
    println!("  the per-slot part of it, not the per-object part.");

    assert!(total > 0.0, "measured nothing");
}

/// Would specialising `ObjectBlockRefs::by_component` for the one-component case pay?
///
/// The object page lookup costs 238 B/record (`what_each_home_costs`), second only to the slot
/// map, and unlike the slot map it does not shrink when the slot range is bounded -- it is keyed
/// per object either way. Its inner `refs` already collapses to an inline `One` variant, and the
/// outer `by_component` is still a `Vec` that in practice holds exactly one element: a heap
/// allocation and a three-word header spent to express a list of one.
///
/// That specialisation was deliberately left undone pending a measurement, because inline is 56 B
/// against a Vec header's 24 and the byte comparison alone is a wash. So measure both halves: how
/// many components an object really has, and what the two shapes cost in allocations as well as
/// bytes.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib would_an_inline_component_pay -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn would_an_inline_component_pay() {
    const RECORDS: u64 = 20_000;

    // --- half one: what the real corpus looks like -------------------------------------
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("inline-{index:08}"),
                value: vec![b'v'; 512],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let (objects, components, one_component) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let mut objects = 0usize;
        let mut components = 0usize;
        let mut one_component = 0usize;
        for (_model, _key, refs) in shard.bucket_index.object_page_lookup.iter() {
            objects += 1;
            components += refs.by_component.len();
            if refs.by_component.len() == 1 {
                one_component += 1;
            }
        }
        (objects, components, one_component)
    };

    // --- half two: what the two shapes cost --------------------------------------------
    // The proposed shape, mirroring BlockRefs which already does this for the inner list.
    enum Proposed {
        One(crate::engine::state::ComponentBlocks),
        #[allow(dead_code)]
        Many(Vec<crate::engine::state::ComponentBlocks>),
    }

    let sample = crate::engine::state::ComponentBlocks {
        component: None,
        refs: crate::engine::state::BlockRefs::One(crate::engine::state::BlockLookupRef {
            routing_bucket: 7,
            page_ref_key: 99,
        }),
    };

    let probe = crate::alloc_probe::Probe::start();
    let current: Vec<Vec<crate::engine::state::ComponentBlocks>> =
        (0..RECORDS).map(|_| vec![sample.clone()]).collect();
    let current_counts = probe.stop();
    std::hint::black_box(&current);

    let probe = crate::alloc_probe::Probe::start();
    let proposed: Vec<Proposed> = (0..RECORDS).map(|_| Proposed::One(sample.clone())).collect();
    let proposed_counts = probe.stop();
    std::hint::black_box(&proposed);

    let per = |v: u64| v as f64 / RECORDS as f64;
    let cur_live = current_counts.alloc_bytes.saturating_sub(current_counts.free_bytes);
    let pro_live = proposed_counts.alloc_bytes.saturating_sub(proposed_counts.free_bytes);

    println!();
    println!("  the corpus, {RECORDS} records:");
    println!("    objects in the lookup          {objects:>8}");
    println!("    components total               {components:>8}");
    println!("    components per object          {:>8.3}", components as f64 / objects.max(1) as f64);
    println!("    objects with exactly one       {one_component:>8}  ({:.1}%)",
             100.0 * one_component as f64 / objects.max(1) as f64);
    println!();
    println!("  shape                allocations/object   live B/object");
    println!("  Vec of one          {:>17.2} {:>15.0}", per(current_counts.allocs), per(cur_live));
    println!("  inline One          {:>17.2} {:>15.0}", per(proposed_counts.allocs), per(pro_live));
    println!();
    println!("  inlining changes    {:+.2} allocations   {:+.0} B per object",
             per(proposed_counts.allocs) - per(current_counts.allocs),
             per(pro_live) - per(cur_live));
    println!();
    if per(proposed_counts.allocs) < per(current_counts.allocs) - 0.5 {
        println!("  It removes an allocation per object. On the live node's 2.4M objects that is");
        println!("  2.4M allocations not made and 2.4M headers not held, and the precedent is in");
        println!("  the same file: BlockRefs already serializes as a sequence via from/into, so");
        println!("  the on-disk index would not change.");
    } else {
        println!("  It does NOT remove an allocation, so the case for it is bytes alone and the");
        println!("  byte difference above is what to judge it on.");
    }

    assert!(objects > 0, "the lookup should hold objects");
}

/// What one write actually spends, split into the part that copies the payload and the part that
/// does not.
///
/// A write allocates ~40.7 kB and frees 97% of it to store a 512-byte value
/// (`does_a_bounded_slot_range_make_writes_cheaper`), an eighty-fold amplification, and that churn
/// is the write path's latency rather than anything retained. Whether to attack the copying or the
/// bookkeeping depends on which one it is, and a single value size cannot tell them apart.
///
/// So measure across value sizes and take the slope and the intercept: the slope is what each
/// payload byte costs to move, the intercept is what a write costs before it has moved any.
///
/// Counted rather than timed -- this box swings 5-30% run to run and a count says the same thing
/// under load.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_one_write_spends -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_one_write_spends() {
    const WRITES: u64 = 8_000;
    const WARM: u64 = 2_000;
    const SIZES: [usize; 9] = [64, 128, 192, 256, 384, 512, 1024, 4096, 16384];

    let arm = |payload: usize| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        // Warm first: the opening writes build fixed structure every arm would otherwise be
        // charged for, and it is the steady-state write being measured.
        for index in 0..WARM {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("warm-{index:08}"),
                    value: vec![b'v'; payload],
                },
            });
            assert!(response.status.ok, "warm {index}: {:?}", response.status);
        }
        let probe = crate::alloc_probe::Probe::start();
        for index in 0..WRITES {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cost-{index:08}"),
                    value: vec![b'v'; payload],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        let counts = probe.stop();
        let n = WRITES as f64;
        (counts.allocs as f64 / n, counts.alloc_bytes as f64 / n)
    };

    let measured: Vec<(usize, f64, f64)> =
        SIZES.iter().map(|&s| { let (a, b) = arm(s); (s, a, b) }).collect();

    println!();
    println!("  value size   allocations/write   allocated B/write   amplification");
    for (size, allocs, bytes) in &measured {
        println!("  {size:>10} {allocs:>18.1} {bytes:>19.0} {:>15.1}x",
                 bytes / *size as f64);
    }

    // Segment slopes, NOT one fit across everything. A single least-squares line over this
    // data reports an intercept that describes no part of it -- the cost is piecewise, and
    // averaging across a step hides the step, which is the interesting part.
    println!();
    println!("  segment                 B per payload byte");
    let mut step: Option<(usize, usize, f64)> = None;
    for pair in measured.windows(2) {
        let (lo, _, lo_bytes) = pair[0];
        let (hi, _, hi_bytes) = pair[1];
        let per = (hi_bytes - lo_bytes) / (hi - lo) as f64;
        println!("  {lo:>6} -> {hi:<6} {per:>26.1}");
        if per > 20.0 && step.is_none() {
            step = Some((lo, hi, hi_bytes - lo_bytes));
        }
    }

    let alloc_spread = measured.iter().map(|m| m.1).fold(f64::MIN, f64::max)
        - measured.iter().map(|m| m.1).fold(f64::MAX, f64::min);

    println!();
    println!("  allocation COUNT spread over the whole range  {alloc_spread:>8.1}");
    println!();
    if let Some((lo, hi, jump)) = step {
        println!("  There is a STEP between {lo} and {hi} bytes worth {jump:.0} B per write, and");
        println!("  the segments on either side of it are flat. That is a threshold being crossed,");
        println!("  not a cost that scales -- so the number to quote for a write is which SIDE of");
        println!("  the threshold it falls on, and a single slope through both is meaningless.");
    } else {
        println!("  No step: the cost is smooth across the range and a single slope describes it.");
    }
    println!();
    let smallest = measured.first().expect("a measurement").2;
    let largest = measured.last().expect("a measurement").2;
    println!("  Even so, the shape of the whole is clear: {smallest:.0} B to store 64 bytes and");
    println!("  {largest:.0} B to store 16384. The value is a minority of what a write allocates at");
    println!("  every size measured, so shrinking values cannot move write cost -- compression and");
    println!("  tighter encodings buy disk, not latency.");
    if alloc_spread < 20.0 {
        println!();
        println!("  And the allocation COUNT barely moves across the whole range, so a write performs");
        println!("  a fixed number of steps and only the bytes they carry change.");
    }

    assert_eq!(measured.len(), SIZES.len(), "every size should be measured");
    assert!(measured.iter().all(|m| m.2 > 0.0), "measured nothing");
}

/// Does the write path's compressor have to be rebuilt for every record?
///
/// Write cost is flat at ~2 B per payload byte on both sides of a sharp step between 192 and 256
/// bytes worth 32,535 B per write (`what_one_write_spends`). 256 is
/// `PAGE_RECORD_COMPRESSION_MIN_BYTES`, and the encoder above it is `zstd::stream::encode_all`,
/// which builds a compressor, uses it once and drops it. Records in the soak corpus average 1,244
/// bytes, so essentially every real write is above the threshold and pays this.
///
/// The zstd crate also exposes a compressor that can be held and reused. This measures whether
/// reuse actually recovers the step or whether the cost is inherent to compressing at all --
/// worth knowing before proposing to restructure a write path around it.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_rebuilding_the_compressor_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_rebuilding_the_compressor_costs() {
    const CALLS: u64 = 4_000;
    // 1244 B is the soak corpus average; 512 is the probe size used elsewhere in this file.
    const SIZES: [usize; 2] = [512, 1244];
    const LEVEL: i32 = 3;

    for size in SIZES {
        // Not a uniform byte: an all-identical buffer compresses to almost nothing and would
        // flatter both arms equally while making the ratio meaningless.
        let payload: Vec<u8> = (0..size).map(|i| ((i * 7 + (i >> 3)) % 251) as u8).collect();

        let probe = crate::alloc_probe::Probe::start();
        let mut fresh_total = 0usize;
        for _ in 0..CALLS {
            let out = zstd::stream::encode_all(std::io::Cursor::new(&payload), LEVEL)
                .expect("compress");
            fresh_total += out.len();
        }
        let fresh = probe.stop();

        let probe = crate::alloc_probe::Probe::start();
        let mut held = zstd::bulk::Compressor::new(LEVEL).expect("compressor");
        let mut held_total = 0usize;
        for _ in 0..CALLS {
            let out = held.compress(&payload).expect("compress");
            held_total += out.len();
        }
        let reused = probe.stop();

        let per = |v: u64| v as f64 / CALLS as f64;
        println!();
        println!("  payload {size} B, zstd level {LEVEL}");
        println!("  arm                  allocations/call   allocated B/call   output B");
        println!("  rebuilt each call  {:>18.1} {:>18.0} {:>10.0}",
                 per(fresh.allocs), per(fresh.alloc_bytes), fresh_total as f64 / CALLS as f64);
        println!("  held and reused    {:>18.1} {:>18.0} {:>10.0}",
                 per(reused.allocs), per(reused.alloc_bytes), held_total as f64 / CALLS as f64);
        println!("  reuse saves        {:>18.1} {:>18.0}",
                 per(fresh.allocs) - per(reused.allocs),
                 per(fresh.alloc_bytes) - per(reused.alloc_bytes));
        // NOT an equality check on the compressed bytes. The two calls frame a zstd stream
        // differently, so their outputs differ in size -- asserting they match fails, and the
        // useful property is that each still decodes back to exactly what went in.
        let fresh_once = zstd::stream::encode_all(std::io::Cursor::new(&payload), LEVEL)
            .expect("compress");
        let held_once = held.compress(&payload).expect("compress");
        for (label, blob) in [("rebuilt", &fresh_once), ("reused", &held_once)] {
            let back = zstd::stream::decode_all(std::io::Cursor::new(blob)).expect("decompress");
            assert_eq!(back, payload, "{label} arm must round-trip");
        }
        println!("  output differs by       {:>18} B  (reused is {})",
                 fresh_once.len() as i64 - held_once.len() as i64,
                 if held_once.len() < fresh_once.len() { "smaller" } else { "larger" });
    }

    println!();
    println!("  The two arms do NOT produce identical bytes -- they frame the stream differently,");
    println!("  and the reused one is smaller. Both decode back to exactly the input, which is the");
    println!("  property that matters; an equality assertion on the compressed bytes fails here and");
    println!("  saying so is the point, because the saving is real and the sameness was assumed.");
    println!();
    println!("  What it does NOT settle: the write path holds no obvious place to keep a");
    println!("  compressor, and one held across threads needs a lock or a thread-local. That is a");
    println!("  design question, and this measures only the size of the prize.");
}

/// How badly does a 1,000-record measurement mislead, and where does it stop?
///
/// The embedding-storage guidance rests on a datanode holding 1,000 records in 27.0 MB -- 27 kB
/// per record -- and concludes a 384-dim vector is 7.1% of resident memory. An eight-hour run
/// over 5.76M records measures the same node at 1.59 kB per record, seventeen times smaller, so
/// one of those numbers cannot be used for capacity and it is worth knowing which and by how much.
///
/// A single corpus size cannot answer it: fixed setup divided by a small corpus looks exactly like
/// a large per-record cost. This walks 1k -> 1M and reports both the naive per-record figure and
/// the MARGINAL cost between sizes, which is the only part that scales.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_thousand_records_hides -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_thousand_records_hides() {
    const SIZES: [u64; 4] = [1_000, 10_000, 100_000, 1_000_000];
    const VECTOR_DIMS: usize = 384;

    // Live bytes and allocations for `records` writes, engine construction INCLUDED so that the
    // fixed cost a small corpus hides is inside the measurement rather than excluded from it.
    let arm = |records: u64, with_vector: bool| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let probe = crate::alloc_probe::Probe::start();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let vector: Vec<f32> = if with_vector {
            (0..VECTOR_DIMS).map(|i| (i as f32) * 0.001).collect()
        } else {
            Vec::new()
        };
        for index in 0..records {
            let node = crate::types::ContextNode {
                node_hash: 1_000_000 + index,
                parent_hash: 0,
                kind: 1,
                canonical_name: format!("scale-{index:08}"),
                l0: String::new(),
                status: 0,
                last_event_time_ms: 1_780_000_000_000,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: vector.clone(),
                embedding_model_hash: 909,
                embedding_updated_at_ms: 1_780_000_000_000,
                summary_vector: Vec::new(),
                summary_vector_valid_from_ms: 0,
                summary_vector_model_hash: 0,
            };
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode { tenant_hash: 7, node: Box::new(node) },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        let counts = probe.stop();
        std::hint::black_box(&engine);
        let live = counts.alloc_bytes.saturating_sub(counts.free_bytes) as f64;
        (live, counts.allocs as f64 / records as f64)
    };

    println!();
    println!("  records      live total    live/record   marginal/record   allocs/write");
    let mut prev: Option<(u64, f64)> = None;
    let mut rows: Vec<(u64, f64, f64)> = Vec::new();
    for size in SIZES {
        let (live, allocs) = arm(size, true);
        let per = live / size as f64;
        let marginal = match prev {
            Some((psize, plive)) => (live - plive) / (size - psize) as f64,
            None => f64::NAN,
        };
        if marginal.is_nan() {
            println!("  {size:>8} {live:>14.0} {per:>14.0} B {:>17} {allocs:>14.1}", "--");
        } else {
            println!("  {size:>8} {live:>14.0} {per:>14.0} B {marginal:>15.0} B {allocs:>14.1}");
        }
        rows.push((size, per, marginal));
        prev = Some((size, live));
    }

    let naive = rows[0].1;
    let settled = rows[rows.len() - 1].2;
    println!();
    println!("  at {:>9} records a record looks like {naive:.0} B", SIZES[0]);
    println!("  the marginal cost at {:>4} records is    {settled:.0} B", SIZES[SIZES.len()-1]);
    println!("  the small figure overstates by         {:.1}x", naive / settled);
    println!();

    // Does the vector show up in RESIDENT memory at all? Same corpus, no vector.
    const CHECK_AT: u64 = 100_000;
    let (with_vec, _) = arm(CHECK_AT, true);
    let (no_vec, _) = arm(CHECK_AT, false);
    let saved = (with_vec - no_vec) / CHECK_AT as f64;
    let vector_bytes = (VECTOR_DIMS * 4) as f64;
    println!("  a {VECTOR_DIMS}-dim vector is {vector_bytes:.0} B on the wire");
    println!("  removing it saves {saved:>6.1} B/record resident  ({:.1}% of the vector)",
             100.0 * saved / vector_bytes);
    println!();
    if saved < vector_bytes * 0.1 {
        println!("  So the vector is NOT resident. Quoting it as a percentage of a per-record RSS");
        println!("  figure invites the wrong inference -- shrinking vectors moves durable bytes and");
        println!("  read bandwidth, and moves resident memory by approximately nothing.");
    } else {
        println!("  The vector IS a real share of resident memory and shrinking it moves RSS.");
    }

    assert!(rows.iter().all(|r| r.1 > 0.0), "measured nothing");
}

/// Dropping emptied buckets must not walk every bucket.
///
/// `sync_bucket_index_object_pages` ended with `bucket_map.retain(..)` to drop buckets it had
/// emptied. `dirty` is true for a write, so that ran on every one, and at the default routing-slot
/// range the map holds a bucket per record -- O(corpus) per command. Profiling a degraded node put
/// 94.2% of self time in that one retain, and an 8-hour run went 7 ms -> 105 ms per message.
///
/// Two properties, and the test needs BOTH: the cost must not grow with the store, and the
/// emptied buckets must still be gone. Either alone is satisfiable by a wrong change -- never
/// dropping anything is flat, and walking everything is correct.
///
///   cargo test -p temporalstore-rust --lib emptied_buckets_are_dropped_without_walking_the_map -- --nocapture
#[test]
fn emptied_buckets_are_dropped_without_walking_the_map() {
    use crate::types::ContextNode;

    fn node(nid: u64) -> Box<ContextNode> {
        Box::new(ContextNode {
            node_hash: nid, parent_hash: 0, kind: 1,
            canonical_name: format!("session/turn/{nid}"),
            l0: "body".to_string(), status: 0,
            last_event_time_ms: 1_780_000_000_000,
            l1_ref: String::new(), raw_metadata_ref: String::new(),
            vector: Vec::new(), embedding_model_hash: 0, embedding_updated_at_ms: 0,
            summary_vector: Vec::new(), summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        })
    }

    // --- correctness: a bucket emptied by a delete must not survive -----------------------
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024, dir.path().join("cache"),
        dir.path().join("pages"), dir.path().join("indexes"));
    engine.load_shard(1);
    for index in 0..200u64 {
        let r = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: format!("drop-{index}"), value: vec![b'v'; 64] },
        });
        assert!(r.status.ok, "write: {:?}", r.status);
    }
    let before = {
        let shards = engine.shards.read().expect("lock");
        shards.get(&1).expect("shard").bucket_index.bucket_map.len()
    };
    for index in 0..200u64 {
        let r = engine.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringDelete { key: format!("drop-{index}") } });
        assert!(r.status.ok, "delete: {:?}", r.status);
    }
    let (after, empties) = {
        let shards = engine.shards.read().expect("lock");
        let map = &shards.get(&1).expect("shard").bucket_index.bucket_map;
        let empties = map
            .values()
            .filter(|b| b.page_index.is_empty() && b.object_index.is_empty())
            .count();
        (map.len(), empties)
    };
    println!();
    println!("  buckets after 200 writes: {before}, after deleting all: {after} ({empties} empty)");
    assert!(before > 0, "the writes should have created buckets");
    assert_eq!(
        empties, 0,
        "an emptied bucket survived in bucket_map: {empties} of {after} hold nothing"
    );

    // --- the shape: per-write cost must not grow with the store --------------------------
    // The batch a real message sends. An upsert ALONE does not reproduce this -- the first
    // version of this test used one, passed on the unfixed code, and guarded nothing. The
    // removals that reach the whole-map walk come from the event and index-ref writes.
    let per_write_us = |corpus: u64| -> f64 {
        use crate::types::{ContextEvent, ContextIndexRef};
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            4 * 1024 * 1024, dir.path().join("cache"),
            dir.path().join("pages"), dir.path().join("indexes"));
        engine.load_shard(1);
        let msg = |nid: u64| vec![
            Command::ContextUpsertNode { tenant_hash: 7, node: node(nid) },
            Command::ContextWriteEvent {
                tenant_hash: 7, node_hash: nid, first_write_only: true, cold_storage: false,
                event: Box::new(ContextEvent {
                    event_id_hash: nid, event_time_ms: 1_780_000_000_000,
                    ingestion_time_ms: 1_780_000_000_000, kind: 1, event_type: 1,
                    actor_hash: 42, status: 1, valid_until_ms: 0, confidence: 1.0,
                    importance: 1.0, text: "body".to_string(),
                    source_ref: format!("msg-{nid}"), related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(), vector: Vec::new(),
                }),
            },
            Command::ContextWriteIndexRef {
                tenant_hash: 7, index_name: "source".to_string(),
                index_value_hash: nid % 100_000, scope_hash: 0,
                event_time_ms: 1_780_000_000_000,
                index_ref: ContextIndexRef {
                    primary_node_hash: nid, primary_event_time_ms: 1_780_000_000_000,
                    event_id_hash: nid,
                },
            },
        ];
        let mut nid = 5_000_000u64;
        for _ in 0..corpus {
            nid += 1;
            engine.batch_execute(crate::types::BatchExecuteRequest {
                shard_id: 1, commands: msg(nid) });
        }
        const N: u64 = 300;
        let t0 = std::time::Instant::now();
        for _ in 0..N {
            nid += 1;
            engine.batch_execute(crate::types::BatchExecuteRequest {
                shard_id: 1, commands: msg(nid) });
        }
        t0.elapsed().as_micros() as f64 / N as f64
    };
    let small = per_write_us(2_000);
    let large = per_write_us(20_000);
    let growth = large / small;
    println!("  per-write: {small:.0} us at 2k -> {large:.0} us at 20k  ({growth:.2}x)");
    // Generous: the defect measured 2.97x over this range and 15x over a real run. Anything
    // under 2x is not the walk coming back; timing on a shared machine is not tighter than this.
    assert!(
        growth < 2.0,
        "per-write cost grew {growth:.2}x from a 2k to a 20k corpus -- the whole-map walk is back"
    );
}
