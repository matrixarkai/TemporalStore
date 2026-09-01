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
    };
    let native_node = ContextNode {
        status: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
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
                vector: Vec::new(),
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
                event,
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
                event,
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
                    vector: Vec::new(),
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
                    vector: Vec::new(),
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
fn live_page_slab_ids_scan_all_index_backed_data_models() {
    let mut shard = ShardState::default();
    shard.strings.insert(
        "string".to_string(),
        BlockAddress::from_parts(7, 0, 1, None, None, None, None, None, None),
    );
    shard.hashes.entry("hash".to_string()).or_default().insert(
        "field".to_string(),
        BlockAddress::from_parts(8, 0, 1, None, None, None, None, None, None),
    );
    shard.sets.entry("set".to_string()).or_default().insert(
        b"member".to_vec(),
        BlockAddress::from_parts(9, 0, 1, None, None, None, None, None, None),
    );
    shard
        .features
        .entry("feature".to_string())
        .or_default()
        .insert(
            10,
            BlockAddress::from_parts(10, 0, 1, None, None, None, None, None, None),
        );
    shard
        .features
        .entry("sequence".to_string())
        .or_default()
        .insert(
            11,
            BlockAddress::from_parts(11, 0, 1, None, None, None, None, None, None),
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
                vector: Vec::new(),
            },
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
            .find(|page| page.object_key == "owned")
            .expect("owned slot page");
        page.address.set_object_id(Some(page.object_id.wrapping_add(1)));
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
    };
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node,
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
            node: ContextNode {
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
            },
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
                node: ContextNode {
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
                },
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
                node: ContextNode {
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
                },
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
        BlockAddress::from_parts(7, 11, 13, None, None, None, None, None, None),
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
            BlockAddress::from_parts(i, i * 7, i + 1, Some(i), Some(i * 3), Some((i % 8) as u32), Some(i), None, None),
        );
    }
    shard.hashes.entry("hash-object".to_string()).or_default().insert(
        "component".to_string(),
        BlockAddress::from_parts(9, 1, 2, None, None, None, None, None, None),
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
