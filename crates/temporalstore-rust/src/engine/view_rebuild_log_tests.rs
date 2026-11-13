// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn view_rebuild_log_control_state_keeps_its_slot_owned_refs() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "cache-control");
    load(&source);
    assert!(
        source
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    async_storage: true,
                    extend_config: std::collections::BTreeMap::from([(
                        "control_coalesce_persist".into(),
                        "off".into(),
                    )]),
                    ..Default::default()
                },
            })
            .ok
    );
    execute(
        &source,
        Command::StringSet {
            key: "anchor".into(),
            value: b"live".to_vec(),
        },
    );
    execute(
        &source,
        Command::ControlStateIncrement {
            key: "counter".into(),
            timestamp_ms: 100,
            amount: 17,
        },
    );
    let query = || Command::ControlStateQuery {
        key: "counter".into(),
        start_ms: 0,
        end_ms: 200,
        aggregator: "sum".into(),
    };
    let expected = execute(&source, query());
    {
        let shards = source.shards.read().unwrap();
        let shard = shards.get(&1).unwrap();
        let slot = &shard.bucket_index.objects
            [&storage_bucket_internals::StoredModelKind::ControlState]["counter"];
        assert!(
            slot.refs().is_some_and(|refs| refs.len() > 0),
            "fixture must write and file a real counter block"
        );
    }
    source.flush_shard_index(1);
    source.wal_store.flush(1).unwrap();
    drop(source);
    for cache in ["cache-control-restored-1", "cache-control-restored-2"] {
        storage_bucket_internals::reset_view_rebuild_block_failure_counts();
        let restored = engine(dir.path(), cache);
        load(&restored);
        assert_eq!(
            storage_bucket_internals::view_rebuild_block_failure_counts(),
            (0, 0)
        );
        {
            let shards = restored.shards.read().unwrap();
            let shard = shards.get(&1).unwrap();
            let slot = &shard.bucket_index.objects
                [&storage_bucket_internals::StoredModelKind::ControlState]["counter"];
            assert!(
                slot.refs().is_some_and(|refs| refs.len() > 0),
                "reconstruction must preserve filed ownership"
            );
        }
        restored.cache().invalidate_shard(1).unwrap();
        assert_eq!(execute(&restored, query()), expected);
        restored.flush_shard_index(1);
        restored.wal_store.flush(1).unwrap();
    }
}

#[test]
fn view_rebuild_log_feature_delete_retires_blocks_before_dropping_the_owner() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "cache-delete");
        load(&source);
        assert!(
            source
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 2,
                        async_storage,
                        ..Default::default()
                    },
                })
                .ok
        );
        let points: Vec<_> = (0u64..4)
            .map(|timestamp_ms| crate::types::FeaturePoint {
                timestamp_ms,
                value: timestamp_ms.to_le_bytes().to_vec(),
            })
            .collect();
        execute(
            &source,
            Command::FeatureAppend {
                key: "series".into(),
                points: points.clone(),
            },
        );
        {
            let shards = source.shards.read().unwrap();
            let shard = shards.get(&1).unwrap();
            assert!(shard.features().contains_key("series"));
            assert!(shard.bucket_index.bucket_map.values().any(|bucket| bucket
                .block_index
                .values()
                .any(|block| block.model_id().as_str() == "feature" && !block.deleted())));
        }
        execute(
            &source,
            Command::FeatureDelete {
                key: "series".into(),
            },
        );
        {
            let shards = source.shards.read().unwrap();
            let shard = shards.get(&1).unwrap();
            assert!(!shard.features().contains_key("series"));
            assert!(
                shard.bucket_index.bucket_map.values().all(|bucket| bucket
                    .block_index
                    .values()
                    .all(|block| block.model_id().as_str() != "feature" || block.deleted())),
                "deletion must not leave live blocks with no owning series"
            );
        }
        source.flush_shard_index(1);
        source.wal_store.flush(1).unwrap();
        drop(source);
        let restored = engine(dir.path(), "cache-delete-restored");
        load(&restored);
        let query = || Command::FeatureQuery {
            key: "series".into(),
            start_ms: 0,
            end_ms: 4,
            count: None,
        };
        assert_eq!(
            execute(&restored, query()),
            CommandResponse::FeaturePoints { points: vec![] }
        );
        execute(
            &restored,
            Command::FeatureAppend {
                key: "series".into(),
                points: points.clone(),
            },
        );
        assert_eq!(
            execute(&restored, query()),
            CommandResponse::FeaturePoints { points }
        );
    }
}

#[test]
fn view_rebuild_log_locator_size_attribution() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "cache-size");
    load(&source);
    for i in 0..8000 {
        execute(
            &source,
            Command::StringSet {
                key: format!("object-{i:07}"),
                value: vec![b'x'; 128],
            },
        );
    }
    let shards = source.shards.read().unwrap();
    let state = shards.get(&1).unwrap();
    let count = state.wal_resident_blocks.len();
    assert_eq!(count, 8000);
    let json = serde_json::to_vec(state).unwrap().len();
    let packed = crate::engine::encode_index_bytes(state).len();
    if let Some(path) = std::env::var_os("TS_LOCATOR_WIRE_FIXTURE") {
        std::fs::write(path, rmp_serde::to_vec_named(state).unwrap()).unwrap();
    }
    let mut diagnostic = state.clone();
    // Only a diagnostic copy: serving and published images must keep their recovery locators.
    diagnostic.wal_resident_blocks.clear();
    let without_json = serde_json::to_vec(&diagnostic).unwrap().len();
    let without_packed = crate::engine::encode_index_bytes(&diagnostic).len();
    println!("locator attribution: {count} entries; JSON {json} -> {without_json}; shipped {packed} -> {without_packed}");
    assert!(json > without_json);
    assert!(packed > without_packed);
    assert_eq!(state.wal_resident_blocks.len(), count);
}

fn engine(root: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        4096,
        root.join(cache),
        root.join("blocks"),
        root.join("indexes"),
    )
}

fn load(engine: &TemporalEngine) {
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                load_version: 1,
                local_node_id: None,
                shard_uri: String::new(),
                readonly: false,
                table_name: String::new(),
            })
            .status
            .ok
    );
}

fn execute(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let result = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(result.status.ok, "{result:?}");
    result.response
}

fn async_writes(engine: &TemporalEngine) {
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok
    );
}

#[test]
fn view_rebuild_log_feature_checkpoint_has_no_unreadable_frames() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "cache-source");
    load(&source);
    async_writes(&source);
    let points: Vec<_> = (0..128)
        .map(|at| crate::types::FeaturePoint {
            timestamp_ms: at,
            value: at.to_le_bytes().to_vec(),
        })
        .collect();
    for point in &points {
        execute(
            &source,
            Command::FeatureAppend {
                key: "series".into(),
                points: vec![point.clone()],
            },
        );
    }
    assert_eq!(source.registration_count_for_test(1), points.len());
    source.create_bucket_dump_manifest(1, []).unwrap();
    source.flush_shard_index(1);
    source.wal_store.flush(1).unwrap();
    drop(source);
    storage_bucket_internals::reset_view_rebuild_block_failure_counts();
    let restored = engine(dir.path(), "cache-restored");
    load(&restored);
    let counts = storage_bucket_internals::view_rebuild_block_failure_counts();
    assert_eq!(
        counts,
        (0, 0),
        "WAL-backed frames are readable during index reconstruction"
    );
    restored.cache().invalidate_shard(1).unwrap();
    assert_eq!(
        execute(
            &restored,
            Command::FeatureQuery {
                key: "series".into(),
                start_ms: 0,
                end_ms: 128,
                count: None,
            }
        ),
        CommandResponse::FeaturePoints { points }
    );
}

fn queries() -> Vec<Command> {
    vec![
        Command::ContextQueryEvents {
            tenant_hash: 7,
            node_hash: 42,
            start_time_ms: 1000,
            end_time_ms: 2000,
            limit: Some(256),
            max_scan: Some(256),
            current_valid_only: false,
            as_of_ms: 0,
            kinds: vec![],
            statuses: vec![],
            min_confidence: 0.0,
            min_importance: 0.0,
        },
        Command::ContextQueryIndex {
            tenant_hash: 7,
            index_name: "source".into(),
            index_value_hash: 9,
            scope_hash: 0,
            start_time_ms: 1000,
            end_time_ms: 2000,
            limit: Some(256),
        },
    ]
}

fn event_write(id: u64, at: u64, first_write_only: bool) -> Command {
    Command::ContextWriteEvent {
        tenant_hash: 7,
        node_hash: 42,
        first_write_only,
        cold_storage: false,
        event: Box::new(crate::types::ContextEvent {
            event_id_hash: id,
            event_time_ms: at,
            ingestion_time_ms: at,
            kind: 1,
            event_type: 1,
            actor_hash: 42,
            status: 1,
            valid_until_ms: 0,
            confidence: 1.0,
            importance: 1.0,
            text: format!("event-{id}-at-{at}"),
            source_ref: format!("source-{id}"),
            related_node_hashes: vec![],
            compact_attrs: vec![],
            vector: vec![],
            embedding_model_hash: 0,
        }),
    }
}

fn index_write(id: u64, at: u64) -> Command {
    Command::ContextWriteIndexRef {
        tenant_hash: 7,
        index_name: "source".into(),
        index_value_hash: 9,
        scope_hash: 0,
        event_time_ms: at,
        index_ref: crate::types::ContextIndexRef {
            primary_node_hash: 42,
            primary_event_time_ms: at,
            event_id_hash: id,
        },
    }
}

#[test]
fn context_series_index_batches_checkpoint_every_frame_and_recover_cold_queries() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "cache-source");
    load(&source);
    async_writes(&source);
    execute(
        &source,
        Command::StringSet {
            key: "bootstrap".into(),
            value: vec![1],
        },
    );
    super::context_series_index::take_work();
    let result = source.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands: (1..=128)
            .flat_map(|id| [event_write(id, 1000 + id, true), index_write(id, 1000 + id)])
            .collect(),
    });
    assert!(result.status.ok, "{result:?}");
    assert_eq!(
        super::context_series_index::take_work(),
        (256, 0),
        "batch appends must publish their new frames without restating earlier history"
    );
    source.cache().invalidate_shard(1).unwrap();
    let expected: Vec<_> = queries().into_iter().map(|q| execute(&source, q)).collect();
    assert!(
        matches!(&expected[0], CommandResponse::ContextEvents { events, .. } if events.len() == 128)
    );
    assert!(
        matches!(&expected[1], CommandResponse::ContextIndexRefs { refs, .. } if refs.len() == 128)
    );
    let manifest = source.create_bucket_dump_manifest(1, []).unwrap();
    let image = crate::engine::decode_index_bytes(&manifest.index_bytes).unwrap();
    let entries = storage_bucket_internals::collect_bucket_index_live_block_entries(&image);
    for kind in ["context_event", "context_index"] {
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.kind.as_str() == kind && !entry.deleted)
                .count(),
            128
        );
    }
    source.flush_shard_index(1);
    source.wal_store.flush(1).unwrap();
    drop(source);
    let restored = engine(dir.path(), "cache-restored");
    load(&restored);
    assert_eq!(
        super::lifecycle::LAST_REPLAY_WATERMARK.load(std::sync::atomic::Ordering::SeqCst),
        manifest.wal_sequence
    );
    restored.cache().invalidate_shard(1).unwrap();
    let actual: Vec<_> = queries()
        .into_iter()
        .map(|q| execute(&restored, q))
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn context_series_index_append_publication_is_bounded_at_longer_histories() {
    for history in [64, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "cache-source");
        load(&source);
        async_writes(&source);
        let result = source.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: (1..=history)
                .map(|id| event_write(id, 1000 + id, true))
                .collect(),
        });
        assert!(result.status.ok);
        super::context_series_index::take_work();
        execute(&source, event_write(history + 1, 1001 + history, true));
        assert_eq!(
            super::context_series_index::take_work(),
            (1, 0),
            "history={history}"
        );
        let shards = source.shards.read().unwrap();
        let entries =
            storage_bucket_internals::collect_bucket_index_live_block_entries(&shards[&1]);
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.kind.as_str() == "context_event" && !entry.deleted)
                .count(),
            (history + 1) as usize
        );
    }
}

#[test]
fn context_series_index_overwrite_and_delete_do_not_resurrect_old_events() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "cache-source");
    load(&source);
    async_writes(&source);
    for id in 1..=8 {
        execute(&source, event_write(id, 1000 + id, true));
    }
    execute(&source, event_write(3, 1800, false));
    source.cache().invalidate_shard(1).unwrap();
    let expected = execute(&source, queries().remove(0));
    assert!(
        matches!(&expected, CommandResponse::ContextEvents { events, .. }
        if events.len() == 8 && events.iter().filter(|event| event.event_id_hash == 3).count() == 1
        && events.iter().find(|event| event.event_id_hash == 3).unwrap().event_time_ms == 1800)
    );
    source.create_bucket_dump_manifest(1, []).unwrap();
    source.flush_shard_index(1);
    source.wal_store.flush(1).unwrap();
    drop(source);
    let restored = engine(dir.path(), "cache-restored");
    load(&restored);
    restored.cache().invalidate_shard(1).unwrap();
    assert_eq!(execute(&restored, queries().remove(0)), expected);
    execute(
        &restored,
        Command::CommonDelete {
            key: "ctx:event:7:42".into(),
        },
    );
    restored.create_bucket_dump_manifest(1, []).unwrap();
    restored.flush_shard_index(1);
    restored.wal_store.flush(1).unwrap();
    drop(restored);
    let deleted = engine(dir.path(), "cache-deleted");
    load(&deleted);
    assert!(
        matches!(execute(&deleted, queries().remove(0)), CommandResponse::ContextEvents { events, .. } if events.is_empty())
    );
}

#[test]
fn context_series_index_explicit_install_preserves_loaded_and_unloaded_views() {
    for loaded in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "cache-source");
        source.load_shard(1);
        async_writes(&source);
        for id in 1..=8 {
            execute(&source, event_write(id, 1000 + id, true));
            execute(&source, index_write(id, 1000 + id));
        }
        // Explicit installation requires durable physical slabs, unlike local WAL-backed loads.
        source.compact_shard_blocks(1).unwrap();
        let expected: Vec<_> = queries().into_iter().map(|q| execute(&source, q)).collect();
        let manifest = source.create_bucket_dump_manifest(1, []).unwrap();
        source.wal_store.flush(1).unwrap();
        let target = if loaded {
            source
        } else {
            drop(source);
            engine(dir.path(), "cache-target")
        };
        target.install_bucket_dump_manifest(&manifest).unwrap();
        if !loaded {
            target.load_shard(1);
        }
        target.cache().invalidate_shard(1).unwrap();
        let actual: Vec<_> = queries().into_iter().map(|q| execute(&target, q)).collect();
        assert_eq!(actual, expected, "explicit install, loaded={loaded}");
    }
}

#[test]
fn view_rebuild_log_deferred_loader_refuses_unsafe_delta_folding() {
    let dir = tempfile::tempdir().unwrap();
    let source = engine(dir.path(), "cache-source");
    assert!(source.load_index_for_recovery(1, false).unwrap().is_none());
    let result = source.load_index_for_recovery(1, true);
    if wal_single_barrier() {
        assert_eq!(result.unwrap_err().code, "index_delta_not_durable");
    } else {
        assert!(result.unwrap().is_none());
    }
}

#[test]
fn view_rebuild_log_contexts_survive_base_and_manifest_recovery() {
    for base in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = engine(dir.path(), "cache-source");
        load(&source);
        async_writes(&source);
        for at in 0..8u64 {
            execute(
                &source,
                Command::ContextWriteEvent {
                    tenant_hash: 7,
                    node_hash: 42,
                    first_write_only: true,
                    cold_storage: false,
                    event: Box::new(crate::types::ContextEvent {
                        event_id_hash: at + 1,
                        event_time_ms: 1000 + at,
                        ingestion_time_ms: 1000 + at,
                        kind: 1,
                        event_type: 1,
                        actor_hash: 42,
                        status: 1,
                        valid_until_ms: 0,
                        confidence: 1.0,
                        importance: 1.0,
                        text: format!("event-{at}"),
                        source_ref: format!("source-{at}"),
                        related_node_hashes: vec![],
                        compact_attrs: vec![],
                        vector: vec![],
                        embedding_model_hash: 0,
                    }),
                },
            );
            execute(
                &source,
                Command::ContextWriteIndexRef {
                    tenant_hash: 7,
                    index_name: "source".into(),
                    index_value_hash: 9,
                    scope_hash: 0,
                    event_time_ms: 1000 + at,
                    index_ref: crate::types::ContextIndexRef {
                        primary_node_hash: 42,
                        primary_event_time_ms: 1000 + at,
                        event_id_hash: at + 1,
                    },
                },
            );
        }
        source.cache().invalidate_shard(1).unwrap();
        let expected: Vec<_> = queries().into_iter().map(|q| execute(&source, q)).collect();
        assert!(
            matches!(&expected[0], CommandResponse::ContextEvents { events, .. } if events.len() == 8)
        );
        assert!(
            matches!(&expected[1], CommandResponse::ContextIndexRefs { refs, .. } if refs.len() == 8)
        );
        let manifest = source.create_bucket_dump_manifest(1, []).unwrap();
        let image = crate::engine::decode_index_bytes(&manifest.index_bytes).unwrap();
        let entries = storage_bucket_internals::collect_bucket_index_live_block_entries(&image);
        for kind in ["context_event", "context_index"] {
            assert_eq!(
                entries
                    .iter()
                    .filter(|entry| entry.kind.as_str() == kind)
                    .count(),
                8,
                "the checkpoint must carry every {kind} frame, base={base}"
            );
        }
        assert!(manifest.wal_sequence > 0);
        assert_eq!(
            manifest.wal_sequence,
            source.wal_store.stats(1).last_sequence
        );
        if base {
            source.flush_shard_index(1);
        }
        source.wal_store.flush(1).unwrap();
        drop(source);
        let restored = engine(dir.path(), "cache-restored");
        load(&restored);
        assert_eq!(
            super::lifecycle::LAST_REPLAY_WATERMARK.load(std::sync::atomic::Ordering::SeqCst),
            manifest.wal_sequence,
            "the dump must cover the corpus without tail replay"
        );
        restored.cache().invalidate_shard(1).unwrap();
        let actual: Vec<_> = queries()
            .into_iter()
            .map(|q| execute(&restored, q))
            .collect();
        assert_eq!(
            actual, expected,
            "context queries after recovery, base={base}"
        );
    }
}
