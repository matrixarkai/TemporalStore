// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::control::Config;
use crate::types::{CommandResponse, ExecuteRequest, FeaturePoint};
use crate::SetConfigRequest;
use std::path::Path;

fn engine(root: &Path, role: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        4096,
        root.join(format!("{role}-cache")),
        root.join(format!("{role}-blocks")),
        root.join(format!("{role}-indexes")),
    )
}

#[test]
fn shared_wal_cannot_install_an_unbound_address_that_reads_wrong_local_bytes() {
    use crate::wal::{StagedBlock, WalOutcomeItem};

    let root = tempfile::tempdir().unwrap();
    let source = engine(root.path(), "source");
    let recipient = engine(root.path(), "recipient");
    recipient.load_shard(1);
    let append = |engine: &TemporalEngine, bytes: &[u8]| {
        engine.block_store()
            .append_block_of_object_for_shard(1, bytes, None, None, 0)
            .unwrap()
    };
    let foreign = append(&source, b"right");
    let local = append(&recipient, b"wrong");
    let carried_first = append(&source, b"first");
    assert_eq!(foreign.address_word(), local.address_word());
    assert_eq!(foreign.length(), local.length());
    assert_eq!(recipient.block_store().read(&foreign).unwrap(), b"wrong");

    let outcome = |id, key: &str, address| WalOutcomeItem {
        kind: "string".into(),
        object_key: key.to_owned(),
        element_name: None,
        object_id: id,
        routing_bucket: 1,
        address: Some(address),
        value: None,
        ttl: None,
        deleted: false,
        meta: false,
    };
    let mut entry = SharedStoreWalEntry {
        shard_id: 1,
        wal_index: 2,
        command: None,
        outcomes: vec![outcome(1, "first", carried_first), outcome(2, "second", foreign)],
        staged_blocks: vec![StagedBlock {
            object_id: 1,
            element_name: None,
            bytes: b"first".to_vec(),
        }],
    };
    let read = |key: &str| recipient.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: key.to_owned() },
    }).response;
    let replay = |entry| SharedStoreReplicator::<FileObjectStore>::apply_replayed_wal_entry(
        1, 2, entry, &recipient, false,
    );
    match replay(entry.clone()).unwrap_err() {
        SharedStoreReplicationError::ApplyFailed { wal_index: 2, status } => {
            assert_eq!(status.code, "shared_wal_unbound_address");
        }
        error => panic!("wrong replay error: {error}"),
    }
    assert_eq!(read("first"), CommandResponse::Bytes { value: None });
    assert_eq!(read("second"), CommandResponse::Bytes { value: None });
    assert_eq!(recipient.block_store().read(&local).unwrap(), b"wrong");

    entry.staged_blocks.push(StagedBlock {
        object_id: 2,
        element_name: None,
        bytes: b"right".to_vec(),
    });
    replay(entry).unwrap();
    assert_eq!(read("first"), CommandResponse::Bytes { value: Some(b"first".to_vec()) });
    assert_eq!(read("second"), CommandResponse::Bytes { value: Some(b"right".to_vec()) });
    assert_eq!(recipient.block_store().read(&local).unwrap(), b"wrong");
}

#[tokio::test]
async fn owned_checkpoints_from_independent_stores_can_share_a_destination() {
    independent_checkpoint_restore(false).await;
}

#[tokio::test]
async fn independent_lazy_checkpoint_sources_survive_restart_before_any_fetch() {
    independent_checkpoint_restore(true).await;
}

async fn independent_checkpoint_restore(lazy: bool) {
    let root = tempfile::tempdir().unwrap();
    let objects = Arc::new(FileObjectStore::new(root.path().join("objects")));
    let replicator = SharedStoreReplicator::new("owned-checkpoints", objects);
    let mut manifests = Vec::new();
    for (shard, role, bytes) in [(1, "source-a", b"first"), (42, "source-b", b"other")] {
        let source = engine(root.path(), role);
        source.load_shard(shard);
        let response = source.execute(ExecuteRequest {
            shard_id: shard,
            command: Command::StringSet { key: "key".into(), value: bytes.to_vec() },
        });
        assert!(response.status.ok);
        let manifest = replicator.publish_checkpoint(shard, 1, &source, &source.block_store())
            .await.unwrap();
        assert_eq!(manifest.block_slabs.len(), 1);
        assert_eq!(slab_owner_from_transfer_key(
            manifest.block_slabs[0].block_slab_id, &manifest.block_slabs[0].key,
        ).unwrap(), Some(shard));
        manifests.push(manifest);
    }
    assert_eq!(manifests[0].block_slabs[0].block_slab_id, manifests[1].block_slabs[0].block_slab_id);
    let follower = engine(root.path(), "follower");
    let store = follower.block_store();
    for manifest in &manifests {
        if lazy {
            replicator.restore_index_and_block_addresses(manifest.shard_id, &follower, &store)
                .await.unwrap();
        } else {
            replicator.restore_checkpoint(manifest, &follower, &store).await.unwrap();
        }
    }
    let installed = store.slab_ids().unwrap();
    assert_eq!(installed.len(), if lazy { 0 } else { 2 });
    drop(store);
    drop(follower);

    let follower = engine(root.path(), "follower");
    if lazy {
        assert!(follower.block_store().has_saved_lazy_checkpoints().unwrap());
        assert!(!follower.block_store().has_shared_slab_source());
        replicator.reattach_lazy_checkpoint_sources(&follower.block_store()).unwrap();
        assert!(follower.block_store().has_shared_slab_source());
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);
    } else {
        replicator.restore_checkpoint(&manifests[0], &follower, &follower.block_store()).await.unwrap();
    }
    assert_eq!(follower.block_store().slab_ids().unwrap(), installed);
    for (shard, expected) in [(1, b"first"), (42, b"other")] {
        follower.load_shard(shard);
        let response = follower.execute(ExecuteRequest {
            shard_id: shard,
            command: Command::StringGet { key: "key".into() },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Bytes { value: Some(expected.to_vec()) });
    }
    assert_eq!(follower.block_store().slab_ids().unwrap().len(), 2);
    if lazy {
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 2);
    }
}

#[tokio::test]
async fn rejected_eager_checkpoint_preserves_the_previous_served_index_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let objects = Arc::new(FileObjectStore::new(root.path().join("objects")));
    let replicator = SharedStoreReplicator::new("restore-publication", Arc::clone(&objects));
    let mut manifests = Vec::new();
    for (role, bytes) in [("source-a", b"first"), ("source-b", b"other")] {
        let source = engine(root.path(), role);
        source.load_shard(1);
        let response = source.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: "key".into(), value: bytes.to_vec() },
        });
        assert!(response.status.ok);
        let manifest = replicator.publish_checkpoint(1, 1, &source, &source.block_store())
            .await.unwrap();
        manifests.push(manifest);
    }
    assert_eq!(manifests[0].block_slabs.len(), 1);
    assert_eq!(manifests[1].block_slabs.len(), 1);
    assert_eq!(manifests[0].block_slabs[0].block_slab_id, manifests[1].block_slabs[0].block_slab_id);

    let follower = engine(root.path(), "follower");
    let store = follower.block_store();
    replicator.restore_checkpoint(&manifests[0], &follower, &store).await.unwrap();
    let installed = store.slab_ids().unwrap();
    let remote = &manifests[1].block_slabs[0].key;
    let good_bytes = objects.get(remote).await.unwrap();
    objects.put(remote, Bytes::from_static(b"invalid incoming slab")).await.unwrap();
    assert!(replicator.restore_checkpoint(&manifests[1], &follower, &store).await.is_err());
    assert_eq!(store.slab_ids().unwrap(), installed);
    drop(store);
    drop(follower);

    let follower = engine(root.path(), "follower");
    follower.load_shard(1);
    let query = Command::StringGet { key: "key".into() };
    let old = follower.execute(ExecuteRequest { shard_id: 1, command: query.clone() });
    assert!(old.status.ok);
    assert_eq!(old.response, CommandResponse::Bytes { value: Some(b"first".to_vec()) });
    drop(follower);

    objects.put(remote, good_bytes).await.unwrap();
    let follower = engine(root.path(), "follower");
    replicator.restore_checkpoint(&manifests[1], &follower, &follower.block_store()).await.unwrap();
    follower.load_shard(1);
    let new = follower.execute(ExecuteRequest { shard_id: 1, command: query });
    assert!(new.status.ok);
    assert_eq!(new.response, CommandResponse::Bytes { value: Some(b"other".to_vec()) });
    assert_eq!(follower.block_store().slab_ids().unwrap().len(), 2);
}

async fn portable_model(kind: &str) {
    let dir = tempfile::tempdir().unwrap();
    let primary = engine(dir.path(), "primary");
    primary.load_shard(1);
    assert!(
        primary
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
    for i in 0..48 {
        let bytes = format!("value-{i:03}").into_bytes();
        let command = match kind {
            "hash" => Command::HashSet {
                key: "model".into(),
                field: format!("f-{i:03}"),
                value: bytes,
            },
            "set" => Command::SetAdd {
                key: "model".into(),
                member: bytes,
            },
            "list" => Command::ListPush {
                key: "model".into(),
                member: bytes,
                left: false,
            },
            "zset" => Command::ZSetAdd {
                key: "model".into(),
                member: bytes,
                score: i as f64,
            },
            "feature" => Command::FeatureAppend {
                key: "model".into(),
                points: vec![FeaturePoint {
                    timestamp_ms: i,
                    value: bytes,
                }],
            },
            _ => unreachable!(),
        };
        assert!(
            primary
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command
                })
                .status
                .ok
        );
    }
    let queries = match kind {
        "hash" => (0..48)
            .map(|i| Command::HashGet {
                key: "model".into(),
                field: format!("f-{i:03}"),
            })
            .collect(),
        "set" => vec![Command::SetMembers {
            key: "model".into(),
        }],
        "list" => vec![Command::ListRange {
            key: "model".into(),
            start: 0,
            stop: -1,
        }],
        "zset" => (0..48)
            .map(|i| Command::ZSetScore {
                key: "model".into(),
                member: format!("value-{i:03}").into_bytes(),
            })
            .collect(),
        "feature" => vec![Command::FeatureQuery {
            key: "model".into(),
            start_ms: 0,
            end_ms: 48,
            count: None,
        }],
        _ => unreachable!(),
    };
    let expected: Vec<_> = queries
        .iter()
        .map(|query| {
            let before = primary.execute(ExecuteRequest {
                shard_id: 1,
                command: query.clone(),
            });
            assert!(before.status.ok, "{before:?}");
            before.response
        })
        .collect();
    assert!(
        primary
            .live_block_slab_ids(1)
            .iter()
            .any(|id| crate::wal_record::is_wal_resident(*id)),
        "the fixture must include log-only blocks"
    );
    primary.cache().invalidate_shard(1).unwrap();
    let store = Arc::new(FileObjectStore::new(dir.path().join("objects")));
    let replicator = SharedStoreReplicator::new("portable-models", store);
    let manifest = replicator
        .publish_checkpoint(1, 48, &primary, &primary.block_store())
        .await
        .unwrap();
    assert!(
        primary
            .live_block_slab_ids(1)
            .iter()
            .all(|id| !crate::wal_record::is_wal_resident(*id)),
        "a portable checkpoint must relocate every model's log-only blocks"
    );
    drop(primary);
    let follower = engine(dir.path(), "follower");
    assert_eq!(
        follower
            .write_ahead_log_store()
            .stats_without_rescan(1)
            .last_sequence,
        0
    );
    replicator
        .restore_checkpoint(&manifest, &follower, &follower.block_store())
        .await
        .unwrap();
    follower.load_shard(1);
    follower.cache().invalidate_shard(1).unwrap();
    for (query, before) in queries.into_iter().zip(expected) {
        let after = follower.execute(ExecuteRequest {
            shard_id: 1,
            command: query,
        });
        assert!(after.status.ok, "{after:?}");
        assert_eq!(
            after.response, before,
            "{kind} changed across a checkpoint-only restore"
        );
    }
}

#[tokio::test]
async fn portable_checkpoint_hash_without_source_log() {
    portable_model("hash").await;
}

#[tokio::test]
async fn portable_checkpoint_set_without_source_log() {
    portable_model("set").await;
}

#[tokio::test]
async fn portable_checkpoint_list_without_source_log() {
    portable_model("list").await;
}

#[tokio::test]
async fn portable_checkpoint_zset_without_source_log() {
    portable_model("zset").await;
}

#[tokio::test]
async fn portable_checkpoint_feature_without_source_log() {
    portable_model("feature").await;
}

#[tokio::test]
async fn portable_checkpoint_refuses_a_partial_relocation_and_can_retry() {
    let dir = tempfile::tempdir().unwrap();
    let primary = engine(dir.path(), "primary");
    primary.load_shard(1);
    assert!(
        primary
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                }
            })
            .ok
    );
    let points: Vec<_> = (0..16u64)
        .map(|timestamp_ms| FeaturePoint {
            timestamp_ms,
            value: timestamp_ms.to_le_bytes().to_vec(),
        })
        .collect();
    for point in &points {
        assert!(
            primary
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppend {
                        key: "series".into(),
                        points: vec![point.clone()],
                    }
                })
                .status
                .ok
        );
    }
    let store = Arc::new(FileObjectStore::new(dir.path().join("objects")));
    let replicator = SharedStoreReplicator::new("portable-failure", store);
    crate::engine::fail_only_compaction_block_read_at_for_test(Some(0));
    let refused = replicator
        .publish_checkpoint(1, 16, &primary, &primary.block_store())
        .await;
    crate::engine::fail_only_compaction_block_read_at_for_test(None);
    assert!(refused.is_err());
    assert!(replicator.list_checkpoints(1).await.unwrap().is_empty());
    assert_eq!(
        primary.registration_count_for_test(1),
        1,
        "the unreadable frame must remain registered after the partial durable rewrite"
    );
    let manifest = replicator
        .publish_checkpoint(1, 16, &primary, &primary.block_store())
        .await
        .unwrap();
    assert_eq!(primary.registration_count_for_test(1), 0);
    drop(primary);
    let follower = engine(dir.path(), "follower");
    replicator
        .restore_checkpoint(&manifest, &follower, &follower.block_store())
        .await
        .unwrap();
    follower.load_shard(1);
    follower.cache().invalidate_shard(1).unwrap();
    let read = follower.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "series".into(),
            start_ms: 0,
            end_ms: 16,
            count: None,
        },
    });
    assert!(read.status.ok);
    assert_eq!(
        read.response,
        crate::types::CommandResponse::FeaturePoints { points }
    );
}

struct WriteDuringUpload {
    inner: FileObjectStore,
    engine: Arc<TemporalEngine>,
    fired: std::sync::atomic::AtomicBool,
}

#[tokio::test]
async fn portable_checkpoint_keeps_a_deleted_series_absent() {
    let dir = tempfile::tempdir().unwrap();
    let primary = engine(dir.path(), "primary");
    primary.load_shard(1);
    assert!(
        primary
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                }
            })
            .ok
    );
    for key in ["gone", "kept"] {
        assert!(
            primary
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppend {
                        key: key.into(),
                        points: vec![FeaturePoint {
                            timestamp_ms: 1,
                            value: b"value".to_vec()
                        }],
                    }
                })
                .status
                .ok
        );
    }
    assert!(
        primary
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete { key: "gone".into() }
            })
            .status
            .ok
    );
    let store = Arc::new(FileObjectStore::new(dir.path().join("objects")));
    let replicator = SharedStoreReplicator::new("portable-delete", store);
    let manifest = replicator
        .publish_checkpoint(1, 3, &primary, &primary.block_store())
        .await
        .unwrap();
    drop(primary);
    let follower = engine(dir.path(), "follower");
    replicator
        .restore_checkpoint(&manifest, &follower, &follower.block_store())
        .await
        .unwrap();
    follower.load_shard(1);
    for (key, exists) in [("gone", 0), ("kept", 1)] {
        let response = follower.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExists { key: key.into() },
        });
        assert!(response.status.ok);
        assert_eq!(
            response.response,
            crate::types::CommandResponse::Integer { value: exists }
        );
    }
}

#[async_trait::async_trait]
impl ObjectStore for WriteDuringUpload {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.inner.put(key, bytes).await?;
        if key.contains("/block_segments/") && !self.fired.swap(true, Ordering::SeqCst) {
            self.engine.block_store().roll_slab_for_shard(1).unwrap();
            assert!(
                self.engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: "after-capture".into(),
                            value: b"later".to_vec(),
                        }
                    })
                    .status
                    .ok
            );
        }
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        self.inner.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }
    fn uri(&self, key: &str) -> String {
        self.inner.uri(key)
    }
}

#[tokio::test]
async fn portable_checkpoint_checks_the_uploaded_image_not_later_live_writes() {
    let dir = tempfile::tempdir().unwrap();
    let primary = Arc::new(engine(dir.path(), "primary"));
    primary.load_shard(1);
    assert!(
        primary
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "before-capture".into(),
                    value: b"before".to_vec(),
                }
            })
            .status
            .ok
    );
    let store = Arc::new(WriteDuringUpload {
        inner: FileObjectStore::new(dir.path().join("objects")),
        engine: primary.clone(),
        fired: std::sync::atomic::AtomicBool::new(false),
    });
    let replicator = SharedStoreReplicator::new("portable-capture", store.clone());
    let manifest = replicator
        .publish_checkpoint(1, 1, &primary, &primary.block_store())
        .await
        .unwrap();
    assert!(store.fired.load(Ordering::SeqCst));
    assert!(primary.live_block_slab_ids(1).iter().any(|id| !manifest
        .block_slabs
        .iter()
        .any(|slab| slab.block_slab_id == *id)));
    let follower = engine(dir.path(), "follower");
    replicator
        .restore_checkpoint(&manifest, &follower, &follower.block_store())
        .await
        .unwrap();
    follower.load_shard(1);
    for (key, value) in [
        ("before-capture", Some(b"before".to_vec())),
        ("after-capture", None),
    ] {
        let response = follower.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key.into() },
        });
        assert!(response.status.ok);
        assert_eq!(
            response.response,
            crate::types::CommandResponse::Bytes { value }
        );
    }
}
