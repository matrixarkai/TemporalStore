// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::FeaturePoint;

fn open(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::SetConfigRequest {
                shard_id: 1,
                config: crate::Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok
    );
    engine
}

fn append(engine: &TemporalEngine, value: u8) {
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "series".into(),
            points: vec![FeaturePoint {
                timestamp_ms: 7,
                value: vec![value; 48],
            }],
        },
    });
    assert!(answer.status.ok, "{:?}", answer.status);
}

#[test]
fn feature_window_pins_the_old_wal_locator_across_overwrite() {
    use crate::engine::timestamped_series::TimestampedSeries as _;
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    append(&engine, 1);
    let window = engine.shards.read().unwrap()[&1]
        .features()
        .get("series")
        .unwrap()
        .range(..)
        .map(|(instant, address)| (instant, address.clone()))
        .collect::<Vec<_>>();
    assert_eq!(window.len(), 1);
    let old = window[0].1.clone();
    assert!(crate::wal_record::is_wal_resident(old.block_slab_id()));
    let object = stable_block_object_id(1, "feature", "series");
    let expected = block_in_wal::read_block_at(&engine.block_store, 1, object, None, &old).unwrap();
    let plan = FastPathRead::FeatureRange {
        key: "series",
        window,
        routing_bucket: 0,
        filters: None,
    };
    let guard = plan
        .pin_log_locations(&engine.block_store, 1)
        .expect("WAL window must capture a locator");
    append(&engine, 9);
    // Feature locators retire at durable publication, not at the timestamp overwrite.
    engine.create_bucket_dump_manifest(1, []).unwrap();
    engine.cache.invalidate_shard(1).unwrap();
    {
        let nested = block_in_wal::ReadSnapshotGuard::new(&engine.block_store, 1);
        assert!(
            block_in_wal::read_block_at(&engine.block_store, 1, object, None, &old).is_none(),
            "checkpoint publication must actually retire the uncaptured old locator"
        );
        drop(nested);
    }
    assert_eq!(
        Some(expected),
        block_in_wal::read_block_at(&engine.block_store, 1, object, None, &old)
    );
    drop(guard);
    assert!(
        block_in_wal::read_block_at(&engine.block_store, 1, object, None, &old).is_none(),
        "dropping the captured scope must not restore the retired locator"
    );
}

#[test]
fn feature_query_keeps_its_captured_window_during_checkpoint_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    append(&engine, 1);
    engine.cache.invalidate_shard(1).unwrap();
    let writer = engine.clone();
    AFTER_FAST_READ_PLAN.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(move || {
            assert!(writer.shards.try_write().is_ok());
            std::thread::spawn(move || {
                append(&writer, 9);
                writer.create_bucket_dump_manifest(1, []).unwrap();
                writer.cache.invalidate_shard(1).unwrap();
            })
            .join()
            .unwrap();
        }));
    });
    let query = || {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "series".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(10),
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
        match response.response {
            CommandResponse::FeaturePoints { points } => points,
            other => panic!("expected feature points, got {other:?}"),
        }
    };
    let captured = query();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].timestamp_ms, 7);
    assert_eq!(captured[0].value, vec![1; 48]);
    AFTER_FAST_READ_PLAN.with(|slot| assert!(slot.borrow().is_none()));
    let current = query();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].timestamp_ms, 7);
    assert_eq!(current[0].value, vec![9; 48]);
}

#[test]
fn feature_windows_pin_only_wal_backed_addresses() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for window in [
        Vec::new(),
        vec![(7, ElementEntry::from_parts(1, 0, 64, Some(0), None))],
    ] {
        let plan = FastPathRead::FeatureRange {
            key: "series",
            window,
            routing_bucket: 0,
            filters: None,
        };
        assert!(plan.pin_log_locations(&engine.block_store, 1).is_none());
    }
    let plan = FastPathRead::FeatureRange {
        key: "series",
        window: vec![
            (7, ElementEntry::from_parts(1, 0, 64, Some(0), None)),
            (
                8,
                ElementEntry::from_parts(crate::wal_record::WAL_LOG_SLAB_ID, 64, 64, None, None),
            ),
        ],
        routing_bucket: 0,
        filters: None,
    };
    assert!(plan.pin_log_locations(&engine.block_store, 1).is_some());
}
