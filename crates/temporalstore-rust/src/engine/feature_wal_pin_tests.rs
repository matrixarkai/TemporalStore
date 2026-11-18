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
    engine.cache.invalidate_shard(1).unwrap();
    assert_eq!(
        Some(expected),
        block_in_wal::read_block_at(&engine.block_store, 1, object, None, &old)
    );
    drop(guard);
    assert!(
        block_in_wal::read_block_at(&engine.block_store, 1, object, None, &old).is_none(),
        "the write must actually retire the old locator for this regression to exercise pinning"
    );
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
