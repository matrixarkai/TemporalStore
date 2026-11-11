// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn fixture(rounds: usize, deleted_only: bool, readd: bool) -> ShardState {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("index"),
    );
    engine.load_shard(1);
    let apply = |commands: Vec<Command>| {
        let count = commands.len();
        let response = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "{:?}", response.status);
        assert_eq!(response.responses.len(), count);
        assert!(response.responses.iter().all(|response| response.status.ok));
    };
    let mut seed = (0..16)
        .map(|index| Command::SetAdd {
            key: "members".into(),
            member: format!("cold-{index}").into_bytes(),
        })
        .collect::<Vec<_>>();
    seed.push(Command::SetAdd {
        key: "members".into(),
        member: b"hot-0".to_vec(),
    });
    seed.push(Command::StringSet {
        key: "outside".into(),
        value: b"retained".to_vec(),
    });
    apply(seed);
    for index in 1..=rounds {
        apply(vec![
            Command::SetRemove {
                key: "members".into(),
                member: format!("hot-{}", index - 1).into_bytes(),
            },
            Command::SetAdd {
                key: "members".into(),
                member: format!("hot-{index}").into_bytes(),
            },
        ]);
    }
    if readd {
        apply(vec![Command::SetAdd {
            key: "members".into(),
            member: b"hot-0".to_vec(),
        }]);
    }
    if deleted_only {
        let mut commands = (0..16)
            .map(|index| Command::SetRemove {
                key: "members".into(),
                member: format!("cold-{index}").into_bytes(),
            })
            .collect::<Vec<_>>();
        commands.push(Command::SetRemove {
            key: "members".into(),
            member: format!("hot-{rounds}").into_bytes(),
        });
        commands.push(Command::CommonDelete {
            key: "outside".into(),
        });
        apply(commands);
    }
    let shard = engine.shards.read().unwrap().get(&1).unwrap().clone();
    shard
}

fn rebuild<const BATCHED: bool>(shard: &mut ShardState) -> (usize, usize) {
    let mut calls = 0;
    let mut visits = 0;
    rebuild_bucket_first_index_with_layout::<BATCHED>(1, shard, 0, u32::MAX, |bucket, ids| {
        calls += 1;
        visits += bucket.block_index.len();
        update_bucket_layout_with_live_objects(bucket, ids);
    });
    (calls, visits)
}

#[test]
fn completed_bucket_finalization_preserves_the_scalar_rebuild() {
    for rounds in [0, 32, 128] {
        for (deleted_only, readd) in [(false, false), (true, false), (false, true)] {
            let original = fixture(rounds, deleted_only, readd);
            let mut expected = original.clone();
            let mut actual = original.clone();
            let old = rebuild::<false>(&mut expected);
            let new = rebuild::<true>(&mut actual);
            assert_eq!(
                serde_json::to_value(&actual.bucket_index).unwrap(),
                serde_json::to_value(&expected.bucket_index).unwrap()
            );
            let buckets = actual.bucket_index.bucket_map.len();
            let blocks: usize = actual
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.block_index.len())
                .sum();
            assert!(new.0 <= buckets * 2, "calls={new:?} buckets={buckets}");
            assert!(new.1 <= blocks * 2, "visits={new:?} blocks={blocks}");
            if rounds >= 32 {
                assert!(new.1 < old.1, "old={old:?} new={new:?}");
            }
            let first = serde_json::to_value(&actual.bucket_index).unwrap();
            rebuild::<true>(&mut actual);
            assert_eq!(first, serde_json::to_value(&actual.bucket_index).unwrap());
            println!(
                "rounds={rounds} deleted_only={deleted_only} readd={readd} old={old:?} new={new:?}"
            );
        }
    }
}

#[test]
fn indexed_tombstone_capture_preserves_scalar_rows_and_order() {
    for rounds in [0, 32, 128] {
        let shard = fixture(rounds, false, false);
        let scalar = collect_rebuild_tombstones::<false>(&shard);
        let indexed = collect_rebuild_tombstones::<true>(&shard);
        assert_eq!(scalar.len(), indexed.len());
        for (left, right) in scalar.iter().zip(&indexed) {
            assert_eq!(left.0, right.0);
            assert_eq!(left.1.address, right.1.address);
            assert_eq!(left.2, right.2);
        }
    }
}

#[test]
fn rebuilt_tombstone_rows_preserve_scalar_identity_order_and_shared_names() {
    let shard = fixture(32, false, false);
    let source = collect_rebuild_tombstones::<false>(&shard);
    assert!(!source.is_empty());
    for count in [0, 64, 4096, 16384] {
        let mut expected = BucketNode::default();
        let mut actual = BucketNode::default();
        let mut first_names = HashMap::new();
        let mut rows = Vec::new();
        for index in 0..count {
            let (_, block, row) = &source[(index / 17) % source.len()];
            let mut row = row.clone().unwrap();
            row.object_key = Arc::from(format!("object-{}", index % 3));
            row.element_name = Arc::from(format!("member-{}", index % 2048));
            row.model_id = if index % 2 == 0 {
                StoredModelKind::Set
            } else {
                StoredModelKind::Hash
            };
            row.block = crate::engine::state::tombstone_block_key(&block.address);
            first_names
                .entry((
                    row.model_id,
                    Arc::clone(&row.object_key),
                    Arc::clone(&row.element_name),
                ))
                .or_insert_with(|| (Arc::clone(&row.object_key), Arc::clone(&row.element_name)));
            expected.record_tombstone_element(
                &row.object_key,
                row.model_id,
                &row.element_name,
                &block.address,
            );
            rows.push(row);
        }
        actual.rebuild_tombstone_elements(rows);
        assert_eq!(actual.tombstone_elements, expected.tombstone_elements);
        for row in actual.tombstone_elements.iter() {
            let (object, name) = &first_names[&(
                row.model_id,
                Arc::clone(&row.object_key),
                Arc::clone(&row.element_name),
            )];
            assert!(Arc::ptr_eq(object, &row.object_key));
            assert!(Arc::ptr_eq(name, &row.element_name));
        }
        actual.rebuild_tombstone_elements(Vec::new());
        assert!(actual.tombstone_elements.is_empty());
        assert_eq!(
            serde_json::to_string(&actual.tombstone_elements).unwrap(),
            "[]"
        );
    }
}
