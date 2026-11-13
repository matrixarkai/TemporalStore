// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::state::{measure_block_ref_vector_shifts_for_test, BlockLookupRef, BlockRefs};

fn fresh_index() -> CoreIndex {
    let mut index = CoreIndex::default();
    index.object_element_name_block_refs = Some(0);
    index
        .objects
        .entry(StoredModelKind::String)
        .or_default()
        .insert(
            Arc::from("row"),
            crate::engine::object_record::ObjectSlot::from_record(Default::default()),
        );
    index
}

fn filings(count: usize) -> Vec<(u32, u64, Arc<str>, BlockIndex)> {
    let mut rows = Vec::new();
    let key: Arc<str> = Arc::from("wide");
    for i in 0..count {
        let handle = (count - i) as u64;
        let block = BlockIndex::new(
            StoredModelKind::Hash,
            ElementEntry::from_parts(7, i as u64 * 64, 64, None, None),
            false,
            false,
        );
        rows.push((17, handle, key.clone(), block.clone()));
        if i % 5 == 0 {
            rows.push((17, handle, key.clone(), block));
        }
        if i % 7 == 0 {
            rows.push((
                23,
                handle,
                key.clone(),
                BlockIndex::new(
                    StoredModelKind::Set,
                    ElementEntry::from_parts(9, i as u64 * 64, 64, None, None),
                    false,
                    false,
                ),
            ));
        }
    }
    rows.push((
        0,
        91,
        Arc::from("row"),
        BlockIndex::new(
            StoredModelKind::String,
            ElementEntry::from_parts(11, 0, 64, None, None),
            false,
            false,
        ),
    ));
    rows.push((
        17,
        92,
        Arc::from("deleted"),
        BlockIndex::new(
            StoredModelKind::Hash,
            ElementEntry::from_parts(11, 64, 64, None, None),
            false,
            true,
        ),
    ));
    rows
}

#[test]
fn ordered_filings_keep_both_homes_counts_duplicates_and_deleted_skips() {
    for count in [0, 1, 64, 4096, 16384] {
        let mut rows = filings(count);
        let (expected, old_shifts) = measure_block_ref_vector_shifts_for_test(|| {
            let mut index = fresh_index();
            for (bucket, handle, key, block) in &rows {
                index.insert_object_block_lookup(*bucket, *handle, key, block);
            }
            index
        });
        let (actual, new_shifts) = measure_block_ref_vector_shifts_for_test(|| {
            let mut index = fresh_index();
            file_rebuilt_block_refs(&mut index, &mut rows);
            index
        });
        for (kind, key) in [
            ("hash", "wide"),
            ("set", "wide"),
            ("string", "row"),
            ("hash", "deleted"),
        ] {
            assert_eq!(
                actual.object_block_refs(kind, key),
                expected.object_block_refs(kind, key)
            );
        }
        assert_eq!(
            actual.object_element_name_block_refs,
            expected.object_element_name_block_refs
        );
        let distinct = count + (count + 6) / 7 + 1;
        assert_eq!(actual.object_element_name_block_refs, Some(distinct));
        assert!(matches!(
            actual.object_block_refs("string", "row"),
            Some(BlockRefs::One(_))
        ));
        assert!(actual.object_block_lookup.get("string", "row").is_none());
        assert!(actual.object_block_refs("hash", "deleted").is_none());
        assert_eq!(new_shifts, 0);
        if count >= 64 {
            assert!(old_shifts > count * count / 4);
        }
        println!("wide_fields={count} scalar_shifted_refs={old_shifts} ordered_shifted_refs={new_shifts}");
    }
}

#[test]
fn filing_order_matches_the_block_lookup_ref_order_across_buckets() {
    let mut rows = filings(64);
    rows.reverse();
    let mut expected = rows
        .iter()
        .map(|row| BlockLookupRef {
            routing_bucket: row.0,
            block_ref_key: row.1,
        })
        .collect::<Vec<_>>();
    expected.sort_unstable();
    let mut index = fresh_index();
    file_rebuilt_block_refs(&mut index, &mut rows);
    let actual = rows
        .iter()
        .map(|row| BlockLookupRef {
            routing_bucket: row.0,
            block_ref_key: row.1,
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn open(dir: &std::path::Path, async_storage: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn check_values(engine: &TemporalEngine) {
    for i in 0..256 {
        let result = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGet {
                key: "catalog".into(),
                field: format!("field-{i}"),
            },
        });
        assert!(result.status.ok);
        assert!(
            matches!(result.response, CommandResponse::Bytes { value: Some(value) } if value == vec![i as u8; 64])
        );
    }
    let result = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: "members".into(),
        },
    });
    assert!(result.status.ok);
    let CommandResponse::Members { members } = result.response else {
        panic!("missing members")
    };
    assert_eq!(members.len(), 33);
    assert!(members.contains(&b"member-0".to_vec()));
    assert!(!members.contains(&b"member-1".to_vec()));
    let result = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "outside".into(),
        },
    });
    assert!(result.status.ok);
    assert!(
        matches!(result.response, CommandResponse::Bytes { value: Some(value) } if value == b"retained")
    );
}

#[test]
fn both_rebuilds_keep_tombstones_values_and_ownership_watermarks() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), async_storage);
        let mut commands = vec![
            Command::HashMultiSet {
                key: "catalog".into(),
                entries: (0..256)
                    .map(|i| (format!("field-{i}"), vec![i as u8; 64]))
                    .collect(),
            },
            Command::StringSet {
                key: "outside".into(),
                value: b"retained".to_vec(),
            },
        ];
        commands.extend((0..64).map(|i| Command::SetAdd {
            key: "members".into(),
            member: format!("member-{i}").into_bytes(),
        }));
        commands.extend((0..32).map(|i| Command::SetRemove {
            key: "members".into(),
            member: format!("member-{i}").into_bytes(),
        }));
        commands.push(Command::SetAdd {
            key: "members".into(),
            member: b"member-0".to_vec(),
        });
        let result = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(result.status.ok);
        assert!(result.responses.iter().all(|row| row.status.ok));
        {
            let mut shards = engine.shards_write_marked();
            let shard = shards.get_mut(&1).unwrap();
            let tombstones = collect_rebuild_tombstones::<true>(shard);
            assert!(!tombstones.is_empty());
            rebuild_bucket_first_index(1, shard, 0, u32::MAX);
            let generations = shard
                .bucket_index
                .bucket_map
                .iter()
                .map(|(id, bucket)| (*id, bucket.dirty_generation))
                .collect::<BTreeMap<_, _>>();
            rebuild_bucket_block_ownership(1, shard, 0, u32::MAX);
            for (id, generation) in generations {
                assert_eq!(
                    shard
                        .bucket_index
                        .bucket_map
                        .get(&id)
                        .unwrap()
                        .dirty_generation,
                    generation
                );
            }
            let after = collect_rebuild_tombstones::<true>(shard);
            assert_eq!(after.len(), tombstones.len());
            for (before, after) in tombstones.iter().zip(after.iter()) {
                assert_eq!(before.0, after.0);
                assert_eq!(before.1.address, after.1.address);
                assert_eq!(before.2, after.2);
            }
            assert!(validate_bucket_ownership_index(1, shard, 0, u32::MAX)
                .mismatches
                .is_empty());
        }
        check_values(&engine);
        assert!(engine.compact_shard_blocks(1).unwrap().rewritten_block_refs > 0);
        drop(engine);
        check_values(&open(dir.path(), async_storage));
    }
}
