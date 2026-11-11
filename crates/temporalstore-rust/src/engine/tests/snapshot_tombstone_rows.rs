// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::index_log::{IndexDeltaRecord, IndexItem, LocalIndexLogStore};

const SHARD: u64 = 1;

fn apply(engine: &TemporalEngine, commands: Vec<Command>) {
    let expected = commands.len();
    let result = engine.batch_execute(BatchExecuteRequest {
        shard_id: SHARD,
        commands,
    });
    assert!(result.status.ok, "{:?}", result.status);
    assert_eq!(result.responses.len(), expected);
    assert!(result.responses.iter().all(|response| response.status.ok));
}

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        root.join("cache"),
        root.join("blocks"),
        root.join("index"),
    );
    engine.load_shard(SHARD);
    engine
}

fn fixture(rounds: usize) -> (tempfile::TempDir, TemporalEngine, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut seed = (0..16)
        .map(|index| Command::SetAdd {
            key: "order".into(),
            member: format!("cold-{index}").into_bytes(),
        })
        .collect::<Vec<_>>();
    seed.extend([
        Command::SetAdd {
            key: "order".into(),
            member: b"hot-0".to_vec(),
        },
        Command::HashMultiSet {
            key: "catalog".into(),
            entries: vec![
                ("changing".into(), b"initial".to_vec()),
                ("untouched".into(), b"retained".to_vec()),
            ],
        },
        Command::StringSet {
            key: "outside".into(),
            value: b"outside payload".to_vec(),
        },
    ]);
    apply(&engine, seed);
    engine.flush_shard_index(SHARD);
    let base = {
        let shards = engine.shards.read().unwrap();
        serialize_index(shards.get(&SHARD).unwrap())
    };
    for index in 1..=rounds {
        apply(
            &engine,
            vec![
                Command::SetRemove {
                    key: "order".into(),
                    member: format!("hot-{}", index - 1).into_bytes(),
                },
                Command::SetAdd {
                    key: "order".into(),
                    member: format!("hot-{index}").into_bytes(),
                },
                Command::HashSet {
                    key: "catalog".into(),
                    field: "changing".into(),
                    value: index.to_string().into_bytes(),
                },
                Command::HashSet {
                    key: "history".into(),
                    field: index.to_string(),
                    value: format!("history-{index}").into_bytes(),
                },
            ],
        );
    }
    (dir, engine, base)
}

fn rows<const COMPACT: bool>(engine: &TemporalEngine, keys: &[String]) -> Vec<IndexItem> {
    let shards = engine.shards.read().unwrap();
    collect_command_index_items_with_policy::<COMPACT>(
        shards.get(&SHARD).unwrap(),
        keys,
        0,
        u32::MAX,
        None,
    )
}

fn coverage(items: &[IndexItem]) -> BTreeSet<(String, u32, String)> {
    items
        .iter()
        .map(|item| {
            (
                item.object_key.to_string(),
                item.routing_bucket,
                item.model_id.clone(),
            )
        })
        .collect()
}

fn decoded_record(
    store: &LocalIndexLogStore,
    items: Vec<IndexItem>,
    sequence: u64,
) -> IndexDeltaRecord {
    let actual = store
        .append_delta(SHARD, items, Vec::new(), Some(sequence), None, false, true)
        .unwrap();
    assert_eq!(actual, sequence);
    store
        .read_delta_records(SHARD, sequence - 1)
        .unwrap()
        .pop()
        .unwrap()
}

#[test]
fn snapshot_rows_are_bounded_by_live_blocks_and_deleted_groups() {
    for rounds in [32, 128] {
        let (_dir, engine, _base) = fixture(rounds);
        let keys = vec!["order".into(), "catalog".into(), "history".into()];
        let full = rows::<false>(&engine, &keys);
        let compact = rows::<true>(&engine, &keys);
        let dead_before = full.iter().filter(|item| item.deleted).count();
        let dead_after = compact.iter().filter(|item| item.deleted).count();
        assert!(
            dead_before >= rounds,
            "fixture must accumulate distinct tombstones"
        );
        assert_eq!(dead_after, 1);
        assert_eq!(coverage(&compact), coverage(&full));
        assert_eq!(
            compact
                .iter()
                .filter(|item| !item.deleted)
                .collect::<Vec<_>>(),
            full.iter().filter(|item| !item.deleted).collect::<Vec<_>>()
        );
        let full_bytes = rmp_serde::to_vec(&full).unwrap().len();
        let compact_bytes = rmp_serde::to_vec(&compact).unwrap().len();
        assert!(compact_bytes < full_bytes);
        println!("snapshot_rows rounds={rounds} full={} compact={} deleted_full={dead_before} deleted_compact={dead_after} full_bytes={full_bytes} compact_bytes={compact_bytes}",full.len(),compact.len());
    }
}

#[test]
fn existing_decoder_and_snapshot_fold_restore_the_same_index() {
    let (dir, engine, base) = fixture(64);
    let keys = vec!["order".into(), "catalog".into(), "history".into()];
    let store = LocalIndexLogStore::new(dir.path().join("wire-check"));
    let full = decoded_record(&store, rows::<false>(&engine, &keys), 1);
    let compact = decoded_record(&store, rows::<true>(&engine, &keys), 2);
    let mut expected = decode_index_bytes(&base).unwrap();
    let mut actual = decode_index_bytes(&base).unwrap();
    fold_delta_block_items(
        &mut expected.bucket_index,
        &delta_record_covered_keys(&full),
        &full.items,
        false,
    );
    fold_delta_block_items(
        &mut actual.bucket_index,
        &delta_record_covered_keys(&compact),
        &compact.items,
        false,
    );
    assert_eq!(
        serde_json::to_value(&actual.bucket_index).unwrap(),
        serde_json::to_value(&expected.bucket_index).unwrap()
    );
    assert_eq!(coverage(&compact.items), coverage(&full.items));
    assert!(compact.items.len() < full.items.len());
}

#[test]
fn tombstone_only_snapshot_keeps_coverage_and_removes_old_blocks() {
    let (_dir, engine, base) = fixture(0);
    let commands = (0..16)
        .map(|index| Command::SetRemove {
            key: "order".into(),
            member: format!("cold-{index}").into_bytes(),
        })
        .chain(std::iter::once(Command::SetRemove {
            key: "order".into(),
            member: b"hot-0".to_vec(),
        }))
        .collect();
    apply(&engine, commands);
    let full = rows::<false>(&engine, &["order".into()]);
    let compact = rows::<true>(&engine, &["order".into()]);
    assert_eq!(full.len(), 17);
    assert_eq!(compact.len(), 1);
    assert!(compact[0].deleted);
    assert_eq!(coverage(&full), coverage(&compact));
    let mut old = decode_index_bytes(&base).unwrap();
    let mut new = decode_index_bytes(&base).unwrap();
    let covered = BTreeSet::from(["order".to_string()]);
    fold_delta_block_items(&mut old.bucket_index, &covered, &full, false);
    fold_delta_block_items(&mut new.bucket_index, &covered, &compact, false);
    assert_eq!(
        serde_json::to_value(&old.bucket_index).unwrap(),
        serde_json::to_value(&new.bucket_index).unwrap()
    );
    assert!(new
        .bucket_index
        .block_refs_for("set", "order", None)
        .map_or(true, |refs| refs.is_empty()));
}

#[test]
fn deleted_coverage_is_kept_for_each_object_and_model() {
    let (_dir, engine, _base) = fixture(0);
    apply(
        &engine,
        (0..16)
            .map(|index| Command::ZSetAdd {
                key: "rank".into(),
                member: index.to_string().into_bytes(),
                score: index as f64,
            })
            .collect(),
    );
    let mut commands = (0..16)
        .map(|index| Command::ZSetRemove {
            key: "rank".into(),
            member: index.to_string().into_bytes(),
        })
        .collect::<Vec<_>>();
    commands.extend((0..16).map(|index| Command::SetRemove {
        key: "order".into(),
        member: format!("cold-{index}").into_bytes(),
    }));
    apply(&engine, commands);
    let keys = vec!["order".into(), "rank".into()];
    let full = rows::<false>(&engine, &keys);
    let compact = rows::<true>(&engine, &keys);
    assert_eq!(coverage(&full), coverage(&compact));
    assert_eq!(compact.iter().filter(|item| item.deleted).count(), 2);
    assert!(compact
        .iter()
        .any(|item| item.object_key.as_ref() == "rank" && item.deleted));
    assert!(compact
        .iter()
        .any(|item| item.object_key.as_ref() == "order" && item.deleted));
}

fn check_values(engine: &TemporalEngine, rounds: usize) {
    let members = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::SetMembers {
            key: "order".into(),
        },
    });
    assert!(members.status.ok);
    let CommandResponse::Members { members } = members.response else {
        panic!("expected order members")
    };
    assert_eq!(members.len(), 17);
    assert!(members.contains(&format!("hot-{rounds}").into_bytes()));
    assert!(!members.contains(&b"hot-0".to_vec()));
    for (key, field, value) in [
        ("catalog", "changing", rounds.to_string()),
        ("catalog", "untouched", "retained".into()),
        ("history", "1", "history-1".into()),
        ("history", "32", "history-32".into()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::HashGet {
                key: key.into(),
                field: field.into(),
            },
        });
        assert!(response.status.ok);
        assert!(
            matches!(response.response,CommandResponse::Bytes {value:Some(ref bytes)} if bytes==value.as_bytes())
        );
    }
}

#[test]
fn restart_preserves_order_catalog_and_retained_history() {
    let (dir, engine, _base) = fixture(32);
    check_values(&engine, 32);
    engine.write_ahead_log_store().flush(SHARD).unwrap();
    drop(engine);
    let recovered = open(dir.path());
    check_values(&recovered, 32);
}

#[test]
fn index_log_fold_preserves_membership_and_packed_hash_values() {
    let (_dir, engine, _base) = fixture(32);
    let folded = engine.load_index_checked(SHARD, false).unwrap().unwrap();
    let members = folded
        .bucket_index
        .sets
        .get("order")
        .unwrap()
        .iter()
        .map(|(member, _)| member.clone())
        .collect::<Vec<_>>();
    assert_eq!(members.len(), 17);
    assert!(members.contains(&b"hot-32".to_vec()));
    assert!(!members.contains(&b"hot-0".to_vec()));
    for (field, expected) in [
        ("changing", b"32".as_slice()),
        ("untouched", b"retained".as_slice()),
    ] {
        let address = folded
            .bucket_index
            .hashes
            .get("catalog")
            .unwrap()
            .get(field)
            .unwrap();
        let bytes = read_block_bytes(
            &engine.cache,
            &engine.block_store,
            SHARD,
            address,
            BlockIdentity::of(SHARD, "hash", "catalog", Some(field)),
            Some(block_routing_bucket("catalog", 0, u32::MAX)),
        )
        .unwrap();
        assert_eq!(bytes, expected);
    }
}
