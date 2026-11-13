// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn append(start: usize, count: usize) -> Vec<Command> {
    (start..start + count)
        .map(|i| Command::SetAdd {
            key: "order".into(),
            member: format!("member-{i:08}").into_bytes(),
        })
        .collect()
}

fn write(engine: &TemporalEngine, commands: Vec<Command>) {
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert!(response.responses.iter().all(|row| row.status.ok));
}

fn reset() {
    SET_WRITE_SCANS.with(|counts| counts.set((0, 0)));
}

fn counts() -> (u64, u64) {
    SET_WRITE_SCANS.with(std::cell::Cell::get)
}

fn assert_dirty(engine: &TemporalEngine, key: &str) {
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let refs = shard.bucket_index.block_refs_for("set", key, None).unwrap();
    assert!(!refs.is_empty());
    assert!(refs.iter().all(|at| shard
        .bucket_index
        .bucket_map
        .get(&at.routing_bucket)
        .and_then(|bucket| bucket.block_index.get(&at.block_ref_key))
        .is_some_and(|block| block.dirty())));
}

#[test]
fn fresh_set_batches_do_not_scan_preexisting_refs_per_member() {
    for (existing, async_storage) in [(32, false), (2_048, false), (32, true), (2_048, true)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage,
                        ..Config::default()
                    },
                })
                .ok
        );
        write(&engine, append(0, existing));
        reset();
        let before = bucket_block_upserts();
        write(&engine, append(existing, 128));
        let (supersede, dirty) = counts();
        assert_eq!(supersede, 0, "fresh frames supersede no existing frame");
        assert_eq!(
            bucket_block_upserts() - before,
            0,
            "the set writer already filed its blocks"
        );
        assert_eq!(
            dirty,
            (existing + 128) as u64,
            "propagate once per touched object"
        );
        assert_dirty(&engine, "order");
        println!("existing={existing} async={async_storage} supersede_visits={supersede} dirty_visits={dirty}");
    }
}

#[test]
fn single_set_append_does_not_refile_the_whole_object() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    write(&engine, append(0, 128));
    reset();
    let before = bucket_block_upserts();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: append(128, 1).pop().unwrap(),
    });
    assert!(response.status.ok);
    assert_eq!(counts(), (0, 129));
    assert_eq!(bucket_block_upserts() - before, 0);
    assert_dirty(&engine, "order");
}

#[test]
fn deferred_dirty_propagation_marks_previously_clean_blocks_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        write(&engine, append(0, 32));
        engine.unload_shard(1);
        engine.load_shard(1);
        reset();
        write(&engine, append(32, 128));
        assert_eq!(counts().1, 160);
        assert_dirty(&engine, "order");
    }
    let recovered = open(dir.path());
    let response = recovered.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: "order".into(),
        },
    });
    assert!(response.status.ok);
    let CommandResponse::Members { members } = response.response else {
        panic!("members expected")
    };
    assert_eq!(members.len(), 160);
    assert!(members.contains(&b"member-00000000".to_vec()));
    assert!(members.contains(&b"member-00000159".to_vec()));
}

#[test]
fn set_rewrites_and_readds_keep_one_live_copy_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        write(&engine, append(0, 32));
        write(&engine, append(0, 32));
        write(
            &engine,
            vec![Command::SetRemove {
                key: "order".into(),
                member: b"member-00000005".to_vec(),
            }],
        );
        write(&engine, append(5, 1));
        assert_dirty(&engine, "order");
        let shards = engine.shards.read().unwrap();
        assert_eq!(
            shards
                .get(&1)
                .unwrap()
                .bucket_index
                .block_refs_for("set", "order", None)
                .unwrap()
                .len(),
            32
        );
    }
    let recovered = open(dir.path());
    let response = recovered.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: "order".into(),
        },
    });
    assert!(response.status.ok);
    let CommandResponse::Members { members } = response.response else {
        panic!("members expected")
    };
    assert_eq!(members.len(), 32);
    assert!(members.contains(&b"member-00000005".to_vec()));
}

#[test]
fn appended_set_install_still_records_exactly_once() {
    use crate::engine::recorded_map::{install_appended_element, SetKind};
    let _ = block_in_wal::take_outcomes();
    let mut shard = ShardState::default();
    shard.set_shard_id(1);
    shard.set_routing_range(0, crate::DEFAULT_END_ROUTING_BUCKET);
    let member = b"new-member".to_vec();
    let address = ElementEntry::from_parts(3, 128, 64, Some(1), Some(1));
    install_appended_element::<SetKind>(
        &mut shard,
        1,
        "fresh",
        Some(execute_on_shard::set_element_name(&member)),
        member.clone(),
        address.clone(),
        true,
        address,
    );
    let records = block_in_wal::take_outcomes();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, "set");
    assert!(!records[0].deleted);
    assert!(shard
        .bucket_index
        .sets
        .get("fresh")
        .unwrap()
        .contains_key(&member));
    assert_eq!(
        shard
            .bucket_index
            .block_refs_for("set", "fresh", None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn rejected_commands_do_not_skip_dirty_propagation_for_successful_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    write(&engine, append(0, 16));
    engine.unload_shard(1);
    engine.load_shard(1);
    let mut commands = append(16, 1);
    commands.push(Command::SetScan {
        key: "order".into(),
        after: Vec::new(),
        limit: 0,
    });
    commands.extend(append(17, 1));
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert_eq!(response.responses.len(), 3);
    assert!(response.responses[0].status.ok);
    assert!(!response.responses[1].status.ok);
    assert!(response.responses[2].status.ok);
    assert_dirty(&engine, "order");
}

#[test]
fn fresh_hash_batches_do_not_scan_preexisting_refs_per_field() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage,
                        ..Config::default()
                    },
                })
                .ok
        );
        let hash = |start, count| {
            vec![Command::HashMultiSet {
                key: "catalog".into(),
                entries: (start..start + count)
                    .map(|i| (format!("field-{i:08}"), vec![7; 64]))
                    .collect(),
            }]
        };
        write(&engine, hash(0, 2_048));
        reset();
        write(&engine, hash(2_048, 128));
        assert_eq!(counts(), (0, 2_176));
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashLen {
                key: "catalog".into(),
            },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Integer { value: 2_176 });
    }
}
