// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(root: &std::path::Path, cache: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 << 20, root.join(cache), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config { version: 2, async_storage: true, ..Config::default() },
    }).ok);
    engine
}

fn run(engine: &TemporalEngine, batch: bool, command: Command) {
    if batch {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1, commands: vec![command],
        });
        assert!(response.status.ok, "{:?}", response.status);
        assert_eq!(response.responses.len(), 1);
        assert!(response.responses[0].status.ok, "{:?}", response.responses[0].status);
    } else {
        let response = engine.execute(ExecuteRequest { shard_id: 1, command });
        assert!(response.status.ok, "{:?}", response.status);
    }
}

fn seed(engine: &TemporalEngine, count: usize) {
    for index in 0..count {
        for command in [
            Command::StringSet { key: format!("s-{index}"), value: vec![index as u8; 32] },
            Command::HashSet {
                key: format!("h-{index}"), field: "f".into(), value: vec![index as u8; 32],
            },
        ] { run(engine, false, command); }
    }
}

fn live_blocks(engine: &TemporalEngine) -> Vec<String> {
    engine.shards.with_state(1, |shard_state| {
        let shard = shard_state.unwrap();
        let mut rows = super::tests::blocks_with_identity(&shard.bucket_index).into_iter()
            .filter(|(_, _, _, block)| !block.deleted())
            .map(|(bucket, _, key, block)| format!(
                "{bucket}|{key}|{}|{}|{}|{}",
                block.model_id(), block.address.block_slab_id(),
                block.address.offset(), block.address.length(),
            )).collect::<Vec<_>>();
        rows.sort();
        rows
    })
}

fn backdate(engine: &TemporalEngine, key: &str) {
    engine.shards.with_state_mut(1, |shard_state| {
        set_expiry(shard_state.unwrap(), key.to_owned(), resolve_now_ms().saturating_sub(60_000));
    })
}

fn assert_no_reconstruct() {
    let (visits, ..) = bucket_visit_sites::snapshot();
    assert_eq!(visits, 0, "TTL maintenance reconstructed unrelated blocks");
}

#[test]
fn ttl_metadata_updates_preserve_block_addresses_without_reconstructing() {
    for batch in [false, true] {
        for count in [32, 256] {
            let root = tempfile::tempdir().unwrap();
            let engine = open(root.path(), "cache");
            seed(&engine, count);
            let before = live_blocks(&engine);
            assert_eq!(before.len(), count * 2);
            for key in ["s-0", "h-0"] {
                for command in [
                    Command::CommonExpire { key: key.into(), ttl_ms: 60_000 },
                    Command::CommonExpire { key: key.into(), ttl_ms: 120_000 },
                    Command::CommonPersist { key: key.into() },
                ] {
                    bucket_visit_sites::reset();
                    run(&engine, batch, command);
                    assert_no_reconstruct();
                    assert_eq!(before, live_blocks(&engine));
                    assert!(engine.shards.with_state(1, |state| state.unwrap().dirty_objects.contains(key)));
                }
            }
        }
    }
}

#[test]
fn ttl_updates_keep_container_feature_sequence_and_control_owners() {
    for batch in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path(), "cache");
        seed(&engine, 16);
        let now = resolve_now_ms();
        for command in [
            Command::SetAdd { key: "set".into(), member: b"m".to_vec() },
            Command::ListPush { key: "list".into(), member: b"m".to_vec(), left: false },
            Command::ZSetAdd { key: "zset".into(), member: b"m".to_vec(), score: 1.0 },
            Command::FeatureAppend { key: "feature".into(), points: vec![FeaturePoint {
                timestamp_ms: now, value: b"point".to_vec(),
            }] },
            Command::SequenceAdd { key: "sequence".into(), rows: vec![SequenceFeatureRow {
                timestamp_ms: now, gid: 2, action_type: 3, duration: 4, author_id: 5,
            }] },
            Command::ControlStateSet {
                family: ControlStateFamily::Counter, key: "counter".into(),
                timestamp_ms: now, amount: 7,
            },
        ] { run(&engine, false, command); }
        let counter = associated_record_keys("counter")[1].clone();
        let before = live_blocks(&engine);
        assert!(before.len() > 32);
        for key in ["set", "list", "zset", "feature", "sequence", counter.as_str()] {
            for command in [
                Command::CommonExpire { key: key.into(), ttl_ms: 3_600_000 },
                Command::CommonPersist { key: key.into() },
            ] {
                bucket_visit_sites::reset();
                run(&engine, batch, command);
                assert_no_reconstruct();
                assert_eq!(before, live_blocks(&engine));
            }
        }
    }
}

#[test]
fn ttl_lazy_persist_removes_only_expired_record_owners() {
    for batch in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path(), "cache");
        seed(&engine, 16);
        let before = live_blocks(&engine);
        assert_eq!(before.len(), 32);
        for key in ["s-0", "h-0"] {
            backdate(&engine, key);
            bucket_visit_sites::reset();
            run(&engine, batch, Command::CommonPersist { key: key.into() });
            assert_no_reconstruct();
        }
        let after = live_blocks(&engine);
        let expected = before.into_iter().filter(|row| {
            let key = row.split('|').nth(1).unwrap();
            key != "s-0" && key != "h-0"
        }).collect::<Vec<_>>();
        assert_eq!(after, expected);
        engine.shards.with_state(1, |shard_state| {
            let shard = shard_state.unwrap();
            assert!(!shard.strings().contains_key("s-0"));
            assert!(!shard.bucket_index.hashes.contains_key("h-0"));
            assert!(!shard.expires_at_ms().contains_key("s-0"));
            assert!(!shard.expires_at_ms().contains_key("h-0"));
        });
    }
}

#[test]
fn ttl_expire_keeps_live_base_when_related_record_expires() {
    for batch in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path(), "cache");
        seed(&engine, 16);
        let related = associated_record_keys("s-0")[1].clone();
        run(&engine, false, Command::StringSet { key: related.clone(), value: b"related".to_vec() });
        let before = live_blocks(&engine);
        backdate(&engine, &related);
        bucket_visit_sites::reset();
        run(&engine, batch, Command::CommonExpire { key: "s-0".into(), ttl_ms: 60_000 });
        assert_no_reconstruct();
        let after = live_blocks(&engine);
        let expected = before.into_iter()
            .filter(|row| row.split('|').nth(1).unwrap() != related).collect::<Vec<_>>();
        assert_eq!(after, expected);
        engine.shards.with_state(1, |shard_state| {
            let shard = shard_state.unwrap();
            assert!(shard.strings().contains_key("s-0"));
            assert!(shard.expires_at_ms().contains_key("s-0"));
            assert!(!shard.strings().contains_key(related.as_str()));
            assert!(!shard.expires_at_ms().contains_key(related.as_str()));
        });
    }
}

#[test]
fn ttl_metadata_and_lazy_deletion_survive_reopen_before_reads() {
    for batch in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path(), "cache");
        seed(&engine, 16);
        run(&engine, batch, Command::CommonExpire { key: "s-1".into(), ttl_ms: 3_600_000 });
        run(&engine, batch, Command::CommonExpire { key: "h-1".into(), ttl_ms: 3_600_000 });
        run(&engine, batch, Command::CommonPersist { key: "h-1".into() });
        for key in ["s-0", "h-0"] {
            run(&engine, false, Command::CommonExpire { key: key.into(), ttl_ms: 1 });
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        for key in ["s-0", "h-0"] {
            run(&engine, batch, Command::CommonPersist { key: key.into() });
        }
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let reopened = open(root.path(), "reopened-cache");
        reopened.shards.with_state(1, |shard_state| {
            let shard = shard_state.unwrap();
            assert!(!shard.strings().contains_key("s-0"));
            assert!(!shard.bucket_index.hashes.contains_key("h-0"));
            assert!(shard.strings().contains_key("s-1"));
            assert!(shard.bucket_index.hashes.contains_key("h-1"));
            assert!(shard.expires_at_ms().contains_key("s-1"));
            assert!(!shard.expires_at_ms().contains_key("h-1"));
            assert_eq!(shard.bucket_index.objects_naming_blocks(), 30);
        });
    }
}
