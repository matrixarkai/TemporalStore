// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::TemporalEngine;
use crate::types::{
    BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue,
    Command, CommandResponse, ExecuteRequest,
};

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    assert!(engine.set_config(crate::SetConfigRequest {
        shard_id: 1,
        config: crate::Config { version: 2, async_storage: true, ..Default::default() },
    }).ok);
    engine
}

fn write_hash(engine: &TemporalEngine, generation: u8) {
    let (answer, _) = engine.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["a".into(), "b".into()],
        ttl_ms: None,
        rows: vec![BatchHashMultiSetRow("row".into(), vec![
            BatchHashMultiSetValue(vec![generation; 48]),
            BatchHashMultiSetValue(vec![generation + 1; 48]),
        ])],
    });
    assert!(answer.status.ok, "{answer:?}");
    assert_eq!(answer.applied, 1);
}

fn hash_address(engine: &TemporalEngine) -> crate::block_store::ElementEntry {
    engine.shards.read().unwrap().get(&1).unwrap()
        .bucket_index.hashes.get("row").unwrap().get("a").unwrap().clone()
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let answer = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(answer.status.ok, "{:?}", answer.status);
    answer.response
}

#[test]
fn captured_locators_survive_pruning_and_nested_scope_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let engine = open(directory.path());
    write_hash(&engine, 1);
    let object = crate::engine::stable_block_object_id(1, "hash", "row");
    let old = hash_address(&engine);
    let second = engine.shards.read().unwrap().get(&1).unwrap()
        .bucket_index.hashes.get("row").unwrap().get("b").unwrap().clone();
    assert_eq!(crate::engine::live_block_key(&old), crate::engine::live_block_key(&second));
    assert!(old.block_id().is_none(), "the fixture must exercise a WAL-backed address");
    let expected = read_block_at(&engine.block_store, 1, object, Some("a"), &old).unwrap();
    let mut outer = ReadSnapshotGuard::new(&engine.block_store, 1);
    outer.pin(&engine.block_store, object, Some("a"), &old);
    outer.pin(&engine.block_store, object, Some("b"), &second);
    READ_PINS.with(|slot| assert!(slot.borrow().as_ref().unwrap().rest.is_none()));
    write_hash(&engine, 7);
    let current = hash_address(&engine);
    assert_ne!(crate::engine::live_block_key(&old), crate::engine::live_block_key(&current));
    engine.cache.invalidate_shard(1).unwrap();
    assert_eq!(Some(expected.clone()), read_block_at(&engine.block_store, 1, object, Some("a"), &old));
    {
        let mut nested = ReadSnapshotGuard::new(&engine.block_store, 1);
        nested.pin(&engine.block_store, object, Some("a"), &current);
        assert!(read_block_at(&engine.block_store, 1, object, Some("a"), &old).is_none(),
            "the nested scope must not inherit an outer locator it did not capture");
        assert_ne!(Some(expected.clone()), read_block_at(&engine.block_store, 1, object, Some("a"), &current));
    }
    assert_eq!(Some(expected), read_block_at(&engine.block_store, 1, object, Some("a"), &old));
    drop(outer);
    READ_PINS.with(|slot| assert!(slot.borrow().is_none()));
    assert!(read_block_at(&engine.block_store, 1, object, Some("a"), &old).is_none(),
        "negative control: pruning must actually have removed the old live locator");
}

#[test]
fn fast_wal_reads_keep_complete_snapshots_after_concurrent_overwrite() {
    let directory = tempfile::tempdir().unwrap();
    let engine = open(directory.path());
    let cases = vec![
        (Command::HashGetAll { key: "row".into() }, CommandResponse::HashEntries {
            entries: vec![("a".into(), vec![1; 48]), ("b".into(), vec![2; 48])],
        }),
        (Command::HashGet { key: "row".into(), field: "a".into() },
            CommandResponse::Bytes { value: Some(vec![1; 48]) }),
        (Command::StringGet { key: "string".into() },
            CommandResponse::Bytes { value: Some(b"012345".to_vec()) }),
        (Command::StringLen { key: "string".into() }, CommandResponse::Integer { value: 6 }),
        (Command::StringGetRange { key: "string".into(), start: 1, end: 3 },
            CommandResponse::Bytes { value: Some(b"123".to_vec()) }),
    ];
    let mut completed = 0;
    for (command, expected) in cases {
        write_hash(&engine, 1);
        run(&engine, Command::StringSet { key: "string".into(), value: b"012345".to_vec() });
        engine.cache.invalidate_shard(1).unwrap();
        let writer = engine.clone();
        crate::engine::AFTER_FAST_READ_PLAN.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(Box::new(move || {
                assert!(writer.shards.try_write().is_ok(), "payload loading must not retain the index guard");
                std::thread::spawn(move || {
                    write_hash(&writer, 7);
                    run(&writer, Command::StringSet { key: "string".into(), value: b"replacement".to_vec() });
                    writer.cache.invalidate_shard(1).unwrap();
                }).join().unwrap();
            }));
        });
        assert_eq!(expected, run(&engine, command));
        crate::engine::AFTER_FAST_READ_PLAN.with(|slot| assert!(slot.borrow().is_none()));
        assert_eq!(CommandResponse::Bytes { value: Some(vec![7; 48]) },
            run(&engine, Command::HashGet { key: "row".into(), field: "a".into() }));
        completed += 1;
    }
    assert_eq!(completed, 5);
}
