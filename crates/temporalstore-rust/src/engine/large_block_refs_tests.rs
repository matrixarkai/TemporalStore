// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::{BlockRefs, CoreIndex};
use crate::engine::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

fn open(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.status);
    outcome.response
}

fn assert_members(engine: &TemporalEngine, start: u64, end: u64) {
    let CommandResponse::Members { mut members } = run(
        engine,
        Command::SetMembers {
            key: "order".into(),
        },
    ) else {
        panic!("expected set members");
    };
    members.sort_unstable();
    let mut expected: Vec<_> = (start..end)
        .map(|index| index.to_be_bytes().to_vec())
        .collect();
    expected.sort_unstable();
    assert_eq!(members, expected);
}

#[test]
fn large_set_reference_tree_survives_index_flush_and_wal_restart() {
    for flush in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = open(dir.path());
            for index in 0..384u64 {
                run(
                    &engine,
                    Command::SetAdd {
                        key: "order".into(),
                        member: index.to_be_bytes().to_vec(),
                    },
                );
            }
            let shards = engine.shards.read().unwrap();
            let index = &shards[&1].bucket_index;
            assert!(matches!(
                index.object_block_refs("set", "order"),
                Some(BlockRefs::Large(_))
            ));
            let bytes = rmp_serde::to_vec_named(index).unwrap();
            let loaded: CoreIndex = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(
                loaded.object_block_refs("set", "order"),
                index.object_block_refs("set", "order")
            );
            drop(shards);
            if flush {
                engine.flush_shard_index(1);
            }
        }
        let engine = open(dir.path());
        assert_members(&engine, 0, 384);
        for index in 0..96u64 {
            run(
                &engine,
                Command::SetRemove {
                    key: "order".into(),
                    member: index.to_be_bytes().to_vec(),
                },
            );
        }
        assert_members(&engine, 96, 384);
        engine.flush_shard_index(1);
        drop(engine);
        assert_members(&open(dir.path()), 96, 384);
    }
}

#[test]
fn large_object_lookup_demotion_preserves_counts_order_and_last_ref_removal() {
    use super::{BlockIndex, BlockLookupRef};
    use crate::block_store::ElementEntry;
    use crate::engine::storage_bucket_internals::StoredModelKind;
    let mut index = CoreIndex::default();
    let key = std::sync::Arc::<str>::from("large");
    index.object_element_name_block_refs = Some(0);
    for handle in 0..512u64 {
        let block = BlockIndex::new(
            StoredModelKind::Set,
            ElementEntry::from_parts(1, handle * 64, 64, Some(handle), None),
            false,
            false,
        );
        index.insert_object_block_lookup(7, handle, &key, &block);
    }
    assert!(matches!(
        index.object_block_refs("set", "large"),
        Some(BlockRefs::Large(_))
    ));
    for handle in 0..512 {
        assert!(index.remove_object_block_lookup_ref(
            "set",
            "large",
            None,
            &BlockLookupRef {
                routing_bucket: 7,
                block_ref_key: handle
            }
        ));
        assert_eq!(
            index.object_element_name_block_refs,
            Some((511 - handle) as usize)
        );
        if handle == 383 {
            let refs = index.object_block_refs("set", "large").unwrap();
            assert!(matches!(refs, BlockRefs::Many(_)));
            assert!(refs.iter().map(|at| at.block_ref_key).eq(384..512));
        }
    }
    assert!(index.object_block_refs("set", "large").is_none());
}
