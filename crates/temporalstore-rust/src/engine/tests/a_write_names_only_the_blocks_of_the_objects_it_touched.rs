// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WRITE NAMES THE BLOCKS OF THE OBJECTS IT TOUCHED, NOT THE BLOCKS OF THE SHARD.
//!
//! The index-log record of a write is built by a walk over the buckets the written keys route to.
//! Each row states the object a block belongs to, and that identity comes from an inversion of the
//! shard's object -> block-refs maps ([`BlockKeysByPosition`]). The inversion was built over EVERY
//! object in the shard on every write -- two hash maps grown to the shard's block count -- and the
//! walk then discarded every row whose object was not one of the written keys. Measured, a single
//! set removal from a four-member set allocated:
//!
//! ```text
//!   objects in the shard        bytes allocated per removal
//!                   100                         80 KB
//!                 2,000                        2.4 MB
//! ```
//!
//! [`BlockKeysByPosition::from_parts_for_keys`] builds the same inversion for the named keys only.
//!
//! TWO ARMS, BECAUSE THEY GUARD DIFFERENT FAILURES.
//!   * `a_restricted_inversion_names_what_the_whole_shard_one_names_for_its_keys` runs in a default
//!     build and states EQUIVALENCE: across add, remove, remove-and-re-add, delete, expiry and a
//!     reload, every block (live or deleted) is named exactly as before for the objects asked
//!     about, a live block of any other object is not named, and the rows the walk emits are the
//!     rows the whole-shard inversion would have produced. It passes with the narrowing replaced by
//!     the whole-shard constructor too -- that is what equivalence means -- so it is not the guard
//!     for the speed-up.
//!   * `a_write_does_not_pay_for_the_objects_it_did_not_touch` needs `alloc-probe` and `--ignored`.
//!     It varies the NUMBER OF OBJECTS in the shard and carries a control: the whole-shard
//!     inversion must grow with it, or the probe is blind to the dimension.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::SequenceFeatureRow;
use std::collections::BTreeSet;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn member(j: usize) -> Vec<u8> {
    format!("m{j:03}").into_bytes()
}

fn routing_range(engine: &TemporalEngine) -> (u32, u32) {
    let infos = engine.infos.read().expect("info lock poisoned");
    let info = infos.get(&1).expect("shard info");
    (info.start_routing_bucket, info.end_routing_bucket)
}

/// What a checkpoint saw, so a run that compared nothing cannot pass.
#[derive(Default, Debug)]
struct Tally {
    live_named_in_subset: usize,
    live_other_object_left_unnamed: usize,
    deleted_blocks: usize,
    deleted_blocks_named: usize,
    emitted_rows: usize,
    emitted_deleted_rows: usize,
}

fn subsets(all: &[String]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    out.push(Vec::new());
    out.push(vec!["no-such-object".to_string()]);
    for key in all {
        out.push(vec![key.clone()]);
    }
    for pair in all.windows(2) {
        out.push(vec![pair[0].clone(), pair[1].clone(), "no-such-object".to_string()]);
    }
    out.push(all.to_vec());
    out
}

fn check(engine: &TemporalEngine, all: &[String], tally: &mut Tally) {
    use crate::engine::state::BlockKeysByPosition;
    let (start, end) = routing_range(engine);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let index = &shard.bucket_index;
    assert!(!index.bucket_map.is_empty(), "no bucket index was built");
    let full = BlockKeysByPosition::from_parts(&index.objects, &index.object_block_lookup);

    for subset in subsets(all) {
        let wanted: BTreeSet<&str> = subset.iter().map(String::as_str).collect();
        let restricted = BlockKeysByPosition::from_parts_for_keys(
            &index.objects,
            &index.object_block_lookup,
            wanted.iter().copied(),
        );
        for (_, node) in index.bucket_map.iter() {
            for (handle, block) in node.block_index.iter() {
                let before = full
                    .identity_of(node, *handle, block)
                    .map(|identity| identity.object_key.to_string());
                let after = restricted
                    .identity_of(node, *handle, block)
                    .map(|identity| identity.object_key.to_string());
                if block.deleted() {
                    // A DELETED BLOCK IS NAMED BY ITS BUCKET'S TOMBSTONE ROW, never by the
                    // inversion, so restricting the inversion must not move the answer at all --
                    // whichever objects were asked about.
                    assert_eq!(before, after, "a deleted block is named differently");
                    tally.deleted_blocks += 1;
                    if before.is_some() {
                        tally.deleted_blocks_named += 1;
                    }
                } else {
                    match &before {
                        Some(key) if wanted.contains(key.as_str()) => {
                            assert_eq!(before, after, "a live block of a named object differs");
                            tally.live_named_in_subset += 1;
                        }
                        _ => {
                            assert_eq!(
                                None, after,
                                "a live block of an object that was not asked about was named"
                            );
                            tally.live_other_object_left_unnamed += 1;
                        }
                    }
                }
            }
        }

        // END TO END: the rows the walk emits for these keys against the rows the whole-shard
        // inversion yields for the same keys, compared as sets of everything a row states.
        let keys: Vec<String> = subset.clone();
        let items = collect_command_index_items_for(shard, &keys, start, end, None);
        let row = |routing_bucket: u32, handle: &str, object: &str, model: &str, deleted: bool| {
            format!("{routing_bucket}|{handle}|{object}|{model}|{deleted}")
        };
        let mut got: Vec<String> = items
            .iter()
            .map(|item| {
                row(
                    item.routing_bucket,
                    &item.block_ref_key,
                    &item.object_key,
                    &item.model_id,
                    item.deleted,
                )
            })
            .collect();
        let buckets: BTreeSet<u32> = wanted
            .iter()
            .map(|key| block_routing_bucket(key, start, end))
            .collect();
        let mut expected: Vec<String> = Vec::new();
        for routing_bucket in buckets {
            let Some(node) = index.bucket_map.get(&routing_bucket) else {
                continue;
            };
            for (handle, block) in node.block_index.iter() {
                let Some(beside) = full.identity_of(node, *handle, block) else {
                    continue;
                };
                if wanted.contains(beside.object_key.as_ref()) {
                    expected.push(row(
                        routing_bucket,
                        &handle.to_string(),
                        beside.object_key,
                        block.model_id().as_str(),
                        block.deleted(),
                    ));
                }
            }
        }
        got.sort();
        expected.sort();
        assert_eq!(expected, got, "the walk emitted different rows for {subset:?}");
        tally.emitted_rows += got.len();
        tally.emitted_deleted_rows += items.iter().filter(|item| item.deleted).count();
    }
}

#[test]
fn a_restricted_inversion_names_what_the_whole_shard_one_names_for_its_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let mut tally = Tally::default();

    let mut all: Vec<String> = Vec::new();
    for i in 0..3 {
        for prefix in ["s", "z", "l", "h", "x", "q"] {
            all.push(format!("{prefix}-{i}"));
        }
    }

    // ADD
    for i in 0..3 {
        for j in 0..6 {
            run(&engine, Command::SetAdd { key: format!("s-{i}"), member: member(j) });
            run(
                &engine,
                Command::ZSetAdd { key: format!("z-{i}"), member: member(j), score: j as f64 },
            );
            run(
                &engine,
                Command::ListPush { key: format!("l-{i}"), member: member(j), left: false },
            );
            run(
                &engine,
                Command::HashSet {
                    key: format!("h-{i}"),
                    field: format!("f{j}"),
                    value: vec![b'v'; 16],
                },
            );
            run(
                &engine,
                Command::SequenceAdd {
                    key: format!("q-{i}"),
                    rows: vec![SequenceFeatureRow {
                        timestamp_ms: 1_787_270_070_000 + j as u64 * 1000,
                        gid: j as u64,
                        action_type: 1,
                        duration: 5,
                        author_id: 9,
                    }],
                },
            );
        }
        run(&engine, Command::StringSet { key: format!("x-{i}"), value: vec![b'a'; 32] });
    }
    check(&engine, &all, &mut tally);

    // REMOVE
    for i in 0..3 {
        run(&engine, Command::SetRemove { key: format!("s-{i}"), member: member(1) });
        run(&engine, Command::ZSetRemove { key: format!("z-{i}"), member: member(2) });
        run(&engine, Command::ListPop { key: format!("l-{i}"), left: true });
        run(&engine, Command::HashDelete { key: format!("h-{i}"), field: "f3".to_string() });
    }
    check(&engine, &all, &mut tally);

    // REMOVE, THEN ADD THE SAME ELEMENT BACK
    run(&engine, Command::SetAdd { key: "s-0".to_string(), member: member(1) });
    run(&engine, Command::ZSetAdd { key: "z-0".to_string(), member: member(2), score: 2.0 });
    run(&engine, Command::HashSet { key: "h-0".to_string(), field: "f3".to_string(), value: vec![b'w'; 8] });
    check(&engine, &all, &mut tally);

    // DELETE THE WHOLE OBJECT
    for key in ["s-1", "z-1", "l-1", "h-1", "x-1", "q-1"] {
        run(&engine, Command::CommonDelete { key: key.to_string() });
    }
    check(&engine, &all, &mut tally);

    // EXPIRE: a deadline in the past, then a read that has to notice it
    run(&engine, Command::CommonExpire { key: "s-2".to_string(), ttl_ms: 1 });
    run(&engine, Command::CommonExpire { key: "x-2".to_string(), ttl_ms: 1 });
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers { key: "s-2".to_string() },
    });
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: "x-2".to_string() },
    });
    check(&engine, &all, &mut tally);

    // RELOAD
    engine.unload_shard(1);
    engine.load_shard(1);
    check(&engine, &all, &mut tally);

    println!("{tally:?}");
    // DENOMINATORS. A run that compared nothing, or never met a deleted block, proves nothing
    // about the arm that matters.
    assert!(tally.live_named_in_subset > 200, "{tally:?}");
    assert!(tally.live_other_object_left_unnamed > 200, "{tally:?}");
    assert!(tally.deleted_blocks > 0, "no deleted block was ever compared: {tally:?}");
    assert!(
        tally.deleted_blocks_named > 0,
        "no deleted block was ever named by a tombstone row: {tally:?}"
    );
    assert!(tally.emitted_rows > 200, "{tally:?}");
    assert!(tally.emitted_deleted_rows > 0, "{tally:?}");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;
    use crate::engine::state::BlockKeysByPosition;

    const TARGET: &str = "target-set";

    /// A shard of `objects` four-member sets plus the target set, built before any span opens.
    fn shard_of(objects: usize) -> (tempfile::TempDir, Arc<TemporalEngine>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        for i in 0..objects {
            for j in 0..4 {
                run(&engine, Command::SetAdd { key: format!("o-{i:05}"), member: member(j) });
            }
        }
        for j in 0..4 {
            run(&engine, Command::SetAdd { key: TARGET.to_string(), member: member(j) });
        }
        (dir, engine)
    }

    /// (allocations, bytes) of the walk that builds a write's index-log rows for `TARGET`.
    fn walk_cost(engine: &TemporalEngine) -> (u64, u64, usize) {
        let (start, end) = routing_range(engine);
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let keys = vec![TARGET.to_string()];
        let probe = Probe::start();
        let items = collect_command_index_items_for(shard, &keys, start, end, None);
        let counts = probe.stop();
        (counts.allocs, counts.alloc_bytes, items.len())
    }

    /// (allocations, bytes) of the WHOLE-SHARD inversion, the cost this removes from the walk.
    fn whole_shard_cost(engine: &TemporalEngine) -> (u64, u64) {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let index = &shard.bucket_index;
        let probe = Probe::start();
        let keys = BlockKeysByPosition::from_parts(&index.objects, &index.object_block_lookup);
        let counts = probe.stop();
        assert!(!keys.names_no_position());
        (counts.allocs, counts.alloc_bytes)
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_write_does_not_pay_for_the_objects_it_did_not_touch() {
        const SMALL: usize = 20;
        const LARGE: usize = 1000;
        let (_dir_small, small) = shard_of(SMALL);
        let (_dir_large, large) = shard_of(LARGE);

        // THE CONTROL, FIRST. The whole-shard inversion is proportional to the objects in the
        // shard by construction. If this probe cannot see that, a flat walk below means nothing.
        let (_, whole_small) = whole_shard_cost(&small);
        let (_, whole_large) = whole_shard_cost(&large);
        println!("control  whole-shard inversion: {whole_small} B at {SMALL} objects, {whole_large} B at {LARGE}");
        assert!(
            whole_large > whole_small * 10,
            "the control did not grow with the object count ({whole_small} -> {whole_large}), so \
             this probe is blind to that dimension"
        );

        let (allocs_small, bytes_small, rows_small) = walk_cost(&small);
        let (allocs_large, bytes_large, rows_large) = walk_cost(&large);
        println!("walk: {allocs_small} allocs / {bytes_small} B at {SMALL} objects, {allocs_large} allocs / {bytes_large} B at {LARGE}");
        // DENOMINATOR: the walk must have found the target's blocks, or a free walk is an empty one.
        assert!(rows_small >= 4 && rows_small == rows_large, "{rows_small} vs {rows_large} rows");
        assert!(
            bytes_large <= bytes_small + bytes_small / 10 + 512,
            "the walk allocated {bytes_small} B at {SMALL} objects and {bytes_large} B at {LARGE}: \
             it still pays for the objects it did not touch"
        );
        assert!(
            allocs_large <= allocs_small + 4,
            "the walk made {allocs_small} allocations at {SMALL} objects and {allocs_large} at \
             {LARGE}: it still pays for the objects it did not touch"
        );
    }
}
