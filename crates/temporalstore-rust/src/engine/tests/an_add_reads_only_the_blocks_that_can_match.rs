// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN ADD READS THE BLOCKS OF THE REFS THAT CAN MATCH, NOT EVERY BLOCK THE OBJECT HOLDS.
//!
//! Filing a block for a container member asks which of the object's existing blocks it supersedes:
//! the one at the landing address and, for a rewrite, the one being left. The filter asked a block
//! about EVERY ref the object held, two lookups each, so an add cost the member count. Callgrind,
//! 20 adds, debug build, instructions per add at 128 / 512 / 2,048 members:
//!
//! ```text
//!   hash   1.11M  2.84M  10.3M      set 1.02M  2.31M  7.81M
//!   list   1.22M  3.08M  11.05M     zset 0.97M 2.28M  7.83M
//! ```
//!
//! 62.5% of the hash add at 2,048 members was that filter. The refs are now narrowed by handle (a
//! hash of the model, the object key and the address, which the two addresses in hand give) and the
//! block is read for the one or two that carry it. Beside it, in every test build, the old filter
//! runs and the two must agree.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::storage_bucket_internals::{set_supersede_by_every_ref, supersede_block_reads};
use crate::types::CommandResponse;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn add(model: &str, index: usize) -> Command {
    let member = format!("m{index:06}").into_bytes();
    match model {
        "hash" => Command::HashSet {
            key: "k".to_string(),
            field: format!("f{index:06}"),
            value: vec![b'v'; 32],
        },
        "zset" => Command::ZSetAdd { key: "k".to_string(), member, score: index as f64 },
        "set" => Command::SetAdd { key: "k".to_string(), member },
        "list" => Command::ListPush { key: "k".to_string(), member, left: false },
        other => panic!("unknown model {other}"),
    }
}

fn reads_per_add(model: &str, members: usize) -> f64 {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    for index in 0..members {
        run(&engine, add(model, index));
    }
    let before = supersede_block_reads();
    for step in 0..20 {
        run(&engine, add(model, members + step));
    }
    (supersede_block_reads() - before) as f64 / 20.0
}

#[test]
fn an_add_reads_only_the_blocks_that_can_match() {
    for model in ["hash", "set", "zset", "list"] {
        let small = reads_per_add(model, 100);
        let large = reads_per_add(model, 1600);
        eprintln!("  {model}: {small:.1} blocks read per add at 100 members, {large:.1} at 1,600");
        // THE CONTROL: asked about every ref, the cost is the member count, or the counter is blind.
        set_supersede_by_every_ref(true);
        let control = reads_per_add(model, 1600);
        set_supersede_by_every_ref(false);
        assert!(control > 1_000.0, "{model}: the control read {control:.1} blocks per add at 1,600 members");
        assert!(
            small <= 2.0 && large <= 2.0,
            "{model}: an add read {small:.1} blocks at 100 members and {large:.1} at 1,600"
        );
    }
}

/// Overwrites, rescores, removals and re-adds, which are the writes that leave a block, with the
/// old filter asserting beside the new one on every one of them, then a reload that must serve
/// what the live shard served.
#[test]
fn a_history_of_rewrites_files_the_same_blocks_whichever_filter_names_them() {
    fn dump(engine: &TemporalEngine) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        if let CommandResponse::HashEntries { mut entries } = run(engine, Command::HashGetAll { key: "k".to_string() }) {
            entries.sort();
            out.push(entries.into_iter().flat_map(|(f, v)| [f.into_bytes(), v]).collect());
        }
        if let CommandResponse::Members { mut members } = run(engine, Command::SetMembers { key: "k".to_string() }) {
            members.sort();
            out.push(members);
        }
        if let CommandResponse::Members { members } =
            run(engine, Command::ZSetRange { key: "k".to_string(), start: 0, stop: -1, rev: false })
        {
            out.push(members);
        }
        if let CommandResponse::Members { members } =
            run(engine, Command::ListRange { key: "k".to_string(), start: 0, stop: -1 })
        {
            out.push(members);
        }
        out
    }
    let dir = tempfile::tempdir().unwrap();
    let live = {
        let engine = engine_on(dir.path());
        let mut seed = 7u64;
        let mut next = |bound: u64| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) % bound
        };
        for _ in 0..500 {
            let index = next(60) as usize;
            let member = format!("m{index:06}").into_bytes();
            match next(9) {
                0 => {
                    run(&engine, Command::HashSet { key: "k".to_string(), field: format!("f{index:06}"), value: vec![b'a' + next(20) as u8; 1 + next(40) as usize] });
                }
                1 => {
                    run(&engine, Command::HashDelete { key: "k".to_string(), field: format!("f{index:06}") });
                }
                2 => {
                    run(&engine, Command::SetAdd { key: "k".to_string(), member });
                }
                3 => {
                    run(&engine, Command::SetRemove { key: "k".to_string(), member });
                }
                4 | 5 => {
                    run(&engine, Command::ZSetAdd { key: "k".to_string(), member, score: next(1000) as f64 });
                }
                6 => {
                    run(&engine, Command::ZSetRemove { key: "k".to_string(), member });
                }
                7 => {
                    run(&engine, Command::ListPush { key: "k".to_string(), member, left: next(2) == 0 });
                }
                _ => {
                    run(&engine, Command::ListPop { key: "k".to_string(), left: next(2) == 0 });
                }
            }
        }
        let live = dump(&engine);
        engine.unload_shard(1);
        live
    };
    let engine = engine_on(dir.path());
    assert_eq!(dump(&engine), live, "a reload served other contents than the live shard did");
}
