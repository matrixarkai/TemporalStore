// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SORTED-SET REMOVAL OR A LIST POP STATES NO BLOCK ROWS, AS A SET REMOVAL ALREADY DID.
//!
//! The index-log record of a write lists blocks. A removal that cannot name what it removed fell
//! back to restating EVERY block the object holds, one row per resident block, so removing one
//! member of a thousand wrote a thousand rows and allocated for each. `SetRemove` and `HashDelete`
//! name their element and state none; `ZSetRemove` was left out on the belief that its element name
//! folds in the score -- true once, false since the score came off the string
//! (`zset_element_name` is `member_element_name`, the same spelling a set uses) -- and `ListPop` on
//! the belief that its name is the sequence it took, which the record does not need: a named
//! removal states no row, whatever the name is.
//!
//! WHAT MOVES ON DISK. The record of a `ZSetRemove` or a `ListPop` carries no block rows where it
//! carried one per block of the object. Nothing else in the record moves, and the shipped load path
//! reads none of them (the index-log fold is gated behind the legacy-recovery switch).
//!
//! TWO GUARDS, BECAUSE THEY FAIL ON DISJOINT MUTATIONS.
//!   * `a_removal_states_no_block_rows` reads the record a removal wrote, for all three container
//!     models, with a control that must see rows (the record of an ADD) so a reader that can only
//!     say zero cannot pass.
//!   * `a_removed_member_stays_removed_and_the_rest_stay_across_a_cold_reload` holds the content:
//!     what a reload serves is what the live shard served. It is a safety net and not a proof --
//!     the snapshot a normal unload writes would recover the same answer (see
//!     `zset_element_name_drops_the_score`) -- so it does not stand in for the first.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::CommandResponse;

fn engine_on(dir: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join(cache),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "removal".to_string(),
        shard_uri: "local://removal/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn member(index: usize) -> Vec<u8> {
    format!("rm-{index:04}").into_bytes()
}

#[derive(Clone, Copy, Debug)]
enum Family {
    Set,
    ZSet,
    List,
}

const FAMILIES: [Family; 3] = [Family::Set, Family::ZSet, Family::List];

fn fill(engine: &TemporalEngine, family: Family, members: usize) {
    for index in 0..members {
        run(
            engine,
            match family {
                Family::Set => Command::SetAdd {
                    key: "k".to_string(),
                    member: member(index),
                },
                Family::ZSet => Command::ZSetAdd {
                    key: "k".to_string(),
                    member: member(index),
                    score: index as f64,
                },
                Family::List => Command::ListPush {
                    key: "k".to_string(),
                    member: member(index),
                    left: false,
                },
            },
        );
    }
}

/// One removal; the list pops from the right so the member removed is the newest.
fn remove_one(engine: &TemporalEngine, family: Family, index: usize) {
    run(
        engine,
        match family {
            Family::Set => Command::SetRemove {
                key: "k".to_string(),
                member: member(index),
            },
            Family::ZSet => Command::ZSetRemove {
                key: "k".to_string(),
                member: member(index),
            },
            Family::List => Command::ListPop {
                key: "k".to_string(),
                left: false,
            },
        },
    );
}

fn contents(engine: &TemporalEngine, family: Family) -> Vec<Vec<u8>> {
    let response = run(
        engine,
        match family {
            Family::Set => Command::SetMembers {
                key: "k".to_string(),
            },
            Family::ZSet => Command::ZSetRange {
                key: "k".to_string(),
                start: 0,
                stop: -1,
                rev: false,
            },
            Family::List => Command::ListRange {
                key: "k".to_string(),
                start: 0,
                stop: -1,
            },
        },
    );
    match response {
        CommandResponse::Members { mut members } => {
            if matches!(family, Family::Set) {
                members.sort();
            }
            members
        }
        other => panic!("{family:?} read answered {other:?}"),
    }
}

/// Members, whatever the read returns: a sorted set answers a member and a score for each.
fn member_count(engine: &TemporalEngine, family: Family) -> usize {
    let length = contents(engine, family).len();
    if matches!(family, Family::ZSet) {
        length / 2
    } else {
        length
    }
}

/// The rows of the newest index-log record, and how many records the log holds.
fn newest_record(engine: &TemporalEngine) -> (usize, usize) {
    let records = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("delta records readable");
    (
        records.len(),
        records.last().map(|record| record.items.len()).unwrap_or(0),
    )
}

#[test]
fn a_removal_states_no_block_rows() {
    for family in FAMILIES {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path(), "cache");
        load(&engine);
        fill(&engine, family, 12);

        // THE CONTROL: the record of an ADD states its block, so a reader that can only say
        // "zero" cannot pass this guard.
        let (records_after_fill, add_rows) = newest_record(&engine);
        assert_eq!(
            add_rows, 1,
            "{family:?}: the last add's record states {add_rows} rows; the reader cannot see rows"
        );

        remove_one(&engine, family, 5);
        let (records_after_remove, remove_rows) = newest_record(&engine);
        assert_eq!(
            records_after_remove,
            records_after_fill + 1,
            "{family:?}: the removal did not write exactly one record"
        );
        assert_eq!(
            remove_rows, 0,
            "{family:?}: removing one member of twelve restated {remove_rows} block rows"
        );
        // DENOMINATOR: the removal took effect, so the zero is about a removal that happened.
        assert_eq!(member_count(&engine, family), 11, "{family:?}: nothing was removed");
    }
}

#[test]
fn a_removed_member_stays_removed_and_the_rest_stay_across_a_cold_reload() {
    for family in FAMILIES {
        let dir = tempfile::tempdir().unwrap();
        let before = {
            let engine = engine_on(dir.path(), "cache-writer");
            load(&engine);
            fill(&engine, family, 16);
            for index in [3usize, 9, 4] {
                remove_one(&engine, family, index);
            }
            // A re-add of a removed member must come back exactly once.
            if !matches!(family, Family::List) {
                run(
                    &engine,
                    match family {
                        Family::Set => Command::SetAdd {
                            key: "k".to_string(),
                            member: member(9),
                        },
                        _ => Command::ZSetAdd {
                            key: "k".to_string(),
                            member: member(9),
                            score: 90.0,
                        },
                    },
                );
            }
            let live = contents(&engine, family);
            // Sixteen added, three removed; a set and a sorted set take one back.
            let expected = if matches!(family, Family::List) { 13 } else { 14 };
            assert_eq!(
                member_count(&engine, family),
                expected,
                "{family:?}: the history did not leave the members it should"
            );
            engine.unload_shard(1);
            live
        };
        let engine = engine_on(dir.path(), "cache-reloaded");
        load(&engine);
        assert_eq!(contents(&engine, family), before, "{family:?}: a cold reload served other contents");
    }
}

/// A REMOVAL THAT STATES NO ROW DOES NOT BUILD THE INVERSION THAT NAMES ROWS.
///
/// Naming a row's object needs an inversion of the object's block refs, and for a removal from a
/// collection that is one entry per member in two hash maps -- built, and then every row it could
/// have named was filtered out. Measured on a ZSetRemove from a thousand-member key: 86% of its
/// instructions and ~307 KB of its ~307 KB, flat in the number of blocks it did not need to name.
/// The count of inversions is the dimension the cost lives in, and it does not depend on the box.
///
/// The control is a delete the engine cannot name: it still states its rows, so it must build one.
#[test]
fn a_named_removal_builds_no_identity_inversion() {
    let before = |engine: &TemporalEngine| {
        let _ = engine;
        crate::engine::state::keyed_block_inversions()
    };
    for family in FAMILIES {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path(), "cache");
        load(&engine);
        fill(&engine, family, 40);
        let started = before(&engine);
        remove_one(&engine, family, 7);
        assert_eq!(
            crate::engine::state::keyed_block_inversions(),
            started,
            "{family:?}: removing one member of forty built a block-identity inversion"
        );
        assert_eq!(member_count(&engine, family), 39, "{family:?}: nothing was removed");
    }
    // A hash field is named too.
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), "cache");
    load(&engine);
    for index in 0..40 {
        run(
            &engine,
            Command::HashSet {
                key: "h".to_string(),
                field: format!("f{index:03}"),
                value: vec![b'v'; 16],
            },
        );
    }
    let started = crate::engine::state::keyed_block_inversions();
    run(
        &engine,
        Command::HashDelete {
            key: "h".to_string(),
            field: "f007".to_string(),
        },
    );
    assert_eq!(
        crate::engine::state::keyed_block_inversions(),
        started,
        "a hash field removal built a block-identity inversion"
    );

    // THE CONTROL: a delete that names no element states its rows, and has to name them.
    run(
        &engine,
        Command::StringSet {
            key: "s".to_string(),
            value: vec![b'v'; 16],
        },
    );
    let started = crate::engine::state::keyed_block_inversions();
    run(&engine, Command::StringDelete { key: "s".to_string() });
    assert!(
        crate::engine::state::keyed_block_inversions() > started,
        "the unnamed delete built no inversion, so the counter cannot see one"
    );
}

/// Profiler and allocation driver, not a gate: `REM_FAMILY=set|zset|list REM_E=<members>
/// REM_K=<removals>`; run under `valgrind --tool=callgrind --toggle-collect='*removal_span*'`.
#[inline(never)]
fn removal_span(engine: &TemporalEngine, family: Family, removals: usize) {
    for index in 0..removals {
        remove_one(engine, family, index);
    }
}

#[test]
#[ignore]
fn a_removal_for_the_profiler() {
    let family = match std::env::var("REM_FAMILY").unwrap_or_default().as_str() {
        "set" => Family::Set,
        "list" => Family::List,
        _ => Family::ZSet,
    };
    let members = std::env::var("REM_E").ok().and_then(|v| v.parse().ok()).unwrap_or(1000usize);
    let removals = std::env::var("REM_K").ok().and_then(|v| v.parse().ok()).unwrap_or(20usize);
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), "cache");
    load(&engine);
    fill(&engine, family, members);
    #[cfg(feature = "alloc-probe")]
    let probe = crate::alloc_probe::Probe::start();
    removal_span(&engine, family, removals);
    #[cfg(feature = "alloc-probe")]
    {
        let counts = probe.stop();
        eprintln!(
            "REMOVAL {family:?} E={members} K={removals}: {} allocs {} bytes per removal",
            counts.allocs as f64 / removals as f64,
            counts.alloc_bytes as f64 / removals as f64
        );
    }
    let (_, rows) = newest_record(&engine);
    eprintln!("REMOVAL {family:?} E={members} K={removals}: newest record states {rows} rows");
}
