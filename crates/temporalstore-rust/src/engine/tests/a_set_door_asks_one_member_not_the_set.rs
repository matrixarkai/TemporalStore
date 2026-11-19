// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `SADD`, `SREM`, `SMOVE` AND `TYPE` AT THE RESP DOOR ASK ABOUT ONE MEMBER, OR THE LENGTH, NOT
//! FOR THE WHOLE SET.
//!
//! Each of them listed every member of the set (`SetMembers`) to search it or to ask whether it
//! was empty. The cost followed the size of the set for an answer about one member. They now
//! ask about one member (`SetIsMember` for `SMOVE`, inside `SetAddMany` and `SetRemoveMany` for `SADD`
//! and `SREM`) or the length (`SetCard` for `TYPE`).
//!
//! WHAT DID NOT MOVE: the integers the door answers, the order the writes are issued in, and every
//! stored byte.
//!
//! - `the_door_answers_what_it_always_answered`: counts for duplicates, present and absent
//!   members, a move into the same key, and `TYPE` of a set, of an emptied set and of nothing.
//! - `the_door_never_lists_the_set`: records every command the door issues and asserts none is
//!   `SetMembers` while the lookups it needs ARE issued. A door that listed again fails the first
//!   half; a door that asked nothing fails the second.
//! - `probe::the_door_allocates_the_same_over_twenty_members_and_over_two_thousand`: allocation
//!   count of a door call is flat in the set size, with the listing as the control that must grow.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::redis::{execute_redis_command, RespValue};

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

fn door(engine: &TemporalEngine, issued: &mut Vec<&'static str>, args: &[&[u8]]) -> RespValue {
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.to_vec()).collect();
    execute_redis_command(argv, 1, |command| {
        issued.push(match &command {
            Command::SetMembers { .. } => "SetMembers",
            Command::SetIsMember { .. } => "SetIsMember",
            Command::SetAddMany { .. } => "SetAddMany",
            Command::SetRemoveMany { .. } => "SetRemoveMany",
            Command::SetCard { .. } => "SetCard",
            _ => "other",
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message.clone())
        }
    })
}

fn int(value: i64) -> RespValue {
    RespValue::Integer(value)
}

#[test]
fn the_door_answers_what_it_always_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let mut log = Vec::new();
    let mut run = |args: &[&[u8]]| door(&engine, &mut log, args);

    // A member named twice in one call is added once; a present one is not added again.
    assert_eq!(int(2), run(&[b"SADD", b"s", b"a", b"b", b"a"]));
    assert_eq!(int(1), run(&[b"SADD", b"s", b"b", b"c"]));
    assert_eq!(int(3), run(&[b"SCARD", b"s"]));
    // Removal: present counts, absent and repeated do not.
    assert_eq!(int(2), run(&[b"SREM", b"s", b"a", b"zz", b"b", b"a"]));
    assert_eq!(int(0), run(&[b"SREM", b"never", b"a"]));
    assert_eq!(int(1), run(&[b"SCARD", b"s"]));
    // A move: hit, miss, and a member moved into the key it is already in.
    assert_eq!(int(1), run(&[b"SMOVE", b"s", b"d", b"c"]));
    assert_eq!(int(0), run(&[b"SMOVE", b"s", b"d", b"c"]));
    assert_eq!(int(0), run(&[b"SISMEMBER", b"s", b"c"]));
    assert_eq!(int(1), run(&[b"SISMEMBER", b"d", b"c"]));
    assert_eq!(int(1), run(&[b"SMOVE", b"d", b"d", b"c"]));
    assert_eq!(int(1), run(&[b"SISMEMBER", b"d", b"c"]));
    // TYPE: a set, an emptied set, a key that is nothing.
    assert_eq!(RespValue::SimpleString("set".to_string()), run(&[b"TYPE", b"d"]));
    assert_eq!(int(1), run(&[b"SREM", b"d", b"c"]));
    assert_eq!(RespValue::SimpleString("none".to_string()), run(&[b"TYPE", b"d"]));
    assert_eq!(RespValue::SimpleString("none".to_string()), run(&[b"TYPE", b"nothing"]));
}

#[test]
fn the_door_never_lists_the_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let mut seed = Vec::new();
    for member in 0..50 {
        let name = format!("m{member:03}");
        door(&engine, &mut seed, &[b"SADD", b"s", name.as_bytes()]);
    }
    for (label, args) in [
        ("SADD", vec![&b"SADD"[..], b"s", b"m001", b"new"]),
        ("SREM", vec![&b"SREM"[..], b"s", b"m002", b"missing"]),
        ("SMOVE", vec![&b"SMOVE"[..], b"s", b"t", b"m003"]),
        ("TYPE", vec![&b"TYPE"[..], b"s"]),
    ] {
        let mut issued = Vec::new();
        door(&engine, &mut issued, &args);
        assert!(
            !issued.contains(&"SetMembers"),
            "{label} listed the whole set: {issued:?}"
        );
        // SADD and SREM name their members in one command (each asked about inside it, never by
        // listing the set); the others ask about one member or the length.
        let point = match label {
            "TYPE" => "SetCard",
            "SADD" => "SetAddMany",
            "SREM" => "SetRemoveMany",
            _ => "SetIsMember",
        };
        assert!(issued.contains(&point), "{label} never asked {point}: {issued:?}");
    }
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 20;

    /// Allocations per call of one door verb over a set of `count` members. `listing` swaps the
    /// call for the whole-set listing the door used to issue: the clone control.
    fn per_call(count: usize, verb: &str, listing: bool) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        let mut log = Vec::new();
        for index in 0..count {
            let name = format!("member-{index:06}");
            door(&engine, &mut log, &[b"SADD", b"s", name.as_bytes()]);
        }
        let ask = |engine: &TemporalEngine, log: &mut Vec<&'static str>| {
            if listing {
                let _ = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SetMembers { key: "s".to_string() },
                });
            } else {
                // SADD of a member already there: the lookup is the whole cost.
                let _ = match verb {
                    "SADD" => door(engine, log, &[b"SADD", b"s", b"member-000001"]),
                    _ => door(engine, log, &[b"TYPE", b"s"]),
                };
            }
        };
        ask(&engine, &mut log);
        log.clear();
        let probe = Probe::start();
        for _ in 0..OPS {
            ask(&engine, &mut log);
            log.clear();
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    /// The same count at 20 and at 2,000 members, within a margin; the listing must grow by at
    /// least one allocation per member or the probe cannot see that dimension.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn the_door_allocates_the_same_over_twenty_members_and_over_two_thousand() {
        let mut rows = Vec::new();
        for verb in ["SADD", "TYPE"] {
            let small = per_call(20, verb, false);
            let large = per_call(2000, verb, false);
            println!("MEAS {verb}: {small:.1} @20, {large:.1} @2000");
            rows.push((verb, small, large));
        }
        let listing_small = per_call(20, "SADD", true);
        let listing_large = per_call(2000, "SADD", true);
        println!("MEAS listing control: {listing_small:.1} @20, {listing_large:.1} @2000");
        assert!(
            listing_large >= listing_small + 1000.0,
            "the control listing must allocate per member ({listing_small} -> {listing_large})"
        );
        for (verb, small, large) in rows {
            assert!(
                (large - small).abs() <= 2.0,
                "{verb} spent {small:.1} allocations at 20 members and {large:.1} at 2,000"
            );
        }
    }
}
