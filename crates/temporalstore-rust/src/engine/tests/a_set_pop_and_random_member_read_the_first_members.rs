// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `SPOP` AND `SRANDMEMBER` READ THE FIRST MEMBERS THEY NAME, NOT THE WHOLE SET.
//!
//! Both listed every member, sorted them and took a prefix. They now page the set scan (which
//! reads the ordered member map) for exactly as many members as the answer can name, so the cost
//! follows the count asked for and not the size of the set.
//!
//! WHAT DID NOT MOVE: the members returned and their order (byte order, the order the sorted
//! listing gave), the repetition rule for a negative count, and every stored byte.
//!
//! - `the_door_answers_what_the_sorted_listing_answered`: over a small set holding the empty
//!   member and binary members, and over a set larger than one scan page, against an oracle
//!   that sorts the members the test itself wrote.
//! - `the_door_reads_a_prefix_and_never_lists`: the commands the door issues: no `SetMembers`,
//!   and the number of scan pages is the number the count needs.
//! - `probe::a_prefix_read_allocates_the_same_over_twenty_members_and_over_two_thousand`.
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
            Command::SetScan { .. } => "SetScan",
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

fn add_all(engine: &TemporalEngine, key: &str, members: &[Vec<u8>]) {
    for member in members {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
}

fn bulks(members: &[Vec<u8>]) -> RespValue {
    RespValue::Array(
        members
            .iter()
            .map(|member| RespValue::Bulk(Some(member.clone())))
            .collect(),
    )
}

fn small_set() -> Vec<Vec<u8>> {
    // Written out of order; sorted it is: "", "a", "ab", [0,255], "b", "zz".
    vec![
        b"zz".to_vec(),
        b"ab".to_vec(),
        Vec::new(),
        vec![0, 255],
        b"a".to_vec(),
        b"b".to_vec(),
    ]
}

#[test]
fn the_door_answers_what_the_sorted_listing_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let mut log = Vec::new();
    let mut sorted = small_set();
    sorted.sort();
    add_all(&engine, "s", &small_set());
    let mut run = |args: &[&[u8]]| door(&engine, &mut log, args);

    // SRANDMEMBER: none, within the size, past it, zero.
    assert_eq!(RespValue::Bulk(Some(sorted[0].clone())), run(&[b"SRANDMEMBER", b"s"]));
    assert_eq!(bulks(&sorted[..3]), run(&[b"SRANDMEMBER", b"s", b"3"]));
    assert_eq!(bulks(&sorted), run(&[b"SRANDMEMBER", b"s", b"50"]));
    assert_eq!(RespValue::Array(Vec::new()), run(&[b"SRANDMEMBER", b"s", b"0"]));
    // Negative: repeats cycle the members in order, within the size and past it.
    assert_eq!(bulks(&sorted[..2]), run(&[b"SRANDMEMBER", b"s", b"-2"]));
    let mut cycled: Vec<Vec<u8>> = Vec::new();
    for index in 0..9 {
        cycled.push(sorted[index % sorted.len()].clone());
    }
    assert_eq!(bulks(&cycled), run(&[b"SRANDMEMBER", b"s", b"-9"]));
    assert_eq!(RespValue::Array(Vec::new()), run(&[b"SRANDMEMBER", b"missing", b"-4"]));
    assert_eq!(RespValue::Bulk(None), run(&[b"SRANDMEMBER", b"missing"]));
    // SPOP removes exactly what it returns, smallest first.
    assert_eq!(RespValue::Bulk(Some(sorted[0].clone())), run(&[b"SPOP", b"s"]));
    assert_eq!(bulks(&sorted[1..3]), run(&[b"SPOP", b"s", b"2"]));
    assert_eq!(RespValue::Array(Vec::new()), run(&[b"SPOP", b"s", b"0"]));
    assert_eq!(RespValue::Integer(3), run(&[b"SCARD", b"s"]));
    assert_eq!(bulks(&sorted[3..]), run(&[b"SPOP", b"s", b"50"]));
    assert_eq!(RespValue::Integer(0), run(&[b"SCARD", b"s"]));
    assert_eq!(RespValue::Bulk(None), run(&[b"SPOP", b"s"]));

    // A set LARGER THAN ONE SCAN PAGE (10,000), so the paging is what is asserted.
    let big: Vec<Vec<u8>> = (0..10_050).map(|n| format!("m{n:05}").into_bytes()).collect();
    add_all(&engine, "big", &big);
    let mut run = |args: &[&[u8]]| door(&engine, &mut log, args);
    assert_eq!(bulks(&big[..10_030]), run(&[b"SRANDMEMBER", b"big", b"10030"]));
    assert_eq!(bulks(&big), run(&[b"SRANDMEMBER", b"big", b"20000"]));
    let popped = run(&[b"SPOP", b"big", b"10020"]);
    assert_eq!(bulks(&big[..10_020]), popped);
    assert_eq!(RespValue::Integer(30), run(&[b"SCARD", b"big"]));
    assert_eq!(RespValue::Bulk(Some(big[10_020].clone())), run(&[b"SPOP", b"big"]));
}

#[test]
fn the_door_reads_a_prefix_and_never_lists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let members: Vec<Vec<u8>> = (0..10_050).map(|n| format!("m{n:05}").into_bytes()).collect();
    add_all(&engine, "s", &members);
    for (label, args, pages) in [
        ("SRANDMEMBER", vec![&b"SRANDMEMBER"[..], b"s"], 1),
        ("SRANDMEMBER 5", vec![&b"SRANDMEMBER"[..], b"s", b"5"], 1),
        ("SRANDMEMBER -5", vec![&b"SRANDMEMBER"[..], b"s", b"-5"], 1),
        ("SRANDMEMBER 10001", vec![&b"SRANDMEMBER"[..], b"s", b"10001"], 2),
        ("SPOP", vec![&b"SPOP"[..], b"s"], 1),
        ("SPOP 3", vec![&b"SPOP"[..], b"s", b"3"], 1),
    ] {
        let mut issued = Vec::new();
        door(&engine, &mut issued, &args);
        assert!(!issued.contains(&"SetMembers"), "{label} listed the set: {issued:?}");
        let scans = issued.iter().filter(|name| **name == "SetScan").count();
        assert_eq!(pages, scans, "{label}: scan pages issued {issued:?}");
    }
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 20;

    fn per_call(count: usize, listing: bool) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        let members: Vec<Vec<u8>> =
            (0..count).map(|n| format!("member-{n:06}").into_bytes()).collect();
        add_all(&engine, "s", &members);
        let mut log = Vec::new();
        let mut ask = |log: &mut Vec<&'static str>| {
            if listing {
                let _ = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SetMembers { key: "s".to_string() },
                });
            } else {
                let _ = door(&engine, log, &[b"SRANDMEMBER", b"s", b"3"]);
            }
        };
        ask(&mut log);
        log.clear();
        let probe = Probe::start();
        for _ in 0..OPS {
            ask(&mut log);
            log.clear();
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_prefix_read_allocates_the_same_over_twenty_members_and_over_two_thousand() {
        let small = per_call(20, false);
        let large = per_call(2000, false);
        let listing_small = per_call(20, true);
        let listing_large = per_call(2000, true);
        println!(
            "MEAS SRANDMEMBER 3: {small:.1} @20, {large:.1} @2000; listing control {listing_small:.1} @20, {listing_large:.1} @2000"
        );
        assert!(
            listing_large >= listing_small + 1000.0,
            "the control listing must allocate per member ({listing_small} -> {listing_large})"
        );
        assert!(
            (large - small).abs() <= 2.0,
            "SRANDMEMBER 3 spent {small:.1} allocations at 20 members and {large:.1} at 2,000"
        );
    }
}
