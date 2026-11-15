// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A DISTINCT COUNT COMPARES THE MEMBERS WHERE THEY ARE STORED, NOT COPIES OF THEM.
//!
//! The exact distinct count over a window took the union of the buckets' member sets by cloning
//! every member of every bucket into a fresh `BTreeSet<Vec<u8>>` and reading its length: one
//! allocation and one copy per member per read, for an answer that is a single number. The union
//! now holds borrows of the members, and a window that covers one bucket -- the common read -- is
//! that bucket's length and builds nothing.
//!
//! WHAT MOVES: a read's allocations stop growing with the members it counts (a window of one
//! bucket allocates none for them; a window of several allocates the set's own nodes, about one
//! block per ten members, and no copy of any member). WHAT DOES NOT: the count, which is the size
//! of the union of the members, and the sketch arm, which this does not touch.
//!
//! TWO GUARDS.
//!   * `a_distinct_count_answers_the_size_of_the_union` drives windows over buckets that share
//!     members -- one bucket, several, none, a window with no bucket in it, a key with nothing --
//!     against a union computed here.
//!   * `probe::a_distinct_count_does_not_copy_its_members` (needs `alloc-probe`, `--ignored`) holds
//!     the allocations of a count over ten times the members to a small multiple of the node count,
//!     with the one-bucket window flat, and carries a control: the union of clones the count used to
//!     build, made here over the same sets, must allocate per member, or the probe cannot see it.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::CommandResponse;
use std::collections::{BTreeMap, BTreeSet};

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
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

fn add(engine: &TemporalEngine, key: &str, at: u64, member: &[u8]) {
    run(
        engine,
        Command::ControlStateChangeAdd {
            key: key.to_string(),
            timestamp_ms: at,
            value: member.to_vec(),
            precision_ms: None,
            ttl_ms: None,
        },
    );
}

fn count(engine: &TemporalEngine, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    match run(
        engine,
        Command::ControlStateQuery {
            key: key.to_string(),
            start_ms,
            end_ms,
            aggregator: "change".to_string(),
        },
    ) {
        CommandResponse::Integer { value } => value,
        other => panic!("expected an integer, got {other:?}"),
    }
}

#[test]
fn a_distinct_count_answers_the_size_of_the_union() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut model: BTreeMap<u64, BTreeSet<Vec<u8>>> = BTreeMap::new();
    // Buckets that share members with their neighbours, one that is empty of overlap, and a
    // member added twice to the same bucket.
    let history: [(u64, &[u8]); 14] = [
        (1_000, b"a"),
        (1_000, b"b"),
        (1_000, b"a"),
        (1_010, b"b"),
        (1_010, b"c"),
        (1_020, b"c"),
        (1_020, b"d"),
        (1_020, b"e"),
        (1_100, b"a"),
        (1_100, b"z"),
        (1_100, b""),
        (2_000, b"only-here"),
        (3_000, b"aa"),
        (3_000, b"a"),
    ];
    for (at, member) in history {
        add(&engine, "k", at, member);
        model.entry(at).or_default().insert(member.to_vec());
    }
    let windows = [
        (0u64, u64::MAX),
        (1_000, 1_000),
        (1_000, 1_010),
        (1_005, 1_025),
        (1_010, 1_100),
        (1_101, 1_999),
        (2_000, 2_000),
        (2_500, 2_900),
        (1_000, 3_000),
        (9_000, 9_999),
    ];
    let mut multi_bucket = 0usize;
    for (start, end) in windows {
        let mut union: BTreeSet<&Vec<u8>> = BTreeSet::new();
        let mut buckets = 0usize;
        for (_, members) in model.range(start..=end) {
            union.extend(members.iter());
            buckets += 1;
        }
        multi_bucket += usize::from(buckets > 1);
        assert_eq!(
            count(&engine, "k", start, end),
            union.len() as i64,
            "window [{start}, {end}] over {buckets} bucket(s)"
        );
    }
    // THE CONTROL: the windows really spanned buckets that overlap, so the union is not a sum.
    assert!(multi_bucket >= 5, "only {multi_bucket} windows spanned several buckets");
    assert_eq!(count(&engine, "k", 1_000, 1_020), 5, "a, b, c, d, e");
    assert_eq!(count(&engine, "absent", 0, u64::MAX), 0);
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const READS: usize = 10;
    const BUCKETS: u64 = 4;

    /// Allocations per read of a count over `members` distinct members spread over BUCKETS buckets
    /// (`one_bucket`: the whole set in one), the window covering all of them.
    fn per_read(members: usize, one_bucket: bool) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = open(dir.path());
        for member in 0..members {
            let at = if one_bucket { 1_000 } else { 1_000 + (member as u64 % BUCKETS) * 10 };
            add(&engine, "k", at, format!("member-{member:06}").as_bytes());
        }
        let commands: Vec<Command> = (0..READS)
            .map(|_| Command::ControlStateQuery {
                key: "k".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                aggregator: "change".to_string(),
            })
            .collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / READS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_distinct_count_does_not_copy_its_members() {
        let canary = crate::alloc_probe::Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`"
        );
        drop(sink);

        // THE CONTROL: the union the count used to build, over the same members in the same
        // buckets. Built before the probe window for each size so only the clone is counted.
        let cloned = |members: usize| -> f64 {
            let buckets: Vec<BTreeSet<Vec<u8>>> = (0..BUCKETS)
                .map(|bucket| {
                    (0..members)
                        .filter(|member| *member as u64 % BUCKETS == bucket)
                        .map(|member| format!("member-{member:06}").into_bytes())
                        .collect()
                })
                .collect();
            let probe = Probe::start();
            let mut unique: BTreeSet<Vec<u8>> = BTreeSet::new();
            for set in &buckets {
                unique.extend(set.iter().cloned());
            }
            let allocs = probe.stop().allocs as f64;
            assert_eq!(unique.len(), members);
            allocs
        };
        let control_slope = (cloned(1_000) - cloned(100)) / 900.0;
        assert!(
            control_slope >= 1.0,
            "the control (a union of clones) allocated {control_slope:.2} blocks per member, so the \
             probe cannot see a copy per member"
        );

        let one_short = per_read(100, true);
        let one_long = per_read(1_000, true);
        let many_short = per_read(100, false);
        let many_long = per_read(1_000, false);
        let one_slope = (one_long - one_short) / 900.0;
        let many_slope = (many_long - many_short) / 900.0;
        eprintln!(
            "  [distinct count] one bucket: {one_short:.1} @100, {one_long:.1} @1000 ({one_slope:.3}/member); \
             {BUCKETS} buckets: {many_short:.1} @100, {many_long:.1} @1000 ({many_slope:.3}/member)"
        );
        assert!(
            one_slope < 0.01,
            "a count over one bucket allocated {one_slope:.3} blocks per member: it is copying them"
        );
        assert!(
            many_slope < 0.5,
            "a count over {BUCKETS} buckets allocated {many_slope:.3} blocks per member; a union of \
             borrows allocates about one block per ten members, a union of copies one or more per member"
        );
    }
}
