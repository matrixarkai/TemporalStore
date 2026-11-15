// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SET ADD, A ZSET ADD AND A LIST PUSH COST THE SAME NUMBER OF ALLOCATIONS WHATEVER THE
//! CONTAINER ALREADY HOLDS.
//!
//! Measured with the counting allocator, release, 200 operations per cell, objects built before the
//! span opens (allocations per operation):
//!
//! ```text
//!   elements already held       1      16     256    4096
//!   set add (new member)        -    67.7    67.7    67.7
//!   set add (present)        69.0    69.1    69.1    69.1
//!   zset add (new member)       -    60.7    60.7    60.7
//!   zset add (new score)     82.1    82.1    82.1    82.1
//!   list push (right)           -    58.3    58.3    58.3
//! ```
//!
//! Every row is flat in the element count, and the same holds across 1, 100 and 2,000 objects (a
//! few allocations of drift as more objects mean fewer warm reads). Nothing on these three write
//! arms is proportional to the elements already held: the key an element write needs is borrowed
//! from the map and the element is filed by name.
//!
//! This pins that shape, so a change that makes a write walk, copy or re-file the container it
//! writes into is caught by number rather than found by a profile. TWO LIMITS, because they catch
//! different regressions: a growth limit between a small and a large container (work proportional
//! to the elements), and a ceiling per operation (a constant-size regression, such as one more key
//! copy per write, which no growth limit sees).
//!
//! `alloc-probe` and `--ignored`. Its control: reading the whole set is proportional to its
//! members by construction, so the same harness must see growth there or it is blind to the
//! dimension the writes claim to be flat in.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![cfg(feature = "alloc-probe")]
#![allow(clippy::all)]
use super::*;

mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 100;
    const SMALL: usize = 16;
    const LARGE: usize = 1024;

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

    fn member(j: usize) -> Vec<u8> {
        format!("m{j:06}").into_bytes()
    }

    /// Allocations per operation of `op` against one object holding `held` elements of `kind`.
    fn per_op(kind: &str, held: usize, op: &dyn Fn(usize) -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        for j in 0..held {
            let command = match kind {
                "set" => Command::SetAdd { key: "o".to_string(), member: member(j) },
                "zset" => Command::ZSetAdd { key: "o".to_string(), member: member(j), score: j as f64 },
                _ => Command::ListPush { key: "o".to_string(), member: member(j), left: false },
            };
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok, "{:?}", response.status);
        }
        let commands: Vec<Command> = (0..OPS).map(|j| op(j)).collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_container_add_does_not_pay_for_the_elements_it_already_has() {
        // THE CONTROL, FIRST: a whole-set read is proportional to the members by construction.
        let read = |_: usize| Command::SetMembers { key: "o".to_string() };
        let read_small = per_op("set", SMALL, &read);
        let read_large = per_op("set", LARGE, &read);
        println!("control  set read-all: {read_small:.1} at {SMALL}, {read_large:.1} at {LARGE}");
        assert!(
            read_large > read_small * 10.0,
            "the control did not grow with the member count ({read_small:.1} -> {read_large:.1}), \
             so this probe is blind to that dimension"
        );

        // (name, kind, ceiling, op). A ceiling is the measured figure plus a margin of about 7.
        type Op = Box<dyn Fn(usize) -> Command>;
        let cases: Vec<(&str, &str, f64, Op)> = vec![
            ("set add new", "set", 75.0, Box::new(|j| Command::SetAdd {
                key: "o".to_string(), member: member(1_000_000 + j) })),
            ("set add present", "set", 77.0, Box::new(|j| Command::SetAdd {
                key: "o".to_string(), member: member(j % SMALL) })),
            ("zset add new", "zset", 68.0, Box::new(|j| Command::ZSetAdd {
                key: "o".to_string(), member: member(1_000_000 + j), score: (1_000_000 + j) as f64 })),
            ("zset add new score", "zset", 90.0, Box::new(|j| Command::ZSetAdd {
                key: "o".to_string(), member: member(j % SMALL), score: (j + 500) as f64 })),
            ("list push right", "list", 66.0, Box::new(|j| Command::ListPush {
                key: "o".to_string(), member: member(j), left: false })),
            ("list push left", "list", 66.0, Box::new(|j| Command::ListPush {
                key: "o".to_string(), member: member(j), left: true })),
        ];
        let mut checked = 0usize;
        for (name, kind, ceiling, op) in &cases {
            let small = per_op(kind, SMALL, op.as_ref());
            let large = per_op(kind, LARGE, op.as_ref());
            println!("{name}: {small:.2} at {SMALL} elements, {large:.2} at {LARGE}");
            // One allocation of growth over 1,008 more elements is already a per-element term in
            // the making; the slack covers only measurement drift.
            assert!(
                large - small < 3.0,
                "{name} cost {small:.2} allocations at {SMALL} elements and {large:.2} at {LARGE}: \
                 it pays for the elements the container already had"
            );
            assert!(
                small <= *ceiling && large <= *ceiling,
                "{name} cost {small:.2} / {large:.2} allocations against a ceiling of {ceiling}"
            );
            assert!(small > 20.0, "{name} measured {small:.2}: the write did not run the path it prices");
            checked += 1;
        }
        assert_eq!(checked, 6);
    }
}
