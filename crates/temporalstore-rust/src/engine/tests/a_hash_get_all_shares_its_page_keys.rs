// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A READ OF A WHOLE HASH BUILDS ITS PAGE KEYS FROM ONE RECORD KEY PER SLAB, NOT THREE STRINGS PER FIELD.
//!
//! Every field of a hash is its own block, and reading a block starts with the key it is cached
//! under. That key was built from scratch for each field: the slab's record-key string, an `Arc`
//! copy of it, and the selector. The first two spell the same slab name for every field of a slab,
//! and a hash sits in few slabs. A whole-hash read now holds the last slab's record key and hands
//! out clones of it, so a field pays for its selector alone.
//!
//! THE ANSWER IS THE SAME BYTES. Two arms:
//!   * `a_whole_hash_read_answers_what_the_per_field_reads_answer` -- default build. The whole-hash
//!     read against the same hash read one field at a time (a path this change does not touch),
//!     over an empty hash, one field, many fields, empty and binary values, an overwrite, a delete,
//!     and after a reload that reads every block cold.
//!   * `probe::a_whole_hash_read_does_not_pay_three_allocations_a_field_for_its_key` needs
//!     `alloc-probe` and `--ignored`. It takes the SLOPE of allocations against the field count.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

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

fn all(engine: &TemporalEngine, key: &str) -> Vec<(String, Vec<u8>)> {
    match run(engine, Command::HashGetAll { key: key.to_string() }) {
        CommandResponse::HashEntries { entries } => entries,
        other => panic!("not entries: {other:?}"),
    }
}

fn one(engine: &TemporalEngine, key: &str, field: &str) -> Option<Vec<u8>> {
    match run(
        engine,
        Command::HashGet {
            key: key.to_string(),
            field: field.to_string(),
        },
    ) {
        CommandResponse::Bytes { value } => value,
        other => panic!("not bytes: {other:?}"),
    }
}

fn set(engine: &TemporalEngine, key: &str, field: &str, value: Vec<u8>) {
    run(
        engine,
        Command::HashSet {
            key: key.to_string(),
            field: field.to_string(),
            value,
        },
    );
}

#[test]
fn a_whole_hash_read_answers_what_the_per_field_reads_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    set(&engine, "single", "only", b"v".to_vec());
    for field in 0..60usize {
        let value = match field % 4 {
            0 => Vec::new(),
            1 => vec![0u8, 255, 1, 254, field as u8],
            2 => vec![b'x'; 1 + field * 53],
            _ => format!("value-{field}").into_bytes(),
        };
        set(&engine, "many", &format!("field-{field:03}"), value);
    }
    // An overwrite replaces a field's block; a delete removes one.
    set(&engine, "many", "field-007", b"overwritten".to_vec());
    run(
        &engine,
        Command::HashDelete {
            key: "many".to_string(),
            field: "field-010".to_string(),
        },
    );

    let check = |engine: &TemporalEngine, label: &str| -> usize {
        // EMPTY and ABSENT.
        assert!(all(engine, "never-written").is_empty(), "{label}");
        let single = all(engine, "single");
        assert_eq!(vec![("only".to_string(), b"v".to_vec())], single, "{label}");

        let entries = all(engine, "many");
        assert_eq!(59, entries.len(), "{label}: 60 written, one deleted");
        let fields: Vec<String> = entries.iter().map(|(field, _)| field.clone()).collect();
        let mut sorted = fields.clone();
        sorted.sort();
        assert_eq!(sorted, fields, "{label}: the answer is in field order");
        assert!(!fields.contains(&"field-010".to_string()), "{label}");
        for (field, value) in &entries {
            assert_eq!(
                Some(value.clone()),
                one(engine, "many", field),
                "{label}: {field}"
            );
        }
        assert_eq!(
            b"overwritten".to_vec(),
            entries
                .iter()
                .find(|(field, _)| field == "field-007")
                .unwrap()
                .1,
            "{label}"
        );
        // A SECOND READ is a warm one for the first's keys, and answers the same.
        assert_eq!(entries, all(engine, "many"), "{label}: warm");
        entries.len()
    };

    let live = check(&engine, "live");
    drop(engine);
    // AFTER A RELOAD every block is read cold, which is the arm that stores a key.
    let reloaded = engine_on(dir.path());
    let cold = check(&reloaded, "reloaded");
    // DENOMINATOR: the comparison covered the fields, both times.
    assert_eq!(59, live);
    assert_eq!(59, cold);
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 20;

    fn per_read(fields: usize) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        for field in 0..fields {
            set(&engine, "h", &format!("f{field:05}"), vec![b'v'; 64]);
        }
        // One warm-up so the span measures the resident answer, not the first fill.
        let _ = all(&engine, "h");
        let commands: Vec<Command> = (0..OPS)
            .map(|_| Command::HashGetAll { key: "h".to_string() })
            .collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    /// Allocations per FIELD, as the slope between two hash sizes.
    ///
    /// Before this change the slope was 8.03 (812 allocations at 100 fields), three of them the
    /// page key. The bound sits between the two, and the control is the planted run: with the
    /// keys built per field again, the slope must exceed it.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_whole_hash_read_does_not_pay_three_allocations_a_field_for_its_key() {
        let short = per_read(10);
        let long = per_read(100);
        let slope = (long - short) / 90.0;
        println!("getall: {short:.1} @10, {long:.1} @100, {slope:.2}/field");
        assert!(
            slope > 1.0,
            "the whole-hash read spent {slope:.2} allocations per field, so the probe cannot see \
             work that grows with the hash"
        );
        assert!(
            slope <= 7.0,
            "the whole-hash read spent {slope:.2} allocations per field; it spent 8.03 when every \
             field built its page key from scratch"
        );
    }
}
