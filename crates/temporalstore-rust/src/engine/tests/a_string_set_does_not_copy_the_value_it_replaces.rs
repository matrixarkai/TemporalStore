// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A STRING WRITE THAT DOES NOT RETURN THE OLD VALUE SHOULD NOT READ IT.
//!
//! `SET`, `SET .. NX`, `SET .. XX` and `SET .. KEEPTTL` all arrive as one conditional write. The
//! shard read the key's whole old value into a `Vec` before deciding anything -- a copy of every byte
//! of the value about to be replaced, and a block-store read when its frame was not cached -- and
//! only a write that returns the old value looked at it. A write needs one bit from the old value,
//! and only under NX/XX: whether the key's block answers. That bit is now read from the cache's shared
//! bytes, and an unconditional write that returns nothing reads nothing.
//!
//! THREE ARMS.
//!   * `a_conditional_write_answers_what_it_always_answered` -- every condition, for a key that is
//!     there and one that is not, with and without the old value asked for, with and without a
//!     deadline to keep, checked against known answers and against a reload.
//!   * `the_value_a_write_leaves_is_the_value_a_read_returns` -- values of every width the store
//!     keeps in its two forms, written over each other, read back byte for byte.
//!   * `probe::a_string_overwrite_does_not_copy_the_old_value` needs `alloc-probe` and `--ignored`:
//!     bytes allocated by an overwrite of a large value, with the write that asks for the old value
//!     as the control (it must still copy it, or the probe cannot see the dimension).
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::StringSetCondition;

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
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn read(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match run(
        engine,
        Command::StringGet {
            key: key.to_string(),
        },
    ) {
        CommandResponse::Bytes { value } => value,
        other => panic!("GET {key} answered {other:?}"),
    }
}

fn ttl(engine: &TemporalEngine, key: &str) -> i64 {
    match run(
        engine,
        Command::CommonTtl {
            key: key.to_string(),
        },
    ) {
        CommandResponse::Integer { value } => value,
        other => panic!("TTL {key} answered {other:?}"),
    }
}

fn conditional(
    key: &str,
    value: &[u8],
    condition: StringSetCondition,
    return_old: bool,
    keep_ttl: bool,
) -> Command {
    Command::StringSetConditional {
        key: key.to_string(),
        value: value.to_vec(),
        ttl_ms: None,
        condition,
        return_old,
        keep_ttl,
    }
}

#[test]
fn a_conditional_write_answers_what_it_always_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let mut cases = 0usize;
    let mut landed = 0usize;
    let mut refused = 0usize;
    for existing in [false, true] {
        for condition in [
            StringSetCondition::Always,
            StringSetCondition::IfExists,
            StringSetCondition::IfNotExists,
        ] {
            for return_old in [false, true] {
                for keep_ttl in [false, true] {
                    let key = format!("k-{existing}-{condition:?}-{return_old}-{keep_ttl}");
                    if existing {
                        // A value and a deadline that is far away, so KEEPTTL has one to keep.
                        run(
                            &engine,
                            Command::StringSetEx {
                                key: key.clone(),
                                value: b"old-value".to_vec(),
                                ttl_ms: 3_600_000,
                            },
                        );
                        assert!(ttl(&engine, &key) > 0);
                    }
                    let will_land = match condition {
                        StringSetCondition::Always => true,
                        StringSetCondition::IfExists => existing,
                        StringSetCondition::IfNotExists => !existing,
                    };
                    let answer = run(
                        &engine,
                        conditional(&key, b"new-value", condition, return_old, keep_ttl),
                    );
                    let label = format!(
                        "existing={existing} {condition:?} return_old={return_old} keep_ttl={keep_ttl}"
                    );
                    if return_old {
                        assert_eq!(
                            CommandResponse::Bytes {
                                value: existing.then(|| b"old-value".to_vec())
                            },
                            answer,
                            "{label}"
                        );
                    } else {
                        assert_eq!(
                            CommandResponse::Integer {
                                value: will_land as i64
                            },
                            answer,
                            "{label}"
                        );
                    }
                    let expected = if will_land {
                        Some(b"new-value".to_vec())
                    } else {
                        existing.then(|| b"old-value".to_vec())
                    };
                    assert_eq!(expected, read(&engine, &key), "{label}: the stored value");
                    // The deadline: kept only by a write that landed with KEEPTTL; cleared by a
                    // write that landed without it; untouched by a write that did not land.
                    if existing {
                        let remaining = ttl(&engine, &key);
                        if will_land && !keep_ttl {
                            assert_eq!(-1, remaining, "{label}: the deadline is cleared");
                        } else {
                            assert!(remaining > 0, "{label}: the deadline is kept, got {remaining}");
                        }
                    }
                    cases += 1;
                    landed += will_land as usize;
                    refused += !will_land as usize;
                }
            }
        }
    }
    // DENOMINATORS: both outcomes of the condition were exercised.
    assert_eq!(24, cases);
    assert!(landed >= 14 && refused >= 6, "landed {landed}, refused {refused}");

    // AFTER A RELOAD the same keys read the same, because the write stored the same bytes.
    let mut before = Vec::new();
    for existing in [false, true] {
        for condition in [
            StringSetCondition::Always,
            StringSetCondition::IfExists,
            StringSetCondition::IfNotExists,
        ] {
            for return_old in [false, true] {
                for keep_ttl in [false, true] {
                    let key = format!("k-{existing}-{condition:?}-{return_old}-{keep_ttl}");
                    before.push((key.clone(), read(&engine, &key)));
                }
            }
        }
    }
    drop(engine);
    let reloaded = engine_on(dir.path());
    for (key, value) in before {
        assert_eq!(value, read(&reloaded, &key), "after a reload: {key}");
    }
}

#[test]
fn the_value_a_write_leaves_is_the_value_a_read_returns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let widths = [0usize, 1, 63, 64, 4_095, 4_096, 65_535, 65_536, 300_000];
    let mut checked = 0usize;
    for (position, width) in widths.iter().enumerate() {
        let first: Vec<u8> = (0..*width).map(|i| (i % 251) as u8).collect();
        let second: Vec<u8> = (0..*width + 7).map(|i| (i % 241) as u8 ^ 0x5a).collect();
        let key = format!("w-{position}");
        run(
            &engine,
            Command::StringSet {
                key: key.clone(),
                value: first.clone(),
            },
        );
        assert_eq!(Some(first.clone()), read(&engine, &key), "width {width}: first");
        // Overwritten without asking for the old value, then under NX (refused) and XX (lands).
        run(
            &engine,
            conditional(&key, &second, StringSetCondition::Always, false, false),
        );
        assert_eq!(Some(second.clone()), read(&engine, &key), "width {width}: always");
        run(
            &engine,
            conditional(&key, &first, StringSetCondition::IfNotExists, false, false),
        );
        assert_eq!(Some(second.clone()), read(&engine, &key), "width {width}: nx refused");
        run(
            &engine,
            conditional(&key, &first, StringSetCondition::IfExists, false, false),
        );
        assert_eq!(Some(first.clone()), read(&engine, &key), "width {width}: xx landed");
        checked += 1;
    }
    assert_eq!(widths.len(), checked);
    // A key that was never written is not made by XX and is made by NX.
    run(
        &engine,
        conditional("absent", b"x", StringSetCondition::IfExists, false, false),
    );
    assert_eq!(None, read(&engine, "absent"));
    run(
        &engine,
        conditional("absent", b"x", StringSetCondition::IfNotExists, false, false),
    );
    assert_eq!(Some(b"x".to_vec()), read(&engine, "absent"));
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const VALUE: usize = 256 * 1024;
    const OPS: usize = 16;

    /// Bytes allocated per operation by `command(j)` over keys that already hold a large value.
    fn bytes_per_op(command: impl Fn(usize, &[u8]) -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        let value = vec![b'v'; VALUE];
        for key in 0..OPS {
            run(
                &engine,
                Command::StringSet {
                    key: format!("s{key}"),
                    value: value.clone(),
                },
            );
        }
        let commands: Vec<Command> = (0..OPS).map(|j| command(j, &value)).collect();
        let probe = Probe::start();
        for command in commands {
            let outcome = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(outcome.status.ok);
        }
        probe.stop().alloc_bytes as f64 / OPS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_string_overwrite_does_not_copy_the_old_value() {
        let always = bytes_per_op(|j, value| {
            conditional(&format!("s{j}"), value, StringSetCondition::Always, false, false)
        });
        let keep = bytes_per_op(|j, value| {
            conditional(&format!("s{j}"), value, StringSetCondition::Always, false, true)
        });
        let xx = bytes_per_op(|j, value| {
            conditional(&format!("s{j}"), value, StringSetCondition::IfExists, false, false)
        });
        let nx_refused = bytes_per_op(|j, value| {
            conditional(&format!("s{j}"), value, StringSetCondition::IfNotExists, false, false)
        });
        // THE CONTROL: the same overwrite, asking for the old value, must still copy it.
        let returns_old = bytes_per_op(|j, value| {
            conditional(&format!("s{j}"), value, StringSetCondition::Always, true, false)
        });
        println!(
            "bytes per op over a {VALUE}-byte value: always {always:.0}, keepttl {keep:.0}, \
             xx {xx:.0}, nx refused {nx_refused:.0}; control (returns the old value) {returns_old:.0}"
        );
        // The control is the SAME overwrite plus the copy of the old value the caller asked for, so
        // what it costs beyond a write that does not ask is the copy, and the difference is the
        // whole claim: it holds whatever else a write allocates for a value of this size.
        let copy = VALUE as f64 * 0.9;
        assert!(
            returns_old >= nx_refused + copy,
            "the control allocated {returns_old:.0} bytes and a refused NX {nx_refused:.0}: the \
             probe cannot see a copy of the old value"
        );
        // The writes that read nothing, or read only whether the block answers, copy no old value.
        for (name, bytes) in [("always", always), ("keepttl", keep), ("xx", xx)] {
            assert!(
                bytes + copy <= returns_old,
                "{name} allocated {bytes:.0} bytes a write over a {VALUE}-byte value against \
                 {returns_old:.0} for the write that returns the old value: it copied it too"
            );
        }
        // A refused write keeps the one copy of the value the command itself carries, and nothing
        // more: the read of the old value it used to make was three further copies of it.
        assert!(
            nx_refused < VALUE as f64 * 1.5,
            "a refused NX allocated {nx_refused:.0} bytes over a {VALUE}-byte value: it read the \
             old one"
        );
    }
}
