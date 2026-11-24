// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `STRLEN` AND `GETRANGE` SHOULD COST WHAT THEY RETURN, NOT THE SIZE OF THE STRING.
//!
//! Both were built out of `StringGet`: the shard copied the whole value out of the cache into a
//! `Vec`, handed it to the door, and the door took its length or copied a range out of that copy.
//! `StringLen` and `StringGetRange` read the cache's shared bytes and return the integer or the
//! slice, so what they allocate no longer follows the length of the value.
//!
//! THREE ARMS.
//!   * `length_and_range_answer_what_the_whole_read_answers` -- the two commands and the RESP door
//!     against hand-written `GETRANGE` rules, over widths either side of the store's size classes,
//!     negative and out-of-range positions, a missing key, an overwrite, and a reload.
//!   * `a_lapsed_string_has_no_length_and_no_range` -- an expired key answers as a missing one.
//!   * `probe::a_length_and_a_range_do_not_allocate_per_byte` needs `alloc-probe` and `--ignored`:
//!     the slope of bytes allocated against the value's length, with the whole-value read as the
//!     control (it must still grow with the value, or the probe cannot see the dimension).
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

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn resp(engine: &TemporalEngine, args: &[&str]) -> RespValue {
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.as_bytes().to_vec()).collect();
    execute_redis_command(argv, 1, |command| {
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

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    run(
        engine,
        Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    );
}

fn length(engine: &TemporalEngine, key: &str) -> i64 {
    match run(
        engine,
        Command::StringLen {
            key: key.to_string(),
        },
    ) {
        CommandResponse::Integer { value } => value,
        other => panic!("not an integer: {other:?}"),
    }
}

fn range(engine: &TemporalEngine, key: &str, start: i64, end: i64) -> Option<Vec<u8>> {
    match run(
        engine,
        Command::StringGetRange {
            key: key.to_string(),
            start,
            end,
        },
    ) {
        CommandResponse::Bytes { value } => value,
        other => panic!("not bytes: {other:?}"),
    }
}

/// `GETRANGE` as the command is specified, written out here and not shared with the engine.
fn expected_range(value: &[u8], start: i64, end: i64) -> Vec<u8> {
    let len = value.len() as i64;
    if len == 0 {
        return Vec::new();
    }
    let mut start = if start < 0 { len + start } else { start };
    let mut end = if end < 0 { len + end } else { end };
    if start < 0 {
        start = 0;
    }
    if end < 0 || start >= len {
        return Vec::new();
    }
    if end >= len {
        end = len - 1;
    }
    if start > end {
        return Vec::new();
    }
    value[start as usize..=end as usize].to_vec()
}

fn value_of(width: usize, salt: u8) -> Vec<u8> {
    (0..width).map(|i| (i % 251) as u8 ^ salt).collect()
}

fn check_all(engine: &TemporalEngine, label: &str) -> usize {
    let mut compared = 0usize;
    for (position, width) in [0usize, 1, 7, 64, 4_095, 4_096, 70_000].iter().enumerate() {
        let key = format!("s{position}");
        let value = value_of(*width, 0x11);
        assert_eq!(*width as i64, length(engine, &key), "{label}: STRLEN width {width}");
        let len = *width as i64;
        for (start, end) in [
            (0i64, -1i64),
            (0, 0),
            (1, 3),
            (-3, -1),
            (-100_000, 5),
            (2, 1),
            (len - 1, len + 10),
            (len, len),
            (len + 5, len + 9),
            (0, i64::MAX),
            (i64::MIN, -1),
            (3, i64::MAX),
        ] {
            assert_eq!(
                Some(expected_range(&value, start, end)),
                range(engine, &key, start, end),
                "{label}: width {width} range [{start}, {end}]"
            );
            compared += 1;
        }
        // The door, end to end.
        assert_eq!(
            RespValue::Integer(*width as i64),
            resp(engine, &["STRLEN", &key]),
            "{label}: door STRLEN"
        );
        assert_eq!(
            RespValue::Bulk(Some(expected_range(&value, 1, -2))),
            resp(engine, &["GETRANGE", &key, "1", "-2"]),
            "{label}: door GETRANGE"
        );
        compared += 2;
    }
    compared
}

#[test]
fn length_and_range_answer_what_the_whole_read_answers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    for (position, width) in [0usize, 1, 7, 64, 4_095, 4_096, 70_000].iter().enumerate() {
        set(&engine, &format!("s{position}"), &value_of(*width, 0x11));
    }
    let live = check_all(&engine, "live");

    // A key that is not there: length 0, no bytes, and the door's usual empty answers.
    assert_eq!(0, length(&engine, "absent"));
    assert_eq!(None, range(&engine, "absent", 0, -1));
    assert_eq!(RespValue::Integer(0), resp(&engine, &["STRLEN", "absent"]));
    assert_eq!(
        RespValue::Bulk(Some(Vec::new())),
        resp(&engine, &["GETRANGE", "absent", "0", "-1"])
    );

    // OVERWRITTEN: the length and the range follow the new value, not a cached old one.
    set(&engine, "s3", b"short");
    assert_eq!(5, length(&engine, "s3"));
    assert_eq!(Some(b"hor".to_vec()), range(&engine, "s3", 1, 3));
    set(&engine, "s3", &value_of(64, 0x11));

    // AFTER A RELOAD every block is read cold.
    drop(engine);
    let reloaded = engine_on(dir.path());
    let cold = check_all(&reloaded, "reloaded");
    // DENOMINATORS: a run that compared little proves little.
    assert!(live >= 7 * 14 && cold >= 7 * 14, "compared {live} and {cold}");
}

#[test]
fn cold_string_lengths_use_only_address_or_header_bytes() {
    const VALUE_BYTES: usize = (2 << 20) + 51;
    let payload = vec![0xff; VALUE_BYTES];
    for (async_storage, compression_enabled) in [(true, false), (false, false), (false, true)] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
            64 << 10,
            root.join("cache"),
            root.join("blocks"),
            root.join("indexes"),
            BlockStoreOptions {
                compression_enabled,
                compression_min_bytes: 0,
                compression_level: 1,
            },
            false,
        );
        engine.load_shard(1);
        assert!(engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage,
                    ..Config::default()
                },
            })
            .ok);
        set(&engine, "chunk", &payload);
        let address = engine
            .shards
            .read()
            .unwrap()
            .get(&1)
            .unwrap()
            .string_address("chunk")
            .unwrap();
        assert_eq!(address.block_slab_id() == HOT_BLOCK_SLAB_ID, async_storage);
        if compression_enabled {
            assert!(address.length() < VALUE_BYTES as u64);
        }
        engine.cache().invalidate_shard(1).unwrap();

        let before = engine.block_store().stats().bytes_read;
        let wal = engine.write_ahead_log_store();
        let wal_before = wal.stats(1).bytes_read;
        assert_eq!(length(&engine, "chunk"), VALUE_BYTES as i64);
        assert_eq!(wal.stats(1).bytes_read, wal_before);
        let slab_bytes = engine.block_store().stats().bytes_read - before;
        let expected = if async_storage {
            0
        } else {
            (crate::block_store::BLOCK_RECORD_HEADER_LEN + 4) as u64
        };
        assert_eq!(slab_bytes, expected, "storage mode {async_storage}");

        let before = engine.block_store().stats().bytes_read;
        assert_eq!(engine.plan_cache_reclamation(1, 0).map(|(bytes, _)| bytes), Some(VALUE_BYTES as u64));
        assert_eq!(wal.stats(1).bytes_read, wal_before);
        assert_eq!(engine.block_store().stats().bytes_read - before, expected);

        assert_eq!(
            run(&engine, Command::StringGet { key: "chunk".to_string() }),
            CommandResponse::Bytes { value: Some(payload.clone()) }
        );
    }
}

#[test]
fn a_lapsed_string_has_no_length_and_no_range() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    set(&engine, "lapsed", b"value");
    set(&engine, "kept", b"value");
    run(
        &engine,
        Command::CommonExpire {
            key: "lapsed".to_string(),
            ttl_ms: 1,
        },
    );
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert_eq!(
        engine.plan_cache_reclamation(1, 1),
        Some((5, vec![("kept".to_string(), 5)])),
    );
    assert_eq!(0, length(&engine, "lapsed"));
    assert_eq!(None, range(&engine, "lapsed", 0, -1));
    assert_eq!(5, length(&engine, "kept"));
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 16;

    /// Bytes allocated per operation of `command(j)` over strings of `width` bytes already in the
    /// cache (each is read once before the span, so the block is resident).
    fn bytes_per_op(width: usize, command: impl Fn(usize) -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        for key in 0..OPS {
            set(&engine, &format!("s{key}"), &vec![b'v'; width]);
            run(
                &engine,
                Command::StringGet {
                    key: format!("s{key}"),
                },
            );
        }
        let commands: Vec<Command> = (0..OPS).map(|j| command(j)).collect();
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

    /// What each command allocates PER BYTE of the value, as the slope between two widths. A
    /// range of sixteen bytes and a length must not grow with the string; the whole-value read is
    /// the control and must.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_length_and_a_range_do_not_allocate_per_byte() {
        const SMALL: usize = 4 * 1024;
        const LARGE: usize = 256 * 1024;
        let slope = |command: &dyn Fn(usize) -> Command| {
            (bytes_per_op(LARGE, command) - bytes_per_op(SMALL, command)) / (LARGE - SMALL) as f64
        };
        let len = slope(&|j| Command::StringLen {
            key: format!("s{j}"),
        });
        let part = slope(&|j| Command::StringGetRange {
            key: format!("s{j}"),
            start: 0,
            end: 15,
        });
        let whole = slope(&|j| Command::StringGet {
            key: format!("s{j}"),
        });
        println!(
            "bytes allocated per byte of value: STRLEN {len:.3}, GETRANGE of 16 bytes {part:.3}, \
             whole read (control) {whole:.3}"
        );
        assert!(
            whole >= 0.9,
            "the control allocated {whole:.3} bytes per byte of value, so the probe cannot see a \
             copy of the value"
        );
        assert!(len <= 0.05, "STRLEN allocated {len:.3} bytes per byte of value: it copied it");
        assert!(
            part <= 0.05,
            "a sixteen-byte GETRANGE allocated {part:.3} bytes per byte of value: it copied it"
        );
    }
}
