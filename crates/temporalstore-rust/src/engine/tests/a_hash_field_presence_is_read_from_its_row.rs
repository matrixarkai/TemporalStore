// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! "IS THIS FIELD IN THE HASH" SHOULD NOT COPY THE FIELD'S VALUE OUT OF ITS BLOCK.
//!
//! `HEXISTS` and the lookup half of `HSETNX` were built out of `HashGet`: the shard opened the
//! field's block, cached it, copied the value into a `Vec` and handed it to the door, which kept one
//! bit of it. `HashFieldExists` answers from the field map the hash already keeps, so the cost no
//! longer follows the size of the value, and a field whose block is not cached costs no block read.
//!
//! THREE ARMS.
//!   * `presence_answers_what_the_point_read_answers` -- the command against `HashGet` over fields
//!     present, absent, deleted, empty-valued and empty-named, an absent key, and after a reload.
//!   * `the_door_answers_hexists_and_hsetnx_as_it_did` -- through the RESP door.
//!   * `probe::a_presence_check_does_not_copy_the_value` needs `alloc-probe` and `--ignored`, with
//!     the point read of the same field as the control (it must still copy the value).
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

fn exists(engine: &TemporalEngine, key: &str, field: &str) -> i64 {
    match run(
        engine,
        Command::HashFieldExists {
            key: key.to_string(),
            field: field.to_string(),
        },
    ) {
        CommandResponse::Integer { value } => value,
        other => panic!("not an integer: {other:?}"),
    }
}

fn point_read_says(engine: &TemporalEngine, key: &str, field: &str) -> i64 {
    match run(
        engine,
        Command::HashGet {
            key: key.to_string(),
            field: field.to_string(),
        },
    ) {
        CommandResponse::Bytes { value } => i64::from(value.is_some()),
        other => panic!("not bytes: {other:?}"),
    }
}

fn set(engine: &TemporalEngine, key: &str, field: &str, value: &[u8]) {
    run(
        engine,
        Command::HashSet {
            key: key.to_string(),
            field: field.to_string(),
            value: value.to_vec(),
        },
    );
}

fn check_all(engine: &TemporalEngine, label: &str) -> (usize, usize) {
    let mut yes = 0usize;
    let mut no = 0usize;
    for (key, field) in [
        ("h", "a"),
        ("h", "b"),
        ("h", "empty-value"),
        ("h", ""),
        ("h", "deleted"),
        ("h", "never"),
        ("absent", "a"),
        ("h2", "only"),
        ("h2", "a"),
    ] {
        let expected = point_read_says(engine, key, field);
        assert_eq!(expected, exists(engine, key, field), "{label}: {key}/{field:?}");
        yes += (expected == 1) as usize;
        no += (expected == 0) as usize;
    }
    (yes, no)
}

#[test]
fn presence_answers_what_the_point_read_answers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    set(&engine, "h", "a", b"1");
    set(&engine, "h", "b", &vec![b'x'; 70_000]);
    set(&engine, "h", "empty-value", b"");
    set(&engine, "h", "", b"unnamed");
    set(&engine, "h", "deleted", b"soon");
    set(&engine, "h2", "only", b"v");
    run(
        &engine,
        Command::HashDelete {
            key: "h".to_string(),
            field: "deleted".to_string(),
        },
    );
    let (yes, no) = check_all(&engine, "live");
    // Known answers, so two readers wrong the same way cannot agree.
    assert_eq!(1, exists(&engine, "h", "a"));
    assert_eq!(1, exists(&engine, "h", "empty-value"));
    assert_eq!(1, exists(&engine, "h", ""));
    assert_eq!(0, exists(&engine, "h", "deleted"));
    assert_eq!(0, exists(&engine, "absent", "a"));
    assert_eq!(0, exists(&engine, "h2", "a"));
    // Removing the last field removes the hash: nothing is left to be present.
    run(
        &engine,
        Command::HashDelete {
            key: "h2".to_string(),
            field: "only".to_string(),
        },
    );
    assert_eq!(0, exists(&engine, "h2", "only"));
    assert!(yes >= 4 && no >= 4, "yes {yes}, no {no}");

    drop(engine);
    let reloaded = engine_on(dir.path());
    let (yes, no) = check_all(&reloaded, "reloaded");
    assert!(yes >= 3 && no >= 4, "after a reload: yes {yes}, no {no}");
    assert_eq!(1, exists(&reloaded, "h", "b"));
    assert_eq!(0, exists(&reloaded, "h", "deleted"));
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

#[test]
fn the_door_answers_hexists_and_hsetnx_as_it_did() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    assert_eq!(RespValue::Integer(0), resp(&engine, &["HEXISTS", "h", "f"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HSETNX", "h", "f", "first"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HEXISTS", "h", "f"]));
    // NX: a second attempt does not replace the value.
    assert_eq!(RespValue::Integer(0), resp(&engine, &["HSETNX", "h", "f", "second"]));
    assert_eq!(
        RespValue::Bulk(Some(b"first".to_vec())),
        resp(&engine, &["HGET", "h", "f"])
    );
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HSETNX", "h", "g", ""]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HEXISTS", "h", "g"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HDEL", "h", "f"]));
    assert_eq!(RespValue::Integer(0), resp(&engine, &["HEXISTS", "h", "f"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HSETNX", "h", "f", "again"]));
    assert_eq!(
        RespValue::Bulk(Some(b"again".to_vec())),
        resp(&engine, &["HGET", "h", "f"])
    );
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const VALUE: usize = 64 * 1024;
    const OPS: usize = 16;

    fn bytes_per_op(command: impl Fn(usize) -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        for field in 0..OPS {
            set(&engine, "h", &format!("f{field}"), &vec![b'v'; VALUE]);
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

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_presence_check_does_not_copy_the_value() {
        let presence = bytes_per_op(|j| Command::HashFieldExists {
            key: "h".to_string(),
            field: format!("f{j}"),
        });
        // THE CONTROL: the point read of the same field must still copy its value.
        let point = bytes_per_op(|j| Command::HashGet {
            key: "h".to_string(),
            field: format!("f{j}"),
        });
        println!("bytes per op over {VALUE}-byte fields: presence {presence:.0}, point read {point:.0}");
        assert!(
            point >= VALUE as f64,
            "the control allocated {point:.0} bytes: the probe cannot see a copy of the value"
        );
        assert!(
            presence < 4096.0,
            "a presence check allocated {presence:.0} bytes over {VALUE}-byte fields: it copied \
             the value"
        );
    }
}
