// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `HSET` AND `HDEL` OVER RESP ASK WHETHER A FIELD IS THERE, NOT FOR ITS VALUE.
//!
//! The two verbs answer a count (fields added, fields removed), so each looks at every field before it
//! writes. They used to do that with `HashGet`, which opens the field's block and copies the value
//! out to throw it away; `HashFieldExists` answers the same question from the field map. The
//! engine commands a verb issues are recorded here, and the answers are compared with what the
//! counts have always been.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::redis::{execute_redis_command, RespValue};
use std::cell::RefCell;
use std::collections::BTreeMap;

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

/// One RESP command against the engine, with the engine commands it issued counted by kind.
fn resp(engine: &TemporalEngine, args: &[&str]) -> (RespValue, BTreeMap<String, usize>) {
    let issued = RefCell::new(BTreeMap::<String, usize>::new());
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.as_bytes().to_vec()).collect();
    let answer = execute_redis_command(argv, 1, |command| {
        let text = format!("{command:?}");
        let kind = text.split(|c: char| c == ' ' || c == '{' || c == '(').next().unwrap_or("").to_string();
        *issued.borrow_mut().entry(kind).or_insert(0) += 1;
        let response = engine.execute(ExecuteRequest { shard_id: 1, command });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message.clone())
        }
    });
    (answer, issued.into_inner())
}

fn count(issued: &BTreeMap<String, usize>, kind: &str) -> usize {
    issued.get(kind).copied().unwrap_or(0)
}

#[test]
fn an_hset_counts_the_fields_it_adds_by_asking_the_field_map_and_never_reads_a_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // Three new fields: all added, three presence questions and one write, no value read.
    let (added, issued) = resp(&engine, &["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    assert_eq!(added, RespValue::Integer(3));
    assert_eq!((count(&issued, "HashFieldExists"), count(&issued, "HashMultiSet"), count(&issued, "HashGet")), (3, 1, 0), "{issued:?}");

    // One field that is there and one that is not: only the new one is counted, and what was
    // written reads back.
    let (added, issued) = resp(&engine, &["HSET", "h", "a", "9", "d", "4"]);
    assert_eq!(added, RespValue::Integer(1));
    assert_eq!((count(&issued, "HashFieldExists"), count(&issued, "HashGet")), (2, 0), "{issued:?}");
    assert_eq!(resp(&engine, &["HGET", "h", "a"]).0, RespValue::Bulk(Some(b"9".to_vec())));
    assert_eq!(resp(&engine, &["HGET", "h", "d"]).0, RespValue::Bulk(Some(b"4".to_vec())));
    assert_eq!(resp(&engine, &["HLEN", "h"]).0, RespValue::Integer(4));

    // A field named twice in one request is asked about before either is written, so both are
    // counted, as they always were.
    let (added, _) = resp(&engine, &["HSET", "dup", "x", "1", "x", "2"]);
    assert_eq!(added, RespValue::Integer(2));
    assert_eq!(resp(&engine, &["HGET", "dup", "x"]).0, RespValue::Bulk(Some(b"2".to_vec())));
    assert_eq!(resp(&engine, &["HLEN", "dup"]).0, RespValue::Integer(1));
}

#[test]
fn an_hdel_counts_the_fields_it_removes_by_asking_the_field_map_and_deletes_only_those() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    resp(&engine, &["HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4"]);

    // Two fields that are there and one that is not: two removals, three questions, two deletes,
    // and no value read.
    let (removed, issued) = resp(&engine, &["HDEL", "h", "missing", "b", "a"]);
    assert_eq!(removed, RespValue::Integer(2));
    assert_eq!(
        (count(&issued, "HashFieldExists"), count(&issued, "HashDelete"), count(&issued, "HashGet")),
        (3, 2, 0),
        "{issued:?}"
    );
    assert_eq!(resp(&engine, &["HLEN", "h"]).0, RespValue::Integer(2));
    assert_eq!(resp(&engine, &["HGET", "h", "a"]).0, RespValue::Bulk(None));
    assert_eq!(resp(&engine, &["HGET", "h", "c"]).0, RespValue::Bulk(Some(b"3".to_vec())));

    // A key that is not there removes nothing and deletes nothing.
    let (removed, issued) = resp(&engine, &["HDEL", "nokey", "f"]);
    assert_eq!(removed, RespValue::Integer(0));
    assert_eq!((count(&issued, "HashFieldExists"), count(&issued, "HashDelete")), (1, 0), "{issued:?}");

    // The same answers after a restart reads every block cold.
    drop(engine);
    let reloaded = engine_on(dir.path());
    assert_eq!(resp(&reloaded, &["HLEN", "h"]).0, RespValue::Integer(2));
    let (removed, issued) = resp(&reloaded, &["HDEL", "h", "c", "a"]);
    assert_eq!(removed, RespValue::Integer(1));
    assert_eq!((count(&issued, "HashDelete"), count(&issued, "HashGet")), (1, 0), "{issued:?}");
}
