// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::parse_msgpack_borrowed;
use crate::types::{BatchHashMultiSetValue, BatchHashTimestampSetRow};
use crate::types::{borrowed_value_materializations_for_test, reset_borrowed_value_materializations_for_test};
use std::borrow::Cow;

#[derive(Clone, PartialEq, prost::Message)]
struct Timestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

fn request(seconds: i64, rows: usize, width: usize, length: usize) -> BatchHashTimestampSetRequest {
    BatchHashTimestampSetRequest {
        shard_id: 1, timestamp_field: "ts".into(), ttl_ms: None,
        fields: std::iter::once("ts".into()).chain((0..width).map(|i| format!("f{i}"))).collect(),
        rows: (0..rows).map(|row| BatchHashTimestampSetRow(format!("borrowed-row-{row}"), seconds, 0,
            std::iter::once(BatchHashMultiSetValue(prost::Message::encode_to_vec(&Timestamp { seconds, nanos: 0 })))
                .chain((0..width).map(|column| BatchHashMultiSetValue(vec![seconds as u8 + column as u8; length])))
                .collect())).collect(),
    }
}

fn open(dir: &std::path::Path, asynchronous: bool) -> DataNodeRuntime {
    let engine = TemporalEngine::with_local_dirs(64 * 1024 * 1024,
        dir.join("cache"), dir.join("blocks"), dir.join("index"));
    engine.load_shard(1);
    assert!(engine.set_config(crate::SetConfigRequest { shard_id: 1,
        config: crate::Config { version: 2, async_storage: asynchronous, ..crate::Config::default() } }).ok);
    DataNodeRuntime::new_without_workers_for_test(engine, 4)
}

fn apply(runtime: &DataNodeRuntime, owned: &BatchHashTimestampSetRequest) -> BatchConditionalWriteResponse {
    let mut wire = rmp_serde::to_vec_named(owned).unwrap();
    let borrowed = parse_msgpack_borrowed::<BorrowedHashTimestampRequest<'_>>(&wire).unwrap();
    let response = runtime.batch_hash_timestamp_set_borrowed(borrowed);
    wire.fill(0xa5);
    response
}

fn verify(runtime: &DataNodeRuntime, wanted: &BatchHashTimestampSetRequest) {
    for row in &wanted.rows {
        let response = runtime.execute(ExecuteRequest { shard_id: 1,
            command: Command::HashMultiGet { key: row.0.clone(), fields: wanted.fields.clone() } });
        assert!(response.status.ok, "{:?}", response.status);
        assert_eq!(response.response, CommandResponse::Values {
            values: row.3.iter().map(|value| Some(value.0.clone())).collect(),
        });
    }
}

#[test]
fn borrowed_timestamp_payloads_binary_values_point_into_the_request() {
    let owned = request(20, 128, 20, 1024);
    let wire = rmp_serde::to_vec_named(&owned).unwrap();
    let borrowed: BorrowedHashTimestampRequest<'_> = parse_msgpack_borrowed(&wire).unwrap();
    let start = wire.as_ptr() as usize;
    let end = start + wire.len();
    let mut count = 0;
    for row in &borrowed.rows {
        for value in &row.3 {
            let Cow::Borrowed(bytes) = &value.0 else { panic!("binary values must not allocate payload buffers") };
            let pointer = bytes.as_ptr() as usize;
            assert!(pointer >= start && pointer + bytes.len() <= end);
            count += 1;
        }
    }
    assert_eq!(count, 128 * 21);
    assert_eq!(rmp_serde::to_vec_named(&borrowed).unwrap(), wire);
    reset_borrowed_value_materializations_for_test();
    assert_eq!(borrowed.into_owned(), owned);
    assert_eq!(borrowed_value_materializations_for_test(), 128 * 21);
}

#[test]
fn borrowed_timestamp_payloads_keep_legacy_byte_shapes_and_decode_errors() {
    for payload in [serde_json::json!("AAH/"), serde_json::json!([0, 1, 255])] {
        let value = serde_json::json!({"shard_id":1, "fields":["ts"], "timestamp_field":"ts",
            "rows":[["key",20,0,[payload]]]});
        let wire = rmp_serde::to_vec_named(&value).unwrap();
        let borrowed: BorrowedHashTimestampRequest<'_> = parse_msgpack_borrowed(&wire).unwrap();
        assert!(matches!(borrowed.rows[0].3[0].0, Cow::Owned(_)));
        let owned: BatchHashTimestampSetRequest = rmp_serde::from_slice(&wire).unwrap();
        assert_eq!(borrowed.into_owned(), owned);
    }
    let wire = rmp_serde::to_vec_named(&serde_json::json!({"shard_id":1,"fields":["ts"],
        "timestamp_field":"ts","rows":[["key",20,0,["not base64!"]]]})).unwrap();
    assert!(parse_msgpack_borrowed::<BorrowedHashTimestampRequest<'_>>(&wire).is_err());
    assert!(rmp_serde::from_slice::<BatchHashTimestampSetRequest>(&wire).is_err());
}

#[test]
fn borrowed_timestamp_payloads_survive_request_reuse_and_restart() {
    for asynchronous in [false, true] {
        for length in [48, 8192] {
            let dir = tempfile::tempdir().unwrap();
            let runtime = open(dir.path(), asynchronous);
            reset_borrowed_value_materializations_for_test();
            let initial = request(10, 8, 3, length);
            assert_eq!(apply(&runtime, &initial).applied, 8);
            let newest = request(20, 8, 3, length);
            assert_eq!(apply(&runtime, &newest).applied, 8);
            let stale = apply(&runtime, &request(15, 8, 3, length));
            assert_eq!((stale.applied, stale.skipped), (0, 8));
            assert_eq!(borrowed_value_materializations_for_test(), 0,
                "fresh and stale hot-path rows must not fall back to owned request payloads");
            verify(&runtime, &newest);
            runtime.inner.engine.write_ahead_log_store().flush(1).unwrap();
            drop(runtime);
            verify(&open(dir.path(), asynchronous), &newest);
        }
    }
}

#[test]
fn borrowed_timestamp_payloads_match_owned_validation_and_duplicate_fields() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let owned = open(first.path(), true);
    let borrowed = open(second.path(), true);
    let mut cases = Vec::new();
    let mut missing = request(10, 1, 2, 48);
    missing.timestamp_field = "missing".into();
    cases.push(missing);
    let mut arity = request(10, 1, 2, 48);
    arity.rows[0].3.pop();
    cases.push(arity);
    let mut mixed = request(10, 3, 2, 48);
    mixed.rows[0].2 = -1;
    mixed.rows[2].2 = 1_000_000_000;
    cases.push(mixed);
    let mut duplicate = request(20, 2, 2, 48);
    duplicate.fields[2] = duplicate.fields[1].clone();
    cases.push(duplicate);
    for case in cases {
        assert_eq!(apply(&borrowed, &case), owned.batch_hash_timestamp_set(case.clone()));
        for row in &case.rows {
            let command = Command::HashGetAll { key: row.0.clone() };
            assert_eq!(borrowed.execute(ExecuteRequest { shard_id: 1, command: command.clone() }),
                owned.execute(ExecuteRequest { shard_id: 1, command }));
        }
    }
}

#[test]
fn borrowed_timestamp_payloads_preserve_row_order_ttl_and_raw_timestamp_bytes() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let owned = open(first.path(), true);
    let borrowed = open(second.path(), true);
    let mut inputs = request(20, 1, 2, 48);
    inputs.ttl_ms = Some(60_000);
    inputs.rows[0].3[0].0 = prost::Message::encode_to_vec(&Timestamp { seconds: 5, nanos: 0 });
    let mut later = inputs.rows[0].clone();
    later.1 = 10;
    later.3[0].0 = prost::Message::encode_to_vec(&Timestamp { seconds: 10, nanos: 0 });
    later.3[1].0 = vec![71; 48];
    inputs.rows.push(later);
    let mut stale = inputs.rows[0].clone();
    stale.1 = 8;
    inputs.rows.push(stale);
    let result = apply(&borrowed, &inputs);
    assert_eq!(result, owned.batch_hash_timestamp_set(inputs.clone()));
    assert_eq!((result.applied, result.skipped), (2, 1));
    let expected = BatchHashTimestampSetRequest { rows: vec![inputs.rows[1].clone()], ..inputs.clone() };
    verify(&borrowed, &expected);
    let ttl = borrowed.execute(ExecuteRequest { shard_id: 1,
        command: Command::CommonTtl { key: inputs.rows[0].0.clone() } });
    let CommandResponse::Integer { value } = ttl.response else { panic!("ttl") };
    assert!(value > 0 && value <= 60_000);
}

#[test]
fn borrowed_timestamp_payloads_mirror_owned_commands_after_the_body_is_gone() {
    #[derive(Debug, Default)]
    struct Sink(Mutex<Vec<Command>>);
    impl SharedWalSink for Sink {
        fn record_write(&self, _: ShardId, command: &Command) {
            self.0.lock().unwrap().push(command.clone());
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let runtime = open(dir.path(), true);
    let sink = Arc::new(Sink::default());
    runtime.set_shared_wal_sink(sink.clone());
    let input = request(20, 1, 2, 48);
    reset_borrowed_value_materializations_for_test();
    let result = apply(&runtime, &input);
    assert_eq!(borrowed_value_materializations_for_test(), 3, "mirroring is the owned-path control");
    assert!(result.status.ok && result.applied == 1);
    let commands = sink.0.lock().unwrap();
    assert!(!commands.is_empty());
    let Command::HashMultiSetIfTimestampOlder { entries, .. } = &commands[0] else { panic!("conditional write") };
    assert_eq!(entries.iter().map(|(_, value)| value.clone()).collect::<Vec<_>>(),
        input.rows[0].3.iter().map(|value| value.0.clone()).collect::<Vec<_>>());
    drop(commands);
    verify(&runtime, &input);
}
