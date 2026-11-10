// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A PER-KEY STATE BLOB IS A PROTOBUF MESSAGE ON DISK, AND THE GUARDS THAT HOLD IT THERE.
//!
//! The schema and the reasoning are in `engine::key_state_wire`. This module proves four things.
//!
//!   1. EVERY FIELD SURVIVES, VALUE FOR VALUE. Sixteen names (twelve capture maps, four container
//!      carries) over a corpus that holds each at least once, plus the two edge shapes the capture
//!      depends on: a PRESENT-but-empty map (the fold inserts an empty one) and an ABSENT one (the
//!      fold removes the entry) must come back as different answers.
//!   2. THE NUMBERS DO NOT MOVE. A round trip written and read with the same schema is blind to two
//!      field numbers being swapped, so each field is pinned against bytes spelled out by hand.
//!   3. A DOCUMENT IS REFUSED. JSON text, a msgpack map, an empty payload, an unknown tag and a
//!      known tag with the wrong wire type are errors and are counted; a record cut anywhere never
//!      invents a field it did not carry; and a record row written before this change (a map where a
//!      message belongs) is refused rather than read.
//!   4. THE REAL WRITERS' BLOBS TAKE THE SAME PATH. A workload over every container model is
//!      written through the engine, read back from the log, and every blob in it round-trips; the
//!      fold over that log restores each model's element; and the bytes a blob costs on disk are
//!      measured against what the document form cost.

#![allow(clippy::all)]
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::hll::{DistinctBucket, Hll};
use crate::engine::key_state_wire::{
    decode_blob, encode_blob, key_state_blobs_refused, reset_key_state_blobs_refused,
    CAPTURE_FIELDS,
};
use crate::engine::state::ControlStateSelectionValue;
use serde_json::{json, Value};
use std::collections::BTreeMap;

const SHARD: ShardId = 4091;

fn entry(slab: u64, offset: u64, length: u64, block: Option<u64>) -> ElementEntry {
    ElementEntry::from_parts(slab, offset, length, block, block)
}

fn series(points: u64) -> Value {
    let map: BTreeMap<u64, ElementEntry> = (0..points)
        .map(|index| (1_700_000_000_000 + index, entry(1 + index, 4_096 * index, 120, Some(index))))
        .collect();
    serde_json::to_value(&map).expect("a series serializes")
}

fn address(slab: u64, offset: u64, length: u64, block: Option<u64>) -> Value {
    serde_json::to_value(entry(slab, offset, length, block)).expect("an address serializes")
}

fn a_capture_with_every_field() -> Value {
    let mut changes: BTreeMap<u64, DistinctBucket> = BTreeMap::new();
    changes.insert(
        1_000,
        DistinctBucket::Exact([b"a".to_vec(), b"bb".to_vec()].into_iter().collect()),
    );
    let mut sketch = Hll::new();
    sketch.add(b"x");
    sketch.add(b"y");
    changes.insert(2_000, DistinctBucket::Sketch(sketch));
    let selection = ControlStateSelectionValue {
        occur_time_ms: 1_700_000_000_123,
        value: vec![1, 2, 3, 255],
        selection_type: crate::types::ControlStateSelectionType::Last,
    };
    json!({
        "key": "tenant/1/object/7",
        "features": series(3),
        "expires_at_ms": 1_800_000_000_000u64,
        "control_state_changes": serde_json::to_value(&changes).unwrap(),
        "control_state_selection": serde_json::to_value(&selection).unwrap(),
        "context_nodes": address(2, 64, 32, None),
        "context_events": series(2),
        "context_indexes": series(1),
        "context_audits": series(2),
        "context_children": series(1),
        "context_summaries": series(2),
        "context_compressions": series(1),
        "context_entities": series(3),
    })
}

fn a_carry_with_every_field() -> Value {
    json!({
        "key": "tenant/1/object/8",
        "set_elements": serde_json::to_value(vec![
            (b"m1".to_vec(), entry(3, 10, 20, Some(0))),
            (b"member-two".to_vec(), entry(3, 40, 20, Some(1))),
        ]).unwrap(),
        "zset_elements": serde_json::to_value(vec![
            (b"z1".to_vec(), (0x4012_0000_0000_0000u64, entry(4, 8, 24, Some(0)))),
        ]).unwrap(),
        "list_elements": serde_json::to_value(vec![
            (-3i64, entry(5, 0, 16, None)),
            (7i64, entry(5, 16, 16, Some(2))),
        ]).unwrap(),
        "hash_fields": serde_json::to_value(vec![
            ("field-a".to_string(), entry(6, 100, 33, Some(4))),
        ]).unwrap(),
    })
}

fn the_corpus() -> Vec<Value> {
    vec![
        a_capture_with_every_field(),
        a_carry_with_every_field(),
        // The removal spelling: a capture naming only its key.
        json!({"key": "only-a-key"}),
        // A present-but-EMPTY map: the fold inserts an empty series.
        json!({"key": "empty-series", "features": {}}),
        // A zero address and a deadline of zero are legal values and must not read as absent.
        json!({"key": "zeroes", "expires_at_ms": 0u64, "context_nodes": address(0, 0, 0, None)}),
    ]
}

// ------------------------------------------------------------------------------------------
// 1. every field survives
// ------------------------------------------------------------------------------------------

/// CLAIM 1.
#[test]
fn every_field_survives_the_wire_value_for_value() {
    let corpus = the_corpus();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for blob in &corpus {
        let bytes = encode_blob(blob).expect("a blob the writers build encodes");
        let back = decode_blob(&bytes).expect("and decodes");
        assert_eq!(&back, blob, "the blob changed on the way through the wire");
        for name in blob.as_object().unwrap().keys() {
            seen.insert(name.clone());
        }
    }
    // NON-VACUITY: the corpus holds every one of the sixteen names, so "every field survives" is not
    // a statement about the nine that happened to be there.
    let mut wanted: Vec<&str> = vec!["key"];
    wanted.extend(CAPTURE_FIELDS);
    wanted.extend(crate::engine::CARRIED_CONTAINER_FIELDS);
    for name in wanted {
        assert!(seen.contains(name), "the corpus never carries `{name}`");
    }
}

/// THE DISTINCTION THE CAPTURE DEPENDS ON: absent REMOVES, present-but-empty INSERTS.
#[test]
fn a_present_but_empty_map_and_an_absent_one_stay_different_answers() {
    let absent = decode_blob(&encode_blob(&json!({"key": "k"})).unwrap()).unwrap();
    let empty = decode_blob(&encode_blob(&json!({"key": "k", "features": {}})).unwrap()).unwrap();
    assert!(absent.get("features").is_none(), "an absent map came back present: {absent}");
    assert_eq!(
        empty.get("features"),
        Some(&json!({})),
        "a present-but-empty map came back absent, which the fold would read as a removal: {empty}"
    );
}

// ------------------------------------------------------------------------------------------
// 2. the numbers do not move
// ------------------------------------------------------------------------------------------

fn tag(field: u64, wire_type: u64) -> Vec<u8> {
    let mut value = (field << 3) | wire_type;
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

fn length_delimited(field: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = tag(field, 2);
    out.push(payload.len() as u8);
    out.extend_from_slice(payload);
    out
}

fn varint_field(field: u64, value: u8) -> Vec<u8> {
    let mut out = tag(field, 0);
    out.push(value);
    out
}

/// Slab 1, offset 2, length 4, no block id and no generation, spelled out by hand.
fn address_bytes() -> Vec<u8> {
    vec![0x08, 0x01, 0x10, 0x02, 0x18, 0x04]
}

/// One point at instant 3 holding that address: `Series { points: [Point { at: 3, address }] }`.
fn series_bytes() -> Vec<u8> {
    let mut point = varint_field(1, 3);
    point.extend(length_delimited(2, &address_bytes()));
    length_delimited(1, &point)
}

fn key_k() -> Vec<u8> {
    length_delimited(1, b"k")
}

/// CLAIM 2. Each field is pinned against the bytes its documented number produces.
#[test]
fn the_schema_numbers_do_not_move() {
    let one_point = json!({"3": address(1, 2, 4, None)});
    for (name, number) in [
        ("features", 2u64),
        ("context_events", 7),
        ("context_indexes", 8),
        ("context_audits", 9),
        ("context_children", 10),
        ("context_summaries", 11),
        ("context_compressions", 12),
        ("context_entities", 13),
    ] {
        let mut expected = key_k();
        expected.extend(length_delimited(number, &series_bytes()));
        let got = encode_blob(&json!({"key": "k", name: one_point.clone()})).unwrap();
        assert_eq!(got, expected, "`{name}` is not written at field {number}");
    }

    let mut expected = key_k();
    expected.extend(varint_field(3, 5));
    assert_eq!(
        encode_blob(&json!({"key": "k", "expires_at_ms": 5u64})).unwrap(),
        expected,
        "`expires_at_ms` is not written at field 3"
    );

    let mut expected = key_k();
    expected.extend(length_delimited(6, &address_bytes()));
    assert_eq!(
        encode_blob(&json!({"key": "k", "context_nodes": address(1, 2, 4, None)})).unwrap(),
        expected,
        "`context_nodes` is not written at field 6"
    );

    // control_state_changes: Changes { buckets: [Bucket { bucket_ms: 1000, exact: ["a"] }] }
    let mut bucket = tag(1, 0);
    bucket.extend([0xe8, 0x07]);
    bucket.extend(length_delimited(2, b"a"));
    let changes = length_delimited(1, &bucket);
    let mut expected = key_k();
    expected.extend(length_delimited(4, &changes));
    let mut exact = BTreeMap::new();
    exact.insert(1_000u64, DistinctBucket::Exact([b"a".to_vec()].into_iter().collect()));
    assert_eq!(
        encode_blob(&json!({"key": "k", "control_state_changes": serde_json::to_value(&exact).unwrap()}))
            .unwrap(),
        expected,
        "`control_state_changes` is not written at field 4 as `{{bucket_ms, exact[]}}`"
    );

    // control_state_selection: Selection { occur_time_ms: 9, value: [7], selection_type: 1 }
    let mut selection = varint_field(1, 9);
    selection.extend(length_delimited(2, &[7]));
    selection.extend(varint_field(3, 1));
    let mut expected = key_k();
    expected.extend(length_delimited(5, &selection));
    let value = ControlStateSelectionValue {
        occur_time_ms: 9,
        value: vec![7],
        selection_type: crate::types::ControlStateSelectionType::Last,
    };
    assert_eq!(
        encode_blob(&json!({"key": "k", "control_state_selection": serde_json::to_value(&value).unwrap()}))
            .unwrap(),
        expected,
        "`control_state_selection` is not written at field 5"
    );

    // The carries: set 20, zset 21, list 22, hash 23.
    let carried = |name: &str, elements: Value, element: Vec<u8>, number: u64| {
        let mut expected = key_k();
        expected.extend(length_delimited(number, &element));
        assert_eq!(
            encode_blob(&json!({"key": "k", name: elements})).unwrap(),
            expected,
            "`{name}` is not written at field {number}"
        );
    };
    let mut set_element = length_delimited(1, b"m");
    set_element.extend(length_delimited(2, &address_bytes()));
    carried(
        "set_elements",
        serde_json::to_value(vec![(b"m".to_vec(), entry(1, 2, 4, None))]).unwrap(),
        set_element,
        20,
    );
    let mut zset_element = length_delimited(1, b"m");
    zset_element.extend(varint_field(2, 5));
    zset_element.extend(length_delimited(3, &address_bytes()));
    carried(
        "zset_elements",
        serde_json::to_value(vec![(b"m".to_vec(), (5u64, entry(1, 2, 4, None)))]).unwrap(),
        zset_element,
        21,
    );
    // A list sequence is zigzag: 3 is written as 6.
    let mut list_element = varint_field(1, 6);
    list_element.extend(length_delimited(2, &address_bytes()));
    carried(
        "list_elements",
        serde_json::to_value(vec![(3i64, entry(1, 2, 4, None))]).unwrap(),
        list_element,
        22,
    );
    let mut hash_field = length_delimited(1, b"f");
    hash_field.extend(length_delimited(2, &address_bytes()));
    carried(
        "hash_fields",
        serde_json::to_value(vec![("f".to_string(), entry(1, 2, 4, None))]).unwrap(),
        hash_field,
        23,
    );
}

// ------------------------------------------------------------------------------------------
// 3. a document is refused
// ------------------------------------------------------------------------------------------

/// CLAIM 3, the byte-level half.
#[test]
fn a_document_or_a_malformed_message_is_refused_and_counted() {
    reset_key_state_blobs_refused();
    let document = json!({"key": "k", "expires_at_ms": 5u64});
    let refusals: Vec<(&str, Vec<u8>)> = vec![
        ("JSON text", serde_json::to_vec(&document).unwrap()),
        ("a msgpack map", rmp_serde::to_vec_named(&document).unwrap()),
        ("an empty payload", Vec::new()),
        ("an unknown tag", {
            let mut bytes = key_k();
            bytes.extend(varint_field(15, 1));
            bytes
        }),
        ("a known tag with the wrong wire type", {
            let mut bytes = key_k();
            bytes.extend(length_delimited(3, &[5]));
            bytes
        }),
        ("a message with no key", varint_field(3, 5)),
        ("a length that runs off the end", vec![0x0a, 0x09, b'k']),
    ];
    for (what, bytes) in &refusals {
        assert!(decode_blob(bytes).is_err(), "{what} was read as a key-state blob");
    }
    assert!(
        key_state_blobs_refused() >= refusals.len() as u64,
        "{} refusal(s) were counted for {} refused blobs",
        key_state_blobs_refused(),
        refusals.len()
    );
    // CONTROL: the same blob as a message is read, so the refusals above are about the SHAPE.
    let control = encode_blob(&document).unwrap();
    assert_eq!(decode_blob(&control).unwrap(), document);
}

/// A RECORD CUT ANYWHERE NEVER INVENTS A FIELD. Cut on a field boundary it is a shorter, valid
/// message, which is why the record's own frame checksum is what detects a torn tail; cut anywhere
/// else it is refused. Either way, what it does return is a subset of what was written.
#[test]
fn a_blob_cut_anywhere_never_invents_a_field() {
    let blob = a_capture_with_every_field();
    let bytes = encode_blob(&blob).unwrap();
    let whole = blob.as_object().unwrap();
    let mut refused = 0usize;
    let mut shorter = 0usize;
    for cut in 0..bytes.len() {
        match decode_blob(&bytes[..cut]) {
            Err(_) => refused += 1,
            Ok(partial) => {
                shorter += 1;
                for (name, value) in partial.as_object().unwrap() {
                    assert_eq!(
                        whole.get(name),
                        Some(value),
                        "a cut at byte {cut} produced `{name}` with a value the blob never carried"
                    );
                }
            }
        }
    }
    assert!(refused > 0, "no cut was refused, so the decode does not check its framing");
    println!("[cut] {} cuts: {refused} refused, {shorter} decoded as a shorter message", bytes.len());
}

/// A row as it is stored: positional, with the blobs in one field.
#[derive(serde::Serialize, serde::Deserialize)]
struct Row {
    #[serde(with = "crate::engine::key_state_wire::blobs")]
    ks: Vec<Value>,
}

/// What a record row written before this change holds in the same slot: a map per blob.
#[derive(serde::Serialize)]
struct OldRow {
    ks: Vec<Value>,
}

/// CLAIM 3, the row half.
#[test]
fn a_row_written_before_this_change_is_refused_and_a_current_one_is_read() {
    let blobs = the_corpus();
    let current = rmp_serde::to_vec(&Row { ks: blobs.clone() }).expect("a current row encodes");
    let read: Row = rmp_serde::from_slice(&current).expect("and reads back");
    assert_eq!(read.ks, blobs, "a current row did not round-trip");

    reset_key_state_blobs_refused();
    let old = rmp_serde::to_vec(&OldRow { ks: blobs }).expect("an old row encodes");
    let refused = rmp_serde::from_slice::<Row>(&old);
    assert!(refused.is_err(), "a row holding maps was read as a row holding messages");
    assert_eq!(
        1,
        key_state_blobs_refused(),
        "the refusal of the old row was not counted exactly once"
    );
}

// ------------------------------------------------------------------------------------------
// 4. the real writers' blobs
// ------------------------------------------------------------------------------------------

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn run(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest { shard_id: SHARD, command });
    assert!(response.status.ok, "a write failed: {}", response.status.message);
}

const MEMBERS: usize = 6;

/// Every container model, a TTL, a feature series and a counter, with removals so the capture runs.
fn write_every_model(engine: &TemporalEngine) {
    engine.load_shard(SHARD);
    run(engine, Command::StringSet { key: "oracle-string".to_string(), value: b"v".to_vec() });
    run(engine, Command::CommonExpire { key: "oracle-string".to_string(), ttl_ms: 3_600_000 });
    for index in 0..MEMBERS {
        run(engine, Command::HashSet {
            key: "oracle-hash".to_string(),
            field: format!("oracle-field-{index}"),
            value: format!("value-{index}").into_bytes(),
        });
        run(engine, Command::SetAdd {
            key: "oracle-set".to_string(),
            member: format!("oracle-member-{index}").into_bytes(),
        });
        run(engine, Command::ZSetAdd {
            key: "oracle-zset".to_string(),
            member: format!("oracle-zmember-{index}").into_bytes(),
            score: index as f64 + 0.5,
        });
        run(engine, Command::ListPush {
            key: "oracle-list".to_string(),
            member: format!("oracle-item-{index}").into_bytes(),
            left: false,
        });
    }
    // Removals, so the shrink capture runs for these keys too.
    run(engine, Command::SetRemove {
        key: "oracle-set".to_string(),
        member: b"oracle-member-0".to_vec(),
    });
    run(engine, Command::HashDelete {
        key: "oracle-hash".to_string(),
        field: "oracle-field-0".to_string(),
    });
    run(engine, Command::ControlStateIncrement {
        key: "oracle-counter".to_string(),
        timestamp_ms: 1_700_000_000_000,
        amount: 3,
    });
    run(engine, Command::ControlStateChangeAdd {
        key: "oracle-change".to_string(),
        timestamp_ms: 1_700_000_000_000,
        value: b"distinct".to_vec(),
        precision_ms: None,
        ttl_ms: None,
    });
}

/// What a blob costs stored as the document form did: the `Value` as a msgpack map in the row.
fn document_cost(blob: &Value) -> usize {
    rmp_serde::to_vec_named(blob).expect("a blob encodes as a document").len()
}

/// What it costs now: one binary field in the row. The field is a msgpack `bin`, whose header is
/// two bytes up to 255 bytes of payload and three up to 65,535.
fn message_cost(blob: &Value) -> usize {
    let length = encode_blob(blob).expect("a blob encodes").len();
    length + if length <= 255 { 2 } else if length <= 65_535 { 3 } else { 5 }
}

/// CLAIM 4: the blobs the engine's own writers put in the log take the wire, and the fold restores
/// each model's element from them.
#[test]
fn the_blobs_the_engine_writes_round_trip_and_the_fold_restores_every_model() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    write_every_model(&engine);

    let records = engine
        .index_log_store
        .read_delta_records(SHARD, 0)
        .expect("the delta log reads back");
    assert!(records.len() >= MEMBERS, "DENOMINATOR: {} records", records.len());

    let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let (mut blobs, mut before, mut after) = (0usize, 0usize, 0usize);
    for record in &records {
        for blob in &record.key_states {
            blobs += 1;
            for name in blob.as_object().expect("a blob is an object").keys() {
                names.insert(name.clone());
            }
            let again = decode_blob(&encode_blob(blob).unwrap()).unwrap();
            assert_eq!(&again, blob, "a blob the engine wrote does not survive a second trip");
            before += document_cost(blob);
            after += message_cost(blob);
        }
    }
    println!("[oracle] {} record(s), {blobs} blob(s), fields seen: {names:?}", records.len());
    for needed in crate::engine::CARRIED_CONTAINER_FIELDS {
        assert!(names.contains(needed), "the engine wrote no blob carrying `{needed}`");
    }
    assert!(names.contains("expires_at_ms"), "the engine wrote no blob carrying a deadline");
    println!(
        "[bytes] the engine's {blobs} blob(s): {before} B as documents, {after} B as messages \
         ({:+.1}%)",
        (after as f64 - before as f64) * 100.0 / before as f64
    );
    assert!(after < before, "the message form is not smaller: {after} against {before}");

    // THE FOLD, over a log with no base index: nothing else can supply these elements.
    drop(engine);
    let base = dir.path().join("indexes").join(format!("shard-{SHARD}.index.json"));
    if base.exists() {
        std::fs::remove_file(&base).expect("delete the base index");
    }
    let reloaded = engine_at(dir.path());
    let folded = reloaded
        .load_index_checked(SHARD, false)
        .expect("the fold must not error")
        .expect("a log with no base folds to a state");
    let member = b"oracle-member-3".to_vec();
    let zmember = b"oracle-zmember-3".to_vec();
    assert!(
        folded.bucket_index.sets.get("oracle-set").and_then(|m| m.get(&member)).is_some(),
        "the set element did not come back from the log"
    );
    assert!(
        folded.bucket_index.zsets.get("oracle-zset").and_then(|m| m.get(&zmember)).is_some(),
        "the zset element did not come back from the log"
    );
    assert!(
        folded.bucket_index.hashes.get("oracle-hash").and_then(|m| m.get("oracle-field-3")).is_some(),
        "the hash field did not come back from the log"
    );
    assert!(
        folded.bucket_index.lists.get("oracle-list").map_or(false, |entries| entries.len() > 0),
        "the list elements did not come back from the log"
    );
    assert!(
        folded.expires_at_ms().get("oracle-string").is_some(),
        "the deadline did not come back from the log"
    );
    // OBSERVED, NOT ASSERTED: whether the member removed above is back after the fold. The fold's
    // restore filters carried elements on whether a block still sits at their address, and a
    // removal's block handling is not this change's subject; the value is printed so a reader can
    // see what this fixture does, and a change here would show in the figure above first.
    let removed_is_back = folded
        .bucket_index
        .sets
        .get("oracle-set")
        .and_then(|m| m.get(&b"oracle-member-0".to_vec()))
        .is_some();
    println!("[oracle] removed set member present after the fold: {removed_is_back}");
}

/// THE BYTES, stated plainly, over a corpus that holds every field: what each blob costs stored as
/// the document form did, and what it costs now. Printed as a table; asserted only in the direction
/// the change claims (never larger), because the sizes themselves move with the corpus.
#[test]
fn what_a_blob_costs_on_disk_as_a_document_and_as_a_message() {
    println!("\n{:<44} {:>10} {:>10} {:>8}", "blob", "document B", "message B", "change");
    let mut rows = Vec::new();
    for blob in the_corpus() {
        let key = blob["key"].as_str().unwrap().to_string();
        rows.push((key, document_cost(&blob), message_cost(&blob)));
    }
    // A distinct-count bucket past the sketch threshold is the widest single value a blob holds.
    let mut changes = BTreeMap::new();
    let mut sketch = Hll::new();
    for index in 0..5_000u32 {
        sketch.add(&index.to_be_bytes());
    }
    changes.insert(1_000u64, DistinctBucket::Sketch(sketch));
    let sketched = json!({"key": "sketched", "control_state_changes": serde_json::to_value(&changes).unwrap()});
    rows.push(("sketched distinct bucket (4096 registers)".to_string(), document_cost(&sketched), message_cost(&sketched)));
    // A 200-point series, which is what a feature append's capture writes for a long posting.
    let long = json!({"key": "long-series", "features": series(200)});
    rows.push(("a 200-point feature series".to_string(), document_cost(&long), message_cost(&long)));
    for (name, document, message) in &rows {
        println!(
            "{name:<44} {document:>10} {message:>10} {:>+7.1}%",
            (*message as f64 - *document as f64) * 100.0 / *document as f64
        );
        assert!(message <= document, "`{name}` costs more as a message: {message} against {document}");
    }
}
