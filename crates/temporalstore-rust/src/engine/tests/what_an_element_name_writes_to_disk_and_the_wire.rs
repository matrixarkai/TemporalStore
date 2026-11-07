// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE NAME OF AN ELEMENT WRITES, PINNED BY THE BYTES AND NOT BY THE FIELD IT RIDES IN.
//!
//! The name of a hash field, a set member or a timestamped point is carried on a handful of
//! structs and through two hand-written encoders. Its Rust field is being renamed (the old word is
//! counted down by `component_is_retired`), and a rename of a serde field is where a published
//! format changes by accident: an `alias` is DESERIALIZE-ONLY, so a field renamed with an alias and
//! no `rename` silently writes a different key. That exact defect reached `main` once and broke a
//! published manifest.
//!
//! So the bytes are held here, captured on the code as it stood BEFORE the rename and asserted
//! after it unchanged. Every record below is built by DECODING text, never by a struct literal,
//! which is what lets this file be read by the old and the new field name alike -- it names
//! neither. A literal would have to be edited by the rename, and a golden that is edited along with
//! the code it pins proves nothing.
//!
//! What is pinned, and why each is a different surface:
//!
//!   * the WAL record as JSON, written from BOTH the short keys a current record carries and the
//!     long key an older record still carries, which must write the same bytes;
//!   * the same record through the protobuf framer, where the name rides tag 11 of an item and
//!     tag 4 of a staged block, and a name that is a number rides `timestamp_ms` instead;
//!   * the key a block is filed under in the index log, where the name is a slot of a colon
//!     string;
//!   * a tombstone row of the index dump, and a row of the published block-index report, which
//!     are serde structs WITHOUT a rename and so spell the key by the field's name.
#![allow(clippy::all)]

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A record built from text. The three outcomes are a hash field, a timestamped point (whose name is
/// a number, so it rides a different protobuf field) and a whole-object string with no name.
fn record_from(name_key_outcome: &str, name_key_block: &str) -> crate::wal::WriteAheadLogRecord {
    let text = format!(
        r#"{{"s":1,"q":9,"p":[{{"object_id":7,{name_key_block}:"f0","bytes":[1,2,3]}}],"t":[{{"k":"hash","o":"user:1",{name_key_outcome}:"f0","i":7,"b":3}},{{"k":"feature","o":"series",{name_key_outcome}:"0000000000000007","i":8,"b":3}},{{"k":"string","o":"plain","i":9,"b":3}}]}}"#
    );
    serde_json::from_str(&text).unwrap_or_else(|err| panic!("the record text must decode: {err}\n{text}"))
}

const WAL_JSON: &str = "{\"s\":1,\"q\":9,\"p\":[{\"object_id\":7,\"k\":\"f0\",\"bytes\":\"AQID\"}],\"t\":[{\"k\":\"hash\",\"o\":\"user:1\",\"c\":\"f0\",\"i\":7,\"b\":3},{\"k\":\"feature\",\"o\":\"series\",\"c\":\"0000000000000007\",\"i\":8,\"b\":3},{\"k\":\"string\",\"o\":\"plain\",\"i\":9,\"b\":3}]}";
/// What the protobuf framing decodes to. NOT `WAL_JSON`: a name that is a number rides `timestamp_ms`
/// and comes back in its canonical decimal spelling, `7` for the `0000000000000007` that went in. That
/// is the framer's existing behaviour and is pinned here as it is.
const WAL_JSON_AFTER_THE_FRAMER: &str = "{\"s\":1,\"q\":9,\"p\":[{\"object_id\":7,\"k\":\"f0\",\"bytes\":\"AQID\"}],\"t\":[{\"k\":\"hash\",\"o\":\"user:1\",\"c\":\"f0\",\"i\":7,\"b\":3},{\"k\":\"feature\",\"o\":\"series\",\"c\":\"7\",\"i\":8,\"b\":3},{\"k\":\"string\",\"o\":\"plain\",\"i\":9,\"b\":3}]}";
const WAL_PROTO_HEX: &str = "b8080110092a1a1a06757365723a312907000000000000005a02663078039801022a191a0673657269657329080000000000000078038001079801062a151a05706c61696e29090000000000000078039801013212090700000000000000120301020322026630";
const BLOCK_REF_KEY: &str = "hash:user:1:f0:1:2:3:4:5|string:plain::1:2:3:4:5";
const TOMBSTONE_ROW_JSON: &str = "{\"object_key\":\"user:1\",\"model_id\":\"hash\",\"component\":\"f0\",\"block\":[1,2,3]}";
const BLOCK_INDEX_REPORT_JSON: &str = "{\"object_key\":\"user:1\",\"model_id\":\"hash\",\"component\":\"f0\",\"routing_slot\":3,\"block_segment_id\":1,\"offset\":2,\"length\":3,\"dirty\":false,\"deleted\":false,\"log_backed\":false,\"native_packed_block_index_len\":0,\"native_packed_block_index_hex\":\"\"}";

/// A current record (short keys) and an older one (the long key) write the SAME JSON, and it is the
/// short key. This is the assertion that fails for a field renamed with an alias and no `rename`.
#[test]
fn the_short_key_and_the_long_key_write_one_wal_json() {
    let current = record_from(r#""c""#, r#""k""#);
    let older = record_from(r#""component""#, r#""component""#);
    let from_current = serde_json::to_string(&current).expect("serializes");
    let from_older = serde_json::to_string(&older).expect("serializes");
    assert_eq!(
        from_current, from_older,
        "a record read from the long key must write the key a current record writes"
    );
    assert_eq!(from_current, WAL_JSON);
    // And the long spelling is never WRITTEN: a reader of an older build sees only the short keys.
    assert!(
        !from_current.contains("component") && !from_current.contains("element_name"),
        "the key a record writes is `c` for an outcome and `k` for a block: {from_current}"
    );
}

/// The protobuf framing of the same record, and its decode back to the same JSON.
#[test]
fn the_protobuf_framing_of_a_named_record_does_not_move() {
    let record = record_from(r#""c""#, r#""k""#);
    let encoded = crate::wal_proto::encode(&record).expect("encodes");
    assert_eq!(hex(&encoded), WAL_PROTO_HEX);
    let decoded = crate::wal_proto::decode(&encoded).expect("decodes");
    assert_eq!(
        serde_json::to_string(&decoded).expect("serializes"),
        WAL_JSON_AFTER_THE_FRAMER,
        "what the framer wrote must decode to the record it was given"
    );
}

/// The key a block is filed under: kind, object key, the NAME, then five numbers.
#[test]
fn the_key_a_block_is_filed_under_spells_the_name_in_its_third_slot() {
    let named = crate::index_log::block_ref_key_from_parts("hash", "user:1", Some("f0"), 1, 2, 3, 4, 5);
    let whole = crate::index_log::block_ref_key_from_parts("string", "plain", None, 1, 2, 3, 4, 5);
    assert_eq!(format!("{named}|{whole}"), BLOCK_REF_KEY);
}

/// A tombstone row of the index dump, read from the key the old build wrote and written back under
/// it. The struct has NO `rename`, so the key it writes is the field's own name.
#[test]
fn the_tombstone_row_keeps_its_key() {
    let tombstone: crate::engine::state::TombstoneElement = serde_json::from_str(
        r#"{"object_key":"user:1","model_id":"hash","component":"f0","block":[1,2,3]}"#,
    )
    .expect("a tombstone row written by the old build must still decode");
    assert_eq!(
        serde_json::to_string(&tombstone).expect("serializes"),
        TOMBSTONE_ROW_JSON
    );
}

/// A row of the published block-index report: the same, and the key here is read by consumers.
#[test]
fn the_report_row_keeps_its_key() {
    let report: crate::engine::reports::StoragePhysicalBlockIndex = serde_json::from_str(
        r#"{"object_key":"user:1","model_id":"hash","component":"f0","routing_slot":3,"block_segment_id":1,"offset":2,"length":3,"dirty":false,"deleted":false,"log_backed":false,"native_packed_block_index_len":0,"native_packed_block_index_hex":""}"#,
    )
    .expect("a report row must decode");
    assert_eq!(
        serde_json::to_string(&report).expect("serializes"),
        BLOCK_INDEX_REPORT_JSON
    );
}
