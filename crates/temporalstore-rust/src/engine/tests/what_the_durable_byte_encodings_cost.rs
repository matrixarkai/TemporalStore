// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE DURABLE PATHS CHARGE FOR A BYTE PAYLOAD — three clean negatives and one real toll.
//!
//! # THE QUESTION, AND WHY IT DID NOT CARRY OVER
//!
//! `the_payload_cache_per_user_byte` found the cached payload charging **two bytes for every byte at
//! or above 0x80**: msgpack serializes a `Vec<u8>` as an ARRAY OF INTEGERS, a positive fixint below
//! 0x80 and a two-byte uint8 above it. #2279 annotated that field as a byte string and the cost
//! went to 1.0x. The open question was what the same encoder costs on the paths that reach disk.
//!
//! Answered by reading the encoders, not by assuming the shape carries:
//!
//! | path | encoder | byte payload | toll |
//! |---|---|---|---|
//! | WAL record | `prost` | `#[prost(bytes = "vec")]` | **none** — length-delimited |
//! | index-log delta | msgpack | *no `Vec<u8>` field at all* | **none** |
//! | index-log anchor | msgpack | `index_sha256`, base64 via `bytes_serde` | **real, and backwards** |
//!
//! # THE ONE REAL FINDING IS THE OPPOSITE WAY ROUND
//!
//! `IndexAnchorRecord::index_sha256` carries `#[serde(with = "crate::bytes_serde")]`, which is the
//! **JSON** codec: it has no `is_human_readable` branch and always emits `serialize_str` of base64.
//! On this msgpack path a 32-byte digest is therefore written as **44 characters**, where a msgpack
//! `bin` string would be 34.
//!
//! The toll is small and bounded — one anchor record, not one per key. It is worth measuring for a
//! different reason than its size: **the anchor's own doc says it carries "the digest as its 32 raw
//! bytes", and it does not.** A record whose documentation names a wire shape it is not using is
//! what makes the next format decision wrong.
//!
//! # THIS ARM MEASURES AND DOES NOT CHANGE THE FORMAT
//!
//! Re-pointing that annotation is a durable-format change: anchors already on disk are base64
//! strings, so it needs a decoder that takes both spellings and a test that proves the old one still
//! loads. That is a separate change carrying a compatibility obligation. Pricing it first means the
//! decision gets made against a number.
//!
//! rust-internal: encodes this crate's own records, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib what_the_durable_byte_encodings_cost -- --ignored --nocapture

/// A digest-shaped payload: uniform bytes, so about half of them sit at or above 0x80 and the
/// array encoding charges two for each of those. A run of one low byte would hide the toll
/// entirely, which is the fixture mistake this whole area has paid for before.
fn digest_shaped(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

#[test]
#[ignore = "encodes a few records and compares their lengths; run by name"]
fn what_the_durable_byte_encodings_cost() {
    const DIGEST: usize = 32;
    let bytes = digest_shaped(DIGEST);
    let above = bytes.iter().filter(|byte| **byte >= 0x80).count();

    // The three spellings, each encoded by the same msgpack serializer so only the SHAPE differs.
    struct AsArray<'a>(&'a [u8]);
    impl serde::Serialize for AsArray<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_seq(self.0.iter())
        }
    }
    struct AsBin<'a>(&'a [u8]);
    impl serde::Serialize for AsBin<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0)
        }
    }
    struct AsBase64<'a>(&'a [u8]);
    impl serde::Serialize for AsBase64<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            crate::bytes_serde::serialize(self.0, serializer)
        }
    }

    let as_array = rmp_serde::to_vec(&AsArray(&bytes)).expect("array packs");
    let as_bin = rmp_serde::to_vec(&AsBin(&bytes)).expect("bin packs");
    let as_base64 = rmp_serde::to_vec(&AsBase64(&bytes)).expect("base64 packs");

    println!();
    println!("  A {DIGEST}-BYTE DIGEST THROUGH ONE MSGPACK SERIALIZER, THREE SHAPES");
    println!();
    println!("    bytes at or above 0x80: {above} of {DIGEST}");
    println!();
    println!("    {:<34} {:>8} {:>10}", "shape", "bytes", "per byte");
    for (label, packed) in [
        ("array of integers (unannotated)", &as_array),
        ("base64 string (bytes_serde, JSON)", &as_base64),
        ("bin string (serialize_bytes)", &as_bin),
    ] {
        println!(
            "    {:<34} {:>8} {:>9.3}x",
            label,
            packed.len(),
            packed.len() as f64 / DIGEST as f64
        );
    }
    println!();
    println!(
        "    the anchor record uses the MIDDLE one on a msgpack path -- {} bytes against {} for",
        as_base64.len(),
        as_bin.len()
    );
    println!(
        "    `bin`, a toll of {} per anchor. Its doc says it carries the digest as 32 raw bytes.",
        as_base64.len() - as_bin.len()
    );
    println!();

    // ---- THE FIXTURE IS THE RIGHT SHAPE. A run of one low byte would make the array encoding look
    // ---- free, which is how a byte-distribution toll hides.
    assert!(
        above > DIGEST / 4,
        "only {above} of {DIGEST} fixture bytes are at or above 0x80, so the array encoding has \
         almost nothing to charge for and the comparison below understates it"
    );

    // ---- `bin` IS THE CHEAPEST, which is the whole claim.
    assert!(
        as_bin.len() < as_base64.len() && as_bin.len() < as_array.len(),
        "bin packs {} bytes against base64 {} and array {}. A byte string carries a length prefix \
         and the bytes; the other two carry an expansion of them, so bin must be smallest",
        as_bin.len(),
        as_base64.len(),
        as_array.len()
    );

    // ---- AND BASE64 IS A REAL TOLL ON THIS PATH, not a rounding difference. Four characters per
    // ---- three bytes is a flat third, whatever the bytes are.
    assert!(
        as_base64.len() >= as_bin.len() + DIGEST / 4,
        "base64 packs {} against bin's {}, a difference of {}. Base64 is four characters per three \
         bytes, so on {DIGEST} bytes it must cost about a third more -- a smaller gap means this \
         is no longer measuring the base64 path",
        as_base64.len(),
        as_bin.len(),
        as_base64.len() - as_bin.len()
    );

    // ---- THE WAL CHARGES NOTHING, asserted at source rather than by encoding: its payload fields
    // ---- are protobuf `bytes`, which is length-delimited.
    let wal_source = include_str!("../../wal_record.rs");
    assert!(
        wal_source.contains("#[prost(bytes = \"vec\", tag = \"2\")]"),
        "the WAL record's payload field is no longer a protobuf `bytes` field. The claim that the \
         WAL pays no byte-encoding toll rests on that, so if it moved to a serde encoder the toll \
         has to be re-measured rather than inherited from this arm"
    );

    // ---- AND THE DELTA RECORD STILL HAS NO RAW BYTE FIELD, which is the other negative. Checked
    // ---- at source, because the claim is about the absence of a field and no encoding can show it.
    let index_source = include_str!("../../index_log.rs");
    let delta = index_source
        .split("pub struct IndexDeltaRecord")
        .nth(1)
        .and_then(|rest| rest.split("\n}").next())
        .expect("the delta record is declared");
    assert!(
        !delta.contains("Vec<u8>"),
        "IndexDeltaRecord has gained a Vec<u8> field. It is written through a msgpack serializer \
         with no byte annotation by default, so a raw buffer there would pay the array toll on \
         every delta the log writes -- which is the largest durable tier on an ingest-heavy store"
    );
}
