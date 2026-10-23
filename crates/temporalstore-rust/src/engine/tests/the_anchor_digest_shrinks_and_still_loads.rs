// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE ANCHOR DIGEST AS RAW BYTES: what it saves, and that every older spelling still loads.
//!
//! # THE CHANGE
//!
//! `IndexAnchorRecord::index_sha256` carried `bytes_serde` — the JSON base64 codec — on a msgpack
//! path, so its 32 bytes were written as 46. It now carries `bytes_serde::binary`, a `bin` string,
//! at 34. Twelve bytes an anchor, and the record's doc finally describes what it holds.
//!
//! # WHAT THIS ARM IS MOSTLY FOR
//!
//! A format change's obligation is the OLD spelling, so the saving is one assertion here and
//! compatibility is three — one per spelling this log has ever written:
//!
//!   * **base64 string**, what the anchor wrote until now;
//!   * **array of integers**, the oldest shape, from before the parent module existed;
//!   * **bin string**, the new one, round-tripped.
//!
//! Each old payload is built by ENCODING a mirror struct that carries the old annotation, never by
//! pasting a byte literal. A literal would pin what I believe the old encoder produced; a mirror
//! pins what it actually produces.
//!
//! # AND THE DOWNGRADE DIRECTION, WHICH IS THE ONE THAT USUALLY BITES
//!
//! A format change is dangerous when an older reader cannot read the newer bytes. Here the decoder
//! is a single visitor — `bytes_serde::EitherShape` — that accepts all three spellings, and it has
//! had `visit_bytes` since the parent module was introduced. So a build from before this change
//! reads the new anchor. Asserted by decoding the new payload through the PARENT module's
//! annotation, which is precisely what that build had.
//!
//! rust-internal: encodes this crate's own records, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_anchor_digest_shrinks_and_still_loads -- --nocapture

use crate::index_log::IndexAnchorRecord;

/// The anchor as it was written until this change: the digest base64'd by the JSON codec.
#[derive(serde::Serialize, serde::Deserialize)]
struct AnchorAsBase64 {
    shard_id: crate::types::ShardId,
    sequence: u64,
    #[serde(with = "crate::bytes_serde")]
    index_sha256: Vec<u8>,
    index_len: u64,
}

/// The anchor in the oldest shape: the digest as a plain sequence of integers.
#[derive(serde::Serialize, serde::Deserialize)]
struct AnchorAsArray {
    shard_id: crate::types::ShardId,
    sequence: u64,
    index_sha256: Vec<u8>,
    index_len: u64,
}

fn digest() -> Vec<u8> {
    // Digest-shaped: uniform, so about half the bytes sit at or above 0x80. A run of one low byte
    // would make the array spelling look free and hide what the encodings actually differ by.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..32)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

#[test]
fn the_anchor_digest_shrinks_and_still_loads() {
    let bytes = digest();
    let above = bytes.iter().filter(|byte| **byte >= 0x80).count();
    let anchor = IndexAnchorRecord {
        shard_id: 1,
        sequence: 7,
        index_sha256: bytes.clone(),
        index_len: 4_096,
    };

    let now = rmp_serde::to_vec(&anchor).expect("the anchor packs");
    let as_base64 = rmp_serde::to_vec(&AnchorAsBase64 {
        shard_id: 1,
        sequence: 7,
        index_sha256: bytes.clone(),
        index_len: 4_096,
    })
    .expect("the base64 spelling packs");
    let as_array = rmp_serde::to_vec(&AnchorAsArray {
        shard_id: 1,
        sequence: 7,
        index_sha256: bytes.clone(),
        index_len: 4_096,
    })
    .expect("the array spelling packs");

    println!();
    println!("  THE ANCHOR RECORD, WHOLE, IN THREE SPELLINGS OF ITS DIGEST");
    println!();
    println!("    digest bytes at or above 0x80: {above} of 32");
    println!();
    println!("    {:<40} {:>8}", "spelling", "bytes");
    println!("    {:<40} {:>8}", "array of integers (oldest)", as_array.len());
    println!("    {:<40} {:>8}", "base64 string (until this change)", as_base64.len());
    println!("    {:<40} {:>8}", "bin string (now)", now.len());
    println!();
    println!("    saving against base64: {} bytes an anchor", as_base64.len() - now.len());
    println!();

    // ---- THE FIXTURE IS THE RIGHT SHAPE.
    assert!(
        above > 8,
        "only {above} of 32 digest bytes are at or above 0x80, so the array spelling has little to \
         charge for and the comparison understates it"
    );

    // ---- THE SAVING. One assertion, because it is the easy half.
    assert!(
        now.len() < as_base64.len(),
        "the anchor packs {} bytes against {} for the base64 spelling it replaces. A bin string is \
         the length prefix plus the bytes; base64 is four characters per three bytes, so this must \
         be smaller",
        now.len(),
        as_base64.len()
    );

    // ---- COMPATIBILITY, THE ACTUAL OBLIGATION: each older payload still decodes as an anchor,
    // ---- with the digest intact. These are the bytes already on disk.
    let from_base64: IndexAnchorRecord =
        rmp_serde::from_slice(&as_base64).expect("an anchor written as base64 still decodes");
    assert_eq!(
        from_base64.index_sha256, bytes,
        "an anchor written with the base64 spelling decoded to a different digest. That is every \
         anchor already on disk, and the decoder is supposed to accept it through `visit_str`"
    );
    assert_eq!(from_base64.sequence, 7, "the base64 anchor lost its sequence");

    let from_array: IndexAnchorRecord =
        rmp_serde::from_slice(&as_array).expect("an anchor written as an array still decodes");
    assert_eq!(
        from_array.index_sha256, bytes,
        "an anchor written with the oldest array spelling decoded to a different digest -- the \
         decoder accepts it through `visit_seq` and must keep doing so"
    );

    let round_trip: IndexAnchorRecord =
        rmp_serde::from_slice(&now).expect("the new spelling round-trips");
    assert_eq!(round_trip.index_sha256, bytes, "the new spelling did not round-trip");
    assert_eq!(round_trip.index_len, 4_096);

    // ---- AND THE DOWNGRADE DIRECTION. A build from before this change read the digest through the
    // ---- PARENT module's annotation; decoding the new payload that way is what such a build does.
    // ---- This is the direction a format change usually breaks, and here it does not.
    let as_old_build: AnchorAsBase64 = rmp_serde::from_slice(&now)
        .expect("a reader built before this change still decodes the new anchor");
    assert_eq!(
        as_old_build.index_sha256, bytes,
        "a pre-change reader decoded the new anchor to a different digest. Both annotations use the \
         same visitor, which has accepted `visit_bytes` since the parent module existed, so this \
         must hold -- if it stops, the change needs a format version gate after all"
    );
}
