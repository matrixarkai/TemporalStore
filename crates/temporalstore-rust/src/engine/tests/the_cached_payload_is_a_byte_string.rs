// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE CACHED RESPONSE PAYLOAD IS A BYTE STRING, NOT AN ARRAY OF INTEGERS.
//!
//! # THE TOLL THIS REMOVES
//!
//! Left to the derive, `rmp_serde` writes a `Vec<u8>` element by element: a byte below 0x80 is a
//! positive fixint and costs one byte, a byte at or above it takes a `uint8` marker and costs TWO.
//! So the cached response inflated its payload by a factor set by the payload's byte distribution
//! -- 1.0x for ASCII, ~1.5x for uniform random bytes, 2.0x worst case.
//!
//! Measured in `the_payload_cache_per_user_byte` before this change: at 2,000 keys x 1 KiB the
//! cache held 4,138,000 B for a run of `b'v'` and 5,161,820 B for incompressible noise, a residual
//! of exactly half the value size with fills unchanged at 2.00 per key -- bytes per entry, not an
//! extra entry. `b'v'` is 0x76, below the line, where the toll is exactly zero, which is why every
//! fixture in this area had read 1.0x and the toll went unpriced.
//!
//! # WHY IT IS SAFE TO CHANGE THE ENCODING
//!
//! `CommandResponse` is serialized in exactly two places in this crate, `encode_cached_response`
//! and `decode_cached_response`, and the put is `put_memory_only` -- the memory tier, which does
//! not survive a restart. Nothing durable and nothing on the wire carries this encoding, and both
//! halves of the codec change together.
//!
//! rust-internal: measures this crate's own encoding, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_cached_payload_is_a_byte_string -- --nocapture

use crate::engine::command_validation::{decode_cached_response, encode_cached_response};
use crate::types::CommandResponse;

/// Uniform bytes, so that about half of them land at or above 0x80 where the old encoding charged
/// two bytes each.
fn uniform_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
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
fn the_cached_payload_survives_its_codec_at_either_byte_distribution() {
    for (label, payload) in [
        ("all low (0x76)", vec![b'v'; 1_024]),
        ("uniform", uniform_bytes(1_024)),
        ("all high (0xFF)", vec![0xFF_u8; 1_024]),
        ("empty", Vec::new()),
    ] {
        let response = CommandResponse::Bytes { value: Some(payload.clone()) };
        let encoded = encode_cached_response(&response).expect("the response encodes");
        let decoded = decode_cached_response(&encoded).expect("the response decodes");
        // THROUGH THE REAL PAIR, so the `CachedBytes` mirror is in the path. A mirror left without
        // the annotation decodes a shape the encoder never wrote, and this is what catches it.
        match decoded {
            CommandResponse::Bytes { value: Some(bytes) } => assert_eq!(
                bytes, payload,
                "{label}: the payload did not survive encode-then-decode"
            ),
            other => panic!("{label}: decoded to {other:?} instead of a byte payload"),
        }
    }

    // ---- AND `None` IS STILL `None`, not an empty payload. The two are different answers and the
    // ---- option codec is hand-written, so this is exactly where they could be collapsed.
    let absent = CommandResponse::Bytes { value: None };
    let encoded = encode_cached_response(&absent).expect("the absent response encodes");
    match decode_cached_response(&encoded).expect("the absent response decodes") {
        CommandResponse::Bytes { value: None } => {}
        other => panic!("an absent payload decoded as {other:?}, so absence became a value"),
    }
}

#[test]
fn the_encoded_payload_no_longer_depends_on_how_high_its_bytes_are() {
    const LEN: usize = 1_024;
    let low = encode_cached_response(&CommandResponse::Bytes { value: Some(vec![b'v'; LEN]) })
        .expect("encodes");
    let high = encode_cached_response(&CommandResponse::Bytes { value: Some(vec![0xFF; LEN]) })
        .expect("encodes");
    let uniform = encode_cached_response(&CommandResponse::Bytes {
        value: Some(uniform_bytes(LEN)),
    })
    .expect("encodes");

    println!();
    println!("  encoded bytes for a {LEN} B payload, by byte distribution:");
    println!("    all low (0x76)   {:>6}", low.len());
    println!("    uniform          {:>6}", uniform.len());
    println!("    all high (0xFF)  {:>6}", high.len());
    println!(
        "    the old integer-array encoding would have charged {} for all-high",
        2 * LEN
    );
    println!();

    // THE DISTRIBUTION STOPPED MATTERING, which is the property -- compared against each other
    // rather than against a magic constant, because the envelope's own size is not the subject.
    assert_eq!(
        low.len(),
        high.len(),
        "a payload of 0x76 encoded to {} B and one of 0xFF to {} B. The byte string form charges \
         by length alone, so these must agree -- if they differ, the payload is still being \
         written element by element",
        low.len(),
        high.len()
    );
    assert_eq!(
        low.len(),
        uniform.len(),
        "a uniform payload encoded to {} B against {} B for an all-low one, so the encoding still \
         depends on the byte distribution",
        uniform.len(),
        low.len()
    );

    // AND IT IS THE PAYLOAD PLUS A SMALL CONSTANT, not a multiple of it. 64 B of slack covers the
    // msgpack map, the variant tag and the length prefix without admitting a second copy.
    assert!(
        high.len() >= LEN && high.len() < LEN + 64,
        "a {LEN} B payload encoded to {} B. It should be the payload plus a small envelope; a \
         figure near {} means the integer-array encoding is still in use",
        high.len(),
        2 * LEN
    );
}

#[test]
fn the_old_integer_array_spelling_still_decodes() {
    // THE POSITIVE CONTROL. This mirror has NO annotation on its payload, so the derive writes
    // exactly what the cache held before this change: a msgpack array of integers. The visitor
    // claims to accept that spelling, and a compatibility path nobody exercises is a claim that
    // quietly stops being true.
    #[derive(serde::Serialize)]
    struct OldStyleBytes {
        kind: &'static str,
        value: Option<Vec<u8>>,
    }

    let payload = uniform_bytes(512);
    let old = {
        let mut packed = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        serde::Serialize::serialize(
            &OldStyleBytes { kind: "bytes", value: Some(payload.clone()) },
            &mut serializer,
        )
        .expect("the old spelling serializes");
        packed
    };

    // The control has to BE the old spelling, or it proves nothing: an array of integers is longer
    // than the payload once about half the bytes are at or above 0x80.
    assert!(
        old.len() > payload.len() + payload.len() / 4,
        "the control encoded {} B for a {} B payload, which is too small to be an integer array -- \
         it is not exercising the old spelling and so cannot show that the old spelling decodes",
        old.len(),
        payload.len()
    );

    match decode_cached_response(&old).expect("the old spelling decodes") {
        CommandResponse::Bytes { value: Some(bytes) } => assert_eq!(
            bytes, payload,
            "the old integer-array spelling decoded to different bytes"
        ),
        other => panic!("the old spelling decoded as {other:?}"),
    }
}
