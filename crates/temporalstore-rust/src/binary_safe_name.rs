// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A NAME THAT IS USUALLY TEXT AND MUST NEVER BE MANGLED.
//!
//! # THE DEFECT THIS EXISTS TO CLOSE
//!
//! A key and a hash field are binary-safe by specification: any byte string, including one that is
//! not valid UTF-8. Our RESP dispatch decodes both with `String::from_utf8_lossy`, which is not a
//! limitation but a WRONG ANSWER -- every invalid byte becomes U+FFFD, so two distinct names can
//! land on one stored name and the second write silently overwrites the first. The read side cannot
//! undo it either: `HKEYS` answers with the name as STORED, so the original bytes are gone rather
//! than merely hidden. `the_lossy_decode_collides_two_distinct_names` below pins that this is real
//! and not a worry.
//!
//! # WHAT THIS MODULE IS, AND WHAT IT IS DELIBERATELY NOT
//!
//! It is the ENCODING half, and the READER half of the sequencing: a name travels as a `String`
//! today, so a build must be able to READ the shape that carries arbitrary bytes before any build
//! WRITES one. Shipping the two together is the rolling-restart hazard -- a node still on the old
//! binary meets a name it cannot decode -- which is why this tree splits a frame format into a
//! reader commit and a writer commit, and this follows that shape.
//!
//! **Nothing in production calls this yet, on purpose.** It is a guard ahead of its caller, the
//! same way `the_tombstone_drop_rule_has_no_caller_on_purpose` is. The call sites are a separate
//! change: `Command::Hash*` carries `key: String` and `field: String`, and those two fields are
//! referenced from 198 non-test places across 32 files (494 including tests), so widening their
//! type is its own reviewable commit and not a side effect of adding an encoding.
//!
//! # THE SHAPE, AND WHY IT IS INJECTIVE
//!
//! Three cases, decided on the bytes:
//!
//! | input | output |
//! |---|---|
//! | valid UTF-8, does not begin with [`BINARY_TAG`] | the text itself, BYTE FOR BYTE |
//! | valid UTF-8, begins with [`BINARY_TAG`] | one [`BINARY_TAG`], then the text |
//! | not valid UTF-8 | one [`BINARY_TAG`], then base64 |
//!
//! The first row is the one that matters for cost: **every realistic name is emitted unchanged**, so
//! a stored name written before this exists reads back identically and a writer flip adds no bytes
//! to any ASCII field. Only a name that genuinely begins with `0x1b`, or that is not text at all,
//! is spelled differently.
//!
//! The second and third rows both begin with one tag, so on their own they would be ambiguous. They
//! are told apart by WHAT FOLLOWS IT, and the disjointness is a property of base64 rather than a
//! convention: **the base64 alphabet is `A-Za-z0-9+/=` and contains no `0x1b`**, so a tagged binary
//! name's second byte is never a tag, while a tagged text name's second byte always is -- the text
//! it tags began with one. That is asserted directly in
//! `the_two_tagged_branches_cannot_be_confused_because_base64_has_no_escape_byte`, so the argument
//! fails loudly if the alphabet assumption is ever broken, rather than being a comment someone has
//! to believe.
//!
//! `0x1b` is not an arbitrary pick: [`crate::bytes_serde::ESCAPE`] already reserves that byte for
//! the same job one layer down, and reusing it keeps one byte spoken for across both encodings
//! instead of two.

// A GUARD AHEAD OF ITS CALLER, so the lint is told rather than left to fire. The wiring is a
// separate commit by measurement: `Command::Hash*`'s `key` and `field` are referenced from
// 198 non-test places across 32 files.
#![allow(dead_code)]

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

/// The byte that marks a name as spelled rather than literal.
///
/// Shared with [`crate::bytes_serde::ESCAPE`] deliberately -- see the module note. A name that
/// begins with this byte is the only text that costs anything to carry.
pub(crate) const BINARY_TAG: u8 = crate::bytes_serde::ESCAPE;

/// `BINARY_TAG` as a `char`, for the string-side work.
const BINARY_TAG_CHAR: char = BINARY_TAG as char;

/// Spell `bytes` as a `String` without losing any of them.
///
/// The inverse is [`decode_name`]. Injective: see the module note for why the two tagged branches
/// cannot collide, and `encoding_is_injective_over_every_two_byte_name` for the exhaustive proof.
pub(crate) fn encode_name(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        // THE COMMON CASE IS A COPY, NOT AN ENCODING. A name that is text and does not begin with
        // the tag is its own spelling, which is what keeps a writer flip free for every ASCII
        // field and keeps already-stored names readable.
        Ok(text) if !text.starts_with(BINARY_TAG_CHAR) => text.to_string(),
        // Text that begins with the tag has to say so, or it would be read as a tagged binary
        // name. One tag in front is enough, because the text's own leading tag then sits in the
        // second position and base64 can never put one there.
        Ok(text) => {
            let mut out = String::with_capacity(text.len() + 1);
            out.push(BINARY_TAG_CHAR);
            out.push_str(text);
            out
        }
        // Not text at all: tag it and spell it.
        Err(_) => {
            let mut out = String::with_capacity(1 + (bytes.len() / 3 + 1) * 4);
            out.push(BINARY_TAG_CHAR);
            STANDARD.encode_string(bytes, &mut out);
            out
        }
    }
}

/// Read back what [`encode_name`] wrote, exactly.
///
/// Also the reader for a name written before this encoding existed: such a name is untagged text,
/// which is the first branch, so it decodes to its own bytes with no conversion and no migration.
pub(crate) fn decode_name(text: &str) -> Result<Vec<u8>, String> {
    let Some(tail) = text.strip_prefix(BINARY_TAG_CHAR) else {
        // Untagged: the name is its own bytes. This arm is every name this build writes today.
        return Ok(text.as_bytes().to_vec());
    };
    if tail.starts_with(BINARY_TAG_CHAR) {
        // Tagged text: give back the text with the tag this encoding added removed, which leaves
        // the tag the name itself started with.
        return Ok(tail.as_bytes().to_vec());
    }
    STANDARD
        .decode(tail.as_bytes())
        .map_err(|err| format!("a tagged name is not base64: {err}"))
}

/// Serde for a name field, so wiring one is an attribute and not a conversion at every site.
///
/// Kept beside the encoding rather than in `bytes_serde` because the two answer different
/// questions: that module asks how to carry a PAYLOAD cheaply and may move it beside the document,
/// while a name is part of the document's shape and has to stay a string a reader can match on.
pub(crate) mod serde_name {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::encode_name(bytes))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        super::decode_name(&text).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE DEFECT, PINNED. If this ever goes green the lossy decode has stopped colliding and the
    /// reason this module exists has changed.
    #[test]
    fn the_lossy_decode_collides_two_distinct_names() {
        let left = String::from_utf8_lossy(&[0xff]).to_string();
        let right = String::from_utf8_lossy(&[0xfe]).to_string();
        assert_eq!(
            left, right,
            "0xff and 0xfe are distinct names that the lossy decode maps onto one string, which is \
             the overwrite this encoding exists to prevent"
        );
        // And the encoding keeps them apart, which is the whole claim in one line.
        assert_ne!(
            encode_name(&[0xff]),
            encode_name(&[0xfe]),
            "the encoding must keep apart the two names the lossy decode merged"
        );
    }

    /// THE COST CLAIM: a name that is text is its own spelling, byte for byte. This is what lets a
    /// later writer flip cost nothing on every ASCII field and lets stored names read unchanged.
    #[test]
    fn a_text_name_is_spelled_as_itself_byte_for_byte() {
        for name in [
            "",
            "f",
            "field",
            "user:1234:embedding",
            "\u{1f600}",
            "a\u{1b}b",
        ] {
            assert_eq!(
                encode_name(name.as_bytes()),
                name,
                "a text name that does not begin with the tag must be emitted unchanged"
            );
            assert_eq!(
                decode_name(name).expect("a text name decodes"),
                name.as_bytes(),
                "and must read back as its own bytes"
            );
        }
    }

    /// THE CASE A REVIEWER ASKS ABOUT: a name whose LITERAL TEXT is the escape form. Both of these
    /// are real text a client may send, and neither may be mistaken for a tagged binary name.
    #[test]
    fn a_name_whose_literal_text_is_the_escape_form_survives() {
        // The nastiest shape available: begins with the tag AND its tail is valid base64, so a
        // decoder that only looked at the leading byte would base64-decode a text name.
        let adversarial: &[&[u8]] = &[
            b"\x1bAAAA",
            b"\x1b",
            b"\x1b\x1b",
            b"\x1bZm llbGQ=",
            b"\x1b\x1bAAAA",
        ];
        for bytes in adversarial {
            let encoded = encode_name(bytes);
            let decoded = decode_name(&encoded).expect("an adversarial text name decodes");
            assert_eq!(
                decoded, *bytes,
                "a name whose literal text is the escape form was not returned unchanged: \
                 encoded as {encoded:?}"
            );
        }
    }

    /// THE DISJOINTNESS IS A PROPERTY OF BASE64, NOT A CONVENTION, so it is asserted rather than
    /// asserted-in-prose. If the alphabet ever gained the escape byte this fails here instead of
    /// corrupting a name somewhere else.
    #[test]
    fn the_two_tagged_branches_cannot_be_confused_because_base64_has_no_escape_byte() {
        // Every byte base64 can emit, from the engine itself rather than from a literal alphabet.
        let mut produced = std::collections::BTreeSet::new();
        for length in 0..4usize {
            for value in 0..=u8::MAX {
                let bytes = vec![value; length];
                produced.extend(STANDARD.encode(&bytes).into_bytes());
            }
        }
        assert!(
            !produced.is_empty(),
            "DENOMINATOR: the sweep produced no base64 output, so it proves nothing"
        );
        assert!(
            !produced.contains(&BINARY_TAG),
            "base64 emitted the escape byte, so a tagged binary name's second byte could be a tag \
             and the two tagged branches would be ambiguous"
        );
    }

    /// EXHAUSTIVE over every one- and two-byte name: encoding is injective and decoding inverts it.
    /// Exhaustive rather than sampled because the interesting inputs are the invalid-UTF-8 ones and
    /// a sample is exactly what would miss the boundary between the branches.
    #[test]
    fn encoding_is_injective_over_every_two_byte_name() {
        let mut seen: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
        let mut checked = 0usize;
        let mut tagged = 0usize;
        for first in 0..=u8::MAX {
            let one = vec![first];
            let mut names = vec![one];
            for second in 0..=u8::MAX {
                names.push(vec![first, second]);
            }
            for name in names {
                let encoded = encode_name(&name);
                assert_eq!(
                    decode_name(&encoded).expect("every encoded name decodes"),
                    name,
                    "round trip lost {name:?}, encoded as {encoded:?}"
                );
                if encoded.as_bytes().first() == Some(&BINARY_TAG) {
                    tagged += 1;
                }
                if let Some(other) = seen.insert(encoded.clone(), name.clone()) {
                    assert_eq!(
                        other, name,
                        "two distinct names encode to {encoded:?}, so the encoding is not injective"
                    );
                }
                checked += 1;
            }
        }
        // DENOMINATORS, so a sweep that silently stopped early cannot read as a pass.
        assert_eq!(
            checked,
            256 + 256 * 256,
            "DENOMINATOR: {checked} names checked, not every one- and two-byte name"
        );
        assert_eq!(
            seen.len(),
            checked,
            "DENOMINATOR: {} distinct encodings for {checked} distinct names",
            seen.len()
        );
        // AND THE SWEEP MUST HAVE EXERCISED BOTH BRANCHES. A sweep that only ever took the cheap
        // arm would pass every assertion above and prove nothing about the tagged ones.
        assert!(
            tagged > 0,
            "DENOMINATOR: no name in the sweep took a tagged branch, so the branches that need \
             proving were never run"
        );
        println!("  {checked} names checked, {tagged} of them tagged, all round-tripped");
    }

    /// A FOUR-BYTE NAME IS THE SHAPE A FEATURE-SERVING CLIENT ACTUALLY SENDS (a 4-byte hash of the
    /// field name), which is the population the collision rate was computed over. Deterministic
    /// walk, not a random sample, so a failure reproduces.
    #[test]
    fn every_four_byte_name_of_a_deterministic_walk_round_trips() {
        let mut state = 0x9e3779b97f4a7c15u64;
        let mut checked = 0usize;
        let mut not_text = 0usize;
        for _ in 0..200_000 {
            // A fixed multiplier rather than a seeded RNG: a randomised fixture that fails once is
            // a fixture nobody can re-run.
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let name = (state >> 32).to_le_bytes()[..4].to_vec();
            if std::str::from_utf8(&name).is_err() {
                not_text += 1;
            }
            let encoded = encode_name(&name);
            assert_eq!(
                decode_name(&encoded).expect("every four-byte name decodes"),
                name,
                "round trip lost the four-byte name {name:?}"
            );
            checked += 1;
        }
        assert_eq!(checked, 200_000, "DENOMINATOR: {checked} names walked");
        // The population has to be mostly non-text or this test is not about the hard case.
        assert!(
            not_text * 2 > checked,
            "DENOMINATOR: only {not_text} of {checked} four-byte names were not text, so this walk \
             is not exercising the branch it exists for"
        );
        println!("  {checked} four-byte names checked, {not_text} of them not valid text");
    }

    /// A tagged name that is not base64 is refused rather than guessed at. A decoder that returned
    /// something for a corrupt name would hand a caller a silently wrong name, which is the class
    /// of defect this module removes.
    #[test]
    fn a_tagged_name_that_is_not_base64_is_refused() {
        let refused = decode_name("\u{1b}not base64!!");
        assert!(
            refused.is_err(),
            "a tagged name that is not base64 decoded to {refused:?} instead of being refused"
        );
    }
}
